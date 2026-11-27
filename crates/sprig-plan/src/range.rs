//! The range analyzer: from a condition to the runs of a key that can
//! hold its rows.
//!
//! For one key, a condition becomes a tree over key parts. A level of the
//! tree is a sorted list of disjoint intervals of one key part's values;
//! an interval that is a single value may carry the level for the next
//! key part. `AND` intersects levels of the same part and hangs a deeper
//! level under every interval of a shallower one; `OR` unites levels of
//! the same part. What can't be said about a key ("no restriction") is the
//! absence of a tree; an empty level means no row can match.
//!
//! The tree never holds fewer rows than the condition accepts: where it
//! can't follow the condition exactly it is looser, and the condition is
//! checked again on the rows read.

use std::cmp::Ordering;
use std::ops::Bound;

use sprig_engine::keys::collation::{Collation, Pad};
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::{BinOp, Expr, ExprType};

use crate::access::KeyRange;
use crate::catalog::{IndexPart, PathPart, TableDef};
use crate::keyval::{KeyVal, key_value, truncate};

/// The most ranges one scan is given; past it the deepest key parts are
/// left to the row check.
const MAX_RANGES: usize = 4096;

/// One end of an interval: a place between the values of a key part.
#[derive(Debug, Clone, PartialEq)]
enum Edge {
    /// Before every value, `NULL` included.
    Min,
    /// Just before (`false`) or just after (`true`) a value.
    At(Value, bool),
    /// After every value.
    Max,
}

fn cmp_edge(ty: &ColumnType, a: &Edge, b: &Edge) -> Ordering {
    match (a, b) {
        (Edge::Min, Edge::Min) | (Edge::Max, Edge::Max) => Ordering::Equal,
        (Edge::Min, _) | (_, Edge::Max) => Ordering::Less,
        (_, Edge::Min) | (Edge::Max, _) => Ordering::Greater,
        (Edge::At(x, xs), Edge::At(y, ys)) => ty.compare(x, y).then(xs.cmp(ys)),
    }
}

/// The values of one key part between two edges.
#[derive(Debug, Clone)]
struct Interval {
    lo: Edge,
    hi: Edge,
    next: Option<Box<PartTree>>,
}

impl Interval {
    /// The single value the interval holds, if it holds just one.
    fn point(&self, ty: &ColumnType) -> Option<&Value> {
        match (&self.lo, &self.hi) {
            (Edge::At(a, false), Edge::At(b, true)) if ty.compare(a, b) == Ordering::Equal => {
                Some(a)
            }
            _ => None,
        }
    }
}

/// One level of a range tree: the intervals of key part `part`.
#[derive(Debug, Clone)]
pub(crate) struct PartTree {
    part: usize,
    list: Vec<Interval>,
    /// Whether the tree holds exactly the rows of the condition it was
    /// built from, not more: every part of the condition found its place
    /// in it.
    exact: bool,
}

impl PartTree {
    /// Whether no row can match.
    pub(crate) fn impossible(&self) -> bool {
        self.list.is_empty()
    }

    /// The key part the tree restricts, when it restricts only that one
    /// and says exactly what its condition says.
    pub(crate) fn exact_part(&self) -> Option<usize> {
        (self.exact && self.list.iter().all(|iv| iv.next.is_none())).then_some(self.part)
    }

    fn never(part: usize) -> Self {
        Self {
            part,
            list: Vec::new(),
            exact: true,
        }
    }
}

/// What the analyzer knows about one key of one table.
pub(crate) struct KeyCtx<'a> {
    /// The table.
    pub(crate) table: &'a TableDef,
    /// The parts ranges may bind: the key's parts and, for a secondary
    /// index, the primary-key parts its entries end with.
    pub(crate) parts: &'a [IndexPart],
    /// The slot of the table's first column in the expressions analysed.
    pub(crate) base: usize,
    /// Evaluates a constant expression: its value and static type, or
    /// `None` when it can't be evaluated now.
    pub(crate) eval: &'a dyn Fn(&Expr) -> Option<(Value, ExprType)>,
    /// Per part of the key, its JSON path ([`PathPart`]); empty for a key
    /// of columns only.
    pub(crate) paths: &'a [Option<PathPart>],
}

/// The column slot and path of `JSON_VALUE(col, path ...)`,
/// `JSON_EXTRACT(col, path)` or `JSON_UNQUOTE(JSON_EXTRACT(col, path))`
/// with a constant path.
fn json_path(e: &Expr) -> Option<(usize, &str)> {
    fn col_path<'e>(c: &Expr, p: &'e Expr) -> Option<(usize, &'e str)> {
        match (c, p) {
            (Expr::Column(c), Expr::Str(p)) => Some((c.slot, p.as_str())),
            _ => None,
        }
    }
    match e {
        Expr::Func(n, a) if n == "JSON_UNQUOTE" && a.len() == 1 => match &a[0] {
            Expr::Func(m, b) if m == "JSON_EXTRACT" && b.len() == 2 => col_path(&b[0], &b[1]),
            _ => None,
        },
        Expr::Func(n, a)
            if (n == "JSON_EXTRACT" && a.len() == 2) || (n == "JSON_VALUE" && a.len() >= 2) =>
        {
            col_path(&a[0], &a[1])
        }
        _ => None,
    }
}

impl KeyCtx<'_> {
    fn path(&self, part: usize) -> Option<&PathPart> {
        self.paths.get(part).and_then(Option::as_ref)
    }

    fn ty(&self, part: usize) -> &ColumnType {
        match self.path(part) {
            Some(p) => &p.ty,
            None => &self.table.columns[self.parts[part].column].ty,
        }
    }

    fn nullable(&self, part: usize) -> bool {
        // A document may lack the path, or hold null there.
        self.path(part).is_some() || self.table.columns[self.parts[part].column].nullable
    }

    /// The first key part over the column `e` reads, if `e` is a bare
    /// column of the table.
    fn part_of(&self, e: &Expr) -> Option<usize> {
        // A path part: the same column and path.
        if let Some((slot, path)) = json_path(e) {
            let col = slot.checked_sub(self.base)?;
            return (0..self.parts.len()).find(|&i| {
                self.parts[i].column == col && self.path(i).is_some_and(|p| p.path == path)
            });
        }
        let Expr::Column(c) = e else { return None };
        let col = c.slot.checked_sub(self.base)?;
        if col >= self.table.columns.len() {
            return None;
        }
        (0..self.parts.len()).find(|&i| self.parts[i].column == col && self.path(i).is_none())
    }

    fn is_real(&self, konst: &Expr) -> bool {
        (self.eval)(konst).is_some_and(|(_, ty)| ty.kind.is_real())
    }

    /// The edge below every non-`NULL` value of a part.
    fn floor(&self, part: usize) -> Edge {
        if self.nullable(part) {
            Edge::At(Value::Null, true)
        } else {
            Edge::Min
        }
    }

    fn level(&self, part: usize, lo: Edge, hi: Edge) -> PartTree {
        let mut t = PartTree::never(part);
        // An index prefix holds the start of a value only; a path part's
        // rows are always checked again.
        t.exact = self.parts[part].prefix.is_none() && self.path(part).is_none();
        if cmp_edge(self.ty(part), &lo, &hi) == Ordering::Less {
            t.list.push(Interval { lo, hi, next: None });
        }
        t
    }

    /// The level for `column op constant`. `real` says the comparison is
    /// carried out in doubles because another operand of the same
    /// predicate (a `BETWEEN` end, an `IN` item) is one.
    fn compare(&self, part: usize, op: BinOp, konst: &Expr, real: bool) -> Option<PartTree> {
        let (v, ty) = (self.eval)(konst)?;
        if real || ty.kind.is_real() {
            let wide = match v {
                Value::Int(i) => i.unsigned_abs() >= 1 << 53,
                Value::UInt(u) => u >= 1 << 53,
                _ => false,
            };
            match self.ty(part) {
                ColumnType::Decimal { .. } => return None,
                ColumnType::Int { bytes: 8, .. } if wide => return None,
                _ => {}
            }
        }
        let prefix = self.parts[part].prefix;
        let fit = |v: Value| match prefix {
            Some(n) => truncate(self.ty(part), v, n),
            None => v,
        };
        let never = || Some(PartTree::never(part));
        let kv = key_value(self.ty(part), &v, &ty);
        if op == BinOp::NullSafeEq && kv == KeyVal::Null {
            return if self.nullable(part) {
                Some(self.level(
                    part,
                    Edge::At(Value::Null, false),
                    Edge::At(Value::Null, true),
                ))
            } else {
                never()
            };
        }
        // An index prefix holds the start of a value: everything at or
        // beyond the constant's own start may match.
        let loose = prefix.is_some();
        match (kv, op) {
            (KeyVal::Unusable, _) => None,
            (KeyVal::Null, _) => never(),
            (KeyVal::Exact(v), BinOp::Eq | BinOp::NullSafeEq) => {
                let v = fit(v);
                Some(self.level(part, Edge::At(v.clone(), false), Edge::At(v, true)))
            }
            (KeyVal::Between(..), BinOp::Eq | BinOp::NullSafeEq) => never(),
            (KeyVal::Exact(v), BinOp::Lt) => {
                Some(self.level(part, self.floor(part), Edge::At(fit(v), loose)))
            }
            (KeyVal::Exact(v), BinOp::Le) => {
                Some(self.level(part, self.floor(part), Edge::At(fit(v), true)))
            }
            (KeyVal::Exact(v), BinOp::Gt) => {
                Some(self.level(part, Edge::At(fit(v), !loose), Edge::Max))
            }
            (KeyVal::Exact(v), BinOp::Ge) => {
                Some(self.level(part, Edge::At(fit(v), false), Edge::Max))
            }
            (KeyVal::Between(below, _), BinOp::Lt | BinOp::Le) => match below {
                Some(b) => Some(self.level(part, self.floor(part), Edge::At(fit(b), true))),
                None => never(),
            },
            (KeyVal::Between(_, above), BinOp::Gt | BinOp::Ge) => match above {
                Some(a) => Some(self.level(part, Edge::At(fit(a), false), Edge::Max)),
                None => never(),
            },
            _ => None,
        }
    }

    /// The level for `column LIKE 'prefix%'`: the strings from the prefix
    /// followed by the lowest characters to the prefix followed by the
    /// highest. Only where strings order by their code points or bytes
    /// (the `_bin` collations and binary strings): under the other
    /// collations "the highest string with this prefix" has no simple
    /// spelling, and the pattern is checked row by row.
    fn like(&self, part: usize, pattern: &Value, ty: &ExprType) -> Option<PartTree> {
        const MAX_CHAR: &str = "\u{10FFFF}";
        if pattern.is_null() {
            return Some(PartTree::never(part));
        }
        let (Value::Str(p) | Value::Bytes(p)) = pattern else {
            return None;
        };
        // The literal start of the pattern: up to the first wildcard or
        // escape character.
        let end = p
            .iter()
            .position(|b| matches!(b, b'%' | b'_' | b'\\'))
            .unwrap_or(p.len());
        let prefix = &p[..end];
        if prefix.is_empty() {
            return None;
        }
        let text = match *self.ty(part) {
            ColumnType::Char { chars, collation } => Some((u32::from(chars), collation)),
            ColumnType::VarChar { chars, collation } => Some((chars, collation)),
            _ => None,
        };
        let (lo, hi) = match (*self.ty(part), text) {
            (_, Some((chars, collation)))
                if matches!(
                    collation,
                    Collation::Utf8mb4Bin | Collation::Utf8mb4NopadBin
                ) && crate::keyval::collation_holds(collation, ty) =>
            {
                let used = collation.charset().char_count(prefix) as u32;
                let rest = chars.saturating_sub(used) as usize;
                let mut hi = prefix.to_vec();
                hi.extend(MAX_CHAR.repeat(rest).as_bytes());
                let mut lo = prefix.to_vec();
                // Under PAD SPACE a string is compared as if followed by
                // spaces, so one that goes on with a lower character
                // sorts before the bare prefix.
                if collation.pad() == Pad::Space {
                    lo.extend(std::iter::repeat_n(0u8, rest));
                }
                (
                    Value::Str(bytes::Bytes::from(lo)),
                    Value::Str(bytes::Bytes::from(hi)),
                )
            }
            (ColumnType::Binary { bytes }, _) if ty.kind == sprig_exec::Kind::Str => {
                Self::byte_bounds(prefix, usize::from(bytes))
            }
            (ColumnType::VarBinary { bytes }, _) if ty.kind == sprig_exec::Kind::Str => {
                Self::byte_bounds(prefix, bytes as usize)
            }
            _ => return None,
        };
        let fit = |v: Value| match self.parts[part].prefix {
            Some(n) => truncate(self.ty(part), v, n),
            None => v,
        };
        let mut t = self.level(part, Edge::At(fit(lo), false), Edge::At(fit(hi), true));
        t.exact = false;
        Some(t)
    }

    fn byte_bounds(prefix: &[u8], max: usize) -> (Value, Value) {
        let mut hi = prefix.to_vec();
        hi.resize(max.max(prefix.len()), 0xFF);
        (Value::bytes(prefix), Value::Bytes(bytes::Bytes::from(hi)))
    }

    /// The tree of a condition; `None` when it doesn't restrict the key.
    pub(crate) fn tree(&self, e: &Expr) -> Option<PartTree> {
        match e {
            Expr::Binary(BinOp::And, a, b) => {
                let (a, b) = (self.tree(a), self.tree(b));
                // A side the key can't follow makes the result a superset.
                let exact =
                    a.as_ref().is_some_and(|t| t.exact) && b.as_ref().is_some_and(|t| t.exact);
                self.and(a, b).map(|mut t| {
                    t.exact = exact;
                    t
                })
            }
            Expr::Binary(BinOp::Or, a, b) => {
                let (a, b) = (self.tree(a)?, self.tree(b)?);
                self.or(a, b)
            }
            Expr::Binary(
                op
                @ (BinOp::Eq | BinOp::NullSafeEq | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge),
                a,
                b,
            ) => {
                if let Some(part) = self.part_of(a) {
                    if crate::exprs::is_const(b) {
                        return self.compare(part, *op, b, false);
                    }
                }
                if let Some(part) = self.part_of(b) {
                    if crate::exprs::is_const(a) {
                        let flipped = match op {
                            BinOp::Lt => BinOp::Gt,
                            BinOp::Le => BinOp::Ge,
                            BinOp::Gt => BinOp::Lt,
                            BinOp::Ge => BinOp::Le,
                            other => *other,
                        };
                        return self.compare(part, flipped, a, false);
                    }
                }
                None
            }
            Expr::Between(x, lo, hi, false) => {
                let part = self.part_of(x)?;
                if !crate::exprs::is_const(lo) || !crate::exprs::is_const(hi) {
                    return None;
                }
                let real = self.is_real(lo) || self.is_real(hi);
                let a = self.compare(part, BinOp::Ge, lo, real);
                let b = self.compare(part, BinOp::Le, hi, real);
                // Both ends must be comparable in the column's type: the
                // servers pick one comparison for the three operands.
                match (a, b) {
                    (Some(a), Some(b)) => self.and(Some(a), Some(b)),
                    _ => None,
                }
            }
            Expr::In(x, list, false) => {
                let part = self.part_of(x)?;
                let mut acc = PartTree::never(part);
                if !list.iter().all(crate::exprs::is_const) {
                    return None;
                }
                let real = list.iter().any(|item| self.is_real(item));
                for item in list {
                    let t = self.compare(part, BinOp::Eq, item, real)?;
                    acc = self.or(acc, t)?;
                }
                Some(acc)
            }
            Expr::Like(x, pattern, None, false) => {
                let part = self.part_of(x)?;
                if !crate::exprs::is_const(pattern) {
                    return None;
                }
                let (v, ty) = (self.eval)(pattern)?;
                self.like(part, &v, &ty)
            }
            Expr::IsNull(x, not) => {
                let part = self.part_of(x)?;
                Some(if *not {
                    self.level(part, self.floor(part), Edge::Max)
                } else if self.nullable(part) {
                    self.level(
                        part,
                        Edge::At(Value::Null, false),
                        Edge::At(Value::Null, true),
                    )
                } else {
                    PartTree::never(part)
                })
            }
            _ => None,
        }
    }

    pub(crate) fn and(&self, a: Option<PartTree>, b: Option<PartTree>) -> Option<PartTree> {
        let (a, b) = match (a, b) {
            (None, x) | (x, None) => return x,
            (Some(a), Some(b)) => (a, b),
        };
        let (mut a, b) = if a.part <= b.part { (a, b) } else { (b, a) };
        if a.part < b.part {
            for iv in &mut a.list {
                let next = iv.next.take().map(|n| *n);
                iv.next = self.and(next, Some(b.clone())).map(Box::new);
            }
            // What must also hold on a deeper part can't hold: no row of
            // the interval matches.
            a.list
                .retain(|iv| !iv.next.as_deref().is_some_and(PartTree::impossible));
            a.exact &= b.exact;
            return Some(a);
        }
        let ty = self.ty(a.part);
        let mut out = PartTree::never(a.part);
        out.exact = a.exact && b.exact;
        for x in &a.list {
            for y in &b.list {
                let lo = if cmp_edge(ty, &x.lo, &y.lo) == Ordering::Less {
                    &y.lo
                } else {
                    &x.lo
                };
                let hi = if cmp_edge(ty, &x.hi, &y.hi) == Ordering::Greater {
                    &y.hi
                } else {
                    &x.hi
                };
                if cmp_edge(ty, lo, hi) != Ordering::Less {
                    continue;
                }
                let next = self.and(x.next.as_deref().cloned(), y.next.as_deref().cloned());
                // A deeper level that can't match empties this interval.
                if next.as_ref().is_some_and(PartTree::impossible) {
                    continue;
                }
                out.list.push(Interval {
                    lo: lo.clone(),
                    hi: hi.clone(),
                    next: next.map(Box::new),
                });
            }
        }
        out.list.sort_by(|p, q| cmp_edge(ty, &p.lo, &q.lo));
        Some(out)
    }

    fn or(&self, a: PartTree, b: PartTree) -> Option<PartTree> {
        let exact = a.exact && b.exact;
        if a.impossible() {
            return Some(PartTree { exact, ..b });
        }
        if b.impossible() {
            return Some(PartTree { exact, ..a });
        }
        if a.part != b.part {
            return None;
        }
        let mut exact = exact;
        let ty = self.ty(a.part);
        let mut all: Vec<Interval> = a.list.into_iter().chain(b.list).collect();
        all.sort_by(|p, q| cmp_edge(ty, &p.lo, &q.lo));
        let mut out: Vec<Interval> = Vec::with_capacity(all.len());
        for iv in all {
            let Some(last) = out.last_mut() else {
                out.push(iv);
                continue;
            };
            if cmp_edge(ty, &iv.lo, &last.hi) != Ordering::Less {
                out.push(iv);
                continue;
            }
            // They overlap. Two copies of one value keep what both say
            // about the next part; anything else forgets the next part.
            let same_point = match (last.point(ty), iv.point(ty)) {
                (Some(x), Some(y)) => ty.compare(x, y) == Ordering::Equal,
                _ => false,
            };
            if same_point {
                last.next = match (last.next.take(), iv.next) {
                    (Some(x), Some(y)) => self.or(*x, *y).map(Box::new),
                    _ => None,
                };
            } else {
                if cmp_edge(ty, &iv.hi, &last.hi) == Ordering::Greater {
                    last.hi = iv.hi;
                }
                if last.next.is_some() || iv.next.is_some() {
                    exact = false;
                }
                last.next = None;
            }
        }
        Some(PartTree {
            part: a.part,
            list: out,
            exact,
        })
    }

    /// The key ranges of a tree, in key order, and what they bind.
    pub(crate) fn ranges(&self, tree: &PartTree) -> Option<RangeSet> {
        if tree.part != 0 {
            return None;
        }
        let mut set = RangeSet {
            ranges: Vec::new(),
            const_parts: usize::MAX,
            point_parts: usize::MAX,
            min_level: usize::MAX,
            cut: false,
        };
        let mut prefix = Vec::new();
        self.walk(tree, &mut prefix, &mut set, Some(0));
        if set.ranges.is_empty() {
            set.const_parts = 0;
            set.point_parts = 0;
            set.min_level = 0;
        }
        Some(set)
    }

    /// `consts` is how many leading parts hold one value over the whole
    /// tree so far; `None` once a level had more than one interval.
    fn walk(
        &self,
        tree: &PartTree,
        prefix: &mut Vec<Value>,
        set: &mut RangeSet,
        consts: Option<usize>,
    ) {
        let part = tree.part;
        let ty = self.ty(part);
        let desc = self.parts[part].desc;
        let single = tree.list.len() == 1;
        let order: Vec<&Interval> = if desc {
            tree.list.iter().rev().collect()
        } else {
            tree.list.iter().collect()
        };
        for iv in order {
            let point = iv.point(ty);
            if let (Some(v), Some(next)) = (point, iv.next.as_deref()) {
                if next.part == part + 1 && set.ranges.len() + next.list.len() <= MAX_RANGES {
                    prefix.push(v.clone());
                    let deeper = match consts {
                        Some(n) if single => Some(n + 1),
                        _ => None,
                    };
                    if deeper.is_none() {
                        set.const_parts = set.const_parts.min(consts.unwrap_or(usize::MAX));
                    }
                    self.walk(next, prefix, set, deeper);
                    prefix.pop();
                    continue;
                }
            }
            if iv.next.is_some() {
                // Deeper parts are known but can't narrow this range.
                set.cut = true;
            }
            let with = |v: &Value| {
                let mut k = prefix.clone();
                k.push(v.clone());
                k
            };
            let open = |incl: bool| {
                if prefix.is_empty() {
                    Bound::Unbounded
                } else if incl {
                    Bound::Included(prefix.clone())
                } else {
                    Bound::Excluded(prefix.clone())
                }
            };
            // In key order: ascending parts run from `lo` to `hi`,
            // descending parts from `hi` to `lo`.
            let (start, end) = if desc {
                (
                    match &iv.hi {
                        Edge::Max => open(true),
                        Edge::At(v, true) => Bound::Included(with(v)),
                        Edge::At(v, false) => Bound::Excluded(with(v)),
                        Edge::Min => open(false),
                    },
                    match &iv.lo {
                        Edge::Min => open(true),
                        Edge::At(v, false) => Bound::Included(with(v)),
                        Edge::At(v, true) => Bound::Excluded(with(v)),
                        Edge::Max => open(false),
                    },
                )
            } else {
                (
                    match &iv.lo {
                        Edge::Min => open(true),
                        Edge::At(v, false) => Bound::Included(with(v)),
                        Edge::At(v, true) => Bound::Excluded(with(v)),
                        Edge::Max => open(false),
                    },
                    match &iv.hi {
                        Edge::Max => open(true),
                        Edge::At(v, true) => Bound::Included(with(v)),
                        Edge::At(v, false) => Bound::Excluded(with(v)),
                        Edge::Min => open(false),
                    },
                )
            };
            let bound = prefix.len() + usize::from(point.is_some());
            if let Some(n) = consts {
                let here = n + usize::from(single && point.is_some());
                set.const_parts = set.const_parts.min(here);
            }
            set.point_parts = set.point_parts.min(bound);
            set.min_level = set.min_level.min(prefix.len());
            set.ranges.push(KeyRange { start, end });
        }
    }
}

/// The ranges a tree gives for its key.
#[derive(Debug, Clone)]
pub(crate) struct RangeSet {
    /// The ranges, in key order.
    pub(crate) ranges: Vec<KeyRange>,
    /// How many leading key parts hold one single value over all the
    /// ranges: reading the ranges in order gives the rows ordered by the
    /// parts after these.
    pub(crate) const_parts: usize,
    /// How many leading key parts hold a single value in every range (the
    /// value may differ from range to range).
    pub(crate) point_parts: usize,
    /// The shallowest key part a range ends on: every range holds one
    /// value of each part before it and an interval of it.
    pub(crate) min_level: usize,
    /// Whether some restriction on a deeper part was left out of a range.
    pub(crate) cut: bool,
}

impl RangeSet {
    /// Whether no row can match.
    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, IndexDef};
    use sprig_exec::{Session, compile};

    fn table() -> TableDef {
        TableDef {
            schema: "d".into(),
            name: "t".into(),
            columns: vec![
                ColumnDef::new("a", ColumnType::int(false)).not_null(),
                ColumnDef::new("b", ColumnType::int(false)),
                ColumnDef::new("c", ColumnType::int(false)),
            ],
            primary: None,
            indexes: vec![IndexDef::new(
                "k",
                false,
                vec![IndexPart::asc(0), IndexPart::asc(1), IndexPart::desc(2)],
            )],
        }
    }

    fn col(i: usize) -> Expr {
        Expr::column(i, ["a", "b", "c"][i], ColumnType::int(false), i > 0)
    }

    fn ranges_of(e: &Expr) -> Option<RangeSet> {
        let t = table();
        let session = Session::new(sprig_exec::Persona::MariaDb);
        let eval = |e: &Expr| {
            let c = compile(e, &session).ok()?;
            let mut ctx = sprig_exec::Ctx::new(&session);
            Some((c.eval(&mut ctx, &[]).ok()?, *c.ty()))
        };
        let ctx = KeyCtx {
            table: &t,
            parts: &t.indexes[0].parts,
            base: 0,
            eval: &eval,
            paths: &[],
        };
        let tree = ctx.tree(e)?;
        ctx.ranges(&tree)
    }

    fn i(v: i64) -> Value {
        Value::Int(v)
    }

    #[test]
    fn equalities_chain_into_one_point() {
        let e = Expr::binary(
            BinOp::And,
            Expr::binary(BinOp::Eq, col(1), Expr::int(7)),
            Expr::binary(BinOp::Eq, col(0), Expr::int(3)),
        );
        let set = ranges_of(&e).expect("a range");
        assert_eq!(set.ranges, vec![KeyRange::point(vec![i(3), i(7)])]);
        assert_eq!(set.const_parts, 2);
    }

    #[test]
    fn a_range_on_a_nullable_part_skips_nulls() {
        let e = Expr::binary(
            BinOp::And,
            Expr::binary(BinOp::Eq, col(0), Expr::int(3)),
            Expr::binary(BinOp::Lt, col(1), Expr::int(7)),
        );
        let set = ranges_of(&e).expect("a range");
        assert_eq!(
            set.ranges,
            vec![KeyRange {
                start: Bound::Excluded(vec![i(3), Value::Null]),
                end: Bound::Excluded(vec![i(3), i(7)]),
            }]
        );
        assert_eq!(set.const_parts, 1);
    }

    #[test]
    fn a_descending_part_runs_from_high_to_low() {
        let e = Expr::binary(
            BinOp::And,
            Expr::binary(
                BinOp::And,
                Expr::binary(BinOp::Eq, col(0), Expr::int(1)),
                Expr::binary(BinOp::Eq, col(1), Expr::int(2)),
            ),
            Expr::Between(
                Box::new(col(2)),
                Box::new(Expr::int(5)),
                Box::new(Expr::int(9)),
                false,
            ),
        );
        let set = ranges_of(&e).expect("a range");
        assert_eq!(
            set.ranges,
            vec![KeyRange {
                start: Bound::Included(vec![i(1), i(2), i(9)]),
                end: Bound::Included(vec![i(1), i(2), i(5)]),
            }]
        );
    }

    #[test]
    fn in_lists_are_sorted_and_deduplicated() {
        let e = Expr::In(
            Box::new(col(0)),
            vec![Expr::int(5), Expr::int(1), Expr::int(5), Expr::int(3)],
            false,
        );
        let set = ranges_of(&e).expect("a range");
        assert_eq!(
            set.ranges,
            vec![
                KeyRange::point(vec![i(1)]),
                KeyRange::point(vec![i(3)]),
                KeyRange::point(vec![i(5)]),
            ]
        );
        assert_eq!(set.const_parts, 0);
        assert_eq!(set.point_parts, 1);
    }

    #[test]
    fn or_of_overlapping_ranges_merges() {
        let e = Expr::binary(
            BinOp::Or,
            Expr::binary(BinOp::Lt, col(0), Expr::int(5)),
            Expr::binary(BinOp::Lt, col(0), Expr::int(9)),
        );
        let set = ranges_of(&e).expect("a range");
        assert_eq!(
            set.ranges,
            vec![KeyRange {
                start: Bound::Unbounded,
                end: Bound::Excluded(vec![i(9)]),
            }]
        );
    }

    #[test]
    fn or_over_different_parts_is_no_range() {
        let e = Expr::binary(
            BinOp::Or,
            Expr::binary(BinOp::Eq, col(0), Expr::int(5)),
            Expr::binary(BinOp::Eq, col(1), Expr::int(9)),
        );
        assert!(ranges_of(&e).is_none());
    }

    #[test]
    fn contradictions_are_impossible() {
        let e = Expr::binary(
            BinOp::And,
            Expr::binary(BinOp::Gt, col(0), Expr::int(5)),
            Expr::binary(BinOp::Lt, col(0), Expr::int(3)),
        );
        assert!(ranges_of(&e).expect("a tree").is_empty());
        let e = Expr::binary(BinOp::Eq, col(0), Expr::Decimal("2.5".into()));
        assert!(ranges_of(&e).expect("a tree").is_empty());
        let e = Expr::binary(BinOp::Lt, col(0), Expr::Decimal("2.5".into()));
        assert_eq!(
            ranges_of(&e).expect("a tree").ranges,
            vec![KeyRange {
                start: Bound::Unbounded,
                end: Bound::Included(vec![i(2)]),
            }]
        );
    }

    #[test]
    fn a_part_without_its_predecessor_is_no_range() {
        let e = Expr::binary(BinOp::Eq, col(1), Expr::int(5));
        assert!(ranges_of(&e).is_none());
    }
}
