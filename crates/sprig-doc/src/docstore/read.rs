//! Reads: an access plan from the filter (an `_id` lookup, an index
//! range, or the whole collection), then a scan that rechecks every
//! document with the matcher. Index bounds only narrow what is read: they
//! are always a superset of the matches.
//!
//! A scan is resumable by key position, so a cursor's later batches start
//! where the last one stopped, each at a fresh snapshot (MongoDB's
//! `local` read concern makes no promise across `getMore`s either).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use bson::{Bson, Document};
use sprig_engine::txn::Dir;
use sprig_proto_mongo::errors::{MongoError, code};

use super::catalog::{FieldKind, IndexMeta};
use super::layout::{self, after_prefix};
use super::{Coll, DocStore, DocTxn};
use crate::agg::{self, Projection, SortSpec};
use crate::filter::{FieldOp, InItem, Pred};
use crate::keyenc::{self, class};

/// One side of a bound on an indexed value, in ascending key terms.
#[derive(Debug, Clone, PartialEq, Eq)]
enum B {
    Incl(Vec<u8>),
    Excl(Vec<u8>),
    /// The first (as a lower bound) or last (upper) key of a class.
    Class(u8),
}

/// The allowed values of one field: `[lo, hi]`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldBound {
    lo: B,
    hi: B,
}

impl FieldBound {
    fn point(k: Vec<u8>) -> Self {
        Self {
            lo: B::Incl(k.clone()),
            hi: B::Incl(k),
        }
    }

    fn as_point(&self) -> Option<&[u8]> {
        match (&self.lo, &self.hi) {
            (B::Incl(a), B::Incl(b)) if a == b => Some(a),
            _ => None,
        }
    }

    /// Whether null or a missing field may be inside.
    fn has_null(&self) -> bool {
        let lo = enc_lo(&self.lo);
        let hi = enc_hi(&self.hi);
        lo.as_slice() <= [class::NULL].as_slice() && hi.as_slice() > [class::UNDEFINED].as_slice()
    }
}

fn enc_lo(b: &B) -> Vec<u8> {
    match b {
        B::Incl(k) => k.clone(),
        B::Excl(k) => after_prefix(k),
        B::Class(c) => vec![*c],
    }
}

fn enc_hi(b: &B) -> Vec<u8> {
    match b {
        B::Incl(k) => after_prefix(k),
        B::Excl(k) => k.clone(),
        B::Class(c) => vec![c.saturating_add(1)],
    }
}

fn comp(k: &[u8]) -> Vec<u8> {
    let mut c = k.to_vec();
    keyenc::complement(&mut c);
    c
}

/// The encoded `[lo, hi)` of a bound for a field of `kind`, as bytes after
/// the entries before it (`None`: unbounded on that side).
fn encode(fb: &FieldBound, kind: FieldKind) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    if kind != FieldKind::Desc {
        return (Some(enc_lo(&fb.lo)), Some(enc_hi(&fb.hi)));
    }
    let lo = match &fb.hi {
        B::Incl(k) => Some(comp(k)),
        B::Excl(k) => Some(after_prefix(&comp(k))),
        B::Class(c) => Some(vec![!*c]),
    };
    let hi = match &fb.lo {
        B::Incl(k) => Some(after_prefix(&comp(k))),
        B::Excl(k) => Some(comp(k)),
        B::Class(c) => Some(vec![(!*c).saturating_add(1)]),
    };
    (lo, hi)
}

/// The bound of one comparison, with MongoDB's type bracketing; `None`
/// when the operator can't bound the index.
fn op_bound(op: &FieldOp, hashed: bool) -> Option<Vec<FieldBound>> {
    let class_of = |v: &Bson| keyenc::class_of(v);
    let usable = |v: &Bson| {
        !matches!(
            v,
            Bson::Array(_) | Bson::MinKey | Bson::MaxKey | Bson::RegularExpression(_)
        )
    };
    let point = |v: &Bson| -> Option<FieldBound> {
        if !usable(v) {
            return None;
        }
        if hashed {
            return Some(FieldBound::point(keyenc::key(&super::keys::hashed(v))));
        }
        if matches!(v, Bson::Null | Bson::Undefined) {
            return Some(FieldBound {
                lo: B::Class(class::UNDEFINED),
                hi: B::Class(class::NULL),
            });
        }
        Some(FieldBound::point(keyenc::key(v)))
    };
    match op {
        FieldOp::Eq(v) => point(v).map(|b| vec![b]),
        FieldOp::In(items) => {
            let mut out = Vec::with_capacity(items.len());
            for i in items {
                match i {
                    InItem::Value(v) => out.push(point(v)?),
                    InItem::Regex(_) => return None,
                }
            }
            Some(out)
        }
        // A regex anchored on a literal prefix reads the strings that start
        // with it.
        FieldOp::Regex(re) if !hashed => {
            let p = re.literal_prefix()?;
            let mut lo = vec![class::STRING];
            for b in p.as_bytes() {
                lo.push(*b);
                if *b == 0 {
                    lo.push(0xFF);
                }
            }
            let hi = after_prefix(&lo);
            Some(vec![FieldBound {
                lo: B::Incl(lo),
                hi: B::Excl(hi),
            }])
        }
        FieldOp::Gt(v) | FieldOp::Gte(v) | FieldOp::Lt(v) | FieldOp::Lte(v)
            if !hashed && usable(v) && !matches!(v, Bson::Null | Bson::Undefined) =>
        {
            let k = keyenc::key(v);
            let c = class_of(v);
            Some(vec![match op {
                FieldOp::Gt(_) => FieldBound {
                    lo: B::Excl(k),
                    hi: B::Class(c),
                },
                FieldOp::Gte(_) => FieldBound {
                    lo: B::Incl(k),
                    hi: B::Class(c),
                },
                FieldOp::Lt(_) => FieldBound {
                    lo: B::Class(c),
                    hi: B::Excl(k),
                },
                _ => FieldBound {
                    lo: B::Class(c),
                    hi: B::Incl(k),
                },
            }])
        }
        _ => None,
    }
}

/// The intersection of two single bounds (non-multikey fields only).
fn intersect(a: &FieldBound, b: &FieldBound) -> FieldBound {
    let lo = if enc_lo(&a.lo) >= enc_lo(&b.lo) {
        a.lo.clone()
    } else {
        b.lo.clone()
    };
    let hi = if enc_hi(&a.hi) <= enc_hi(&b.hi) {
        a.hi.clone()
    } else {
        b.hi.clone()
    };
    FieldBound { lo, hi }
}

/// The top-level conjuncts of a filter.
fn conjuncts<'a>(p: &'a Pred, out: &mut Vec<&'a Pred>) {
    match p {
        Pred::And(k) => {
            for c in k {
                conjuncts(c, out);
            }
        }
        other => out.push(other),
    }
}

/// The bounds a filter puts on `path` (a disjunction), `None` for none.
fn path_bounds(
    conj: &[&Pred],
    path: &str,
    hashed: bool,
    multikey: bool,
) -> Option<Vec<FieldBound>> {
    let mut found: Option<Vec<FieldBound>> = None;
    for c in conj {
        let Pred::Field { path: p, op } = c else {
            continue;
        };
        if p != path {
            continue;
        }
        let Some(bs) = op_bound(op, hashed) else {
            continue;
        };
        found = Some(match found {
            None => bs,
            // A multikey field's conditions may be met by different
            // elements: keep the first condition only.
            Some(prev) if multikey => prev,
            Some(prev) => match (prev.as_slice(), bs.as_slice()) {
                ([a], [b]) => vec![intersect(a, b)],
                // One side is an `$in`: keep the narrower list.
                _ if bs.len() < prev.len() => bs,
                _ => prev,
            },
        });
    }
    found
}

/// Whether the filter's conditions on `path` ask for null (or a missing
/// field): a hashed bound hides it in the hash.
fn asks_null(conj: &[&Pred], path: &str) -> bool {
    let is_null = |v: &Bson| matches!(v, Bson::Null | Bson::Undefined);
    conj.iter().any(|c| match c {
        Pred::Field { path: p, op } if p == path => match op {
            FieldOp::Eq(v) | FieldOp::Gte(v) | FieldOp::Lte(v) => is_null(v),
            FieldOp::In(items) => items
                .iter()
                .any(|i| matches!(i, InItem::Value(v) if is_null(v))),
            _ => false,
        },
        _ => false,
    })
}

/// How a plan reads the collection.
#[derive(Debug, Clone)]
pub enum Access {
    /// Documents by `_id` key.
    Ids(VecDeque<Vec<u8>>),
    /// Several accesses read in turn, each document once (`$or`).
    Union(Vec<Access>),
    /// Key ranges of the primary keyspace (`index: None`) or of an index.
    Ranges {
        /// The index read: its number and whether its entries are unique
        /// entries (whose value is the `_id` key); `None`: the documents.
        index: Option<(u16, bool)>,
        /// `[lo, hi)` full keys, in ascending order.
        intervals: Vec<(Vec<u8>, Vec<u8>)>,
        /// Scan direction.
        dir: Dir,
        /// One document may appear under several entries.
        dedup: bool,
    },
}

/// A chosen plan.
#[derive(Debug, Clone)]
pub struct Plan {
    /// How to read.
    pub access: Access,
    /// The scan returns documents in the requested sort order.
    pub sorted: bool,
    /// The index used (`None`: `_id` lookups or a collection scan).
    pub index: Option<String>,
    /// The index bounds came from the filter (not just the order).
    pub bounded: bool,
}

impl Plan {
    fn scan_all(c: &Coll, dir: Dir) -> Self {
        let p = c.space.docs();
        let e = after_prefix(&p);
        Self {
            access: Access::Ranges {
                index: None,
                intervals: vec![(p, e)],
                dir,
                dedup: false,
            },
            sorted: false,
            index: None,
            bounded: false,
        }
    }

    /// `explain`'s winning plan stage name.
    #[must_use]
    pub fn stage(&self) -> &'static str {
        match &self.access {
            Access::Ids(_) => "IDHACK",
            Access::Union(_) => "OR",
            Access::Ranges { index: None, .. } => "COLLSCAN",
            Access::Ranges { .. } => "IXSCAN",
        }
    }
}

/// The intervals of `idx` for the filter's conjuncts, the number of
/// leading point fields, whether one prefix only (so the order follows
/// the later fields), and whether any bound came from the filter.
#[allow(clippy::type_complexity)]
fn index_intervals(
    c: &Coll,
    pos: usize,
    idx: &IndexMeta,
    conj: &[&Pred],
) -> Option<(Vec<(Vec<u8>, Vec<u8>)>, usize, bool, bool, bool)> {
    let multikey = c.is_multikey(pos);
    let base = c.space.index(idx.id, idx.unique);
    let mut prefixes = vec![base.clone()];
    let mut points = 0usize;
    let mut bounded = false;
    let mut nulls = false;
    if idx.is_wildcard() {
        // A wildcard index serves one path's condition.
        for cj in conj {
            let Pred::Field { path, op } = cj else {
                continue;
            };
            // Entries drop array positions: `a.0` can't be looked up.
            if path
                .split('.')
                .any(|c| c.bytes().all(|b| b.is_ascii_digit()))
            {
                continue;
            }
            // Only leaves have entries: a condition on a document value
            // can't be looked up.
            let doc_arg = match op {
                FieldOp::Eq(v)
                | FieldOp::Gt(v)
                | FieldOp::Gte(v)
                | FieldOp::Lt(v)
                | FieldOp::Lte(v) => matches!(v, Bson::Document(_)),
                FieldOp::In(items) => items
                    .iter()
                    .any(|i| matches!(i, InItem::Value(Bson::Document(_)))),
                _ => false,
            };
            if doc_arg {
                continue;
            }
            let Some(bs) = op_bound(op, false) else {
                continue;
            };
            let keeps = {
                let probe = bson::doc! {path.clone(): 1};
                let (e, _) = super::keys::entries(&probe, idx).ok()?;
                !e.is_empty()
            };
            if !keeps {
                continue;
            }
            let mut p = base.clone();
            p.extend_from_slice(&super::keys::wildcard_path(path));
            let mut iv = Vec::new();
            for fb in &bs {
                nulls |= fb.has_null();
                let (lo, hi) = encode(fb, FieldKind::Asc);
                let mut l = p.clone();
                let mut h = p.clone();
                if let Some(x) = lo {
                    l.extend_from_slice(&x);
                }
                match hi {
                    Some(x) => h.extend_from_slice(&x),
                    None => h = after_prefix(&p),
                }
                iv.push((l, h));
            }
            if nulls {
                // Missing paths have no wildcard entries.
                return None;
            }
            return Some((merge(iv), 0, false, true, false));
        }
        return None;
    }
    let mut out = Vec::new();
    for (i, (path, kind)) in idx.fields.iter().enumerate() {
        let hashed = *kind == FieldKind::Hashed;
        let bs = path_bounds(conj, path, hashed, multikey);
        let Some(bs) = bs else {
            out = prefixes
                .iter()
                .map(|p| (p.clone(), after_prefix(p)))
                .collect();
            break;
        };
        bounded = true;
        nulls |= bs.iter().any(FieldBound::has_null) || (hashed && asks_null(conj, path));
        if bs.iter().all(|b| b.as_point().is_some()) && prefixes.len() * bs.len() <= 256 {
            let mut next = Vec::with_capacity(prefixes.len() * bs.len());
            for p in &prefixes {
                for b in &bs {
                    let mut n = p.clone();
                    let k = b.as_point().unwrap_or_default();
                    if *kind == FieldKind::Desc {
                        n.extend_from_slice(&comp(k));
                    } else {
                        n.extend_from_slice(k);
                    }
                    next.push(n);
                }
            }
            prefixes = next;
            points = i + 1;
            if i + 1 == idx.fields.len() {
                out = prefixes
                    .iter()
                    .map(|p| (p.clone(), after_prefix(p)))
                    .collect();
            }
            continue;
        }
        for p in &prefixes {
            for b in &bs {
                let (lo, hi) = encode(b, *kind);
                let mut l = p.clone();
                if let Some(x) = lo {
                    l.extend_from_slice(&x);
                }
                let h = match hi {
                    Some(x) => {
                        let mut h = p.clone();
                        h.extend_from_slice(&x);
                        h
                    }
                    None => after_prefix(p),
                };
                if l < h {
                    out.push((l, h));
                }
            }
        }
        break;
    }
    let single = prefixes.len() == 1;
    Some((merge(out), points, single, bounded, nulls))
}

/// Sorted, overlapping intervals merged.
fn merge(mut iv: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    iv.sort();
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(iv.len());
    for (l, h) in iv {
        if let Some(last) = out.last_mut()
            && l <= last.1
        {
            if h > last.1 {
                last.1 = h;
            }
            continue;
        }
        out.push((l, h));
    }
    out
}

/// Whether an index scan from field `from` on gives `sort`'s order, and in
/// which direction.
fn sort_dir(idx: &IndexMeta, from: usize, sort: &SortSpec) -> Option<Dir> {
    if sort.is_empty() || idx.is_hashed() || idx.is_wildcard() {
        return None;
    }
    let rest = idx.fields.get(from..)?;
    if sort.len() > rest.len() {
        return None;
    }
    let mut dir = None;
    for ((sp, sd), (ip, ik)) in sort.iter().zip(rest) {
        if sp != ip {
            return None;
        }
        let same = (*sd > 0) == (*ik == FieldKind::Asc);
        let d = if same { Dir::Forward } else { Dir::Reverse };
        match dir {
            None => dir = Some(d),
            Some(x) if x == d => {}
            Some(_) => return None,
        }
    }
    dir
}

/// What a read asks the planner for.
#[derive(Debug, Default, Clone)]
pub struct Want<'a> {
    /// The sort (empty: any order).
    pub sort: SortSpec,
    /// `hint`.
    pub hint: Option<&'a Bson>,
    /// `{$natural: ±1}`.
    pub natural: Option<i32>,
}

/// Plans a read of `c` for `pred`.
///
/// # Errors
/// 2 for a hint that names no index.
pub fn plan(c: &Coll, pred: &Pred, want: &Want<'_>) -> Result<Plan, MongoError> {
    let collated = crate::collation::active().is_some();
    if let Some(n) = want.natural {
        return Ok(Plan::scan_all(
            c,
            if n < 0 { Dir::Reverse } else { Dir::Forward },
        ));
    }
    let id_sort = match want.sort.as_slice() {
        [] => Some(Dir::Forward),
        [(p, d)] if p == "_id" => Some(if *d < 0 { Dir::Reverse } else { Dir::Forward }),
        _ => None,
    };
    let mut conj = Vec::new();
    conjuncts(pred, &mut conj);
    // A hint.
    if let Some(h) = want.hint {
        let pos = match h {
            Bson::String(n) if n == "_id_" => None,
            Bson::Document(k) if k == &bson::doc! {"_id": 1} || k == &bson::doc! {"_id": -1} => {
                None
            }
            Bson::Document(k) if k.get("$natural").is_some() => {
                let d = k
                    .get("$natural")
                    .and_then(crate::filter::as_i64)
                    .unwrap_or(1);
                return Ok(Plan::scan_all(
                    c,
                    if d < 0 { Dir::Reverse } else { Dir::Forward },
                ));
            }
            Bson::String(n) => Some(
                c.meta
                    .indexes
                    .iter()
                    .position(|i| &i.name == n)
                    .ok_or_else(bad_hint)?,
            ),
            Bson::Document(k) => Some(
                c.meta
                    .indexes
                    .iter()
                    .position(|i| &i.key() == k)
                    .ok_or_else(bad_hint)?,
            ),
            _ => return Err(bad_hint()),
        };
        let Some(pos) = pos else {
            let mut p = Plan::scan_all(c, id_sort.unwrap_or(Dir::Forward));
            p.sorted = id_sort.is_some();
            return Ok(p);
        };
        let idx = &c.meta.indexes[pos];
        let (iv, points, single, bounded, _) = if collated {
            None
        } else {
            index_intervals(c, pos, idx, &conj)
        }
        .unwrap_or_else(|| {
            let p = c.space.index(idx.id, idx.unique);
            (vec![(p.clone(), after_prefix(&p))], 0, true, false, false)
        });
        let sd = (single && !c.is_multikey(pos))
            .then(|| sort_dir(idx, points, &want.sort))
            .flatten();
        return Ok(Plan {
            access: Access::Ranges {
                index: Some((idx.id, idx.unique)),
                intervals: iv,
                dir: sd.unwrap_or(Dir::Forward),
                dedup: true,
            },
            sorted: sd.is_some(),
            index: Some(idx.name.clone()),
            bounded,
        });
    }
    // `_id` lookups.
    if let Some(ids) = pred.id_values()
        && !(collated && ids.iter().any(|v| matches!(v, Bson::String(_))))
    {
        let mut keys: Vec<Vec<u8>> = ids
            .iter()
            .filter_map(|v| crate::json::id_key(v).ok())
            .collect();
        keys.sort();
        keys.dedup();
        let sorted = id_sort.is_some();
        if id_sort == Some(Dir::Reverse) {
            keys.reverse();
        }
        return Ok(Plan {
            access: Access::Ids(keys.into()),
            sorted,
            index: None,
            bounded: true,
        });
    }
    // The best bounded index: most leading points, then a range; a
    // unique index pinned on every field first.
    let mut best: Option<(i64, Plan)> = None;
    if !collated {
        for (pos, idx) in c.meta.indexes.iter().enumerate() {
            if idx.hidden || idx.partial.is_some() {
                continue;
            }
            let Some((iv, points, single, bounded, nulls)) = index_intervals(c, pos, idx, &conj)
            else {
                continue;
            };
            if !bounded || (idx.sparse && nulls) {
                continue;
            }
            let multikey = c.is_multikey(pos);
            let sd = (single && !multikey)
                .then(|| sort_dir(idx, points, &want.sort))
                .flatten();
            let full = points == idx.fields.len();
            let mut score = i64::try_from(points).unwrap_or(0) * 10 + 5;
            if full && idx.unique && !nulls && !multikey {
                score += 100;
            }
            if sd.is_some() {
                score += 3;
            }
            if multikey {
                score -= 1;
            }
            if best.as_ref().is_none_or(|(s, _)| score > *s) {
                best = Some((
                    score,
                    Plan {
                        access: Access::Ranges {
                            index: Some((idx.id, idx.unique)),
                            intervals: iv,
                            dir: sd.unwrap_or(Dir::Forward),
                            dedup: true,
                        },
                        sorted: sd.is_some() || want.sort.is_empty(),
                        index: Some(idx.name.clone()),
                        bounded: true,
                    },
                ));
            }
        }
    }
    if let Some((_, p)) = best {
        return Ok(p);
    }
    // An `$or` whose every branch has a bounded plan: their union.
    if want.sort.is_empty() && !collated {
        for cj in &conj {
            let Pred::Or(branches) = cj else { continue };
            let mut parts = Vec::with_capacity(branches.len());
            for br in branches {
                let p = plan(c, br, &Want::default())?;
                if !p.bounded {
                    parts.clear();
                    break;
                }
                parts.push(p.access);
            }
            if !parts.is_empty() {
                return Ok(Plan {
                    access: Access::Union(parts),
                    sorted: true,
                    index: None,
                    bounded: true,
                });
            }
        }
    }
    if let Some(d) = id_sort {
        let mut p = Plan::scan_all(c, d);
        p.sorted = true;
        return Ok(p);
    }
    // An index whose order is the sort's, read whole.
    if !collated {
        for (pos, idx) in c.meta.indexes.iter().enumerate() {
            if idx.hidden || idx.partial.is_some() || idx.sparse || c.is_multikey(pos) {
                continue;
            }
            if let Some(d) = sort_dir(idx, 0, &want.sort) {
                let p = c.space.index(idx.id, idx.unique);
                return Ok(Plan {
                    access: Access::Ranges {
                        index: Some((idx.id, idx.unique)),
                        intervals: vec![(p.clone(), after_prefix(&p))],
                        dir: d,
                        dedup: true,
                    },
                    sorted: true,
                    index: Some(idx.name.clone()),
                    bounded: false,
                });
            }
        }
    }
    Ok(Plan::scan_all(c, Dir::Forward))
}

fn bad_hint() -> MongoError {
    MongoError::new(
        code::BAD_VALUE,
        "error processing query: planner returned error :: caused by :: hint provided does not correspond to an existing index",
    )
}

/// Decodes a stored document.
///
/// # Errors
/// The value is damaged.
pub fn decode(v: &[u8]) -> Result<Document, MongoError> {
    Document::from_reader(&mut layout::doc_bson(v))
        .map_err(|e| MongoError::new(code::INTERNAL_ERROR, format!("damaged document: {e}")))
}

/// Where a scan stands.
#[derive(Debug, Clone)]
pub struct Scan {
    access: Access,
    /// The interval being read, by position in `intervals` (in scan order).
    at: usize,
    /// The next key to read from in the current interval (forward: its
    /// start; reverse: its end, exclusive).
    resume: Option<Vec<u8>>,
    /// `_id` keys already returned (multikey dedup).
    seen: HashSet<Vec<u8>>,
    done: bool,
    /// A union's parts, read in turn.
    parts: Vec<Scan>,
}

impl Scan {
    /// A scan of `plan`.
    #[must_use]
    pub fn new(plan: &Plan) -> Self {
        Self::of(plan.access.clone())
    }

    fn of(access: Access) -> Self {
        let parts = match &access {
            Access::Union(v) => v.iter().cloned().map(Self::of).collect(),
            _ => Vec::new(),
        };
        Self {
            access,
            at: 0,
            resume: None,
            seen: HashSet::new(),
            done: false,
            parts,
        }
    }

    /// The index the scan reads (`None`: documents).
    #[must_use]
    pub fn index_id(&self) -> Option<u16> {
        match &self.access {
            Access::Ranges {
                index: Some((id, _)),
                ..
            } => Some(*id),
            _ => None,
        }
    }

    /// Whether the scan has read everything.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Reads candidates in order until `want` of them match `pred` (or the
    /// keyspace ends), calling `take` with each match's `_id` key and
    /// document. `take` returns false to stop early.
    ///
    /// # Errors
    /// The store failed.
    pub fn run(
        &mut self,
        t: &mut DocTxn,
        c: &Coll,
        pred: &Pred,
        cx: &dyn crate::filter::ExprEval,
        mut take: impl FnMut(Vec<u8>, Document) -> bool,
    ) -> Result<(), MongoError> {
        self.run_dyn(t, c, pred, cx, &mut take)
    }

    fn run_dyn(
        &mut self,
        t: &mut DocTxn,
        c: &Coll,
        pred: &Pred,
        cx: &dyn crate::filter::ExprEval,
        take: &mut dyn FnMut(Vec<u8>, Document) -> bool,
    ) -> Result<(), MongoError> {
        if self.done {
            return Ok(());
        }
        if !self.parts.is_empty() {
            let seen = &mut self.seen;
            while self.at < self.parts.len() {
                let mut stop = false;
                self.parts[self.at].run_dyn(t, c, pred, cx, &mut |k, d| {
                    if !seen.insert(k.clone()) {
                        return true;
                    }
                    if take(k, d) {
                        true
                    } else {
                        stop = true;
                        false
                    }
                })?;
                if stop {
                    return Ok(());
                }
                self.at += 1;
            }
            self.done = true;
            return Ok(());
        }
        match &mut self.access {
            // Its parts were read above.
            Access::Union(_) => {
                self.done = true;
                Ok(())
            }
            Access::Ids(keys) => {
                while let Some(k) = keys.pop_front() {
                    let Some(v) = t.get(&c.space.doc(&k))? else {
                        continue;
                    };
                    let d = decode(&v)?;
                    if pred.matches_with(&d, cx) && !take(k, d) {
                        return Ok(());
                    }
                }
                self.done = true;
                Ok(())
            }
            Access::Ranges {
                index,
                intervals,
                dir,
                dedup,
            } => {
                let n = intervals.len();
                let docs = c.space.docs();
                while self.at < n {
                    let i = if *dir == Dir::Forward {
                        self.at
                    } else {
                        n - 1 - self.at
                    };
                    let (lo, hi) = &intervals[i];
                    let (from, to) = match (&self.resume, *dir) {
                        (Some(r), Dir::Forward) => (r.clone(), hi.clone()),
                        (Some(r), Dir::Reverse) => (lo.clone(), r.clone()),
                        (None, _) => (lo.clone(), hi.clone()),
                    };
                    let rows = t.scan(&from, &to, *dir, 256)?;
                    let full = rows.len() == 256;
                    let mut stop = false;
                    let mut last = None;
                    for (k, v) in rows {
                        last = Some(k.clone());
                        let (idk, d) = if index.is_none() {
                            (k[docs.len()..].to_vec(), decode(&v)?)
                        } else {
                            {
                                let idk = v.to_vec();
                                if *dedup && !self.seen.insert(idk.clone()) {
                                    continue;
                                }
                                let Some(dv) = t.get(&c.space.doc(&idk))? else {
                                    continue;
                                };
                                (idk, decode(&dv)?)
                            }
                        };
                        if pred.matches_with(&d, cx) && !take(idk, d) {
                            stop = true;
                            break;
                        }
                    }
                    if let Some(k) = last {
                        // The engine compares bounds with its versioned
                        // keys (`key | version`): the next start is past
                        // every version of `k`, which no other key extends
                        // (key encodings are prefix-free).
                        self.resume = Some(match dir {
                            Dir::Forward => after_prefix(&k),
                            Dir::Reverse => k,
                        });
                    }
                    if stop {
                        return Ok(());
                    }
                    if !full {
                        self.at += 1;
                        self.resume = None;
                    }
                }
                self.done = true;
                Ok(())
            }
        }
    }
}

/// A find's resumable state, held by a cursor between batches.
#[derive(Debug)]
pub struct StoreCursor {
    /// The tenant.
    pub tenant: u64,
    coll: Arc<Coll>,
    source: Source,
    pred: Pred,
    projection: Option<Projection>,
    skip: u64,
    remaining: Option<u64>,
    collation: Option<crate::collation::Collation>,
}

#[derive(Debug)]
enum Source {
    Scan(Scan),
    Buffer(VecDeque<Document>),
}

impl StoreCursor {
    /// A cursor over ready documents (already filtered and projected).
    #[must_use]
    pub fn buffer(tenant: u64, coll: Arc<Coll>, docs: Vec<Document>) -> Self {
        Self {
            tenant,
            coll,
            source: Source::Buffer(docs.into()),
            pred: Pred::True,
            projection: None,
            skip: 0,
            remaining: None,
            collation: None,
        }
    }

    /// Whether nothing is left.
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.remaining == Some(0)
            || match &self.source {
                Source::Scan(s) => s.is_done(),
                Source::Buffer(b) => b.is_empty(),
            }
    }

    /// The collection read.
    #[must_use]
    pub fn coll(&self) -> &Arc<Coll> {
        &self.coll
    }

    /// The next batch: up to `n` documents (and at most 16 MiB of them).
    ///
    /// # Errors
    /// The store failed; projection errors.
    pub async fn next_batch(
        &mut self,
        store: &DocStore,
        n: usize,
    ) -> Result<Vec<Document>, MongoError> {
        let n = match self.remaining {
            Some(r) => n.min(usize::try_from(r).unwrap_or(usize::MAX)),
            None => n,
        };
        let mut out = Vec::new();
        if n == 0 {
            return Ok(out);
        }
        match &mut self.source {
            Source::Buffer(b) => {
                let mut bytes = 0usize;
                while out.len() < n {
                    let Some(d) = b.pop_front() else { break };
                    bytes += crate::spill::doc_bytes(&d);
                    out.push(d);
                    if bytes > 16 * 1024 * 1024 {
                        break;
                    }
                }
            }
            Source::Scan(s) => {
                // A later batch after the collection or the index went
                // away: MongoDB's QueryPlanKilled, not a short answer.
                let name = super::Name::new(&self.coll.meta.db, &self.coll.meta.name);
                let now_coll = store.coll(self.tenant, &name)?;
                let gone = match &now_coll {
                    None => true,
                    Some(c) => {
                        c.meta.n != self.coll.meta.n
                            || s.index_id()
                                .is_some_and(|i| !c.meta.indexes.iter().any(|x| x.id == i))
                    }
                };
                if gone {
                    return Err(MongoError::new(
                        175,
                        format!(
                            "PlanExecutor error during find command :: caused by :: collection or index of {name} dropped"
                        ),
                    ));
                }
                let mut t = store.begin(false);
                let now = store.now();
                let cx = crate::agg::AggExpr { now };
                let coll = Arc::clone(&self.coll);
                let pred = &self.pred;
                let skip = &mut self.skip;
                let mut bytes = 0usize;
                let r = crate::collation::scope(self.collation, async {
                    s.run(&mut t, &coll, pred, &cx, |_, d| {
                        if *skip > 0 {
                            *skip -= 1;
                            return true;
                        }
                        bytes += crate::spill::doc_bytes(&d);
                        out.push(d);
                        out.len() < n && bytes <= 16 * 1024 * 1024
                    })
                })
                .await;
                t.finish_read().await?;
                r?;
                if let Some(p) = &self.projection {
                    out = out
                        .iter()
                        .map(|d| p.apply(d, now))
                        .collect::<Result<_, _>>()?;
                }
            }
        }
        if let Some(r) = &mut self.remaining {
            *r = r.saturating_sub(out.len() as u64);
        }
        Ok(out)
    }
}

/// What a find asks for.
#[derive(Debug, Default, Clone)]
pub struct FindArgs {
    /// The filter.
    pub filter: Document,
    /// The sort.
    pub sort: Option<Document>,
    /// The projection.
    pub projection: Option<Document>,
    /// Documents to skip.
    pub skip: u64,
    /// At most this many (0: no limit).
    pub limit: u64,
    /// `hint`.
    pub hint: Option<Bson>,
    /// `allowDiskUse` (a blocking sort may then exceed the memory limit).
    pub allow_disk_use: bool,
}

/// The natural-order direction a sort asks for.
fn natural_of(sort: Option<&Document>) -> Option<i32> {
    let s = sort?;
    if s.len() == 1
        && let Some(v) = s.get("$natural")
    {
        return Some(if crate::filter::as_i64(v).unwrap_or(1) < 0 {
            -1
        } else {
            1
        });
    }
    None
}

impl DocStore {
    /// Opens a find on `c`: a cursor positioned at the start. Inside a
    /// multi-document transaction (`txn`), the whole result is read at
    /// once, at the transaction's snapshot.
    ///
    /// # Errors
    /// Parse errors; 2 for a bad hint; 292 for a blocking sort past the
    /// memory limit.
    pub async fn find(
        &self,
        tenant: u64,
        c: &Arc<Coll>,
        args: &FindArgs,
        txn: Option<&mut DocTxn>,
    ) -> Result<StoreCursor, MongoError> {
        let pred = crate::filter::parse(&args.filter)?;
        crate::engine::models::check_near(&pred, false)?;
        let projection = args
            .projection
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(Projection::parse)
            .transpose()?
            .map(|p| p.with_query(&pred));
        let natural = natural_of(args.sort.as_ref());
        let sort = if natural.is_some() {
            Vec::new()
        } else {
            args.sort
                .as_ref()
                .map(agg::parse_sort)
                .transpose()?
                .unwrap_or_default()
        };
        let want = Want {
            sort: sort.clone(),
            hint: args.hint.as_ref(),
            natural,
        };
        let plan = plan(c, &pred, &want)?;
        let remaining = (args.limit > 0).then_some(args.limit);
        let collation = crate::collation::active();
        if plan.sorted && txn.is_none() {
            return Ok(StoreCursor {
                tenant,
                coll: Arc::clone(c),
                source: Source::Scan(Scan::new(&plan)),
                pred,
                projection,
                skip: args.skip,
                remaining,
                collation,
            });
        }
        // A blocking sort, or a transaction's read: everything now.
        let docs = self
            .collect(c, &pred, &plan, txn, Some(self.max_sort_bytes))
            .await
            .map_err(|e| {
                if e.code == code::QUERY_EXCEEDED_MEMORY_LIMIT && !args.allow_disk_use {
                    MongoError::new(
                        code::QUERY_EXCEEDED_MEMORY_LIMIT,
                        "Sort exceeded memory limit of 104857600 bytes, but did not opt in to external sorting.",
                    )
                } else {
                    e
                }
            })?;
        let mut docs = docs;
        if !plan.sorted {
            agg::sort_docs(&mut docs, &sort);
        }
        let skip = usize::try_from(args.skip).unwrap_or(usize::MAX);
        let take = remaining.map_or(usize::MAX, |r| usize::try_from(r).unwrap_or(usize::MAX));
        let now = self.now();
        let docs: Vec<Document> = docs.into_iter().skip(skip).take(take).collect();
        let docs = match &projection {
            Some(p) => docs
                .iter()
                .map(|d| p.apply(d, now))
                .collect::<Result<_, _>>()?,
            None => docs,
        };
        Ok(StoreCursor::buffer(tenant, Arc::clone(c), docs))
    }

    /// Every document of `c` matching `pred` read through `plan`, in the
    /// plan's order; in `txn` when given, else at a fresh snapshot.
    /// `max_bytes`: fail with 292 past that many bytes.
    ///
    /// # Errors
    /// The store failed; 292.
    pub async fn collect(
        &self,
        c: &Coll,
        pred: &Pred,
        plan: &Plan,
        txn: Option<&mut DocTxn>,
        max_bytes: Option<usize>,
    ) -> Result<Vec<Document>, MongoError> {
        let cx = crate::agg::AggExpr { now: self.now() };
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut over = false;
        let mut scan = Scan::new(plan);
        let mut take = |_: Vec<u8>, d: Document| {
            if let Some(m) = max_bytes {
                bytes += crate::spill::doc_bytes(&d);
                if bytes > m {
                    over = true;
                    return false;
                }
            }
            out.push(d);
            true
        };
        match txn {
            Some(t) => scan.run(t, c, pred, &cx, &mut take)?,
            None => {
                let mut t = self.begin(false);
                let r = scan.run(&mut t, c, pred, &cx, &mut take);
                t.finish_read().await?;
                r?;
            }
        }
        if over {
            return Err(MongoError::new(
                code::QUERY_EXCEEDED_MEMORY_LIMIT,
                "the read exceeded its memory limit",
            ));
        }
        Ok(out)
    }

    /// The `_id` keys and documents of `c` matching `pred`, in the plan's
    /// order for `sort` (sorted here when the plan doesn't give it), at
    /// most `limit` of them (0: all). Writes use it to pick their targets.
    ///
    /// # Errors
    /// As for [`Self::collect`].
    pub fn targets(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        pred: &Pred,
        sort: &SortSpec,
        hint: Option<&Bson>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Document)>, MongoError> {
        let want = Want {
            sort: sort.clone(),
            hint,
            natural: None,
        };
        let plan = plan(c, pred, &want)?;
        let cx = crate::agg::AggExpr { now: self.now() };
        let mut out: Vec<(Vec<u8>, Document)> = Vec::new();
        let early = plan.sorted && limit > 0;
        let mut scan = Scan::new(&plan);
        scan.run(t, c, pred, &cx, |k, d| {
            out.push((k, d));
            !(early && out.len() >= limit)
        })?;
        if !plan.sorted && !sort.is_empty() {
            out.sort_by(|a, b| agg::cmp_docs(&a.1, &b.1, sort));
        }
        if limit > 0 {
            out.truncate(limit);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_encode_both_directions() {
        let five = keyenc::key(&Bson::Int32(5));
        let fb = FieldBound {
            lo: B::Excl(five.clone()),
            hi: B::Class(class::NUMBER),
        };
        let (lo, hi) = encode(&fb, FieldKind::Asc);
        let six = keyenc::key(&Bson::Int32(6));
        assert!(lo.clone().unwrap() <= six && six < hi.clone().unwrap());
        assert!(lo.unwrap() > five);
        let (dlo, dhi) = encode(&fb, FieldKind::Desc);
        let c6 = comp(&six);
        let c5 = comp(&five);
        assert!(dlo.clone().unwrap() <= c6 && c6 < dhi.clone().unwrap());
        assert!(c5 >= dhi.unwrap() || c5 < dlo.unwrap());
        assert!(
            FieldBound {
                lo: B::Class(class::UNDEFINED),
                hi: B::Class(class::NULL)
            }
            .has_null()
        );
        assert!(!FieldBound::point(six).has_null());
    }
}
