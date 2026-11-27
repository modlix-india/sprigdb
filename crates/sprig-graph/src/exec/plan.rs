//! The cost-based choices of a `MATCH`, from the store's statistics
//! ([`GraphStore::est_nodes`], [`GraphStore::est_rels`],
//! [`GraphStore::est_prop`]):
//!
//! - **Where a path pattern starts:** every node of a pattern of single
//!   hops is a candidate (the first, the last, or one in the middle, which
//!   then runs as two halves from it). Each is costed as the rows its
//!   start read finds, plus the relationships every hop reads (rows times
//!   the average degree of the near label over the hop's types towards the
//!   far label), the far nodes' conditions cutting the rows after each.
//! - **The order of a `MATCH`'s comma-separated patterns:** greedily, the
//!   cheapest next one given the variables bound so far.
//!
//! A store without statistics (in memory, mapped graphs) answers `None` to
//! every estimate, and the executor keeps its fixed rules (start at the
//! end with the better [`Exec::score`], patterns in written order), as it
//! does when `SPRIGDB_GRAPH_PLANNER=off`.
//!
//! [`GraphStore::est_nodes`]: crate::store::GraphStore::est_nodes
//! [`GraphStore::est_rels`]: crate::store::GraphStore::est_rels
//! [`GraphStore::est_prop`]: crate::store::GraphStore::est_prop

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};

use sprig_common::Result;

use super::Exec;
use crate::ast::{BinOp, Dir, Expr, NodePat, PathKind, PathPattern, RelPat};
use crate::eval::{Row, eval};
use crate::ordkey::{Bound, RangePred};
use crate::store::PropPred;
use crate::value::Value;

/// 0: not read yet; 1: on; 2: off.
static PLANNER: AtomicU8 = AtomicU8::new(0);

/// Whether the cost-based choices are made (`SPRIGDB_GRAPH_PLANNER`, on
/// unless `off`).
pub fn planner_on() -> bool {
    match PLANNER.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let on = std::env::var("SPRIGDB_GRAPH_PLANNER")
                .map_or(true, |v| !v.eq_ignore_ascii_case("off"));
            PLANNER.store(if on { 1 } else { 2 }, Ordering::Relaxed);
            on
        }
    }
}

/// Turns the cost-based choices on or off for this process (benchmarks
/// and tests compare both).
pub fn set_planner(on: bool) {
    PLANNER.store(if on { 1 } else { 2 }, Ordering::Relaxed);
}

/// Where a path pattern starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    /// Its first node, as written.
    First,
    /// Its last node (the pattern read right to left).
    Last,
    /// Node `i` (1-based step index), the pattern run as two halves.
    Middle(usize),
}

/// What the conditions of one node pattern let a start read find.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NodeEst {
    /// Nodes the read finds (rows when bound).
    pub(crate) rows: f64,
    /// Elements the read goes through to find them.
    pub(crate) cost: f64,
    /// The share of the label's nodes passing the conditions (1: none).
    pub(crate) sel: f64,
    /// The label used.
    pub(crate) label: Option<u32>,
    /// Bound by the incoming rows.
    pub(crate) bound: bool,
}

/// `v.k <op> const` as (`k`, the condition), for conditions a range read
/// can serve: `<`, `<=`, `>`, `>=` (either way round) and `STARTS WITH`.
pub(crate) fn range_of(
    c: &Expr,
    v: &str,
    params: &crate::eval::Params,
) -> Option<(String, RangePred)> {
    let Expr::Binary(op, a, b) = c else {
        return None;
    };
    let prop = |e: &Expr| match e {
        Expr::Prop(t, k) if matches!(&**t, Expr::Var(x) if x == v) => Some(k.clone()),
        _ => None,
    };
    let konst = |e: &Expr| -> Option<Value> {
        if !super::matcher::is_const_expr(e) {
            return None;
        }
        match eval(e, &Row::default(), params).ok()? {
            Value::Null => None,
            x => Some(x),
        }
    };
    let (key, val, op) = match (prop(a), prop(b)) {
        (Some(k), None) => (k, konst(b)?, *op),
        (None, Some(k)) => {
            let flipped = match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Le => BinOp::Ge,
                BinOp::Gt => BinOp::Lt,
                BinOp::Ge => BinOp::Le,
                _ => return None,
            };
            (k, konst(a)?, flipped)
        }
        _ => return None,
    };
    let b = |inclusive| {
        Some(Bound {
            value: val.clone(),
            inclusive,
        })
    };
    let pred = match op {
        BinOp::Lt => RangePred {
            hi: b(false),
            ..RangePred::default()
        },
        BinOp::Le => RangePred {
            hi: b(true),
            ..RangePred::default()
        },
        BinOp::Gt => RangePred {
            lo: b(false),
            ..RangePred::default()
        },
        BinOp::Ge => RangePred {
            lo: b(true),
            ..RangePred::default()
        },
        BinOp::StartsWith => match &val {
            Value::String(s) => RangePred {
                prefix: Some(s.clone()),
                ..RangePred::default()
            },
            _ => return None,
        },
        _ => return None,
    };
    Some((key, pred))
}

/// The range conditions of `conj` on `v`'s properties, merged per key.
pub(crate) fn ranges_of(
    conj: &[Expr],
    v: &str,
    params: &crate::eval::Params,
) -> Vec<(String, RangePred)> {
    let mut m: BTreeMap<String, RangePred> = BTreeMap::new();
    for c in conj {
        if let Some((k, p)) = range_of(c, v, params) {
            match m.get_mut(&k) {
                Some(have) => {
                    have.merge(p);
                }
                None => {
                    m.insert(k, p);
                }
            }
        }
    }
    m.into_iter().collect()
}

/// Levels a variable-length step is costed with past its minimum (an
/// unbounded or long one).
const VAR_LEVELS: u32 = 3;

/// Default shares where the statistics don't know.
const EQ_GUESS: f64 = 0.05;
const RANGE_GUESS: f64 = 0.3;

impl Exec<'_> {
    /// The label id of a node pattern's required label: `Some(None)` for
    /// no label, `None` for a label the graph doesn't have (no node
    /// matches).
    async fn label_of(&mut self, np: &NodePat) -> Result<Option<Option<u32>>> {
        match np.labels.as_ref().and_then(|l| l.required()) {
            None => Ok(Some(None)),
            Some(l) => Ok(self.store.label_id(l, false).await?.map(Some)),
        }
    }

    /// What `np`'s conditions (its map and `conj`) let a read find, given
    /// `bound` variables (each bound one: `rows` rows). `None`: no
    /// statistics.
    pub(crate) async fn node_est(
        &mut self,
        np: &NodePat,
        conj: &[Expr],
        bound: &HashSet<String>,
        rows: f64,
        indexes: &[crate::store::IndexDef],
    ) -> Result<Option<NodeEst>> {
        let Some(label) = self.label_of(np).await? else {
            return Ok(Some(NodeEst {
                rows: 0.0,
                cost: 0.0,
                sel: 0.0,
                label: None,
                bound: false,
            }));
        };
        if np.var.as_ref().is_some_and(|v| bound.contains(v)) {
            return Ok(Some(NodeEst {
                rows,
                cost: 0.0,
                sel: 1.0,
                label,
                bound: true,
            }));
        }
        let Some(all) = self.store.est_nodes(label) else {
            return Ok(None);
        };
        let empty = Row::default();
        let mut sel = 1.0f64;
        // The cheapest read: by id, through an index, or the label.
        let mut read: Option<f64> = None;
        let mut eqs: Vec<(String, Vec<Value>)> = Vec::new();
        if let Some(pe) = &np.props
            && super::matcher::is_const_expr(pe)
            && let Ok(Value::Map(m)) = eval(pe, &empty, self.params)
        {
            for (k, v) in m {
                eqs.push((k, vec![v]));
            }
        }
        if let Some(v) = &np.var {
            for c in conj {
                let Expr::Binary(op @ (BinOp::Eq | BinOp::In), a, b) = c else {
                    continue;
                };
                if !super::matcher::is_const_expr(b) {
                    continue;
                }
                let vals = match (op, eval(b, &empty, self.params)) {
                    (BinOp::Eq, Ok(x)) => vec![x],
                    (BinOp::In, Ok(Value::List(l))) => l,
                    _ => continue,
                };
                match &**a {
                    Expr::Func { name, args, .. }
                        if (name == "id" || name == "elementid")
                            && matches!(args.first(), Some(Expr::Var(x)) if x == v) =>
                    {
                        #[allow(clippy::cast_precision_loss)]
                        let n = vals.len() as f64;
                        read = Some(read.map_or(n, |r: f64| r.min(n)));
                        sel = sel.min(n / all.max(1.0));
                    }
                    Expr::Prop(t, k) if matches!(&**t, Expr::Var(x) if x == v) => {
                        eqs.push((k.clone(), vals));
                    }
                    _ => {}
                }
            }
            if let Some(ids) = self.only.get(v) {
                #[allow(clippy::cast_precision_loss)]
                let n = ids.len() as f64;
                read = Some(read.map_or(n, |r: f64| r.min(n)));
                sel = sel.min(n / all.max(1.0));
            }
        }
        let has_index = |key: &str| {
            label.is_some_and(|l| {
                indexes.iter().any(|d| {
                    d.entity == crate::ast::Entity::Node
                        && d.token_id == l
                        && d.state == "online"
                        && d.props.len() == 1
                        && d.props[0] == key
                })
            })
        };
        for (k, vals) in &eqs {
            let s = self
                .store
                .est_prop(label, k, &PropPred::Eq(vals.clone()))
                .unwrap_or(EQ_GUESS);
            sel *= s;
            if has_index(k) {
                let r = all * s;
                read = Some(read.map_or(r, |x: f64| x.min(r)));
            }
        }
        if let Some(v) = &np.var {
            for (k, p) in ranges_of(conj, v, self.params) {
                let s = self
                    .store
                    .est_prop(label, &k, &PropPred::Range(p.clone()))
                    .unwrap_or(RANGE_GUESS);
                sel *= s;
                if let Some(l) = label
                    && self.store.ordered_index(l, &k)
                    && s <= 0.3
                {
                    let r = all * s;
                    read = Some(read.map_or(r, |x: f64| x.min(r)));
                }
            }
        }
        let rows = all * sel;
        Ok(Some(NodeEst {
            rows,
            cost: read.unwrap_or(all),
            sel,
            label,
            bound: false,
        }))
    }

    /// The estimated relationships one row of `near` reads over `rp`
    /// (towards `far`, `dir` as walked), and the share of them the far
    /// node's conditions keep. `None`: no statistics.
    #[allow(clippy::too_many_arguments)]
    async fn hop_est(
        &mut self,
        near: Option<u32>,
        rp: &RelPat,
        dir: Dir,
        far: &NodePat,
        conj: &[Expr],
        bound: &HashSet<String>,
        indexes: &[crate::store::IndexDef],
    ) -> Result<Option<(f64, f64, Option<u32>)>> {
        let mut types = Vec::new();
        for t in &rp.types {
            if let Some(id) = self.store.type_id(t, false).await? {
                types.push(id);
            }
        }
        if !rp.types.is_empty() && types.is_empty() {
            return Ok(Some((0.0, 0.0, None)));
        }
        let Some(far_est) = self
            .node_est(far, conj, &HashSet::new(), 1.0, indexes)
            .await?
        else {
            return Ok(None);
        };
        let Some(rels) = self.store.est_rels(near, &types, dir, far_est.label) else {
            return Ok(None);
        };
        let Some(nears) = self.store.est_nodes(near) else {
            return Ok(None);
        };
        let deg = rels / nears.max(1.0);
        // A far node already bound: one of every node of its label.
        let keep = if far.var.as_ref().is_some_and(|v| bound.contains(v)) {
            1.0 / self.store.est_nodes(far_est.label).unwrap_or(1.0).max(1.0)
        } else {
            far_est.sel
        };
        Ok(Some((deg, keep, far_est.label)))
    }

    /// The estimated cost of matching `pat` from node `at` (0: the start,
    /// `steps.len()`: the last), with `bound` variables at `rows` rows each,
    /// and the rows it ends with. `None`: no statistics.
    async fn walk_cost(
        &mut self,
        pat: &PathPattern,
        at: usize,
        conj: &[Expr],
        bound: &HashSet<String>,
        rows: f64,
        indexes: &[crate::store::IndexDef],
    ) -> Result<Option<(f64, f64)>> {
        let nodes: Vec<&NodePat> = std::iter::once(&pat.start)
            .chain(pat.steps.iter().map(|(_, n)| n))
            .collect();
        let Some(s) = self.node_est(nodes[at], conj, bound, rows, indexes).await? else {
            return Ok(None);
        };
        let mut seen: HashSet<String> = bound.clone();
        if let Some(v) = &nodes[at].var {
            seen.insert(v.clone());
        }
        // A start read once for every incoming row when it isn't bound.
        let (mut cur, mut cost) = if s.bound {
            (s.rows, 0.0)
        } else {
            (rows.max(1.0) * s.rows, s.cost)
        };
        let mut labels: Vec<Option<u32>> = vec![None; nodes.len()];
        labels[at] = s.label;
        // Right of the start, then left of it (from the start outwards).
        let mut order: Vec<(usize, usize, Dir)> = Vec::new();
        for i in at..pat.steps.len() {
            order.push((i, i + 1, pat.steps[i].0.dir));
        }
        for i in (0..at).rev() {
            order.push((i + 1, i, pat.steps[i].0.dir.reverse()));
        }
        for (from, to, dir) in order {
            let rp = &pat.steps[from.min(to)].0;
            let Some((deg, keep, far_label)) = self
                .hop_est(labels[from], rp, dir, nodes[to], conj, &seen, indexes)
                .await?
            else {
                return Ok(None);
            };
            // A variable-length step: the walks of each length in its
            // range (lengths past a few levels counted as a few levels).
            let (read, reached) = match rp.range {
                None => (cur * deg, cur * deg),
                Some((lo, hi)) => {
                    let lo = lo.unwrap_or(1);
                    let hi = hi.unwrap_or(lo.max(1) + VAR_LEVELS).min(lo + VAR_LEVELS);
                    let mut level = cur;
                    let mut read = 0.0;
                    let mut reached = 0.0;
                    for k in 0..=hi {
                        if k >= lo {
                            reached += level;
                        }
                        if k < hi {
                            read += level * deg;
                            level *= deg;
                        }
                    }
                    (read, reached)
                }
            };
            cost += read;
            cur = reached * keep;
            labels[to] = far_label;
            if let Some(v) = &nodes[to].var {
                seen.insert(v.clone());
            }
        }
        Ok(Some((cost + cur, cur)))
    }

    /// Where `pat` should start, from the statistics; `None` keeps the
    /// fixed rules (no statistics, the planner off, or a pattern it leaves
    /// alone: shortest paths, variable-length hops, path variables in the
    /// middle).
    pub(crate) async fn plan_start(
        &mut self,
        rows: &[Row],
        pat: &PathPattern,
        conj: &[Expr],
    ) -> Result<Option<(Start, f64)>> {
        if !planner_on() || pat.kind != PathKind::Normal || pat.steps.is_empty() {
            return Ok(None);
        }
        let bound: HashSet<String> = rows
            .first()
            .map(|r| r.keys().filter(|k| !k.starts_with(' ')).cloned().collect())
            .unwrap_or_default();
        #[allow(clippy::cast_precision_loss)]
        let n = rows.len() as f64;
        let var_len = pat.steps.iter().any(|(r, _)| r.range.is_some());
        // An end bound by the rows or by id is where the fixed rules start
        // anyway: no estimates needed.
        if let Some((_, last)) = pat.steps.last()
            && [&pat.start, last]
                .iter()
                .any(|np| Self::score(np, rows, conj) >= 500)
        {
            return Ok(None);
        }
        let indexes = self.store.indexes();
        let last = pat.steps.len();
        let mut cands = vec![0, last];
        // A middle start runs two halves: only with single hops, no path
        // variable and no top-k pruning of the rows (its last hop must be
        // the pattern's).
        if !var_len && pat.var.is_none() && self.prune.is_none() {
            cands.extend(1..last);
        }
        let mut best: Option<(usize, f64)> = None;
        for at in cands {
            let Some((cost, _)) = self.walk_cost(pat, at, conj, &bound, n, &indexes).await? else {
                return Ok(None);
            };
            // Ties keep the earlier candidate (the pattern as written).
            if best.is_none_or(|(_, c)| cost < c * 0.999) {
                best = Some((at, cost));
            }
        }
        Ok(best.map(|(at, cost)| {
            let s = if at == 0 {
                Start::First
            } else if at == last {
                Start::Last
            } else {
                Start::Middle(at)
            };
            (s, cost)
        }))
    }

    /// The order to match a clause's patterns in: cheapest next given what
    /// is bound so far. `None` keeps the written order.
    pub(crate) async fn plan_order(
        &mut self,
        rows: &[Row],
        patterns: &[PathPattern],
        conj: &[Expr],
    ) -> Result<Option<Vec<usize>>> {
        if !planner_on() || patterns.len() < 2 {
            return Ok(None);
        }
        let mut bound: HashSet<String> = rows
            .first()
            .map(|r| r.keys().filter(|k| !k.starts_with(' ')).cloned().collect())
            .unwrap_or_default();
        #[allow(clippy::cast_precision_loss)]
        let mut cur = rows.len().max(1) as f64;
        let mut left: Vec<usize> = (0..patterns.len()).collect();
        let indexes = self.store.indexes();
        let mut order = Vec::new();
        while !left.is_empty() {
            let mut best: Option<(usize, f64, f64)> = None;
            for (li, &pi) in left.iter().enumerate() {
                let p = &patterns[pi];
                if p.kind != PathKind::Normal {
                    return Ok(None);
                }
                let mut pick: Option<(f64, f64)> = None;
                for at in [0, p.steps.len()] {
                    let Some((cost, out)) =
                        self.walk_cost(p, at, conj, &bound, cur, &indexes).await?
                    else {
                        return Ok(None);
                    };
                    if pick.is_none_or(|(c, _)| cost < c) {
                        pick = Some((cost, out));
                    }
                }
                let Some((cost, out)) = pick else {
                    return Ok(None);
                };
                if best.is_none_or(|(_, c, _)| cost < c * 0.999) {
                    best = Some((li, cost, out));
                }
            }
            let Some((li, _, out)) = best else {
                return Ok(None);
            };
            let pi = left.remove(li);
            for v in super::pattern_vars(&patterns[pi]) {
                bound.insert(v);
            }
            cur = out.max(1.0);
            order.push(pi);
        }
        Ok((order.windows(2).any(|w| w[0] > w[1])).then_some(order))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_are_read_from_conditions() {
        let q = crate::parser::parse(
            "MATCH (n:P) WHERE n.a > 3 AND 10 >= n.a AND n.s STARTS WITH 'x' AND n.b < n.c RETURN n",
        )
        .unwrap();
        let crate::ast::Clause::Match {
            where_: Some(w), ..
        } = &q.parts[0].clauses[0]
        else {
            panic!("not a match")
        };
        let conj: Vec<Expr> = super::super::matcher::conjuncts(w)
            .into_iter()
            .cloned()
            .collect();
        let r = ranges_of(&conj, "n", &crate::eval::Params::new());
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0, "a");
        assert!(
            r[0].1
                .lo
                .as_ref()
                .is_some_and(|b| !b.inclusive && b.value == Value::Int(3))
        );
        assert!(
            r[0].1
                .hi
                .as_ref()
                .is_some_and(|b| b.inclusive && b.value == Value::Int(10))
        );
        assert_eq!(r[1].1.prefix.as_deref(), Some("x"));
    }
}
