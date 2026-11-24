//! Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`,
//! `CUME_DIST`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`,
//! `NTH_VALUE` and the aggregates over a window frame.
//!
//! [`Window`] reads rows already sorted by the window's partition and
//! order keys, holds one partition at a time, and appends one column per
//! function to each row. Frames are `ROWS` or `RANGE` with `UNBOUNDED`,
//! `CURRENT ROW` or (for `ROWS`) a row count as bounds; an aggregate whose
//! frame starts at the partition's first row is computed incrementally,
//! any other frame row by row.

use std::cmp::Ordering;
use std::collections::VecDeque;

use sprig_engine::row::Value;

use super::{AggState, Aggregate, OrdKey, Row, RowSource, SortKey};
use crate::error::Result;
use crate::expr::Compiled;
use crate::session::Ctx;
use crate::types::ExprType;

/// One end of a window frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`
    UnboundedPreceding,
    /// `n PRECEDING` (rows)
    Preceding(u64),
    /// `CURRENT ROW` (with `RANGE`, the row and its peers)
    CurrentRow,
    /// `n FOLLOWING` (rows)
    Following(u64),
    /// `UNBOUNDED FOLLOWING`
    UnboundedFollowing,
}

/// A window frame: `ROWS` (`rows` set) or `RANGE` between two bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    /// `ROWS`; else `RANGE`.
    pub rows: bool,
    /// The first row.
    pub start: FrameBound,
    /// The last row.
    pub end: FrameBound,
}

impl Frame {
    /// The default frame: with an order, from the partition's first row to
    /// the current row's last peer; without one, the whole partition.
    #[must_use]
    pub fn default_for(ordered: bool) -> Self {
        Self {
            rows: false,
            start: FrameBound::UnboundedPreceding,
            end: if ordered {
                FrameBound::CurrentRow
            } else {
                FrameBound::UnboundedFollowing
            },
        }
    }
}

/// What a window function computes.
pub enum WinKind {
    /// `ROW_NUMBER()`
    RowNumber,
    /// `RANK()`
    Rank,
    /// `DENSE_RANK()`
    DenseRank,
    /// `PERCENT_RANK()`
    PercentRank,
    /// `CUME_DIST()`
    CumeDist,
    /// `NTILE(n)`: the bucket count, evaluated on the partition's first row.
    Ntile(Compiled),
    /// `LAG(arg, offset, default)` and `LEAD(...)` (`lead` set).
    Shift {
        /// The value read from the other row.
        arg: Compiled,
        /// How many rows back (`LAG`) or ahead (`LEAD`).
        offset: u64,
        /// The value when there is no such row (else `NULL`).
        default: Option<Compiled>,
        /// `LEAD`.
        lead: bool,
    },
    /// `FIRST_VALUE(arg)`
    FirstValue(Compiled),
    /// `LAST_VALUE(arg)`
    LastValue(Compiled),
    /// `NTH_VALUE(arg, n)`
    NthValue(Compiled, u64),
    /// An aggregate over the frame.
    Agg(Aggregate),
}

/// One window function with its frame and result type.
pub struct WinFunc {
    /// The function.
    pub kind: WinKind,
    /// The frame (used by the value functions and the aggregates).
    pub frame: Frame,
    /// The result type.
    pub ty: ExprType,
}

/// Appends window function values to rows sorted by partition and order.
pub struct Window {
    input: Box<dyn RowSource>,
    partition: Vec<Compiled>,
    order: Vec<SortKey>,
    funcs: Vec<WinFunc>,
    /// The first row of the next partition, read ahead.
    ahead: Option<(Row, Vec<OrdKey>)>,
    out: VecDeque<Row>,
    done: bool,
}

impl Window {
    /// Window functions over `input`, which is sorted by `partition` and
    /// then by `order`.
    #[must_use]
    pub fn new(
        input: Box<dyn RowSource>,
        partition: Vec<Compiled>,
        order: Vec<SortKey>,
        funcs: Vec<WinFunc>,
    ) -> Self {
        Self {
            input,
            partition,
            order,
            funcs,
            ahead: None,
            out: VecDeque::new(),
            done: false,
        }
    }

    fn part_key(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Vec<OrdKey>> {
        let mut k = Vec::with_capacity(self.partition.len());
        for p in &self.partition {
            k.push(OrdKey::of(&p.eval(ctx, row)?, p.ty()));
        }
        Ok(k)
    }

    /// Reads the next partition into `out`.
    fn fill(&mut self, ctx: &mut Ctx<'_>) -> Result<()> {
        let first = match self.ahead.take() {
            Some(a) => a,
            None => match self.input.next(ctx)? {
                Some(r) => {
                    let k = self.part_key(ctx, &r)?;
                    (r, k)
                }
                None => {
                    self.done = true;
                    return Ok(());
                }
            },
        };
        let (row, key) = first;
        let mut rows = vec![row];
        while let Some(r) = self.input.next(ctx)? {
            let k = self.part_key(ctx, &r)?;
            if k != key {
                self.ahead = Some((r, k));
                break;
            }
            rows.push(r);
        }
        if self.ahead.is_none() {
            self.done = true;
        }
        let values = self.compute(ctx, &rows)?;
        for (i, mut r) in rows.into_iter().enumerate() {
            for col in &values {
                r.push(col[i].clone());
            }
            self.out.push_back(r);
        }
        Ok(())
    }

    /// Each function's values for the rows of one partition.
    fn compute(&self, ctx: &mut Ctx<'_>, rows: &[Row]) -> Result<Vec<Vec<Value>>> {
        let n = rows.len();
        // Peers: rows with equal order keys.
        let mut okeys = Vec::with_capacity(n);
        for r in rows {
            let mut k = Vec::with_capacity(self.order.len());
            for o in &self.order {
                k.push(OrdKey::of(&o.expr.eval(ctx, r)?, o.expr.ty()));
            }
            okeys.push(k);
        }
        let mut peer_start = vec![0usize; n];
        let mut peer_end = vec![0usize; n];
        let mut dense = vec![0u64; n];
        let mut i = 0;
        let mut groups = 0u64;
        while i < n {
            let mut j = i;
            while j + 1 < n && okeys[j + 1].cmp_eq(&okeys[i]) {
                j += 1;
            }
            groups += 1;
            for k in i..=j {
                peer_start[k] = i;
                peer_end[k] = j;
                dense[k] = groups;
            }
            i = j + 1;
        }
        let mut out = Vec::with_capacity(self.funcs.len());
        for f in &self.funcs {
            let mut col = Vec::with_capacity(n);
            match &f.kind {
                WinKind::RowNumber => {
                    col.extend((1..=n as u64).map(|v| Value::Int(v as i64)));
                }
                WinKind::Rank => col.extend(peer_start.iter().map(|s| Value::Int(*s as i64 + 1))),
                WinKind::DenseRank => col.extend(dense.iter().map(|d| Value::Int(*d as i64))),
                WinKind::PercentRank => col.extend(peer_start.iter().map(|s| {
                    Value::Double(if n > 1 {
                        *s as f64 / (n - 1) as f64
                    } else {
                        0.0
                    })
                })),
                WinKind::CumeDist => col.extend(
                    peer_end
                        .iter()
                        .map(|e| Value::Double((*e + 1) as f64 / n as f64)),
                ),
                WinKind::Ntile(b) => {
                    let buckets = match b.eval(ctx, &rows[0])? {
                        Value::Int(v) if v > 0 => v as u64,
                        Value::UInt(v) if v > 0 => v,
                        _ => {
                            return Err(crate::error::SqlError::new(
                                1210,
                                "HY000",
                                "Incorrect arguments to ntile",
                            ));
                        }
                    };
                    let n64 = n as u64;
                    let size = n64 / buckets;
                    let extra = n64 % buckets;
                    for r in 0..n64 {
                        // The first `extra` buckets hold one row more.
                        let big = extra * (size + 1);
                        let b = if r < big {
                            r / (size + 1)
                        } else {
                            extra + (r - big) / size.max(1)
                        };
                        col.push(Value::Int(b as i64 + 1));
                    }
                }
                WinKind::Shift {
                    arg,
                    offset,
                    default,
                    lead,
                } => {
                    for r in 0..n {
                        let at = if *lead {
                            r.checked_add(*offset as usize).filter(|x| *x < n)
                        } else {
                            r.checked_sub(*offset as usize)
                        };
                        col.push(match at {
                            Some(a) => arg.eval(ctx, &rows[a])?,
                            None => match default {
                                Some(d) => d.eval(ctx, &rows[r])?,
                                None => Value::Null,
                            },
                        });
                    }
                }
                WinKind::FirstValue(arg) | WinKind::LastValue(arg) | WinKind::NthValue(arg, _) => {
                    for r in 0..n {
                        let (lo, hi) = bounds(&f.frame, r, n, &peer_start, &peer_end);
                        let pick = match &f.kind {
                            WinKind::FirstValue(_) => (lo < hi).then_some(lo),
                            WinKind::LastValue(_) => (lo < hi).then(|| hi - 1),
                            WinKind::NthValue(_, k) => {
                                let at = lo + (*k as usize).saturating_sub(1);
                                (at < hi).then_some(at)
                            }
                            _ => None,
                        };
                        col.push(match pick {
                            Some(p) => arg.eval(ctx, &rows[p])?,
                            None => Value::Null,
                        });
                    }
                }
                WinKind::Agg(agg) => {
                    if f.frame.start == FrameBound::UnboundedPreceding {
                        // A growing frame: add rows as the end moves on.
                        let mut st = AggState::default();
                        let mut added = 0usize;
                        for r in 0..n {
                            let (_, hi) = bounds(&f.frame, r, n, &peer_start, &peer_end);
                            while added < hi {
                                agg.update(&mut st, ctx, &rows[added], 0)?;
                                added += 1;
                            }
                            col.push(agg.finish(ctx, st.clone()));
                        }
                    } else {
                        for r in 0..n {
                            let (lo, hi) = bounds(&f.frame, r, n, &peer_start, &peer_end);
                            let mut st = AggState::default();
                            for row in rows.iter().take(hi).skip(lo) {
                                agg.update(&mut st, ctx, row, 0)?;
                            }
                            col.push(agg.finish(ctx, st));
                        }
                    }
                }
            }
            out.push(col);
        }
        Ok(out)
    }
}

impl OrdKey {
    fn cmp_eq(a: &[OrdKey], b: &[OrdKey]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.cmp(y) == Ordering::Equal)
    }
}

trait KeysEq {
    fn cmp_eq(&self, other: &Self) -> bool;
}

impl KeysEq for Vec<OrdKey> {
    fn cmp_eq(&self, other: &Self) -> bool {
        OrdKey::cmp_eq(self, other)
    }
}

/// The frame of row `r` of a partition of `n` rows: rows `lo..hi`.
fn bounds(f: &Frame, r: usize, n: usize, ps: &[usize], pe: &[usize]) -> (usize, usize) {
    let at = |b: FrameBound, end: bool| -> i64 {
        match b {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::UnboundedFollowing => n as i64,
            FrameBound::CurrentRow => {
                if f.rows {
                    r as i64 + i64::from(end)
                } else if end {
                    pe[r] as i64 + 1
                } else {
                    ps[r] as i64
                }
            }
            FrameBound::Preceding(k) => r as i64 - k as i64 + i64::from(end),
            FrameBound::Following(k) => r as i64 + k as i64 + i64::from(end),
        }
    };
    let lo = at(f.start, false).clamp(0, n as i64) as usize;
    let hi = at(f.end, true).clamp(0, n as i64) as usize;
    (lo, hi.max(lo))
}

impl RowSource for Window {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while self.out.is_empty() && !self.done {
            self.fill(ctx)?;
        }
        Ok(self.out.pop_front())
    }

    fn types(&self) -> Vec<ExprType> {
        let mut t = self.input.types();
        t.extend(self.funcs.iter().map(|f| f.ty));
        t
    }
}
