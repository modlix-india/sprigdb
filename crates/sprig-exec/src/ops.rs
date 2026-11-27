//! Row operators: a synchronous pull executor over rows.
//!
//! Every operator is a [`RowSource`]: the caller supplies the leaves (table
//! and index scans) and stacks filter, projection, limit, sort, top-N,
//! aggregation, distinct, joins and `UNION` on them. Operators that keep
//! rows (sort, hash aggregation, distinct, the build side of a hash join)
//! charge them to a [`Memory`] account; nothing spills to disk yet.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use bytes::Bytes;
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::Value;

use crate::conv;
use crate::error::{Result, SqlError};
use crate::expr::cast::CoerceFn;
use crate::expr::cmp::{CmpKind, Comparator, Key, comparator};
use crate::expr::{Compiled, Env, agg_result_type, coerce_fn};
use crate::num::{Dec, Round};
use crate::session::{Ctx, Session};
use crate::types::{Derivation, ExprType, JSON_TEXT_LEN, Kind, NOT_FIXED};

/// One row: a value per column.
pub type Row = Vec<Value>;

/// Something rows are pulled from.
pub trait RowSource {
    /// The next row, or `None` after the last.
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>>;

    /// The static type of each column.
    fn types(&self) -> Vec<ExprType>;
}

/// The inner side of a nested-loop join: opened once per outer row, so an
/// index lookup can use the outer row's values.
pub trait InnerSource {
    /// Positions the source for one outer row.
    fn open(&mut self, ctx: &mut Ctx<'_>, outer: &[Value]) -> Result<()>;

    /// The next inner row for the outer row given to [`InnerSource::open`].
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>>;

    /// The static type of each inner column.
    fn types(&self) -> Vec<ExprType>;
}

/// A shared count of the bytes operators hold, with an optional limit.
#[derive(Debug, Default)]
pub struct Memory {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: Option<usize>,
}

impl Memory {
    /// An account without a limit.
    #[must_use]
    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// An account that refuses to pass `limit` bytes.
    #[must_use]
    pub fn with_limit(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: Some(limit),
            ..Self::default()
        })
    }

    /// Adds `bytes`; error 1038 when the limit would be passed.
    pub fn charge(&self, bytes: usize) -> Result<()> {
        let now = self.used.fetch_add(bytes, AtomicOrdering::Relaxed) + bytes;
        self.peak.fetch_max(now, AtomicOrdering::Relaxed);
        match self.limit {
            Some(limit) if now > limit => Err(SqlError::new(
                1038,
                "HY001",
                "Out of sort memory, consider increasing server sort buffer size",
            )),
            _ => Ok(()),
        }
    }

    /// Gives `bytes` back.
    pub fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, AtomicOrdering::Relaxed);
    }

    /// The bytes held now.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(AtomicOrdering::Relaxed)
    }

    /// The most bytes held at once.
    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak.load(AtomicOrdering::Relaxed)
    }
}

/// An estimate of the bytes a row holds.
#[must_use]
pub fn row_bytes(row: &[Value]) -> usize {
    row.iter()
        .map(|v| {
            24 + match v {
                Value::Str(b) | Value::Bytes(b) | Value::Json(b) => b.len(),
                Value::Decimal(d) => d.int_digits().len() + d.frac_digits().len(),
                _ => 0,
            }
        })
        .sum()
}

/// Rows held in memory: a leaf for tests and small inputs, and the inner
/// side of a join without an index.
#[derive(Debug, Clone)]
pub struct VecSource {
    rows: Vec<Row>,
    types: Vec<ExprType>,
    pos: usize,
}

impl VecSource {
    /// A source over `rows` whose columns have `types`.
    #[must_use]
    pub fn new(rows: Vec<Row>, types: Vec<ExprType>) -> Self {
        Self {
            rows,
            types,
            pos: 0,
        }
    }
}

impl RowSource for VecSource {
    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        let row = self.rows.get(self.pos).cloned();
        self.pos += 1;
        Ok(row)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

impl InnerSource for VecSource {
    fn open(&mut self, _: &mut Ctx<'_>, _: &[Value]) -> Result<()> {
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        RowSource::next(self, ctx)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// Keeps the rows for which a predicate is true.
pub struct Filter {
    input: Box<dyn RowSource>,
    pred: Compiled,
}

impl Filter {
    /// A filter of `input` by `pred`.
    #[must_use]
    pub fn new(input: Box<dyn RowSource>, pred: Compiled) -> Self {
        Self { input, pred }
    }
}

impl RowSource for Filter {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            if self.pred.eval_bool(ctx, &row)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        self.input.types()
    }
}

/// Computes one expression per output column.
pub struct Project {
    input: Box<dyn RowSource>,
    exprs: Vec<Compiled>,
}

impl Project {
    /// A projection of `input` through `exprs`.
    #[must_use]
    pub fn new(input: Box<dyn RowSource>, exprs: Vec<Compiled>) -> Self {
        Self { input, exprs }
    }
}

impl RowSource for Project {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        let Some(row) = self.input.next(ctx)? else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(self.exprs.len());
        for e in &self.exprs {
            out.push(e.eval(ctx, &row)?);
        }
        Ok(Some(out))
    }

    fn types(&self) -> Vec<ExprType> {
        self.exprs.iter().map(|e| *e.ty()).collect()
    }
}

/// Skips `offset` rows and passes at most `limit`.
pub struct Limit {
    input: Box<dyn RowSource>,
    offset: u64,
    left: u64,
}

impl Limit {
    /// `LIMIT limit OFFSET offset` over `input`.
    #[must_use]
    pub fn new(input: Box<dyn RowSource>, offset: u64, limit: u64) -> Self {
        Self {
            input,
            offset,
            left: limit,
        }
    }
}

impl RowSource for Limit {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while self.offset > 0 {
            if self.input.next(ctx)?.is_none() {
                return Ok(None);
            }
            self.offset -= 1;
        }
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        self.input.next(ctx)
    }

    fn types(&self) -> Vec<ExprType> {
        self.input.types()
    }
}

mod window;
pub use window::{Frame, FrameBound, WinFunc, WinKind, Window};

/// A value normalized so that plain comparison gives its type's SQL order:
/// `NULL` first, numbers by value, strings by their collation's key.
#[derive(Debug, Clone, PartialEq)]
enum OrdKey {
    Null,
    Num(Dec),
    Real(f64),
    Bytes(Vec<u8>),
    /// A JSON value on MySQL, in MySQL's JSON order.
    Json(crate::func::json::J),
}

impl OrdKey {
    /// The key of `v` (of type `ty`) for a session of either server: on
    /// MySQL a JSON value orders as JSON, otherwise as [`OrdKey::of`].
    fn of_in(v: &Value, ty: &ExprType, maria: bool) -> Self {
        if ty.json && !maria {
            return match crate::func::json::compared(v, ty) {
                Some(j) => OrdKey::Json(j),
                None => OrdKey::Null,
            };
        }
        Self::of(v, ty)
    }

    fn of(v: &Value, ty: &ExprType) -> Self {
        // A UUID or address sorts by its stored bytes.
        if let Some(f) = ty.fbt
            && !v.is_null()
        {
            return match crate::expr::fbt_stored(f, v, ty) {
                Some(b) => OrdKey::Bytes(b),
                None => OrdKey::Null,
            };
        }
        match v {
            Value::Null => OrdKey::Null,
            Value::Str(s) | Value::Bytes(s) | Value::Json(s) => {
                if ty.kind == Kind::Str && ty.coll != Collation::Binary {
                    let mut out = Vec::with_capacity(s.len() * 2);
                    ty.coll.encode_key(s, &mut out);
                    OrdKey::Bytes(out)
                } else {
                    OrdKey::Bytes(s.to_vec())
                }
            }
            Value::Double(d) => OrdKey::Real(if *d == 0.0 { 0.0 } else { *d }),
            Value::Float(f) => OrdKey::Real(f64::from(*f)),
            Value::Int(i) => OrdKey::Num(Dec::from_i64(*i)),
            Value::UInt(u) | Value::Bit(u) | Value::Set(u) => OrdKey::Num(Dec::from_u64(*u)),
            Value::Year(y) => OrdKey::Num(Dec::from_u64(u64::from(*y))),
            Value::Enum(e) => OrdKey::Num(Dec::from_u64(u64::from(*e))),
            Value::Decimal(d) => OrdKey::Num(Dec::from_decimal(d)),
            Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
                OrdKey::Num(crate::temporal::to_dec(v))
            }
        }
    }

    /// The key `GROUP_CONCAT`'s `ORDER BY` sorts by: both servers order
    /// a `NULL` there as the zero value of its type (0, the empty string).
    fn of_or_zero(v: &Value, ty: &ExprType) -> Self {
        if !v.is_null() {
            return Self::of(v, ty);
        }
        match ty.kind {
            Kind::Str => OrdKey::Bytes(Vec::new()),
            Kind::Double | Kind::Float => OrdKey::Real(0.0),
            _ => OrdKey::Num(Dec::zero()),
        }
    }

    fn cmp(&self, o: &Self) -> Ordering {
        match (self, o) {
            (OrdKey::Null, OrdKey::Null) => Ordering::Equal,
            (OrdKey::Null, _) => Ordering::Less,
            (_, OrdKey::Null) => Ordering::Greater,
            (OrdKey::Num(a), OrdKey::Num(b)) => a.cmp(b),
            (OrdKey::Real(a), OrdKey::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (OrdKey::Bytes(a), OrdKey::Bytes(b)) => a.cmp(b),
            (OrdKey::Json(a), OrdKey::Json(b)) => crate::func::json::mysql_cmp(a, b),
            (OrdKey::Json(_), _) => Ordering::Greater,
            (_, OrdKey::Json(_)) => Ordering::Less,
            (OrdKey::Num(a), OrdKey::Real(b)) => {
                a.to_f64().partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (OrdKey::Real(a), OrdKey::Num(b)) => {
                a.partial_cmp(&b.to_f64()).unwrap_or(Ordering::Equal)
            }
            (OrdKey::Bytes(_), _) => Ordering::Greater,
            (_, OrdKey::Bytes(_)) => Ordering::Less,
        }
    }

    /// Bytes that are equal exactly when two keys are equal: what
    /// grouping, `DISTINCT` and hash joins hash by.
    fn hash_bytes(&self, out: &mut Vec<u8>) {
        match self {
            OrdKey::Null => out.push(0),
            OrdKey::Num(d) => {
                out.push(1);
                // The same number at any scale gives the same bytes.
                let mut text = d.to_string();
                if text.contains('.') {
                    while text.ends_with('0') {
                        text.pop();
                    }
                    if text.ends_with('.') {
                        text.pop();
                    }
                }
                if text == "-0" {
                    text = "0".to_owned();
                }
                out.extend_from_slice(text.as_bytes());
                out.push(0xff);
            }
            OrdKey::Real(x) => {
                out.push(2);
                out.extend_from_slice(&x.to_bits().to_be_bytes());
            }
            OrdKey::Bytes(b) => {
                out.push(3);
                out.extend_from_slice(&(b.len() as u32).to_be_bytes());
                out.extend_from_slice(b);
            }
            OrdKey::Json(j) => {
                out.push(4);
                crate::func::json::mysql_hash(j, out);
            }
        }
    }
}

fn row_hash_key(row: &[Value], types: &[ExprType], maria: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(row.len() * 12);
    for (v, ty) in row.iter().zip(types) {
        OrdKey::of_in(v, ty, maria).hash_bytes(&mut out);
    }
    out
}

/// One `ORDER BY` element.
pub struct SortKey {
    /// The expression rows are ordered by.
    pub expr: Compiled,
    /// Whether the order is descending.
    pub desc: bool,
}

struct Sortable {
    keys: Vec<OrdKey>,
    seq: usize,
    row: Row,
}

fn cmp_keys(a: &Sortable, b: &Sortable, desc: &[bool]) -> Ordering {
    for (i, d) in desc.iter().enumerate() {
        let o = a.keys[i].cmp(&b.keys[i]);
        if o != Ordering::Equal {
            return if *d { o.reverse() } else { o };
        }
    }
    a.seq.cmp(&b.seq)
}

/// A heap entry ordered so that the heap's top is the row that sorts last.
struct HeapItem {
    item: Sortable,
    desc: Arc<Vec<bool>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_keys(&self.item, &other.item, &self.desc)
    }
}

/// Sorts its input: `NULL` first ascending (last descending), strings by
/// their collation, equal rows in input order. With a row limit it keeps
/// only the first rows (top-N) while reading.
pub struct Sort {
    input: Box<dyn RowSource>,
    keys: Vec<SortKey>,
    top: Option<usize>,
    memory: Arc<Memory>,
    sorted: Option<std::vec::IntoIter<Row>>,
}

impl Sort {
    /// A full sort of `input` by `keys`.
    #[must_use]
    pub fn new(input: Box<dyn RowSource>, keys: Vec<SortKey>, memory: Arc<Memory>) -> Self {
        Self {
            input,
            keys,
            top: None,
            memory,
            sorted: None,
        }
    }

    /// The first `n` rows of the sort (`ORDER BY ... LIMIT n`), keeping at
    /// most `n` rows in memory.
    #[must_use]
    pub fn top_n(
        input: Box<dyn RowSource>,
        keys: Vec<SortKey>,
        n: usize,
        memory: Arc<Memory>,
    ) -> Self {
        Self {
            top: Some(n),
            ..Self::new(input, keys, memory)
        }
    }

    fn fill(&mut self, ctx: &mut Ctx<'_>) -> Result<Vec<Row>> {
        let desc: Arc<Vec<bool>> = Arc::new(self.keys.iter().map(|k| k.desc).collect());
        let key_types: Vec<ExprType> = self.keys.iter().map(|k| *k.expr.ty()).collect();
        let mut seq = 0usize;
        let mut all: Vec<Sortable> = Vec::new();
        let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
        let mut warned = false;
        while let Some(row) = self.input.next(ctx)? {
            let mut keys = Vec::with_capacity(self.keys.len());
            for (k, ty) in self.keys.iter().zip(&key_types) {
                let key = OrdKey::of_in(&k.expr.eval(ctx, &row)?, ty, ctx.maria());
                // MySQL sorts arrays and objects, and says it doesn't
                // fully support that.
                if !warned
                    && let OrdKey::Json(crate::func::json::J::Arr(_) | crate::func::json::J::Obj(_)) =
                        &key
                {
                    warned = true;
                    ctx.warn(
                        1235,
                        "This version of MySQL doesn't yet support 'sorting of non-scalar JSON values'",
                    );
                }
                keys.push(key);
            }
            let bytes = row_bytes(&row);
            let item = Sortable { keys, seq, row };
            seq += 1;
            match self.top {
                None => {
                    self.memory.charge(bytes)?;
                    all.push(item);
                }
                Some(0) => {}
                Some(n) => {
                    self.memory.charge(bytes)?;
                    heap.push(HeapItem {
                        item,
                        desc: desc.clone(),
                    });
                    if heap.len() > n {
                        if let Some(dropped) = heap.pop() {
                            self.memory.release(row_bytes(&dropped.item.row));
                        }
                    }
                }
            }
        }
        if self.top.is_some() {
            all = heap.into_iter().map(|h| h.item).collect();
        }
        all.sort_by(|a, b| cmp_keys(a, b, &desc));
        Ok(all.into_iter().map(|s| s.row).collect())
    }
}

impl RowSource for Sort {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if self.sorted.is_none() {
            let rows = self.fill(ctx)?;
            self.sorted = Some(rows.into_iter());
        }
        Ok(self.sorted.as_mut().and_then(Iterator::next))
    }

    fn types(&self) -> Vec<ExprType> {
        self.input.types()
    }
}

/// Passes each distinct row once (the first of its equals): strings are
/// equal by their collation, `NULL` equals `NULL`.
pub struct Distinct {
    input: Box<dyn RowSource>,
    types: Vec<ExprType>,
    seen: HashSet<Vec<u8>>,
    memory: Arc<Memory>,
}

impl Distinct {
    /// `SELECT DISTINCT` over `input`.
    #[must_use]
    pub fn new(input: Box<dyn RowSource>, memory: Arc<Memory>) -> Self {
        let types = input.types();
        Self {
            input,
            types,
            seen: HashSet::new(),
            memory,
        }
    }
}

impl RowSource for Distinct {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(ctx)? {
            let key = row_hash_key(&row, &self.types, ctx.maria());
            let bytes = key.len();
            if self.seen.insert(key) {
                self.memory.charge(bytes)?;
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// An aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(*)`
    CountStar,
    /// `COUNT(expr)`
    Count,
    /// `SUM(expr)`
    Sum,
    /// `AVG(expr)`
    Avg,
    /// `MIN(expr)`
    Min,
    /// `MAX(expr)`
    Max,
    /// `GROUP_CONCAT(expr SEPARATOR sep)`
    GroupConcat,
    /// `BIT_AND(expr)`: all ones for no rows.
    BitAnd,
    /// `BIT_OR(expr)`
    BitOr,
    /// `BIT_XOR(expr)`
    BitXor,
    /// `STD`, `STDDEV`, `STDDEV_POP`: the population standard deviation.
    StdPop,
    /// `STDDEV_SAMP`: the sample standard deviation.
    StdSamp,
    /// `VARIANCE`, `VAR_POP`: the population variance.
    VarPop,
    /// `VAR_SAMP`: the sample variance.
    VarSamp,
}

/// One aggregate of an aggregation.
pub struct Aggregate {
    kind: AggKind,
    arg: Option<Compiled>,
    distinct: bool,
    separator: Bytes,
    /// `GROUP_CONCAT ... ORDER BY`: the keys and whether each descends.
    order: Vec<SortKey>,
    /// `GROUP_CONCAT ... LIMIT [offset,] count` (MariaDB).
    limit: Option<(u64, u64)>,
    ty: ExprType,
    exact: bool,
    scale: u32,
    incr: u32,
    max_len: usize,
    maria: bool,
}

impl Aggregate {
    /// An aggregate of `arg` (none for `COUNT(*)`), typed as the session's
    /// persona types it.
    pub fn new(
        kind: AggKind,
        arg: Option<Compiled>,
        distinct: bool,
        session: &Session,
    ) -> Result<Self> {
        let env = Env {
            s: session,
            prep: None,
            counts: None,
        };
        let arg_ty = arg.as_ref().map(|a| *a.ty());
        if kind != AggKind::CountStar && arg.is_none() {
            return Err(SqlError::param_count("aggregate"));
        }
        let incr = u32::from(session.div_precision_increment);
        let numeric = arg_ty.map(|t| crate::expr::arith::num_ty(&t));
        let exact = numeric.is_some_and(|n| n.class != crate::expr::arith::Class::Real);
        let scale = numeric.map_or(0, |n| n.scale);
        let ty = match (kind, arg_ty, numeric) {
            (AggKind::CountStar | AggKind::Count, _, _) => ExprType {
                prec: 20,
                ..ExprType::int(21, false)
            },
            (AggKind::Sum, _, Some(n)) if exact => {
                env.decimal(n.prec + 22, n.scale, false).nullable()
            }
            (AggKind::Avg, _, Some(n)) if exact => {
                env.decimal(n.prec + incr, n.scale + incr, false).nullable()
            }
            (AggKind::Sum, _, Some(n)) => ExprType {
                dec: n.dec,
                ..ExprType::double()
            }
            .nullable(),
            (AggKind::Avg, _, Some(n)) => {
                let dec = if n.dec >= NOT_FIXED {
                    NOT_FIXED
                } else {
                    (u32::from(n.dec) + incr).min(u32::from(NOT_FIXED)) as u8
                };
                ExprType {
                    dec,
                    ..ExprType::double()
                }
                .nullable()
            }
            (AggKind::Min | AggKind::Max, Some(t), _) => ExprType {
                column: false,
                // A JSON value is any length of text.
                len: if t.json { JSON_TEXT_LEN } else { t.len },
                ..t.nullable()
            },
            (AggKind::GroupConcat, Some(t), _) => {
                let coll = if t.kind == Kind::Str {
                    t.coll
                } else {
                    session.collation_connection
                };
                let chars = session.group_concat_max_len / 4;
                ExprType::string(
                    u32::try_from(chars).unwrap_or(u32::MAX / 4),
                    coll,
                    if t.kind == Kind::Str {
                        t.deriv
                    } else {
                        Derivation::Numeric
                    },
                )
                .nullable()
            }
            (AggKind::BitAnd | AggKind::BitOr | AggKind::BitXor, _, _) => ExprType {
                prec: 20,
                ..ExprType::int(21, true)
            },
            (AggKind::StdPop | AggKind::StdSamp | AggKind::VarPop | AggKind::VarSamp, _, n) => {
                // Four more decimals than the argument's, up to the
                // marker of unfixed decimals.
                // (MariaDB; MySQL leaves them unfixed.)
                let dec = match n {
                    Some(n) if n.dec < NOT_FIXED && session.persona.is_maria() => {
                        (u32::from(n.dec) + 4).min(30) as u8
                    }
                    _ => NOT_FIXED,
                };
                ExprType {
                    dec,
                    ..ExprType::double()
                }
                .nullable()
            }
            _ => ExprType::null(),
        };
        Ok(Self {
            kind,
            arg,
            distinct,
            order: Vec::new(),
            limit: None,
            separator: Bytes::from_static(b","),
            ty,
            exact,
            scale,
            incr,
            max_len: usize::try_from(session.group_concat_max_len).unwrap_or(usize::MAX),
            maria: session.persona.is_maria(),
        })
    }

    /// `GROUP_CONCAT`'s separator (a comma by default).
    #[must_use]
    pub fn separator(mut self, sep: &[u8]) -> Self {
        self.separator = Bytes::copy_from_slice(sep);
        self
    }

    /// `GROUP_CONCAT`'s `ORDER BY`: its values are joined in this order
    /// (ties in input order).
    #[must_use]
    pub fn order_by(mut self, keys: Vec<SortKey>) -> Self {
        self.order = keys;
        self
    }

    /// `GROUP_CONCAT`'s `LIMIT [offset,] count` (MariaDB): only these of
    /// the group's values, after ordering, are joined.
    #[must_use]
    pub fn limit(mut self, offset: u64, count: u64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    /// The aggregate's result type.
    #[must_use]
    pub fn ty(&self) -> &ExprType {
        &self.ty
    }
}

/// The running state of one aggregate in one group.
#[derive(Default, Clone)]
struct AggState {
    count: u64,
    sum_dec: Option<Dec>,
    sum_real: f64,
    best: Option<(OrdKey, Value)>,
    text: Vec<u8>,
    cut: bool,
    seen: HashSet<Vec<u8>>,
    /// `GROUP_CONCAT` with `ORDER BY` or `LIMIT`: each value's sort keys
    /// and text, joined when the group ends.
    parts: Vec<(Vec<OrdKey>, Vec<u8>)>,
    bits: u64,
    /// Running mean and sum of squared deviations (the recurrence of
    /// Welford and Knuth).
    mean: f64,
    m2: f64,
    group: u64,
}

impl Aggregate {
    fn update(
        &self,
        st: &mut AggState,
        ctx: &mut Ctx<'_>,
        row: &[Value],
        group: u64,
    ) -> Result<()> {
        let Some(arg) = &self.arg else {
            st.count += 1;
            return Ok(());
        };
        let v = arg.eval(ctx, row)?;
        if v.is_null() {
            return Ok(());
        }
        st.group = group;
        if self.distinct {
            let mut key = Vec::new();
            OrdKey::of_in(&v, arg.ty(), ctx.maria()).hash_bytes(&mut key);
            if !st.seen.insert(key) {
                return Ok(());
            }
        }
        match self.kind {
            AggKind::CountStar | AggKind::Count => st.count += 1,
            AggKind::Sum | AggKind::Avg => {
                st.count += 1;
                if self.exact {
                    if let Some(d) = conv::to_dec(ctx, &v) {
                        st.sum_dec = Some(match st.sum_dec.take() {
                            Some(s) => s.add(&d),
                            None => d,
                        });
                    }
                } else if let Some(x) = conv::to_f64(ctx, &v) {
                    st.sum_real += x;
                }
            }
            AggKind::Min | AggKind::Max => {
                let key = OrdKey::of_in(&v, arg.ty(), ctx.maria());
                let better = match &st.best {
                    None => true,
                    Some((k, _)) => {
                        let o = key.cmp(k);
                        if self.kind == AggKind::Min {
                            o == Ordering::Less
                        } else {
                            o == Ordering::Greater
                        }
                    }
                };
                if better {
                    st.best = Some((key, v));
                }
            }
            AggKind::BitAnd | AggKind::BitOr | AggKind::BitXor => {
                let i = arg.node().int(ctx, row)?;
                let b = i.map_or(0, |i| i.v as u64);
                if st.count == 0 && self.kind == AggKind::BitAnd {
                    st.bits = u64::MAX;
                }
                st.count += 1;
                st.bits = match self.kind {
                    AggKind::BitAnd => st.bits & b,
                    AggKind::BitOr => st.bits | b,
                    _ => st.bits ^ b,
                };
            }
            AggKind::StdPop | AggKind::StdSamp | AggKind::VarPop | AggKind::VarSamp => {
                if let Some(x) = conv::to_f64(ctx, &v) {
                    st.count += 1;
                    let delta = x - st.mean;
                    st.mean += delta / st.count as f64;
                    st.m2 += delta * (x - st.mean);
                }
            }
            AggKind::GroupConcat if !self.order.is_empty() || self.limit.is_some() => {
                let Some(text) = conv::to_text_in_string(&v, arg.ty(), self.maria) else {
                    return Ok(());
                };
                let mut keys = Vec::with_capacity(self.order.len());
                for k in &self.order {
                    let kv = k.expr.eval(ctx, row)?;
                    keys.push(OrdKey::of_or_zero(&kv, k.expr.ty()));
                }
                st.count += 1;
                st.parts.push((keys, text.to_vec()));
            }
            AggKind::GroupConcat => {
                if st.cut {
                    return Ok(());
                }
                let Some(text) = conv::to_text_in_string(&v, arg.ty(), self.maria) else {
                    return Ok(());
                };
                if st.count > 0 {
                    st.text.extend_from_slice(&self.separator);
                }
                st.count += 1;
                st.text.extend_from_slice(&text);
                if st.text.len() > self.max_len {
                    st.text.truncate(self.max_len);
                    st.cut = true;
                    ctx.warn(1260, format!("Row {group} was cut by GROUP_CONCAT()"));
                }
            }
        }
        Ok(())
    }

    fn finish(&self, ctx: &mut Ctx<'_>, mut st: AggState) -> Value {
        match self.kind {
            AggKind::BitAnd => {
                return Value::UInt(if st.count == 0 { u64::MAX } else { st.bits });
            }
            AggKind::BitOr | AggKind::BitXor => return Value::UInt(st.bits),
            AggKind::StdPop | AggKind::StdSamp | AggKind::VarPop | AggKind::VarSamp => {
                let sample = matches!(self.kind, AggKind::StdSamp | AggKind::VarSamp);
                let n = st.count as f64;
                if st.count == 0 || (sample && st.count < 2) {
                    return Value::Null;
                }
                let var = st.m2 / if sample { n - 1.0 } else { n };
                return Value::Double(match self.kind {
                    AggKind::StdPop | AggKind::StdSamp => var.sqrt(),
                    _ => var,
                });
            }
            AggKind::GroupConcat if !self.order.is_empty() || self.limit.is_some() => {
                if st.parts.is_empty() {
                    return Value::Null;
                }
                let desc: Vec<bool> = self.order.iter().map(|k| k.desc).collect();
                let mut parts = std::mem::take(&mut st.parts);
                parts.sort_by(|a, b| {
                    for (i, d) in desc.iter().enumerate() {
                        let o = a.0[i].cmp(&b.0[i]);
                        if o != Ordering::Equal {
                            return if *d { o.reverse() } else { o };
                        }
                    }
                    Ordering::Equal
                });
                let (skip, take) = self.limit.unwrap_or((0, u64::MAX));
                let mut text = Vec::new();
                let mut first = true;
                for (_, t) in parts
                    .into_iter()
                    .skip(usize::try_from(skip).unwrap_or(usize::MAX))
                    .take(usize::try_from(take).unwrap_or(usize::MAX))
                {
                    if !first {
                        text.extend_from_slice(&self.separator);
                    }
                    first = false;
                    text.extend_from_slice(&t);
                    if text.len() > self.max_len {
                        text.truncate(self.max_len);
                        ctx.warn(1260, format!("Row {} was cut by GROUP_CONCAT()", st.group));
                        break;
                    }
                }
                return if self.ty.coll == Collation::Binary {
                    Value::Bytes(Bytes::from(text))
                } else {
                    Value::Str(Bytes::from(text))
                };
            }
            _ => {}
        }
        match self.kind {
            AggKind::CountStar | AggKind::Count => {
                Value::Int(i64::try_from(st.count).unwrap_or(i64::MAX))
            }
            AggKind::Sum => {
                if st.count == 0 {
                    Value::Null
                } else if self.exact {
                    st.sum_dec.map_or(Value::Null, |d| conv::dec_value(&d))
                } else {
                    Value::Double(st.sum_real)
                }
            }
            AggKind::Avg => {
                if st.count == 0 {
                    Value::Null
                } else if self.exact {
                    let n = Dec::from_u64(st.count);
                    match st.sum_dec.and_then(|d| d.div(&n, self.incr)) {
                        Some(q) => conv::dec_value(
                            &q.round((self.scale + self.incr) as i32, Round::HalfUp),
                        ),
                        None => Value::Null,
                    }
                } else {
                    Value::Double(st.sum_real / st.count as f64)
                }
            }
            AggKind::Min | AggKind::Max => st.best.map_or(Value::Null, |(_, v)| v),
            AggKind::GroupConcat => {
                if st.count == 0 {
                    Value::Null
                } else if self.ty.coll == Collation::Binary {
                    Value::Bytes(Bytes::from(st.text))
                } else {
                    Value::Str(Bytes::from(st.text))
                }
            }
            // (Answered above.)
            AggKind::BitAnd
            | AggKind::BitOr
            | AggKind::BitXor
            | AggKind::StdPop
            | AggKind::StdSamp
            | AggKind::VarPop
            | AggKind::VarSamp => Value::Null,
        }
    }
}

struct Group {
    values: Row,
    states: Vec<AggState>,
}

/// Grouping and aggregation. The output row is the group expressions'
/// values followed by the aggregates. Without group expressions there is
/// exactly one output row, also for an empty input.
///
/// The hashing form takes rows in any order and emits groups in the order
/// they first appeared; the streaming form needs its input ordered by the
/// group expressions and holds one group at a time.
pub struct Aggregation {
    input: Box<dyn RowSource>,
    group_by: Vec<Compiled>,
    aggs: Vec<Aggregate>,
    streaming: bool,
    memory: Arc<Memory>,
    done: bool,
    out: std::collections::VecDeque<Row>,
    current: Option<(Vec<u8>, Group)>,
    groups_seen: u64,
    /// `WITH ROLLUP`: also the totals of each group of the leading
    /// columns, and the grand total.
    rollup: bool,
}

impl Aggregation {
    /// Hash aggregation.
    #[must_use]
    pub fn hash(
        input: Box<dyn RowSource>,
        group_by: Vec<Compiled>,
        aggs: Vec<Aggregate>,
        memory: Arc<Memory>,
    ) -> Self {
        Self {
            input,
            group_by,
            aggs,
            streaming: false,
            memory,
            done: false,
            out: std::collections::VecDeque::new(),
            current: None,
            groups_seen: 0,
            rollup: false,
        }
    }

    /// Streaming aggregation over an input ordered by the group
    /// expressions.
    #[must_use]
    pub fn streaming(
        input: Box<dyn RowSource>,
        group_by: Vec<Compiled>,
        aggs: Vec<Aggregate>,
    ) -> Self {
        Self {
            streaming: true,
            ..Self::hash(input, group_by, aggs, Memory::unlimited())
        }
    }

    /// `GROUP BY ... WITH ROLLUP`: after each group, the totals of the
    /// groups of its leading columns that end with it (the other columns
    /// `NULL`), and the grand total last; groups in the order of their
    /// values, `NULL` first, as both servers give them.
    #[must_use]
    pub fn with_rollup(mut self) -> Self {
        self.rollup = true;
        self
    }

    fn fresh(&self) -> Vec<AggState> {
        self.aggs.iter().map(|_| AggState::default()).collect()
    }

    fn emit_level(&mut self, ctx: &mut Ctx<'_>, values: &[Value], l: usize, states: Vec<AggState>) {
        let mut row: Row = values
            .iter()
            .enumerate()
            .map(|(c, v)| if c < l { v.clone() } else { Value::Null })
            .collect();
        for (a, st) in self.aggs.iter().zip(states) {
            row.push(a.finish(ctx, st));
        }
        self.out.push_back(row);
    }

    fn run_rollup(&mut self, ctx: &mut Ctx<'_>) -> Result<()> {
        let k = self.group_by.len();
        let mut rows: Vec<(Vec<OrdKey>, Row, Row)> = Vec::new();
        while let Some(row) = self.input.next(ctx)? {
            let mut keys = Vec::with_capacity(k);
            let mut values = Vec::with_capacity(k);
            for g in &self.group_by {
                let v = g.eval(ctx, &row)?;
                keys.push(OrdKey::of_in(&v, g.ty(), ctx.maria()));
                values.push(v);
            }
            self.memory
                .charge(row_bytes(&row) + row_bytes(&values) + 64)?;
            rows.push((keys, values, row));
        }
        rows.sort_by(|a, b| {
            a.0.iter()
                .zip(&b.0)
                .map(|(x, y)| x.cmp(y))
                .find(|o| o.is_ne())
                .unwrap_or(Ordering::Equal)
        });
        // states[l]: the current group of the first `l` columns.
        let mut states: Vec<Vec<AggState>> = (0..=k).map(|_| self.fresh()).collect();
        let mut numbers: Vec<u64> = (1..=k as u64 + 1).collect();
        let mut next_number = k as u64 + 2;
        for i in 0..rows.len() {
            if i > 0 {
                let (before, now) = (&rows[i - 1].0, &rows[i].0);
                if let Some(c) = (0..k).find(|&c| before[c].cmp(&now[c]).is_ne()) {
                    for l in (c + 1..=k).rev() {
                        let st = std::mem::replace(&mut states[l], self.fresh());
                        let values = rows[i - 1].1.clone();
                        self.emit_level(ctx, &values, l, st);
                        numbers[l] = next_number;
                        next_number += 1;
                    }
                }
            }
            for (l, level) in states.iter_mut().enumerate() {
                for (a, st) in self.aggs.iter().zip(level.iter_mut()) {
                    a.update(st, ctx, &rows[i].2, numbers[l])?;
                }
            }
        }
        if let Some(last) = rows.last() {
            let values = last.1.clone();
            for l in (0..=k).rev() {
                let st = std::mem::take(&mut states[l]);
                self.emit_level(ctx, &values, l, st);
            }
        }
        Ok(())
    }

    fn new_group(&self, values: Row) -> Group {
        Group {
            values,
            states: self.aggs.iter().map(|_| AggState::default()).collect(),
        }
    }

    fn finish_group(&self, ctx: &mut Ctx<'_>, g: Group) -> Row {
        let mut row = g.values;
        for (a, st) in self.aggs.iter().zip(g.states) {
            row.push(a.finish(ctx, st));
        }
        row
    }

    fn group_of(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<(Vec<u8>, Row)> {
        let mut values = Vec::with_capacity(self.group_by.len());
        let mut key = Vec::new();
        for g in &self.group_by {
            let v = g.eval(ctx, row)?;
            OrdKey::of_in(&v, g.ty(), ctx.maria()).hash_bytes(&mut key);
            values.push(v);
        }
        Ok((key, values))
    }

    fn run_hash(&mut self, ctx: &mut Ctx<'_>) -> Result<()> {
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut groups: Vec<Group> = Vec::new();
        while let Some(row) = self.input.next(ctx)? {
            let (key, values) = self.group_of(ctx, &row)?;
            let at = match index.get(&key) {
                Some(i) => *i,
                None => {
                    self.memory.charge(key.len() + row_bytes(&values) + 64)?;
                    index.insert(key, groups.len());
                    groups.push(self.new_group(values));
                    groups.len() - 1
                }
            };
            let number = at as u64 + 1;
            for (a, st) in self.aggs.iter().zip(groups[at].states.iter_mut()) {
                a.update(st, ctx, &row, number)?;
            }
        }
        if groups.is_empty() && self.group_by.is_empty() {
            groups.push(self.new_group(Vec::new()));
        }
        for g in groups {
            let row = self.finish_group(ctx, g);
            self.out.push_back(row);
        }
        Ok(())
    }
}

impl RowSource for Aggregation {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if self.rollup {
            if !self.done {
                self.done = true;
                self.run_rollup(ctx)?;
            }
            return Ok(self.out.pop_front());
        }
        if !self.streaming {
            if !self.done {
                self.done = true;
                self.run_hash(ctx)?;
            }
            return Ok(self.out.pop_front());
        }
        loop {
            if self.done {
                return Ok(self.out.pop_front());
            }
            match self.input.next(ctx)? {
                Some(row) => {
                    let (key, values) = self.group_of(ctx, &row)?;
                    let same = self.current.as_ref().is_some_and(|(k, _)| *k == key);
                    let mut finished = None;
                    if !same {
                        finished = self.current.take().map(|(_, g)| self.finish_group(ctx, g));
                        self.groups_seen += 1;
                        self.current = Some((key, self.new_group(values)));
                    }
                    let number = self.groups_seen;
                    if let Some((_, g)) = self.current.as_mut() {
                        for (a, st) in self.aggs.iter().zip(g.states.iter_mut()) {
                            a.update(st, ctx, &row, number)?;
                        }
                    }
                    if finished.is_some() {
                        return Ok(finished);
                    }
                }
                None => {
                    self.done = true;
                    let last = self.current.take().map(|(_, g)| self.finish_group(ctx, g));
                    if let Some(row) = last {
                        return Ok(Some(row));
                    }
                    if self.groups_seen == 0 && self.group_by.is_empty() {
                        let g = self.new_group(Vec::new());
                        return Ok(Some(self.finish_group(ctx, g)));
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn types(&self) -> Vec<ExprType> {
        self.group_by
            .iter()
            .map(|g| {
                if self.rollup {
                    g.ty().nullable()
                } else {
                    *g.ty()
                }
            })
            .chain(self.aggs.iter().map(|a| a.ty))
            .collect()
    }
}

/// The kind of a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Rows with a match on both sides.
    Inner,
    /// Every left row; an unmatched one is completed with `NULL`s.
    Left,
    /// Each left row that has a match, once, without the right side's
    /// columns.
    Semi,
    /// Each left row that has no match, without the right side's columns.
    Anti,
}

impl JoinKind {
    /// Whether the join only tests for a match and shows the left row.
    fn left_only(self) -> bool {
        matches!(self, JoinKind::Semi | JoinKind::Anti)
    }
}

/// A nested-loop join: for each outer row the inner source is opened (an
/// index lookup can use the outer row) and each inner row is tested with
/// the `ON` condition over the joined row.
pub struct NestedLoopJoin {
    outer: Box<dyn RowSource>,
    inner: Box<dyn InnerSource>,
    on: Option<Compiled>,
    kind: JoinKind,
    inner_width: usize,
    current: Option<(Row, bool)>,
}

impl NestedLoopJoin {
    /// A join of `outer` with `inner` on `on` (none for a cross join).
    #[must_use]
    pub fn new(
        outer: Box<dyn RowSource>,
        inner: Box<dyn InnerSource>,
        on: Option<Compiled>,
        kind: JoinKind,
    ) -> Self {
        let inner_width = inner.types().len();
        Self {
            outer,
            inner,
            on,
            kind,
            inner_width,
            current: None,
        }
    }
}

impl RowSource for NestedLoopJoin {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        loop {
            if self.current.is_none() {
                let Some(outer) = self.outer.next(ctx)? else {
                    return Ok(None);
                };
                self.inner.open(ctx, &outer)?;
                self.current = Some((outer, false));
            }
            let Some((outer, matched)) = self.current.as_mut() else {
                continue;
            };
            match self.inner.next(ctx)? {
                Some(inner) => {
                    let mut joined = outer.clone();
                    joined.extend(inner);
                    let pass = match &self.on {
                        Some(on) => on.eval_bool(ctx, &joined)?,
                        None => true,
                    };
                    if pass {
                        *matched = true;
                        match self.kind {
                            JoinKind::Semi => {
                                // The first match settles it: on to the
                                // next outer row.
                                let row = std::mem::take(outer);
                                self.current = None;
                                return Ok(Some(row));
                            }
                            JoinKind::Anti => self.current = None,
                            _ => return Ok(Some(joined)),
                        }
                    }
                }
                None => {
                    let unmatched =
                        !*matched && matches!(self.kind, JoinKind::Left | JoinKind::Anti);
                    let mut row = std::mem::take(outer);
                    self.current = None;
                    if unmatched {
                        if self.kind == JoinKind::Left {
                            row.extend(std::iter::repeat_n(Value::Null, self.inner_width));
                        }
                        return Ok(Some(row));
                    }
                }
            }
        }
    }

    fn types(&self) -> Vec<ExprType> {
        let mut t = self.outer.types();
        if self.kind.left_only() {
            return t;
        }
        let inner = self.inner.types();
        t.extend(inner.into_iter().map(|ty| {
            if self.kind == JoinKind::Left {
                ty.nullable()
            } else {
                ty
            }
        }));
        t
    }
}

fn key_bytes(k: &Key, kind: CmpKind, out: &mut Vec<u8>) {
    match k {
        Key::Str(s) => {
            let mut enc = Vec::with_capacity(s.len() * 2);
            match kind {
                CmpKind::Str(coll) if coll != Collation::Binary => coll.encode_key(s, &mut enc),
                _ => enc.extend_from_slice(s),
            }
            OrdKey::Bytes(enc).hash_bytes(out);
        }
        Key::Int(i) => OrdKey::Num(i.dec()).hash_bytes(out),
        Key::Dec(d) => OrdKey::Num(d.clone()).hash_bytes(out),
        Key::Real(x) => OrdKey::Real(if *x == 0.0 { 0.0 } else { *x }).hash_bytes(out),
        Key::Json(j) => OrdKey::Json(j.clone()).hash_bytes(out),
    }
}

/// A hash join on equalities: the right side is read into a hash table
/// (charged to the memory account), then each left row probes it. Each
/// key pair is compared in the type and collation `=` would use; a `NULL`
/// key matches nothing. An optional residual condition is tested on the
/// joined row.
pub struct HashJoin {
    left: Box<dyn RowSource>,
    right: Option<Box<dyn RowSource>>,
    left_keys: Vec<Compiled>,
    right_keys: Vec<Compiled>,
    cmps: Vec<Comparator>,
    residual: Option<Compiled>,
    kind: JoinKind,
    memory: Arc<Memory>,
    table: HashMap<Vec<u8>, Vec<Row>>,
    right_types: Vec<ExprType>,
    pending: std::collections::VecDeque<Row>,
}

impl HashJoin {
    /// A join of `left` with `right` where each `left_keys[i]` equals
    /// `right_keys[i]`.
    pub fn new(
        left: Box<dyn RowSource>,
        right: Box<dyn RowSource>,
        left_keys: Vec<Compiled>,
        right_keys: Vec<Compiled>,
        residual: Option<Compiled>,
        kind: JoinKind,
        session: &Session,
        memory: Arc<Memory>,
    ) -> Result<Self> {
        let env = Env {
            s: session,
            prep: None,
            counts: None,
        };
        let mut cmps = Vec::with_capacity(left_keys.len());
        for (l, r) in left_keys.iter().zip(&right_keys) {
            cmps.push(comparator(&[l.ty(), r.ty()], "=", env)?);
        }
        let right_types = right.types();
        Ok(Self {
            left,
            right: Some(right),
            left_keys,
            right_keys,
            cmps,
            residual,
            kind,
            memory,
            table: HashMap::new(),
            right_types,
            pending: std::collections::VecDeque::new(),
        })
    }

    fn key_of(
        &self,
        ctx: &mut Ctx<'_>,
        row: &[Value],
        keys: &[Compiled],
    ) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        for (k, cmp) in keys.iter().zip(&self.cmps) {
            let Some(key) = cmp.node_key(ctx, k.node(), row)? else {
                return Ok(None);
            };
            key_bytes(&key, cmp.kind, &mut out);
        }
        Ok(Some(out))
    }

    fn build(&mut self, ctx: &mut Ctx<'_>) -> Result<()> {
        let Some(mut right) = self.right.take() else {
            return Ok(());
        };
        while let Some(row) = right.next(ctx)? {
            let keys = std::mem::take(&mut self.right_keys);
            let key = self.key_of(ctx, &row, &keys);
            self.right_keys = keys;
            if let Some(key) = key? {
                self.memory.charge(key.len() + row_bytes(&row))?;
                self.table.entry(key).or_default().push(row);
            }
        }
        Ok(())
    }
}

impl RowSource for HashJoin {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        self.build(ctx)?;
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            let Some(left) = self.left.next(ctx)? else {
                return Ok(None);
            };
            let keys = std::mem::take(&mut self.left_keys);
            let key = self.key_of(ctx, &left, &keys);
            self.left_keys = keys;
            let mut matched = false;
            if let Some(rows) = key?.and_then(|k| self.table.get(&k)) {
                for r in rows {
                    let mut joined = left.clone();
                    joined.extend(r.iter().cloned());
                    let pass = match &self.residual {
                        Some(p) => p.eval_bool(ctx, &joined)?,
                        None => true,
                    };
                    if pass {
                        matched = true;
                        if self.kind.left_only() {
                            break;
                        }
                        self.pending.push_back(joined);
                    }
                }
            }
            if self.kind.left_only() {
                if matched == (self.kind == JoinKind::Semi) {
                    return Ok(Some(left));
                }
                continue;
            }
            if !matched && self.kind == JoinKind::Left {
                let mut row = left;
                row.extend(std::iter::repeat_n(Value::Null, self.right_types.len()));
                return Ok(Some(row));
            }
        }
    }

    fn types(&self) -> Vec<ExprType> {
        let mut t = self.left.types();
        if self.kind.left_only() {
            return t;
        }
        t.extend(self.right_types.iter().map(|ty| {
            if self.kind == JoinKind::Left {
                ty.nullable()
            } else {
                *ty
            }
        }));
        t
    }
}

/// `UNION [ALL]`: the branches one after another, each column converted
/// to the type merged over the branches; without `ALL`, each distinct row
/// once.
pub struct Union {
    inputs: Vec<Box<dyn RowSource>>,
    coerce: Vec<Vec<CoerceFn>>,
    types: Vec<ExprType>,
    at: usize,
    seen: Option<HashSet<Vec<u8>>>,
    memory: Arc<Memory>,
}

impl Union {
    /// A union of `inputs`, which must have the same number of columns
    /// (error 1222 otherwise).
    pub fn new(
        inputs: Vec<Box<dyn RowSource>>,
        all: bool,
        session: &Session,
        memory: Arc<Memory>,
    ) -> Result<Self> {
        let env = Env {
            s: session,
            prep: None,
            counts: None,
        };
        let branch_types: Vec<Vec<ExprType>> = inputs.iter().map(|i| i.types()).collect();
        let width = branch_types.first().map_or(0, Vec::len);
        if branch_types.iter().any(|t| t.len() != width) {
            return Err(SqlError::new(
                1222,
                "21000",
                "The used SELECT statements have a different number of columns",
            ));
        }
        let mut types = Vec::with_capacity(width);
        for c in 0..width {
            let col: Vec<ExprType> = branch_types.iter().map(|t| t[c]).collect();
            let mut ty = agg_result_type(&col, "UNION", env)?;
            // Two columns whose collations can't be merged can't be
            // united: rows couldn't be compared.
            if ty.kind == Kind::Str && ty.deriv == Derivation::None {
                return Err(SqlError::new(
                    1271,
                    "HY000",
                    "Illegal mix of collations for operation 'UNION'",
                ));
            }
            ty.column = false;
            types.push(ty);
        }
        let coerce = branch_types
            .iter()
            .map(|bt| {
                bt.iter()
                    .zip(&types)
                    .map(|(from, to)| coerce_fn(from, to, env))
                    .collect()
            })
            .collect();
        Ok(Self {
            inputs,
            coerce,
            types,
            at: 0,
            seen: (!all).then(HashSet::new),
            memory,
        })
    }
}

impl RowSource for Union {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while self.at < self.inputs.len() {
            let Some(row) = self.inputs[self.at].next(ctx)? else {
                self.at += 1;
                continue;
            };
            let mut out = Vec::with_capacity(row.len());
            for (v, co) in row.into_iter().zip(&self.coerce[self.at]) {
                out.push(co(ctx, v)?);
            }
            if let Some(seen) = &mut self.seen {
                let key = row_hash_key(&out, &self.types, ctx.maria());
                let bytes = key.len();
                if !seen.insert(key) {
                    continue;
                }
                self.memory.charge(bytes)?;
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// The rows of a recursive common table expression as they are made. The
/// columns have the types the non-recursive parts gave (every one
/// nullable); each row a part makes is stored into them as into a
/// temporary table's columns: converted, a string longer than its column
/// failing with 1406 under a strict mode (cut with 1265 otherwise), text
/// that is no number failing with 1366. Under `UNION DISTINCT` a row
/// already held is dropped. The rows held are charged to the statement's
/// memory; past its limit the statement fails with 1114.
pub struct CteRows {
    name: String,
    names: Vec<String>,
    types: Vec<ExprType>,
    seen: Option<HashSet<Vec<u8>>>,
    memory: Arc<Memory>,
    /// The rows so far, in the order they were made.
    pub rows: Vec<Row>,
}

impl CteRows {
    /// An empty set of rows for the expression `name` with columns
    /// `names` of types `types`; `all` keeps duplicates (`UNION ALL`).
    #[must_use]
    pub fn new(
        name: &str,
        names: Vec<String>,
        types: Vec<ExprType>,
        all: bool,
        memory: Arc<Memory>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            names,
            types,
            seen: (!all).then(HashSet::new),
            memory,
            rows: Vec::new(),
        }
    }

    /// The column types.
    #[must_use]
    pub fn types(&self) -> &[ExprType] {
        &self.types
    }

    /// Stores a row a part made, whose values have the static types
    /// `from`. `at` is the row number an error names. Gives whether the
    /// row was kept (`false`: a duplicate under `UNION DISTINCT`).
    ///
    /// # Errors
    /// 1406 and 1366 as described on [`CteRows`]; 1114 when the rows no
    /// longer fit the statement's memory.
    pub fn add(&mut self, ctx: &mut Ctx<'_>, row: Row, from: &[ExprType], at: u64) -> Result<bool> {
        let env = Env {
            s: ctx.session,
            prep: None,
            counts: None,
        };
        let strict = ctx.session.sql_mode.has(crate::session::SqlMode::STRICT);
        let maria = ctx.maria();
        let mut out = Vec::with_capacity(row.len());
        for (i, v) in row.into_iter().enumerate() {
            let to = self.types[i];
            let fty = from.get(i).copied().unwrap_or(to);
            let name = self.names.get(i).map_or("", String::as_str);
            let column = if maria {
                format!("``.`(temporary)`.`{name}`")
            } else {
                format!("'{name}'")
            };
            let text = matches!(v, Value::Str(_) | Value::Bytes(_));
            let shown = match &v {
                Value::Str(b) | Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                _ => String::new(),
            };
            let kept = ctx.warnings.len();
            let counted = ctx.warning_count;
            let mut v = coerce_fn(&fty, &to, env)(ctx, v)?;
            if text && ctx.warning_count > counted && strict {
                let word = match to.kind {
                    Kind::Int | Kind::Year | Kind::Bit => Some("integer"),
                    Kind::Decimal => Some("decimal"),
                    Kind::Double | Kind::Float => Some("double"),
                    _ => None,
                };
                if let Some(word) = word {
                    ctx.warnings.truncate(kept);
                    ctx.warning_count = counted;
                    return Err(SqlError::new(
                        1366,
                        if maria { "22007" } else { "HY000" },
                        format!(
                            "Incorrect {word} value: '{shown}' for column {column} at row {at}"
                        ),
                    ));
                }
            }
            if to.kind == Kind::Str {
                let cut = match &v {
                    Value::Str(b) | Value::Bytes(b) => {
                        let chars = crate::expr::like::chars_of(to.coll.charset(), b);
                        let max = usize::try_from(to.len).unwrap_or(usize::MAX);
                        (chars.len() > max).then(|| chars[..max].concat())
                    }
                    _ => None,
                };
                if let Some(short) = cut {
                    if strict {
                        return Err(SqlError::new(
                            1406,
                            "22001",
                            format!("Data too long for column '{name}' at row {at}"),
                        ));
                    }
                    ctx.warn(
                        1265,
                        format!("Data truncated for column '{name}' at row {at}"),
                    );
                    v = match v {
                        Value::Bytes(_) => Value::Bytes(short.into()),
                        _ => Value::Str(short.into()),
                    };
                }
            }
            out.push(v);
        }
        let mut bytes = row_bytes(&out);
        if let Some(seen) = &mut self.seen {
            let key = row_hash_key(&out, &self.types, maria);
            bytes += key.len();
            if !seen.insert(key) {
                return Ok(false);
            }
        }
        if self.memory.charge(bytes).is_err() {
            return Err(SqlError::new(
                1114,
                "HY000",
                format!("The table '{}' is full", self.name),
            ));
        }
        self.rows.push(out);
        Ok(true)
    }
}

/// `INTERSECT` or `EXCEPT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetKind {
    /// Rows of the left input that the right one has.
    Intersect,
    /// Rows of the left input that the right one hasn't.
    Except,
}

/// `left INTERSECT|EXCEPT [ALL|DISTINCT] right`. The columns' types are
/// merged as for `UNION`; rows are equal when every value compares equal
/// (`NULL` equals `NULL`, as in `DISTINCT`). The right input is read
/// whole first, then the left one streams through in its order:
///
/// | | `DISTINCT` | `ALL` |
/// |---|---|---|
/// | `INTERSECT` | each left row the right has, once | as often as both have it (the smaller count) |
/// | `EXCEPT` | each left row the right hasn't, once | as often as the left has it beyond the right's count |
pub struct SetOp {
    left: Box<dyn RowSource>,
    right: Option<Box<dyn RowSource>>,
    kind: SetKind,
    all: bool,
    coerce: [Vec<CoerceFn>; 2],
    types: Vec<ExprType>,
    /// The right input's rows: how many times each is left to match.
    counts: HashMap<Vec<u8>, u64>,
    /// Rows already given (`DISTINCT`).
    given: HashSet<Vec<u8>>,
    memory: Arc<Memory>,
}

impl SetOp {
    /// `left` `kind` `right`; both must have the same number of columns
    /// (error 1222 otherwise).
    pub fn new(
        left: Box<dyn RowSource>,
        right: Box<dyn RowSource>,
        kind: SetKind,
        all: bool,
        session: &Session,
        memory: Arc<Memory>,
    ) -> Result<Self> {
        let env = Env {
            s: session,
            prep: None,
            counts: None,
        };
        let (lt, rt) = (left.types(), right.types());
        if lt.len() != rt.len() {
            return Err(SqlError::new(
                1222,
                "21000",
                "The used SELECT statements have a different number of columns",
            ));
        }
        let op = match kind {
            SetKind::Intersect => "INTERSECT",
            SetKind::Except => "EXCEPT",
        };
        let mut types = Vec::with_capacity(lt.len());
        for (a, b) in lt.iter().zip(&rt) {
            let mut ty = agg_result_type(&[*a, *b], op, env)?;
            if ty.kind == Kind::Str && ty.deriv == Derivation::None {
                return Err(SqlError::new(
                    1271,
                    "HY000",
                    format!("Illegal mix of collations for operation '{op}'"),
                ));
            }
            ty.column = false;
            types.push(ty);
        }
        let co = |bt: &[ExprType]| -> Vec<CoerceFn> {
            bt.iter()
                .zip(&types)
                .map(|(from, to)| coerce_fn(from, to, env))
                .collect()
        };
        let coerce = [co(&lt), co(&rt)];
        Ok(Self {
            left,
            right: Some(right),
            kind,
            all,
            coerce,
            types,
            counts: HashMap::new(),
            given: HashSet::new(),
            memory,
        })
    }

    fn key(&self, ctx: &mut Ctx<'_>, side: usize, row: Row) -> Result<(Row, Vec<u8>)> {
        let mut out = Vec::with_capacity(row.len());
        for (v, co) in row.into_iter().zip(&self.coerce[side]) {
            out.push(co(ctx, v)?);
        }
        let key = row_hash_key(&out, &self.types, ctx.maria());
        Ok((out, key))
    }
}

impl RowSource for SetOp {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if let Some(mut right) = self.right.take() {
            while let Some(row) = right.next(ctx)? {
                let (_, key) = self.key(ctx, 1, row)?;
                let bytes = key.len();
                let n = self.counts.entry(key).or_insert(0);
                if *n == 0 {
                    self.memory.charge(bytes)?;
                }
                *n += 1;
            }
        }
        while let Some(row) = self.left.next(ctx)? {
            let (row, key) = self.key(ctx, 0, row)?;
            let n = self.counts.get_mut(&key);
            let keep = match (self.kind, self.all) {
                (SetKind::Intersect, true) => match n {
                    Some(c) if *c > 0 => {
                        *c -= 1;
                        true
                    }
                    _ => false,
                },
                (SetKind::Except, true) => match n {
                    Some(c) if *c > 0 => {
                        *c -= 1;
                        false
                    }
                    _ => true,
                },
                (SetKind::Intersect, false) => n.is_some(),
                (SetKind::Except, false) => n.is_none(),
            };
            if !keep {
                continue;
            }
            if !self.all {
                let bytes = key.len();
                if !self.given.insert(key) {
                    continue;
                }
                self.memory.charge(bytes)?;
            }
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// Reads every row of a source.
pub fn collect(source: &mut dyn RowSource, ctx: &mut Ctx<'_>) -> Result<Vec<Row>> {
    let mut out = Vec::new();
    while let Some(row) = source.next(ctx)? {
        out.push(row);
    }
    Ok(out)
}
