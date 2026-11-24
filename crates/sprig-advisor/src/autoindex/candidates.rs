//! Triggers and candidate generation, from the workload's shapes.
//!
//! ```text
//! qualify(window):
//!   SQL SELECT, UPDATE and DELETE patterns only
//!   slow   runs >= min_runs and p95 > p95_threshold_ms
//!   heavy  among the top_n by total time, and rows examined per row
//!          returned > min_examined_per_returned
//!   scan   at least 5 large-scan runs from at least 2 sessions
//!
//! generate(shape, table):
//!   E := equality, IS NULL and short IN conditions on the table, and its
//!        side of each join equality
//!   R := ranges, LIKE 'abc%', IS NOT NULL, long IN lists
//!   O := ORDER BY when every column is the table's (uniform DESC becomes
//!        ascending: a backward scan serves it)
//!   G := GROUP BY when every column is the table's
//!   eq := E, most distinct values first, ties by name
//!   r  := the member of R with the most distinct values
//!   A  := eq ++ [r] ++ (O, or G without O) -- order columns only when
//!         there is no r or r is O's first column
//!   also eq ++ O (with LIMIT, r not O's first) and eq ++ G (r not G's first)
//!   and A ++ the selected columns it lacks (a covering variant, at most
//!   3 more columns)
//!   skipped: no columns; a candidate an existing index already starts
//!   with; one that starts with the primary key's columns; columns that
//!   can't be key parts (BLOB, TEXT, JSON, geometry)
//! ```

use std::collections::BTreeSet;

use sprig_workload::fingerprint::FpKind;
use sprig_workload::shape::{ColumnRef, JoinOp, PredOp, Shape, SortDir, StmtType, TableRef};
use sprig_workload::window::FpWindowStats;

use super::model::{IndexSpec, KeyCol};
use super::settings::AutoIndexSettings;

/// The most columns of a candidate's key.
pub const MAX_KEY_COLUMNS: usize = 6;
/// The most columns a covering variant adds.
pub const MAX_COVER_COLUMNS: usize = 3;
/// An `IN` list longer than this is a range, not an equality.
pub const IN_EQ_MAX: u64 = 32;

/// Why a pattern qualified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Repeats and is slow at p95.
    SlowP95,
    /// Among the top by total time, reading far more rows than it returns.
    TopTotalTime,
    /// Scans large tables from several sessions.
    LargeScan,
}

impl Reason {
    /// The name records carry.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::SlowP95 => "slow_p95",
            Self::TopTotalTime => "top_total_time",
            Self::LargeScan => "large_scan",
        }
    }
}

/// One column as the generator sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    /// The name.
    pub name: String,
    /// Whether it can be a whole key part (not `BLOB`, `TEXT`, `JSON` or
    /// geometry).
    pub indexable: bool,
    /// Estimated distinct values, when known.
    pub distinct: Option<f64>,
}

/// An index the table already has.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingIndex {
    /// Its name.
    pub name: String,
    /// Its columns.
    pub cols: Vec<KeyCol>,
    /// `IGNORED`: maintained, read by no plan.
    pub ignored: bool,
}

/// One table as the generator sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
    /// The columns.
    pub columns: Vec<ColumnInfo>,
    /// The primary key's columns (empty without one).
    pub pk: Vec<String>,
    /// The secondary indexes.
    pub indexes: Vec<ExistingIndex>,
    /// Estimated rows.
    pub rows: u64,
}

impl TableInfo {
    /// The column called `name` (any case).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Whether a visible index (or the primary key) already starts with
    /// `cols`, in any direction.
    #[must_use]
    pub fn covered(&self, cols: &[KeyCol]) -> bool {
        let starts = |have: &[String]| {
            cols.len() <= have.len()
                && cols
                    .iter()
                    .zip(have)
                    .all(|(c, h)| c.column.eq_ignore_ascii_case(h))
        };
        if starts(&self.pk) {
            return true;
        }
        self.indexes.iter().filter(|x| !x.ignored).any(|x| {
            let names: Vec<String> = x.cols.iter().map(|c| c.column.clone()).collect();
            starts(&names)
        })
    }
}

/// Whether a pattern's kind and statement can use a secondary index.
#[must_use]
pub fn indexable(f: &FpWindowStats) -> bool {
    f.kind == FpKind::Sql
        && matches!(
            f.shape.stmt_type,
            StmtType::Select | StmtType::Update | StmtType::Delete
        )
}

/// The window's patterns that qualify, with why: positions in `window`.
#[must_use]
pub fn qualify(window: &[FpWindowStats], s: &AutoIndexSettings) -> Vec<(usize, Reason)> {
    let mut by_total: Vec<usize> = (0..window.len()).collect();
    by_total.sort_by(|&a, &b| window[b].total_ms().total_cmp(&window[a].total_ms()));
    let top: BTreeSet<usize> = by_total.into_iter().take(s.top_n as usize).collect();
    let mut out = Vec::new();
    for (i, f) in window.iter().enumerate() {
        if !indexable(f) || f.runs() == 0 {
            continue;
        }
        let slow = f.runs() >= u64::from(s.min_runs) && f.p95_ms() > s.p95_threshold_ms;
        let heavy = top.contains(&i)
            && f.runs() >= u64::from(s.min_runs.min(10))
            && f.examined_per_returned()
                .is_some_and(|x| x > s.min_examined_per_returned);
        let scan = f.stats.scan_runs >= 5 && f.stats.scan_sessions.len() >= 2;
        let reason = if slow {
            Reason::SlowP95
        } else if heavy {
            Reason::TopTotalTime
        } else if scan {
            Reason::LargeScan
        } else {
            continue;
        };
        out.push((i, reason));
    }
    out
}

/// Whether `c` names a column of `t` (by alias, or unqualified in a
/// one-table statement).
fn on_table(c: &ColumnRef, t: &TableRef, single: bool) -> bool {
    match &c.table {
        Some(a) => a.eq_ignore_ascii_case(&t.alias) || a.eq_ignore_ascii_case(&t.table),
        None => single,
    }
}

fn push_unique(out: &mut Vec<KeyCol>, c: KeyCol) {
    if !out.iter().any(|o| o.column.eq_ignore_ascii_case(&c.column)) {
        out.push(c);
    }
}

/// The candidates for table reference `t` of `shape`, whose definition is
/// `info`. `in_list_max`: the longest `IN` list the pattern ran with.
#[must_use]
pub fn generate(
    shape: &Shape,
    t: &TableRef,
    info: &TableInfo,
    in_list_max: Option<u64>,
) -> Vec<IndexSpec> {
    let single = shape.tables.iter().filter(|x| !x.derived).count() == 1;
    let usable = |name: &str| info.column(name).is_some_and(|c| c.indexable);
    let distinct = |name: &str| info.column(name).and_then(|c| c.distinct).unwrap_or(0.0);
    let mut eq: Vec<String> = Vec::new();
    let mut ranges: Vec<String> = Vec::new();
    for p in &shape.predicates {
        if !on_table(&p.column, t, single) || !usable(&p.column.col) {
            continue;
        }
        let col = p.column.col.clone();
        match &p.op {
            PredOp::Eq | PredOp::IsNull => eq.push(col),
            PredOp::In { list } => {
                if *list && in_list_max.is_some_and(|n| n > IN_EQ_MAX) {
                    ranges.push(col);
                } else {
                    eq.push(col);
                }
            }
            PredOp::Range { .. } | PredOp::Prefix | PredOp::NotNull => ranges.push(col),
            _ => {}
        }
    }
    for j in &shape.joins {
        if j.op != JoinOp::Eq {
            continue;
        }
        for side in [&j.l, &j.r] {
            if on_table(side, t, false) && usable(&side.col) {
                eq.push(side.col.clone());
            }
        }
    }
    eq.sort_by(|a, b| {
        distinct(b)
            .total_cmp(&distinct(a))
            .then_with(|| a.to_lowercase().cmp(&b.to_lowercase()))
    });
    eq.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
    ranges.retain(|r| !eq.iter().any(|e| e.eq_ignore_ascii_case(r)));
    ranges.sort_by(|a, b| distinct(b).total_cmp(&distinct(a)));
    let order: Vec<KeyCol> = if !shape.order_by.is_empty()
        && !shape.order_by_expr
        && shape
            .order_by
            .iter()
            .all(|o| on_table(&o.column, t, single) && usable(&o.column.col))
    {
        let uniform = shape.order_by.iter().all(|o| o.dir == SortDir::Desc)
            || shape.order_by.iter().all(|o| o.dir == SortDir::Asc);
        shape
            .order_by
            .iter()
            .map(|o| KeyCol {
                column: o.column.col.clone(),
                desc: !uniform && o.dir == SortDir::Desc,
            })
            .collect()
    } else {
        Vec::new()
    };
    let group: Vec<KeyCol> = if !shape.group_by.is_empty()
        && !shape.group_by_expr
        && shape
            .group_by
            .iter()
            .all(|g| on_table(g, t, single) && usable(&g.col))
    {
        shape.group_by.iter().map(|g| KeyCol::asc(&g.col)).collect()
    } else {
        Vec::new()
    };
    let r = ranges.first().cloned();
    let eq_cols: Vec<KeyCol> = eq.iter().map(|c| KeyCol::asc(c)).collect();
    let same = |a: &Option<String>, b: Option<&KeyCol>| match (a, b) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(&b.column),
        _ => false,
    };
    let mut shapes: Vec<Vec<KeyCol>> = Vec::new();
    let mut a = eq_cols.clone();
    if let Some(r) = &r {
        push_unique(&mut a, KeyCol::asc(r));
    }
    let tail = if order.is_empty() { &group } else { &order };
    if r.is_none() || same(&r, tail.first()) {
        for c in tail {
            push_unique(&mut a, c.clone());
        }
    }
    shapes.push(a.clone());
    if !order.is_empty() && shape.limit && r.is_some() && !same(&r, order.first()) {
        let mut v = eq_cols.clone();
        for c in &order {
            push_unique(&mut v, c.clone());
        }
        shapes.push(v);
    }
    if !group.is_empty() && r.is_some() && !same(&r, group.first()) {
        let mut v = eq_cols.clone();
        for c in &group {
            push_unique(&mut v, c.clone());
        }
        shapes.push(v);
    }
    // The covering variant: the columns the statement reads from this
    // table that the key lacks (never for writes).
    if shape.stmt_type == StmtType::Select
        && let Some(sel) = shape
            .select
            .get(&t.alias)
            .or_else(|| shape.select.get(&t.table))
        && !sel.iter().any(|c| c == "*")
    {
        let mut need: Vec<String> = sel
            .iter()
            .filter(|c| !a.iter().any(|k| k.column.eq_ignore_ascii_case(c)))
            .filter(|c| !info.pk.iter().any(|p| p.eq_ignore_ascii_case(c)))
            .cloned()
            .collect();
        need.sort();
        if !need.is_empty() && need.len() <= MAX_COVER_COLUMNS && need.iter().all(|c| usable(c)) {
            let mut v = a.clone();
            for c in &need {
                push_unique(&mut v, KeyCol::asc(c));
            }
            shapes.push(v);
        }
    }
    let mut out: Vec<IndexSpec> = Vec::new();
    for mut cols in shapes {
        cols.truncate(MAX_KEY_COLUMNS);
        if cols.is_empty() || info.covered(&cols) {
            continue;
        }
        // Resolve the declared spelling of each column.
        for c in &mut cols {
            if let Some(ci) = info.column(&c.column) {
                c.column.clone_from(&ci.name);
            }
        }
        let spec = IndexSpec {
            db: info.db.clone(),
            table: info.table.clone(),
            cols,
        };
        if !out.contains(&spec) {
            out.push(spec);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use sprig_common::ids::{Fingerprint, TenantId};
    use sprig_workload::fingerprint::FpKind;
    use sprig_workload::recorder::Stats;
    use sprig_workload::shape::{OrderItem, Predicate};

    use super::*;

    pub(crate) fn info() -> TableInfo {
        let col = |n: &str, d: f64| ColumnInfo {
            name: n.into(),
            indexable: n != "body",
            distinct: Some(d),
        };
        TableInfo {
            db: "shop".into(),
            table: "orders".into(),
            columns: vec![
                col("id", 1e6),
                col("customer_id", 1e4),
                col("status", 5.0),
                col("created_at", 9e5),
                col("total", 1e5),
                col("body", 1e6),
            ],
            pk: vec!["id".into()],
            indexes: vec![ExistingIndex {
                name: "k_status".into(),
                cols: vec![KeyCol::asc("status")],
                ignored: false,
            }],
            rows: 1_000_000,
        }
    }

    fn tref() -> TableRef {
        TableRef {
            alias: "orders".into(),
            object_id: None,
            db: Some("shop".into()),
            table: "orders".into(),
            db_id: None,
            derived: false,
        }
    }

    fn pred(col: &str, op: PredOp) -> Predicate {
        Predicate {
            column: ColumnRef {
                table: None,
                col: col.into(),
            },
            op,
            args: vec![0],
        }
    }

    pub(crate) fn shape(preds: Vec<Predicate>) -> Shape {
        let mut s = Shape::empty(StmtType::Select);
        s.tables = vec![tref()];
        s.predicates = preds;
        s
    }

    fn names(v: &[IndexSpec]) -> Vec<String> {
        v.iter().map(IndexSpec::canonical).collect()
    }

    #[test]
    fn equalities_come_first_most_selective_first_then_one_range() {
        let s = shape(vec![
            pred("status", PredOp::Eq),
            pred(
                "created_at",
                PredOp::Range {
                    lo: true,
                    hi: false,
                },
            ),
            pred("customer_id", PredOp::Eq),
            pred("total", PredOp::Range { lo: true, hi: true }),
        ]);
        let c = generate(&s, &tref(), &info(), None);
        assert_eq!(
            names(&c),
            vec!["secondary(customer_id,status,created_at)".to_owned()]
        );
    }

    #[test]
    fn order_by_with_limit_adds_an_ordered_variant() {
        let mut s = shape(vec![
            pred("customer_id", PredOp::Eq),
            pred(
                "total",
                PredOp::Range {
                    lo: true,
                    hi: false,
                },
            ),
        ]);
        s.order_by = vec![OrderItem {
            column: ColumnRef {
                table: None,
                col: "created_at".into(),
            },
            dir: SortDir::Desc,
        }];
        s.limit = true;
        let c = generate(&s, &tref(), &info(), None);
        assert_eq!(
            names(&c),
            vec![
                "secondary(customer_id,total)".to_owned(),
                "secondary(customer_id,created_at)".to_owned()
            ]
        );
    }

    #[test]
    fn covered_unindexable_and_primary_key_columns_get_nothing() {
        // An existing index starts with `status`.
        let c = generate(
            &shape(vec![pred("status", PredOp::Eq)]),
            &tref(),
            &info(),
            None,
        );
        assert!(c.is_empty(), "{c:?}");
        // The primary key serves `id`.
        let c = generate(&shape(vec![pred("id", PredOp::Eq)]), &tref(), &info(), None);
        assert!(c.is_empty(), "{c:?}");
        // A TEXT column can't be a key part.
        let c = generate(
            &shape(vec![pred("body", PredOp::Eq)]),
            &tref(),
            &info(),
            None,
        );
        assert!(c.is_empty(), "{c:?}");
        // Unsargable conditions give nothing.
        let c = generate(
            &shape(vec![pred("total", PredOp::Ne)]),
            &tref(),
            &info(),
            None,
        );
        assert!(c.is_empty(), "{c:?}");
    }

    #[test]
    fn a_short_select_list_gets_a_covering_variant() {
        let mut s = shape(vec![pred("customer_id", PredOp::Eq)]);
        s.select
            .insert("orders".into(), vec!["id".into(), "total".into()]);
        let c = generate(&s, &tref(), &info(), None);
        assert_eq!(
            names(&c),
            vec![
                "secondary(customer_id)".to_owned(),
                "secondary(customer_id,total)".to_owned()
            ]
        );
    }

    #[test]
    fn long_in_lists_are_ranges() {
        let s = shape(vec![
            pred("customer_id", PredOp::In { list: true }),
            pred("total", PredOp::Eq),
        ]);
        let c = generate(&s, &tref(), &info(), Some(100));
        assert_eq!(names(&c), vec!["secondary(total,customer_id)".to_owned()]);
    }

    pub(crate) fn window_fp(
        id: u64,
        runs: u64,
        us_each: u64,
        examined: u64,
        returned: u64,
    ) -> FpWindowStats {
        let mut stats = Stats {
            exec_count: runs,
            total_us: runs * us_each,
            examined_samples: runs,
            rows_examined: runs * examined,
            returned_sampled: runs * returned,
            rows_returned: runs * returned,
            ..Stats::default()
        };
        for _ in 0..runs {
            stats.latency_hist.record(us_each);
        }
        FpWindowStats {
            tenant: TenantId(1),
            id: Fingerprint(id),
            kind: FpKind::Sql,
            text: Arc::from("SELECT * FROM orders WHERE customer_id = ?"),
            shape: Arc::new(shape(vec![pred("customer_id", PredOp::Eq)])),
            caller: None,
            stats,
            samples: Vec::new(),
        }
    }

    #[test]
    fn slow_and_heavy_patterns_qualify() {
        let s = AutoIndexSettings::default();
        let w = vec![
            window_fp(1, 100, 200_000, 1000, 1), // slow
            window_fp(2, 100, 1_000, 10, 10),    // fast and selective
            window_fp(3, 10, 50_000, 50_000, 2), // heavy: few runs, many rows
            window_fp(4, 20, 200_000, 1000, 1),  // slow but too few runs... heavy
        ];
        let q = qualify(&w, &s);
        let got: Vec<(u64, &str)> = q.iter().map(|(i, r)| (w[*i].id.0, r.name())).collect();
        assert_eq!(
            got,
            vec![
                (1, "slow_p95"),
                (3, "top_total_time"),
                (4, "top_total_time")
            ]
        );
    }
}
