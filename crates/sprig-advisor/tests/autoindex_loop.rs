//! The automatic-indexing loop over a simulated tenant, with time
//! compressed: a clock the test moves an hour at a time, a table whose
//! query patterns are slow without an index on their column and fast with
//! one, and a pass every simulated hour.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sprig_advisor::autoindex::candidates::{ColumnInfo, ExistingIndex, TableInfo};
use sprig_advisor::autoindex::model::{
    Action, BuildReport, IndexRecord, IndexSpec, KeyCol, OperatorRequest, State,
};
use sprig_advisor::autoindex::pass::{self, Approval, Env};
use sprig_advisor::autoindex::policy::{DAY_US, HOUR_US, Replay};
use sprig_advisor::autoindex::settings::{AutoIndexSettings, Mode};
use sprig_advisor::autoindex::store::AutoIndexStore;
use sprig_catalog::records::mem::MemRecords;
use sprig_catalog::records::{RecordStore, RecordTx};
use sprig_common::Result;
use sprig_common::ids::{Fingerprint, TenantId};
use sprig_workload::fingerprint::FpKind;
use sprig_workload::recorder::Stats;
use sprig_workload::shape::{ColumnRef, PredOp, Predicate, Shape, StmtType, TableRef};
use sprig_workload::window::FpWindowStats;

const ROWS: u64 = 100_000;
const SLOW_US: u64 = 300_000;
const FAST_US: u64 = 2_000;

/// One simulated hour of one pattern.
#[derive(Clone)]
struct Bucket {
    at: u64,
    fp: u64,
    runs: u64,
    us_each: u64,
    examined: u64,
}

struct Sim {
    now: AtomicU64,
    settings: Mutex<AutoIndexSettings>,
    paused: Mutex<Option<String>>,
    log: Mutex<Vec<Bucket>>,
    indexes: Mutex<Vec<ExistingIndex>>,
    reads: Mutex<HashMap<String, u64>>,
    last_read: Mutex<HashMap<String, u64>>,
    approvals: Mutex<HashMap<String, Approval>>,
    records: MemRecords,
    builds: Mutex<Vec<(u64, String)>>,
    drops: Mutex<Vec<(u64, String, String)>>,
}

/// Pattern `fp` filters on column `col` of `t`.
fn column_of(fp: u64) -> &'static str {
    match fp {
        1 => "a",
        2 => "b",
        3 => "c",
        4 => "d",
        5 => "e",
        6 => "f",
        _ => "g",
    }
}

fn shape_of(fp: u64) -> Shape {
    let mut s = Shape::empty(StmtType::Select);
    s.tables = vec![TableRef {
        alias: "t".into(),
        object_id: None,
        db: Some("app".into()),
        table: "t".into(),
        db_id: None,
        derived: false,
    }];
    s.predicates = vec![Predicate {
        column: ColumnRef {
            table: None,
            col: column_of(fp).into(),
        },
        op: PredOp::Eq,
        args: vec![0],
    }];
    s
}

impl Sim {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: AtomicU64::new(1_000 * DAY_US),
            settings: Mutex::new(AutoIndexSettings::default()),
            paused: Mutex::new(None),
            log: Mutex::new(Vec::new()),
            indexes: Mutex::new(Vec::new()),
            reads: Mutex::new(HashMap::new()),
            last_read: Mutex::new(HashMap::new()),
            approvals: Mutex::new(HashMap::new()),
            records: MemRecords::new(),
            builds: Mutex::new(Vec::new()),
            drops: Mutex::new(Vec::new()),
        })
    }

    /// The visible index whose first column is `col`.
    fn index_on(&self, col: &str) -> Option<String> {
        self.indexes
            .lock()
            .unwrap()
            .iter()
            .find(|x| !x.ignored && x.cols[0].column == col)
            .map(|x| x.name.clone())
    }

    /// Runs `runs` statements of each pattern in `fps` during the hour
    /// that ends now.
    fn hour(&self, fps: &[u64], runs: u64) {
        let at = self.now.load(Ordering::SeqCst);
        for &fp in fps {
            let idx = self.index_on(column_of(fp));
            let (us, examined) = match &idx {
                Some(_) => (FAST_US, 100),
                None => (SLOW_US, ROWS),
            };
            if let Some(name) = idx {
                if runs > 0 {
                    self.last_read.lock().unwrap().insert(name.clone(), at);
                }
                *self.reads.lock().unwrap().entry(name).or_default() += runs;
            }
            self.log.lock().unwrap().push(Bucket {
                at,
                fp,
                runs,
                us_each: us,
                examined,
            });
        }
    }

    fn advance(&self, us: u64) {
        self.now.fetch_add(us, Ordering::SeqCst);
    }

    /// The window's p95 of `fp`, ms (0 without runs).
    fn p95_now(&self, fp: u64) -> f64 {
        let now = self.now.load(Ordering::SeqCst);
        let w = self.window_of(now - HOUR_US, now + 1);
        w.iter()
            .find(|f| f.id.0 == fp)
            .map_or(0.0, FpWindowStats::p95_ms)
    }

    fn window_of(&self, from: u64, to: u64) -> Vec<FpWindowStats> {
        let mut by_fp: HashMap<u64, Stats> = HashMap::new();
        for b in self
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.at >= from && b.at < to)
        {
            let s = by_fp.entry(b.fp).or_default();
            s.exec_count += b.runs;
            s.total_us += b.runs * b.us_each;
            s.examined_samples += b.runs;
            s.rows_examined += b.runs * b.examined;
            s.returned_sampled += b.runs;
            s.rows_returned += b.runs;
            for _ in 0..b.runs.min(200) {
                s.latency_hist.record(b.us_each);
            }
        }
        by_fp
            .into_iter()
            .map(|(fp, stats)| FpWindowStats {
                tenant: TenantId(1),
                id: Fingerprint(fp),
                kind: FpKind::Sql,
                text: Arc::from(format!("SELECT * FROM t WHERE {} = ?", column_of(fp))),
                shape: Arc::new(shape_of(fp)),
                caller: None,
                stats,
                samples: Vec::new(),
            })
            .collect()
    }

    fn builds_of(&self, col: &str) -> usize {
        let name = spec(col).name();
        self.builds
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, n)| *n == name)
            .count()
    }

    async fn record(&self, col: &str) -> Option<IndexRecord> {
        let mut tx = self.records.begin(TenantId(1)).await.unwrap();
        AutoIndexStore::index(&mut *tx, &spec(col).name())
            .await
            .unwrap()
    }
}

fn spec(col: &str) -> IndexSpec {
    IndexSpec {
        db: "app".into(),
        table: "t".into(),
        cols: vec![KeyCol::asc(col)],
    }
}

struct SimEnv(Arc<Sim>);

#[async_trait::async_trait]
impl Env for SimEnv {
    fn now_us(&self) -> u64 {
        self.0.now.load(Ordering::SeqCst)
    }

    fn settings(&self) -> AutoIndexSettings {
        self.0.settings.lock().unwrap().clone()
    }

    fn paused(&self) -> Option<String> {
        self.0.paused.lock().unwrap().clone()
    }

    async fn window(&self, from_us: u64, to_us: u64) -> Result<Vec<FpWindowStats>> {
        Ok(self.0.window_of(from_us, to_us + 1))
    }

    async fn table(&self, db: &str, table: &str) -> Result<Option<TableInfo>> {
        if db != "app" || table != "t" {
            return Ok(None);
        }
        let col = |n: &str| ColumnInfo {
            name: n.into(),
            indexable: true,
            distinct: Some(10_000.0),
        };
        Ok(Some(TableInfo {
            db: "app".into(),
            table: "t".into(),
            columns: ["id", "a", "b", "c", "d", "e", "f", "g"]
                .iter()
                .map(|n| col(n))
                .collect(),
            pk: vec!["id".into()],
            indexes: self.0.indexes.lock().unwrap().clone(),
            rows: ROWS,
        }))
    }

    async fn replay(
        &self,
        fp: &FpWindowStats,
        spec: &IndexSpec,
        max: usize,
    ) -> Result<Vec<Replay>> {
        let hit = spec.cols[0].column == column_of(fp.id.0);
        Ok((0..max.min(5))
            .map(|_| Replay {
                before: ROWS as f64,
                after: if hit { 10.0 } else { ROWS as f64 },
                reads: hit,
            })
            .collect())
    }

    async fn build(&self, spec: &IndexSpec, _s: &AutoIndexSettings) -> Result<BuildReport> {
        let now = self.now_us();
        self.0.builds.lock().unwrap().push((now, spec.name()));
        self.0.indexes.lock().unwrap().push(ExistingIndex {
            name: spec.name(),
            cols: spec.cols.clone(),
            ignored: false,
        });
        Ok(BuildReport {
            states: ["absent", "delete_only", "write_only", "backfill", "public"]
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
            rows: ROWS,
            ms: 10,
            batches: ROWS / 1024 + 1,
        })
    }

    async fn drop_index(&self, spec: &IndexSpec, name: &str) -> Result<()> {
        assert!(
            name.starts_with("sprig_auto_"),
            "only automatic indexes are dropped"
        );
        self.0.indexes.lock().unwrap().retain(|x| x.name != name);
        self.0
            .drops
            .lock()
            .unwrap()
            .push((self.now_us(), name.to_owned(), spec.canonical()));
        Ok(())
    }

    fn reads(&self, _spec: &IndexSpec, name: &str) -> u64 {
        self.0.reads.lock().unwrap().get(name).copied().unwrap_or(0)
    }

    fn last_read_us(&self, _spec: &IndexSpec, name: &str) -> Option<u64> {
        self.0.last_read.lock().unwrap().get(name).copied()
    }

    async fn auto_indexes(&self) -> Result<Vec<(IndexSpec, String)>> {
        Ok(self
            .0
            .indexes
            .lock()
            .unwrap()
            .iter()
            .filter(|x| x.name.starts_with("sprig_auto_"))
            .map(|x| {
                (
                    IndexSpec {
                        db: "app".into(),
                        table: "t".into(),
                        cols: x.cols.clone(),
                    },
                    x.name.clone(),
                )
            })
            .collect())
    }

    async fn propose(&self, rec: &IndexRecord) -> Result<String> {
        let id = format!("ap-{}", rec.name);
        self.0
            .approvals
            .lock()
            .unwrap()
            .insert(id.clone(), Approval::Pending);
        Ok(id)
    }

    async fn approval(&self, id: &str) -> Result<Approval> {
        Ok(self
            .0
            .approvals
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .unwrap_or(Approval::Pending))
    }

    async fn records(&self) -> Result<Box<dyn RecordTx>> {
        self.0.records.begin(TenantId(1)).await
    }
}

/// One simulated hour: the workload, then a pass.
async fn tick(sim: &Arc<Sim>, fps: &[u64], runs: u64) -> pass::PassReport {
    sim.advance(HOUR_US);
    sim.hour(fps, runs);
    pass::run(&SimEnv(Arc::clone(sim))).await.unwrap()
}

#[tokio::test]
async fn a_missing_index_is_built_latency_drops_and_an_unused_one_is_dropped() {
    let sim = Sim::new();
    // Hour 1: pattern 1 (on `a`) runs 100 times at 300 ms: slow.
    let r = tick(&sim, &[1], 100).await;
    assert_eq!(r.qualified, 1);
    assert_eq!(r.built, vec![spec("a").name()], "{r:?}");
    assert!(sim.p95_now(1) > 100.0);
    let rec = sim.record("a").await.unwrap();
    assert_eq!(rec.state, State::Observing);
    assert_eq!(rec.origin, "auto");
    assert_eq!(rec.build.as_ref().unwrap().states.len(), 5);
    // The next hours run on the index: fast.
    for _ in 0..48 {
        let r = tick(&sim, &[1], 100).await;
        assert!(r.built.is_empty(), "{r:?}");
    }
    assert!(sim.p95_now(1) < 10.0, "p95 {}", sim.p95_now(1));
    // From day 3 the application stops running pattern 1 and runs pattern
    // 2 (on `b`), which gets its own index.
    let mut b_built_at = None;
    for h in 0..24 * 12 {
        let r = tick(&sim, &[2], 100).await;
        if r.built.contains(&spec("b").name()) {
            b_built_at = Some(h);
        }
    }
    assert_eq!(b_built_at, Some(0));
    // `a` stayed through its observation (it was read and faster), then went
    // unread for observe_days: dropped as unused. Nothing else dropped.
    let a = sim.record("a").await.unwrap();
    assert_eq!(a.state, State::Dropped);
    assert_eq!(a.end_reason.as_deref(), Some("unused"));
    let drops = sim.drops.lock().unwrap().clone();
    assert_eq!(drops.len(), 1, "{drops:?}");
    let dropped_at = drops[0].0;
    let last_read = 1_000 * DAY_US + 49 * HOUR_US;
    let idle = dropped_at - last_read;
    assert!(
        (7 * DAY_US..7 * DAY_US + 2 * HOUR_US).contains(&idle),
        "idle {idle}"
    );
    assert!(sim.index_on("a").is_none());
    assert_eq!(sim.record("b").await.unwrap().state, State::Active);
    // The events say what happened.
    let mut tx = sim.records.begin(TenantId(1)).await.unwrap();
    let kinds: Vec<String> = AutoIndexStore::events(&mut *tx, 0, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.kind)
        .collect();
    for k in ["candidate", "built", "observed", "dropped"] {
        assert!(kinds.iter().any(|x| x == k), "{k} missing from {kinds:?}");
    }
}

#[tokio::test]
async fn an_index_never_read_is_dropped_when_its_observation_ends() {
    let sim = Sim::new();
    let r = tick(&sim, &[3], 100).await;
    assert_eq!(r.built.len(), 1);
    // The pattern stops at once: nothing reads the index.
    let mut dropped = None;
    for h in 1..=24 * 8 {
        let r = tick(&sim, &[], 0).await;
        if !r.dropped.is_empty() {
            dropped = Some((h, r.dropped[0].1.clone()));
        }
    }
    assert_eq!(dropped, Some((24 * 7, "unused".to_owned())));
}

#[tokio::test]
async fn a_flip_flopping_workload_does_not_thrash() {
    let sim = Sim::new();
    // Patterns 1 and 2 take turns for 9 days each, for 60 days.
    for day in 0..60u64 {
        let fp = if (day / 9) % 2 == 0 { 1 } else { 2 };
        for _ in 0..24 {
            tick(&sim, &[fp], 100).await;
        }
    }
    let builds = sim.builds.lock().unwrap().clone();
    let drops = sim.drops.lock().unwrap().clone();
    // No definition is built again within recreate_block_days (14) of its
    // drop, and none more than twice in 60 days.
    for col in ["a", "b"] {
        let name = spec(col).name();
        assert!(sim.builds_of(col) <= 3, "{col}: {builds:?}");
        for (at, _, _) in drops.iter().filter(|d| d.1 == name) {
            let again = builds.iter().find(|(b, bn)| *bn == name && b > at);
            if let Some((b, _)) = again {
                assert!(
                    b - at >= 14 * DAY_US,
                    "{col} rebuilt {} h after its drop",
                    (b - at) / HOUR_US
                );
            }
        }
    }
    // Some churn is expected (each pattern goes quiet for 9 days), but it is
    // bounded: at most one build and one drop per definition per 18 days.
    assert!(builds.len() <= 7, "{builds:?}");
    assert!(drops.len() <= 6, "{drops:?}");
    // A pattern alternating every 12 hours keeps both indexes: no drops.
    let sim = Sim::new();
    for h in 0..24 * 30u64 {
        let fp = if (h / 12) % 2 == 0 { 1 } else { 2 };
        tick(&sim, &[fp], 100).await;
    }
    assert_eq!(sim.builds.lock().unwrap().len(), 2);
    assert!(sim.drops.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dampers_hold_builds_back() {
    let sim = Sim::new();
    {
        let mut s = sim.settings.lock().unwrap();
        s.max_new_per_day = 3;
        s.max_per_table = 10;
    }
    // Six slow patterns at once: three built, three held by the budget.
    let r = tick(&sim, &[1, 2, 3, 4, 5, 6], 100).await;
    assert_eq!(r.built.len(), 3, "{r:?}");
    assert_eq!(
        r.deferred.iter().filter(|d| d.1 == "budget").count(),
        3,
        "{r:?}"
    );
    // A day later the rest are built.
    for _ in 0..23 {
        tick(&sim, &[1, 2, 3, 4, 5, 6], 100).await;
    }
    let r = tick(&sim, &[1, 2, 3, 4, 5, 6], 100).await;
    assert_eq!(r.built.len(), 3, "{r:?}");
    // While units move, nothing is built.
    let sim = Sim::new();
    *sim.paused.lock().unwrap() = Some("a unit is moving".into());
    let r = tick(&sim, &[1], 100).await;
    assert!(r.built.is_empty());
    assert_eq!(r.deferred[0].1, "paused");
    // A pause only holds the build back: built once resumed.
    *sim.paused.lock().unwrap() = None;
    let r = tick(&sim, &[1], 100).await;
    assert_eq!(r.built, vec![spec("a").name()], "{r:?}");
    assert_eq!(sim.builds_of("a"), 1);
    // Disabled: nothing at all.
    let sim = Sim::new();
    sim.settings.lock().unwrap().enabled = false;
    let r = tick(&sim, &[1], 100).await;
    assert!(r.disabled);
    assert!(sim.builds.lock().unwrap().is_empty());
}

#[tokio::test]
async fn recommend_mode_waits_for_approval() {
    let sim = Sim::new();
    sim.settings.lock().unwrap().mode = Mode::Recommend;
    let r = tick(&sim, &[1], 100).await;
    assert_eq!(r.recommended, vec![spec("a").name()]);
    assert!(sim.builds.lock().unwrap().is_empty());
    let rec = sim.record("a").await.unwrap();
    assert_eq!(rec.state, State::Recommended);
    let id = rec.approval.clone().unwrap();
    tick(&sim, &[1], 100).await;
    assert!(sim.builds.lock().unwrap().is_empty());
    sim.approvals.lock().unwrap().insert(id, Approval::Approved);
    let r = tick(&sim, &[1], 100).await;
    assert_eq!(r.built, vec![spec("a").name()]);
    assert_eq!(sim.record("a").await.unwrap().state, State::Observing);
    // A refused recommendation is rejected and blocks its definition.
    let sim = Sim::new();
    sim.settings.lock().unwrap().mode = Mode::Recommend;
    tick(&sim, &[2], 100).await;
    let id = sim.record("b").await.unwrap().approval.unwrap();
    sim.approvals.lock().unwrap().insert(id, Approval::Rejected);
    tick(&sim, &[2], 100).await;
    assert_eq!(sim.record("b").await.unwrap().state, State::Rejected);
    for _ in 0..48 {
        let r = tick(&sim, &[2], 100).await;
        assert!(r.recommended.is_empty(), "{r:?}");
    }
}

async fn request(sim: &Arc<Sim>, col: &str, action: Action) {
    let mut tx = sim.records.begin(TenantId(1)).await.unwrap();
    AutoIndexStore::put_request(
        &mut *tx,
        &OperatorRequest {
            index: spec(col).name().to_uppercase(),
            action,
            actor: "op:test".into(),
            reason: "testing".into(),
            at_us: sim.now.load(Ordering::SeqCst),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn usage_reaches_the_records_between_passes_and_survives_a_restart() {
    let sim = Sim::new();
    tick(&sim, &[1], 100).await;
    let name = spec("a").name();
    // Half an hour of reads, then the minute's flush: in the record, with
    // the time of the last read, before any pass.
    sim.advance(HOUR_US / 2);
    sim.hour(&[1], 40);
    let read_at = sim.now.load(Ordering::SeqCst);
    sim.advance(60_000_000);
    assert_eq!(
        pass::note_usage(&SimEnv(Arc::clone(&sim))).await.unwrap(),
        1
    );
    let r = sim.record("a").await.unwrap();
    assert_eq!(r.reads_total, 40);
    assert_eq!(r.last_used_us, Some(read_at));
    // Nothing new: nothing written.
    assert_eq!(
        pass::note_usage(&SimEnv(Arc::clone(&sim))).await.unwrap(),
        0
    );
    // The node restarts: its in-memory counts start again from zero, and
    // what was flushed stays.
    sim.reads.lock().unwrap().clear();
    sim.hour(&[1], 5);
    pass::note_usage(&SimEnv(Arc::clone(&sim))).await.unwrap();
    let r = sim.record("a").await.unwrap();
    assert_eq!(r.reads_total, 45);
    assert_eq!(sim.reads.lock().unwrap().get(&name).copied(), Some(5));
}

#[tokio::test]
async fn operators_drop_pin_and_restore() {
    let sim = Sim::new();
    tick(&sim, &[1, 2], 100).await;
    assert!(sim.index_on("a").is_some() && sim.index_on("b").is_some());
    let env = SimEnv(Arc::clone(&sim));
    // Drop `a`: gone at once, and blocked like a refused definition.
    request(&sim, "a", Action::Drop).await;
    let done = pass::apply_requests(&env).await.unwrap();
    assert_eq!(done[0].outcome, "done", "{done:?}");
    assert!(sim.index_on("a").is_none());
    let a = sim.record("a").await.unwrap();
    assert_eq!(a.state, State::Dropped);
    assert_eq!(a.end_reason.as_deref(), Some("operator"));
    assert!(pass::apply_requests(&env).await.unwrap().is_empty());
    for _ in 0..30 {
        let r = tick(&sim, &[1, 2], 100).await;
        assert!(r.built.is_empty(), "{r:?}");
    }
    // A second drop is refused by the state; the request still goes.
    request(&sim, "a", Action::Drop).await;
    let done = pass::apply_requests(&env).await.unwrap();
    assert!(done[0].outcome.contains("dropped"), "{done:?}");
    // Restore `a`: built again at once, pinned, kept while unread.
    request(&sim, "a", Action::Restore).await;
    let done = pass::apply_requests(&env).await.unwrap();
    assert_eq!(done[0].outcome, "done", "{done:?}");
    assert!(sim.index_on("a").is_some());
    let a = sim.record("a").await.unwrap();
    assert!(a.pinned);
    assert_eq!(a.state, State::Observing);
    // Pin `b` too; nothing runs for 20 days: both are kept.
    request(&sim, "b", Action::Pin).await;
    pass::apply_requests(&env).await.unwrap();
    for _ in 0..24 * 20 {
        let r = tick(&sim, &[], 0).await;
        assert!(r.dropped.is_empty(), "{r:?}");
    }
    assert_eq!(sim.record("a").await.unwrap().state, State::Active);
    // Unpinned, `b` is judged again: unread for 7 days, dropped.
    request(&sim, "b", Action::Unpin).await;
    pass::apply_requests(&env).await.unwrap();
    let r = tick(&sim, &[], 0).await;
    assert_eq!(r.dropped, vec![(spec("b").name(), "unused".to_owned())]);
    // A restore waits while builds must wait.
    request(&sim, "b", Action::Restore).await;
    *sim.paused.lock().unwrap() = Some("failover".into());
    assert_eq!(
        pass::apply_requests(&env).await.unwrap()[0].outcome,
        "waiting"
    );
    *sim.paused.lock().unwrap() = None;
    assert_eq!(pass::apply_requests(&env).await.unwrap()[0].outcome, "done");
    assert!(sim.index_on("b").is_some());
    // Unknown names are refused.
    let mut tx = sim.records.begin(TenantId(1)).await.unwrap();
    AutoIndexStore::put_request(
        &mut *tx,
        &OperatorRequest {
            index: "sprig_auto_ffffffff".into(),
            action: Action::Pin,
            actor: String::new(),
            reason: String::new(),
            at_us: 0,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        pass::apply_requests(&env).await.unwrap()[0].outcome,
        "no_such_index"
    );
}

#[tokio::test]
async fn an_automatic_index_no_record_explains_is_dropped() {
    let sim = Sim::new();
    tick(&sim, &[1], 100).await;
    sim.indexes.lock().unwrap().push(ExistingIndex {
        name: "sprig_auto_deadbeef".into(),
        cols: vec![KeyCol::asc("g")],
        ignored: false,
    });
    sim.indexes.lock().unwrap().push(ExistingIndex {
        name: "k_user".into(),
        cols: vec![KeyCol::asc("f")],
        ignored: false,
    });
    let r = tick(&sim, &[1], 100).await;
    assert_eq!(
        r.dropped,
        vec![("sprig_auto_deadbeef".to_owned(), "unexplained".to_owned())]
    );
    let names: Vec<String> = sim
        .indexes
        .lock()
        .unwrap()
        .iter()
        .map(|x| x.name.clone())
        .collect();
    assert_eq!(names, vec![spec("a").name(), "k_user".to_owned()]);
}
