//! One evaluation of one tenant: watch, trigger, generate, validate,
//! cost, damp, act. Everything outside the decision logic goes through
//! [`Env`], which the gateway hosting the tenant's engine implements (and
//! tests fake, with a clock they move).
//!
//! ```text
//! run(env):
//!   off (auto_index.enabled = false): nothing
//!   watch    every built index: reads counted since the last pass; the
//!            watch rule; drop -> Env::drop_index, record dropped
//!   approve  every recommendation: approved -> build; rejected or past
//!            recommend_ttl_days -> rejected
//!   resume   a record left `building` by a crash: built again (the build
//!            picks up an index already added)
//!   decide   for each qualifying pattern not cooling down, most total
//!            time first: candidates, each replayed (Env::replay) and
//!            judged; the best accepted one per pattern (most time saved,
//!            then fewer columns); a shorter one merged into a longer one
//!            on the same table; the pattern's cooldown starts
//!   act      cost rule, then the dampers; mode auto -> build now
//!            (one at a time), mode recommend -> an approval request
//! ```
//!
//! Every change of a record and every decision is written at once, in a
//! transaction of its own, so a pass cut short leaves records that the
//! next pass continues from.

use std::collections::HashMap;

use sprig_catalog::records::RecordTx;
use sprig_common::Result;
use sprig_workload::shape::StmtType;
use sprig_workload::window::FpWindowStats;

use super::candidates::{self, Reason, TableInfo};
use super::model::{Baseline, BuildReport, Event, IndexRecord, IndexSpec, State};
use super::policy::{self, DAY_US, Dampers, HOUR_US, Replay, Verdict, Watch};
use super::settings::{AutoIndexSettings, Mode};
use super::store::AutoIndexStore;

/// An approval request's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Approval {
    /// Not answered yet.
    Pending,
    /// Approved: build it.
    Approved,
    /// Refused.
    Rejected,
}

/// What a pass needs from the node.
#[async_trait::async_trait]
pub trait Env: Send + Sync {
    /// Now, µs since the epoch.
    fn now_us(&self) -> u64;
    /// The tenant's `auto_index.*` values.
    fn settings(&self) -> AutoIndexSettings;
    /// Why builds must wait (units moving, a failover), if they must.
    fn paused(&self) -> Option<String>;
    /// The workload window `[from, to)`.
    async fn window(&self, from_us: u64, to_us: u64) -> Result<Vec<FpWindowStats>>;
    /// A table's definition and figures; `None` when it is gone.
    async fn table(&self, db: &str, table: &str) -> Result<Option<TableInfo>>;
    /// Up to `max` recent statements of `fp`, planned without and with
    /// `spec` (a hypothetical index).
    async fn replay(&self, fp: &FpWindowStats, spec: &IndexSpec, max: usize)
    -> Result<Vec<Replay>>;
    /// Builds `spec` online, as an automatic index (origin `auto`).
    async fn build(&self, spec: &IndexSpec, s: &AutoIndexSettings) -> Result<BuildReport>;
    /// Drops automatic index `name` of `spec`'s table.
    async fn drop_index(&self, spec: &IndexSpec, name: &str) -> Result<()>;
    /// Plans that read index `name` of `spec`'s table, as counted by this
    /// node since it started.
    fn reads(&self, spec: &IndexSpec, name: &str) -> u64;
    /// Sends `rec` to the approvals inbox: the request's id.
    async fn propose(&self, rec: &IndexRecord) -> Result<String>;
    /// The answer to approval request `id`.
    async fn approval(&self, id: &str) -> Result<Approval>;
    /// A transaction on the tenant's records.
    async fn records(&self) -> Result<Box<dyn RecordTx>>;
}

/// What a pass did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PassReport {
    /// The tenant's loop is off.
    pub disabled: bool,
    /// Patterns that qualified.
    pub qualified: usize,
    /// Candidates replayed.
    pub candidates: usize,
    /// Indexes built.
    pub built: Vec<String>,
    /// Indexes sent for approval.
    pub recommended: Vec<String>,
    /// Indexes dropped, with why.
    pub dropped: Vec<(String, String)>,
    /// Builds held back, with why.
    pub deferred: Vec<(String, String)>,
    /// Candidates refused, with why.
    pub rejected: Vec<(String, String)>,
    /// Indexes whose observation ended with them kept.
    pub activated: Vec<String>,
}

struct Recorder<'a> {
    env: &'a dyn Env,
    seq: u32,
}

impl Recorder<'_> {
    async fn save(&mut self, r: &IndexRecord) -> Result<()> {
        let mut tx = self.env.records().await?;
        AutoIndexStore::put_index(&mut *tx, r).await?;
        tx.commit().await
    }

    async fn event(
        &mut self,
        kind: &str,
        spec: &IndexSpec,
        fps: &[u64],
        reason: &str,
        est: Option<(f64, f64)>,
    ) -> Result<()> {
        let e = Event {
            at_us: self.env.now_us(),
            kind: kind.into(),
            index: spec.name(),
            def: spec.canonical(),
            table: spec.table_key(),
            fingerprints: fps.to_vec(),
            reason: reason.into(),
            est,
        };
        self.seq += 1;
        let mut tx = self.env.records().await?;
        AutoIndexStore::add_event(&mut *tx, &e, self.seq).await?;
        tx.commit().await
    }

    async fn cool(&mut self, fp: u64) -> Result<()> {
        let mut tx = self.env.records().await?;
        AutoIndexStore::set_cooldown(&mut *tx, fp, self.env.now_us()).await?;
        tx.commit().await
    }
}

fn baseline(f: &FpWindowStats) -> Baseline {
    Baseline {
        runs: f.runs(),
        p95_ms: f.p95_ms(),
        mean_ms: f.mean_ms(),
    }
}

/// One accepted candidate.
#[derive(Debug, Clone)]
struct Chosen {
    spec: IndexSpec,
    fps: Vec<u64>,
    baseline: Vec<(u64, Baseline)>,
    before: f64,
    after: f64,
    benefit: f64,
    reason: Reason,
}

/// Rows written per hour to `table_key` over the window.
fn writes_per_hour(window: &[FpWindowStats], table_key: &str, hours: f64) -> f64 {
    window
        .iter()
        .filter(|f| {
            matches!(
                f.shape.stmt_type,
                StmtType::Insert | StmtType::Replace | StmtType::Update | StmtType::Delete
            )
        })
        .filter(|f| {
            f.shape.tables.iter().any(|t| {
                t.db.as_deref()
                    .is_some_and(|d| format!("{}.{}", d, t.table).to_lowercase() == table_key)
            })
        })
        .map(|f| (f.stats.rows_affected.max(f.runs())) as f64)
        .sum::<f64>()
        / hours.max(1e-9)
}

/// Runs one evaluation of the tenant behind `env`.
///
/// # Errors
/// The records' and the environment's errors; a pass stops at the first.
#[allow(clippy::too_many_lines)] // the steps of one pass, in order
pub async fn run(env: &dyn Env) -> Result<PassReport> {
    let s = env.settings();
    let mut report = PassReport::default();
    if !s.enabled {
        report.disabled = true;
        return Ok(report);
    }
    let now = env.now_us();
    let mut rec = Recorder { env, seq: 0 };
    let mut records = {
        let mut tx = env.records().await?;
        let r = AutoIndexStore::indexes(&mut *tx).await?;
        AutoIndexStore::purge_events(&mut *tx, now).await?;
        tx.commit().await?;
        r
    };
    let window_us = u64::from(s.window_hours) * HOUR_US;
    let window = env.window(now.saturating_sub(window_us), now).await?;
    let hours = f64::from(s.window_hours);
    let mut tables: HashMap<String, Option<TableInfo>> = HashMap::new();
    let current: Vec<(u64, Baseline)> = window.iter().map(|f| (f.id.0, baseline(f))).collect();

    // 1. Watch.
    for r in records.iter_mut().filter(|r| r.state.built()) {
        let key = r.spec.table_key();
        if !tables.contains_key(&key) {
            let info = env.table(&r.spec.db, &r.spec.table).await?;
            tables.insert(key.clone(), info);
        }
        let present = tables[&key].as_ref().is_some_and(|t| {
            t.indexes
                .iter()
                .any(|x| x.name.eq_ignore_ascii_case(&r.name))
        });
        if !present {
            r.state = State::Dropped;
            r.changed_us = now;
            r.end_reason = Some("index_gone".into());
            rec.save(r).await?;
            rec.event("dropped", &r.spec, &r.serves, "index_gone", None)
                .await?;
            report.dropped.push((r.name.clone(), "index_gone".into()));
            continue;
        }
        let reads = env.reads(&r.spec, &r.name);
        let fresh = if reads >= r.reads_seen {
            reads - r.reads_seen
        } else {
            // The node restarted: its count started again from zero.
            reads
        };
        if fresh > 0 {
            r.last_used_us = Some(now);
            r.reads_total += fresh;
        }
        r.reads_seen = reads;
        match policy::watch(r, now, &current, &s) {
            Watch::Keep => {
                if fresh > 0 {
                    rec.save(r).await?;
                }
            }
            Watch::Activate => {
                r.state = State::Active;
                r.changed_us = now;
                rec.save(r).await?;
                rec.event(
                    "observed",
                    &r.spec,
                    &r.serves,
                    "kept: read and faster",
                    None,
                )
                .await?;
                report.activated.push(r.name.clone());
            }
            Watch::Drop(why) => {
                env.drop_index(&r.spec, &r.name).await?;
                r.state = State::Dropped;
                r.changed_us = now;
                r.end_reason = Some(why.into());
                rec.save(r).await?;
                rec.event("dropped", &r.spec, &r.serves, why, None).await?;
                report.dropped.push((r.name.clone(), why.into()));
            }
        }
    }

    // 2. Recommendations: approved, refused or expired.
    let ttl = u64::from(s.recommend_ttl_days) * DAY_US;
    for i in 0..records.len() {
        if records[i].state != State::Recommended {
            continue;
        }
        let answer = match &records[i].approval {
            Some(id) => env.approval(id).await?,
            None => Approval::Pending,
        };
        match answer {
            Approval::Approved => {
                let paused = env.paused();
                let d = Dampers {
                    now_us: now,
                    records: &records,
                    paused: paused.as_deref(),
                };
                // The recommendation itself is the table's; count the others.
                if let Err(why) = d.may_build(
                    "",
                    &AutoIndexSettings {
                        max_per_table: u32::MAX,
                        ..s.clone()
                    },
                ) {
                    report.deferred.push((records[i].name.clone(), why.into()));
                    continue;
                }
                rec.event(
                    "approved",
                    &records[i].spec,
                    &records[i].serves,
                    "approved",
                    None,
                )
                .await?;
                let mut r = records[i].clone();
                build(env, &mut rec, &mut r, &s, &mut report).await?;
                records[i] = r;
            }
            Approval::Rejected => {
                let r = &mut records[i];
                r.state = State::Rejected;
                r.changed_us = now;
                r.end_reason = Some("rejected".into());
                rec.save(r).await?;
                rec.event("rejected", &r.spec, &r.serves, "approval refused", None)
                    .await?;
            }
            Approval::Pending if now.saturating_sub(records[i].created_us) >= ttl => {
                let r = &mut records[i];
                r.state = State::Rejected;
                r.changed_us = now;
                r.end_reason = Some("expired".into());
                rec.save(r).await?;
                rec.event("rejected", &r.spec, &r.serves, "expired", None)
                    .await?;
            }
            Approval::Pending => {}
        }
    }

    // 3. A build a crash cut short.
    for i in 0..records.len() {
        if records[i].state == State::Building {
            let mut r = records[i].clone();
            build(env, &mut rec, &mut r, &s, &mut report).await?;
            records[i] = r;
        }
    }

    // 4. Decide.
    let qualified = candidates::qualify(&window, &s);
    report.qualified = qualified.len();
    let mut order: Vec<(usize, Reason)> = qualified;
    order.sort_by(|a, b| window[b.0].total_ms().total_cmp(&window[a.0].total_ms()));
    let mut chosen: Vec<Chosen> = Vec::new();
    for (i, reason) in order {
        let f = &window[i];
        let last = {
            let mut tx = env.records().await?;
            let at = AutoIndexStore::cooldown(&mut *tx, f.id.0).await?;
            tx.commit().await?;
            at
        };
        let d = Dampers {
            now_us: now,
            records: &records,
            paused: None,
        };
        if d.cooling(f.id.0, last, &s) {
            continue;
        }
        let mut best: Option<Chosen> = None;
        let mut tried = false;
        for t in f.shape.tables.iter().filter(|t| !t.derived) {
            let Some(db) = t.db.clone() else { continue };
            let key = format!("{db}.{}", t.table).to_lowercase();
            if !tables.contains_key(&key) {
                let info = env.table(&db, &t.table).await?;
                tables.insert(key.clone(), info);
            }
            let Some(info) = tables[&key].clone() else {
                continue;
            };
            for spec in candidates::generate(&f.shape, t, &info, f.in_list_max()) {
                let name = spec.name();
                if records
                    .iter()
                    .any(|r| !r.state.terminal() && (r.name == name || r.spec.extends(&spec)))
                {
                    continue;
                }
                if d.blocked(spec.def_hash(), &spec.table_key(), &s) {
                    report.rejected.push((name, "blocked".into()));
                    continue;
                }
                tried = true;
                report.candidates += 1;
                let replays = env.replay(f, &spec, s.replay_samples as usize).await?;
                match policy::verdict(&replays, &s) {
                    Verdict::Reject(why) => {
                        rec.event("rejected_candidate", &spec, &[f.id.0], why, None)
                            .await?;
                        report.rejected.push((name, why.into()));
                    }
                    Verdict::Accept { before, after } => {
                        let benefit = policy::benefit_ms_per_hour(
                            f.runs() as f64 / hours,
                            f.mean_ms(),
                            before,
                            after,
                        );
                        let c = Chosen {
                            spec,
                            fps: vec![f.id.0],
                            baseline: vec![(f.id.0, baseline(f))],
                            before,
                            after,
                            benefit,
                            reason,
                        };
                        let better = best.as_ref().is_none_or(|b| {
                            c.benefit > b.benefit * 1.05
                                || (c.benefit >= b.benefit * 0.95
                                    && c.spec.cols.len() < b.spec.cols.len())
                        });
                        if better {
                            best = Some(c);
                        }
                    }
                }
            }
        }
        let Some(c) = best else {
            // Every candidate refused: the pattern is decided.
            if tried {
                rec.cool(f.id.0).await?;
            }
            continue;
        };
        // Merge with an accepted candidate on the same table that one of
        // the two extends.
        if let Some(o) = chosen
            .iter_mut()
            .find(|o| o.spec.extends(&c.spec) || c.spec.extends(&o.spec))
        {
            if c.spec.extends(&o.spec) && c.spec != o.spec {
                o.spec = c.spec.clone();
            }
            o.fps.extend(c.fps);
            o.baseline.extend(c.baseline);
            o.benefit += c.benefit;
            o.before = o.before.max(c.before);
            o.after = o.after.max(c.after);
        } else {
            chosen.push(c);
        }
    }

    // 5. Act.
    for c in chosen {
        let name = c.spec.name();
        let key = c.spec.table_key();
        if let Err(why) =
            policy::worth_building(c.benefit, writes_per_hour(&window, &key, hours), &s)
        {
            // Not worth it: decided.
            for fp in &c.fps {
                rec.cool(*fp).await?;
            }
            rec.event("deferred", &c.spec, &c.fps, why, Some((c.before, c.after)))
                .await?;
            report.deferred.push((name, why.into()));
            continue;
        }
        let paused = env.paused();
        let d = Dampers {
            now_us: now,
            records: &records,
            paused: paused.as_deref(),
        };
        // A damper only holds the build back: the patterns are not decided,
        // and the next pass tries again.
        if let Err(why) = d.may_build(&key, &s) {
            rec.event("deferred", &c.spec, &c.fps, why, Some((c.before, c.after)))
                .await?;
            report.deferred.push((name, why.into()));
            continue;
        }
        for fp in &c.fps {
            rec.cool(*fp).await?;
        }
        let mut r = IndexRecord::new(c.spec.clone(), State::Building, c.reason.name(), now);
        r.serves = c.fps.clone();
        r.baseline = c.baseline.clone();
        r.est_before = c.before;
        r.est_after = c.after;
        r.benefit_ms_per_hour = c.benefit;
        rec.event(
            "candidate",
            &c.spec,
            &c.fps,
            c.reason.name(),
            Some((c.before, c.after)),
        )
        .await?;
        match s.mode {
            Mode::Auto => {
                build(env, &mut rec, &mut r, &s, &mut report).await?;
            }
            Mode::Recommend => {
                r.state = State::Recommended;
                rec.save(&r).await?;
                let id = env.propose(&r).await?;
                r.approval = Some(id);
                rec.save(&r).await?;
                rec.event(
                    "recommended",
                    &r.spec,
                    &r.serves,
                    "waiting for approval",
                    Some((r.est_before, r.est_after)),
                )
                .await?;
                report.recommended.push(r.name.clone());
            }
        }
        records.push(r);
    }
    Ok(report)
}

/// Builds `r` (state `building` on entry and while it runs).
async fn build(
    env: &dyn Env,
    rec: &mut Recorder<'_>,
    r: &mut IndexRecord,
    s: &AutoIndexSettings,
    report: &mut PassReport,
) -> Result<()> {
    r.state = State::Building;
    r.changed_us = env.now_us();
    rec.save(r).await?;
    match env.build(&r.spec, s).await {
        Ok(b) => {
            let now = env.now_us();
            r.state = State::Observing;
            r.changed_us = now;
            r.built_us = Some(now);
            r.reads_seen = env.reads(&r.spec, &r.name);
            let why = format!(
                "{} rows in {} ms, states {}",
                b.rows,
                b.ms,
                b.states.join(" > ")
            );
            r.build = Some(b);
            rec.save(r).await?;
            rec.event(
                "built",
                &r.spec,
                &r.serves,
                &why,
                Some((r.est_before, r.est_after)),
            )
            .await?;
            report.built.push(r.name.clone());
        }
        // The table's users didn't finish in time, a deadlock, or its
        // definition changed mid-build: the record stays `building` and the
        // next pass builds again (the index added so far is kept).
        Err(e) if matches!(e.protocol().mysql_errno, 1205 | 1213 | 1412) => {
            rec.event("build_deferred", &r.spec, &r.serves, &e.to_string(), None)
                .await?;
            report.deferred.push((r.name.clone(), "build_retry".into()));
        }
        Err(e) => {
            r.state = State::Failed;
            r.changed_us = env.now_us();
            r.end_reason = Some(e.to_string());
            rec.save(r).await?;
            rec.event("build_failed", &r.spec, &r.serves, &e.to_string(), None)
                .await?;
        }
    }
    Ok(())
}
