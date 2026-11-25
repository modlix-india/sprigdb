//! One evaluation of one tenant: watch, trigger, generate, validate,
//! cost, damp, act. Everything outside the decision logic goes through
//! [`Env`], which the gateway hosting the tenant's engine implements (and
//! tests fake, with a clock they move).
//!
//! ```text
//! run(env):
//!   off (auto_index.enabled = false): nothing
//!   watch    every built index: reads counted since the last pass; the
//!            watch rule (a pinned index is never dropped); drop ->
//!            Env::drop_index, record dropped
//!   reconcile  an automatic index on a table that no live record explains
//!            is dropped (`unexplained`)
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
//!
//! Between passes the node calls [`note_usage`] (every minute, and when it
//! stops) so reads counted in memory reach the records and a restart
//! forgets at most a minute of them, and [`apply_requests`] for operators'
//! requests (`sprigctl index drop|pin|unpin|restore`).

use std::collections::HashMap;

use sprig_catalog::records::RecordTx;
use sprig_common::Result;
use sprig_workload::shape::StmtType;
use sprig_workload::window::FpWindowStats;

use super::candidates::{self, Reason, TableInfo};
use super::model::{
    Action, Baseline, BuildReport, Event, IndexRecord, IndexSpec, OperatorRequest, State,
};
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
    /// When a plan last read index `name` of `spec`'s table, as this node
    /// saw it; `None`: not known (the pass's time is used).
    fn last_read_us(&self, _spec: &IndexSpec, _name: &str) -> Option<u64> {
        None
    }
    /// Every automatic index (`sprig_auto_*`) on the tenant's tables, as
    /// the catalog has it: its table and columns, and its name.
    async fn auto_indexes(&self) -> Result<Vec<(IndexSpec, String)>> {
        Ok(Vec::new())
    }
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
    /// Index records the tenant has (any state), after the pass.
    pub records: usize,
}

struct Recorder<'a> {
    env: &'a dyn Env,
}

/// Tells apart events of the same µs, across passes and requests.
static EVENT_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

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
        self.event_named(kind, &spec.name(), spec, fps, reason, est)
            .await
    }

    async fn event_named(
        &mut self,
        kind: &str,
        name: &str,
        spec: &IndexSpec,
        fps: &[u64],
        reason: &str,
        est: Option<(f64, f64)>,
    ) -> Result<()> {
        let e = Event {
            at_us: self.env.now_us(),
            kind: kind.into(),
            index: name.to_owned(),
            def: spec.canonical(),
            table: spec.table_key(),
            fingerprints: fps.to_vec(),
            reason: reason.into(),
            est,
        };
        let seq = EVENT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut tx = self.env.records().await?;
        AutoIndexStore::add_event(&mut *tx, &e, seq).await?;
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
        // Operators' requests still apply to the records it has.
        let mut tx = env.records().await?;
        report.records = AutoIndexStore::indexes(&mut *tx).await?.len();
        tx.commit().await?;
        return Ok(report);
    }
    let now = env.now_us();
    let mut rec = Recorder { env };
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
        let fresh = absorb(env, r, now);
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

    // Reconcile: an automatic index no live record explains goes (a
    // record lost, or a drop cut short). The catalog is the truth for what
    // exists; the records only say why.
    for (spec, name) in env.auto_indexes().await? {
        let explained = records.iter().any(|r| {
            !r.state.terminal()
                && r.name.eq_ignore_ascii_case(&name)
                && r.spec.table_key() == spec.table_key()
        });
        if explained {
            continue;
        }
        env.drop_index(&spec, &name).await?;
        rec.event_named("dropped", &name, &spec, &[], "unexplained", None)
            .await?;
        report.dropped.push((name, "unexplained".into()));
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
    report.records = records.len();
    Ok(report)
}

/// Takes the reads counted since the record last saw them into `r`:
/// `reads_total`, `last_used_us`. The fresh reads.
fn absorb(env: &dyn Env, r: &mut IndexRecord, now: u64) -> u64 {
    let reads = env.reads(&r.spec, &r.name);
    let fresh = if reads >= r.reads_seen {
        reads - r.reads_seen
    } else {
        // The node restarted: its count started again from zero.
        reads
    };
    if fresh > 0 {
        let at = env
            .last_read_us(&r.spec, &r.name)
            .map_or(now, |t| t.min(now));
        r.last_used_us = Some(r.last_used_us.map_or(at, |u| u.max(at)));
        r.reads_total += fresh;
    }
    r.reads_seen = reads;
    fresh
}

/// Writes the reads counted in memory since the last pass (or the last
/// call) into the built indexes' records, so a restart doesn't forget
/// them. Returns the records written.
///
/// # Errors
/// The records'.
pub async fn note_usage(env: &dyn Env) -> Result<usize> {
    let now = env.now_us();
    let mut tx = env.records().await?;
    let mut records = AutoIndexStore::indexes(&mut *tx).await?;
    let mut written = 0;
    for r in records.iter_mut().filter(|r| r.state.built()) {
        if absorb(env, r, now) > 0 {
            AutoIndexStore::put_index(&mut *tx, r).await?;
            written += 1;
        }
    }
    tx.commit().await?;
    Ok(written)
}

/// What [`apply_requests`] did with one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// The index.
    pub index: String,
    /// The action.
    pub action: Action,
    /// `done`, `waiting` (a restore while builds must wait; kept for the
    /// next call), or why it was refused (`no_such_index`, a state).
    pub outcome: String,
}

/// Applies operators' pending requests: drop, pin, unpin, restore. Each
/// one is checked again against the record as it is now, recorded as an
/// event with its actor, and deleted.
///
/// # Errors
/// The records' and the environment's; a call stops at the first.
pub async fn apply_requests(env: &dyn Env) -> Result<Vec<Applied>> {
    let (reqs, mut records) = {
        let mut tx = env.records().await?;
        let q = AutoIndexStore::requests(&mut *tx).await?;
        let r = if q.is_empty() {
            Vec::new()
        } else {
            AutoIndexStore::indexes(&mut *tx).await?
        };
        tx.commit().await?;
        (q, r)
    };
    let s = env.settings();
    let mut rec = Recorder { env };
    let mut out = Vec::new();
    for q in reqs {
        let outcome = apply_one(env, &mut rec, &mut records, &q, &s).await?;
        if outcome != "waiting" {
            let mut tx = env.records().await?;
            AutoIndexStore::delete_request(&mut *tx, &q.index).await?;
            tx.commit().await?;
        }
        out.push(Applied {
            index: q.index.clone(),
            action: q.action,
            outcome,
        });
    }
    Ok(out)
}

async fn apply_one(
    env: &dyn Env,
    rec: &mut Recorder<'_>,
    records: &mut [IndexRecord],
    q: &OperatorRequest,
    s: &AutoIndexSettings,
) -> Result<String> {
    let Some(i) = records
        .iter()
        .position(|r| r.name.eq_ignore_ascii_case(&q.index))
    else {
        return Ok("no_such_index".into());
    };
    if let Err(why) = q.action.allowed(&records[i]) {
        return Ok(why);
    }
    let now = env.now_us();
    let mut who = String::from("operator");
    if !q.actor.is_empty() {
        who = format!("{who} {}", q.actor);
    }
    if !q.reason.is_empty() {
        who = format!("{who}: {}", q.reason);
    }
    match q.action {
        Action::Drop => {
            let r = &mut records[i];
            if r.state == State::Recommended {
                r.state = State::Rejected;
            } else {
                env.drop_index(&r.spec, &r.name).await?;
                r.state = State::Dropped;
            }
            r.changed_us = now;
            r.pinned = false;
            r.end_reason = Some("operator".into());
            rec.save(r).await?;
            rec.event("dropped", &r.spec, &r.serves, &who, None).await?;
        }
        Action::Pin | Action::Unpin => {
            let r = &mut records[i];
            r.pinned = q.action == Action::Pin;
            rec.save(r).await?;
            let kind = if r.pinned { "pinned" } else { "unpinned" };
            rec.event(kind, &r.spec, &r.serves, &who, None).await?;
        }
        Action::Restore => {
            let busy = records.iter().any(|r| r.state == State::Building);
            if env.paused().is_some() || busy {
                return Ok("waiting".into());
            }
            let mut r = records[i].clone();
            r.end_reason = None;
            r.pinned = true;
            r.reads_seen = env.reads(&r.spec, &r.name);
            rec.event("restored", &r.spec, &r.serves, &who, None)
                .await?;
            let mut report = PassReport::default();
            build(env, rec, &mut r, s, &mut report).await?;
            records[i] = r;
        }
    }
    Ok("done".into())
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
