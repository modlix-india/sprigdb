//! The advisor's periodic run for a tenant whose tables are in table
//! groups, and its `Rekey` verdict.
//!
//! ```text
//! periodic(tenant), in a tick once every group is active:
//!   due every advisor.interval_hours (from the newest periodic advisor_runs
//!     row, so a restart doesn't run it again early); advisor.mode = off or
//!     an interval of 0: never
//!   skipped while a job of the tenant runs or holds it (a split, merge,
//!     move or re-key): it never scores a half-moved layout
//!   input: each group's first shard (columns, keys, sizes × its shards),
//!     the default schemas, the workload window, pins, stored code
//!   sprig_advisor::sharding::periodic::evaluate → the best re-key
//!   history: the previous advisor.rekey_sustain_runs − 1 periodic runs'
//!     proposals; the group's last group.rekey decision (the cooldown)
//!   an advisor_runs row (cause periodic) and a decision (group.design)
//!   apply  → advisor.mode on: the re-key starts at once (reason periodic);
//!            approve or recommend: an advisor.rekey approvals item; once
//!            approved, the next tick starts it
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_advisor::sharding::design::{Member, Routing, Verdict};
use sprig_advisor::sharding::periodic::{self, History, LiveGroup, Proposal};
use sprig_advisor::sharding::{AdvisorMode, input, runs};
use sprig_balancer::decisions::{DecisionStore, Mode, NewDecision, Outcome};
use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigError};
use sprig_shard::{KeySpec, shard_schema};

use super::{RekeyRequest, ShardingLoop, TenantSettings, TickReport, lock, now_us};

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::CatalogUnavailable,
        format!("{what}: {e}"),
    )
}

/// One group row: id, key spec, members (object, db, routing, key
/// columns, parent, parent FK, pinned).
type GroupRows = Vec<(u32, String, Vec<MemberRow>)>;
type MemberRow = (
    u64,
    u32,
    String,
    Option<String>,
    Option<u64>,
    Option<String>,
    bool,
);
/// A `group_members` row with its group id first.
type GroupMemberRow = (
    u32,
    u64,
    u32,
    String,
    Option<String>,
    Option<u64>,
    Option<String>,
    bool,
);

/// A re-key held for approval: the run that proposed it and the proposal,
/// gated again every tick until it is approved, refused or superseded.
#[derive(Debug, Clone)]
pub(super) struct PendingRekey {
    run: u64,
    proposal: Proposal,
}

/// The re-key a periodic run's stored evidence proposes: its best
/// proposal, when the run's design names one.
pub(super) fn stored_rekey(design: &Value, evidence: &Value) -> Option<Proposal> {
    let g = design.get("rekey")?.get("group")?.as_u64()?;
    let all: Vec<Proposal> = serde_json::from_value(evidence["rekey_proposals"].clone()).ok()?;
    all.into_iter().find(|p| u64::from(p.group) == g)
}

/// Whether a periodic run is due: never checked yet, its time has come, or
/// `advisor.interval_hours` changed since the due time was computed (a
/// shorter interval then takes effect at once, a longer one from the last
/// run's time).
fn periodic_due(next: Option<(Instant, Duration)>, every: Duration, now: Instant) -> bool {
    next.is_none_or(|(t, was)| was != every || now >= t)
}

impl ShardingLoop {
    /// The periodic run when it is due (module docs). Returns whether it
    /// started a re-key (the tick then leaves the groups alone).
    ///
    /// # Errors
    /// Catalog and cluster errors; the next tick retries.
    pub(super) async fn periodic(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        report: &mut TickReport,
    ) -> Result<bool> {
        if ts.advisor.mode == AdvisorMode::Off || ts.rekey.every.is_zero() {
            return Ok(false);
        }
        // An approved item starts its re-key at the next tick.
        if let Some(started) = self.pending_rekey(tenant, ts, home).await? {
            return Ok(started);
        }
        let next = lock(&self.state).get(&tenant).and_then(|s| s.next_periodic);
        let due = periodic_due(next, ts.rekey.every, Instant::now());
        if !due {
            return Ok(false);
        }
        let cat = SchemaName::catalog(tenant).to_string();
        let mut c = self.conn(home).await?;
        runs::migrate(&mut c, &cat).await?;
        let since: Option<Option<u64>> = c
            .query_first(format!(
                "SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), UTC_TIMESTAMP(6)) \
                 FROM `{cat}`.advisor_runs WHERE cause = 'periodic'"
            ))
            .await
            .map_err(|e| db_err("periodic runs", &e))?;
        if let Some(Some(s)) = since
            && Duration::from_secs(s) < ts.rekey.every
        {
            let wait = ts.rekey.every.saturating_sub(Duration::from_secs(s));
            lock(&self.state).entry(tenant).or_default().next_periodic =
                Some((Instant::now() + wait, ts.rekey.every));
            return Ok(false);
        }
        // No run while the tenant's layout moves.
        let (running, _) = self.follow(tenant).await?;
        let busy = running.iter().any(|r| r.tenant == tenant)
            || self.jobs.tenant_lock_holder(tenant).await?.is_some();
        if busy {
            report
                .waiting
                .push((format!("t{tenant}"), "advisor_periodic"));
            return Ok(false);
        }
        lock(&self.state).entry(tenant).or_default().next_periodic =
            Some((Instant::now() + ts.rekey.every, ts.rekey.every));
        let groups = Self::live_group_rows(&mut c, &cat).await?;
        drop(c);
        if groups.is_empty() {
            return Ok(false);
        }
        self.periodic_run(tenant, ts, home, &groups, report).await
    }

    /// Every active sharded group with its members.
    async fn live_group_rows(c: &mut mysql_async::Conn, cat: &str) -> Result<GroupRows> {
        let gs: Vec<(u32, String)> = c
            .query(format!(
                "SELECT group_id, CAST(key_spec AS CHAR) FROM `{cat}`.table_groups \
                 WHERE kind = 'sharded' AND state = 'active' ORDER BY group_id"
            ))
            .await
            .map_err(|e| db_err("table groups", &e))?;
        let ms: Vec<GroupMemberRow> = c
            .query(format!(
                "SELECT group_id, object_id, db_id, routing, CAST(key_columns AS CHAR), \
                 parent_object_id, parent_fk, pinned = 1 FROM `{cat}`.group_members \
                 ORDER BY group_id, object_id"
            ))
            .await
            .map_err(|e| db_err("group members", &e))?;
        Ok(gs
            .into_iter()
            .map(|(g, spec)| {
                let members = ms
                    .iter()
                    .filter(|m| m.0 == g)
                    .map(|m| (m.1, m.2, m.3.clone(), m.4.clone(), m.5, m.6.clone(), m.7))
                    .collect();
                (g, spec, members)
            })
            .collect())
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the run, step by step
    async fn periodic_run(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
        groups: &GroupRows,
        report: &mut TickReport,
    ) -> Result<bool> {
        let now = SystemTime::now();
        let s = ts.advisor;
        // Tables: each group's first shard, then the default schemas.
        let mut c = self.conn(home).await?;
        let objects = input::catalog_tables(&mut c, tenant).await?;
        let (mut tables, mut fks, mut located) = input::tables(&mut c, tenant).await?;
        drop(c);
        let mut live: Vec<LiveGroup> = Vec::new();
        for (g, spec, members) in groups {
            let Ok((map, _)) = self.maps.load(tenant, GroupId(*g)).await else {
                continue;
            };
            let Some(first) = map.ranges.first() else {
                continue;
            };
            let mut dbs: Vec<u32> = members.iter().map(|m| m.1).collect();
            dbs.sort_unstable();
            dbs.dedup();
            let schemas: Vec<(DbId, String)> = dbs
                .iter()
                .map(|d| {
                    (
                        DbId(*d),
                        shard_schema(tenant, DbId(*d), GroupId(*g), first.shard),
                    )
                })
                .collect();
            let mut sc = self.conn(&first.cluster).await?;
            let (mut t, f, l) = input::tables_in(&mut sc, &objects, &schemas).await?;
            drop(sc);
            let ids: Vec<u64> = members.iter().map(|m| m.0).collect();
            #[allow(clippy::cast_precision_loss)] // shard counts
            let n = map.ranges.len() as f64;
            t.retain(|x| ids.contains(&x.object));
            for x in &mut t {
                // One shard read; every shard counted.
                x.size_bytes *= n;
            }
            let bytes = t.iter().map(|x| x.size_bytes).sum();
            tables.retain(|x| !ids.contains(&x.object));
            tables.extend(t);
            fks.extend(f.into_iter().filter(|x| ids.contains(&x.child)));
            located.extend(l.into_iter().filter(|x| ids.contains(&x.object)));
            let key = serde_json::from_str::<Value>(spec)
                .ok()
                .and_then(|v| KeySpec::from_json(&v).ok())
                .map(|k| k.parts)
                .unwrap_or_default();
            let mut clusters: Vec<&ClusterId> = map.ranges.iter().map(|r| &r.cluster).collect();
            clusters.sort();
            clusters.dedup();
            live.push(LiveGroup {
                id: *g,
                key,
                members: members
                    .iter()
                    .filter_map(|(o, _, routing, cols, parent, fk, _)| {
                        let routing = match (routing.as_str(), parent, fk) {
                            ("parent", Some(p), Some(fk)) => Routing::Parent {
                                parent: *p,
                                fk: fk.clone(),
                            },
                            ("columns", _, _) => Routing::Columns {
                                columns: serde_json::from_str(cols.as_deref()?).ok()?,
                            },
                            _ => return None,
                        };
                        Some(Member {
                            object: *o,
                            routing,
                        })
                    })
                    .collect(),
                bytes,
                several_clusters: clusters.len() > 1,
                pinned: members.iter().any(|m| m.6),
            });
        }
        // Key-value element tables leave the run: key-value units follow
        // their own rule, and their samples read the home cluster only.
        tables.retain(|t| input::kv_key_table(&t.name).is_none());
        let mut c = self.conn(home).await?;
        let asm = input::assemble_from(
            &mut c,
            tenant,
            now,
            s.window,
            ts.sharding.split_bytes,
            (tables, fks, located),
        )
        .await?;
        for g in &mut live {
            g.pinned |= g.members.iter().any(|m| asm.input.pin(m.object).is_some());
        }
        let e = periodic::evaluate(&asm.input, &live, &s, &ts.rekey);
        let cat = SchemaName::catalog(tenant).to_string();
        // History: the earlier periodic runs' proposals, newest first.
        let need = usize::try_from(ts.rekey.sustain_runs.saturating_sub(1)).unwrap_or(0);
        let earlier: Vec<Option<String>> = c
            .query(format!(
                "SELECT CAST(evidence AS CHAR) FROM `{cat}`.advisor_runs \
                 WHERE cause = 'periodic' AND status <> 'failed' \
                 ORDER BY run_id DESC LIMIT {need}"
            ))
            .await
            .map_err(|e| db_err("periodic runs", &e))?;
        let earlier: Vec<Vec<Proposal>> = earlier
            .into_iter()
            .map(|ev| {
                ev.and_then(|t| serde_json::from_str::<Value>(&t).ok())
                    .and_then(|v| serde_json::from_value(v["rekey_proposals"].clone()).ok())
                    .unwrap_or_default()
            })
            .collect();
        let best = e.proposals.first();
        let since_rekey = match best {
            Some(p) => self.since_rekey(&mut c, tenant, p.group).await,
            None => None,
        };
        let history = History {
            earlier,
            since_rekey,
        };
        let held = best
            .map(|p| periodic::holds(p, &history, &ts.rekey))
            .unwrap_or_default();
        let verdict = periodic::decide(best, &held, &s);
        let ready = best.is_some_and(|p| p.qualifies) && held.is_empty();
        let window_start = now.checked_sub(s.window).unwrap_or(SystemTime::UNIX_EPOCH);
        let us = |t: SystemTime| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(0))
        };
        let mut design = e.best.to_json(&asm.input);
        if let Some(p) = best {
            design["rekey"] = json!({ "group": p.group, "columns": p.columns, "column": p.column });
        }
        let evidence = json!({
            "notes": e.notes,
            "current_score": e.current_score,
            "rekey_proposals": e.proposals,
            "held": held,
            "groups": live.iter().map(|g| g.id).collect::<Vec<_>>(),
        });
        let run = runs::insert(
            &mut c,
            &cat,
            &runs::RunRecord {
                cause: "periodic",
                mode: match s.mode {
                    AdvisorMode::On => "on",
                    AdvisorMode::Recommend | AdvisorMode::Approve => "recommend",
                    AdvisorMode::Off => "off",
                },
                window_us: (us(window_start), us(now)),
                schema_version: asm.schema_version,
                units: asm.input.units_sampled(),
                best: best.map_or(e.current_score, |p| p.proposed),
                runner_up: Some(e.current_score),
                confidence: best.map_or(0.0, |p| p.confidence),
                verdict: verdict.as_str(),
                design,
                alternatives: json!(e.proposals.iter().skip(1).collect::<Vec<_>>()),
                evidence: evidence.clone(),
            },
        )
        .await?;
        report.advisor = Some((run, verdict.as_str()));
        metrics::counter!("sprig_advisor_runs_total", "cause" => "periodic", "verdict" => verdict.as_str())
            .increment(1);
        let summary = match best {
            Some(p) => format!(
                "periodic run {run}: re-key group {} by {} gains {:.1} points (confidence {:.2}), \
                 verdict {}",
                p.group,
                p.column,
                p.gain_pts,
                p.confidence,
                verdict.as_str()
            ),
            None => format!(
                "periodic run {run}: no better key for {} group(s), verdict {}",
                live.len(),
                verdict.as_str()
            ),
        };
        let d = self
            .record(
                tenant,
                &NewDecision {
                    decided_at_us: now_us(),
                    component: "advisor".into(),
                    kind: "group.design".into(),
                    subject_type: "tenant".into(),
                    subject_id: format!("t{tenant}"),
                    mode: if verdict == Verdict::Apply {
                        Mode::Applied
                    } else {
                        Mode::Recommended
                    },
                    outcome: Outcome::Proposed,
                    reason_code: "periodic".into(),
                    summary: summary.chars().take(512).collect(),
                    evidence,
                    alternatives: None,
                    job_id: None,
                    parent_id: None,
                    actor: self.actor.clone(),
                    dc_id: self.dc.clone(),
                },
            )
            .await;
        if d > 0 {
            runs::set_decision(&mut c, &cat, run, d).await?;
        }
        drop(c);
        tracing::info!(tenant = %tenant, run, verdict = verdict.as_str(), summary = %summary,
            "advisor periodic run");
        let Some(p) = best.filter(|_| ready).cloned() else {
            return Ok(false);
        };
        if s.mode == AdvisorMode::On {
            return self.start_rekey(tenant, home, run, &p, None).await;
        }
        // Approve or recommend: an approvals item; an approval starts it.
        lock(&self.state).entry(tenant).or_default().pending_rekey =
            Some(PendingRekey { run, proposal: p });
        self.pending_rekey(tenant, ts, home)
            .await
            .map(|s| s.unwrap_or(false))
    }

    /// Since group `g`'s newest `group.rekey` decision that wasn't
    /// rejected; `None` without one.
    async fn since_rekey(
        &self,
        c: &mut mysql_async::Conn,
        tenant: TenantId,
        g: u32,
    ) -> Option<Duration> {
        let rows = DecisionStore::for_tenant(tenant)
            .by_subject(c, "group", &format!("t{tenant}/g{g}"), 50)
            .await
            .ok()?;
        let at = rows
            .iter()
            .filter(|r| r.d.kind == "group.rekey" && r.d.outcome != Outcome::Rejected)
            .map(|r| r.d.decided_at_us)
            .max()?;
        let ago = now_us().saturating_sub(at).max(0);
        Some(Duration::from_micros(u64::try_from(ago).unwrap_or(0)))
    }

    /// Gates a re-key held for approval again: `Some(true)` when it was
    /// approved and started, `Some(false)` while it waits (nothing else
    /// runs for the tenant's periodic step), `None` without one.
    async fn pending_rekey(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        home: &ClusterId,
    ) -> Result<Option<bool>> {
        use sprig_approvals::{Action, Kind, Proposal as Gated, Verdict as Gate};
        let Some(pending) = lock(&self.state)
            .get(&tenant)
            .and_then(|s| s.pending_rekey.clone())
        else {
            return Ok(None);
        };
        let Some(a) = self.approvals.get() else {
            // No hub: under recommend the run is advice only.
            lock(&self.state).entry(tenant).or_default().pending_rekey = None;
            return Ok(None);
        };
        if ts.advisor.mode == AdvisorMode::On {
            lock(&self.state).entry(tenant).or_default().pending_rekey = None;
            return self
                .start_rekey(tenant, home, pending.run, &pending.proposal, None)
                .await
                .map(Some);
        }
        let p = &pending.proposal;
        let policy = a
            .policy(Kind::AdvisorRekey, Some(tenant), None, None, None)
            .await;
        let gp = Gated {
            kind: Kind::AdvisorRekey,
            subject: a.subject(tenant).await,
            target: format!("t{tenant}/g{}/rekey", p.group),
            action: Action {
                text: format!(
                    "sprigctl shard rekey {tenant} {} --key {}{}",
                    p.group,
                    p.column,
                    p.columns.iter().filter(|(_, c)| **c != p.column).fold(
                        String::new(),
                        |mut out, (t, c)| {
                            let _ = write!(out, " --member {t}={c}");
                            out
                        }
                    )
                ),
                machine: json!({ "group": p.group, "column": p.column, "columns": p.columns }),
            },
            evidence: json!({
                "run": pending.run,
                "gain_pts": p.gain_pts,
                "confidence": p.confidence,
                "current": p.current,
                "proposed": p.proposed,
                "bytes": p.bytes,
            }),
            score: p.gain_pts.max(0.0),
            summary: format!(
                "re-key table group {} by {}: {:.1} points better (confidence {:.2})",
                p.group, p.column, p.gain_pts, p.confidence
            ),
            emergency: false,
        };
        match a.gate(&gp, &policy).await {
            Gate::Proceed(t) => {
                lock(&self.state).entry(tenant).or_default().pending_rekey = None;
                self.start_rekey(tenant, home, pending.run, &pending.proposal, Some(t))
                    .await
                    .map(Some)
            }
            Gate::Wait(_) | Gate::Advice(_) => Ok(Some(false)),
            Gate::Suppressed(_) | Gate::Off => {
                lock(&self.state).entry(tenant).or_default().pending_rekey = None;
                Ok(Some(false))
            }
        }
    }

    /// Starts the re-key `p` proposes (run `run`), reporting to the
    /// approval `ticket` when one let it through.
    pub(super) async fn start_rekey(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        run: u64,
        p: &Proposal,
        ticket: Option<sprig_approvals::Ticket>,
    ) -> Result<bool> {
        let members: BTreeMap<String, String> = p
            .columns
            .iter()
            .filter(|(_, c)| **c != p.column)
            .map(|(t, c)| (t.clone(), c.clone()))
            .collect();
        let req = RekeyRequest {
            column: p.column.clone(),
            members,
            reason: Some("periodic".into()),
            ..RekeyRequest::default()
        };
        let r = self
            .rekey(tenant, p.group, req, &format!("advisor run {run}"))
            .await;
        let detail = match &r {
            Ok(job) => format!("re-key of group {} started: job {}", p.group, job.0),
            Err(e) => format!("re-key of group {} refused: {}", p.group, e.message),
        };
        tracing::info!(tenant = %tenant, run, detail = %detail, "advisor periodic re-key");
        self.approval_done(
            ticket.as_ref().filter(|t| t.id.is_some()),
            r.is_ok(),
            &detail,
        )
        .await;
        if r.is_err() {
            metrics::counter!("sprig_advisor_rekeys_total", "outcome" => "refused").increment(1);
        }
        let mut c = self.conn(home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let plan = json!({ "rekey": detail });
        // The advisor's part is done once the job is submitted; the job's
        // own outcome goes to its group.rekey decision.
        let status = if r.is_ok() { "applied" } else { "failed" };
        runs::set_status(
            &mut c,
            &cat,
            run,
            status,
            Some(&plan),
            r.as_ref().err().map(|e| e.message.as_str()),
        )
        .await?;
        Ok(r.is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_changed_interval_is_checked_again_at_once() {
        let now = Instant::now();
        let day = Duration::from_secs(86_400);
        let short = Duration::from_secs(10);
        assert!(periodic_due(None, day, now), "never checked");
        assert!(!periodic_due(Some((now + day, day)), day, now), "not yet");
        assert!(
            periodic_due(Some((now, day)), day, now + short),
            "its time came"
        );
        // Lowered from a day to 10 s after a run: due now, and the check
        // against the newest run's time decides.
        assert!(periodic_due(Some((now + day, day)), short, now));
        // Raised: checked again too (the newest run's time then defers it).
        assert!(periodic_due(Some((now + short, short)), day, now));
    }
}
