//! The rules the loop applies: the verdict over what-if replays, the cost
//! rule, the dampers and the watch rules. Every function is pure; times are
//! µs since the epoch.
//!
//! ```text
//! verdict(replays):            each replay: rows examined without and
//!                              with the candidate, and whether its plan
//!                              read the candidate
//!   none replayed              reject  no_statements
//!   no plan reads it           reject  plan_ignores
//!   one gets worse             reject  regression   (after > before x
//!                                      (1 + max_regression_pct) + 1)
//!   median gain too small      reject  small_gain   (< min_improvement_pct)
//!   else                       accept
//! cost:   benefit = runs/hour x mean ms x (1 - after/before)
//!         write   = writes/hour x maintenance_cost_ms
//!         build when benefit - write >= min_net_ms_per_hour and
//!         benefit >= 2 x write
//! dampers (any one defers the build):
//!   paused        units moving or failing over
//!   building      another build of the tenant runs
//!   budget        max_new_per_day builds in the last 24 hours
//!   table_full    max_per_table automatic indexes on the table
//!   blocked       the definition was dropped or rejected within
//!                 recreate_block_days
//!   cooldown      the pattern was decided within cooldown_hours
//! watch (built indexes):
//!   observing, observe_days over: unused since built -> drop (unused);
//!     no served pattern faster by min_improvement_pct -> drop (no_gain);
//!     else active
//!   active: not read for observe_days -> drop (unused)
//! ```

use super::model::{Baseline, IndexRecord, State};
use super::settings::AutoIndexSettings;

/// µs in an hour.
pub const HOUR_US: u64 = 3_600_000_000;
/// µs in a day.
pub const DAY_US: u64 = 24 * HOUR_US;

/// One replayed statement: the planner's estimates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Replay {
    /// Rows examined without the candidate.
    pub before: f64,
    /// Rows examined with it.
    pub after: f64,
    /// Whether the plan with it reads it.
    pub reads: bool,
}

/// What validation decided.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// A clear win: the medians without and with.
    Accept {
        /// Median rows examined without.
        before: f64,
        /// Median rows examined with.
        after: f64,
    },
    /// No: why.
    Reject(&'static str),
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        f64::midpoint(v[n / 2 - 1], v[n / 2])
    }
}

/// The verdict over a candidate's replays.
#[must_use]
pub fn verdict(replays: &[Replay], s: &AutoIndexSettings) -> Verdict {
    if replays.is_empty() {
        return Verdict::Reject("no_statements");
    }
    if !replays.iter().any(|r| r.reads) {
        return Verdict::Reject("plan_ignores");
    }
    let limit = 1.0 + s.max_regression_pct / 100.0;
    if replays.iter().any(|r| r.after > r.before * limit + 1.0) {
        return Verdict::Reject("regression");
    }
    let before = median(replays.iter().map(|r| r.before).collect());
    let after = median(replays.iter().map(|r| r.after).collect());
    if before <= 0.0 || (1.0 - after / before) * 100.0 < s.min_improvement_pct {
        return Verdict::Reject("small_gain");
    }
    Verdict::Accept { before, after }
}

/// Estimated time saved per hour, ms: a pattern's runs per hour times its
/// mean time times the share of rows it no longer examines.
#[must_use]
pub fn benefit_ms_per_hour(runs_per_hour: f64, mean_ms: f64, before: f64, after: f64) -> f64 {
    if before <= 0.0 {
        return 0.0;
    }
    runs_per_hour * mean_ms * (1.0 - after / before).max(0.0)
}

/// Whether the saved time pays for the writes: `Err` with why not.
///
/// # Errors
/// `low_net` or `write_cost`.
pub fn worth_building(
    benefit_ms_per_hour: f64,
    writes_per_hour: f64,
    s: &AutoIndexSettings,
) -> Result<(), &'static str> {
    let write = writes_per_hour * s.maintenance_cost_ms;
    if benefit_ms_per_hour - write < s.min_net_ms_per_hour {
        return Err("low_net");
    }
    if benefit_ms_per_hour < 2.0 * write {
        return Err("write_cost");
    }
    Ok(())
}

/// What the dampers read.
#[derive(Debug, Clone, Copy)]
pub struct Dampers<'a> {
    /// Now.
    pub now_us: u64,
    /// The tenant's index records.
    pub records: &'a [IndexRecord],
    /// Units are moving or failing over: why.
    pub paused: Option<&'a str>,
}

impl Dampers<'_> {
    /// Builds the tenant started in the last 24 hours (every record that
    /// got as far as a build).
    #[must_use]
    pub fn built_today(&self) -> usize {
        self.records
            .iter()
            .filter(|r| r.built_us.is_some() || r.state == State::Building)
            .filter(|r| self.now_us.saturating_sub(r.created_us) < DAY_US)
            .count()
    }

    /// Whether a definition with this hash was dropped or rejected within
    /// the block.
    #[must_use]
    pub fn blocked(&self, def_hash: u64, table_key: &str, s: &AutoIndexSettings) -> bool {
        let block = u64::from(s.recreate_block_days) * DAY_US;
        self.records.iter().any(|r| {
            matches!(r.state, State::Dropped | State::Rejected)
                && r.spec.def_hash() == def_hash
                && r.spec.table_key() == table_key
                && self.now_us.saturating_sub(r.changed_us) < block
        })
    }

    /// Whether `fp` was decided within the cooldown.
    #[must_use]
    pub fn cooling(&self, fp: u64, last_decided_us: Option<u64>, s: &AutoIndexSettings) -> bool {
        let cool = u64::from(s.cooldown_hours) * HOUR_US;
        last_decided_us.is_some_and(|at| self.now_us.saturating_sub(at) < cool)
            || self
                .records
                .iter()
                .any(|r| r.serves.contains(&fp) && self.now_us.saturating_sub(r.created_us) < cool)
    }

    /// Whether a new build may start on `table_key`: `Err` with why not.
    ///
    /// # Errors
    /// `paused`, `building`, `budget` or `table_full`.
    pub fn may_build(&self, table_key: &str, s: &AutoIndexSettings) -> Result<(), &'static str> {
        if self.paused.is_some() {
            return Err("paused");
        }
        if self.records.iter().any(|r| r.state == State::Building) {
            return Err("building");
        }
        if self.built_today() >= s.max_new_per_day as usize {
            return Err("budget");
        }
        let on_table = self
            .records
            .iter()
            .filter(|r| !r.state.terminal() && r.spec.table_key() == table_key)
            .count();
        if on_table >= s.max_per_table as usize {
            return Err("table_full");
        }
        Ok(())
    }
}

/// What the watch does with a built index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Watch {
    /// Nothing yet.
    Keep,
    /// The observation is over and it pays: active.
    Activate,
    /// Drop it: why.
    Drop(&'static str),
}

/// The watch rule for `rec` at `now_us`; `current`: the served patterns'
/// figures over the latest window (absent: no runs). A pinned index is
/// never dropped: its observation ends with it kept.
#[must_use]
pub fn watch(
    rec: &IndexRecord,
    now_us: u64,
    current: &[(u64, Baseline)],
    s: &AutoIndexSettings,
) -> Watch {
    let w = judge(rec, now_us, current, s);
    match w {
        Watch::Drop(_) if rec.pinned && rec.state == State::Observing => Watch::Activate,
        Watch::Drop(_) if rec.pinned => Watch::Keep,
        other => other,
    }
}

fn judge(
    rec: &IndexRecord,
    now_us: u64,
    current: &[(u64, Baseline)],
    s: &AutoIndexSettings,
) -> Watch {
    let Some(built) = rec.built_us else {
        return Watch::Keep;
    };
    let period = u64::from(s.observe_days) * DAY_US;
    let last_use = rec.last_used_us.filter(|&u| u >= built);
    let idle_since = last_use.unwrap_or(built);
    match rec.state {
        State::Observing => {
            if now_us.saturating_sub(built) < period {
                return Watch::Keep;
            }
            if last_use.is_none() || now_us.saturating_sub(idle_since) >= period {
                return Watch::Drop("unused");
            }
            // Faster than before? Judged on the patterns that still run
            // (at least 5 runs in the window).
            let judged: Vec<bool> = rec
                .baseline
                .iter()
                .filter_map(|(fp, before)| {
                    let now = current.iter().find(|(f, _)| f == fp)?;
                    (now.1.runs >= 5 && before.p95_ms > 0.0).then(|| {
                        now.1.p95_ms <= before.p95_ms * (1.0 - s.min_improvement_pct / 100.0)
                    })
                })
                .collect();
            if !judged.is_empty() && !judged.iter().any(|&faster| faster) {
                return Watch::Drop("no_gain");
            }
            Watch::Activate
        }
        State::Active => {
            if now_us.saturating_sub(idle_since) >= period {
                Watch::Drop("unused")
            } else {
                Watch::Keep
            }
        }
        _ => Watch::Keep,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoindex::model::{IndexSpec, KeyCol};

    fn s() -> AutoIndexSettings {
        AutoIndexSettings::default()
    }

    fn r(before: f64, after: f64, reads: bool) -> Replay {
        Replay {
            before,
            after,
            reads,
        }
    }

    pub(crate) fn rec(state: State, created: u64, built: Option<u64>) -> IndexRecord {
        let spec = IndexSpec {
            db: "d".into(),
            table: "t".into(),
            cols: vec![KeyCol::asc("a")],
        };
        let mut r = IndexRecord::new(spec, state, "slow_p95", created);
        r.serves = vec![7];
        r.baseline = vec![(
            7,
            Baseline {
                runs: 100,
                p95_ms: 200.0,
                mean_ms: 150.0,
            },
        )];
        r.changed_us = built.unwrap_or(created);
        r.built_us = built;
        r
    }

    #[test]
    fn verdicts() {
        let s = s();
        assert_eq!(verdict(&[], &s), Verdict::Reject("no_statements"));
        assert_eq!(
            verdict(&[r(1000.0, 1000.0, false)], &s),
            Verdict::Reject("plan_ignores")
        );
        assert_eq!(
            verdict(&[r(1000.0, 10.0, true), r(100.0, 200.0, true)], &s),
            Verdict::Reject("regression")
        );
        assert_eq!(
            verdict(&[r(1000.0, 800.0, true)], &s),
            Verdict::Reject("small_gain")
        );
        assert_eq!(
            verdict(
                &[
                    r(1000.0, 10.0, true),
                    r(3000.0, 20.0, true),
                    r(5.0, 5.0, false)
                ],
                &s
            ),
            Verdict::Accept {
                before: 1000.0,
                after: 10.0
            }
        );
    }

    #[test]
    fn cost_rule() {
        let s = s();
        let b = benefit_ms_per_hour(100.0, 50.0, 1000.0, 10.0);
        assert!((b - 4950.0).abs() < 1e-6);
        assert_eq!(worth_building(b, 1000.0, &s), Ok(()));
        assert_eq!(worth_building(50.0, 0.0, &s), Err("low_net"));
        assert_eq!(worth_building(5000.0, 60_000.0, &s), Err("write_cost"));
    }

    #[test]
    fn dampers() {
        let s = AutoIndexSettings {
            max_new_per_day: 2,
            max_per_table: 2,
            ..s()
        };
        let now = 100 * DAY_US;
        let mut recs = vec![rec(State::Active, now - HOUR_US, Some(now - HOUR_US))];
        let d = Dampers {
            now_us: now,
            records: &recs,
            paused: None,
        };
        assert_eq!(d.may_build("d.t", &s), Ok(()));
        assert!(d.cooling(7, None, &s));
        assert!(!d.cooling(8, Some(now - 25 * HOUR_US), &s));
        assert!(d.cooling(8, Some(now - 23 * HOUR_US), &s));
        let d = Dampers {
            paused: Some("moving"),
            ..d
        };
        assert_eq!(d.may_build("d.t", &s), Err("paused"));
        recs.push(rec(State::Observing, now - 2 * HOUR_US, Some(now)));
        let d = Dampers {
            now_us: now,
            records: &recs,
            paused: None,
        };
        assert_eq!(d.may_build("d.t", &s), Err("budget"));
        assert_eq!(
            Dampers {
                now_us: now + DAY_US,
                ..d
            }
            .may_build("d.t", &s),
            Err("table_full")
        );
        recs.push(rec(State::Building, now, None));
        let d = Dampers {
            now_us: now + 3 * DAY_US,
            records: &recs,
            paused: None,
        };
        assert_eq!(d.may_build("x.y", &s), Err("building"));
        // A dropped definition is blocked for 14 days.
        let mut dropped = rec(State::Dropped, now, Some(now));
        dropped.changed_us = now;
        let recs = [dropped.clone()];
        let d = Dampers {
            now_us: now + 13 * DAY_US,
            records: &recs,
            paused: None,
        };
        assert!(d.blocked(dropped.spec.def_hash(), "d.t", &s));
        assert!(!d.blocked(dropped.spec.def_hash(), "d.u", &s));
        let d = Dampers {
            now_us: now + 15 * DAY_US,
            ..d
        };
        assert!(!d.blocked(dropped.spec.def_hash(), "d.t", &s));
    }

    #[test]
    fn a_pinned_index_is_never_dropped() {
        let s = s();
        let built = 10 * DAY_US;
        let mut x = rec(State::Observing, built, Some(built));
        x.pinned = true;
        // Never read in its observation: kept, and active.
        assert_eq!(watch(&x, built + 7 * DAY_US, &[], &s), Watch::Activate);
        x.state = State::Active;
        assert_eq!(watch(&x, built + 30 * DAY_US, &[], &s), Watch::Keep);
        x.pinned = false;
        assert_eq!(
            watch(&x, built + 30 * DAY_US, &[], &s),
            Watch::Drop("unused")
        );
    }

    #[test]
    fn watch_rules() {
        let s = s();
        let built = 10 * DAY_US;
        let mut x = rec(State::Observing, built, Some(built));
        // Within the observation: nothing.
        assert_eq!(watch(&x, built + 6 * DAY_US, &[], &s), Watch::Keep);
        // Never read: dropped.
        assert_eq!(
            watch(&x, built + 7 * DAY_US, &[], &s),
            Watch::Drop("unused")
        );
        // Read, and the pattern is faster: active.
        x.last_used_us = Some(built + 6 * DAY_US);
        let faster = [(
            7,
            Baseline {
                runs: 100,
                p95_ms: 5.0,
                mean_ms: 4.0,
            },
        )];
        assert_eq!(watch(&x, built + 7 * DAY_US, &faster, &s), Watch::Activate);
        // Read, but no faster: no gain.
        let same = [(
            7,
            Baseline {
                runs: 100,
                p95_ms: 190.0,
                mean_ms: 150.0,
            },
        )];
        assert_eq!(
            watch(&x, built + 7 * DAY_US, &same, &s),
            Watch::Drop("no_gain")
        );
        // Active, then unread for the period: dropped.
        x.state = State::Active;
        assert_eq!(watch(&x, built + 12 * DAY_US, &[], &s), Watch::Keep);
        assert_eq!(
            watch(&x, built + 13 * DAY_US, &[], &s),
            Watch::Drop("unused")
        );
    }
}
