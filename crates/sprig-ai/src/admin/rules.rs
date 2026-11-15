//! The health rules: deterministic checks over the metric history. The
//! rules decide; the administration model only words what they found.
//!
//! | Rule | Series | Finds |
//! |---|---|---|
//! | `shard_split` | `sprig_shard_split_ratio` | above its split size for 24 h (medium: splits aren't happening); projected to reach it within 7 days (low: it will split by itself) |
//! | `cluster_full` | `sprig_balancer_cluster_full_ratio` | at 0.9 or more now (high); projected to reach 0.9 within 7 days (medium) |
//! | `replica_lag` | `sprig_backend_replica_lag_seconds` | 60 s or more in each of the last 3 hours (high); the daily peak rose over the last 3 whole days to 10 s or more (medium) |
//! | `cluster_health` | `sprig_cluster_health_state` | not healthy in any of the last 3 hours (high) |
//! | `connections` | `sprig_gateway_connections` | a peak of 80% of `gateway.max_connections` in the last day (medium); projected to reach 90% within 14 days (low) |
//! | `backup_age` | `sprig_backup_last_success_timestamp_seconds` | the newest backup older than 26 h (medium) or 72 h (high) |
//! | `mover_backlog` | `sprig_mover_oldest_waiting_seconds` | a data move waiting 6 h or more for a worker (medium) |
//!
//! Projections fit a straight line to the hourly means of the last 72
//! hours (at least 24 of them) and need a rising slope.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::history::{DAY_MS, HOUR_MS, Point, SeriesRecord, series_id};

/// How far ahead projections look, in days.
pub const PROJECT_DAYS: f64 = 7.0;
/// How far ahead connection projections look, in days.
pub const PROJECT_CONNECTION_DAYS: f64 = 14.0;
/// Hours of points a projection fits.
pub const FIT_HOURS: u64 = 72;
/// Points a projection needs at least.
pub const FIT_MIN_POINTS: usize = 24;
/// A cluster this full is a finding now.
pub const FULL_RATIO: f64 = 0.9;
/// Sustained lag, in seconds.
pub const LAG_HIGH_SECONDS: f64 = 60.0;
/// The daily lag peak that counts as rising.
pub const LAG_RISING_SECONDS: f64 = 10.0;
/// A peak share of `gateway.max_connections`.
pub const CONNECTIONS_PEAK: f64 = 0.8;
/// The projected share of `gateway.max_connections`.
pub const CONNECTIONS_PROJECTED: f64 = 0.9;
/// Backup age, hours: medium.
pub const BACKUP_HOURS: f64 = 26.0;
/// Backup age, hours: high.
pub const BACKUP_HOURS_HIGH: f64 = 72.0;
/// A data move waiting this long, seconds.
pub const MOVER_WAIT_SECONDS: f64 = 6.0 * 3600.0;

/// What the rules need besides the series.
#[derive(Debug, Clone, PartialEq)]
pub struct Limits {
    /// `gateway.max_connections`.
    pub max_connections: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 20_000.0,
        }
    }
}

/// One thing a rule found.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    /// The rule.
    pub rule: String,
    /// What it is about, for deduplication: the rule and the series id.
    pub key: String,
    /// `low`, `medium` or `high`.
    pub severity: String,
    /// The series.
    pub series: String,
    /// The series' labels.
    pub labels: BTreeMap<String, String>,
    /// What was found, in one line, with its numbers. Label values (ids)
    /// appear as they are; the egress gate replaces them.
    pub text: String,
    /// The value that triggered it.
    pub value: f64,
    /// Days until the threshold is reached, for projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<f64>,
}

impl Finding {
    fn new(rule: &str, r: &SeriesRecord, severity: &str, text: String, value: f64) -> Self {
        Self {
            rule: rule.to_owned(),
            key: format!("{rule}-{}", series_id(&r.name, &r.labels)),
            severity: severity.to_owned(),
            series: r.name.clone(),
            labels: r.labels.clone(),
            text,
            value,
            days: None,
        }
    }

    fn in_days(mut self, d: f64) -> Self {
        self.days = Some(d);
        self
    }
}

/// `(now's estimate, slope per day)` of a straight line through the
/// hourly means of the last [`FIT_HOURS`], or `None` with fewer than
/// [`FIT_MIN_POINTS`] points.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn fit(points: &[Point], now_ms: u64) -> Option<(f64, f64)> {
    let now_h = now_ms / HOUR_MS;
    let pts: Vec<&Point> = points
        .iter()
        .filter(|p| p.t + FIT_HOURS > now_h && p.t <= now_h)
        .collect();
    if pts.len() < FIT_MIN_POINTS {
        return None;
    }
    let n = pts.len() as f64;
    let xs: Vec<f64> = pts.iter().map(|p| p.t as f64 - now_h as f64).collect();
    let mx = xs.iter().sum::<f64>() / n;
    let my = pts.iter().map(|p| p.mean).sum::<f64>() / n;
    let mut sxy = 0.0;
    let mut sxx = 0.0;
    for (x, p) in xs.iter().zip(&pts) {
        sxy += (x - mx) * (p.mean - my);
        sxx += (x - mx) * (x - mx);
    }
    if sxx <= 0.0 {
        return None;
    }
    let slope_h = sxy / sxx;
    let now = my - slope_h * mx;
    Some((now, slope_h * 24.0))
}

/// Days until a rising line reaches `threshold`, if it does within
/// `within` days and isn't there yet.
#[must_use]
pub fn days_to(points: &[Point], now_ms: u64, threshold: f64, within: f64) -> Option<f64> {
    let (now, slope) = fit(points, now_ms)?;
    if slope <= 0.0 || now >= threshold {
        return None;
    }
    let d = (threshold - now) / slope;
    (d <= within).then_some(d)
}

fn label<'a>(r: &'a SeriesRecord, k: &str) -> &'a str {
    r.labels.get(k).map_or("?", String::as_str)
}

fn pct(x: f64) -> String {
    format!("{:.0}%", x * 100.0)
}

fn days(d: f64) -> String {
    if d < 1.0 {
        "less than a day".to_owned()
    } else {
        format!("about {:.0} days", d.round())
    }
}

/// The last `n` hourly points, if there are that many in the last `n`
/// hours.
fn last_hours(r: &SeriesRecord, now_ms: u64, n: u64) -> Option<&[Point]> {
    let pts = r.hours_since(now_ms.saturating_sub((n - 1) * HOUR_MS));
    (pts.len() as u64 >= n).then_some(pts)
}

/// Every finding over `series` at `now_ms`, highest severity first.
#[must_use]
pub fn evaluate(series: &[SeriesRecord], limits: &Limits, now_ms: u64) -> Vec<Finding> {
    let mut out = Vec::new();
    for r in series {
        // Stale series (nothing in the last 3 hours) say nothing about now.
        if r.latest().is_none_or(|p| p.t + 3 < now_ms / HOUR_MS) {
            continue;
        }
        match r.name.as_str() {
            "sprig_shard_split_ratio" => shard_split(r, now_ms, &mut out),
            "sprig_balancer_cluster_full_ratio" => cluster_full(r, now_ms, &mut out),
            "sprig_backend_replica_lag_seconds" => replica_lag(r, now_ms, &mut out),
            "sprig_cluster_health_state" => cluster_health(r, now_ms, &mut out),
            "sprig_gateway_connections" => connections(r, limits, now_ms, &mut out),
            "sprig_backup_last_success_timestamp_seconds" => backup_age(r, now_ms, &mut out),
            "sprig_mover_oldest_waiting_seconds" => mover_backlog(r, &mut out),
            _ => {}
        }
    }
    let rank = |s: &str| match s {
        "high" => 0,
        "medium" => 1,
        _ => 2,
    };
    out.sort_by(|a, b| {
        rank(&a.severity)
            .cmp(&rank(&b.severity))
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

fn shard_split(r: &SeriesRecord, now_ms: u64, out: &mut Vec<Finding>) {
    let subject = format!(
        "table group {} of tenant {}",
        label(r, "group"),
        label(r, "tenant")
    );
    if let Some(day) = last_hours(r, now_ms, 24)
        && day.iter().all(|p| p.min >= 1.0)
    {
        let last = day.last().map_or(0.0, |p| p.last);
        out.push(Finding::new(
            "shard_split",
            r,
            "medium",
            format!(
                "The largest shard of {subject} has been above its split size for 24 hours ({} of sharding.split_size_gb now) and hasn't split.",
                pct(last)
            ),
            last,
        ));
        return;
    }
    let last = r.latest().map_or(0.0, |p| p.last);
    if let Some(d) = days_to(&r.hours, now_ms, 1.0, PROJECT_DAYS) {
        out.push(
            Finding::new(
                "shard_split",
                r,
                "low",
                format!(
                    "The largest shard of {subject} is at {} of its split size and growing; it reaches it in {} and then splits by itself.",
                    pct(last),
                    days(d)
                ),
                last,
            )
            .in_days(d),
        );
    }
}

fn cluster_full(r: &SeriesRecord, now_ms: u64, out: &mut Vec<Finding>) {
    let subject = format!(
        "Cluster {} ({} is its bottleneck)",
        label(r, "cluster"),
        label(r, "dimension")
    );
    let last = r.latest().map_or(0.0, |p| p.last);
    if last >= FULL_RATIO {
        out.push(Finding::new(
            "cluster_full",
            r,
            "high",
            format!(
                "{subject} is {} full, at or over the {} the balancer treats as full.",
                pct(last),
                pct(FULL_RATIO)
            ),
            last,
        ));
    } else if let Some(d) = days_to(&r.hours, now_ms, FULL_RATIO, PROJECT_DAYS) {
        out.push(
            Finding::new(
                "cluster_full",
                r,
                "medium",
                format!(
                    "{subject} is {} full and filling; at this rate it reaches {} in {}.",
                    pct(last),
                    pct(FULL_RATIO),
                    days(d)
                ),
                last,
            )
            .in_days(d),
        );
    }
}

fn replica_lag(r: &SeriesRecord, now_ms: u64, out: &mut Vec<Finding>) {
    let c = label(r, "cluster");
    if let Some(h) = last_hours(r, now_ms, 3)
        && h.iter().all(|p| p.mean >= LAG_HIGH_SECONDS)
    {
        let last = h.last().map_or(0.0, |p| p.last);
        out.push(Finding::new(
            "replica_lag",
            r,
            "high",
            format!(
                "Replication lag on cluster {c} has stayed at {LAG_HIGH_SECONDS:.0} s or more for 3 hours (hourly means {}).",
                h.iter()
                    .map(|p| format!("{:.0} s", p.mean))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            last,
        ));
        return;
    }
    let today = now_ms / DAY_MS;
    // The last three whole days: today has only begun.
    let peaks: Vec<&Point> = r
        .days
        .iter()
        .filter(|p| p.t < today && p.t + 4 > today)
        .collect();
    if peaks.len() == 3
        && peaks.windows(2).all(|w| w[1].max > w[0].max)
        && peaks[2].max >= LAG_RISING_SECONDS
    {
        out.push(Finding::new(
            "replica_lag",
            r,
            "medium",
            format!(
                "The daily peak of replication lag on cluster {c} rose three days running: {}.",
                peaks
                    .iter()
                    .map(|p| format!("{:.0} s", p.max))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            peaks[2].max,
        ));
    }
}

fn cluster_health(r: &SeriesRecord, now_ms: u64, out: &mut Vec<Finding>) {
    if let Some(h) = last_hours(r, now_ms, 3)
        && h.iter().all(|p| p.min >= 1.0)
    {
        let state = match h.last().map_or(0.0, |p| p.last) {
            x if x >= 3.0 => "unknown (no probe of its writer succeeds)",
            x if x >= 2.0 => "without a writer",
            _ => "degraded",
        };
        out.push(Finding::new(
            "cluster_health",
            r,
            "high",
            format!(
                "Cluster {} has not been healthy for 3 hours; it is {state} now.",
                label(r, "cluster")
            ),
            h.last().map_or(0.0, |p| p.last),
        ));
    }
}

fn connections(r: &SeriesRecord, limits: &Limits, now_ms: u64, out: &mut Vec<Finding>) {
    let limit = limits.max_connections.max(1.0);
    let g = label(r, "gateway");
    let day = r.hours_since(now_ms.saturating_sub(23 * HOUR_MS));
    let peak = day.iter().map(|p| p.max).fold(0.0, f64::max);
    if peak / limit >= CONNECTIONS_PEAK {
        out.push(Finding::new(
            "connections",
            r,
            "medium",
            format!(
                "Gateway {g} peaked at {peak:.0} client sessions in the last day, {} of gateway.max_connections ({limit:.0}).",
                pct(peak / limit)
            ),
            peak,
        ));
        return;
    }
    let scaled: Vec<Point> = r
        .hours
        .iter()
        .map(|p| Point {
            mean: p.mean / limit,
            ..*p
        })
        .collect();
    if let Some(d) = days_to(
        &scaled,
        now_ms,
        CONNECTIONS_PROJECTED,
        PROJECT_CONNECTION_DAYS,
    ) {
        out.push(
            Finding::new(
                "connections",
                r,
                "low",
                format!(
                    "Client sessions on gateway {g} are growing; at this rate they reach {} of gateway.max_connections ({limit:.0}) in {}.",
                    pct(CONNECTIONS_PROJECTED),
                    days(d)
                ),
                peak,
            )
            .in_days(d),
        );
    }
}

#[allow(clippy::cast_precision_loss)]
fn backup_age(r: &SeriesRecord, now_ms: u64, out: &mut Vec<Finding>) {
    let Some(last) = r.latest().map(|p| p.last) else {
        return;
    };
    let age_h = (now_ms as f64 / 1000.0 - last) / 3600.0;
    let sev = if age_h >= BACKUP_HOURS_HIGH {
        "high"
    } else if age_h >= BACKUP_HOURS {
        "medium"
    } else {
        return;
    };
    out.push(Finding::new(
        "backup_age",
        r,
        sev,
        format!(
            "The newest successful {} backup of cluster {} finished {age_h:.0} hours ago.",
            label(r, "kind"),
            label(r, "cluster")
        ),
        age_h,
    ));
}

fn mover_backlog(r: &SeriesRecord, out: &mut Vec<Finding>) {
    let Some(last) = r.latest().map(|p| p.last) else {
        return;
    };
    if last >= MOVER_WAIT_SECONDS {
        out.push(Finding::new(
            "mover_backlog",
            r,
            "medium",
            format!(
                "A data-move job has waited {:.1} hours for a worker.",
                last / 3600.0
            ),
            last,
        ));
    }
}
