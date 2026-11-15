//! The administration agent's metric history: a few series sampled on the
//! DC leader, kept small enough for etcd.
//!
//! ```text
//! every SAMPLE_INTERVAL (the health loop): samples -> this hour's point per
//!     series, in memory (count, mean, min, max, last)
//! every FLUSH_INTERVAL and at each new hour: the points of the hours seen
//!     are written into ai/metrics/<series id> (DC tier), replacing that
//!     hour's point; the record's days are recomputed from its hours
//! a record keeps HOURS_KEPT hourly points and DAYS_KEPT daily points;
//!     at most MAX_SERIES records; one not updated for HOURS_KEPT is dropped
//! ```
//!
//! Writing an hour's whole point (not adding to it) makes a flush
//! idempotent; a new DC leader starts its hour over, so at most the part
//! of one hour before the change is lost.
//!
//! The series hold no names of their own: label values are ids (clusters,
//! gateways, `t<id>` for tenants), and every one of them is registered
//! with the egress gate's scrubber before anything leaves.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

/// The DC-tier prefix of the series records.
pub const PREFIX: &str = "ai/metrics/";
/// Hourly points kept per series: 14 days.
pub const HOURS_KEPT: u64 = 14 * 24;
/// Daily points kept per series.
pub const DAYS_KEPT: u64 = 30;
/// Series kept per DC at most.
pub const MAX_SERIES: usize = 512;
/// How often the health loop samples.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(300);
/// How often the samples are written to etcd (and at each new hour).
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(900);

/// One hour, in ms.
pub const HOUR_MS: u64 = 3_600_000;
/// One day, in ms.
pub const DAY_MS: u64 = 24 * HOUR_MS;

/// The series the history keeps, with what each measures. The health rules
/// read them; `metrics_trend` serves them.
pub const SERIES: &[(&str, &str)] = &[
    (
        "sprig_shard_split_ratio",
        "the largest shard of a table group over its split size (sharding.split_size_gb); labels tenant (t<id>) and group",
    ),
    (
        "sprig_balancer_cluster_full_ratio",
        "how full a cluster is, 0 to 1, by its bottleneck dimension (storage, connections, ...); labels cluster and dimension",
    ),
    (
        "sprig_backend_replica_lag_seconds",
        "the largest replication lag among a cluster's replicas, from the controller's probes; label cluster",
    ),
    (
        "sprig_cluster_health_state",
        "a cluster's health: 0 healthy, 1 degraded, 2 no writer, 3 unknown; label cluster",
    ),
    (
        "sprig_gateway_connections",
        "open client sessions on a gateway (gateway.max_connections is its limit); label gateway",
    ),
    (
        "sprig_backup_last_success_timestamp_seconds",
        "when a cluster's newest successful backup of a kind finished, Unix seconds; labels cluster and kind",
    ),
    (
        "sprig_mover_jobs_waiting",
        "data-move jobs created and waiting for a worker",
    ),
    (
        "sprig_mover_oldest_waiting_seconds",
        "how long the oldest waiting data-move job has waited",
    ),
];

/// One sampled value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// The series name (one of [`SERIES`]).
    pub name: String,
    /// Its labels.
    pub labels: BTreeMap<String, String>,
    /// The value.
    pub value: f64,
}

impl Sample {
    /// A sample of `name` with `labels`.
    #[must_use]
    pub fn new(name: &str, labels: &[(&str, &str)], value: f64) -> Self {
        Self {
            name: name.to_owned(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            value,
        }
    }
}

/// A series' id: 16 hex digits of SHA-256 over the name and labels.
#[must_use]
pub fn series_id(name: &str, labels: &BTreeMap<String, String>) -> String {
    let mut h = Sha256::new();
    h.update(name.as_bytes());
    for (k, v) in labels {
        h.update([0]);
        h.update(k.as_bytes());
        h.update([1]);
        h.update(v.as_bytes());
    }
    let d = h.finalize();
    let mut s = String::with_capacity(16);
    for b in &d[..8] {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// One hour's or one day's values.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// The hour (or day) since the Unix epoch.
    pub t: u64,
    /// Samples in it.
    pub n: u32,
    /// Their mean.
    pub mean: f64,
    /// The smallest.
    pub min: f64,
    /// The largest.
    pub max: f64,
    /// The last one.
    pub last: f64,
}

impl Point {
    /// A point of one sample.
    #[must_use]
    pub fn of(t: u64, v: f64) -> Self {
        Self {
            t,
            n: 1,
            mean: v,
            min: v,
            max: v,
            last: v,
        }
    }

    /// Adds a later sample.
    pub fn add(&mut self, v: f64) {
        let n = f64::from(self.n);
        self.mean = (self.mean * n + v) / (n + 1.0);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.last = v;
        self.n = self.n.saturating_add(1);
    }

    /// Adds a later point (weighted by its samples).
    fn merge(&mut self, o: &Point) {
        let (a, b) = (f64::from(self.n), f64::from(o.n));
        if a + b > 0.0 {
            self.mean = (self.mean * a + o.mean * b) / (a + b);
        }
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
        self.last = o.last;
        self.n = self.n.saturating_add(o.n);
    }
}

/// `ai/metrics/<id>`: one series' points.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesRecord {
    /// The series name.
    pub name: String,
    /// Its labels.
    pub labels: BTreeMap<String, String>,
    /// Hourly points, oldest first.
    pub hours: Vec<Point>,
    /// Daily points, oldest first.
    pub days: Vec<Point>,
    /// Last written, ms since the epoch.
    pub updated_ms: u64,
}

impl Versioned for SeriesRecord {
    const V: u32 = 1;
}

impl SeriesRecord {
    /// An empty record.
    #[must_use]
    pub fn new(name: &str, labels: &BTreeMap<String, String>) -> Self {
        Self {
            name: name.to_owned(),
            labels: labels.clone(),
            hours: Vec::new(),
            days: Vec::new(),
            updated_ms: 0,
        }
    }

    /// Its id.
    #[must_use]
    pub fn id(&self) -> String {
        series_id(&self.name, &self.labels)
    }

    /// Sets hour `p.t`'s point (replacing what was there), then keeps the
    /// windows and recomputes the days the hours cover.
    pub fn set_hour(&mut self, p: Point, now_ms: u64) {
        match self.hours.binary_search_by_key(&p.t, |x| x.t) {
            Ok(i) => self.hours[i] = p,
            Err(i) => self.hours.insert(i, p),
        }
        let now_h = now_ms / HOUR_MS;
        self.hours
            .retain(|x| x.t + HOURS_KEPT > now_h && x.t <= now_h + 1);
        let mut days: BTreeMap<u64, Point> = BTreeMap::new();
        for h in &self.hours {
            let d = h.t / 24;
            match days.get_mut(&d) {
                Some(x) => x.merge(&Point { t: d, ..*h }),
                None => {
                    days.insert(d, Point { t: d, ..*h });
                }
            }
        }
        let first = days.keys().next().copied().unwrap_or(u64::MAX);
        let now_d = now_ms / DAY_MS;
        let mut kept: Vec<Point> = self
            .days
            .iter()
            .filter(|x| x.t < first && x.t + DAYS_KEPT > now_d)
            .copied()
            .collect();
        kept.extend(days.into_values());
        self.days = kept;
        self.updated_ms = self.updated_ms.max(now_ms);
    }

    /// The hourly points from `from_ms` on.
    #[must_use]
    pub fn hours_since(&self, from_ms: u64) -> &[Point] {
        let from = from_ms / HOUR_MS;
        let i = self.hours.partition_point(|p| p.t < from);
        &self.hours[i..]
    }

    /// The newest point.
    #[must_use]
    pub fn latest(&self) -> Option<&Point> {
        self.hours.last()
    }
}

/// `YYYY-MM-DD` of day `d` since the Unix epoch.
#[must_use]
pub fn day_text(d: u64) -> String {
    // Civil from days (Howard Hinnant's algorithm), for days >= 0.
    #[allow(clippy::cast_possible_wrap)]
    let z = d as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// `YYYY-MM-DDTHH:00Z` of hour `h` since the Unix epoch.
#[must_use]
pub fn hour_text(h: u64) -> String {
    format!("{}T{:02}:00Z", day_text(h / 24), h % 24)
}

/// `YYYY-MM-DDTHH:MMZ` of `ms` since the Unix epoch.
#[must_use]
pub fn time_text(ms: u64) -> String {
    let min = ms / 60_000;
    format!(
        "{}T{:02}:{:02}Z",
        day_text(min / 1440),
        (min / 60) % 24,
        min % 60
    )
}

/// A number for people and models: three significant digits at most.
#[must_use]
pub fn num(x: f64) -> Value {
    if !x.is_finite() {
        return Value::Null;
    }
    if x.fract() == 0.0 && x.abs() < 1e15 {
        #[allow(clippy::cast_possible_truncation)]
        return json!(x as i64);
    }
    let digits = if x.abs() >= 100.0 {
        0
    } else if x.abs() >= 1.0 {
        2
    } else {
        3
    };
    let s = format!("{x:.digits$}");
    serde_json::from_str(&s).unwrap_or(Value::Null)
}

/// A series' name, labels and points not yet written.
type Held = (String, BTreeMap<String, String>, BTreeMap<u64, Point>);
/// A series to write: its id, name, labels and points.
type ToWrite = (String, String, BTreeMap<String, String>, Vec<Point>);

#[derive(Debug, Default)]
struct Pending {
    /// Per series id.
    series: BTreeMap<String, Held>,
    last_flush_ms: u64,
}

/// The history over a DC tier.
pub struct History {
    dc: Arc<dyn MetaStore>,
    pending: Mutex<Pending>,
}

impl std::fmt::Debug for History {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("History").finish_non_exhaustive()
    }
}

/// What [`History::trend`] reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrendQuery {
    /// The series name.
    pub name: String,
    /// `label=value` filters, all of which must match.
    pub labels: Vec<(String, String)>,
    /// Days to look back, 1 to [`DAYS_KEPT`].
    pub days: u64,
}

impl TrendQuery {
    /// From a tool's input: `name`, `labels` (`["k=v", ...]`) and `days`.
    #[must_use]
    pub fn from_input(input: &Value) -> Self {
        let labels = input["labels"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
            .collect();
        Self {
            name: input["name"].as_str().unwrap_or("").trim().to_owned(),
            labels,
            days: input["days"].as_u64().unwrap_or(7).clamp(1, DAYS_KEPT),
        }
    }
}

/// Series shown in one trend answer at most.
const MAX_TREND_SERIES: usize = 20;

impl History {
    /// The history in `dc` (a DC tier).
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self {
            dc,
            pending: Mutex::new(Pending::default()),
        }
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds `samples` taken at `now_ms` to this hour's points.
    pub fn record(&self, samples: &[Sample], now_ms: u64) {
        let h = now_ms / HOUR_MS;
        let mut p = self.pending();
        for s in samples {
            if !s.value.is_finite() {
                continue;
            }
            let id = series_id(&s.name, &s.labels);
            let n = p.series.len();
            let e = match p.series.get_mut(&id) {
                Some(e) => e,
                None if n >= MAX_SERIES => continue,
                None => p
                    .series
                    .entry(id)
                    .or_insert_with(|| (s.name.clone(), s.labels.clone(), BTreeMap::new())),
            };
            match e.2.get_mut(&h) {
                Some(pt) => pt.add(s.value),
                None => {
                    e.2.insert(h, Point::of(h, s.value));
                }
            }
        }
    }

    /// Whether a flush is due at `now_ms`: [`FLUSH_INTERVAL`] after the
    /// last, or a new hour.
    #[must_use]
    pub fn flush_due(&self, now_ms: u64) -> bool {
        let p = self.pending();
        let last = p.last_flush_ms;
        let every = u64::try_from(FLUSH_INTERVAL.as_millis()).unwrap_or(u64::MAX);
        last == 0 || now_ms.saturating_sub(last) >= every || now_ms / HOUR_MS != last / HOUR_MS
    }

    /// Writes the points held to their records; points of past hours are
    /// then dropped from memory. Returns the records written.
    ///
    /// # Errors
    /// The store's.
    pub async fn flush(&self, now_ms: u64) -> Result<usize> {
        let todo: Vec<ToWrite> = {
            let mut p = self.pending();
            p.last_flush_ms = now_ms;
            p.series
                .iter()
                .map(|(id, (name, labels, pts))| {
                    (
                        id.clone(),
                        name.clone(),
                        labels.clone(),
                        pts.values().copied().collect(),
                    )
                })
                .collect()
        };
        let mut known: Option<BTreeSet<String>> = None;
        let mut written = 0;
        for (id, name, labels, pts) in todo {
            let key = self.dc.tier().key(&format!("{PREFIX}{id}"))?;
            for _ in 0..3 {
                let cur = self.dc.get(&key, Consistency::Linearizable).await?;
                let (mut rec, rev) = if let Some(kv) = &cur {
                    (
                        Record::<SeriesRecord>::decode(&kv.value)
                            .map_or_else(|_| SeriesRecord::new(&name, &labels), |r| r.body),
                        Some(kv.mod_revision),
                    )
                } else {
                    if known.is_none() {
                        known = Some(self.ids().await?);
                    }
                    if known.as_ref().is_some_and(|k| k.len() >= MAX_SERIES) {
                        tracing::debug!(series = %name, "metric history full: a new series isn't kept");
                        break;
                    }
                    (SeriesRecord::new(&name, &labels), None)
                };
                for p in &pts {
                    rec.set_hour(*p, now_ms);
                }
                let when = match rev {
                    Some(r) => vec![Cmp::ModRev(key.clone(), r)],
                    None => vec![Cmp::Missing(key.clone())],
                };
                let r = self
                    .dc
                    .txn(Txn {
                        when,
                        then: vec![Op::Put {
                            key: key.clone(),
                            value: Record::new(rec).encode()?,
                            lease: None,
                        }],
                        otherwise: Vec::new(),
                    })
                    .await?;
                if r.succeeded {
                    written += 1;
                    if let Some(k) = known.as_mut() {
                        k.insert(id.clone());
                    }
                    break;
                }
            }
        }
        // Past hours are written; this hour goes on accumulating.
        let h = now_ms / HOUR_MS;
        let mut p = self.pending();
        for (_, _, pts) in p.series.values_mut() {
            pts.retain(|t, _| *t >= h);
        }
        p.series.retain(|_, (_, _, pts)| !pts.is_empty());
        Ok(written)
    }

    async fn ids(&self) -> Result<BTreeSet<String>> {
        let prefix = self.dc.tier().key(PREFIX)?;
        let mut out = BTreeSet::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Some(id) = kv.key.as_str().rsplit('/').next() {
                    out.insert(id.to_owned());
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    /// Every series, with this hour's points held in memory merged in.
    ///
    /// # Errors
    /// The store's.
    pub async fn series(&self, now_ms: u64) -> Result<Vec<SeriesRecord>> {
        let prefix = self.dc.tier().key(PREFIX)?;
        let mut out: BTreeMap<String, SeriesRecord> = BTreeMap::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<SeriesRecord>::decode(&kv.value) {
                    Ok(r) => {
                        out.insert(r.body.id(), r.body);
                    }
                    Err(e) => {
                        tracing::debug!(key = %kv.key, error = %e, "unreadable metric series");
                    }
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        let p = self.pending();
        for (id, (name, labels, pts)) in &p.series {
            let rec = out
                .entry(id.clone())
                .or_insert_with(|| SeriesRecord::new(name, labels));
            // Points held in memory don't count as written.
            let written = rec.updated_ms;
            for pt in pts.values() {
                rec.set_hour(*pt, now_ms);
            }
            rec.updated_ms = written;
        }
        Ok(out.into_values().collect())
    }

    /// Drops records not updated for [`HOURS_KEPT`] hours; how many.
    ///
    /// # Errors
    /// The store's.
    pub async fn prune(&self, now_ms: u64) -> Result<usize> {
        let mut n = 0;
        for r in self.series(now_ms).await? {
            if r.updated_ms > 0 && r.updated_ms + HOURS_KEPT * HOUR_MS < now_ms {
                let key = self.dc.tier().key(&format!("{PREFIX}{}", r.id()))?;
                self.dc
                    .txn(Txn {
                        when: Vec::new(),
                        then: vec![Op::Delete(key)],
                        otherwise: Vec::new(),
                    })
                    .await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// `metrics_trend`'s answer: each matching series' daily points over
    /// the last `days` days and its last 24 hourly points.
    ///
    /// # Errors
    /// 39792 for a series the history doesn't keep (the answer lists the
    /// ones it does); the store's.
    pub async fn trend(&self, q: &TrendQuery, now_ms: u64) -> Result<Value> {
        let all = self.series(now_ms).await?;
        trend_of(&all, q, now_ms)
    }
}

/// [`History::trend`] over `all`.
///
/// # Errors
/// 39792 for a series the history doesn't keep.
pub fn trend_of(all: &[SeriesRecord], q: &TrendQuery, now_ms: u64) -> Result<Value> {
    if !SERIES.iter().any(|(n, _)| *n == q.name) {
        let kept: Vec<String> = SERIES.iter().map(|(n, d)| format!("{n}: {d}")).collect();
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!(
                "the metric history doesn't keep {:?}; it keeps: {}",
                q.name,
                kept.join("; ")
            ),
        ));
    }
    let days = q.days.clamp(1, DAYS_KEPT);
    let from_d = (now_ms / DAY_MS).saturating_sub(days - 1);
    let from_h = (now_ms / HOUR_MS).saturating_sub(23);
    let matching: Vec<&SeriesRecord> = all
        .iter()
        .filter(|r| r.name == q.name)
        .filter(|r| {
            q.labels
                .iter()
                .all(|(k, v)| r.labels.get(k).is_some_and(|x| x == v))
        })
        .collect();
    let more = matching.len() > MAX_TREND_SERIES;
    let series: Vec<Value> = matching
        .iter()
        .take(MAX_TREND_SERIES)
        .map(|r| {
            let daily: Vec<Value> = r
                .days
                .iter()
                .filter(|p| p.t >= from_d)
                .map(|p| {
                    json!({"day": day_text(p.t), "mean": num(p.mean), "min": num(p.min),
                           "max": num(p.max), "last": num(p.last)})
                })
                .collect();
            let hourly: Vec<Value> = r
                .hours
                .iter()
                .filter(|p| p.t >= from_h)
                .map(|p| json!({"hour": hour_text(p.t), "mean": num(p.mean), "max": num(p.max)}))
                .collect();
            json!({"labels": r.labels, "days": daily, "last_24_hours": hourly})
        })
        .collect();
    let about = SERIES
        .iter()
        .find(|(n, _)| *n == q.name)
        .map_or("", |(_, d)| *d);
    Ok(json!({
        "name": q.name,
        "about": about,
        "days": days,
        "series": series,
        "more": more,
        "note": if series.is_empty() { "no samples yet: the history starts when ai.admin.enabled is turned on" } else { "" },
    }))
}
