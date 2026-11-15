//! What the administration agent is told about an incident, and how its
//! answers are read.
//!
//! The intake is the incident's fingerprint, code and name, component,
//! span, counts, versions and a topology summary (counts and kinds), and
//! nothing else:
//!
//! ```text
//! Incident fingerprint 3f09a1c2b7de: code 39100 (ER_SPRIG_CLUSTER_UNAVAILABLE), component controller, span loop.balancer.
//! Count: 12 in the last hour; 7-day baseline 0.2 per hour. 40 since first seen 3 hours ago, from 2 processes.
//! New fingerprint.
//! Topology: 1 DC; this DC has 2 gateways, 1 worker and 2 clusters (1 primary_replica of 3 nodes, 1 galera of 3 nodes).
//! Versions: SprigDB 0.9.0; MariaDB 11.8.2.
//! ```
//!
//! Every name that could appear (DCs, clusters, nodes and their
//! addresses, processes, tenants) is registered with the egress gate's
//! scrubber before the text leaves ([`Names`]), so even a name that found
//! its way into a span becomes a placeholder. Counts of a million or more
//! are written with separators, so the scrubber doesn't take them for
//! values.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::record::{IncidentRecord, Why};
use crate::egress::{NameKind, Scrubber};

/// What triage is told, after its one-line identity.
pub const TRIAGE_RULES: &str = "\
You read one incident from SprigDB's error intake: an error code, where it happened, counts against its 7-day baseline, versions and the topology. No data, no names.
- noise: expected application behaviour at about its usual rate (syntax errors, duplicate keys, missing tables, lock waits, failed or throttled logins), or a limit doing its job at its usual rate.
- needs_advice: a fault in SprigDB or its servers (clusters, catalogs, backends, pools, replication, jobs), a rate well above its baseline, anything about data safety or an internal error, or anything new that SprigDB itself raised.
Answer with exactly one word: noise or needs_advice.";

/// What the advice turn is told, after the administration prompt.
pub const ADVICE_RULES: &str = "\
You are advising SprigDB's operators about one incident from the error intake. You know only what the intake says: codes, components, spans, counts, versions and the topology; never data or names.
- First look the error and its symptoms up with search_docs, audience operator. Base the advice on what the docs say, and say when you are unsure.
- Name a metric, setting, error code or command only when a search result shows it, exactly as written there; search for the metric or setting you need (the metrics and settings references are operator pages). Never guess a name.
- Then answer in this shape, briefly:
Severity: low, medium, high or critical
**Cause:** the most likely cause, in one or two sentences.
**Steps:**
1. what to check or do first, in order.
**Docs:** the pages you used.
- Your answer becomes an advisory item in the approvals inbox. Nothing is changed by it: never say a change was made, and never propose running anything automatically.";

/// The topology summary: counts and kinds only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// Data centers.
    pub dcs: u32,
    /// Gateways registered in this DC.
    pub gateways: u32,
    /// Workers registered in this DC.
    pub workers: u32,
    /// This DC's clusters in service.
    pub clusters: Vec<ClusterShape>,
}

/// One cluster's shape.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClusterShape {
    /// `primary_replica`, `galera`, `single_node` or `external`.
    pub kind: String,
    /// Its nodes.
    pub nodes: u32,
    /// The server version, `major.minor.patch`, when probed.
    pub server: Option<String>,
}

fn plural(n: u32, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

impl Topology {
    /// `Topology: ...` (module docs).
    #[must_use]
    pub fn summary(&self) -> String {
        let mut shapes: BTreeMap<(&str, u32), u32> = BTreeMap::new();
        for c in &self.clusters {
            *shapes.entry((c.kind.as_str(), c.nodes)).or_default() += 1;
        }
        let shapes: Vec<String> = shapes
            .iter()
            .map(|((kind, nodes), n)| format!("{n} {kind} of {}", plural(*nodes, "node", "nodes")))
            .collect();
        let n = u32::try_from(self.clusters.len()).unwrap_or(u32::MAX);
        let clusters = if shapes.is_empty() {
            plural(n, "cluster", "clusters")
        } else {
            format!(
                "{} ({})",
                plural(n, "cluster", "clusters"),
                shapes.join(", ")
            )
        };
        format!(
            "Topology: {}; this DC has {}, {} and {}.",
            plural(self.dcs.max(1), "DC", "DCs"),
            plural(self.gateways, "gateway", "gateways"),
            plural(self.workers, "worker", "workers"),
            clusters
        )
    }

    /// The server versions, lowest first.
    #[must_use]
    pub fn servers(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .clusters
            .iter()
            .filter_map(|c| c.server.clone())
            .collect();
        v.sort();
        v.dedup();
        v
    }
}

/// Every name the intake might contain, for the scrubber.
#[derive(Debug, Clone, Default)]
pub struct Names {
    names: Vec<(NameKind, String)>,
}

impl Names {
    /// No names.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `name` as a `kind`, and each of its parts around `:`, `@` and
    /// `/` (a host of a `host:port`).
    pub fn add(&mut self, kind: NameKind, name: &str) {
        let name = name.trim();
        if name.is_empty() {
            return;
        }
        self.names.push((kind, name.to_owned()));
        for part in name.split([':', '@', '/']) {
            if !part.is_empty() && part != name && !part.bytes().all(|b| b.is_ascii_digit()) {
                self.names.push((kind, part.to_owned()));
            }
        }
        // A host's first label (`db-eu-2` of `db-eu-2.internal:3306`).
        if matches!(kind, NameKind::Host | NameKind::Address)
            && let Some(label) = name.split([':', '@', '/']).find(|p| !p.is_empty())
            && let Some((first, _)) = label.split_once('.')
            && first.len() >= 3
            && !first.bytes().all(|b| b.is_ascii_digit())
        {
            self.names.push((kind, first.to_owned()));
        }
    }

    /// How many.
    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Every name, as added.
    pub fn iter(&self) -> impl Iterator<Item = &(NameKind, String)> {
        self.names.iter()
    }

    /// A scrubber with a fresh key and every name registered.
    #[must_use]
    pub fn scrubber(&self) -> Scrubber {
        let mut s = Scrubber::random();
        for (k, n) in &self.names {
            s.add(*k, n);
        }
        s
    }
}

/// `n` for the text: with `,` separators from a million up, so the
/// scrubber never reads a count as a value.
#[must_use]
pub fn count(n: u64) -> String {
    let digits = n.to_string();
    if digits.len() < 7 {
        return digits;
    }
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[allow(clippy::cast_precision_loss)]
fn rate(x: f64) -> String {
    if x >= 1_000_000.0 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        return count(x.round() as u64);
    }
    if x >= 10.0 || x == 0.0 {
        format!("{x:.0}")
    } else {
        format!("{x:.1}")
    }
}

fn ago(ms: u64) -> String {
    let h = ms / 3_600_000;
    match h {
        0 => "less than an hour ago".to_owned(),
        1 => "1 hour ago".to_owned(),
        2..48 => format!("{h} hours ago"),
        _ => format!("{} days ago", h / 24),
    }
}

/// The intake text of `rec` now (module docs). `why` is why it goes
/// further; `version` is this build's.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn intake(rec: &IncidentRecord, why: Why, topology: &Topology, now_ms: u64) -> String {
    let last = rec.last_hour(now_ms);
    let base = rec.baseline(now_ms);
    let name = if rec.code_name.is_empty() {
        if rec.code == 0 {
            "no code".to_owned()
        } else {
            "a MariaDB error".to_owned()
        }
    } else {
        rec.code_name.clone()
    };
    let mut t = String::new();
    let _ = writeln!(
        t,
        "Incident fingerprint {}: code {} ({name}), component {}, span {}.",
        rec.fingerprint, rec.code, rec.component, rec.span
    );
    let _ = writeln!(
        t,
        "Count: {} in the last hour; 7-day baseline {} per hour. {} since first seen {}, from {}.",
        count(last),
        rate(base),
        count(rec.total),
        ago(now_ms.saturating_sub(rec.first_seen_ms)),
        plural(rec.processes.max(1), "process", "processes")
    );
    let _ = match why {
        Why::New => writeln!(t, "New fingerprint."),
        Why::Critical => writeln!(t, "Critical code."),
        Why::Rising if base > 0.0 => {
            writeln!(t, "Rising: {}x its baseline.", rate(last as f64 / base))
        }
        Why::Rising => writeln!(t, "Rising: it had no baseline."),
    };
    let _ = writeln!(t, "{}", topology.summary());
    let mut versions = format!("Versions: SprigDB {}", rec.versions.join(", "));
    if rec.versions.is_empty() {
        versions = format!("Versions: SprigDB {}", env!("CARGO_PKG_VERSION"));
    }
    let servers = topology.servers();
    if !servers.is_empty() {
        let _ = write!(versions, "; MariaDB {}", servers.join(", "));
    }
    let _ = write!(t, "{versions}.");
    let mut cut = t.len().min(super::record::MAX_INTAKE);
    while !t.is_char_boundary(cut) {
        cut -= 1;
    }
    t.truncate(cut);
    t
}

/// Triage's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Not worth advice.
    Noise,
    /// Worth advice.
    NeedsAdvice,
}

impl Verdict {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Verdict::Noise => "noise",
            Verdict::NeedsAdvice => "needs_advice",
        }
    }

    /// Triage's answer read: `needs_advice` (or "needs advice") wins,
    /// then `noise`; anything else needs advice, so nothing is dropped on
    /// an unclear answer.
    #[must_use]
    pub fn read(answer: &str) -> Verdict {
        let a = answer.to_lowercase();
        if a.contains("needs_advice") || a.contains("needs advice") {
            Verdict::NeedsAdvice
        } else if a.contains("noise") {
            Verdict::Noise
        } else {
            Verdict::NeedsAdvice
        }
    }
}

/// Severities, lowest first.
pub const SEVERITIES: &[&str] = &["low", "medium", "high", "critical"];

/// The advice, as read from the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Advice {
    /// `low`, `medium`, `high` or `critical`.
    pub severity: String,
    /// The cause, in one line.
    pub cause: String,
    /// The whole answer.
    pub text: String,
}

impl Advice {
    /// Reads `answer`: the `Severity:` line (else `high` for a critical
    /// code, `medium` otherwise) and the cause (the `Cause:` line, else
    /// the first line of prose).
    #[must_use]
    pub fn read(answer: &str, critical: bool) -> Advice {
        let mut severity = None;
        let mut cause = None;
        for line in answer.lines() {
            let plain = line.replace(['*', '_', '#'], "");
            let plain = plain.trim();
            let low = plain.to_lowercase();
            if severity.is_none()
                && let Some(rest) = low.strip_prefix("severity")
            {
                severity = SEVERITIES
                    .iter()
                    .find(|s| rest.contains(*s))
                    .map(|s| (*s).to_owned());
            } else if cause.is_none()
                && plain
                    .get(..5)
                    .is_some_and(|p| p.eq_ignore_ascii_case("cause"))
            {
                let c = plain[5..].trim_start_matches([':', ' ']).trim();
                if !c.is_empty() {
                    cause = Some(c.to_owned());
                }
            }
        }
        let cause = cause.unwrap_or_else(|| {
            answer
                .lines()
                .map(str::trim)
                .find(|l| !l.is_empty() && !l.to_lowercase().starts_with("severity"))
                .unwrap_or("")
                .replace(['*', '#'], "")
                .trim()
                .to_owned()
        });
        Advice {
            severity: severity
                .unwrap_or_else(|| if critical { "high" } else { "medium" }.to_owned()),
            cause: clip(&cause, 300),
            text: answer.trim().to_owned(),
        }
    }
}

/// `s` cut to at most `max` bytes on a character boundary, with `…` when
/// cut.
#[must_use]
pub fn clip(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max.saturating_sub(3);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}
