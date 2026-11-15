//! Incident write-ups: after a failover, a failed move or an outage, the
//! evidence SprigDB recorded is put together and the administration model
//! drafts a timeline, cause, effects and follow-ups from it.
//!
//! ```text
//! event (a cluster failover, a DC failover, a failed data move, an incident)
//!   window := 2 hours before it to 1 hour after
//!   evidence := the event's own record (detection, decision, promotion, the
//!       former writer's fate), incidents from the error intake seen in the
//!       window, and the metric history's hourly points of the series about
//!       the same cluster (or, without one, the DC's) in the window
//!   brief(evidence) -> through the scrubber -> the admin model, with
//!       search_docs, metrics_trend and decision_log
//!   -> ai/writeups/<id> (DC tier), and an ai_incident_advice item
//! ```

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use super::history::{HOUR_MS, SeriesRecord, hour_text, time_text};

/// The DC-tier prefix of write-ups.
pub const PREFIX: &str = "ai/writeups/";
/// The window starts this long before the event.
pub const BEFORE_MS: u64 = 2 * HOUR_MS;
/// The window ends this long after it.
pub const AFTER_MS: u64 = HOUR_MS;
/// A write-up is drafted this long after its event, so the hour after it
/// is in the evidence.
pub const SETTLE_MS: u64 = 15 * 60 * 1000;
/// Events older than this aren't written up on their own.
pub const MAX_AGE_MS: u64 = 24 * HOUR_MS;
/// The longest brief sent.
pub const MAX_BRIEF: usize = 12 * 1024;

/// Something worth a write-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Its id: `cf-<cluster>-<epoch>`, `df-<failover>`, `mv-<job>` or
    /// `inc-<fingerprint>`.
    pub id: String,
    /// `cluster_failover`, `dc_failover`, `move_failed` or `incident`.
    pub kind: String,
    /// When it happened (or ended), ms since the epoch.
    pub at_ms: u64,
    /// The cluster (or DC) it is about, when there is one: series about it
    /// go into the evidence.
    pub cluster: Option<String>,
    /// What its record says, one fact per line, timed where it can be.
    pub facts: Vec<(Option<u64>, String)>,
}

/// An incident from the error intake seen in the window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    /// First seen in the window (or before it), ms.
    pub first_ms: u64,
    /// Last seen, ms.
    pub last_ms: u64,
    /// `code (NAME) in component span: n in the window`.
    pub text: String,
}

/// An event's evidence.
#[derive(Debug, Clone, PartialEq)]
pub struct Evidence {
    /// The event.
    pub event: Event,
    /// Incidents seen in the window.
    pub incidents: Vec<Seen>,
    /// The series about the event's cluster (or the DC), in the window.
    pub series: Vec<SeriesRecord>,
}

/// The window of an event at `at_ms`: `(from, to)`.
#[must_use]
pub fn window(at_ms: u64) -> (u64, u64) {
    (at_ms.saturating_sub(BEFORE_MS), at_ms + AFTER_MS)
}

/// A key segment from an id (letters, digits and `-`).
#[must_use]
pub fn safe_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(120)
        .collect()
}

fn plain(x: f64) -> String {
    if x.abs() >= 100.0 || x.fract() == 0.0 {
        format!("{x:.0}")
    } else {
        format!("{x:.2}")
    }
}

/// What the model is told about `e`: the event, its timeline, the
/// incidents and the metric points. Ids appear as they are; the caller
/// passes the text through the egress gate.
#[must_use]
pub fn brief(e: &Evidence) -> String {
    use std::fmt::Write as _;
    let ev = &e.event;
    let (from, to) = window(ev.at_ms);
    let mut t = String::new();
    let _ = writeln!(
        t,
        "Write-up of {} {} at {}. Window: {} to {}.",
        ev.kind.replace('_', " "),
        ev.id,
        time_text(ev.at_ms),
        time_text(from),
        time_text(to)
    );
    t.push_str("\nRecorded by SprigDB:\n");
    let mut facts = ev.facts.clone();
    facts.sort_by_key(|(at, _)| at.unwrap_or(ev.at_ms));
    for (at, f) in &facts {
        match at {
            Some(ms) => {
                let _ = writeln!(t, "- {} {f}", time_text(*ms));
            }
            None => {
                let _ = writeln!(t, "- {f}");
            }
        }
    }
    t.push_str("\nErrors from the error intake in the window:\n");
    if e.incidents.is_empty() {
        t.push_str("- none\n");
    }
    for s in &e.incidents {
        let _ = writeln!(
            t,
            "- {} first seen {}, last seen {}",
            s.text,
            time_text(s.first_ms),
            time_text(s.last_ms)
        );
    }
    t.push_str("\nMetric history in the window (hourly mean, max):\n");
    let (fh, th) = (from / HOUR_MS, to / HOUR_MS);
    let mut any = false;
    for s in &e.series {
        let pts: Vec<String> = s
            .hours
            .iter()
            .filter(|p| p.t >= fh && p.t <= th)
            .map(|p| {
                format!(
                    "{} {} ({})",
                    &hour_text(p.t)[11..16],
                    plain(p.mean),
                    plain(p.max)
                )
            })
            .collect();
        if pts.is_empty() {
            continue;
        }
        any = true;
        let labels: Vec<String> = s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let _ = writeln!(
            t,
            "- {}{{{}}}: {}",
            s.name,
            labels.join(","),
            pts.join(", ")
        );
    }
    if !any {
        t.push_str("- no samples (the history starts when ai.admin.enabled is on)\n");
    }
    let mut cut = t.len().min(MAX_BRIEF);
    while !t.is_char_boundary(cut) {
        cut -= 1;
    }
    t.truncate(cut);
    t
}

/// What the write-up turn is told, after the administration prompt.
pub const RULES: &str = "\
You are drafting an incident write-up for SprigDB's operators from the evidence SprigDB recorded: its own records of the event, errors from the error intake and the metric history. You never see data or names; ids appear as placeholders.
- Use only the evidence and what your tools return (search_docs with audience operator, metrics_trend, decision_log). When the evidence doesn't show a cause, say it is unknown and what would show it. Never invent times or numbers.
- Answer in this shape, briefly:
**Summary:** one or two sentences.
**Timeline:** times in UTC, one line each, from the evidence.
**Cause:** the most likely cause, and how sure you are.
**Effects:** what clients and data saw (errors, write unavailability, expected loss), from the evidence.
**Follow-ups:**
1. what to check or change, in order.
**Docs:** the pages you used.
- The write-up is filed in the approvals inbox as advice. Nothing is changed by it.";

/// `ai/writeups/<id>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteupRecord {
    /// The event's id.
    pub id: String,
    /// The event's kind.
    pub kind: String,
    /// When it happened, ms.
    pub at_ms: u64,
    /// `drafted`, `failed` or `held` (no model, a budget, the cap).
    pub status: String,
    /// The brief exactly as sent (after the scrubber).
    #[serde(default)]
    pub intake: String,
    /// The write-up.
    #[serde(default)]
    pub text: String,
    /// The docs it cited: `title (page)`.
    #[serde(default)]
    pub sources: Vec<String>,
    /// The approvals item.
    #[serde(default)]
    pub advice_id: Option<String>,
    /// Its conversation in the system store.
    #[serde(default)]
    pub conversation: Option<u64>,
    /// SHA-256 of each payload sent.
    #[serde(default)]
    pub sent: Vec<String>,
    /// Why it failed or is held.
    #[serde(default)]
    pub error: Option<String>,
    /// Drafted (or last tried), ms.
    pub updated_ms: u64,
}

impl Versioned for WriteupRecord {
    const V: u32 = 1;
}

/// Write-ups in a DC tier.
#[derive(Clone)]
pub struct WriteupStore {
    dc: std::sync::Arc<dyn MetaStore>,
}

impl std::fmt::Debug for WriteupStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteupStore").finish_non_exhaustive()
    }
}

impl WriteupStore {
    /// The write-ups in `dc`.
    #[must_use]
    pub fn new(dc: std::sync::Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// One write-up.
    ///
    /// # Errors
    /// The store's.
    pub async fn get(&self, id: &str) -> Result<Option<WriteupRecord>> {
        let key = self.dc.tier().key(&format!("{PREFIX}{}", safe_id(id)))?;
        match self.dc.get(&key, Consistency::Linearizable).await? {
            Some(kv) => Ok(Some(Record::<WriteupRecord>::decode(&kv.value)?.body)),
            None => Ok(None),
        }
    }

    /// Writes `w` (replacing it).
    ///
    /// # Errors
    /// The store's.
    pub async fn put(&self, w: &WriteupRecord) -> Result<()> {
        let key = self.dc.tier().key(&format!("{PREFIX}{}", safe_id(&w.id)))?;
        self.dc
            .txn(Txn {
                when: Vec::<Cmp>::new(),
                then: vec![Op::Put {
                    key,
                    value: Record::new(w.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// Every write-up, newest first.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Vec<WriteupRecord>> {
        let prefix = self.dc.tier().key(PREFIX)?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(r) = Record::<WriteupRecord>::decode(&kv.value) {
                    out.push(r.body);
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        out.sort_by(|a, b| b.at_ms.cmp(&a.at_ms));
        Ok(out)
    }
}
