//! The administration agent beyond error intake: the metric history,
//! health checks, upgrade readiness, incident write-ups and SprigDB's own
//! decisions, as data and rules. The controller runs them.
//!
//! ```text
//! history: a few series sampled on the DC leader, ai/metrics/<id>
//! rules:   deterministic checks over the history -> findings
//!          a new finding -> the admin model words it (search_docs,
//!          metrics_trend, decision_log) -> an ai_health_warning item;
//!          a finding still open -> its item is refreshed, nothing is sent
//! upgrade: the next version's settings and codes against this deployment's
//!          use -> a ranked report; the model only explains it
//! writeup: a failover, failed move or outage -> its evidence -> a draft
//!          timeline, cause, effects and follow-ups
//! ```
//!
//! What leaves for the provider is built from ids, counts and numbers, and
//! passes the egress gate with every name SprigDB knows registered
//! ([`crate::incidents::Names`]). Everything is advice: nothing here
//! changes anything.

pub mod history;
pub mod rules;
pub mod upgrade;
pub mod writeup;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

pub use history::{History, Sample, SeriesRecord, TrendQuery};
pub use rules::{Finding, Limits};

use crate::incidents::Topology;
use crate::tools::ToolRegistry;

/// The tools the agent's own turns (health warnings, write-ups, upgrade
/// explanations) offer: reads only.
static AGENT_TOOLS: LazyLock<Arc<ToolRegistry>> = LazyLock::new(|| {
    let b = ToolRegistry::builtin();
    let specs: Vec<_> = ["search_docs", "metrics_trend", "decision_log"]
        .iter()
        .filter_map(|n| b.get(n).map(|s| (**s).clone()))
        .collect();
    Arc::new(ToolRegistry::new(specs).unwrap_or_else(|_| (*b).clone()))
});

/// The tools the agent's own turns offer.
#[must_use]
pub fn agent_tools() -> Arc<ToolRegistry> {
    Arc::clone(&AGENT_TOOLS)
}

/// What a health-warning turn is told, after the administration prompt.
pub const HEALTH_RULES: &str = "\
You are writing a health warning for SprigDB's operators. SprigDB's own rules found the finding below in its metric history; you didn't, and you can't change it. You see ids as placeholders, counts and numbers, never data.
- Check the trend with metrics_trend when it helps, and look the subject up with search_docs (audience operator). Name a metric, setting or command only when a tool result or the finding shows it.
- Answer in this shape, briefly:
Severity: low, medium or high (keep the rule's unless the trend clearly says otherwise)
**Finding:** what was found and how soon it matters, with the numbers.
**Steps:**
1. what to check or do first, in order.
**Docs:** the pages you used.
- Your answer becomes an advisory item in the approvals inbox. Nothing is changed by it.";

/// The health warning's text without a model: the rule's own words.
#[must_use]
pub fn plain_warning(f: &Finding, why: &str) -> String {
    format!(
        "Severity: {}\n**Finding:** {}\n\n(Not worded by a model: {why}.)",
        f.severity, f.text
    )
}

/// What a health-warning turn is sent about `f`. Ids appear as they are;
/// the caller passes it through the egress gate.
#[must_use]
pub fn health_brief(f: &Finding, topology: &Topology, now_ms: u64) -> String {
    let mut t = String::new();
    let _ = writeln!(
        t,
        "Health check finding at {} (rule {}, severity {}):",
        history::time_text(now_ms),
        f.rule,
        f.severity
    );
    let _ = writeln!(t, "{}", f.text);
    let labels: Vec<String> = f.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let _ = writeln!(
        t,
        "Series: {}{{{}}} (metrics_trend serves its history).",
        f.series,
        labels.join(",")
    );
    if let Some(d) = f.days {
        let _ = writeln!(t, "Projected to reach the threshold in {d:.1} days.");
    }
    let _ = writeln!(t, "{}", topology.summary());
    let _ = write!(t, "Version: SprigDB {}.", env!("CARGO_PKG_VERSION"));
    t
}

/// The DC-tier prefix of health findings.
pub const FINDINGS: &str = "ai/health/";
/// A finding whose warning was decided (acknowledged, ignored, expired)
/// isn't warned about again for this long, unless its severity rises.
pub const QUIET: Duration = Duration::from_secs(24 * 3600);

/// `ai/health/<key>`: one finding over time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FindingRecord {
    /// The finding, as last found.
    pub finding: Finding,
    /// First found, ms.
    pub first_ms: u64,
    /// Last found, ms.
    pub last_ms: u64,
    /// Checks that found it.
    pub seen: u64,
    /// `open` (found, no warning yet), `warned`, `held` or `failed`.
    pub status: String,
    /// Why it is held or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The `ai_health_warning` item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advice_id: Option<String>,
    /// The severity it was warned at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warned_severity: Option<String>,
    /// When it was warned about, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warned_ms: Option<u64>,
    /// The warning's text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
    /// The brief exactly as sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intake: Option<String>,
    /// Its conversation in the system store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<u64>,
    /// SHA-256 of each payload sent.
    #[serde(default)]
    pub sent: Vec<String>,
}

impl Versioned for FindingRecord {
    const V: u32 = 1;
}

/// The severity's rank: higher is worse.
#[must_use]
pub fn severity_rank(s: &str) -> u8 {
    match s {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

impl FindingRecord {
    /// A record of `f`, first found now.
    #[must_use]
    pub fn new(f: Finding, now_ms: u64) -> Self {
        Self {
            finding: f,
            first_ms: now_ms,
            last_ms: now_ms,
            seen: 1,
            status: "open".to_owned(),
            reason: None,
            advice_id: None,
            warned_severity: None,
            warned_ms: None,
            warning: None,
            intake: None,
            conversation: None,
            sent: Vec::new(),
        }
    }

    /// Whether a new warning is due, given whether its item is still
    /// waiting (`live`): never while it waits; after a decision, once
    /// [`QUIET`] has passed or the severity rose.
    #[must_use]
    pub fn due(&self, live: bool, now_ms: u64) -> bool {
        if live {
            return false;
        }
        let Some(at) = self.warned_ms else {
            return true;
        };
        let rose = self
            .warned_severity
            .as_deref()
            .is_some_and(|w| severity_rank(&self.finding.severity) > severity_rank(w));
        let quiet = u64::try_from(QUIET.as_millis()).unwrap_or(u64::MAX);
        rose || now_ms.saturating_sub(at) >= quiet
    }
}

/// Findings in a DC tier.
#[derive(Clone)]
pub struct FindingStore {
    dc: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for FindingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FindingStore").finish_non_exhaustive()
    }
}

impl FindingStore {
    /// The findings in `dc`.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// One record and its revision.
    ///
    /// # Errors
    /// The store's.
    pub async fn get(&self, key: &str) -> Result<Option<(FindingRecord, u64)>> {
        let k = self
            .dc
            .tier()
            .key(&format!("{FINDINGS}{}", writeup::safe_id(key)))?;
        match self.dc.get(&k, Consistency::Linearizable).await? {
            Some(kv) => Ok(Some((
                Record::<FindingRecord>::decode(&kv.value)?.body,
                kv.mod_revision,
            ))),
            None => Ok(None),
        }
    }

    /// Writes `r`: created when `rev` is `None`, else only at `rev`.
    ///
    /// # Errors
    /// The store's.
    pub async fn put(&self, r: &FindingRecord, rev: Option<u64>) -> Result<bool> {
        let key = self
            .dc
            .tier()
            .key(&format!("{FINDINGS}{}", writeup::safe_id(&r.finding.key)))?;
        let when = match rev {
            Some(v) => vec![Cmp::ModRev(key.clone(), v)],
            None => vec![Cmp::Missing(key.clone())],
        };
        let t = self
            .dc
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key,
                    value: Record::new(r.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(t.succeeded)
    }

    /// Drops `key`'s record.
    ///
    /// # Errors
    /// The store's.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let k = self
            .dc
            .tier()
            .key(&format!("{FINDINGS}{}", writeup::safe_id(key)))?;
        self.dc
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Delete(k)],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// Every record, by key.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<BTreeMap<String, (FindingRecord, u64)>> {
        let prefix = self.dc.tier().key(FINDINGS)?;
        let mut out = BTreeMap::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(r) = Record::<FindingRecord>::decode(&kv.value) {
                    out.insert(r.body.finding.key.clone(), (r.body, kv.mod_revision));
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
}
