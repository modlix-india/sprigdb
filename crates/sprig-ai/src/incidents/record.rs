//! Incidents: one record per fingerprint, the counts behind it, and the
//! rule that decides which ones go further.
//!
//! ```text
//! fingerprint := first 6 bytes of SHA-256(code 0 component 0 span), hex
//! per pass, per fingerprint with new errors:
//!   hours[now] += n; last seen, versions, processes
//!   goes further (pending) when it isn't cooling down and:
//!     it is new (first seen in this pass), or
//!     its code is critical, or
//!     it is rising: last hour >= RISING_FACTOR x its hourly baseline over the
//!     7 days before, and at least MIN_RISING errors
//!   cooling down: escalated in the last COOLDOWN, or its advice is still open
//! ```
//!
//! Records live in the DC tier at `ai/incidents/<fingerprint>`, written by
//! the DC leader only, each well under etcd's value limit: at most
//! [`MAX_HOURS`] hourly buckets, [`MAX_VERSIONS`] versions, and the
//! intake text that was sent.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sprig_common::{Result, SprigCode};
use sprig_meta::{Cmp, Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

use super::intake::Delta;

/// The DC-tier prefix of incident records.
pub const PREFIX: &str = "ai/incidents/";
/// Hourly buckets kept: seven days and the hour now.
pub const MAX_HOURS: usize = 7 * 24 + 1;
/// Versions kept per incident.
pub const MAX_VERSIONS: usize = 8;
/// How far above its baseline a fingerprint must be to count as rising.
pub const RISING_FACTOR: f64 = 3.0;
/// The fewest errors in an hour that can count as rising.
pub const MIN_RISING: u64 = 3;
/// After an escalation, the same fingerprint waits this long.
pub const COOLDOWN: Duration = Duration::from_secs(6 * 3600);
/// A pending incident that couldn't go further for this long is dropped.
pub const PENDING_FOR: Duration = Duration::from_secs(24 * 3600);
/// The longest intake text kept.
pub const MAX_INTAKE: usize = 4096;

const HOUR_MS: u64 = 3_600_000;

/// Codes that always go to advice: data safety and internal faults.
pub const CRITICAL: &[SprigCode] = &[
    SprigCode::OutcomeUnknown,
    SprigCode::CommitOutcomeUnknown,
    SprigCode::PartialCommit,
    SprigCode::Internal,
];

/// Whether `code` is critical.
#[must_use]
pub fn critical(code: u16) -> bool {
    CRITICAL.iter().any(|c| c.number() == code)
}

/// The fingerprint of (code, component, span): 12 hex digits.
#[must_use]
pub fn fingerprint(code: u16, component: &str, span: &str) -> String {
    let mut h = Sha256::new();
    h.update(code.to_string().as_bytes());
    h.update([0]);
    h.update(component.as_bytes());
    h.update([0]);
    h.update(span.as_bytes());
    crate::provider::hex(&h.finalize()[..6])
}

/// The name of `code`: `ER_SPRIG_...` for SprigDB's codes, else empty.
#[must_use]
pub fn code_name(code: u16) -> String {
    SprigCode::from_number(code)
        .map(|c| c.name().to_owned())
        .unwrap_or_default()
}

/// Why an incident went further.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Why {
    /// First seen.
    New,
    /// Well above its baseline.
    Rising,
    /// A critical code.
    Critical,
}

impl Why {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Why::New => "new",
            Why::Rising => "rising",
            Why::Critical => "critical",
        }
    }
}

/// Where an incident stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Counted; nothing sent.
    #[default]
    Counting,
    /// Waiting to go to triage or advice (a cap, the budget, no model).
    Pending,
    /// Triage said noise.
    Noise,
    /// Advice was filed.
    Advised,
    /// Triage or advice failed; it may go again after the cooldown.
    Failed,
}

/// `/sprig/dc/<dc>/ai/incidents/<fingerprint>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentRecord {
    /// [`fingerprint`].
    pub fingerprint: String,
    /// The error's number.
    pub code: u16,
    /// Its name, for SprigDB's codes.
    #[serde(default)]
    pub code_name: String,
    /// `gateway`, `controller` or `worker`.
    pub component: String,
    /// Where.
    pub span: String,
    /// First seen, ms since the Unix epoch.
    pub first_seen_ms: u64,
    /// Last seen.
    pub last_seen_ms: u64,
    /// Errors counted since first seen.
    pub total: u64,
    /// `[hour since the epoch, errors]`, oldest first, at most
    /// [`MAX_HOURS`].
    #[serde(default)]
    pub hours: Vec<[u64; 2]>,
    /// Versions of the processes that reported it, newest last.
    #[serde(default)]
    pub versions: Vec<String>,
    /// Processes that reported it in the last pass with errors.
    #[serde(default)]
    pub processes: u32,
    /// Where it stands.
    #[serde(default)]
    pub status: Status,
    /// Why it went further, the last time it did.
    #[serde(default)]
    pub why: Option<Why>,
    /// Waiting to go further since.
    #[serde(default)]
    pub pending_since_ms: Option<u64>,
    /// Why it is still waiting: `advice_cap`, `triage_cap`, `budget`,
    /// `no_model`, `error`.
    #[serde(default)]
    pub held: Option<String>,
    /// Last sent to triage or advice.
    #[serde(default)]
    pub escalated_ms: Option<u64>,
    /// Triage's answer: `noise` or `needs_advice` (`critical` when triage
    /// was skipped for a critical code).
    #[serde(default)]
    pub verdict: Option<String>,
    /// The advice's severity.
    #[serde(default)]
    pub severity: Option<String>,
    /// The advice's one line.
    #[serde(default)]
    pub summary: Option<String>,
    /// The advice as filed.
    #[serde(default)]
    pub advice: Option<String>,
    /// The approvals item (`dc.<dc>.<n>`).
    #[serde(default)]
    pub advice_id: Option<String>,
    /// The administration conversation holding the turns, with every
    /// payload.
    #[serde(default)]
    pub conversation: Option<u64>,
    /// SHA-256 of each payload sent, hexadecimal.
    #[serde(default)]
    pub sent: Vec<String>,
    /// The intake text, exactly as sent.
    #[serde(default)]
    pub intake: Option<String>,
    /// The last error, when triage or advice failed.
    #[serde(default)]
    pub error: Option<String>,
    /// Last written.
    #[serde(default)]
    pub updated_ms: u64,
}

impl Versioned for IncidentRecord {
    const V: u32 = 1;
}

impl IncidentRecord {
    /// A record for (code, component, span), first seen `now_ms`.
    #[must_use]
    pub fn new(code: u16, component: &str, span: &str, now_ms: u64) -> Self {
        Self {
            fingerprint: fingerprint(code, component, span),
            code,
            code_name: code_name(code),
            component: component.to_owned(),
            span: span.to_owned(),
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
            total: 0,
            hours: Vec::new(),
            versions: Vec::new(),
            processes: 0,
            status: Status::Counting,
            why: None,
            pending_since_ms: None,
            held: None,
            escalated_ms: None,
            verdict: None,
            severity: None,
            summary: None,
            advice: None,
            advice_id: None,
            conversation: None,
            sent: Vec::new(),
            intake: None,
            error: None,
            updated_ms: now_ms,
        }
    }

    /// Adds one pass's errors.
    pub fn add(&mut self, d: &Delta, now_ms: u64) {
        let hour = now_ms / HOUR_MS;
        match self.hours.last_mut() {
            Some(h) if h[0] == hour => h[1] += d.n,
            _ => self.hours.push([hour, d.n]),
        }
        let oldest = hour.saturating_sub(MAX_HOURS as u64 - 1);
        self.hours.retain(|h| h[0] >= oldest);
        self.total = self.total.saturating_add(d.n);
        self.last_seen_ms = now_ms;
        self.processes = d.processes;
        for v in &d.versions {
            if !self.versions.contains(v) {
                self.versions.push(v.clone());
            }
        }
        let cut = self.versions.len().saturating_sub(MAX_VERSIONS);
        self.versions.drain(..cut);
        self.updated_ms = now_ms;
    }

    /// Errors in the last 60 minutes: this hour's bucket and the share of
    /// the previous one still inside the window.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn last_hour(&self, now_ms: u64) -> u64 {
        let hour = now_ms / HOUR_MS;
        let into = (now_ms % HOUR_MS) as f64 / HOUR_MS as f64;
        let at = |h: u64| self.hours.iter().find(|b| b[0] == h).map_or(0, |b| b[1]);
        at(hour) + (at(hour.saturating_sub(1)) as f64 * (1.0 - into)).round() as u64
    }

    /// The errors per hour over the seven days before the last two hours,
    /// counting only hours since it was first seen.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn baseline(&self, now_ms: u64) -> f64 {
        let hour = now_ms / HOUR_MS;
        let end = hour.saturating_sub(2);
        let start = hour.saturating_sub(MAX_HOURS as u64 - 1);
        let first = self.first_seen_ms / HOUR_MS;
        let from = start.max(first);
        if end < from {
            return 0.0;
        }
        let sum: u64 = self
            .hours
            .iter()
            .filter(|b| b[0] >= from && b[0] <= end)
            .map(|b| b[1])
            .sum();
        sum as f64 / (end - from + 1) as f64
    }

    /// Whether it is cooling down (module docs). `advice_open` says whether
    /// its approvals item is still waiting.
    #[must_use]
    pub fn cooling(&self, now_ms: u64, advice_open: bool) -> bool {
        if advice_open {
            return true;
        }
        self.escalated_ms.is_some_and(|t| {
            now_ms.saturating_sub(t) < u64::try_from(COOLDOWN.as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// Whether this pass's errors make it go further, and why. `new` says
    /// it was first seen in this pass.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn goes_further(&self, new: bool, now_ms: u64, advice_open: bool) -> Option<Why> {
        if self.cooling(now_ms, advice_open) {
            return None;
        }
        if critical(self.code) {
            return Some(Why::Critical);
        }
        if new {
            return Some(Why::New);
        }
        let last = self.last_hour(now_ms);
        if last >= MIN_RISING && last as f64 >= RISING_FACTOR * self.baseline(now_ms) {
            return Some(Why::Rising);
        }
        None
    }

    /// Marks it pending for `why`, unless it already is.
    pub fn pend(&mut self, why: Why, now_ms: u64) {
        if self.status != Status::Pending {
            self.status = Status::Pending;
            self.pending_since_ms = Some(now_ms);
            self.held = None;
        }
        self.why = Some(why);
    }

    /// Whether it has waited too long to go further.
    #[must_use]
    pub fn stale_pending(&self, now_ms: u64) -> bool {
        self.status == Status::Pending
            && self.pending_since_ms.is_some_and(|t| {
                now_ms.saturating_sub(t)
                    > u64::try_from(PENDING_FOR.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// Reads and writes incident records in one DC tier.
pub struct IncidentStore {
    dc: std::sync::Arc<dyn MetaStore>,
}

impl std::fmt::Debug for IncidentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncidentStore").finish_non_exhaustive()
    }
}

impl IncidentStore {
    /// The records in `dc` (a DC tier).
    #[must_use]
    pub fn new(dc: std::sync::Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    /// One record and its revision.
    ///
    /// # Errors
    /// The store's; 39999 for a key that can't be built (a bad
    /// fingerprint).
    pub async fn get(&self, fp: &str) -> Result<Option<(IncidentRecord, u64)>> {
        let key = self.dc.tier().key(&format!("{PREFIX}{fp}"))?;
        match self.dc.get(&key, Consistency::Linearizable).await? {
            Some(kv) => Ok(Some((
                Record::<IncidentRecord>::decode(&kv.value)?.body,
                kv.mod_revision,
            ))),
            None => Ok(None),
        }
    }

    /// Writes `rec`: created when `rev` is `None`, else only if it is
    /// still at `rev`. Returns whether it was written.
    ///
    /// # Errors
    /// The store's.
    pub async fn put(&self, rec: &IncidentRecord, rev: Option<u64>) -> Result<bool> {
        let key = self
            .dc
            .tier()
            .key(&format!("{PREFIX}{}", rec.fingerprint))?;
        let when = match rev {
            Some(r) => vec![Cmp::ModRev(key.clone(), r)],
            None => vec![Cmp::Missing(key.clone())],
        };
        let r = self
            .dc
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key,
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded)
    }

    /// Every record, newest first.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Vec<IncidentRecord>> {
        let prefix = self.dc.tier().key(PREFIX)?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Record::<IncidentRecord>::decode(&kv.value) {
                    Ok(r) => out.push(r.body),
                    Err(e) => tracing::debug!(key = %kv.key, error = %e, "unreadable incident"),
                }
            }
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
        out.sort_by(|a, b| b.last_seen_ms.cmp(&a.last_seen_ms));
        Ok(out)
    }

    /// Fingerprints known, for a quick look.
    ///
    /// # Errors
    /// The store's.
    pub async fn fingerprints(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|r| r.fingerprint)
            .collect())
    }
}
