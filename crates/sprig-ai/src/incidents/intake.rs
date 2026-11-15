//! Each process's error counts in etcd, and how the DC leader turns them
//! into new errors.
//!
//! ```text
//! every process, while ai.admin.enabled is on, every FLUSH_PERIOD:
//!   counts := sprig_common::incidents::snapshot()     -- cumulative since start
//!   changed: put DC tier ai/intake/<process> under the process's lease (LEASE_TTL)
//!   else:    keep the lease alive
//! the DC leader, each pass:
//!   list ai/intake/; per process: the counts minus what it saw last time
//!   (a process it sees first: everything if the process started after the
//!   leader's intake did, else nothing; a restarted process: everything)
//! ```
//!
//! A record holds codes, spans and counts, the process's component,
//! version and start time; never a message, a name or a value. It is
//! bounded by [`sprig_common::incidents::MAX_KEYS`] pairs (well under
//! etcd's 64 KiB value limit) and vanishes with the process's lease.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::incidents::{self, Snapshot};
use sprig_common::{CancellationToken, Result};
use sprig_meta::{LeaseId, ListOpts, MetaStore, Op, Record, Txn, Versioned};

/// How often a process writes its counts.
pub const FLUSH_PERIOD: Duration = Duration::from_secs(60);
/// The lease a process's record lives under.
pub const LEASE_TTL: Duration = Duration::from_secs(180);
/// The DC-tier prefix of the records.
pub const PREFIX: &str = "ai/intake/";

/// One (code, span) count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntakeCount {
    /// The error's number (a SprigDB code or a MySQL error number; 0 for
    /// none).
    pub code: u16,
    /// Where (a static path such as `mysql.query`).
    pub span: String,
    /// Errors since the process started.
    pub n: u64,
}

/// `/sprig/dc/<dc>/ai/intake/<process>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntakeRecord {
    /// The process: its role and id.
    pub process: String,
    /// `gateway`, `controller` or `worker`.
    pub component: String,
    /// The build.
    pub version: String,
    /// When the process's counting started, ms since the Unix epoch.
    pub started_ms: u64,
    /// When this was written.
    pub flushed_ms: u64,
    /// Cumulative counts.
    pub counts: Vec<IntakeCount>,
    /// Errors the process couldn't count (too many distinct pairs).
    #[serde(default)]
    pub dropped: u64,
}

impl Versioned for IntakeRecord {
    const V: u32 = 1;
}

impl IntakeRecord {
    /// The record of `snap` for `process`.
    #[must_use]
    pub fn of(process: &str, component: &str, snap: &Snapshot, now_ms: u64) -> Self {
        Self {
            process: process.to_owned(),
            component: component.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            started_ms: snap.started_ms,
            flushed_ms: now_ms,
            counts: snap
                .counts
                .iter()
                .map(|c| IntakeCount {
                    code: c.code,
                    span: c.span.clone(),
                    n: c.n,
                })
                .collect(),
            dropped: snap.dropped,
        }
    }
}

/// `process` as a key segment: `[a-z0-9._-]`, other characters `_`, at
/// most 128 bytes.
#[must_use]
pub fn key_segment(process: &str) -> String {
    let s: String = process
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    if s.is_empty() { "_".to_owned() } else { s }
}

/// Ms since the Unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Writes a process's counts (module docs).
pub struct Flusher {
    store: Arc<dyn MetaStore>,
    process: String,
    component: &'static str,
    lease: Option<LeaseId>,
    last: Option<Snapshot>,
}

impl std::fmt::Debug for Flusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flusher")
            .field("process", &self.process)
            .finish_non_exhaustive()
    }
}

impl Flusher {
    /// A flusher of `process`'s counts into `store` (the DC tier).
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, process: &str, component: &'static str) -> Self {
        Self {
            store,
            process: key_segment(process),
            component,
            lease: None,
            last: None,
        }
    }

    async fn lease(&mut self) -> Result<LeaseId> {
        if let Some(l) = self.lease {
            match self.store.keep_alive(l).await {
                Ok(Some(_)) => return Ok(l),
                Ok(None) | Err(_) => self.lease = None,
            }
        }
        let l = self.store.grant_lease(LEASE_TTL).await?;
        self.lease = Some(l);
        Ok(l)
    }

    /// One flush of `snap`: written when it changed since the last write,
    /// else only the lease is kept. Returns whether it wrote.
    ///
    /// # Errors
    /// The store's.
    pub async fn flush(&mut self, snap: Snapshot) -> Result<bool> {
        if snap.counts.is_empty() && snap.dropped == 0 {
            return Ok(false);
        }
        let lease = self.lease().await?;
        if self.last.as_ref() == Some(&snap) {
            return Ok(false);
        }
        let rec = IntakeRecord::of(&self.process, self.component, &snap, now_ms());
        let key = self
            .store
            .tier()
            .key(&format!("{PREFIX}{}", self.process))?;
        self.store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key,
                    value: Record::new(rec).encode()?,
                    lease: Some(lease),
                }],
                otherwise: Vec::new(),
            })
            .await?;
        self.last = Some(snap);
        Ok(true)
    }

    /// Flushes the process's recorder every [`FLUSH_PERIOD`] while
    /// `enabled` says so, until `stop`.
    pub async fn run(
        mut self,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
        stop: CancellationToken,
    ) {
        let mut tick = tokio::time::interval(FLUSH_PERIOD);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                _ = tick.tick() => {}
            }
            if !enabled() {
                continue;
            }
            match self.flush(incidents::snapshot()).await {
                Ok(_) => crate::metrics::intake_flush("ok"),
                Err(e) => {
                    crate::metrics::intake_flush("error");
                    tracing::debug!(error = %e, "error counts weren't written for the administration agent");
                }
            }
        }
        if let Some(l) = self.lease.take() {
            let _ = self.store.revoke_lease(l).await;
        }
    }
}

/// Starts this process's flusher: names its component in the recorder,
/// and writes its counts into `dc` while `enabled` returns true.
pub fn spawn(
    dc: Arc<dyn MetaStore>,
    component: &'static str,
    id: &str,
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    incidents::set_component(component);
    let f = Flusher::new(dc, &format!("{component}-{id}"), component);
    tokio::spawn(f.run(enabled, stop))
}

/// Every process's record in `dc`.
///
/// # Errors
/// The store's.
pub async fn list(dc: &dyn MetaStore) -> Result<Vec<IntakeRecord>> {
    let prefix = dc.tier().key(PREFIX)?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = dc.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            match Record::<IntakeRecord>::decode(&kv.value) {
                Ok(r) => out.push(r.body),
                Err(e) => tracing::debug!(key = %kv.key, error = %e, "unreadable intake record"),
            }
        }
        if !page.more {
            return Ok(out);
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
}

/// What one pass found new for one (code, component, span).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Delta {
    /// Errors since the last pass.
    pub n: u64,
    /// The versions of the processes that reported them.
    pub versions: BTreeSet<String>,
    /// How many processes reported them.
    pub processes: u32,
}

/// (code, component, span).
pub type DeltaKey = (u16, String, String);

/// One process's counts, by (code, span).
type Counts = HashMap<(u16, String), u64>;

/// The leader's memory of what each process reported (module docs).
#[derive(Debug, Clone, Default)]
pub struct Merger {
    started_ms: u64,
    seen: HashMap<String, (u64, Counts)>,
}

impl Merger {
    /// A merger whose intake started at `started_ms`.
    #[must_use]
    pub fn new(started_ms: u64) -> Self {
        Self {
            started_ms,
            seen: HashMap::new(),
        }
    }

    /// The new errors in `records` since the last call.
    pub fn merge(&mut self, records: &[IntakeRecord]) -> BTreeMap<DeltaKey, Delta> {
        let mut out: BTreeMap<DeltaKey, Delta> = BTreeMap::new();
        let mut live = BTreeSet::new();
        for r in records {
            live.insert(r.process.clone());
            let now: HashMap<(u16, String), u64> = r
                .counts
                .iter()
                .map(|c| ((c.code, incidents::sanitize_span(&c.span)), c.n))
                .collect();
            let base = match self.seen.get(&r.process) {
                Some((started, prev)) if *started == r.started_ms => Some(prev),
                // Restarted: everything it reports is new.
                Some(_) => None,
                // First seen: new only if it started after the intake did.
                None if r.started_ms >= self.started_ms => None,
                None => {
                    self.seen.insert(r.process.clone(), (r.started_ms, now));
                    continue;
                }
            };
            for ((code, span), n) in &now {
                let before = base.and_then(|b| b.get(&(*code, span.clone()))).copied();
                let d = match before {
                    Some(b) if *n >= b => n - b,
                    _ => *n,
                };
                if d == 0 {
                    continue;
                }
                let e = out
                    .entry((*code, r.component.clone(), span.clone()))
                    .or_default();
                e.n += d;
                e.processes += 1;
                e.versions.insert(r.version.clone());
            }
            self.seen.insert(r.process.clone(), (r.started_ms, now));
        }
        self.seen.retain(|p, _| live.contains(p));
        out
    }
}
