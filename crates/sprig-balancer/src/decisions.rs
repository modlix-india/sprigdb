//! The decision log: each tenant's decisions as the engine's catalog
//! records, the [`DecisionLog`] interface every component writes through,
//! the retention job, and the reads behind `sprigctl explain decision`.
//!
//! ```text
//! m/decisions/rows/<decision id u64>                        DecisionRow
//! m/decisions/by_time/<decided µs><decision id>             (empty)
//! m/decisions/by_subject/<type><id><decided µs><decision>   (empty)
//! ctr/decision                                              the last id handed out
//! ```
//!
//! Decisions are not versioned catalog state (they are read on demand),
//! so each write is a short engine transaction of its own, outside
//! catalog versions. The time index serves the retention purge (a key
//! range) and the newest-first reads; the subject index serves
//! `by_subject`.
//!
//! Implementations:
//!
//! - [`CatalogDecisionLog`] writes to the tenant's catalog records through
//!   a [`RecordStore`], buffering up to [`BUFFER_LIMIT`] decisions per
//!   process while they are unreachable;
//! - [`MemDecisionLog`] keeps them in memory (tests and the fleet
//!   simulator).
//!
//! **Times** travel as microseconds since the epoch, as in the other
//! catalog modules.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, next_ids};
use sprig_catalog::{Migration, Module};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

/// The module name in the catalog's format records.
pub const MODULE: &str = "decisions";

/// Decisions by id.
pub const ROWS: RowTable = RowTable::new(MODULE, "rows");
/// Decision ids by time.
pub const BY_TIME: RowTable = RowTable::new(MODULE, "by_time");
/// Decision ids by subject and time.
pub const BY_SUBJECT: RowTable = RowTable::new(MODULE, "by_subject");
/// The id counter.
pub const COUNTER: &str = "decision";

/// The module's MariaDB migrations: none, its rows are engine records.
pub static DECISIONS_MIGRATIONS: &[Migration] = &[];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// the catalog's `BootstrapHooks.modules`.
pub const DECISIONS_MODULE: Module = Module {
    name: MODULE,
    migrations: DECISIONS_MIGRATIONS,
};

/// Decisions held per process while catalogs are unreachable.
pub const BUFFER_LIMIT: usize = 1000;

/// Rows the retention job deletes per statement.
pub const PURGE_BATCH: u32 = 1000;

macro_rules! text_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// The SQL text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the SQL text.
            ///
            /// # Errors
            /// `Internal` for any other text.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    _ => Err(SprigError::internal(format!(
                        concat!("decisions: bad ", stringify!($name), " {:?}"), s
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

text_enum! {
    /// `decisions.mode`.
    Mode {
        /// Done automatically.
        Applied = "applied",
        /// Recommended; nothing runs.
        Recommended = "recommended",
        /// `dry_run`: computed and logged only.
        DryRun = "dry_run",
        /// An operator's command.
        Operator = "operator",
    }
}

text_enum! {
    /// `decisions.outcome`.
    Outcome {
        /// Decided, not yet scheduled.
        Proposed = "proposed",
        /// A job exists.
        Scheduled = "scheduled",
        /// The job runs.
        Running = "running",
        /// Done.
        Succeeded = "succeeded",
        /// The job failed.
        Failed = "failed",
        /// Refused by a guardrail or constraint.
        Rejected = "rejected",
        /// Cancelled before running.
        Cancelled = "cancelled",
        /// A later decision replaced it.
        Superseded = "superseded",
    }
}

impl Outcome {
    /// Whether nothing more will happen to the decision.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Rejected | Self::Cancelled | Self::Superseded
        )
    }
}

/// A decision to record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewDecision {
    /// When, microseconds since the epoch.
    pub decided_at_us: i64,
    /// `balancer`, `mover`, `advisor`, `autoindex`, `catalog`, ...
    pub component: String,
    /// `shard.place`, `shard.move`, `index.create`, ...
    pub kind: String,
    /// `shard`, `group`, `object`, `index`, `fingerprint`, `cluster`.
    pub subject_type: String,
    /// The subject's id, such as `t42/g1/s3`.
    pub subject_id: String,
    /// How it was applied.
    pub mode: Mode,
    /// Where it stands.
    pub outcome: Outcome,
    /// `cluster_hot`, `spread`, `residency_change`, ...
    pub reason_code: String,
    /// One line for people (cut to 512 characters).
    pub summary: String,
    /// Inputs, scores, thresholds, windows: identifiers and numbers only.
    pub evidence: serde_json::Value,
    /// Options considered, with scores.
    pub alternatives: Option<serde_json::Value>,
    /// The job, a UUIDv7.
    pub job_id: Option<[u8; 16]>,
    /// A parent decision.
    pub parent_id: Option<u64>,
    /// Controller id, or operator principal.
    pub actor: String,
    /// The DC that decided.
    pub dc_id: String,
}

/// A stored decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRow {
    /// Its id.
    pub decision_id: u64,
    /// The decision.
    #[serde(flatten)]
    pub d: NewDecision,
    /// Last change, microseconds since the epoch.
    pub updated_at_us: i64,
}

/// Where decisions go.
#[async_trait::async_trait]
pub trait DecisionLog: Send + Sync {
    /// Records `d` in `tenant`'s log; returns its id (0 when buffered).
    ///
    /// # Errors
    /// The log's.
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64>;
    /// Changes a decision's outcome.
    ///
    /// # Errors
    /// The log's.
    async fn update_outcome(&self, tenant: TenantId, id: u64, o: Outcome, at_us: i64)
    -> Result<()>;
}

/// An order-preserving key part for a signed time.
fn at_key(us: i64) -> [u8; 8] {
    #[allow(clippy::cast_sign_loss)]
    let u = (us as u64) ^ (1u64 << 63);
    u.to_be_bytes()
}

fn id_of(pk: &Pk) -> Option<u64> {
    let b = pk.0.get(pk.0.len().checked_sub(8)?..)?;
    <[u8; 8]>::try_from(b).ok().map(u64::from_be_bytes)
}

fn subject_pk(t: &str, id: &str) -> Pk {
    Pk::new().bytes(t.as_bytes()).bytes(id.as_bytes())
}

/// Access to one tenant's decision records, in the caller's transaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecisionStore;

impl DecisionStore {
    /// Inserts `d` (its texts cut to the columns' old widths); returns its
    /// id (from the tenant's decision counter, never reused).
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert(rec: &mut dyn RecordTx, d: &NewDecision) -> Result<u64> {
        let id = next_ids(rec, COUNTER, 1).await?;
        let d = NewDecision {
            component: cut(&d.component, 24),
            kind: cut(&d.kind, 48),
            subject_type: cut(&d.subject_type, 24),
            subject_id: cut(&d.subject_id, 64),
            reason_code: cut(&d.reason_code, 64),
            summary: cut(&d.summary, 512),
            actor: cut(&d.actor, 128),
            dc_id: cut(&d.dc_id, 32),
            ..d.clone()
        };
        let row = DecisionRow {
            decision_id: id,
            updated_at_us: d.decided_at_us,
            d,
        };
        ROWS.put(rec, &Pk::new().u64(id), &row).await?;
        BY_TIME
            .put(
                rec,
                &Pk::new().raw(&at_key(row.d.decided_at_us)).u64(id),
                &(),
            )
            .await?;
        BY_SUBJECT
            .put(
                rec,
                &subject_pk(&row.d.subject_type, &row.d.subject_id)
                    .raw(&at_key(row.d.decided_at_us))
                    .u64(id),
                &(),
            )
            .await?;
        Ok(id)
    }

    /// Sets a decision's outcome (and its job, when given).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_outcome(
        rec: &mut dyn RecordTx,
        id: u64,
        o: Outcome,
        job_id: Option<[u8; 16]>,
        at_us: i64,
    ) -> Result<()> {
        let pk = Pk::new().u64(id);
        if let Some(mut r) = ROWS.get_for_update::<DecisionRow>(rec, &pk).await? {
            r.d.outcome = o;
            if job_id.is_some() {
                r.d.job_id = job_id;
            }
            r.updated_at_us = at_us;
            ROWS.put(rec, &pk, &r).await?;
        }
        Ok(())
    }

    /// One decision.
    ///
    /// # Errors
    /// The record store's.
    pub async fn get(rec: &mut dyn RecordTx, id: u64) -> Result<Option<DecisionRow>> {
        ROWS.get(rec, &Pk::new().u64(id)).await
    }

    async fn rows_of(rec: &mut dyn RecordTx, ids: Vec<u64>) -> Result<Vec<DecisionRow>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(r) = Self::get(rec, id).await? {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// The newest decisions about one subject, newest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn by_subject(
        rec: &mut dyn RecordTx,
        subject_type: &str,
        subject_id: &str,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let ids: Vec<u64> = BY_SUBJECT
            .scan::<()>(rec, &subject_pk(subject_type, subject_id))
            .await?
            .iter()
            .rev()
            .filter_map(|(pk, ())| id_of(pk))
            .take(limit as usize)
            .collect();
        Self::rows_of(rec, ids).await
    }

    /// Decisions decided at or after `since_us`, newest first, passing
    /// `keep`, at most `limit`.
    async fn newest(
        rec: &mut dyn RecordTx,
        since_us: i64,
        limit: u32,
        keep: impl Fn(&DecisionRow) -> bool + Send,
    ) -> Result<Vec<DecisionRow>> {
        let lo = BY_TIME.key(&Pk::new().raw(&at_key(since_us)));
        let hi = sprig_catalog::records::prefix_end(&BY_TIME.prefix());
        let base = BY_TIME.prefix().len();
        let entries = sprig_catalog::records::scan_all(rec, &lo, &hi).await?;
        let mut out = Vec::new();
        for (k, _) in entries.iter().rev() {
            if out.len() >= limit as usize {
                break;
            }
            let Some(id) = id_of(&Pk(k[base..].to_vec())) else {
                continue;
            };
            if let Some(r) = Self::get(rec, id).await?
                && keep(&r)
            {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// The newest decisions of a component, newest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn recent(
        rec: &mut dyn RecordTx,
        component: &str,
        since_us: i64,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        Self::newest(rec, since_us, limit, |r| r.d.component == component).await
    }

    /// The newest decisions since `since_us` that mention any of `words`
    /// (at most 8) in their subject, kind, reason or summary, without
    /// regard to case (all of them when `words` is empty), newest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn search(
        rec: &mut dyn RecordTx,
        words: &[String],
        since_us: i64,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let words: Vec<String> = words.iter().take(8).map(|w| w.to_lowercase()).collect();
        Self::newest(rec, since_us, limit, |r| {
            words.is_empty()
                || words.iter().any(|w| {
                    [&r.d.subject_id, &r.d.kind, &r.d.reason_code, &r.d.summary]
                        .iter()
                        .any(|f| f.to_lowercase().contains(w.as_str()))
                })
        })
        .await
    }

    /// The retention job's step: deletes up to [`PURGE_BATCH`] decisions
    /// decided before `before_us`, oldest first (a key range of the time
    /// index); returns how many.
    ///
    /// # Errors
    /// The record store's.
    pub async fn purge_batch(rec: &mut dyn RecordTx, before_us: i64) -> Result<u64> {
        let lo = BY_TIME.prefix();
        let hi = BY_TIME.key(&Pk::new().raw(&at_key(before_us)));
        let base = lo.len();
        let entries = rec.scan(&lo, &hi, PURGE_BATCH as usize).await?;
        let mut n = 0;
        for (k, _) in entries {
            let pk = Pk(k[base..].to_vec());
            rec.delete(&k).await?;
            let Some(id) = id_of(&pk) else { continue };
            if let Some(r) = Self::get(rec, id).await? {
                BY_SUBJECT
                    .delete(
                        rec,
                        &subject_pk(&r.d.subject_type, &r.d.subject_id)
                            .raw(&at_key(r.d.decided_at_us))
                            .u64(id),
                    )
                    .await?;
                ROWS.delete(rec, &Pk::new().u64(id)).await?;
            }
            n += 1;
        }
        Ok(n)
    }
}

fn cut(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    s.chars().take(max).collect()
}

/// Decisions in memory, per tenant (tests and the simulator).
#[derive(Debug, Default)]
pub struct MemDecisionLog {
    rows: Mutex<BTreeMap<TenantId, Vec<DecisionRow>>>,
}

impl MemDecisionLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every decision of `tenant`, oldest first.
    #[must_use]
    pub fn of(&self, tenant: TenantId) -> Vec<DecisionRow> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()
            .unwrap_or_default()
    }

    /// Every decision, by tenant.
    #[must_use]
    pub fn all(&self) -> Vec<(TenantId, DecisionRow)> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .flat_map(|(t, v)| v.iter().map(move |r| (*t, r.clone())))
            .collect()
    }

    /// Deletes rows decided before `before_us`; returns how many.
    pub fn purge_before(&self, before_us: i64) -> usize {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let mut n = 0;
        for v in g.values_mut() {
            let len = v.len();
            v.retain(|r| r.d.decided_at_us >= before_us);
            n += len - v.len();
        }
        n
    }
}

#[async_trait::async_trait]
impl DecisionLog for MemDecisionLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let id = g.values().map(Vec::len).sum::<usize>() as u64 + 1;
        g.entry(tenant).or_default().push(DecisionRow {
            decision_id: id,
            d: d.clone(),
            updated_at_us: d.decided_at_us,
        });
        Ok(id)
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> Result<()> {
        let mut g = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(r) = g
            .get_mut(&tenant)
            .and_then(|v| v.iter_mut().find(|r| r.decision_id == id))
        {
            r.d.outcome = o;
            r.updated_at_us = at_us;
        }
        Ok(())
    }
}

enum Pending {
    Record(TenantId, NewDecision),
}

/// Writes decisions to the tenant's catalog records; while they are
/// unreachable, keeps up to [`BUFFER_LIMIT`] of them and retries on the
/// next write or [`flush`](Self::flush).
pub struct CatalogDecisionLog {
    records: Arc<dyn RecordStore>,
    buffer: Mutex<VecDeque<Pending>>,
}

impl fmt::Debug for CatalogDecisionLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogDecisionLog").finish_non_exhaustive()
    }
}

impl CatalogDecisionLog {
    /// A log over the tenants' catalog records in `records`.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>) -> Self {
        Self {
            records,
            buffer: Mutex::new(VecDeque::new()),
        }
    }

    /// Decisions waiting for their catalog.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    async fn write(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        let mut tx = self.records.begin(tenant).await?;
        let id = DecisionStore::insert(&mut *tx, d).await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Retries buffered decisions; returns how many were written.
    pub async fn flush(&self) -> usize {
        let pending: Vec<Pending> = self
            .buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect();
        let mut written = 0;
        let mut back = VecDeque::new();
        for p in pending {
            let Pending::Record(t, d) = p;
            if self.write(t, &d).await.is_ok() {
                written += 1;
            } else {
                back.push_back(Pending::Record(t, d));
            }
        }
        if !back.is_empty() {
            let mut g = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
            for p in back.into_iter().rev() {
                g.push_front(p);
            }
            while g.len() > BUFFER_LIMIT {
                g.pop_front();
            }
        }
        written
    }

    /// The decision with `id` in `tenant`'s log.
    ///
    /// # Errors
    /// The record store's.
    pub async fn get(&self, tenant: TenantId, id: u64) -> Result<Option<DecisionRow>> {
        let mut tx = self.records.begin(tenant).await?;
        DecisionStore::get(&mut *tx, id).await
    }

    /// `tenant`'s newest decisions since `since_us` that mention any of
    /// `words` ([`DecisionStore::search`]).
    ///
    /// # Errors
    /// The record store's.
    pub async fn search(
        &self,
        tenant: TenantId,
        words: &[String],
        since_us: i64,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let mut tx = self.records.begin(tenant).await?;
        DecisionStore::search(&mut *tx, words, since_us, limit).await
    }

    /// The newest decisions about one subject in `tenant`'s log
    /// ([`DecisionStore::by_subject`]).
    ///
    /// # Errors
    /// The record store's.
    pub async fn by_subject(
        &self,
        tenant: TenantId,
        subject_type: &str,
        subject_id: &str,
        limit: u32,
    ) -> Result<Vec<DecisionRow>> {
        let mut tx = self.records.begin(tenant).await?;
        DecisionStore::by_subject(&mut *tx, subject_type, subject_id, limit).await
    }

    /// The retention job for one tenant: deletes decisions older than
    /// `retention_days` before `now_us`, a batch per transaction.
    ///
    /// # Errors
    /// The record store's.
    pub async fn purge(&self, tenant: TenantId, retention_days: u32, now_us: i64) -> Result<u64> {
        let before = now_us - i64::from(retention_days) * 86_400 * 1_000_000;
        let mut total = 0;
        loop {
            let mut tx = self.records.begin(tenant).await?;
            let n = DecisionStore::purge_batch(&mut *tx, before).await?;
            tx.commit().await?;
            total += n;
            if n < u64::from(PURGE_BATCH) {
                return Ok(total);
            }
        }
    }
}

#[async_trait::async_trait]
impl DecisionLog for CatalogDecisionLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> Result<u64> {
        if self.buffered() > 0 {
            self.flush().await;
        }
        match self.write(tenant, d).await {
            Ok(id) => Ok(id),
            Err(e) if e.code() == Some(SprigCode::CatalogUnavailable) || e.retryable => {
                tracing::warn!(tenant = %tenant, kind = %d.kind, error = %e,
                    "decision buffered: catalog unreachable");
                let mut g = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
                g.push_back(Pending::Record(tenant, d.clone()));
                while g.len() > BUFFER_LIMIT {
                    g.pop_front();
                }
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> Result<()> {
        if id == 0 {
            return Ok(());
        }
        let mut tx = self.records.begin(tenant).await?;
        DecisionStore::set_outcome(&mut *tx, id, o, None, at_us).await?;
        tx.commit().await
    }
}

#[cfg(test)]
mod tests;
