//! The tenant catalog's recommendations and email subscriptions as the
//! engine's catalog records ([`CatalogRecStore`]).
//!
//! ```text
//! m/approvals/recs/<rec id u64>              RecRow (the whole item, its due time)
//! m/approvals/live/<fingerprint>             rec id (one live item per fingerprint)
//! m/approvals/due/<due µs><rec id>           (empty: the due index)
//! m/approvals/raise/<kind><target>           (empty: the raise's lock)
//! m/approvals/subs/<user name>               Subscription
//! ctr/rec                                    the last rec id handed out
//! ```
//!
//! Like the decision log, these are not versioned catalog state: they are
//! read on demand and written in short engine transactions of their own,
//! so a recommendation never invalidates a gateway's catalog cache. The
//! live-fingerprint record stands for the unique key on live items' fingerprints
//! (1062 when a second live item would take one); the raise locks its
//! kind and target first, as `SELECT ... FOR UPDATE` on that index range
//! did. Times are microseconds since the epoch.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, next_ids};
use sprig_catalog::{Migration, Module};
use sprig_common::ids::TenantId;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use crate::machine::{self, Verdict, VerdictPlan};
use crate::model::{Micros, Proposal, RecId, Recommendation, State};
use crate::policy::Policy;
use crate::store::{DUE_APPLYING_TIMEOUT, Filter, Raised, RecStore};

/// The module name in the catalog's format records.
pub const MODULE: &str = "approvals";

/// Items by number.
pub const RECS: RowTable = RowTable::new(MODULE, "recs");
/// Live items by fingerprint.
pub const LIVE: RowTable = RowTable::new(MODULE, "live");
/// Items by due time.
pub const DUE: RowTable = RowTable::new(MODULE, "due");
/// The raise's lock per kind and target.
pub const RAISE: RowTable = RowTable::new(MODULE, "raise");
/// Email subscriptions by user.
pub const SUBS: RowTable = RowTable::new(MODULE, "subs");
/// The item counter.
pub const COUNTER: &str = "rec";

/// The module's MariaDB migrations: none, its rows are engine records.
pub static APPROVALS_MIGRATIONS: &[Migration] = &[];

/// The module, for [`sprig_catalog::Migrator::new`] and the catalog
/// bootstrap hooks.
pub const APPROVALS_MODULE: Module = Module {
    name: MODULE,
    migrations: APPROVALS_MIGRATIONS,
};

fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        ErrorKind::Backend { errno, .. } => Some(errno),
        _ => None,
    }
}

/// A stored item: the item and its next due time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecRow {
    /// The item (its id's number is the record's key).
    pub rec: Recommendation,
    /// When its timers may fire next.
    pub due_at_us: Option<Micros>,
}

/// An order-preserving key part for a signed time.
fn at_key(us: i64) -> [u8; 8] {
    #[allow(clippy::cast_sign_loss)]
    let u = (us as u64) ^ (1u64 << 63);
    u.to_be_bytes()
}

fn live(state: State) -> bool {
    matches!(
        state,
        State::Open | State::Approved | State::Applying | State::Snoozed
    )
}

fn closed(state: State) -> bool {
    matches!(
        state,
        State::Applied | State::Failed | State::Ignored | State::Superseded | State::Expired
    )
}

fn id_of(pk: &Pk) -> Option<u64> {
    let b = pk.0.get(pk.0.len().checked_sub(8)?..)?;
    <[u8; 8]>::try_from(b).ok().map(u64::from_be_bytes)
}

/// One tenant's items and subscriptions, in the caller's transaction.
#[derive(Debug, Clone, Copy)]
pub struct RecRecords {
    tenant: TenantId,
}

impl RecRecords {
    /// `tenant`'s.
    #[must_use]
    pub fn new(tenant: TenantId) -> Self {
        Self { tenant }
    }

    fn decode(self, n: u64, r: RecRow) -> Recommendation {
        let mut rec = r.rec;
        rec.id = RecId::Tenant(self.tenant, n);
        rec
    }

    /// Removes item `n`'s index entries for its stored form `old`.
    async fn unindex(rec: &mut dyn RecordTx, n: u64, old: &RecRow) -> Result<()> {
        let fp = Pk::new().bytes(old.rec.fingerprint.as_bytes());
        if live(old.rec.state) && LIVE.get_for_update::<u64>(rec, &fp).await? == Some(n) {
            LIVE.delete(rec, &fp).await?;
        }
        if let Some(d) = old.due_at_us {
            DUE.delete(rec, &Pk::new().raw(&at_key(d)).u64(n)).await?;
        }
        Ok(())
    }

    /// Writes item `n` as `r` with its index entries.
    async fn store(rec: &mut dyn RecordTx, n: u64, r: &Recommendation) -> Result<()> {
        if live(r.state) {
            let fp = Pk::new().bytes(r.fingerprint.as_bytes());
            // Locked: two writers making live items of one fingerprint are
            // serial here, as the unique key made them.
            match LIVE.get_for_update::<u64>(rec, &fp).await? {
                Some(other) if other != n => {
                    return Err(SprigError::backend(
                        1062,
                        *b"23000",
                        format!("Duplicate entry '{}' for key 'uk_live_fp'", r.fingerprint),
                    ));
                }
                Some(_) => {}
                None => LIVE.put(rec, &fp, &n).await?,
            }
        }
        let due = machine::next_due(r, DUE_APPLYING_TIMEOUT);
        if let Some(d) = due {
            DUE.put(rec, &Pk::new().raw(&at_key(d)).u64(n), &()).await?;
        }
        let row = RecRow {
            rec: r.clone(),
            due_at_us: due,
        };
        RECS.put(rec, &Pk::new().u64(n), &row).await
    }

    /// Inserts a new item; returns its number.
    ///
    /// # Errors
    /// The record store's (a backend 1062 when a live item with the
    /// fingerprint exists).
    pub async fn insert(&self, rec: &mut dyn RecordTx, r: &Recommendation) -> Result<u64> {
        let n = next_ids(rec, COUNTER, 1).await?;
        Self::store(rec, n, r).await?;
        Ok(n)
    }

    /// Writes `r` if its stored version is `expected`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn update(
        &self,
        rec: &mut dyn RecordTx,
        r: &Recommendation,
        expected: u64,
    ) -> Result<bool> {
        let n = r.id.number();
        let pk = Pk::new().u64(n);
        let Some(old) = RECS.get_for_update::<RecRow>(rec, &pk).await? else {
            return Ok(false);
        };
        if old.rec.version != expected {
            return Ok(false);
        }
        Self::unindex(rec, n, &old).await?;
        Self::store(rec, n, r).await?;
        Ok(true)
    }

    /// One item.
    ///
    /// # Errors
    /// The record store's.
    pub async fn get(&self, rec: &mut dyn RecordTx, n: u64) -> Result<Option<Recommendation>> {
        Ok(RECS
            .get::<RecRow>(rec, &Pk::new().u64(n))
            .await?
            .map(|r| self.decode(n, r)))
    }

    async fn all(&self, rec: &mut dyn RecordTx) -> Result<Vec<Recommendation>> {
        let mut out = Vec::new();
        for (pk, r) in RECS.scan::<RecRow>(rec, &Pk::new()).await? {
            if let Some(n) = id_of(&pk) {
                out.push(self.decode(n, r));
            }
        }
        Ok(out)
    }

    /// Items passing `f`, newest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn list(&self, rec: &mut dyn RecordTx, f: &Filter) -> Result<Vec<Recommendation>> {
        let states: Vec<State> = if !f.states.is_empty() {
            f.states.clone()
        } else if f.all {
            Vec::new()
        } else {
            State::ALL.iter().copied().filter(|s| s.is_live()).collect()
        };
        let mut out: Vec<Recommendation> = self
            .all(rec)
            .await?
            .into_iter()
            .rev()
            .filter(|r| states.is_empty() || states.contains(&r.state))
            .filter(|r| f.level.is_none_or(|l| r.level == l))
            .filter(|r| f.db.is_none_or(|d| r.subject.db == Some(d)))
            .filter(|r| f.object.is_none_or(|o| r.subject.object == Some(o)))
            .filter(|r| {
                f.kinds.is_empty()
                    || f.kinds.iter().any(|k| {
                        let kind = r.kind.as_str();
                        kind == k
                            || kind
                                .strip_prefix(k.as_str())
                                .is_some_and(|x| x.starts_with('.'))
                    })
            })
            .collect();
        out.truncate(f.limit());
        Ok(out)
    }

    /// Items due at `now`, soonest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn due(
        &self,
        rec: &mut dyn RecordTx,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<Recommendation>> {
        let lo = DUE.prefix();
        let hi = DUE.key(&Pk::new().raw(&at_key(now.saturating_add(1))));
        let base = lo.len();
        let entries = rec.scan(&lo, &hi, limit).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (k, _) in entries {
            if let Some(n) = id_of(&Pk(k[base..].to_vec()))
                && let Some(r) = self.get(rec, n).await?
            {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// Deletes up to 1000 closed items changed before `before` (an ignored
    /// item with no end to its ignoring stays).
    ///
    /// # Errors
    /// The record store's.
    pub async fn purge(&self, rec: &mut dyn RecordTx, before: Micros) -> Result<u64> {
        let mut n = 0u64;
        for (pk, r) in RECS.scan::<RecRow>(rec, &Pk::new()).await? {
            if n >= 1000 {
                break;
            }
            let item = &r.rec;
            let forever = item.state == State::Ignored
                && serde_json::to_value(item)
                    .ok()
                    .and_then(|v| v.get("ignore").and_then(|i| i.get("until_us")).cloned())
                    .is_none_or(|u| u.is_null());
            if closed(item.state) && item.updated_at_us < before && !forever {
                let Some(id) = id_of(&pk) else { continue };
                Self::unindex(rec, id, &r).await?;
                RECS.delete(rec, &pk).await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// The raise, in `rec`'s transaction.
    ///
    /// # Errors
    /// The record store's; 1062 or 39071 when another raise of the same
    /// advice won (the caller retries).
    pub async fn raise(
        &self,
        rec: &mut dyn RecordTx,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        let fp = p.fingerprint();
        // The raises of one kind and target are serial.
        let lock = Pk::new()
            .bytes(p.kind.as_str().as_bytes())
            .bytes(p.target.as_bytes());
        RAISE.get_for_update::<()>(rec, &lock).await?;
        RAISE.put(rec, &lock, &()).await?;
        let everything = self.all(rec).await?;
        let same: Vec<Recommendation> = everything
            .iter()
            .filter(|r| r.kind == p.kind && r.target == p.target && live(r.state))
            .cloned()
            .collect();
        let ignored = if same.iter().any(|r| r.fingerprint == fp) {
            None
        } else {
            everything
                .iter()
                .filter(|r| r.fingerprint == fp && r.state == State::Ignored)
                .max_by_key(|r| r.updated_at_us)
                .cloned()
        };
        let plan = machine::raise(
            p,
            policy,
            &same,
            ignored.as_ref(),
            ancestry,
            RecId::Tenant(self.tenant, 0),
            now,
        );
        let mut stored = Vec::new();
        for w in &plan.writes {
            let mut w = w.clone();
            if w.id.number() == 0 {
                let n = self.insert(rec, &w).await?;
                w.id = RecId::Tenant(self.tenant, n);
            } else {
                let expected = same.iter().find(|x| x.id == w.id).map_or(0, |x| x.version);
                if !self.update(rec, &w, expected).await? {
                    return Err(SprigError::new(
                        SprigCode::CatalogConflict,
                        "recommendation changed during the raise",
                    ));
                }
            }
            stored.push(w);
        }
        let mut out = Raised::from_plan(&plan, &stored);
        if plan.verdict == VerdictPlan::Suppressed
            && let Some(i) = ignored
        {
            out.verdict = Verdict::Suppressed(i.id);
        }
        Ok(out)
    }

    /// Email subscriptions, by user.
    ///
    /// # Errors
    /// The record store's.
    pub async fn subscriptions(&self, rec: &mut dyn RecordTx) -> Result<Vec<Subscription>> {
        SUBS.rows(rec, &Pk::new()).await
    }

    /// Sets one user's subscription (`delivery = off` keeps the row; the
    /// last digest time stays).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_subscription(
        &self,
        rec: &mut dyn RecordTx,
        s: &Subscription,
        now: Micros,
    ) -> Result<()> {
        let pk = Pk::new().bytes(s.user.as_bytes());
        let last = SUBS
            .get_for_update::<Subscription>(rec, &pk)
            .await?
            .and_then(|o| o.last_digest_us);
        let row = Subscription {
            last_digest_us: last,
            updated_at_us: Some(now),
            ..s.clone()
        };
        SUBS.put(rec, &pk, &row).await
    }

    /// Records a sent digest.
    ///
    /// # Errors
    /// The record store's.
    pub async fn digest_sent(&self, rec: &mut dyn RecordTx, user: &str, now: Micros) -> Result<()> {
        let pk = Pk::new().bytes(user.as_bytes());
        if let Some(mut s) = SUBS.get_for_update::<Subscription>(rec, &pk).await? {
            s.last_digest_us = Some(now);
            SUBS.put(rec, &pk, &s).await?;
        }
        Ok(())
    }
}

/// One user's email preference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Subscription {
    /// The tenant user (or an operator label).
    pub user: String,
    /// The address.
    pub email: String,
    /// `immediate`, `digest` or `off`.
    pub delivery: String,
    /// `*` or a comma list of kinds and families.
    #[serde(default = "star")]
    pub kinds: String,
    /// The last digest sent.
    #[serde(default)]
    pub last_digest_us: Option<i64>,
    /// When it was last set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_us: Option<i64>,
}

fn star() -> String {
    "*".into()
}

/// A tenant's recommendations in its catalog records.
pub struct CatalogRecStore {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for CatalogRecStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogRecStore")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl CatalogRecStore {
    /// `tenant`'s store.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    fn recs(&self) -> RecRecords {
        RecRecords::new(self.tenant)
    }

    async fn begin(&self) -> Result<Box<dyn RecordTx>> {
        self.records.begin(self.tenant).await
    }

    /// Email subscriptions, by user.
    ///
    /// # Errors
    /// The record store's.
    pub async fn subscriptions(&self) -> Result<Vec<Subscription>> {
        let mut tx = self.begin().await?;
        self.recs().subscriptions(&mut *tx).await
    }

    /// Sets one user's subscription.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_subscription(&self, s: &Subscription, now: Micros) -> Result<()> {
        let mut tx = self.begin().await?;
        self.recs().set_subscription(&mut *tx, s, now).await?;
        tx.commit().await
    }

    /// Records a sent digest.
    ///
    /// # Errors
    /// The record store's.
    pub async fn digest_sent(&self, user: &str, now: Micros) -> Result<()> {
        let mut tx = self.begin().await?;
        self.recs().digest_sent(&mut *tx, user, now).await?;
        tx.commit().await
    }
}

#[async_trait::async_trait]
impl RecStore for CatalogRecStore {
    async fn raise(
        &self,
        p: &Proposal,
        policy: &Policy,
        ancestry: &[TenantId],
        now: Micros,
    ) -> Result<Raised> {
        let mut last = None;
        for _ in 0..3 {
            let r = async {
                let mut tx = self.begin().await?;
                let out = self
                    .recs()
                    .raise(&mut *tx, p, policy, ancestry, now)
                    .await?;
                tx.commit().await?;
                Ok(out)
            }
            .await;
            match r {
                Err(e)
                    if matches!(errno(&e), Some(1062))
                        || e.code() == Some(SprigCode::CatalogConflict) =>
                {
                    last = Some(e);
                }
                r => return r,
            }
        }
        Err(last.unwrap_or_else(|| SprigError::internal("raise retries")))
    }

    async fn get(&self, n: u64) -> Result<Option<Recommendation>> {
        let mut tx = self.begin().await?;
        self.recs().get(&mut *tx, n).await
    }

    async fn list(&self, f: &Filter) -> Result<Vec<Recommendation>> {
        let mut tx = self.begin().await?;
        self.recs().list(&mut *tx, f).await
    }

    async fn put(&self, rec: &Recommendation, expected: u64) -> Result<bool> {
        let mut tx = self.begin().await?;
        let ok = self.recs().update(&mut *tx, rec, expected).await?;
        if ok {
            tx.commit().await?;
        }
        Ok(ok)
    }

    async fn due(&self, now: Micros, limit: usize) -> Result<Vec<Recommendation>> {
        let mut tx = self.begin().await?;
        self.recs().due(&mut *tx, now, limit).await
    }

    async fn purge(&self, before: Micros) -> Result<u64> {
        let mut tx = self.begin().await?;
        let n = self.recs().purge(&mut *tx, before).await?;
        tx.commit().await?;
        Ok(n)
    }
}

#[cfg(test)]
mod live_tests;
