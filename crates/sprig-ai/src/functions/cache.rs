//! The AI function cache: provider answers by the SHA-256 of function,
//! model, arguments and input, per tenant, in the tenant's catalog records
//! (`m/ai_functions/cache/<key>`, [`RecordFnCache`]).
//!
//! - **Life.** An answer is reused for `ai.functions.cache_days` after it
//!   was stored; older entries are never read and are deleted when answers
//!   are next stored.
//! - **Size.** At most `ai.functions.cache_max_entries` entries: past it
//!   the least recently used go first. A hit refreshes its use time (at
//!   most once a minute, so reads stay reads).
//! - **Who.** The cache is the tenant's: an answer depends only on its
//!   key, so a hit tells the caller nothing they didn't send.
//!
//! The functions' tokens are counted in the assistants' usage records
//! under the role `functions`, so both share the daily budgets
//! ([`add_usage`], [`usage_today`]).
//!
//! The `ai_functions` module's MariaDB migration (the old
//! `t<id>_sprig.ai_fn_cache` table) stays declared so catalog format
//! checks agree with older catalogs; nothing reads or writes that table.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_catalog::migrate::{Migration, Module, Phase};
use sprig_catalog::records::{Pk, RecordStore, RowTable, prefix_end};
use sprig_common::Result;
use sprig_common::ids::{TenantId, UserId};

use super::AiFn;
use crate::budget::UsageToday;
use crate::provider::Usage;

/// The catalog module.
pub const MODULE: &str = "ai_functions";

const DDL_CACHE: &str = "CREATE TABLE IF NOT EXISTS ai_fn_cache (
  k          BINARY(32) PRIMARY KEY,
  fn         VARCHAR(16) NOT NULL,
  model      VARCHAR(255) NOT NULL,
  result     MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  created_at DATETIME(3) NOT NULL,
  used_at    DATETIME(3) NOT NULL,
  KEY by_used (used_at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_fn_cache",
    statements: &[DDL_CACHE],
}];

/// The module, for migrators.
pub const FN_MODULE: Module = Module {
    name: MODULE,
    migrations: MIGRATIONS,
};

/// Entries written in one record transaction at most.
const CHUNK: usize = 500;

/// The cache's records.
pub const CACHE: RowTable = RowTable::new(MODULE, "cache");

/// A hit refreshes its use time only when it is older than this.
const TOUCH_AFTER_MS: u64 = 60_000;

/// The shortest time between two size checks of one tenant's cache.
const PRUNE_GAP: Duration = Duration::from_secs(60);

/// Stores between two size checks of a tenant's cache.
const PRUNE_EVERY: u64 = 32;

/// Where answers are kept.
#[async_trait]
pub trait FnCache: Send + Sync {
    /// The answers stored under `keys` less than `days` days ago.
    ///
    /// # Errors
    /// The store's.
    async fn get(&self, keys: &[[u8; 32]], days: u64) -> Result<HashMap<[u8; 32], String>>;

    /// Stores answers of `f` by `model`; keeps at most `max_entries` and
    /// drops what is older than `days`.
    ///
    /// # Errors
    /// The store's.
    async fn put(
        &self,
        f: AiFn,
        model: &str,
        entries: &[([u8; 32], String)],
        max_entries: u64,
        days: u64,
    ) -> Result<()>;
}

// ---- in memory, for tests ----

/// A cache in memory.
#[derive(Debug, Default)]
pub struct MemoryFnCache {
    rows: Mutex<HashMap<[u8; 32], (String, SystemTime, u64)>>,
    tick: AtomicU64,
}

impl MemoryFnCache {
    /// The number of answers kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether it is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Makes every answer `by` older.
    pub fn age(&self, by: Duration) {
        for v in self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values_mut()
        {
            v.1 = v.1.checked_sub(by).unwrap_or(SystemTime::UNIX_EPOCH);
        }
    }
}

fn fresh(at: SystemTime, days: u64) -> bool {
    at.elapsed()
        .is_ok_and(|e| e < Duration::from_secs(days.saturating_mul(86_400)))
}

#[async_trait]
impl FnCache for MemoryFnCache {
    async fn get(&self, keys: &[[u8; 32]], days: u64) -> Result<HashMap<[u8; 32], String>> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let mut out = HashMap::new();
        for k in keys {
            if let Some(v) = rows.get_mut(k)
                && fresh(v.1, days)
            {
                v.2 = self.tick.fetch_add(1, Ordering::Relaxed);
                out.insert(*k, v.0.clone());
            }
        }
        Ok(out)
    }

    async fn put(
        &self,
        _f: AiFn,
        _model: &str,
        entries: &[([u8; 32], String)],
        max_entries: u64,
        days: u64,
    ) -> Result<()> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        for (k, v) in entries {
            let t = self.tick.fetch_add(1, Ordering::Relaxed);
            rows.insert(*k, (v.clone(), SystemTime::now(), t));
        }
        rows.retain(|_, v| fresh(v.1, days));
        let max = usize::try_from(max_entries).unwrap_or(usize::MAX);
        if rows.len() > max {
            let mut by_use: Vec<([u8; 32], u64)> = rows.iter().map(|(k, v)| (*k, v.2)).collect();
            by_use.sort_by_key(|x| x.1);
            for (k, _) in by_use.into_iter().take(rows.len() - max) {
                rows.remove(&k);
            }
        }
        Ok(())
    }
}

// ---- the tenant's catalog records ----

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(u64::MAX)
}

/// One cached answer.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    /// The function (`classify`, ...).
    #[serde(rename = "f")]
    func: String,
    /// The model.
    #[serde(rename = "m")]
    model: String,
    /// The answer.
    #[serde(rename = "r")]
    result: String,
    /// Stored, ms since the epoch.
    #[serde(rename = "c")]
    created_ms: u64,
    /// Last used, ms since the epoch.
    #[serde(rename = "u")]
    used_ms: u64,
}

fn fresh_ms(created_ms: u64, now: u64, days: u64) -> bool {
    now.saturating_sub(created_ms) < days.saturating_mul(86_400_000)
}

/// Per tenant in this process: stores since the last size check, and when
/// it ran.
type Prunes = Mutex<HashMap<TenantId, (u64, Option<std::time::Instant>)>>;

fn prunes() -> &'static Prunes {
    static P: OnceLock<Prunes> = OnceLock::new();
    P.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The cache in a tenant's catalog records.
pub struct RecordFnCache {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for RecordFnCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordFnCache")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl RecordFnCache {
    /// The cache of `tenant` in `records` (the engine node's catalog
    /// records).
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    /// Whether a size check is due for this tenant (every
    /// [`PRUNE_EVERY`]th store, the first store in a process, and never
    /// twice within [`PRUNE_GAP`]).
    fn prune_due(&self) -> bool {
        let mut p = prunes().lock().unwrap_or_else(PoisonError::into_inner);
        let e = p.entry(self.tenant).or_insert((0, None));
        e.0 += 1;
        let due = (e.0 == 1 || e.0 >= PRUNE_EVERY) && e.1.is_none_or(|t| t.elapsed() >= PRUNE_GAP);
        if due {
            e.0 = 2;
            e.1 = Some(std::time::Instant::now());
        }
        due
    }

    /// Deletes entries older than `days` and the least recently used past
    /// `max_entries`.
    async fn prune(&self, max_entries: u64, days: u64) -> Result<()> {
        let now = now_ms();
        let mut tx = self.records.begin(self.tenant).await?;
        let rows: Vec<(Pk, Entry)> = CACHE.scan(&mut *tx, &Pk::new()).await?;
        let mut keep: Vec<(u64, Pk)> = Vec::with_capacity(rows.len());
        let mut gone = 0usize;
        for (pk, e) in rows {
            if fresh_ms(e.created_ms, now, days) {
                keep.push((e.used_ms, pk));
            } else {
                CACHE.delete(&mut *tx, &pk).await?;
                gone += 1;
            }
        }
        let max = usize::try_from(max_entries).unwrap_or(usize::MAX);
        if keep.len() > max {
            keep.sort_by_key(|x| x.0);
            let over = keep.len() - max;
            for (_, pk) in keep.into_iter().take(over) {
                CACHE.delete(&mut *tx, &pk).await?;
                gone += 1;
            }
        }
        if gone == 0 {
            return tx.rollback().await;
        }
        tx.commit().await
    }
}

#[async_trait]
impl FnCache for RecordFnCache {
    async fn get(&self, keys: &[[u8; 32]], days: u64) -> Result<HashMap<[u8; 32], String>> {
        let mut out = HashMap::new();
        if keys.is_empty() || days == 0 {
            return Ok(out);
        }
        let now = now_ms();
        let mut touch: Vec<([u8; 32], Entry)> = Vec::new();
        {
            let mut tx = self.records.begin(self.tenant).await?;
            for k in keys {
                let Some(mut e) = CACHE.get::<Entry>(&mut *tx, &Pk::new().raw(k)).await? else {
                    continue;
                };
                if !fresh_ms(e.created_ms, now, days) {
                    continue;
                }
                out.insert(*k, e.result.clone());
                if now.saturating_sub(e.used_ms) >= TOUCH_AFTER_MS {
                    e.used_ms = now;
                    touch.push((*k, e));
                }
            }
            tx.rollback().await?;
        }
        if !touch.is_empty() {
            // Best effort: a missed refresh only ages an entry sooner.
            let r: Result<()> = async {
                let mut tx = self.records.begin(self.tenant).await?;
                for (k, e) in &touch {
                    CACHE.put(&mut *tx, &Pk::new().raw(k), e).await?;
                }
                tx.commit().await
            }
            .await;
            if let Err(e) = r {
                tracing::debug!(error = %e, "AI function cache hits weren't refreshed");
            }
        }
        Ok(out)
    }

    async fn put(
        &self,
        f: AiFn,
        model: &str,
        entries: &[([u8; 32], String)],
        max_entries: u64,
        days: u64,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = now_ms();
        for chunk in entries.chunks(CHUNK) {
            let mut tx = self.records.begin(self.tenant).await?;
            for (k, v) in chunk {
                let e = Entry {
                    func: f.as_str().to_owned(),
                    model: model.to_owned(),
                    result: v.clone(),
                    created_ms: now,
                    used_ms: now,
                };
                CACHE.put(&mut *tx, &Pk::new().raw(k), &e).await?;
            }
            tx.commit().await?;
        }
        if self.prune_due() {
            self.prune(max_entries, days).await?;
        }
        Ok(())
    }
}

// ---- usage, shared with the assistants ----

/// The assistants' usage records: `m/ai/usage/<day><user><role>`, the
/// day as days since 1970-01-01 UTC and the user as big-endian u32s, the
/// role a [`Pk::bytes`] part; the value `{"input", "output", "calls"}`.
const USAGE: RowTable = RowTable::new("ai", "usage");

/// The role the functions count their tokens under.
pub const USAGE_ROLE: &str = "functions";

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct UsageRow {
    input: u64,
    output: u64,
    calls: u64,
}

fn today() -> u32 {
    u32::try_from(now_ms() / 86_400_000).unwrap_or(u32::MAX)
}

/// Counts the tokens of a function call against `user` in the tenant's
/// usage records, under the role `functions`, so the functions and the
/// assistants share the daily budgets.
///
/// # Errors
/// The records'.
pub async fn add_usage(
    records: &dyn RecordStore,
    tenant: TenantId,
    user: UserId,
    usage: &Usage,
) -> Result<()> {
    let pk = Pk::new()
        .u32(today())
        .u32(user.0)
        .bytes(USAGE_ROLE.as_bytes());
    let mut tx = records.begin(tenant).await?;
    let mut row: UsageRow = USAGE
        .get_for_update(&mut *tx, &pk)
        .await?
        .unwrap_or_default();
    row.input = row
        .input
        .saturating_add(usage.input_tokens + usage.cache_write_tokens);
    row.output = row.output.saturating_add(usage.output_tokens);
    row.calls = row.calls.saturating_add(1);
    USAGE.put(&mut *tx, &pk, &row).await?;
    tx.commit().await
}

/// Tokens used today (UTC) by the tenant and by `user`, every role
/// counted.
///
/// # Errors
/// The records'.
pub async fn usage_today(
    records: &dyn RecordStore,
    tenant: TenantId,
    user: UserId,
) -> Result<UsageToday> {
    let mut tx = records.begin(tenant).await?;
    let lo = USAGE.key(&Pk::new().u32(today()));
    let hi = prefix_end(&lo);
    let rows = sprig_catalog::records::scan_all(&mut *tx, &lo, &hi).await?;
    tx.rollback().await?;
    let mut out = UsageToday::default();
    for (k, v) in rows {
        let Ok(r) = serde_json::from_slice::<UsageRow>(&v) else {
            continue;
        };
        let n = r.input.saturating_add(r.output);
        out.tenant = out.tenant.saturating_add(n);
        let uid = k
            .get(lo.len()..lo.len() + 4)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes);
        if uid == Some(user.0) {
            out.user = out.user.saturating_add(n);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod record_tests {
    use super::*;
    use sprig_catalog::records::mem::MemRecords;

    fn store() -> Arc<dyn RecordStore> {
        Arc::new(MemRecords::default())
    }

    #[tokio::test]
    async fn answers_are_kept_read_back_and_pruned() {
        let rs = store();
        let c = RecordFnCache::new(Arc::clone(&rs), TenantId(7));
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];
        c.put(AiFn::Classify, "p/m", &[(k1, "bug".into())], 10, 30)
            .await
            .unwrap();
        let got = c.get(&[k1, k2], 30).await.unwrap();
        assert_eq!(got.get(&k1).map(String::as_str), Some("bug"));
        assert!(!got.contains_key(&k2));
        // Another tenant sees nothing.
        let other = RecordFnCache::new(Arc::clone(&rs), TenantId(8));
        assert!(other.get(&[k1], 30).await.unwrap().is_empty());
        // Zero days: nothing is read.
        assert!(c.get(&[k1], 0).await.unwrap().is_empty());
        // Past the cap, the least recently used go.
        let many: Vec<([u8; 32], String)> = (10u8..20).map(|i| ([i; 32], i.to_string())).collect();
        c.put(AiFn::Summarize, "p/m", &many, 3, 30).await.unwrap();
        c.prune(3, 30).await.unwrap();
        let mut tx = rs.begin(TenantId(7)).await.unwrap();
        let left: Vec<(Pk, Entry)> = CACHE.scan(&mut *tx, &Pk::new()).await.unwrap();
        assert_eq!(left.len(), 3, "{left:?}");
    }

    #[tokio::test]
    async fn usage_is_summed_for_the_tenant_and_the_user() {
        let rs = store();
        let t = TenantId(9);
        let u = Usage {
            input_tokens: 10,
            output_tokens: 5,
            ..Usage::default()
        };
        add_usage(&*rs, t, UserId(1), &u).await.unwrap();
        add_usage(&*rs, t, UserId(1), &u).await.unwrap();
        add_usage(&*rs, t, UserId(2), &u).await.unwrap();
        let got = usage_today(&*rs, t, UserId(1)).await.unwrap();
        assert_eq!((got.tenant, got.user), (45, 30));
        let got = usage_today(&*rs, t, UserId(3)).await.unwrap();
        assert_eq!((got.tenant, got.user), (45, 0));
    }
}
