//! The AI function cache: provider answers by the SHA-256 of function,
//! model, arguments and input, per tenant in `t<id>_sprig.ai_fn_cache`
//! (catalog module `ai_functions`, created the first time a gateway needs
//! it for a tenant).
//!
//! - **Life.** An answer is reused for `ai.functions.cache_days` after it
//!   was stored; older rows are never read and are deleted when answers
//!   are next stored.
//! - **Size.** At most `ai.functions.cache_max_entries` rows: past it the
//!   least recently used go first. Each hit refreshes `used_at`.
//! - **Who.** The cache is the tenant's: an answer depends only on its
//!   key, so a hit tells the caller nothing they didn't send.
//!
//! The functions' tokens are counted in the assistants' `ai_usage` table
//! under the role `functions`, so both share the daily budgets
//! ([`add_usage`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_common::Result;
use sprig_common::ids::UserId;
use sprig_common::names::quote;

use super::AiFn;
use crate::audit::{CatalogAudit, unavailable};
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

/// Keys read or written in one statement at most.
const CHUNK: usize = 500;

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

// ---- the tenant catalog ----

/// Catalog schemas known to hold the module, in this process.
fn ready() -> &'static Mutex<HashSet<String>> {
    static READY: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    READY.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Stores into each schema since its last size check.
fn stores() -> &'static Mutex<HashMap<String, u64>> {
    static STORES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The cache in a tenant's catalog schema.
pub struct CatalogFnCache {
    pool: mysql_async::Pool,
    schema: String,
}

impl std::fmt::Debug for CatalogFnCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogFnCache")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

fn marks(n: usize) -> String {
    vec!["?"; n].join(", ")
}

impl CatalogFnCache {
    /// The cache in `schema` (`t<id>_sprig`), reached through `pool` (the
    /// catalog writer's).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
        }
    }

    fn t(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote("ai_fn_cache"))
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self.pool.get_conn_bounded().await.map_err(unavailable)?;
        let known = ready()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&self.schema);
        if !known {
            let have: Option<u64> = conn
                .exec_first(
                    "SELECT COUNT(*) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_fn_cache'",
                    (self.schema.as_str(),),
                )
                .await
                .map_err(unavailable)?;
            if have.unwrap_or(0) == 0 {
                Migrator::new(&[FN_MODULE])?
                    .run(&mut conn, &self.schema, &MigrateOpts::existing())
                    .await?;
            }
            ready()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(self.schema.clone());
        }
        Ok(conn)
    }

    /// Deletes rows older than `days` and the least recently used past
    /// `max_entries`.
    async fn prune(&self, c: &mut mysql_async::Conn, max_entries: u64, days: u64) -> Result<()> {
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE created_at < UTC_TIMESTAMP(3) - INTERVAL ? DAY LIMIT 10000",
                self.t()
            ),
            (days,),
        )
        .await
        .map_err(unavailable)?;
        let n: Option<u64> = c
            .query_first(format!("SELECT COUNT(*) FROM {}", self.t()))
            .await
            .map_err(unavailable)?;
        let over = n.unwrap_or(0).saturating_sub(max_entries);
        if over > 0 {
            c.exec_drop(
                format!("DELETE FROM {} ORDER BY used_at LIMIT ?", self.t()),
                (over,),
            )
            .await
            .map_err(unavailable)?;
        }
        Ok(())
    }
}

#[async_trait]
impl FnCache for CatalogFnCache {
    async fn get(&self, keys: &[[u8; 32]], days: u64) -> Result<HashMap<[u8; 32], String>> {
        let mut out = HashMap::new();
        if keys.is_empty() || days == 0 {
            return Ok(out);
        }
        let mut c = self.conn().await?;
        for chunk in keys.chunks(CHUNK) {
            let mut params: Vec<mysql_async::Value> =
                chunk.iter().map(|k| k.to_vec().into()).collect();
            params.push(days.into());
            let rows: Vec<(Vec<u8>, String)> = c
                .exec(
                    format!(
                        "SELECT k, result FROM {} WHERE k IN ({}) \
                         AND created_at >= UTC_TIMESTAMP(3) - INTERVAL ? DAY",
                        self.t(),
                        marks(chunk.len())
                    ),
                    params,
                )
                .await
                .map_err(unavailable)?;
            let hits: Vec<mysql_async::Value> =
                rows.iter().map(|(k, _)| k.clone().into()).collect();
            for (k, v) in rows {
                if let Ok(k) = <[u8; 32]>::try_from(k.as_slice()) {
                    out.insert(k, v);
                }
            }
            if !hits.is_empty() {
                // Best effort: a missed refresh only ages an entry sooner.
                let n = hits.len();
                if let Err(e) = c
                    .exec_drop(
                        format!(
                            "UPDATE {} SET used_at = UTC_TIMESTAMP(3) WHERE k IN ({})",
                            self.t(),
                            marks(n)
                        ),
                        hits,
                    )
                    .await
                {
                    tracing::debug!(error = %e, "AI function cache hits weren't refreshed");
                }
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
        let mut c = self.conn().await?;
        for chunk in entries.chunks(CHUNK) {
            let mut params: Vec<mysql_async::Value> = Vec::with_capacity(chunk.len() * 4);
            for (k, v) in chunk {
                params.push(k.to_vec().into());
                params.push(f.as_str().into());
                params.push(model.into());
                params.push(v.as_str().into());
            }
            let rows =
                vec!["(?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))"; chunk.len()].join(", ");
            c.exec_drop(
                format!(
                    "INSERT INTO {} (k, fn, model, result, created_at, used_at) VALUES {rows} \
                     ON DUPLICATE KEY UPDATE result = VALUES(result), \
                     created_at = VALUES(created_at), used_at = VALUES(used_at)",
                    self.t()
                ),
                params,
            )
            .await
            .map_err(unavailable)?;
        }
        let due = {
            let mut s = stores().lock().unwrap_or_else(PoisonError::into_inner);
            let n = s.entry(self.schema.clone()).or_insert(0);
            *n += 1;
            if *n >= PRUNE_EVERY {
                *n = 0;
                true
            } else {
                // The first store of a process checks too.
                *n == 1
            }
        };
        if due {
            self.prune(&mut c, max_entries, days).await?;
        }
        Ok(())
    }
}

/// Counts the tokens of a function call against `user` in the tenant's
/// `ai_usage`, under the role `functions`.
///
/// # Errors
/// The catalog's.
pub async fn add_usage(audit: &CatalogAudit, user: UserId, usage: &Usage) -> Result<()> {
    let mut c = audit.conn().await?;
    c.exec_drop(
        format!(
            "INSERT INTO {} (day, user_id, role, input_tokens, output_tokens, calls) \
             VALUES (UTC_DATE(), ?, 'functions', ?, ?, 1) \
             ON DUPLICATE KEY UPDATE input_tokens = input_tokens + VALUES(input_tokens), \
             output_tokens = output_tokens + VALUES(output_tokens), calls = calls + 1",
            audit.t("ai_usage")
        ),
        (
            user.0,
            usage.input_tokens + usage.cache_write_tokens,
            usage.output_tokens,
        ),
    )
    .await
    .map_err(unavailable)?;
    Ok(())
}
