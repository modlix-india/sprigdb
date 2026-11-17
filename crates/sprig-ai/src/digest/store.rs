//! Where digests are kept: the tenant catalog's `ai_digests` table
//! (module `ai_digests`, created the first time a digest is stored), one
//! row per cadence and period.

use sprig_catalog::pool_wait::Bounded as _;
use std::sync::Mutex;

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};
use tokio::sync::OnceCell;

use super::{Cadence, Facts};
use crate::audit::{ConvId, unavailable};

/// The catalog module.
pub const MODULE: &str = "ai_digests";

const DDL_DIGESTS: &str = "CREATE TABLE IF NOT EXISTS ai_digests (
  digest_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  cadence      ENUM('weekly','daily') NOT NULL,
  period_start DATETIME(3) NOT NULL,
  period_end   DATETIME(3) NOT NULL,
  status       ENUM('worded','facts') NOT NULL,
  facts        JSON NOT NULL,
  facts_text   MEDIUMTEXT NOT NULL,
  words        MEDIUMTEXT NULL,
  note         VARCHAR(500) NULL,
  conv_id      BIGINT UNSIGNED NULL,
  model        VARCHAR(200) NULL,
  token_usage  JSON NULL,
  emailed_at   DATETIME(3) NULL,
  created_at   DATETIME(3) NOT NULL,
  UNIQUE KEY by_period (cadence, period_end),
  KEY by_created (created_at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static DIGEST_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_digests",
    statements: &[DDL_DIGESTS],
}];

/// The module, for migrators.
pub const DIGEST_MODULE: Module = Module {
    name: MODULE,
    migrations: DIGEST_MIGRATIONS,
};

/// Whether a digest was worded by the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// The facts and the model's words.
    Worded,
    /// The facts alone (no model, a spent budget, a failed call).
    Facts,
}

impl Status {
    /// The catalog's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Status::Worded => "worded",
            Status::Facts => "facts",
        }
    }
}

/// A digest before it is stored.
#[derive(Debug, Clone, PartialEq)]
pub struct NewDigest {
    /// Worded or not.
    pub status: Status,
    /// The facts.
    pub facts: Facts,
    /// The facts as Markdown.
    pub facts_text: String,
    /// The model's words.
    pub text: Option<String>,
    /// Why there are no words.
    pub note: Option<String>,
    /// The conversation the wording is stored in.
    pub conv: Option<ConvId>,
    /// `provider/model`.
    pub model: Option<String>,
    /// Tokens used.
    pub usage: Value,
}

/// A stored digest.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Digest {
    /// Its id.
    pub id: u64,
    /// Weekly or daily.
    pub cadence: Cadence,
    /// The period's start, ms.
    pub from_ms: i64,
    /// Its end, ms.
    pub to_ms: i64,
    /// Worded or not.
    pub status: Status,
    /// The facts.
    pub facts: Facts,
    /// The facts as Markdown.
    pub facts_text: String,
    /// The model's words.
    pub text: Option<String>,
    /// Why there are no words.
    pub note: Option<String>,
    /// The conversation of the wording, with what was sent.
    pub conversation: Option<u64>,
    /// `provider/model`.
    pub model: Option<String>,
    /// Tokens used.
    pub usage: Value,
    /// When it was emailed, ms.
    pub emailed_ms: Option<i64>,
    /// When it was made, ms.
    pub created_ms: i64,
}

impl Digest {
    /// What people read: the facts, then the model's words.
    #[must_use]
    pub fn body(&self) -> String {
        match &self.text {
            Some(t) => format!("{}\n## In short\n\n{t}\n", self.facts_text),
            None => format!(
                "{}\n{}\n",
                self.facts_text,
                self.note.as_deref().unwrap_or("Not worded.")
            ),
        }
    }
}

/// Where digests are kept.
#[async_trait]
pub trait DigestStore: Send + Sync {
    /// The digest of `cadence` whose period ends at `to_ms`.
    async fn for_period(&self, cadence: Cadence, to_ms: i64) -> Result<Option<Digest>>;
    /// The newest digest of `cadence`.
    async fn last(&self, cadence: Cadence) -> Result<Option<Digest>>;
    /// Stores one; returns its id. A second for the same period is
    /// ignored and the first one's id returned.
    async fn insert(&self, d: &NewDigest) -> Result<u64>;
    /// One digest.
    async fn get(&self, id: u64) -> Result<Option<Digest>>;
    /// The newest digests, newest first.
    async fn list(&self, limit: u32) -> Result<Vec<Digest>>;
    /// Marks one emailed.
    async fn emailed(&self, id: u64) -> Result<()>;
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

// ---- in memory ----

/// Digests in memory, for tests.
#[derive(Debug, Default)]
pub struct MemoryDigests {
    inner: Mutex<Vec<Digest>>,
}

impl MemoryDigests {
    fn all(&self) -> std::sync::MutexGuard<'_, Vec<Digest>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl DigestStore for MemoryDigests {
    async fn for_period(&self, cadence: Cadence, to_ms: i64) -> Result<Option<Digest>> {
        Ok(self
            .all()
            .iter()
            .find(|d| d.cadence == cadence && d.to_ms == to_ms)
            .cloned())
    }

    async fn last(&self, cadence: Cadence) -> Result<Option<Digest>> {
        Ok(self
            .all()
            .iter()
            .filter(|d| d.cadence == cadence)
            .max_by_key(|d| d.to_ms)
            .cloned())
    }

    async fn insert(&self, d: &NewDigest) -> Result<u64> {
        let mut all = self.all();
        if let Some(x) = all
            .iter()
            .find(|x| x.cadence == d.facts.cadence && x.to_ms == d.facts.to_ms)
        {
            return Ok(x.id);
        }
        let id = all.len() as u64 + 1;
        all.push(Digest {
            id,
            cadence: d.facts.cadence,
            from_ms: d.facts.from_ms,
            to_ms: d.facts.to_ms,
            status: d.status,
            facts: d.facts.clone(),
            facts_text: d.facts_text.clone(),
            text: d.text.clone(),
            note: d.note.clone(),
            conversation: d.conv.map(|c| c.0),
            model: d.model.clone(),
            usage: d.usage.clone(),
            emailed_ms: None,
            created_ms: now_ms(),
        });
        Ok(id)
    }

    async fn get(&self, id: u64) -> Result<Option<Digest>> {
        Ok(self.all().iter().find(|d| d.id == id).cloned())
    }

    async fn list(&self, limit: u32) -> Result<Vec<Digest>> {
        let mut v = self.all().clone();
        v.sort_by_key(|d| std::cmp::Reverse((d.to_ms, d.id)));
        v.truncate(limit as usize);
        Ok(v)
    }

    async fn emailed(&self, id: u64) -> Result<()> {
        if let Some(d) = self.all().iter_mut().find(|d| d.id == id) {
            d.emailed_ms = Some(now_ms());
        }
        Ok(())
    }
}

// ---- the tenant catalog ----

/// Digests in a tenant's catalog schema.
pub struct CatalogDigests {
    pool: mysql_async::Pool,
    schema: String,
    ready: OnceCell<()>,
}

impl std::fmt::Debug for CatalogDigests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDigests")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

const MS: &str = "CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {}) DIV 1000 AS SIGNED)";

fn ms(col: &str) -> String {
    MS.replace("{}", col)
}

impl CatalogDigests {
    /// The store in `schema` (`t<id>_sprig`), through `pool` (the catalog
    /// writer's).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            ready: OnceCell::new(),
        }
    }

    fn t(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote("ai_digests"))
    }

    /// Whether the module exists, without creating it (a reader that finds
    /// none has nothing to list).
    ///
    /// # Errors
    /// 39021 when the catalog is unreachable.
    pub async fn exists(&self) -> Result<bool> {
        let mut c = self.pool.get_conn_bounded().await.map_err(unavailable)?;
        let have: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_digests'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(unavailable)?;
        Ok(have.unwrap_or(0) > 0)
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self.pool.get_conn_bounded().await.map_err(unavailable)?;
        self.ready
            .get_or_try_init(|| async {
                let have: Option<u64> = conn
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ai_digests'",
                        (self.schema.as_str(),),
                    )
                    .await
                    .map_err(unavailable)?;
                if have.unwrap_or(0) == 0 {
                    Migrator::new(&[DIGEST_MODULE])?
                        .run(&mut conn, &self.schema, &MigrateOpts::default())
                        .await?;
                }
                Ok::<(), SprigError>(())
            })
            .await?;
        Ok(conn)
    }

    fn select(&self, filter: &str) -> String {
        format!(
            "SELECT digest_id, cadence, {}, {}, status, CAST(facts AS CHAR), facts_text, words, \
             note, conv_id, model, CAST(token_usage AS CHAR), \
             IF(emailed_at IS NULL, NULL, {}), {} FROM {} {filter}",
            ms("period_start"),
            ms("period_end"),
            ms("emailed_at"),
            ms("created_at"),
            self.t()
        )
    }

    async fn rows(&self, sql: String, params: mysql_async::Params) -> Result<Vec<Digest>> {
        let mut c = self.conn().await?;
        let rows: Vec<mysql_async::Row> = c.exec(sql, params).await.map_err(unavailable)?;
        Ok(rows.into_iter().filter_map(Self::digest).collect())
    }

    fn digest(mut r: mysql_async::Row) -> Option<Digest> {
        let text = |r: &mut mysql_async::Row, i: usize| -> Option<String> {
            r.take_opt::<Option<String>, _>(i)
                .and_then(Result::ok)
                .flatten()
        };
        let cadence = Cadence::parse(&text(&mut r, 1)?)?;
        let facts: Facts = serde_json::from_str(&text(&mut r, 5)?).ok()?;
        Some(Digest {
            id: r.take_opt::<u64, _>(0)?.ok()?,
            cadence,
            from_ms: r.take_opt::<i64, _>(2)?.ok()?,
            to_ms: r.take_opt::<i64, _>(3)?.ok()?,
            status: if text(&mut r, 4).as_deref() == Some("worded") {
                Status::Worded
            } else {
                Status::Facts
            },
            facts,
            facts_text: text(&mut r, 6).unwrap_or_default(),
            text: text(&mut r, 7),
            note: text(&mut r, 8),
            conversation: r
                .take_opt::<Option<u64>, _>(9)
                .and_then(Result::ok)
                .flatten(),
            model: text(&mut r, 10),
            usage: text(&mut r, 11)
                .and_then(|u| serde_json::from_str(&u).ok())
                .unwrap_or(Value::Null),
            emailed_ms: r
                .take_opt::<Option<i64>, _>(12)
                .and_then(Result::ok)
                .flatten(),
            created_ms: r
                .take_opt::<Option<i64>, _>(13)
                .and_then(Result::ok)
                .flatten()
                .unwrap_or(0),
        })
    }
}

/// `ms` as `YYYY-MM-DD HH:MM:SS.mmm` (UTC), for a `DATETIME(3)`.
fn datetime(ms: i64) -> String {
    let day = ms.div_euclid(86_400_000);
    let rest = ms.rem_euclid(86_400_000);
    format!(
        "{} {:02}:{:02}:{:02}.{:03}",
        super::date(day * 86_400_000),
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}

#[async_trait]
impl DigestStore for CatalogDigests {
    async fn for_period(&self, cadence: Cadence, to_ms: i64) -> Result<Option<Digest>> {
        Ok(self
            .rows(
                self.select("WHERE cadence = ? AND period_end = ?"),
                (cadence.as_str(), datetime(to_ms)).into(),
            )
            .await?
            .into_iter()
            .next())
    }

    async fn last(&self, cadence: Cadence) -> Result<Option<Digest>> {
        Ok(self
            .rows(
                self.select("WHERE cadence = ? ORDER BY period_end DESC LIMIT 1"),
                (cadence.as_str(),).into(),
            )
            .await?
            .into_iter()
            .next())
    }

    async fn insert(&self, d: &NewDigest) -> Result<u64> {
        let mut c = self.conn().await?;
        let facts = serde_json::to_string(&d.facts)
            .map_err(|e| SprigError::internal(format!("a digest's facts: {e}")))?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (cadence, period_start, period_end, status, facts, facts_text, \
                 words, note, conv_id, model, token_usage, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3)) \
                 ON DUPLICATE KEY UPDATE digest_id = LAST_INSERT_ID(digest_id)",
                self.t()
            ),
            (
                d.facts.cadence.as_str(),
                datetime(d.facts.from_ms),
                datetime(d.facts.to_ms),
                d.status.as_str(),
                facts,
                &d.facts_text,
                &d.text,
                d.note
                    .as_deref()
                    .map(|n| n.chars().take(500).collect::<String>()),
                d.conv.map(|c| c.0),
                &d.model,
                d.usage.to_string(),
            ),
        )
        .await
        .map_err(unavailable)?;
        Ok(c.last_insert_id().unwrap_or(0))
    }

    async fn get(&self, id: u64) -> Result<Option<Digest>> {
        Ok(self
            .rows(self.select("WHERE digest_id = ?"), (id,).into())
            .await?
            .into_iter()
            .next())
    }

    async fn list(&self, limit: u32) -> Result<Vec<Digest>> {
        self.rows(
            self.select("ORDER BY period_end DESC, digest_id DESC LIMIT ?"),
            (limit,).into(),
        )
        .await
    }

    async fn emailed(&self, id: u64) -> Result<()> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "UPDATE {} SET emailed_at = UTC_TIMESTAMP(3) WHERE digest_id = ?",
                self.t()
            ),
            (id,),
        )
        .await
        .map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn datetimes_are_utc() {
        assert_eq!(super::datetime(0), "1970-01-01 00:00:00.000");
        assert_eq!(
            super::datetime(1_794_319_391_123),
            "2026-11-10 14:03:11.123"
        );
    }
}
