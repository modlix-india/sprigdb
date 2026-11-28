//! Where digests are kept: the [`DigestStore`] trait and its in-memory
//! store; the tenant's catalog records are [`super::rec::RecDigests`].

use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_common::Result;

use super::{Cadence, Facts};
use crate::audit::ConvId;

pub use super::rec::MODULE;

/// The module's MariaDB migrations: none, its rows are engine records
/// ([`super::rec`]).
pub static DIGEST_MIGRATIONS: &[sprig_catalog::migrate::Migration] = &[];

/// The module, for the catalog's readers.
pub const DIGEST_MODULE: sprig_catalog::migrate::Module = sprig_catalog::migrate::Module {
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

pub(super) fn now_ms() -> i64 {
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
