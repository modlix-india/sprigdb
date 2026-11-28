//! Digests as the engine's catalog records (module `ai_digests`), one per
//! cadence and period.
//!
//! ```text
//! m/ai_digests/d/<digest u64>                   DigestRec (JSON)
//! m/ai_digests/period/<cadence u8><to ms u64>   the digest's id (u64 BE): one per period
//! ctr/ai_digests.d/                             the id counter
//! ```
//!
//! The period record is read for update before a digest is written, so two
//! leaders racing for one period store one digest, as the table's unique key
//! made them.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, next_ids};
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::store::{Digest, DigestStore, NewDigest, Status, now_ms};
use super::{Cadence, Facts};

/// The module.
pub const MODULE: &str = "ai_digests";
const DIGESTS: RowTable = RowTable::new(MODULE, "d");
const PERIODS: RowTable = RowTable::new(MODULE, "period");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DigestRec {
    cadence: Cadence,
    from_ms: i64,
    to_ms: i64,
    worded: bool,
    facts: Facts,
    facts_text: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    conversation: Option<u64>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Value,
    #[serde(default)]
    emailed_ms: Option<i64>,
    created_ms: i64,
}

impl DigestRec {
    fn digest(self, id: u64) -> Digest {
        Digest {
            id,
            cadence: self.cadence,
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            status: if self.worded {
                Status::Worded
            } else {
                Status::Facts
            },
            facts: self.facts,
            facts_text: self.facts_text,
            text: self.text,
            note: self.note,
            conversation: self.conversation,
            model: self.model,
            usage: self.usage,
            emailed_ms: self.emailed_ms,
            created_ms: self.created_ms,
        }
    }
}

fn cadence_code(c: Cadence) -> u8 {
    match c {
        Cadence::Daily => 0,
        Cadence::Weekly => 1,
    }
}

fn period_pk(c: Cadence, to_ms: i64) -> Pk {
    Pk::new()
        .u8(cadence_code(c))
        .u64(u64::try_from(to_ms.max(0)).unwrap_or(0))
}

fn be_u64(b: &[u8]) -> Option<u64> {
    b.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

/// A tenant's digests in its catalog records.
pub struct RecDigests {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for RecDigests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecDigests")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl RecDigests {
    /// `tenant`'s digests.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    /// The newest digests by id, at most `n` (ids only move up and digests
    /// are never deleted, so the counter's last ids are the newest).
    async fn newest(&self, tx: &mut dyn RecordTx, n: usize) -> Result<Vec<Digest>> {
        let last = tx
            .get(&sprig_catalog::records::counter_key("ai_digests.d"))
            .await?
            .and_then(|v| be_u64(&v))
            .unwrap_or(0);
        let mut out = Vec::new();
        let mut id = last;
        while id > 0 && out.len() < n {
            if let Some(d) = DIGESTS.get::<DigestRec>(tx, &Pk::new().u64(id)).await? {
                out.push(d.digest(id));
            }
            id -= 1;
        }
        Ok(out)
    }
}

#[async_trait]
impl DigestStore for RecDigests {
    async fn for_period(&self, cadence: Cadence, to_ms: i64) -> Result<Option<Digest>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let Some(id) = tx
            .get(&PERIODS.key(&period_pk(cadence, to_ms)))
            .await?
            .and_then(|v| be_u64(&v))
        else {
            return Ok(None);
        };
        Ok(DIGESTS
            .get::<DigestRec>(&mut *tx, &Pk::new().u64(id))
            .await?
            .map(|d| d.digest(id)))
    }

    async fn last(&self, cadence: Cadence) -> Result<Option<Digest>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let under = Pk::new().u8(cadence_code(cadence));
        let Some(id) = last_id(PERIODS, &mut *tx, &under).await? else {
            return Ok(None);
        };
        Ok(DIGESTS
            .get::<DigestRec>(&mut *tx, &Pk::new().u64(id))
            .await?
            .map(|d| d.digest(id)))
    }

    async fn insert(&self, d: &NewDigest) -> Result<u64> {
        let mut tx = self.records.begin(self.tenant).await?;
        let pkey = PERIODS.key(&period_pk(d.facts.cadence, d.facts.to_ms));
        if let Some(id) = tx.get_for_update(&pkey).await?.and_then(|v| be_u64(&v)) {
            return Ok(id);
        }
        let id = next_ids(&mut *tx, "ai_digests.d", 1).await?;
        let rec = DigestRec {
            cadence: d.facts.cadence,
            from_ms: d.facts.from_ms,
            to_ms: d.facts.to_ms,
            worded: d.status == Status::Worded,
            facts: d.facts.clone(),
            facts_text: d.facts_text.clone(),
            text: d.text.clone(),
            note: d.note.clone(),
            conversation: d.conv.map(|c| c.0),
            model: d.model.clone(),
            usage: d.usage.clone(),
            emailed_ms: None,
            created_ms: now_ms(),
        };
        DIGESTS.put(&mut *tx, &Pk::new().u64(id), &rec).await?;
        tx.put(&pkey, &id.to_be_bytes()).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn get(&self, id: u64) -> Result<Option<Digest>> {
        let mut tx = self.records.begin(self.tenant).await?;
        Ok(DIGESTS
            .get::<DigestRec>(&mut *tx, &Pk::new().u64(id))
            .await?
            .map(|d| d.digest(id)))
    }

    async fn list(&self, limit: u32) -> Result<Vec<Digest>> {
        let mut tx = self.records.begin(self.tenant).await?;
        // Digests are stored as their periods end, so the newest ids are the
        // newest periods; a few more than asked cover a late one.
        let want = (limit as usize).saturating_mul(2).saturating_add(8);
        let mut v = self.newest(&mut *tx, want).await?;
        v.sort_by_key(|d| std::cmp::Reverse((d.to_ms, d.id)));
        v.truncate(limit as usize);
        Ok(v)
    }

    async fn emailed(&self, id: u64) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        let pk = Pk::new().u64(id);
        if let Some(mut d) = DIGESTS.get_for_update::<DigestRec>(&mut *tx, &pk).await? {
            d.emailed_ms = Some(now_ms());
            DIGESTS.put(&mut *tx, &pk, &d).await?;
        }
        tx.commit().await
    }
}

/// The value (an id) of the last record of `t` under `under`.
async fn last_id(t: RowTable, tx: &mut dyn RecordTx, under: &Pk) -> Result<Option<u64>> {
    let lo = t.key(under);
    let hi = sprig_catalog::records::prefix_end(&lo);
    Ok(sprig_catalog::records::scan_all(tx, &lo, &hi)
        .await?
        .pop()
        .and_then(|(_, v)| be_u64(&v)))
}

#[cfg(test)]
mod tests {
    use sprig_catalog::records::mem::MemRecords;

    use super::*;

    fn new(c: Cadence, to: i64) -> NewDigest {
        NewDigest {
            status: Status::Facts,
            facts: Facts::new(c, to - 86_400_000, to),
            facts_text: "facts".into(),
            text: None,
            note: Some("no model".into()),
            conv: None,
            model: None,
            usage: Value::Null,
        }
    }

    #[tokio::test]
    async fn one_digest_per_period_and_the_newest_first() {
        let store = RecDigests::new(Arc::new(MemRecords::new()), TenantId(5));
        let day = 86_400_000;
        let d10 = store.insert(&new(Cadence::Daily, 10 * day)).await.unwrap();
        assert_eq!(
            store.insert(&new(Cadence::Daily, 10 * day)).await.unwrap(),
            d10
        );
        let d11 = store.insert(&new(Cadence::Daily, 11 * day)).await.unwrap();
        let week = store.insert(&new(Cadence::Weekly, 7 * day)).await.unwrap();
        assert_ne!(d10, d11);
        assert_eq!(store.last(Cadence::Daily).await.unwrap().unwrap().id, d11);
        assert_eq!(store.last(Cadence::Weekly).await.unwrap().unwrap().id, week);
        assert_eq!(
            store
                .for_period(Cadence::Daily, 10 * day)
                .await
                .unwrap()
                .unwrap()
                .id,
            d10
        );
        assert!(
            store
                .for_period(Cadence::Weekly, 10 * day)
                .await
                .unwrap()
                .is_none()
        );
        let listed = store.list(10).await.unwrap();
        assert_eq!(
            listed.iter().map(|d| d.id).collect::<Vec<_>>(),
            vec![d11, d10, week]
        );
        store.emailed(d10).await.unwrap();
        assert!(store.get(d10).await.unwrap().unwrap().emailed_ms.is_some());
        assert_eq!(store.get(d10).await.unwrap().unwrap().status, Status::Facts);
    }
}
