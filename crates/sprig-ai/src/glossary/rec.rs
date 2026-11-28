//! The glossary as the engine's catalog records (module `ai_glossary`).
//!
//! ```text
//! m/ai_glossary/terms/<term key bytes><status u8>   Entry (JSON)
//! ```
//!
//! The term key is the term trimmed and in lower case, so `Revenue` and
//! `revenue` are one term, as the MariaDB table's case-insensitive unique
//! key made them; the entry keeps the term as written. Status 0 is
//! accepted, 1 proposed. Each call is one engine transaction.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sprig_catalog::records::{Pk, RecordStore, RowTable};
use sprig_common::Result;
use sprig_common::ids::{TenantId, UserId};

use super::{Entry, GlossaryStore, NewTerm, Status, entry, key};

/// The module.
pub const MODULE: &str = "ai_glossary";
/// Entries by term and status.
pub const TERMS: RowTable = RowTable::new(MODULE, "terms");

fn code(s: Status) -> u8 {
    match s {
        Status::Accepted => 0,
        Status::Proposed => 1,
    }
}

fn pk(term: &str, s: Status) -> Pk {
    Pk::new().bytes(key(term).as_bytes()).u8(code(s))
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// A tenant's glossary in its catalog records.
pub struct RecGlossary {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for RecGlossary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecGlossary")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl RecGlossary {
    /// `tenant`'s glossary.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }
}

#[async_trait]
impl GlossaryStore for RecGlossary {
    async fn list(&self, status: Option<Status>) -> Result<Vec<Entry>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let mut all: Vec<Entry> = TERMS.rows(&mut *tx, &Pk::new()).await?;
        all.retain(|e| status.is_none_or(|s| e.status == s));
        // By term, then status (accepted before proposed), as the table
        // listed them.
        all.sort_by(|a, b| {
            (key(&a.term), a.status.as_str()).cmp(&(key(&b.term), b.status.as_str()))
        });
        Ok(all)
    }

    async fn put(&self, t: &NewTerm, author: UserId, author_name: &str) -> Result<()> {
        t.check()?;
        let mut tx = self.records.begin(self.tenant).await?;
        let mut e = entry(t, Status::Accepted, author, author_name, None);
        e.updated_ms = now_ms();
        TERMS
            .put(&mut *tx, &pk(&t.term, Status::Accepted), &e)
            .await?;
        TERMS
            .delete(&mut *tx, &pk(&t.term, Status::Proposed))
            .await?;
        tx.commit().await
    }

    async fn propose(
        &self,
        t: &NewTerm,
        author: UserId,
        author_name: &str,
        proposal_id: Option<&str>,
    ) -> Result<()> {
        t.check()?;
        let mut tx = self.records.begin(self.tenant).await?;
        let mut e = entry(t, Status::Proposed, author, author_name, proposal_id);
        e.updated_ms = now_ms();
        TERMS
            .put(&mut *tx, &pk(&t.term, Status::Proposed), &e)
            .await?;
        tx.commit().await
    }

    async fn accept(&self, term: &str, by: UserId) -> Result<bool> {
        let mut tx = self.records.begin(self.tenant).await?;
        let Some(mut e) = TERMS
            .get_for_update::<Entry>(&mut *tx, &pk(term, Status::Proposed))
            .await?
        else {
            return Ok(false);
        };
        e.status = Status::Accepted;
        e.accepted_by = Some(by.0);
        e.updated_ms = now_ms();
        TERMS.delete(&mut *tx, &pk(term, Status::Proposed)).await?;
        TERMS.put(&mut *tx, &pk(term, Status::Accepted), &e).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn reject(&self, term: &str) -> Result<bool> {
        let mut tx = self.records.begin(self.tenant).await?;
        let p = pk(term, Status::Proposed);
        if TERMS.get_for_update::<Entry>(&mut *tx, &p).await?.is_none() {
            return Ok(false);
        }
        TERMS.delete(&mut *tx, &p).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn delete(&self, term: &str) -> Result<bool> {
        let mut tx = self.records.begin(self.tenant).await?;
        let mut had = false;
        for s in [Status::Accepted, Status::Proposed] {
            let p = pk(term, s);
            if TERMS.get_for_update::<Entry>(&mut *tx, &p).await?.is_some() {
                TERMS.delete(&mut *tx, &p).await?;
                had = true;
            }
        }
        tx.commit().await?;
        Ok(had)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sprig_catalog::records::mem::MemRecords;

    use super::*;

    fn term(t: &str, d: &str) -> NewTerm {
        NewTerm::from_json(&json!({"term": t, "definition": d})).unwrap()
    }

    #[tokio::test]
    async fn proposals_wait_and_accepting_replaces_the_entry() {
        let g = RecGlossary::new(Arc::new(MemRecords::new()), TenantId(3));
        g.put(&term("Revenue", "paid orders"), UserId(1), "ann")
            .await
            .unwrap();
        g.propose(
            &term("revenue", "paid minus refunds"),
            UserId(2),
            "bob",
            Some("p-9"),
        )
        .await
        .unwrap();
        let all = g.list(None).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].status, Status::Accepted);
        assert_eq!(all[1].proposal_id.as_deref(), Some("p-9"));
        assert_eq!(g.list(Some(Status::Proposed)).await.unwrap().len(), 1);
        assert!(g.accept("REVENUE", UserId(1)).await.unwrap());
        let all = g.list(None).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].definition, "paid minus refunds");
        assert_eq!(all[0].accepted_by, Some(1));
        assert!(!g.accept("revenue", UserId(1)).await.unwrap());
        assert!(!g.reject("revenue").await.unwrap());
        g.propose(&term("churn", "left in 30 days"), UserId(2), "bob", None)
            .await
            .unwrap();
        assert!(g.reject("Churn").await.unwrap());
        // An administrator's own entry drops a waiting proposal.
        g.propose(&term("revenue", "x"), UserId(2), "bob", None)
            .await
            .unwrap();
        g.put(&term("revenue", "y"), UserId(1), "ann")
            .await
            .unwrap();
        assert_eq!(g.list(Some(Status::Proposed)).await.unwrap().len(), 0);
        assert!(g.delete("revenue").await.unwrap());
        assert!(!g.delete("revenue").await.unwrap());
        assert!(g.list(None).await.unwrap().is_empty());
    }
}
