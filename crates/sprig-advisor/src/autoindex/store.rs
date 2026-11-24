//! The loop's records in the tenant catalog (`autoindex` module):
//!
//! ```text
//! m/autoindex/idx/<name>                 IndexRecord (JSON)
//! m/autoindex/events/<µs u64><seq u32>   Event (JSON)
//! m/autoindex/cool/<fingerprint u64>     when the pattern was last decided (µs, 8 bytes)
//! ```
//!
//! Each loop write is a short engine transaction of its own and never
//! bumps a catalog version, as the workload records' are. The index
//! definitions themselves change only through catalog changes (the
//! table's definition holds the index); these records only say why and
//! what happened. Events older than [`EVENT_RETENTION_DAYS`] are purged.

use sprig_catalog::records::{Pk, RecordTx, RowTable, prefix_end, scan_all};
use sprig_common::{Result, SprigError};

use super::model::{Event, IndexRecord};
use super::policy::DAY_US;

/// The module name.
pub const MODULE: &str = "autoindex";
/// Index records by name.
pub const IDX: RowTable = RowTable::new(MODULE, "idx");
/// Events by time.
pub const EVENTS: RowTable = RowTable::new(MODULE, "events");
/// Last decision per pattern.
pub const COOL: RowTable = RowTable::new(MODULE, "cool");
/// Days events are kept.
pub const EVENT_RETENTION_DAYS: u64 = 30;

fn bad(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("autoindex record {what} doesn't decode: {e}"))
}

/// Reads and writes the loop's records.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoIndexStore;

impl AutoIndexStore {
    /// Every index record, by name.
    ///
    /// # Errors
    /// The record store's; `Internal` for a record that doesn't decode.
    pub async fn indexes(rec: &mut dyn RecordTx) -> Result<Vec<IndexRecord>> {
        let lo = IDX.prefix();
        let mut out = Vec::new();
        for (_, v) in scan_all(rec, &lo, &prefix_end(&lo)).await? {
            out.push(serde_json::from_slice(&v).map_err(|e| bad("idx", e))?);
        }
        Ok(out)
    }

    /// The record of index `name`.
    ///
    /// # Errors
    /// As [`AutoIndexStore::indexes`].
    pub async fn index(rec: &mut dyn RecordTx, name: &str) -> Result<Option<IndexRecord>> {
        let key = IDX.key(&Pk::new().bytes(name.to_lowercase().as_bytes()));
        match rec.get(&key).await? {
            Some(v) => Ok(Some(serde_json::from_slice(&v).map_err(|e| bad("idx", e))?)),
            None => Ok(None),
        }
    }

    /// Writes an index record.
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_index(rec: &mut dyn RecordTx, r: &IndexRecord) -> Result<()> {
        let key = IDX.key(&Pk::new().bytes(r.name.to_lowercase().as_bytes()));
        let v = serde_json::to_vec(r).map_err(|e| SprigError::internal(e.to_string()))?;
        rec.put(&key, &v).await
    }

    /// Appends an event (`seq` tells apart events of the same µs).
    ///
    /// # Errors
    /// The record store's.
    pub async fn add_event(rec: &mut dyn RecordTx, e: &Event, seq: u32) -> Result<()> {
        let key = EVENTS.key(&Pk::new().u64(e.at_us).u32(seq));
        let v = serde_json::to_vec(e).map_err(|e| SprigError::internal(e.to_string()))?;
        rec.put(&key, &v).await
    }

    /// Events at or after `since_us`, oldest first, at most `limit`
    /// (the newest ones when there are more).
    ///
    /// # Errors
    /// As [`AutoIndexStore::indexes`].
    pub async fn events(rec: &mut dyn RecordTx, since_us: u64, limit: usize) -> Result<Vec<Event>> {
        let lo = EVENTS.key(&Pk::new().u64(since_us));
        let hi = prefix_end(&EVENTS.prefix());
        let mut out: Vec<Event> = Vec::new();
        for (_, v) in scan_all(rec, &lo, &hi).await? {
            out.push(serde_json::from_slice(&v).map_err(|e| bad("event", e))?);
        }
        if out.len() > limit {
            out.drain(..out.len() - limit);
        }
        Ok(out)
    }

    /// Deletes events older than the retention.
    ///
    /// # Errors
    /// The record store's.
    pub async fn purge_events(rec: &mut dyn RecordTx, now_us: u64) -> Result<usize> {
        let cut = now_us.saturating_sub(EVENT_RETENTION_DAYS * DAY_US);
        let lo = EVENTS.prefix();
        let hi = EVENTS.key(&Pk::new().u64(cut));
        let old = scan_all(rec, &lo, &hi).await?;
        for (k, _) in &old {
            rec.delete(k).await?;
        }
        Ok(old.len())
    }

    /// When pattern `fp` was last decided.
    ///
    /// # Errors
    /// The record store's.
    pub async fn cooldown(rec: &mut dyn RecordTx, fp: u64) -> Result<Option<u64>> {
        let key = COOL.key(&Pk::new().u64(fp));
        Ok(rec
            .get(&key)
            .await?
            .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
            .map(u64::from_be_bytes))
    }

    /// Notes that pattern `fp` was decided at `at_us`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_cooldown(rec: &mut dyn RecordTx, fp: u64, at_us: u64) -> Result<()> {
        let key = COOL.key(&Pk::new().u64(fp));
        rec.put(&key, &at_us.to_be_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_catalog::records::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    use super::*;
    use crate::autoindex::model::{IndexSpec, KeyCol, State};

    fn event(at: u64, kind: &str) -> Event {
        Event {
            at_us: at,
            kind: kind.into(),
            index: "sprig_auto_00000000".into(),
            def: "secondary(a)".into(),
            table: "d.t".into(),
            fingerprints: vec![1],
            reason: "test".into(),
            est: None,
        }
    }

    #[tokio::test]
    async fn records_round_trip() {
        let store = MemRecords::new();
        let t = TenantId(5);
        let spec = IndexSpec {
            db: "d".into(),
            table: "t".into(),
            cols: vec![KeyCol::asc("a")],
        };
        let mut r = IndexRecord::new(spec.clone(), State::Building, "slow_p95", 1);
        let mut tx = store.begin(t).await.unwrap();
        AutoIndexStore::put_index(&mut *tx, &r).await.unwrap();
        r.state = State::Active;
        AutoIndexStore::put_index(&mut *tx, &r).await.unwrap();
        AutoIndexStore::add_event(&mut *tx, &event(10, "built"), 0)
            .await
            .unwrap();
        AutoIndexStore::add_event(&mut *tx, &event(10, "observed"), 1)
            .await
            .unwrap();
        AutoIndexStore::add_event(&mut *tx, &event(40 * DAY_US, "dropped"), 0)
            .await
            .unwrap();
        AutoIndexStore::set_cooldown(&mut *tx, 9, 1234)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut tx = store.begin(t).await.unwrap();
        let all = AutoIndexStore::indexes(&mut *tx).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].state, State::Active);
        let one = AutoIndexStore::index(&mut *tx, &spec.name().to_uppercase())
            .await
            .unwrap();
        assert_eq!(one.unwrap().name, spec.name());
        let ev = AutoIndexStore::events(&mut *tx, 0, 10).await.unwrap();
        let kinds: Vec<&str> = ev.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(kinds, vec!["built", "observed", "dropped"]);
        assert_eq!(
            AutoIndexStore::events(&mut *tx, 0, 1).await.unwrap()[0].kind,
            "dropped"
        );
        assert_eq!(
            AutoIndexStore::cooldown(&mut *tx, 9).await.unwrap(),
            Some(1234)
        );
        assert_eq!(AutoIndexStore::cooldown(&mut *tx, 8).await.unwrap(), None);
        assert_eq!(
            AutoIndexStore::purge_events(&mut *tx, 40 * DAY_US)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            AutoIndexStore::events(&mut *tx, 0, 10).await.unwrap().len(),
            1
        );
    }
}
