//! ID-block sequences as the engine's catalog records: what the catalog's
//! `sequences` and `sequence_blocks` tables held, as unversioned counters
//! (their own short engine transactions, outside catalog versions, as the
//! tables were written).
//!
//! ```text
//! m/sharding/sequences/<seq u64>            SeqRow
//! m/sharding/blocks/<seq u64><start 16>     BlockRow (a held block, under a lease)
//! ```
//!
//! Each fetch, raise and explicit-value check reads the sequence's row for
//! update (the engine's exclusive record lock, as `SELECT ... FOR UPDATE`
//! took MariaDB's row lock), so they are ordered per sequence exactly as
//! before; a held block's row is written in the same transaction as the
//! counter it was taken from.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use super::{BLOCK_LEASE, Explicit, Ordering, SeqId, SequenceSource, exhausted};

/// Sequences by id.
pub const SEQUENCES: RowTable = RowTable::new("sharding", "sequences");
/// Held blocks by sequence and first value.
pub const BLOCKS: RowTable = RowTable::new("sharding", "blocks");

/// A sequence: what `sequences` held. Values are decimal text (they range
/// up to 2^64).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeqRow {
    /// The object it numbers.
    pub object_id: u64,
    /// Its column (`AUTO_INCREMENT`); none for a sequence.
    pub column_name: Option<String>,
    /// `auto_increment` or `sequence`.
    pub kind: String,
    /// `blocks` or `strict`.
    pub ordering: String,
    /// The next value no block holds.
    pub next_value: String,
    /// The step.
    pub increment_by: String,
    /// The smallest value.
    pub min_value: String,
    /// The largest value.
    pub max_value: String,
    /// Starts again after the largest.
    pub cycle: bool,
    /// Ms since the epoch.
    pub updated_ms: u64,
}

/// A block a gateway holds: what `sequence_blocks` held besides its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRow {
    /// The first value after it.
    pub end_value: String,
    /// Who holds it.
    pub holder: String,
    /// Ms since the epoch.
    pub lease_until_ms: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// An order-preserving key part for a signed 128-bit value.
fn v128(v: i128) -> [u8; 16] {
    #[allow(clippy::cast_sign_loss)]
    let u = (v as u128) ^ (1u128 << 127);
    u.to_be_bytes()
}

fn parse(s: &str) -> Result<i128> {
    s.parse()
        .map_err(|_| SprigError::internal(format!("bad sequence value {s}")))
}

fn missing(seq: SeqId) -> SprigError {
    SprigError::new(
        SprigCode::IdBlockUnavailable,
        format!("sequence {} doesn't exist", seq.0),
    )
}

/// The record store unreachable: 39460, as a catalog connection failure was.
fn unavailable(e: SprigError) -> SprigError {
    match e.code() {
        Some(SprigCode::CatalogUnavailable) => SprigError::new(
            SprigCode::IdBlockUnavailable,
            format!("the catalog can't hand out an ID block: {}", e.message),
        ),
        _ => e,
    }
}

/// A tenant's sequences in its catalog records, as a [`SequenceSource`].
#[derive(Clone)]
pub struct RecordSequences {
    store: Arc<dyn RecordStore>,
    tenant: TenantId,
    /// Who holds the blocks this source fetches (a gateway's coordinator
    /// id); empty: blocks aren't recorded and never expire.
    holder: String,
    lease: Duration,
}

impl std::fmt::Debug for RecordSequences {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordSequences")
            .field("tenant", &self.tenant)
            .field("holder", &self.holder)
            .finish_non_exhaustive()
    }
}

impl RecordSequences {
    /// `tenant`'s sequences in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self {
            store,
            tenant,
            holder: String::new(),
            lease: BLOCK_LEASE,
        }
    }

    /// Records each fetched block as held by `holder` for `lease`, and
    /// checks explicit values against other holders' blocks.
    #[must_use]
    pub fn held_by(mut self, holder: impl Into<String>, lease: Duration) -> Self {
        self.holder = holder.into();
        self.lease = lease;
        self
    }

    async fn begin(&self) -> Result<Box<dyn RecordTx>> {
        self.store.begin(self.tenant).await.map_err(unavailable)
    }

    /// Creates a sequence (at the first split: `next = MAX(col) + 1`).
    ///
    /// # Errors
    /// A backend 1062 when it exists; 39460 when the catalog can't be
    /// reached.
    pub async fn create(
        &self,
        seq: SeqId,
        object: u64,
        column: Option<&str>,
        next: i128,
        ordering: Ordering,
    ) -> Result<()> {
        let pk = Pk::new().u64(seq.0);
        let mut tx = self.begin().await?;
        if SEQUENCES
            .get_for_update::<SeqRow>(&mut *tx, &pk)
            .await
            .map_err(unavailable)?
            .is_some()
        {
            return Err(SprigError::backend(
                1062,
                *b"23000",
                format!("Duplicate entry '{}' for key 'PRIMARY'", seq.0),
            ));
        }
        let row = SeqRow {
            object_id: object,
            column_name: column.map(str::to_owned),
            kind: if column.is_some() {
                "auto_increment"
            } else {
                "sequence"
            }
            .to_owned(),
            ordering: match ordering {
                Ordering::Blocks => "blocks",
                Ordering::Strict => "strict",
            }
            .to_owned(),
            next_value: next.to_string(),
            increment_by: "1".to_owned(),
            min_value: "1".to_owned(),
            max_value: u64::MAX.to_string(),
            cycle: false,
            updated_ms: now_ms(),
        };
        SEQUENCES
            .put(&mut *tx, &pk, &row)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }

    /// Whether sequence `seq` exists.
    ///
    /// # Errors
    /// 39460 when the catalog can't be reached.
    pub async fn exists(&self, seq: SeqId) -> Result<bool> {
        let mut tx = self.begin().await?;
        Ok(SEQUENCES
            .get::<SeqRow>(&mut *tx, &Pk::new().u64(seq.0))
            .await
            .map_err(unavailable)?
            .is_some())
    }

    async fn locked(&self, tx: &mut dyn RecordTx, seq: SeqId) -> Result<SeqRow> {
        SEQUENCES
            .get_for_update::<SeqRow>(tx, &Pk::new().u64(seq.0))
            .await
            .map_err(unavailable)?
            .ok_or_else(|| missing(seq))
    }

    async fn fetch_once(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let mut tx = self.begin().await?;
        let mut row = self.locked(&mut *tx, seq).await?;
        let next = parse(&row.next_value)?;
        let max = parse(&row.max_value)?;
        if next > max {
            return Err(exhausted());
        }
        let size = i128::try_from(size).unwrap_or(i128::MAX);
        let end = next.saturating_add(size).min(max + 1);
        row.next_value = end.to_string();
        row.updated_ms = now_ms();
        SEQUENCES
            .put(&mut *tx, &Pk::new().u64(seq.0), &row)
            .await
            .map_err(unavailable)?;
        if !self.holder.is_empty() {
            // The block, under its lease; blocks whose lease ended can't
            // hand out values any more and go.
            let now = now_ms();
            for (pk, b) in BLOCKS
                .scan::<BlockRow>(&mut *tx, &Pk::new().u64(seq.0))
                .await
                .map_err(unavailable)?
            {
                if b.lease_until_ms <= now {
                    BLOCKS.delete(&mut *tx, &pk).await.map_err(unavailable)?;
                }
            }
            let lease = u64::try_from(self.lease.as_millis()).unwrap_or(u64::MAX);
            let b = BlockRow {
                end_value: end.to_string(),
                holder: self.holder.clone(),
                lease_until_ms: now.saturating_add(lease),
            };
            BLOCKS
                .put(&mut *tx, &Pk::new().u64(seq.0).raw(&v128(next)), &b)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok((next, end))
    }

    async fn explicit_once(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        let mut tx = self.begin().await?;
        // The row lock orders this with every fetch of the sequence.
        let mut row = self.locked(&mut *tx, seq).await?;
        let next = parse(&row.next_value)?;
        let out = if v >= next {
            // Every block handed out so far is below `next`: none holds v.
            row.next_value = (v + 1).to_string();
            row.updated_ms = now_ms();
            SEQUENCES
                .put(&mut *tx, &Pk::new().u64(seq.0), &row)
                .await
                .map_err(unavailable)?;
            Explicit::Free
        } else {
            let now = now_ms();
            let mut held = None;
            for (pk, b) in BLOCKS
                .scan::<BlockRow>(&mut *tx, &Pk::new().u64(seq.0))
                .await
                .map_err(unavailable)?
            {
                let start_key = pk.0.get(8..).unwrap_or_default();
                if start_key > v128(v).as_slice() {
                    break;
                }
                let end = parse(&b.end_value)?;
                if end > v && b.holder != self.holder && b.lease_until_ms > now {
                    // The last such block in key order: the highest start.
                    held = Some(end);
                }
            }
            match held {
                Some(end) => Explicit::Reserved { end },
                None => Explicit::Free,
            }
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(out)
    }
}

#[async_trait::async_trait]
impl SequenceSource for RecordSequences {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        super::retry(|| self.fetch_once(seq, size)).await
    }

    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        super::retry(|| async {
            let mut tx = self.begin().await?;
            let mut row = self.locked(&mut *tx, seq).await?;
            if parse(&row.next_value)? < v + 1 {
                row.next_value = (v + 1).to_string();
                row.updated_ms = now_ms();
                SEQUENCES
                    .put(&mut *tx, &Pk::new().u64(seq.0), &row)
                    .await
                    .map_err(unavailable)?;
            }
            tx.commit().await.map_err(unavailable)
        })
        .await
    }

    fn lease(&self) -> Option<Duration> {
        (!self.holder.is_empty()).then_some(self.lease)
    }

    async fn explicit(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        super::retry(|| self.explicit_once(seq, v)).await
    }

    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        let mut tx = self.begin().await?;
        let row = SEQUENCES
            .get::<SeqRow>(&mut *tx, &Pk::new().u64(seq.0))
            .await
            .map_err(unavailable)?
            .ok_or_else(|| missing(seq))?;
        Ok(if row.ordering == "strict" {
            Ordering::Strict
        } else {
            Ordering::Blocks
        })
    }
}

#[cfg(test)]
mod tests {
    use sprig_catalog::records::mem::MemRecords;

    use super::*;
    use crate::ids::{AutoIncSession, BlockAllocator, BlockConfig, IdAllocator};

    const T: TenantId = TenantId(57_406_000_004);

    fn src(holder: &str) -> RecordSequences {
        let store: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
        RecordSequences::new(store, T).held_by(holder, Duration::from_secs(60))
    }

    #[tokio::test]
    async fn blocks_never_overlap_and_explicit_values_follow_the_rules() {
        let a = src("gw-a");
        let b = RecordSequences {
            holder: "gw-b".into(),
            ..a.clone()
        };
        assert!(!a.exists(SeqId(7)).await.unwrap());
        assert_eq!(
            a.fetch(SeqId(7), 10).await.unwrap_err().code(),
            Some(SprigCode::IdBlockUnavailable)
        );
        a.create(SeqId(7), 70, Some("id"), 100, Ordering::Blocks)
            .await
            .unwrap();
        let dup = a
            .create(SeqId(7), 70, Some("id"), 1, Ordering::Blocks)
            .await
            .unwrap_err();
        assert!(matches!(
            dup.kind,
            sprig_common::ErrorKind::Backend { errno: 1062, .. }
        ));
        assert!(a.exists(SeqId(7)).await.unwrap());
        assert_eq!(a.ordering(SeqId(7)).await.unwrap(), Ordering::Blocks);
        assert_eq!(a.fetch(SeqId(7), 10).await.unwrap(), (100, 110));
        assert_eq!(b.fetch(SeqId(7), 5).await.unwrap(), (110, 115));
        // A value in another holder's live block is reserved; in one's own
        // block it is free; past every block it raises the counter.
        assert_eq!(
            a.explicit(SeqId(7), 112).await.unwrap(),
            Explicit::Reserved { end: 115 }
        );
        assert_eq!(b.explicit(SeqId(7), 112).await.unwrap(), Explicit::Free);
        assert_eq!(a.explicit(SeqId(7), 500).await.unwrap(), Explicit::Free);
        assert_eq!(a.fetch(SeqId(7), 1).await.unwrap(), (501, 502));
        a.raise(SeqId(7), 999).await.unwrap();
        a.raise(SeqId(7), 10).await.unwrap();
        assert_eq!(b.fetch(SeqId(7), 2).await.unwrap(), (1000, 1002));
        // A block whose lease ended reserves nothing and goes at the next
        // fetch.
        let c = RecordSequences {
            holder: "gw-c".into(),
            lease: Duration::from_millis(0),
            ..a.clone()
        };
        let (s, _) = c.fetch(SeqId(7), 3).await.unwrap();
        assert_eq!(a.explicit(SeqId(7), s).await.unwrap(), Explicit::Free);
        a.fetch(SeqId(7), 1).await.unwrap();
        let mut tx = a.store.begin(T).await.unwrap();
        let holders: Vec<String> = BLOCKS
            .rows::<BlockRow>(&mut *tx, &Pk::new().u64(7))
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.holder)
            .collect();
        assert!(!holders.contains(&"gw-c".to_owned()));
        // The sequence ends at its largest value.
        let mut row = SEQUENCES
            .get::<SeqRow>(&mut *tx, &Pk::new().u64(7))
            .await
            .unwrap()
            .unwrap();
        row.next_value = u64::MAX.to_string();
        SEQUENCES
            .put(&mut *tx, &Pk::new().u64(7), &row)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            a.fetch(SeqId(7), 10).await.unwrap(),
            (i128::from(u64::MAX), i128::from(u64::MAX) + 1)
        );
        assert_eq!(
            a.fetch(SeqId(7), 10).await.unwrap_err().code(),
            exhausted().code()
        );
    }

    #[tokio::test]
    async fn an_allocator_over_records_hands_out_unique_values() {
        let s = src("gw-a");
        s.create(SeqId(1), 10, Some("id"), 1, Ordering::Blocks)
            .await
            .unwrap();
        let x = BlockAllocator::new(s.clone(), BlockConfig::default());
        let y = BlockAllocator::new(
            RecordSequences {
                holder: "gw-b".into(),
                ..s
            },
            BlockConfig::default(),
        );
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..200u64 {
            let a = if i % 2 == 0 { &x } else { &y };
            let run = a
                .take(SeqId(1), 3, &AutoIncSession::default())
                .await
                .unwrap();
            for v in run.values() {
                assert!(seen.insert(v), "{v} handed out twice");
            }
        }
        assert_eq!(seen.len(), 600);
    }
}
