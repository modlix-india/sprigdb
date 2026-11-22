//! Module rows on the catalog version, while the catalog version row and
//! the core tables are still in the tenant's MariaDB catalog schema.
//!
//! A module whose rows moved to engine records but whose changes are
//! catalog-versioned changes (types, streams, queues, sinks, inheritance)
//! must commit its rows together with the catalog version and the core
//! rows the same change writes, which live in another store. The bridge:
//!
//! - **The change writes its records into a [`StagedTx`]**: reads see the
//!   engine's records with the change's own writes merged in; writes stay
//!   in memory. Every writer of these records holds the catalog version
//!   row, so no two changes write them at once.
//! - **At commit, the writes go into the MariaDB transaction** as one row
//!   of the catalog's record log (`rec_log`: a sequence number, the
//!   version, the writes): MariaDB's commit is the one commit point of the
//!   core rows, the version and the module rows together.
//! - **Then the engine applies the log row** ([`apply_log`]): its puts and
//!   deletes, and the record `a/catalog` = the last sequence number
//!   applied, in one engine transaction. Applying is idempotent and goes
//!   in sequence order; a crash before it leaves the row in the log, and
//!   the next change catches the engine up before it reads anything.
//! - **Snapshot loads overlay what the engine hasn't applied yet**: a load
//!   opens its engine read view first, then the MariaDB snapshot, and
//!   merges the log rows after the view's `a/catalog` into the records it
//!   reads ([`StagedTx::replay`]), so it sees exactly the module rows of
//!   the catalog version it loads.
//! - **Log rows are purged** once applied and a minute old; the purge
//!   records the last sequence it removed (`sprig.rec_log_purged`), and a
//!   load whose engine view is older than that loads again.

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};

use super::{RecordStore, RecordTx, bad_record, be_u64};

/// The engine record holding the last log sequence number applied.
pub const APPLIED_KEY: &[u8] = b"a/catalog";

/// Rows asked of the base store per scan call.
const PAGE: usize = 1024;

/// One change's writes: each key's final value (`None`: deleted), in key
/// order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ops(pub BTreeMap<Vec<u8>, Option<Vec<u8>>>);

impl Ops {
    /// Whether the change wrote nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The log row's bytes: per write, the key's length (u32 BE), the key,
    /// a flag (1 put, 0 delete) and for a put the value's length (u32 BE)
    /// and the value.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (k, v) in &self.0 {
            out.extend_from_slice(&len32(k.len()).to_be_bytes());
            out.extend_from_slice(k);
            match v {
                Some(v) => {
                    out.push(1);
                    out.extend_from_slice(&len32(v.len()).to_be_bytes());
                    out.extend_from_slice(v);
                }
                None => out.push(0),
            }
        }
        out
    }

    /// Reads [`encode`](Self::encode)'s bytes back.
    ///
    /// # Errors
    /// `Internal` for bytes that don't decode.
    pub fn decode(mut b: &[u8]) -> Result<Self> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            if b.len() < n {
                return Err(bad_record("log row"));
            }
            let (h, t) = b.split_at(n);
            *b = t;
            Ok(h)
        }
        fn take_len(b: &mut &[u8]) -> Result<usize> {
            let raw = take(b, 4)?;
            let n = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
            usize::try_from(n).map_err(|_| bad_record("log row"))
        }
        let mut ops = BTreeMap::new();
        while !b.is_empty() {
            let kl = take_len(&mut b)?;
            let k = take(&mut b, kl)?.to_vec();
            let flag = take(&mut b, 1)?[0];
            let v = match flag {
                1 => {
                    let vl = take_len(&mut b)?;
                    Some(take(&mut b, vl)?.to_vec())
                }
                0 => None,
                _ => return Err(bad_record("log row")),
            };
            ops.insert(k, v);
        }
        Ok(Self(ops))
    }
}

fn len32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// A record transaction whose writes stay in memory, over a base engine
/// transaction: the change's view of module rows on the catalog version,
/// and a snapshot's view with the log rows the engine hasn't applied.
pub struct StagedTx {
    base: Box<dyn RecordTx>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl std::fmt::Debug for StagedTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedTx")
            .field("writes", &self.writes.len())
            .finish_non_exhaustive()
    }
}

impl StagedTx {
    /// Writes staged over `base`.
    #[must_use]
    pub fn new(base: Box<dyn RecordTx>) -> Self {
        Self {
            base,
            writes: BTreeMap::new(),
        }
    }

    /// Merges a log row's writes (they then read as if applied).
    pub fn replay(&mut self, ops: Ops) {
        self.writes.extend(ops.0);
    }

    /// The writes staged so far, taken out (the transaction then reads
    /// the base alone).
    pub fn take_ops(&mut self) -> Ops {
        Ops(std::mem::take(&mut self.writes))
    }

    /// Whether anything is staged.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        !self.writes.is_empty()
    }

    /// Ends the base transaction.
    pub async fn finish(self) {
        let _ = self.base.rollback().await;
    }
}

#[async_trait::async_trait]
impl RecordTx for StagedTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.writes.get(key) {
            Some(v) => Ok(v.clone()),
            None => self.base.get(key).await,
        }
    }

    // Every writer of staged records holds the catalog version row, so
    // a plain read is already the latest value nobody else can change.
    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }

    async fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mine: Vec<(&Vec<u8>, &Option<Vec<u8>>)> =
            self.writes.range(lo.to_vec()..hi.to_vec()).collect();
        // Each staged delete hides at most one base row: read that many
        // more, so the merged first `limit` rows are complete.
        let deletes = mine.iter().filter(|(_, v)| v.is_none()).count();
        let want = limit.saturating_add(deletes);
        // In pages, until `want` rows or an empty answer: a store may cap
        // one scan below what was asked (the peer service does), so a
        // short answer isn't the end of the range.
        let mut base: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut from = lo.to_vec();
        while base.len() < want {
            let page = want.saturating_sub(base.len()).min(PAGE);
            // Only rows at or after the page's start count: a store that
            // answers a page with rows before it (seen with the engine's
            // range scan from a key between two rows) would otherwise
            // fill `base` with repeats, mark it full, and hide staged rows
            // past its last key.
            let got: Vec<(Vec<u8>, Vec<u8>)> = self
                .base
                .scan(&from, hi, page)
                .await?
                .into_iter()
                .filter(|(k, _)| k.as_slice() >= from.as_slice())
                .collect();
            let Some((k, _)) = got.last() else {
                break;
            };
            from.clone_from(k);
            from.push(0);
            base.extend(got);
        }
        let base_full = base.len() >= want;
        let last_base = base.last().map(|(k, _)| k.clone());
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = base.into_iter().collect();
        for (k, v) in mine {
            // A full batch already holds at least `limit` merged rows up
            // to its last key; staged rows past it come in a later call.
            if base_full && last_base.as_ref().is_some_and(|l| k > l) {
                continue;
            }
            match v {
                Some(v) => {
                    merged.insert(k.clone(), v.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        Err(SprigError::internal(
            "staged catalog records commit with their catalog change",
        ))
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        self.base.rollback().await
    }
}

/// The last log sequence number the engine applied, at `tx`'s read view.
///
/// # Errors
/// The store's; `Internal` for a record that doesn't decode.
pub async fn applied(tx: &mut dyn RecordTx) -> Result<u64> {
    match tx.get(APPLIED_KEY).await? {
        Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("applied mark")),
        None => Ok(0),
    }
}

/// Applies log rows `entries` (sequence number, writes; in sequence
/// order) to `tenant`'s engine records in one transaction: the rows after
/// the engine's mark, then the mark. With `only_after = Some(s)` nothing is
/// applied unless the engine's mark is exactly `s` (the writer's own row,
/// applied right after its commit, must not jump over rows it didn't
/// see). Returns the engine's mark afterwards.
///
/// # Errors
/// The store's (the engine keeps its previous mark).
pub async fn apply_log(
    store: &dyn RecordStore,
    tenant: TenantId,
    entries: &[(u64, Ops)],
    only_after: Option<u64>,
) -> Result<u64> {
    let mut tx = store.begin(tenant).await?;
    let mark = match tx.get_for_update(APPLIED_KEY).await? {
        Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("applied mark"))?,
        None => 0,
    };
    if only_after.is_some_and(|s| s != mark) {
        let _ = tx.rollback().await;
        return Ok(mark);
    }
    if let Some((seq, _)) = entries
        .iter()
        .find(|(seq, _)| *seq <= mark)
        .filter(|_| only_after.is_some())
    {
        // A writer's own row at or below the mark: its sequence number was
        // used before (the log lost rows the engine had applied).
        let _ = tx.rollback().await;
        return Err(super::unavailable(format!(
            "catalog record log row {seq} is at or below the engine's applied mark {mark}"
        )));
    }
    let mut last = mark;
    for (seq, ops) in entries {
        if *seq <= last {
            continue;
        }
        for (k, v) in &ops.0 {
            match v {
                Some(v) => tx.put(k, v).await?,
                None => tx.delete(k).await?,
            }
        }
        last = *seq;
    }
    if last == mark {
        let _ = tx.rollback().await;
        return Ok(mark);
    }
    tx.put(APPLIED_KEY, &last.to_be_bytes()).await?;
    tx.commit().await?;
    Ok(last)
}

/// Where a change's staged records come from: the engine store and the
/// tenant, with the log rows seen when the change caught the engine up.
pub struct Staging {
    /// The engine store.
    pub store: Arc<dyn RecordStore>,
    /// The tenant.
    pub tenant: TenantId,
    /// The change's records.
    pub tx: StagedTx,
    /// The last log sequence number the engine had applied (or was
    /// caught up to) when the change began reading.
    pub seen: u64,
}

impl std::fmt::Debug for Staging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Staging")
            .field("tenant", &self.tenant)
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::mem::MemRecords;
    use crate::records::scan_all;

    const T: TenantId = TenantId(7);

    #[test]
    fn ops_round_trip() {
        let mut ops = Ops::default();
        ops.0.insert(b"m/x/a".to_vec(), Some(b"1".to_vec()));
        ops.0.insert(b"m/x/b".to_vec(), None);
        ops.0.insert(vec![0, 255, 0], Some(Vec::new()));
        assert_eq!(Ops::decode(&ops.encode()).unwrap(), ops);
        assert_eq!(Ops::decode(&[]).unwrap(), Ops::default());
        assert!(Ops::decode(&[0, 0, 0, 5, 1]).is_err());
    }

    async fn seed(store: &MemRecords, rows: &[(&str, &str)]) {
        let mut tx = store.begin(T).await.unwrap();
        for (k, v) in rows {
            tx.put(k.as_bytes(), v.as_bytes()).await.unwrap();
        }
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn staged_reads_merge_writes_and_scans_page_correctly() {
        let store = MemRecords::new();
        let rows: Vec<(String, String)> = (0..20)
            .map(|i| (format!("m/t/{i:02}"), format!("v{i}")))
            .collect();
        let refs: Vec<(&str, &str)> = rows.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        seed(&store, &refs).await;
        let mut s = StagedTx::new(store.begin(T).await.unwrap());
        // Deletes at the front, puts in the middle and past the end.
        for i in [0, 1, 2, 3] {
            s.delete(format!("m/t/{i:02}").as_bytes()).await.unwrap();
        }
        s.put(b"m/t/05a", b"new").await.unwrap();
        s.put(b"m/t/99", b"tail").await.unwrap();
        s.put(b"m/t/07", b"changed").await.unwrap();
        assert_eq!(s.get(b"m/t/00").await.unwrap(), None);
        assert_eq!(s.get(b"m/t/07").await.unwrap(), Some(b"changed".to_vec()));
        assert_eq!(s.get(b"m/t/08").await.unwrap(), Some(b"v8".to_vec()));
        // Small pages walk the same rows as one big read.
        let mut paged = Vec::new();
        let mut from = b"m/t/".to_vec();
        loop {
            let batch = s.scan(&from, b"m/t0", 3).await.unwrap();
            let done = batch.len() < 3;
            if let Some((k, _)) = batch.last() {
                from = k.clone();
                from.push(0);
            }
            paged.extend(batch);
            if done {
                break;
            }
        }
        let all = scan_all(&mut s, b"m/t/", b"m/t0").await.unwrap();
        assert_eq!(paged, all);
        let keys: Vec<String> = all
            .iter()
            .map(|(k, _)| String::from_utf8(k.clone()).unwrap())
            .collect();
        assert_eq!(keys.len(), 16 + 2);
        assert_eq!(keys[0], "m/t/04");
        assert_eq!(keys[1], "m/t/05");
        assert_eq!(keys[2], "m/t/05a");
        assert_eq!(keys.last().unwrap(), "m/t/99");
        // Nothing reached the store.
        assert_eq!(store.dump(T).len(), 20);
        let ops = s.take_ops();
        assert_eq!(ops.0.len(), 7);
        assert!(!s.is_dirty());
    }

    #[tokio::test]
    async fn apply_is_ordered_idempotent_and_guarded() {
        let store = MemRecords::new();
        let mut a = Ops::default();
        a.0.insert(b"m/x/1".to_vec(), Some(b"a".to_vec()));
        let mut b = Ops::default();
        b.0.insert(b"m/x/1".to_vec(), Some(b"b".to_vec()));
        b.0.insert(b"m/x/2".to_vec(), Some(b"b".to_vec()));
        let mut c = Ops::default();
        c.0.insert(b"m/x/2".to_vec(), None);
        // A writer's own row is held back while the engine is behind it.
        assert_eq!(
            apply_log(&store, T, &[(9, c.clone())], Some(5))
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            apply_log(&store, T, &[(3, a.clone()), (5, b.clone())], None)
                .await
                .unwrap(),
            5
        );
        // Again: nothing changes (a replay after a crash).
        assert_eq!(
            apply_log(&store, T, &[(3, a), (5, b)], None).await.unwrap(),
            5
        );
        assert_eq!(apply_log(&store, T, &[(9, c)], Some(5)).await.unwrap(), 9);
        // A writer's own row at or below the mark (its sequence number
        // was used before) is refused, not skipped.
        let mut d = Ops::default();
        d.0.insert(b"m/x/3".to_vec(), Some(b"d".to_vec()));
        assert!(apply_log(&store, T, &[(9, d)], Some(9)).await.is_err());
        let mut tx = store.begin(T).await.unwrap();
        assert_eq!(tx.get(b"m/x/3").await.unwrap(), None);
        assert_eq!(tx.get(b"m/x/1").await.unwrap(), Some(b"b".to_vec()));
        assert_eq!(tx.get(b"m/x/2").await.unwrap(), None);
        assert_eq!(applied(&mut *tx).await.unwrap(), 9);
    }

    /// A base that answers at most 2 rows per scan (as a store with a
    /// per-call cap does).
    struct Capped(Box<dyn RecordTx>);

    #[async_trait::async_trait]
    impl RecordTx for Capped {
        async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.0.get_for_update(key).await
        }
        async fn scan(
            &mut self,
            lo: &[u8],
            hi: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.0.scan(lo, hi, limit.min(2)).await
        }
        async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.0.put(key, value).await
        }
        async fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.0.delete(key).await
        }
        async fn commit(self: Box<Self>) -> Result<()> {
            self.0.commit().await
        }
        async fn rollback(self: Box<Self>) -> Result<()> {
            self.0.rollback().await
        }
    }

    #[tokio::test]
    async fn a_capped_base_is_read_in_pages() {
        let store = MemRecords::new();
        let rows: Vec<(String, String)> = (0..10)
            .map(|i| (format!("m/c/{i:02}"), format!("v{i}")))
            .collect();
        let refs: Vec<(&str, &str)> = rows.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        seed(&store, &refs).await;
        let mut s = StagedTx::new(Box::new(Capped(store.begin(T).await.unwrap())));
        s.delete(b"m/c/00").await.unwrap();
        s.put(b"m/c/05x", b"new").await.unwrap();
        let all = scan_all(&mut s, b"m/c/", b"m/c0").await.unwrap();
        assert_eq!(all.len(), 10, "{all:?}");
        assert_eq!(all[0].0, b"m/c/01".to_vec());
    }

    /// A base whose scan from a key between rows starts again at the
    /// range's beginning (what the engine's range scan was seen doing).
    struct Restarting(Box<dyn RecordTx>, Vec<u8>);

    #[async_trait::async_trait]
    impl RecordTx for Restarting {
        async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.0.get_for_update(key).await
        }
        async fn scan(
            &mut self,
            lo: &[u8],
            hi: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let start = if lo.ends_with(&[0]) {
                self.1.clone()
            } else {
                lo.to_vec()
            };
            self.0.scan(&start, hi, limit).await
        }
        async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.0.put(key, value).await
        }
        async fn delete(&mut self, key: &[u8]) -> Result<()> {
            self.0.delete(key).await
        }
        async fn commit(self: Box<Self>) -> Result<()> {
            self.0.commit().await
        }
        async fn rollback(self: Box<Self>) -> Result<()> {
            self.0.rollback().await
        }
    }

    /// A staged row past every base row still comes back when the base
    /// repeats rows on a later page (the newest change's row, unapplied).
    #[tokio::test]
    async fn a_staged_row_after_the_base_survives_a_repeating_base() {
        let store = MemRecords::new();
        seed(&store, &[("m/g/1", "a"), ("m/g/2", "b"), ("m/g/3", "c")]).await;
        let base = Restarting(store.begin(T).await.unwrap(), b"m/g/".to_vec());
        let mut s = StagedTx::new(Box::new(base));
        s.put(b"m/g/9", b"new").await.unwrap();
        let all = scan_all(&mut s, b"m/g/", b"m/g0").await.unwrap();
        let keys: Vec<Vec<u8>> = all.into_iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                b"m/g/1".to_vec(),
                b"m/g/2".to_vec(),
                b"m/g/3".to_vec(),
                b"m/g/9".to_vec()
            ]
        );
    }
}
