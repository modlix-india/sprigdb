//! Catalog records in memory, for unit and property tests: the same rules
//! as the engine's (a read view taken at the first plain read, exclusive
//! record locks held until the transaction ends with a wait limit, writes
//! applied only at commit), with no durability.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_common::Result;
use sprig_common::ids::TenantId;
use tokio::sync::OwnedMutexGuard;

use super::{LOCK_WAIT, RecordStore, RecordTx, busy};

type Map = BTreeMap<(u64, Vec<u8>), Vec<u8>>;

/// One lock per (tenant, key).
type Locks = HashMap<(u64, Vec<u8>), Arc<tokio::sync::Mutex<()>>>;

#[derive(Default)]
struct Inner {
    committed: Mutex<Arc<Map>>,
    locks: Mutex<Locks>,
}

/// Every tenant's catalog records, in memory.
#[derive(Clone, Default)]
pub struct MemRecords {
    inner: Arc<Inner>,
    lock_wait: Option<Duration>,
}

impl std::fmt::Debug for MemRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemRecords").finish_non_exhaustive()
    }
}

impl MemRecords {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The same store with another lock wait (tests of 39071).
    #[must_use]
    pub fn with_lock_wait(mut self, wait: Duration) -> Self {
        self.lock_wait = Some(wait);
        self
    }

    /// Every committed record of `tenant`, in key order.
    #[must_use]
    pub fn dump(&self, tenant: TenantId) -> Vec<(Vec<u8>, Vec<u8>)> {
        let map = Arc::clone(
            &self
                .inner
                .committed
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        map.iter()
            .filter(|((t, _), _)| *t == tenant.0)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[async_trait::async_trait]
impl RecordStore for MemRecords {
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        Ok(Box::new(MemTx {
            store: self.clone(),
            tenant: tenant.0,
            view: None,
            writes: BTreeMap::new(),
            held: HashMap::new(),
        }))
    }
}

struct MemTx {
    store: MemRecords,
    tenant: u64,
    view: Option<Arc<Map>>,
    /// Own writes: `None` deletes.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    held: HashMap<Vec<u8>, OwnedMutexGuard<()>>,
}

impl MemTx {
    fn committed(&self) -> Arc<Map> {
        Arc::clone(
            &self
                .store
                .inner
                .committed
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    fn view(&mut self) -> Arc<Map> {
        if self.view.is_none() {
            self.view = Some(self.committed());
        }
        self.view.clone().unwrap_or_default()
    }

    async fn lock(&mut self, key: &[u8]) -> Result<()> {
        if self.held.contains_key(key) {
            return Ok(());
        }
        let m = Arc::clone(
            self.store
                .inner
                .locks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry((self.tenant, key.to_vec()))
                .or_default(),
        );
        let wait = self.store.lock_wait.unwrap_or(LOCK_WAIT);
        let g = tokio::time::timeout(wait, m.lock_owned())
            .await
            .map_err(|_| busy())?;
        self.held.insert(key.to_vec(), g);
        Ok(())
    }
}

#[async_trait::async_trait]
impl RecordTx for MemTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(w) = self.writes.get(key) {
            return Ok(w.clone());
        }
        Ok(self.view().get(&(self.tenant, key.to_vec())).cloned())
    }

    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.lock(key).await?;
        if let Some(w) = self.writes.get(key) {
            return Ok(w.clone());
        }
        Ok(self.committed().get(&(self.tenant, key.to_vec())).cloned())
    }

    async fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let view = self.view();
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = view
            .range((self.tenant, lo.to_vec())..(self.tenant, hi.to_vec()))
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect();
        for (k, w) in self.writes.range(lo.to_vec()..hi.to_vec()) {
            match w {
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
        self.lock(key).await?;
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.lock(key).await?;
        self.writes.insert(key.to_vec(), None);
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        if !self.writes.is_empty() {
            let mut g = self
                .store
                .inner
                .committed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let map = Arc::make_mut(&mut g);
            for (k, w) in std::mem::take(&mut self.writes) {
                match w {
                    Some(v) => {
                        map.insert((self.tenant, k), v);
                    }
                    None => {
                        map.remove(&(self.tenant, k));
                    }
                }
            }
        }
        self.held.clear();
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
