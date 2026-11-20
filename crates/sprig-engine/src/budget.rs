//! Memory budgets: the engine's share of node memory, split into pools.
//!
//! The engine gets a share of the node's memory (70% by default, to be
//! tuned by measurement) and splits it:
//!
//! | Budget | Share of the engine | Mechanism |
//! |---|---|---|
//! | Block cache | 40% | RocksDB's `HyperClock` cache |
//! | Memtables | 15% | RocksDB's write buffer manager, charged to the cache |
//! | Log buffers | 5% | raft-engine's index and the commit queue |
//! | Transactions | 10% | [`Pool`]: write sets, the lock table, pending versions |
//! | Queries | 20% | [`Pool`]: reader permits for scans and operators; per tenant at most a quarter |
//! | Apply-time indexes | 10% | [`Pool`]: unused for now |
//!
//! Nothing grows past its pool: a [`Pool::try_acquire`] that doesn't fit
//! fails, and the caller spills or refuses with the protocol's error.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// How the engine's memory is split.
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetConfig {
    /// The engine's memory in MiB.
    pub engine_mb: u64,
    /// Block cache share, percent.
    pub block_cache_pct: u64,
    /// Memtable share, percent.
    pub memtable_pct: u64,
    /// Log buffer share, percent.
    pub log_pct: u64,
    /// Transaction share, percent.
    pub txn_pct: u64,
    /// Query share, percent.
    pub query_pct: u64,
    /// Apply-time index share, percent.
    pub index_pct: u64,
    /// The most one tenant may hold of the query share, percent.
    pub tenant_query_pct: u64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            engine_mb: 1024,
            block_cache_pct: 40,
            memtable_pct: 15,
            log_pct: 5,
            txn_pct: 10,
            query_pct: 20,
            index_pct: 10,
            tenant_query_pct: 25,
        }
    }
}

impl BudgetConfig {
    /// The default split of `share_pct` percent of a node with
    /// `node_memory_mb`.
    #[must_use]
    pub fn for_node(node_memory_mb: u64, share_pct: u64) -> Self {
        Self {
            engine_mb: node_memory_mb * share_pct / 100,
            ..Self::default()
        }
    }

    fn bytes(&self, pct: u64) -> u64 {
        (self.engine_mb << 20) * pct / 100
    }
}

/// A byte budget that hands out permits. Pools nest: a permit from a child
/// is charged to every ancestor too.
#[derive(Debug)]
pub struct Pool {
    name: String,
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
    refused: AtomicU64,
    parent: Option<Arc<Pool>>,
}

impl Pool {
    /// A top-level pool.
    #[must_use]
    pub fn new(name: impl Into<String>, limit: u64) -> Arc<Self> {
        Self::with_parent(name, limit, None)
    }

    fn with_parent(name: impl Into<String>, limit: u64, parent: Option<Arc<Pool>>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            parent,
        })
    }

    /// The pool's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its limit in bytes.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes held now.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// The most bytes ever held at once.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Requests refused because they didn't fit.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    fn charge(&self, bytes: u64) -> bool {
        let mut cur = self.used.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_add(bytes);
            if next > self.limit {
                self.refused.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            match self
                .used
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::Relaxed);
                    break;
                }
                Err(c) => cur = c,
            }
        }
        if let Some(p) = &self.parent
            && !p.charge(bytes)
        {
            self.used.fetch_sub(bytes, Ordering::AcqRel);
            return false;
        }
        true
    }

    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
        if let Some(p) = &self.parent {
            p.release(bytes);
        }
    }

    /// A permit for `bytes`, if it fits here and in every ancestor.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>, bytes: u64) -> Option<Permit> {
        self.charge(bytes).then(|| Permit {
            pool: Arc::clone(self),
            bytes,
        })
    }
}

/// Bytes held from a [`Pool`], returned on drop.
#[derive(Debug)]
#[must_use]
pub struct Permit {
    pool: Arc<Pool>,
    bytes: u64,
}

impl Permit {
    /// Bytes held.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Holds `more` bytes too, if they fit. On failure nothing changes.
    pub fn grow(&mut self, more: u64) -> bool {
        if self.pool.charge(more) {
            self.bytes += more;
            true
        } else {
            false
        }
    }

    /// Returns `less` bytes early.
    pub fn shrink(&mut self, less: u64) {
        let less = less.min(self.bytes);
        self.pool.release(less);
        self.bytes -= less;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.pool.release(self.bytes);
        }
    }
}

/// The engine's budgets.
#[derive(Debug)]
pub struct Budgets {
    cfg: BudgetConfig,
    /// RocksDB block cache bytes.
    pub block_cache_bytes: u64,
    /// RocksDB memtable bytes (charged to the cache).
    pub memtable_bytes: u64,
    /// Log buffer bytes: the commit queue's cap.
    pub log_bytes: u64,
    /// Transactions: write sets, locks, pending versions.
    pub txn: Arc<Pool>,
    /// Queries: reader permits.
    pub query: Arc<Pool>,
    /// Apply-time indexes.
    pub index: Arc<Pool>,
    tenants: Mutex<HashMap<u64, Arc<Pool>>>,
}

impl Budgets {
    /// Budgets from a split.
    #[must_use]
    pub fn new(cfg: &BudgetConfig) -> Self {
        Self {
            block_cache_bytes: cfg.bytes(cfg.block_cache_pct),
            memtable_bytes: cfg.bytes(cfg.memtable_pct),
            log_bytes: cfg.bytes(cfg.log_pct),
            txn: Pool::new("engine.txn", cfg.bytes(cfg.txn_pct)),
            query: Pool::new("engine.query", cfg.bytes(cfg.query_pct)),
            index: Pool::new("engine.index", cfg.bytes(cfg.index_pct)),
            tenants: Mutex::new(HashMap::new()),
            cfg: cfg.clone(),
        }
    }

    /// The query pool of one tenant: a child of the query pool capped at
    /// `tenant_query_pct` of it.
    #[must_use]
    pub fn tenant_query(&self, tenant: u64) -> Arc<Pool> {
        let mut t = self.tenants.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(t.entry(tenant).or_insert_with(|| {
            Pool::with_parent(
                format!("engine.query.tenant.{tenant}"),
                self.query.limit() * self.cfg.tenant_query_pct / 100,
                Some(Arc::clone(&self.query)),
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and_permits() {
        let b = Budgets::new(&BudgetConfig::for_node(1000, 70));
        assert_eq!(b.block_cache_bytes, (700u64 << 20) * 40 / 100);
        let q = &b.query;
        let mut p = q.try_acquire(q.limit() / 2).unwrap();
        assert!(q.try_acquire(q.limit()).is_none());
        assert!(!p.grow(q.limit()));
        assert!(p.grow(10));
        p.shrink(10);
        drop(p);
        assert_eq!(q.used(), 0);
        assert_eq!(q.refused(), 2);
    }

    #[test]
    fn tenant_cap_and_parent() {
        let b = Budgets::new(&BudgetConfig::default());
        let t1 = b.tenant_query(1);
        let cap = t1.limit();
        assert_eq!(cap, b.query.limit() / 4);
        let a = t1.try_acquire(cap).unwrap();
        assert!(t1.try_acquire(1).is_none(), "tenant cap");
        assert_eq!(b.query.used(), cap, "charged to the parent");
        drop(a);
        assert_eq!(b.query.used(), 0);
        // Four tenants at their cap fill the parent; a fifth is refused there.
        let held: Vec<_> = (10..14)
            .map(|t| b.tenant_query(t).try_acquire(cap).unwrap())
            .collect();
        assert!(b.tenant_query(20).try_acquire(1).is_none());
        assert_eq!(
            b.tenant_query(20).used(),
            0,
            "a refused parent charge is rolled back"
        );
        drop(held);
    }
}
