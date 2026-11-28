//! Unit leadership on this node, and state rebuilt when it changes.
//!
//! Whatever a unit keeps in memory beside its keys (id blocks, the
//! newest queue lane states, stream producer state) is a cache of what
//! its log wrote. When a node becomes a unit's leader it must rebuild
//! those caches from the store, not trust anything it held before: a
//! leader elsewhere may have written in between. Each unit led here has a
//! **leader epoch**: a number that changes every time this node takes the
//! unit's leadership (0 while it doesn't lead it). A cache records the
//! epoch it was built under and rebuilds itself when the epoch differs.
//!
//! On one node every unit is led here from the start, under the epoch the
//! engine opened with. On a replicated engine only the node's own unit is;
//! a tenant unit is led here from when the node's Raft group for it calls
//! [`Engine::lead_at`] (with the Raft term as the epoch) until it calls
//! [`Engine::step_down`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use crate::Engine;
use crate::log::UnitId;
use crate::txn::{LockRequest, Locked, TxnOptions};

/// Told when this node takes or loses a unit's leadership.
pub trait LeaderListener: Send + Sync {
    /// `unit` is led here under `epoch` (0: no longer led here).
    fn leader_changed(&self, unit: UnitId, epoch: u64);
}

/// The leadership table of a node.
pub(crate) struct Leaders {
    /// The epoch every unit not listed has: units are led here by default.
    pub(crate) base: u64,
    /// Replicated: tenant units not listed aren't led here (epoch 0).
    replicated: bool,
    /// Units whose leadership changed since the engine opened.
    changed: RwLock<HashMap<UnitId, u64>>,
    next: AtomicU64,
    listeners: RwLock<Vec<Arc<dyn LeaderListener>>>,
}

impl Leaders {
    pub(crate) fn new(base: u64, replicated: bool) -> Self {
        Self {
            base,
            replicated,
            changed: RwLock::default(),
            next: AtomicU64::new(base + 1),
            listeners: RwLock::default(),
        }
    }

    pub(crate) fn epoch(&self, unit: UnitId) -> u64 {
        self.changed
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .copied()
            .unwrap_or_else(|| {
                if self.replicated && unit != crate::NODE_UNIT && unit.0 != u64::MAX {
                    0
                } else {
                    self.base
                }
            })
    }

    fn set(&self, unit: UnitId, epoch: u64) {
        self.changed
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(unit, epoch);
        let ls = self
            .listeners
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for l in ls {
            l.leader_changed(unit, epoch);
        }
    }
}

impl Engine {
    /// The epoch under which this node leads `unit`; 0 if it doesn't.
    #[must_use]
    pub fn leader_epoch(&self, unit: UnitId) -> u64 {
        self.leaders().epoch(unit)
    }

    /// This node takes `unit`'s leadership: a fresh epoch, so every cache
    /// of the unit rebuilds from the store. Returns the epoch (callers
    /// that only take the leadership may ignore it).
    #[allow(clippy::must_use_candidate)]
    pub fn lead(&self, unit: UnitId) -> u64 {
        let l = self.leaders();
        let e = l.next.fetch_add(1, Ordering::AcqRel);
        self.lead_at(unit, e);
        e
    }

    /// This node takes `unit`'s leadership under `epoch`, which must differ
    /// from every epoch this node led the unit under before (a replicated
    /// node passes the Raft term it leads in). The node's clock moves above
    /// every version the store applied, so the new leader's commits come
    /// after its predecessors'.
    pub fn lead_at(&self, unit: UnitId, epoch: u64) {
        self.txn_shared().observe(self.applied_max_ts());
        // Nothing from before counts: transactions that locked the unit's
        // rows while another node led it roll back, and versions an older
        // leadership of this node left pending are gone (each is applied
        // already, or was replaced in the log).
        self.forget_unit_state(unit, "took");
        let replicated = self.is_replicated_unit(unit);
        if replicated {
            // Commits across units prepared here under earlier leaders, or
            // anchored here: read back (their keys locked again) before the
            // unit counts as led, so nothing reads or writes past them.
            self.txn_shared().cross_taking(unit.0, true);
            self.txn_shared().load_cross_led(self, unit.0);
        }
        let l = self.leaders();
        l.next.fetch_max(epoch.saturating_add(1), Ordering::AcqRel);
        l.set(unit, epoch);
        if replicated {
            self.txn_shared().cross_taking(unit.0, false);
            self.txn_shared().load_xa_of(self, Some(unit.0));
            // The resolver finishes them from the anchors' records (not
            // from here: this may run inside the log's own turn).
        }
    }

    /// This node no longer leads `unit`. Transactions holding or waiting
    /// for the unit's locks are rolled back at their next step with 1213
    /// (their locks mean nothing once another node leads the unit), and
    /// caches of the unit's state drop (epoch 0). Commits already proposed
    /// fail as retryable if the new leader's log replaces them; replies
    /// that read their early-released writes fail with them.
    pub fn step_down(&self, unit: UnitId) {
        self.leaders().set(unit, 0);
        self.forget_unit_state(unit, "lost");
        // Commits across units this node was finishing for the unit are
        // its next leader's: the resolver drops what is kept of them here.
    }

    /// Dooms the unit's lock holders and drops its pending versions.
    fn forget_unit_state(&self, unit: UnitId, what: &str) {
        if unit == crate::NODE_UNIT || unit.0 == crate::keys::META_TENANT || !self.is_replicated() {
            return;
        }
        let range = crate::keys::tenant_range(unit.0);
        let shared = self.txn_shared();
        // Prepared XA transactions of the unit are listed again from its
        // store by whichever node leads it.
        shared.forget_xa_of(unit.0);
        let doomed = shared.locks.doom_range(&range);
        let dropped = shared.overlay.remove_range(&range);
        if !doomed.is_empty() || dropped > 0 {
            tracing::info!(
                unit = unit.0,
                transactions = doomed.len(),
                pending_versions = dropped,
                "this node {what} the unit's leadership: its lock holders roll back"
            );
        }
    }

    /// Registers a listener for leadership changes.
    pub fn on_leader_change(&self, l: Arc<dyn LeaderListener>) {
        self.leaders()
            .listeners
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(l);
    }
}

/// Ids from a counter record, handed out in blocks: each block is one
/// commit that raises the counter (through the unit's log), and the ids
/// of a block are handed out from memory. A block taken under an older
/// leader epoch is dropped, never reused: a new leader starts from the
/// counter, so ids stay unique across leaders and restarts (with gaps).
pub struct IdBlocks {
    engine: Engine,
    key: Bytes,
    unit: UnitId,
    block: u64,
    state: Mutex<IdState>,
}

#[derive(Debug, Default)]
struct IdState {
    epoch: u64,
    next: u64,
    limit: u64,
}

impl std::fmt::Debug for IdBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdBlocks")
            .field("unit", &self.unit)
            .field("block", &self.block)
            .finish_non_exhaustive()
    }
}

impl IdBlocks {
    /// Ids from the counter at `key` (a key of the unit that holds it),
    /// `block` at a time. The counter holds the next id not yet taken
    /// (8 bytes, big-endian); a missing counter starts at 1.
    #[must_use]
    pub fn new(engine: Engine, key: impl Into<Bytes>, block: u64) -> Self {
        let key = key.into();
        let unit = engine.unit_of_key(&key);
        Self {
            engine,
            key,
            unit,
            block: block.max(1),
            state: Mutex::new(IdState::default()),
        }
    }

    /// The next id.
    ///
    /// # Errors
    /// This node doesn't lead the unit, or the commit raising the counter
    /// failed.
    pub fn next(&self) -> Result<u64> {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let epoch = self.engine.leader_epoch(self.unit);
        if epoch == 0 {
            return Err(SprigError::internal(format!(
                "ids: unit {} isn't led here",
                self.unit.0
            )));
        }
        if s.epoch != epoch || s.next >= s.limit {
            let from = self.take_block()?;
            *s = IdState {
                epoch,
                next: from,
                limit: from + self.block,
            };
        }
        let id = s.next;
        s.next += 1;
        Ok(id)
    }

    /// Raises the counter by a block in one commit; returns the block's
    /// first id once the commit is durable.
    fn take_block(&self) -> Result<u64> {
        let mut t = self.engine.begin(TxnOptions::default())?;
        let from = match t.lock_get_blocking(&self.key, LockRequest::for_update())? {
            Locked::Row(Some(v)) => u64::from_be_bytes(
                v.as_ref()
                    .try_into()
                    .map_err(|_| SprigError::internal("ids: the counter is corrupt"))?,
            ),
            Locked::Row(None) | Locked::Skipped => 1,
        };
        let to = from
            .checked_add(self.block)
            .ok_or_else(|| SprigError::internal("ids: the counter is exhausted"))?;
        t.put(self.key.to_vec(), Bytes::copy_from_slice(&to.to_be_bytes()))?;
        t.commit()?.wait()?;
        Ok(from)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::config::{EngineConfig, StoreKind};
    use crate::keys::{KeyPrefix, kind};
    use crate::testutil::TempDir;

    fn counter(tenant: u64) -> Vec<u8> {
        KeyPrefix {
            tenant,
            shard: 0,
            object: 9,
            kind: kind::COUNTERS,
            index: 0,
        }
        .to_vec()
    }

    fn open(dir: &TempDir) -> Engine {
        let mut c = EngineConfig::new(dir.path());
        c.store = StoreKind::Rocks;
        c.checkpoint_interval = std::time::Duration::ZERO;
        c.log.sync = crate::log::SyncMode::Off;
        Engine::open(c).unwrap()
    }

    struct Count(AtomicUsize);

    impl LeaderListener for Count {
        fn leader_changed(&self, _: UnitId, _: u64) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Ids are unique across a leadership change and a restart: a block
    /// held under an older epoch is dropped, never handed out again.
    #[test]
    fn id_blocks_never_reuse_an_id_across_leaders_and_restarts() {
        let dir = TempDir::new("ids");
        let mut seen = std::collections::BTreeSet::new();
        {
            let e = open(&dir);
            let count = Arc::new(Count(AtomicUsize::new(0)));
            e.on_leader_change(count.clone());
            let ids = IdBlocks::new(e.clone(), counter(7), 10);
            for _ in 0..25 {
                assert!(seen.insert(ids.next().unwrap()));
            }
            assert_eq!(e.leader_epoch(UnitId(7)), 1);
            // Leadership leaves and comes back: the cached block goes.
            e.step_down(UnitId(7));
            assert_eq!(e.leader_epoch(UnitId(7)), 0);
            assert!(ids.next().is_err());
            let epoch = e.lead(UnitId(7));
            assert!(epoch > 1);
            let first = ids.next().unwrap();
            assert_eq!(first, 31, "starts at the counter, past the dropped block");
            assert!(seen.insert(first));
            assert_eq!(count.0.load(Ordering::Relaxed), 2);
            // Another unit's leadership is its own.
            assert_eq!(e.leader_epoch(UnitId(8)), 1);
        }
        let e = open(&dir);
        let ids = IdBlocks::new(e, counter(7), 10);
        let next = ids.next().unwrap();
        assert_eq!(next, 41);
        assert!(seen.insert(next));
    }
}
