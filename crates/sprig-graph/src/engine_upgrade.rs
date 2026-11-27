//! Rebuilding equality-only indexes into ordered keys, online.
//!
//! Indexes made before ordered keys ([`crate::ordkey`]) keep typed-text
//! keys, which serve equality and `IN` only. Each is rebuilt in the
//! background when its graph opens, as `CREATE INDEX` builds one:
//!
//! 1. Under the schema lock, a twin definition is published with a new
//!    index number, ordered keys and the state `upgrading`: every writer
//!    keeps its entries from then on, as for any definition, and no read
//!    or `SHOW` sees it.
//! 2. The twin is backfilled in pages, each element locked ([`backfill`]).
//! 3. Under the schema lock, the twin takes the old definition's place
//!    (same name, state `online`) and the old definition goes; then the old
//!    entries are deleted in batches, and a statistics pass rebuilds the
//!    histograms under the new number.
//!
//! A restart part-way resumes at step 2 (a backfill writes the same
//! entries again). `DROP INDEX` meanwhile drops both definitions; the
//! upgrade then deletes what the twin had. A uniqueness constraint whose
//! values clash under ordered keys (`1` and `1.0` are one key there) keeps
//! its old keys, and the clash is logged. `SPRIGDB_GRAPH_INDEX_UPGRADE=off`
//! turns the background rebuild off.
//!
//! [`backfill`]: EngineGraph::backfill

use std::sync::atomic::Ordering;

use sprig_common::Result;
use sprig_engine::txn::Txn;

use super::{C_INDEX, EngineGraph, encode_def, poisoned};
use crate::store::IndexDef;

/// The state of a twin definition being built.
pub(crate) const UPGRADING: &str = "upgrading";

/// Whether equality-only indexes are rebuilt when a graph opens
/// (`SPRIGDB_GRAPH_INDEX_UPGRADE`, on unless `off`).
#[must_use]
pub fn upgrade_on() -> bool {
    std::env::var("SPRIGDB_GRAPH_INDEX_UPGRADE").map_or(true, |v| !v.eq_ignore_ascii_case("off"))
}

/// One step of work: an old definition and its twin (when published).
type Job = (Option<IndexDef>, Option<IndexDef>);

impl EngineGraph {
    /// Whether an index waits for its rebuild into ordered keys.
    #[must_use]
    pub fn needs_upgrade(&self) -> bool {
        self.next_upgrade().is_some()
    }

    fn next_upgrade(&self) -> Option<Job> {
        let defs = self.defs();
        let failed = self.upgrade_failed.lock().unwrap_or_else(poisoned);
        if let Some(t) = defs.iter().find(|d| d.state == UPGRADING) {
            let old = defs
                .iter()
                .find(|d| d.name == t.name && d.id != t.id && d.state == "online")
                .cloned();
            return Some((old, Some(t.clone())));
        }
        defs.iter()
            .find(|d| d.state == "online" && !self.is_ordered(d.id) && !failed.contains(&d.id))
            .map(|d| (Some(d.clone()), None))
    }

    /// Rebuilds every equality-only index into ordered keys (see the
    /// module notes), one at a time; the number rebuilt.
    ///
    /// # Errors
    /// The engine's.
    pub async fn upgrade_indexes(&self) -> Result<usize> {
        let _one = self.upgrade_lock.lock().await;
        let mut done = 0;
        while let Some((old, twin)) = self.next_upgrade() {
            if self.dropped.load(Ordering::Acquire) {
                break;
            }
            let Some(old) = old else {
                // The old index was dropped: the twin goes too.
                if let Some(t) = twin {
                    self.remove_twin(&t).await?;
                }
                continue;
            };
            let twin = match twin {
                Some(t) => t,
                None => self.publish_twin(&old).await?,
            };
            if let Err(e) = self.backfill(&twin).await {
                tracing::warn!(error = %e, index = %old.name, graph = self.graph_id,
                    "graph index kept its equality-only keys");
                self.upgrade_failed
                    .lock()
                    .unwrap_or_else(poisoned)
                    .insert(old.id);
                self.remove_twin(&twin).await?;
                continue;
            }
            if self.swap_twin(&old, &twin).await? {
                done += 1;
            }
        }
        if done > 0 && super::stats::enabled() {
            self.analyze_in_background();
        }
        Ok(done)
    }

    /// Step 1: the twin of `old`, published under the schema lock.
    pub(super) async fn publish_twin(&self, old: &IndexDef) -> Result<IndexDef> {
        let mut x = self.schema_exclusive().await?;
        let r: Result<IndexDef> = async {
            let (start, _) = self.take_block(C_INDEX, 1).await?;
            let id = u32::try_from(start)
                .ok()
                .filter(|i| u16::try_from(*i).is_ok())
                .ok_or_else(|| {
                    sprig_common::SprigError::new(
                        sprig_common::SprigCode::GraphUnsupported,
                        "the graph has used every index number",
                    )
                })?;
            let mut t = old.clone();
            t.id = id;
            t.state = UPGRADING.into();
            x.put(self.space.index_def(id), encode_def(&t, true))?;
            Ok(t)
        }
        .await;
        let t = match r {
            Ok(t) => t,
            Err(e) => {
                x.rollback();
                return Err(e);
            }
        };
        self.ordered.write().unwrap_or_else(poisoned).insert(t.id);
        self.indexes
            .write()
            .unwrap_or_else(poisoned)
            .push(t.clone());
        if let Err(e) = commit(x).await {
            self.indexes
                .write()
                .unwrap_or_else(poisoned)
                .retain(|y| y.id != t.id);
            return Err(e);
        }
        Ok(t)
    }

    /// Step 3: the twin replaces `old`. `false` when `old` went meanwhile
    /// (the twin is removed instead).
    pub(super) async fn swap_twin(&self, old: &IndexDef, twin: &IndexDef) -> Result<bool> {
        let mut x = self.schema_exclusive().await?;
        let defs = self.defs();
        let old_there = defs.iter().any(|d| d.id == old.id && d.state == "online");
        if !old_there {
            x.rollback();
            self.remove_twin(twin).await?;
            return Ok(false);
        }
        let mut new = twin.clone();
        new.state = "online".into();
        let r: Result<()> = (|| {
            x.delete(self.space.index_def(old.id))?;
            x.put(self.space.index_def(new.id), encode_def(&new, true))?;
            Ok(())
        })();
        if let Err(e) = r {
            x.rollback();
            return Err(e);
        }
        // In memory while the lock is held (the commit releases it).
        let before = self.defs();
        {
            let mut v = self.indexes.write().unwrap_or_else(poisoned);
            v.retain(|y| y.id != old.id);
            for y in v.iter_mut() {
                if y.id == new.id {
                    y.state = "online".into();
                }
            }
        }
        if let Err(e) = commit(x).await {
            *self.indexes.write().unwrap_or_else(poisoned) = before;
            return Err(e);
        }
        self.ordered
            .write()
            .unwrap_or_else(poisoned)
            .remove(&old.id);
        self.delete_entries(old).await?;
        Ok(true)
    }

    /// Deletes a twin's definition (if still stored) and its entries.
    async fn remove_twin(&self, twin: &IndexDef) -> Result<()> {
        let mut x = self.schema_exclusive().await?;
        if let Err(e) = x.delete(self.space.index_def(twin.id)) {
            x.rollback();
            return Err(e);
        }
        self.indexes
            .write()
            .unwrap_or_else(poisoned)
            .retain(|y| y.id != twin.id);
        commit(x).await?;
        self.delete_entries(twin).await
    }

    /// Starts [`EngineGraph::upgrade_indexes`] in the background, when an
    /// index waits for it and a runtime is at hand.
    pub(super) fn upgrade_in_background(&self) {
        if !upgrade_on() || !self.needs_upgrade() {
            return;
        }
        let Some(me) = self.me.upgrade() else { return };
        let Ok(h) = tokio::runtime::Handle::try_current() else {
            return;
        };
        h.spawn(async move {
            match me.upgrade_indexes().await {
                Ok(n) if n > 0 => {
                    tracing::info!(
                        graph = me.graph_id,
                        indexes = n,
                        "graph indexes rebuilt with ordered keys"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, graph = me.graph_id, "graph index rebuild stopped");
                }
            }
        });
    }
}

async fn commit(x: Txn) -> Result<()> {
    x.commit()?.wait_async().await
}

/// Makes the next indexes made on this thread keep equality-only keys, as
/// indexes made before ordered keys do (tests of the rebuild).
#[cfg(test)]
pub(crate) fn set_legacy_indexes(on: bool) {
    LEGACY.with(|c| c.set(on));
}

#[cfg(test)]
thread_local! {
    static LEGACY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether new indexes keep equality-only keys (tests only).
pub(crate) fn legacy_indexes() -> bool {
    #[cfg(test)]
    {
        LEGACY.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    {
        false
    }
}
