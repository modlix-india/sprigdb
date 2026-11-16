//! The statistics journal: a validation's snapshot of the statistics rows,
//! kept outside the process.
//!
//! A replica or RSU validation snapshots the node's `mysql.table_stats`,
//! `column_stats` and `index_stats` rows of its table, runs `ANALYZE LOCAL
//! ... PERSISTENT` with the binary log off, and puts the rows back at the
//! end. A controller that stops in between loses a snapshot kept only in
//! memory, and the node keeps the refreshed statistics. The journal keeps
//! the snapshot where the next controller finds it: one entry per node and
//! table. The controller's journal is a key in the DC tier of etcd; this
//! module has the trait and an in-memory version for tests.
//!
//! ```text
//! a replica or RSU validation, when it would ANALYZE:
//!   an entry for (node, table) exists:
//!     another validation of this process runs on the table → no ANALYZE
//!     else put its rows back (binary log off), remove it   (a cut-off one's)
//!   snapshot; journal it (only if no entry exists); can't → no ANALYZE
//!   ... build, ANALYZE, replay ...
//!   put the rows back (or keep the index for the rollout); remove the entry
//! the drift reconciler, on every node of the object's cluster:
//!   each entry of the object's tables that no validation of this process
//!   runs on → put its rows back (binary log off), remove it
//! ```
//!
//! An entry is removed only while it is still the one read, so a pass that
//! restores an old entry never removes a newer validation's.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_cluster::sql::{NodeEndpoint, ProbeError};
use sprig_common::ids::NodeId;
use sprig_common::{Result, SprigCode, SprigError};

use super::IndexTarget;
use super::node::StatsSnapshot;
use super::replica::quiet_session;

/// The largest entry a validation journals (its JSON). A table with many
/// long histograms is bigger: its validation replays without `ANALYZE`.
pub const MAX_ENTRY_BYTES: usize = 512 * 1024;

/// One statistics table's rows in an entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatTable {
    /// `table_stats`, `column_stats` or `index_stats` (in `mysql`).
    pub table: String,
    /// Its columns, in order.
    pub columns: Vec<String>,
    /// The rows, each value hex-encoded (`NULL` as `None`).
    pub rows: Vec<Vec<Option<String>>>,
}

/// A validation's statistics snapshot of one node's table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsEntry {
    /// The node.
    pub node: String,
    /// The table's schema, as the validation named it.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The candidate's columns (their `column_stats` rows are held).
    pub columns: Vec<String>,
    /// The validation's index (its `index_stats` rows are held).
    pub index: String,
    /// When the snapshot was taken, in Unix milliseconds.
    pub taken_at_ms: u64,
    /// The rows.
    pub stats: Vec<StatTable>,
}

impl StatsEntry {
    /// Rows held.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.stats.iter().map(|s| s.rows.len()).sum()
    }

    /// The table, as the validators name it.
    #[must_use]
    pub fn target(&self) -> IndexTarget {
        IndexTarget {
            schema: self.schema.clone(),
            table: self.table.clone(),
            pk: Vec::new(),
        }
    }
}

/// Where snapshots are kept while their validation runs.
#[async_trait::async_trait]
pub trait StatsJournal: Send + Sync {
    /// Writes `e` unless an entry for its node and table exists (`false`
    /// then). Schema and table names compare without case.
    async fn insert(&self, e: &StatsEntry) -> Result<bool>;
    /// The entry for `node`'s table, if any.
    async fn entry(&self, node: &NodeId, schema: &str, table: &str) -> Result<Option<StatsEntry>>;
    /// Every entry of `node`.
    async fn entries(&self, node: &NodeId) -> Result<Vec<StatsEntry>>;
    /// Removes the entry for `e`'s node and table if it is still `e`;
    /// `false` when it is gone or another one.
    async fn remove(&self, e: &StatsEntry) -> Result<bool>;
}

type MemKey = (String, String, String);

fn mem_key(node: &str, schema: &str, table: &str) -> MemKey {
    (
        node.to_owned(),
        schema.to_ascii_lowercase(),
        table.to_ascii_lowercase(),
    )
}

/// A journal in memory, for tests. [`MemJournal::set_failing`] makes
/// every call fail.
#[derive(Debug, Default)]
pub struct MemJournal {
    entries: Mutex<BTreeMap<MemKey, StatsEntry>>,
    failing: AtomicBool,
}

impl MemJournal {
    /// Every call fails while `on`.
    pub fn set_failing(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    /// Every entry held.
    #[must_use]
    pub fn all(&self) -> Vec<StatsEntry> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn check(&self) -> Result<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(SprigError::new(
                SprigCode::MetaUnavailable,
                "the statistics journal is failing (test)",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StatsJournal for MemJournal {
    async fn insert(&self, e: &StatsEntry) -> Result<bool> {
        self.check()?;
        let mut m = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let k = mem_key(&e.node, &e.schema, &e.table);
        if m.contains_key(&k) {
            return Ok(false);
        }
        m.insert(k, e.clone());
        Ok(true)
    }

    async fn entry(&self, node: &NodeId, schema: &str, table: &str) -> Result<Option<StatsEntry>> {
        self.check()?;
        Ok(self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&mem_key(node.as_str(), schema, table))
            .cloned())
    }

    async fn entries(&self, node: &NodeId) -> Result<Vec<StatsEntry>> {
        self.check()?;
        Ok(self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(k, _)| k.0 == node.as_str())
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn remove(&self, e: &StatsEntry) -> Result<bool> {
        self.check()?;
        let mut m = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let k = mem_key(&e.node, &e.schema, &e.table);
        if m.get(&k) == Some(e) {
            m.remove(&k);
            return Ok(true);
        }
        Ok(false)
    }
}

fn probe_err(message: String) -> ProbeError {
    ProbeError {
        errno: None,
        message,
        timeout: false,
    }
}

/// Puts entry `e`'s rows back on `node`, on a session with the binary log
/// off (as a validation's own restore); returns the rows written.
///
/// # Errors
/// The node is unreachable, the entry isn't a valid snapshot, or a
/// statement failed.
pub async fn restore_entry(node: &NodeEndpoint, e: &StatsEntry) -> Result<usize, ProbeError> {
    let snap = StatsSnapshot::from_entry(e).map_err(probe_err)?;
    let mut s = quiet_session(node, Duration::from_secs(30), Duration::from_secs(5)).await?;
    let res = snap.restore(&mut s).await;
    s.s.close().await;
    res.map(|()| snap.rows())
}

/// What [`restore_journaled`] did on one node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Restored {
    /// Tables whose statistics were put back, as `schema.table`.
    pub tables: Vec<String>,
    /// Failures, retried next time.
    pub errors: Vec<String>,
}

/// The drift reconciler's pass on one node: every journal entry of
/// `tables` (names compared without case) that no validation of this
/// process runs on is put back with the binary log off and removed: the
/// snapshot of a validation the controller didn't finish.
pub async fn restore_journaled(
    node: &NodeEndpoint,
    journal: &dyn StatsJournal,
    tables: &[IndexTarget],
) -> Restored {
    let mut out = Restored::default();
    let entries = match journal.entries(&node.node).await {
        Ok(e) => e,
        Err(e) => {
            out.errors.push(format!("statistics journal not read: {e}"));
            return out;
        }
    };
    for e in entries {
        let t = e.target();
        let ours = tables.iter().any(|x| {
            x.schema.eq_ignore_ascii_case(&t.schema) && x.table.eq_ignore_ascii_case(&t.table)
        });
        if !ours || super::orphans::is_running(&node.node, &t) {
            continue;
        }
        match restore_entry(node, &e).await {
            Ok(rows) => {
                if let Err(err) = journal.remove(&e).await {
                    out.errors.push(format!(
                        "{}.{}: entry not removed: {err}",
                        e.schema, e.table
                    ));
                }
                tracing::warn!(node = %node.node, schema = %e.schema, table = %e.table, rows,
                    taken_at_ms = e.taken_at_ms,
                    "statistics of a cut-off validation put back (binary log off)");
                out.tables.push(format!("{}.{}", e.schema, e.table));
            }
            Err(err) => out.errors.push(format!(
                "{}.{}: statistics not put back: {err}",
                e.schema, e.table
            )),
        }
    }
    out
}

/// Before a validation on `node`'s table `t` snapshots its statistics: an
/// entry a cut-off validation left is put back and removed. `Ok(Some(_))`
/// is a note for the report; `Err` means the validation must not
/// `ANALYZE` (another validation of this process runs on the table, or the
/// journal or node failed).
pub(crate) async fn prepare(
    journal: Option<&dyn StatsJournal>,
    node: &NodeEndpoint,
    t: &IndexTarget,
) -> std::result::Result<Option<String>, String> {
    let Some(j) = journal else {
        return Ok(None);
    };
    let e = j
        .entry(&node.node, &t.schema, &t.table)
        .await
        .map_err(|e| format!("statistics journal not read: {e}"))?;
    let Some(e) = e else {
        return Ok(None);
    };
    if super::orphans::others_running(&node.node, t) {
        return Err("another validation's statistics snapshot is journaled for the table".into());
    }
    let rows = restore_entry(node, &e)
        .await
        .map_err(|err| format!("a cut-off validation's statistics not put back: {err}"))?;
    j.remove(&e)
        .await
        .map_err(|err| format!("statistics journal entry not removed: {err}"))?;
    tracing::warn!(node = %node.node, schema = %t.schema, table = %t.table, rows,
        "statistics of a cut-off validation put back before validating");
    Ok(Some(format!(
        "statistics of a cut-off validation put back first ({rows} rows)"
    )))
}

/// Journals `snap` before the validation's `ANALYZE`. `Ok(None)` without
/// a journal; `Err` means the validation must not `ANALYZE`.
pub(crate) async fn record(
    journal: Option<&dyn StatsJournal>,
    node: &NodeEndpoint,
    t: &IndexTarget,
    snap: &StatsSnapshot,
    (columns, index): (&[String], &str),
    now_ms: u64,
) -> std::result::Result<Option<StatsEntry>, String> {
    let Some(j) = journal else {
        return Ok(None);
    };
    let e = snap.to_entry(node.node.as_str(), t, columns, index, now_ms);
    let size = serde_json::to_vec(&e).map_or(usize::MAX, |v| v.len());
    if size > MAX_ENTRY_BYTES {
        return Err(format!(
            "the statistics snapshot is too large to journal ({size} bytes)"
        ));
    }
    match j.insert(&e).await {
        Ok(true) => Ok(Some(e)),
        Ok(false) => Err("another statistics snapshot is journaled for the table".into()),
        Err(err) => Err(format!("statistics journal not written: {err}")),
    }
}

/// Step 3 of the replica and RSU validators: [`prepare`], the snapshot,
/// then [`record`]. `None` (with a note in `r`) means no `ANALYZE`; the
/// entry is `None` without a journal.
pub(crate) async fn snapshot(
    journal: Option<&dyn StatsJournal>,
    node: &NodeEndpoint,
    mon: &mut super::node::NodeSql,
    t: &IndexTarget,
    (columns, index): (&[String], &str),
    r: &mut super::ValidationReport,
) -> Option<(StatsSnapshot, Option<StatsEntry>)> {
    match prepare(journal, node, t).await {
        Ok(Some(note)) => r.node_check.ok(note),
        Ok(None) => {}
        Err(why) => {
            r.node_check.ok(format!("{why}; ANALYZE skipped"));
            return None;
        }
    }
    let snap = match StatsSnapshot::take(mon, t, columns, index).await {
        Ok(s) => s,
        Err(e) => {
            r.node_check
                .ok(format!("statistics not snapshotted ({e}); ANALYZE skipped"));
            return None;
        }
    };
    match record(journal, node, t, &snap, (columns, index), super::now_ms()).await {
        Ok(e) => Some((snap, e)),
        Err(why) => {
            r.node_check.ok(format!("{why}; ANALYZE skipped"));
            None
        }
    }
}

/// After the validation put the rows back (or kept its index): its entry
/// goes. A failure is logged; the drift reconciler puts the same rows back
/// later and removes it then.
pub(crate) async fn forget(journal: Option<&dyn StatsJournal>, e: Option<&StatsEntry>) {
    if let (Some(j), Some(e)) = (journal, e)
        && let Err(err) = j.remove(e).await
    {
        tracing::warn!(node = %e.node, schema = %e.schema, table = %e.table, error = %err,
            "statistics journal entry not removed; the drift reconciler will");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn entry(node: &str, table: &str, at: u64) -> StatsEntry {
        StatsEntry {
            node: node.into(),
            schema: "t1_d1_g1_s1".into(),
            table: table.into(),
            columns: vec!["a".into()],
            index: "sprig_auto_00000001".into(),
            taken_at_ms: at,
            stats: vec![StatTable {
                table: "table_stats".into(),
                columns: vec!["db_name".into(), "table_name".into(), "cardinality".into()],
                rows: vec![vec![
                    Some("74315F64315F67315F7331".into()),
                    Some("74".into()),
                    None,
                ]],
            }],
        }
    }

    #[tokio::test]
    async fn one_entry_per_node_and_table_removed_only_while_unchanged() {
        let j = MemJournal::default();
        let n1 = NodeId::new("n1").unwrap();
        let a = entry("n1", "Orders", 1);
        assert!(j.insert(&a).await.unwrap());
        assert!(!j.insert(&entry("n1", "orders", 2)).await.unwrap());
        assert!(j.insert(&entry("n2", "orders", 3)).await.unwrap());
        assert_eq!(
            j.entry(&n1, "T1_D1_G1_S1", "ORDERS").await.unwrap(),
            Some(a.clone())
        );
        assert_eq!(j.entries(&n1).await.unwrap(), vec![a.clone()]);
        // A newer entry under the same key isn't removed by the old one.
        assert!(!j.remove(&entry("n1", "Orders", 9)).await.unwrap());
        assert!(j.remove(&a).await.unwrap());
        assert!(j.entries(&n1).await.unwrap().is_empty());
        j.set_failing(true);
        assert!(j.insert(&a).await.is_err());
    }

    #[test]
    fn entries_become_snapshots_only_when_safe() {
        let ok = entry("n1", "orders", 1);
        let s = StatsSnapshot::from_entry(&ok).unwrap();
        assert_eq!(s.rows(), 1);
        let back = s.to_entry("n1", &ok.target(), &ok.columns, &ok.index, 1);
        assert_eq!(back, ok);
        let mut bad = ok.clone();
        bad.stats[0].table = "user".into();
        assert!(StatsSnapshot::from_entry(&bad).is_err());
        let mut bad = ok.clone();
        bad.stats[0].rows[0][1] = Some("74'); DROP TABLE x; --".into());
        assert!(StatsSnapshot::from_entry(&bad).is_err());
        let mut bad = ok;
        bad.stats[0].rows[0].pop();
        assert!(StatsSnapshot::from_entry(&bad).is_err());
    }
}
