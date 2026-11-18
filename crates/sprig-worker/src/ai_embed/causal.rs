//! Causal reads for the AI writers (embedded columns and retrieval
//! indexes): the read of the rows a feed round names never returns a
//! version older than the round's events.
//!
//! The feed reads one node's binary log, and the writers read the rows
//! again on the cluster's writer (or its relay writer), which can be
//! another node. A row read at an older version looks done (its stored
//! hash is that version's), so the round would be acknowledged past a
//! change that is never embedded. So, before that read:
//!
//! - **Galera:** a barrier on the reading connection,
//!   `SET STATEMENT wsrep_sync_wait = 1 FOR SELECT 1`. It returns once the
//!   node has committed every write set the cluster had ordered when it was
//!   sent: every event the feed has delivered was ordered before (another
//!   node committed and logged it), so whatever the connection reads after
//!   it is at least that new.
//! - **One node, a primary with replicas, a managed service:** the feed
//!   reads the writer itself, but a transaction is in the binary log a
//!   moment before its `InnoDB` commit shows to a plain read. The read is a
//!   locking one (`LOCK IN SHARE MODE`): it waits for the row locks the
//!   committing transaction still holds, then reads the newest committed
//!   version.
//!
//! The barrier is bounded ([`WAIT`]; Galera's own causal read timeout
//! answers 1205 first by default): past it the round fails retryably and
//! is read again; nothing is acknowledged. A locking read waits at most
//! `innodb_lock_wait_timeout` (1205, retried the same way).

use std::collections::HashMap;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use sprig_cluster::record::TopologyKind;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};

/// How a cluster's rows are read causally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reads {
    /// Galera: [`BARRIER`] on the connection, then plain reads.
    Barrier,
    /// Elsewhere: locking reads ([`locking`]).
    Locking,
}

/// The Galera barrier: a causal `SELECT 1`.
pub(crate) const BARRIER: &str = "SET STATEMENT wsrep_sync_wait = 1 FOR SELECT 1";

/// The longest the barrier is waited for.
pub(crate) const WAIT: Duration = Duration::from_secs(30);

/// How a cluster of `topology` is read causally; `galera` when its
/// published profile has Galera facts (a managed Galera service).
#[must_use]
pub(crate) fn reads_of(topology: TopologyKind, galera: bool) -> Reads {
    if topology == TopologyKind::Galera || galera {
        Reads::Barrier
    } else {
        Reads::Locking
    }
}

/// `sql` (a `SELECT` without a locking clause) as a locking read.
#[must_use]
pub(crate) fn locking(sql: &str) -> String {
    format!("{sql} LOCK IN SHARE MODE")
}

/// The barrier on `c`, bounded by [`WAIT`].
///
/// # Errors
/// Retryable: the barrier's own error (1205 when Galera's causal read
/// timeout passes, 1047 while the node isn't synced), or [`WAIT`] passed.
pub(crate) async fn barrier(c: &mut mysql_async::Conn) -> Result<()> {
    let Ok(r) = tokio::time::timeout(WAIT, c.query_drop(BARRIER)).await else {
        let mut e = SprigError::new(
            SprigCode::BackendUnavailable,
            format!(
                "embedding: the node didn't catch up with the cluster within {} s",
                WAIT.as_secs()
            ),
        );
        e.retryable = true;
        return Err(e);
    };
    r.map_err(super::backend)
}

/// `sql` (a `SELECT` to run on `c`) made causal for a cluster read as
/// `reads`: the barrier is run on `c` first, or the read becomes a
/// locking one.
///
/// # Errors
/// As [`barrier`].
pub(crate) async fn read_sql(reads: Reads, c: &mut mysql_async::Conn, sql: &str) -> Result<String> {
    match reads {
        Reads::Barrier => {
            barrier(c).await?;
            Ok(sql.to_owned())
        }
        Reads::Locking => Ok(locking(sql)),
    }
}

/// Each cluster's [`Reads`], read from the registry once per job (a
/// cluster's topology doesn't change).
#[derive(Debug, Default)]
pub(crate) struct Kinds(HashMap<ClusterId, Reads>);

impl Kinds {
    /// How `cluster` is read causally.
    ///
    /// # Errors
    /// The registry unreadable; a retryable error when the cluster isn't
    /// registered (a move's source dropped meanwhile).
    pub(crate) async fn of(
        &mut self,
        registry: &ClusterRegistry,
        cluster: &ClusterId,
    ) -> Result<Reads> {
        if let Some(r) = self.0.get(cluster) {
            return Ok(*r);
        }
        let rec = registry.get(cluster).await?.ok_or_else(|| {
            let mut e =
                SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"));
            e.retryable = true;
            e
        })?;
        let body = rec.body();
        let r = reads_of(
            body.topology,
            body.profile.as_ref().is_some_and(|p| p.galera.is_some()),
        );
        self.0.insert(cluster.clone(), r);
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn galera_waits_for_the_cluster_and_the_rest_lock() {
        assert_eq!(reads_of(TopologyKind::Galera, false), Reads::Barrier);
        assert_eq!(reads_of(TopologyKind::Galera, true), Reads::Barrier);
        // A managed service whose profile shows Galera.
        assert_eq!(reads_of(TopologyKind::External, true), Reads::Barrier);
        assert_eq!(reads_of(TopologyKind::External, false), Reads::Locking);
        assert_eq!(reads_of(TopologyKind::SingleNode, false), Reads::Locking);
        assert_eq!(
            reads_of(TopologyKind::PrimaryReplica, false),
            Reads::Locking
        );
    }

    #[test]
    fn the_causal_statements() {
        assert_eq!(
            locking("SELECT `id` FROM `s`.`t` WHERE (`id`) IN ((1))"),
            "SELECT `id` FROM `s`.`t` WHERE (`id`) IN ((1)) LOCK IN SHARE MODE"
        );
        assert_eq!(
            locking("SELECT `id` FROM `s`.`t` WHERE TRUE ORDER BY `id` LIMIT 5"),
            "SELECT `id` FROM `s`.`t` WHERE TRUE ORDER BY `id` LIMIT 5 LOCK IN SHARE MODE"
        );
        // Per statement: the session's own wsrep_sync_wait is left alone.
        assert!(BARRIER.starts_with("SET STATEMENT wsrep_sync_wait = 1 FOR "));
    }
}
