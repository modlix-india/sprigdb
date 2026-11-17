//! Dump starts a node refuses (MariaDB error 1236) because its binary logs
//! no longer hold what the position asks for.
//!
//! A position names, per GTID domain, the last transaction read. A node
//! serves a dump from the newest binary log whose starting state is at or
//! before the position in every domain; a domain the position doesn't name
//! counts as "from its first transaction". When no binary log qualifies
//! (the ones it would need are purged), the node refuses the dump.
//!
//! On a Galera node with `wsrep_gtid_mode` on, every replicated write goes
//! to the cluster's shared domain (`wsrep_gtid_domain_id`). The node's
//! other domains hold only its own local writes: statements run with
//! `wsrep_on = OFF`, or an index built by hand with
//! `wsrep_OSU_method = RSU`. They are never a tenant's changes, and their
//! numbers differ from node to node. Once the binary logs where such a
//! domain starts are purged, the node refuses every position without the
//! domain (or with it at an older number), at every reconnect, although
//! the reader never needed those events. That stalled every feed reader of
//! the cluster whose saved position predated the local writes.
//!
//! [`plan`] decides from the node's binary logs ([`NodeLogs`], read by
//! [`inspect`]):
//!
//! - a purged node-local domain is added to the position (or moved up) at
//!   its number where the binary log holding the reader's position starts,
//!   so the local events after that point are still read;
//! - a purged domain the reader needs (the shared domain, any domain on a
//!   node that isn't such a Galera node, or a domain a replica connection
//!   brings into the node) is a real loss: the start stays refused, and
//!   the error names the domain, the reader's number and the node's oldest;
//! - a refusal no purge explains (a position ahead of the node) keeps the
//!   node's own error.

use std::collections::BTreeSet;
use std::time::Duration;

use sprig_binlog::{BinlogSource, Gtid, GtidPos, SourceSession};
use sprig_common::{Result, SprigCode, SprigError};

/// How long reading a node's binary log list may take.
pub const INSPECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What a node's binary logs hold, as far as starting a dump goes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeLogs {
    /// The node (`host:port`), for messages.
    pub node: String,
    /// The cluster's shared domain (`wsrep_gtid_domain_id`) on a Galera
    /// node with `wsrep_gtid_mode` on; `None` on any other node.
    pub shared_domain: Option<u32>,
    /// Domains a replica connection applies on the node
    /// (`@@gtid_slave_pos`): never node-local.
    pub replicated: BTreeSet<u32>,
    /// Binary logs the node holds, oldest first: the file's name and the
    /// GTID state it starts at. Only the oldest and the newest ones down to
    /// the one holding the position are read ([`inspect`]).
    pub files: Vec<(String, GtidPos)>,
}

impl NodeLogs {
    fn local(&self, domain: u32) -> bool {
        self.shared_domain.is_some_and(|s| s != domain) && !self.replicated.contains(&domain)
    }
}

/// What to do about a start the node refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Start from `pos` instead: `from` with purged node-local domains
    /// added or moved up. `added` holds each such domain's new GTID and
    /// the one `from` had, if any.
    Start {
        /// The position to start from.
        pos: GtidPos,
        /// (new, old) per node-local domain changed.
        added: Vec<(Gtid, Option<Gtid>)>,
    },
    /// Events the reader needs are purged: what is lost, in words.
    Lost(String),
    /// No purge explains the refusal; the node's own error stands.
    Other,
}

/// The domains of `oldest` (the oldest binary log's starting state) whose
/// events after `from` are purged: missing from `from`, or older there.
fn purged(from: &GtidPos, oldest: &GtidPos) -> Vec<Gtid> {
    oldest
        .iter()
        .filter(|g| from.get(g.domain).is_none_or(|h| h.seq < g.seq))
        .copied()
        .collect()
}

/// Whether a binary log starting at `start` is at or before `from` in
/// every domain `from` names, leaving out `skip`.
fn holds(start: &GtidPos, from: &GtidPos, skip: &BTreeSet<u32>) -> bool {
    from.iter()
        .filter(|g| !skip.contains(&g.domain))
        .all(|g| start.get(g.domain).is_none_or(|x| x.seq <= g.seq))
}

/// What to do when `logs`'s node refused a dump from `from` (1236).
#[must_use]
pub fn plan(from: &GtidPos, logs: &NodeLogs) -> Plan {
    let Some((oldest_file, oldest)) = logs.files.first() else {
        return Plan::Other;
    };
    let gone = purged(from, oldest);
    if gone.is_empty() {
        return Plan::Other;
    }
    let needed: Vec<String> = gone
        .iter()
        .filter(|g| !logs.local(g.domain))
        .map(|g| {
            let had = from
                .get(g.domain)
                .map_or_else(|| "its first transaction".to_owned(), ToString::to_string);
            format!(
                "domain {} is needed from after {had}, but node {}'s oldest binary log ({oldest_file}) starts at {g}",
                g.domain, logs.node
            )
        })
        .collect();
    if !needed.is_empty() {
        return Plan::Lost(format!(
            "binlog events this reader needs are purged: {}; the changes in between are lost to it",
            needed.join("; ")
        ));
    }
    let skip: BTreeSet<u32> = gone.iter().map(|g| g.domain).collect();
    let base = logs
        .files
        .iter()
        .rev()
        .find(|(_, start)| holds(start, from, &skip))
        .map_or(oldest, |(_, start)| start);
    let mut pos = from.clone();
    let mut added = Vec::with_capacity(gone.len());
    for g in gone {
        let new = base
            .get(g.domain)
            .copied()
            .filter(|b| b.seq >= g.seq)
            .unwrap_or(g);
        pos.update(new);
        added.push((new, from.get(g.domain).copied()));
    }
    Plan::Start { pos, added }
}

fn first_value(rows: &[Vec<Option<String>>]) -> Option<String> {
    rows.first().and_then(|r| r.first().cloned().flatten())
}

fn on(v: Option<&str>) -> bool {
    v.is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("ON"))
}

/// The starting state of binary log `file` on the node.
async fn start_of(s: &mut SourceSession, file: &str) -> Result<GtidPos> {
    let q = format!("SELECT BINLOG_GTID_POS('{}', 4)", file.replace('\'', "''"));
    let rows = s.query(&q).await?;
    first_value(&rows).unwrap_or_default().parse()
}

/// Reads what [`plan`] needs from `src`'s node: whether it is a Galera node
/// with `wsrep_gtid_mode` (and its shared domain), the domains replicated
/// into it, the oldest binary log's starting state, and the newest ones'
/// down to the first that holds `from`. One short connection with the
/// dump's account (`BINLOG MONITOR` lists the binary logs).
///
/// # Errors
/// The node unreachable, or a statement refused.
pub async fn inspect(src: &BinlogSource, from: &GtidPos) -> Result<NodeLogs> {
    tokio::time::timeout(INSPECT_TIMEOUT, read_logs(src, from))
        .await
        .map_err(|_| {
            SprigError::new(
                SprigCode::BinlogDisconnected,
                format!(
                    "reading the binary log list of {}:{} timed out",
                    src.host, src.port
                ),
            )
        })?
}

async fn read_logs(src: &BinlogSource, from: &GtidPos) -> Result<NodeLogs> {
    let mut s = SourceSession::open(src, INSPECT_TIMEOUT).await?;
    // Without the wsrep variables (a server built without Galera), not
    // Galera.
    let wsrep = s
        .query("SELECT @@global.wsrep_on, @@global.wsrep_gtid_mode, @@global.wsrep_gtid_domain_id")
        .await
        .unwrap_or_default();
    let row = wsrep.first().cloned().unwrap_or_default();
    let col = |i: usize| row.get(i).cloned().flatten();
    let shared_domain = if on(col(0).as_deref()) && on(col(1).as_deref()) {
        col(2).and_then(|d| d.parse().ok())
    } else {
        None
    };
    let slave: GtidPos = first_value(&s.query("SELECT @@global.gtid_slave_pos").await?)
        .unwrap_or_default()
        .parse()
        .unwrap_or_default();
    let replicated = slave.iter().map(|g| g.domain).collect();
    let names: Vec<String> = s
        .query("SHOW BINARY LOGS")
        .await?
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();
    let Some(oldest_name) = names.first() else {
        return Ok(NodeLogs {
            node: format!("{}:{}", src.host, src.port),
            shared_domain,
            replicated,
            files: Vec::new(),
        });
    };
    let oldest = start_of(&mut s, oldest_name).await?;
    let skip: BTreeSet<u32> = purged(from, &oldest).iter().map(|g| g.domain).collect();
    let mut newer = Vec::new();
    for name in names.iter().skip(1).rev() {
        let start = start_of(&mut s, name).await?;
        let found = holds(&start, from, &skip);
        newer.push((name.clone(), start));
        if found {
            break;
        }
    }
    newer.reverse();
    let mut files = vec![(oldest_name.clone(), oldest)];
    files.extend(newer);
    Ok(NodeLogs {
        node: format!("{}:{}", src.host, src.port),
        shared_domain,
        replicated,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> GtidPos {
        s.parse().unwrap()
    }

    fn g(s: &str) -> Gtid {
        s.parse().unwrap()
    }

    fn galera(files: &[(&str, &str)]) -> NodeLogs {
        NodeLogs {
            node: "n1:3306".into(),
            shared_domain: Some(481),
            replicated: BTreeSet::new(),
            files: files.iter().map(|(n, s)| ((*n).to_owned(), p(s))).collect(),
        }
    }

    #[test]
    fn a_missing_local_domain_starts_where_the_readers_binary_log_starts() {
        // The oldest log already lists node-local domain 4801; the reader's
        // position (shared domain only) lies in log 3.
        let logs = galera(&[
            ("b.000002", "481-1-10,4801-4801-3"),
            ("b.000003", "481-1-20,4801-4801-5"),
            ("b.000004", "481-1-40,4801-4801-9"),
        ]);
        let Plan::Start { pos, added } = plan(&p("481-1-25"), &logs) else {
            panic!("not a start");
        };
        assert_eq!(pos, p("481-1-25,4801-4801-5"));
        assert_eq!(added, vec![(g("4801-4801-5"), None)]);
    }

    #[test]
    fn an_older_local_domain_moves_up_and_others_stay() {
        let logs = galera(&[("b.000007", "481-1-10,4801-4801-3,4802-4802-8")]);
        let from = p("481-1-12,4801-4801-1,4802-4802-9,4803-4803-2");
        let Plan::Start { pos, added } = plan(&from, &logs) else {
            panic!("not a start");
        };
        assert_eq!(pos, p("481-1-12,4801-4801-3,4802-4802-9,4803-4803-2"));
        assert_eq!(added, vec![(g("4801-4801-3"), Some(g("4801-4801-1")))]);
    }

    #[test]
    fn a_purged_shared_domain_is_a_loss() {
        let logs = galera(&[("b.000003", "481-1-20,4801-4801-5")]);
        let Plan::Lost(why) = plan(&p("481-1-12"), &logs) else {
            panic!("not a loss");
        };
        assert!(
            why.contains("domain 481 is needed from after 481-1-12"),
            "{why}"
        );
        assert!(why.contains("b.000003"), "{why}");
        assert!(why.contains("starts at 481-1-20"), "{why}");
        // Only the shared domain names the loss, not the local one.
        assert!(!why.contains("domain 4801"), "{why}");
    }

    #[test]
    fn other_topologies_and_replicated_domains_are_never_filled_in() {
        let mut logs = galera(&[("b.000003", "0-11-20,7-21-5")]);
        logs.shared_domain = None;
        assert!(
            matches!(plan(&p("0-11-25"), &logs), Plan::Lost(w) if w.contains("domain 7 is needed from after its first transaction"))
        );
        let mut logs = galera(&[("b.000003", "481-1-20,9-9-5")]);
        logs.replicated.insert(9);
        assert!(matches!(plan(&p("481-1-25"), &logs), Plan::Lost(w) if w.contains("domain 9")));
    }

    #[test]
    fn a_refusal_no_purge_explains_keeps_the_nodes_error() {
        let logs = galera(&[("b.000003", "481-1-20,4801-4801-5")]);
        // Ahead of the node, not behind it.
        assert_eq!(plan(&p("481-1-90,4801-4801-5"), &logs), Plan::Other);
        assert_eq!(plan(&p("481-1-25"), &galera(&[])), Plan::Other);
    }
}
