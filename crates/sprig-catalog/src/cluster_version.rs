//! The DC's cluster version: the oldest binary version every node is
//! known to run, raised only once all of them run it.
//!
//! ```text
//! /sprig/dc/<dc_id>/cluster-version   {version, set_ms, set_by}
//! ```
//!
//! During a rolling upgrade versions N and N-1 run side by side; a new log
//! command, snapshot format or record version is used only once the
//! cluster version reaches the version that introduced it, so until the
//! flip any node can still go back to N-1. After the flip a node whose
//! binary is older than the cluster version refuses to start: it could not
//! read what the others now write.

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, MetaStore, Op, Record, Versioned};

use crate::units::{self, NODE_REMOVED};

/// The record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterVersion {
    /// The version every node runs at least.
    pub version: String,
    /// When it was set (Unix milliseconds).
    #[serde(default)]
    pub set_ms: u64,
    /// Who set it.
    #[serde(default)]
    pub set_by: String,
}

impl Versioned for ClusterVersion {
    const V: u32 = 1;
}

/// Orders two version strings: numeric dot-separated parts first
/// (`0.10.0` > `0.9.3`), a version with a pre-release suffix (`-rc1`)
/// before the same version without one, then the suffixes as text.
#[must_use]
pub fn compare(a: &str, b: &str) -> std::cmp::Ordering {
    fn split(v: &str) -> (Vec<u64>, Option<&str>) {
        let v = v.trim().trim_start_matches('v');
        let (core, pre) = match v.split_once('-') {
            Some((c, p)) => (c, Some(p)),
            None => (v, None),
        };
        let nums = core
            .split('.')
            .map(|p| p.parse::<u64>().unwrap_or(0))
            .collect();
        (nums, pre)
    }
    let (na, pa) = split(a);
    let (nb, pb) = split(b);
    let len = na.len().max(nb.len());
    for i in 0..len {
        let (x, y) = (
            na.get(i).copied().unwrap_or(0),
            nb.get(i).copied().unwrap_or(0),
        );
        if x != y {
            return x.cmp(&y);
        }
    }
    match (pa, pb) {
        (None, None) => std::cmp::Ordering::Equal,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(y),
    }
}

fn key(store: &dyn MetaStore) -> Result<sprig_meta::Key> {
    store.tier().key("cluster-version")
}

/// The DC's cluster version, `None` before the first finalize.
///
/// # Errors
/// The store's.
pub async fn read(store: &dyn MetaStore) -> Result<Option<ClusterVersion>> {
    match store.get(&key(store)?, Consistency::Linearizable).await? {
        Some(kv) => Ok(Some(Record::<ClusterVersion>::decode(&kv.value)?.body)),
        None => Ok(None),
    }
}

/// Whether a node at `version` may start under `cluster`: not older.
#[must_use]
pub fn may_start(version: &str, cluster: Option<&ClusterVersion>) -> bool {
    cluster.is_none_or(|c| compare(version, &c.version) != std::cmp::Ordering::Less)
}

/// Raises the cluster version to `target` once every node that isn't
/// removed reports at least `target` in its registry record. Never lowers
/// it. Returns the record stored.
///
/// # Errors
/// 39XXX (invalid) naming the nodes still older; the store's.
pub async fn finalize(store: &dyn MetaStore, target: &str, by: &str) -> Result<ClusterVersion> {
    let behind: Vec<String> = units::nodes(store)
        .await?
        .into_iter()
        .filter(|n| n.state != NODE_REMOVED)
        .filter(|n| compare(&n.version, target) == std::cmp::Ordering::Less)
        .map(|n| format!("node {} at {:?} ({})", n.id, n.version, n.state))
        .collect();
    if !behind.is_empty() {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!(
                "the cluster version can't reach {target}: {} still older",
                behind.join(", ")
            ),
        ));
    }
    let k = key(store)?;
    for _ in 0..20 {
        let old = store.get(&k, Consistency::Linearizable).await?;
        let guard = match &old {
            Some(kv) => {
                let cur = Record::<ClusterVersion>::decode(&kv.value)?.body;
                if compare(&cur.version, target) != std::cmp::Ordering::Less {
                    return Ok(cur);
                }
                Cmp::ModRev(k.clone(), kv.mod_revision)
            }
            None => Cmp::Missing(k.clone()),
        };
        let rec = ClusterVersion {
            version: target.to_owned(),
            set_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            set_by: by.to_owned(),
        };
        let r = store
            .txn(sprig_meta::Txn {
                when: vec![guard],
                then: vec![Op::Put {
                    key: k.clone(),
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(rec);
        }
    }
    Err(SprigError::new(
        SprigCode::MetaUnavailable,
        "the cluster version kept changing while it was raised",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::units::{NODE_ACTIVE, NodeRecord};
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};
    use std::cmp::Ordering;

    #[test]
    fn versions_order_numerically_with_pre_releases_first() {
        assert_eq!(compare("0.10.0", "0.9.3"), Ordering::Greater);
        assert_eq!(compare("0.9.3", "0.9.3"), Ordering::Equal);
        assert_eq!(compare("1.0.0-rc1", "1.0.0"), Ordering::Less);
        assert_eq!(compare("1.0.0-rc2", "1.0.0-rc1"), Ordering::Greater);
        assert_eq!(compare("1.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare("v2.0.0", "1.9.9"), Ordering::Greater);
    }

    fn node(id: u64, version: &str, state: &str) -> NodeRecord {
        NodeRecord {
            id,
            gateway_id: format!("gw-{id}"),
            addr: "127.0.0.1".into(),
            raft_addr: String::new(),
            failure_domain: String::new(),
            placement: true,
            state: state.into(),
            version: version.into(),
            since_ms: 0,
            reason: String::new(),
        }
    }

    #[tokio::test]
    async fn the_version_flips_only_when_every_node_runs_it_and_never_goes_back() {
        let s = MemStore::new(Tier::Dc(DcId::new("dc-v").unwrap()));
        assert_eq!(read(&s).await.unwrap(), None);
        assert!(may_start("0.0.1", None));
        for n in [
            node(1, "0.2.0", NODE_ACTIVE),
            node(2, "0.1.0", NODE_ACTIVE),
            node(3, "0.1.0", NODE_REMOVED),
        ] {
            units::register_node(&s, &n).await.unwrap();
        }
        let e = finalize(&s, "0.2.0", "test").await.unwrap_err();
        assert!(e.message.contains("node 2"), "{}", e.message);
        units::register_node(&s, &node(2, "0.2.0", NODE_ACTIVE))
            .await
            .unwrap();
        let v = finalize(&s, "0.2.0", "test").await.unwrap();
        assert_eq!(v.version, "0.2.0");
        // Never lowered.
        assert_eq!(
            finalize(&s, "0.1.0", "test").await.unwrap().version,
            "0.2.0"
        );
        let cur = read(&s).await.unwrap();
        assert!(!may_start("0.1.0", cur.as_ref()));
        assert!(may_start("0.2.0", cur.as_ref()));
        assert!(may_start("0.3.0", cur.as_ref()));
    }
}
