//! The automatic-index validators' statistics journal in the DC tier of
//! etcd ([`sprig_advisor::autoindex::validate::journal`]).
//!
//! ```text
//! /sprig/dc/<dc>/autoindex-stats/<node_id>/<schema>/<table>   (JSON, no lease)
//! insert: txn when [Missing(key)] then [Put(key)]
//! remove: get; same entry → txn when [ModRev(key, rev)] then [Delete(key)]
//! ```
//!
//! Schema and table are lowercased and percent-encoded (every byte but
//! `a-z`, `0-9`, `_`, `$` and `-`), so any name makes one key segment.
//! Only the DC leader's controller writes these keys: the replica and RSU
//! validators while they run, the drift reconciler when it puts a cut-off
//! validation's statistics back.

use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use sprig_advisor::autoindex::validate::journal::{StatsEntry, StatsJournal};
use sprig_common::ids::NodeId;
use sprig_common::{Result, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Txn};

/// [`StatsJournal`] on the DC tier (module docs).
#[derive(Clone)]
pub struct EtcdStatsJournal {
    dc: Arc<dyn MetaStore>,
}

impl fmt::Debug for EtcdStatsJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdStatsJournal")
            .field("tier", self.dc.tier())
            .finish()
    }
}

/// `name` lowercased, every byte but `a-z0-9_$-` as `%XX`.
fn segment(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for b in name.to_lowercase().bytes() {
        if b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'$' | b'-') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    if out.is_empty() {
        out.push('%');
    }
    out
}

impl EtcdStatsJournal {
    /// The journal in `dc`'s tier.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self { dc }
    }

    fn prefix(&self, node: &str) -> Result<Key> {
        self.dc.tier().key(&format!("autoindex-stats/{node}/"))
    }

    fn key(&self, node: &str, schema: &str, table: &str) -> Result<Key> {
        self.dc.tier().key(&format!(
            "autoindex-stats/{node}/{}/{}",
            segment(schema),
            segment(table)
        ))
    }

    fn decode(value: &[u8]) -> Result<StatsEntry> {
        serde_json::from_slice(value).map_err(|e| {
            SprigError::internal(format!("a statistics journal entry doesn't parse: {e}"))
        })
    }
}

#[async_trait::async_trait]
impl StatsJournal for EtcdStatsJournal {
    async fn insert(&self, e: &StatsEntry) -> Result<bool> {
        let key = self.key(&e.node, &e.schema, &e.table)?;
        let value = serde_json::to_vec(e)
            .map_err(|err| SprigError::internal(format!("statistics entry: {err}")))?;
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded)
    }

    async fn entry(&self, node: &NodeId, schema: &str, table: &str) -> Result<Option<StatsEntry>> {
        let key = self.key(node.as_str(), schema, table)?;
        match self.dc.get(&key, Consistency::Linearizable).await? {
            Some(kv) => Ok(Some(Self::decode(&kv.value)?)),
            None => Ok(None),
        }
    }

    async fn entries(&self, node: &NodeId) -> Result<Vec<StatsEntry>> {
        let prefix = self.prefix(node.as_str())?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                match Self::decode(&kv.value) {
                    Ok(e) => out.push(e),
                    // Skipped, not fatal: one bad entry mustn't stop the
                    // others from being put back.
                    Err(err) => tracing::warn!(key = %kv.key, error = %err,
                        "statistics journal entry skipped"),
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    async fn remove(&self, e: &StatsEntry) -> Result<bool> {
        let key = self.key(&e.node, &e.schema, &e.table)?;
        let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await? else {
            return Ok(false);
        };
        if Self::decode(&kv.value).ok().as_ref() != Some(e) {
            return Ok(false);
        }
        let r = self
            .dc
            .txn(Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![Op::Delete(key)],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(r.succeeded)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_advisor::autoindex::validate::journal::StatTable;
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    use super::*;

    fn entry(node: &str, schema: &str, table: &str, at: u64) -> StatsEntry {
        StatsEntry {
            node: node.into(),
            schema: schema.into(),
            table: table.into(),
            columns: vec!["a".into()],
            index: "sprig_auto_00000001".into(),
            taken_at_ms: at,
            stats: vec![StatTable {
                table: "table_stats".into(),
                columns: vec!["db_name".into(), "table_name".into(), "cardinality".into()],
                rows: vec![vec![
                    Some("6162".into()),
                    Some("74".into()),
                    Some("3130".into()),
                ]],
            }],
        }
    }

    #[test]
    fn names_make_one_key_segment() {
        assert_eq!(segment("t42_d1_g3_s17"), "t42_d1_g3_s17");
        assert_eq!(segment("Order Lines/2"), "order%20lines%2F2");
        assert_eq!(segment("ü"), "%C3%BC");
        assert_eq!(segment(""), "%");
    }

    #[tokio::test]
    async fn entries_per_node_and_table_in_the_dc_tier() {
        let tier = Tier::Dc(DcId::new("mum1").unwrap());
        let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier.clone()));
        let j = EtcdStatsJournal::new(Arc::clone(&store));
        let n1 = NodeId::new("mum1-c7-n2").unwrap();
        let a = entry("mum1-c7-n2", "t1_d1_g1_s1", "Order Lines", 1);
        assert!(j.insert(&a).await.unwrap());
        // One per node and table, whatever the case.
        assert!(
            !j.insert(&entry("mum1-c7-n2", "T1_D1_G1_S1", "order lines", 2))
                .await
                .unwrap()
        );
        let b = entry("mum1-c7-n2", "t1_d1_g1_s1", "orders", 3);
        assert!(j.insert(&b).await.unwrap());
        assert!(
            j.insert(&entry("mum1-c7-n3", "t1_d1_g1_s1", "orders", 4))
                .await
                .unwrap()
        );
        let kv = store
            .get(
                &tier
                    .key("autoindex-stats/mum1-c7-n2/t1_d1_g1_s1/order%20lines")
                    .unwrap(),
                Consistency::Linearizable,
            )
            .await
            .unwrap();
        assert!(kv.is_some());
        assert_eq!(
            j.entry(&n1, "t1_d1_g1_s1", "ORDER LINES").await.unwrap(),
            Some(a.clone())
        );
        let mut all = j.entries(&n1).await.unwrap();
        all.sort_by_key(|e| e.taken_at_ms);
        assert_eq!(all, vec![a.clone(), b.clone()]);
        // Only the entry read is removed, never a newer one.
        assert!(
            !j.remove(&entry("mum1-c7-n2", "t1_d1_g1_s1", "orders", 99))
                .await
                .unwrap()
        );
        assert!(j.remove(&b).await.unwrap());
        assert!(!j.remove(&b).await.unwrap());
        assert_eq!(j.entries(&n1).await.unwrap(), vec![a]);
    }
}
