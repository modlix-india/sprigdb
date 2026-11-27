//! The DDL barrier across nodes (the two-version rule): on one replica of
//! several, a catalog change allocates version `v + 1` only once every
//! gateway of the DC holds the tenant's catalog at `v` or newer, so at
//! most two adjacent catalog versions are ever in use.
//!
//! Gateways acknowledge versions through the DC tier keys they already
//! keep (`doc-gw/<gateway>` presence, `doc-publish/<tenant>` request,
//! `doc-acks/<tenant>/<gateway>`; [`crate::mongo::publish`]): a gateway
//! that caches the tenant loads it to the version asked for, one that
//! doesn't acknowledges at once (its first use loads the newest).
//!
//! ```text
//! hold(tenant, v), under the tenant's catalog version lock:
//!   every gateway in doc-gw/ has doc-acks/<tenant>/<gw> >= v: go on
//!   else wait_for_gateways(tenant, v, TIMEOUT): ask, poll; past TIMEOUT
//!     the gateways still missing are fenced (their lease revoked) and the
//!     wait goes on until their own view of the lease has run out; a
//!     fenced gateway reloads every tenant's catalog before its next
//!     statement
//! ```

use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::version::VersionBarrier;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore};

/// How long the gateways get before the ones missing are fenced.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// The barrier over the DC tier's acknowledgement keys.
pub struct NodeBarrier {
    dc: Arc<dyn MetaStore>,
    timeout: Duration,
}

impl std::fmt::Debug for NodeBarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBarrier")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl NodeBarrier {
    /// A barrier over `dc`, fencing after [`TIMEOUT`].
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>) -> Self {
        Self {
            dc,
            timeout: TIMEOUT,
        }
    }

    /// The same barrier fencing after `timeout` (tests).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Whether every present gateway already acknowledged `version`.
    async fn all_hold(&self, tenant: TenantId, version: u64) -> Result<bool> {
        let tier = self.dc.tier();
        let gws = tier.key("doc-gw/")?;
        let acks_prefix = format!("doc-acks/{}/", tenant.0);
        let acks = tier.key(&acks_prefix)?;
        let live = self.dc.list(&gws, ListOpts::default()).await?;
        let held = self.dc.list(&acks, ListOpts::default()).await?;
        let acked = |gw: &str| {
            held.kvs.iter().any(|kv| {
                kv.key.as_str().ends_with(&format!("{acks_prefix}{gw}"))
                    && sprig_shard::lookup::publish::decode(&kv.value).is_some_and(|v| v >= version)
            })
        };
        Ok(live.kvs.iter().all(|kv| {
            let k = kv.key.as_str();
            k.rfind("doc-gw/")
                .map(|at| &k[at + "doc-gw/".len()..])
                .is_some_and(acked)
        }))
    }
}

#[async_trait::async_trait]
impl VersionBarrier for NodeBarrier {
    async fn hold(&self, tenant: TenantId, version: u64) -> Result<()> {
        if version == 0 || self.all_hold(tenant, version).await.unwrap_or(false) {
            return Ok(());
        }
        let out = sprig_shard::lookup::publish::wait_for_gateways(&self.dc, tenant, version, self.timeout)
            .await
            .map_err(|e| {
                SprigError::new(
                    SprigCode::CatalogUnavailable,
                    format!(
                        "catalog unavailable: the gateways' catalog versions of tenant {tenant} can't be checked: {}",
                        e.message
                    ),
                )
            })?;
        metrics::histogram!("sprig_catalog_barrier_seconds").record(out.waited.as_secs_f64());
        if !out.fenced.is_empty() {
            tracing::warn!(%tenant, version, fenced = ?out.fenced,
                "DDL barrier: gateways that didn't load the tenant's catalog in time were fenced");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Op, Tier, Txn};

    async fn put(s: &dyn MetaStore, rel: &str, v: u64) {
        let key = s.tier().key(rel).unwrap();
        s.txn(Txn {
            then: vec![Op::Put {
                key,
                value: sprig_shard::lookup::publish::encode(v),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn it_goes_on_when_every_gateway_holds_the_version_and_waits_otherwise() {
        let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-b").unwrap())));
        let b = NodeBarrier::new(Arc::clone(&s)).with_timeout(Duration::from_millis(50));
        let t = TenantId(9);
        // No gateway present: nothing to wait for.
        b.hold(t, 3).await.unwrap();
        put(&*s, "doc-gw/gw1", 0).await;
        put(&*s, "doc-gw/gw2", 0).await;
        put(&*s, "doc-acks/9/gw1", 3).await;
        put(&*s, "doc-acks/9/gw2", 2).await;
        assert!(!b.all_hold(t, 3).await.unwrap());
        assert!(b.all_hold(t, 2).await.unwrap());
        put(&*s, "doc-acks/9/gw2", 4).await;
        assert!(b.all_hold(t, 3).await.unwrap());
        assert!(!b.all_hold(TenantId(10), 1).await.unwrap());
        b.hold(t, 3).await.unwrap();
    }
}
