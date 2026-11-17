//! Waiting for every gateway to load a lookup state change: the job raises
//! the tenant's catalog version, then asks the gateways of the tenant's DC
//! to acknowledge it through the DC tier, the same registration and version
//! keys the gateways keep for the MongoDB publication wait
//! (`sprig-gateway::mongo::publish`):
//!
//! ```text
//! doc-gw/<gateway>              a gateway's presence, under its lease
//! doc-publish/<tenant>          {version}: "load at least this" (the asker's lease)
//! doc-acks/<tenant>/<gateway>   {version}: what that gateway holds (its lease)
//!
//! wait_for_gateways(tenant, v, timeout):
//!   put doc-publish/<tenant> = v under a lease of the job's own
//!   until every gateway in doc-gw/ has an ack >= v: poll; the request is
//!     written again every REPUT (a newer request of another writer with a
//!     lower version may have replaced it)
//!   past `timeout` (lookup.publish_timeout_ms): each gateway still missing
//!     is fenced: its lease is revoked, which removes its presence and
//!     acks; then the job waits ACK_LEASE_TTL more, so a fenced gateway's
//!     own view of its lease has run out too. A gateway whose lease ran
//!     out (paused, partitioned) reloads each tenant's catalog from the
//!     writer before its next statement of that tenant.
//! ```
//!
//! A gateway that is paused (SIGSTOP) stops renewing; its presence goes
//! when its lease ends, so the wait ends then at the latest, and on
//! resuming the gateway finds its lease gone and reloads first.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_meta::{Consistency, Key, ListOpts, MetaStore, Op, Record, Txn, Versioned};

/// The gateways' presence and acknowledgement lease (`doc-gw/`): a gateway
/// that misses its renewals for this long is gone, and it treats its own
/// registration as lapsed after this long without a renewal.
pub const ACK_LEASE_TTL: Duration = Duration::from_secs(10);

/// How often the waiter looks at the acknowledgements.
const POLL: Duration = Duration::from_millis(10);
/// How often the request is written again while gateways are missing.
const REPUT: Duration = Duration::from_millis(500);

/// The value of `doc-publish/<tenant>` and `doc-acks/<tenant>/<gateway>`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct VersionRec {
    version: u64,
}

impl Versioned for VersionRec {
    const V: u32 = 1;
}

/// A version as the keys hold it.
#[must_use]
pub fn encode(version: u64) -> Vec<u8> {
    Record::new(VersionRec { version })
        .encode()
        .unwrap_or_default()
}

/// The version a key holds.
#[must_use]
pub fn decode(b: &[u8]) -> Option<u64> {
    Record::<VersionRec>::decode(b).ok().map(|r| r.body.version)
}

/// The last path segment of `key` after `prefix`.
fn after<'a>(key: &'a Key, prefix: &str) -> Option<&'a str> {
    let s = key.as_str();
    Some(&s[s.find(prefix)? + prefix.len()..])
}

/// What a wait did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AckOutcome {
    /// Gateways present at the start.
    pub gateways: usize,
    /// How long it took, fencing included.
    pub waited: Duration,
    /// Gateways fenced at the timeout (their leases revoked).
    pub fenced: Vec<String>,
}

/// The gateways present (`doc-gw/`) without an acknowledgement of
/// `version` for `tenant`, with their leases.
async fn missing(
    store: &dyn MetaStore,
    tenant: TenantId,
    version: u64,
) -> Result<(usize, HashMap<String, Option<sprig_meta::LeaseId>>)> {
    let gws = store.tier().key("doc-gw/")?;
    let acks = store.tier().key(&format!("doc-acks/{}/", tenant.0))?;
    let live: HashMap<String, Option<sprig_meta::LeaseId>> = store
        .list(&gws, ListOpts::default())
        .await?
        .kvs
        .into_iter()
        .filter_map(|kv| Some((after(&kv.key, "doc-gw/")?.to_owned(), kv.lease)))
        .collect();
    let prefix = format!("doc-acks/{}/", tenant.0);
    let held: HashMap<String, u64> = store
        .list(&acks, ListOpts::default())
        .await?
        .kvs
        .iter()
        .filter_map(|kv| Some((after(&kv.key, &prefix)?.to_owned(), decode(&kv.value)?)))
        .collect();
    let n = live.len();
    Ok((
        n,
        live.into_iter()
            .filter(|(g, _)| held.get(g).is_none_or(|v| *v < version))
            .collect(),
    ))
}

async fn put(
    store: &dyn MetaStore,
    key: Key,
    version: u64,
    lease: sprig_meta::LeaseId,
) -> Result<()> {
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: encode(version),
                lease: Some(lease),
            }],
            otherwise: Vec::new(),
        })
        .await
        .map(|_| ())
}

/// Waits until every gateway of `store`'s DC holds `tenant`'s catalog at
/// `version` or newer (module docs); past `timeout`, fences the gateways
/// still missing and waits out their lease.
///
/// # Errors
/// The store's, when the request can't be written at all (the caller
/// falls back to a fixed wait).
pub async fn wait_for_gateways(
    store: &Arc<dyn MetaStore>,
    tenant: TenantId,
    version: u64,
    timeout: Duration,
) -> Result<AckOutcome> {
    let started = Instant::now();
    let deadline = started + timeout;
    let req = store.tier().key(&format!("doc-publish/{}", tenant.0))?;
    let lease = store
        .grant_lease(timeout + ACK_LEASE_TTL + Duration::from_secs(10))
        .await?;
    put(store.as_ref(), req.clone(), version, lease).await?;
    let mut last_put = Instant::now();
    let mut out = AckOutcome::default();
    let mut first = true;
    let result = loop {
        let (n, mut waiting) = match missing(store.as_ref(), tenant, version).await {
            Ok(m) => m,
            Err(e) if Instant::now() < deadline => {
                tracing::debug!(%tenant, error = %e, "lookup publish: acknowledgements unreadable");
                tokio::time::sleep(POLL).await;
                continue;
            }
            Err(e) => break Err(e),
        };
        if first {
            out.gateways = n;
            first = false;
        }
        if waiting.is_empty() {
            break Ok(());
        }
        if Instant::now() >= deadline {
            // Fence the laggards: their leases go, with their presence
            // and acks; then their own view of the lease runs out.
            let mut names: Vec<String> = waiting.keys().cloned().collect();
            names.sort();
            let mut revoked = HashSet::new();
            for (g, l) in waiting.drain() {
                if let Some(l) = l
                    && revoked.insert(l)
                    && let Err(e) = store.revoke_lease(l).await
                {
                    tracing::warn!(%tenant, gateway = %g, error = %e, "lookup publish: a laggard's lease wasn't revoked");
                }
            }
            tracing::warn!(%tenant, version, gateways = ?names,
                "lookup publish: gateways didn't acknowledge in time; fenced");
            metrics::counter!("sprig_lookup_publish_fenced_total").increment(names.len() as u64);
            out.fenced = names;
            tokio::time::sleep(ACK_LEASE_TTL).await;
            break Ok(());
        }
        if last_put.elapsed() >= REPUT {
            let _ = put(store.as_ref(), req.clone(), version, lease).await;
            last_put = Instant::now();
        }
        tokio::time::sleep(POLL).await;
    };
    let _ = store.revoke_lease(lease).await;
    result?;
    out.waited = started.elapsed();
    metrics::histogram!("sprig_lookup_publish_seconds").record(out.waited.as_secs_f64());
    tracing::debug!(%tenant, version, gateways = out.gateways, waited_ms = out.waited.as_millis(),
        "lookup publish acknowledged");
    Ok(out)
}

/// Whether a key read of `doc-gw/<gateway>` finds it (tests).
///
/// # Errors
/// The store's.
pub async fn present(store: &dyn MetaStore, gateway: &str) -> Result<bool> {
    let k = store.tier().key(&format!("doc-gw/{gateway}"))?;
    Ok(store.get(&k, Consistency::Linearizable).await?.is_some())
}

impl AckOutcome {
    /// A fenced wait as an error text (for logs).
    #[must_use]
    pub fn fenced_text(&self) -> Option<String> {
        (!self.fenced.is_empty()).then(|| format!("fenced {}", self.fenced.join(", ")))
    }
}

/// An error for a wait that couldn't run.
#[must_use]
pub fn unavailable(why: &str) -> SprigError {
    SprigError::internal(format!("lookup publish: {why}"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_meta::Tier;
    use sprig_meta::mem::MemStore;

    use super::*;

    fn store() -> Arc<dyn MetaStore> {
        Arc::new(MemStore::new(Tier::Dc(
            sprig_common::ids::DcId::new("dc-a").unwrap(),
        )))
    }

    async fn gateway(s: &Arc<dyn MetaStore>, name: &str) -> sprig_meta::LeaseId {
        let l = s.grant_lease(ACK_LEASE_TTL).await.unwrap();
        let k = s.tier().key(&format!("doc-gw/{name}")).unwrap();
        put(s.as_ref(), k, 0, l).await.unwrap();
        l
    }

    async fn ack(s: &Arc<dyn MetaStore>, name: &str, l: sprig_meta::LeaseId, v: u64) {
        let k = s.tier().key(&format!("doc-acks/7/{name}")).unwrap();
        put(s.as_ref(), k, v, l).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::many_single_char_names)] // two gateways, their leases, a tenant
    async fn waits_for_every_gateway_then_fences_laggards() {
        let s = store();
        let t = TenantId(7);
        let a = gateway(&s, "gw-a").await;
        let b = gateway(&s, "gw-b").await;
        // Both acknowledge: done at once.
        ack(&s, "gw-a", a, 5).await;
        ack(&s, "gw-b", b, 6).await;
        let o = wait_for_gateways(&s, t, 5, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!((o.gateways, o.fenced.len()), (2, 0));
        // gw-b answers a request later; gw-a never does: fenced at the
        // timeout, then its lease's time waited out.
        let s2 = Arc::clone(&s);
        let late = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            ack(&s2, "gw-b", b, 9).await;
        });
        let o = wait_for_gateways(&s, t, 9, Duration::from_secs(3))
            .await
            .unwrap();
        late.await.unwrap();
        assert_eq!(o.fenced, ["gw-a"]);
        assert!(
            o.waited >= Duration::from_secs(3) + ACK_LEASE_TTL,
            "{:?}",
            o.waited
        );
        assert!(
            !present(s.as_ref(), "gw-a").await.unwrap(),
            "its presence went with its lease"
        );
        assert!(present(s.as_ref(), "gw-b").await.unwrap());
        // No gateway at all: nothing to wait for.
        let empty = store();
        let o = wait_for_gateways(&empty, t, 1, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            o,
            AckOutcome {
                waited: o.waited,
                ..AckOutcome::default()
            }
        );
    }
}
