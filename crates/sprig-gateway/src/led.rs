//! The tenants a background runner of this node acts for: every tenant
//! whose engine unit this node leads, whether or not a client of the
//! tenant ever reached this process.
//!
//! ```text
//! Led::new(services): one per runner; its catalog load feed subscribed
//! changed(): awaits the next catalog load here (in the runner's select!):
//!   a tenant's load marks it to be looked at again; a lagging feed
//!   (Lagged) marks every tenant
//! catalogs(services, wants): each tick
//!   the led tenants: on a replicated node, the units its engine leads now
//!   (node_engine::led_tenants); on an engine that doesn't replicate, every
//!   active tenant of this data center in the directory (listed again every
//!   30 s), since every unit is led here
//!   a tenant the runner wanted last time: its catalog (loaded on demand,
//!   as fresh as the directory), and whether `wants` still holds
//!   one not looked at yet, or marked: the same
//!   one looked at and not wanted: every 30 s its committed catalog version
//!   is read from the records (one point read); a version other than the
//!   one looked at is looked at again
//!   returns the catalogs `wants` holds for
//! ```
//!
//! A runner that only followed the load feed acted for nothing after a
//! leadership move to a node no client of the tenant reaches, after the
//! tenant's catalog left the cache, or after a lag of the feed; this
//! keeps it acting for every unit led here. Catalogs of tenants a runner
//! has nothing for aren't kept: the cache drops them as usual.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_catalog::{CatalogLoaded, Freshness, TenantCatalog};
use sprig_common::Result;
use sprig_common::ids::TenantId;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::tenant::TenantServices;

/// How often a tenant a runner had nothing for is checked again (its
/// catalog version), and how often an engine that doesn't replicate lists
/// the directory again.
const RECHECK: Duration = Duration::from_secs(30);

/// Directory entries read per page.
const PAGE: usize = 500;

/// What a runner knows of one led tenant.
#[derive(Debug, Clone, Copy)]
struct Seen {
    /// The catalog version last looked at.
    version: u64,
    /// The runner wanted it then.
    wanted: bool,
    /// When it was last looked at or checked; `None`: look again now.
    checked: Option<Instant>,
}

/// One runner's view of the tenants led here.
pub(crate) struct Led {
    loads: broadcast::Receiver<CatalogLoaded>,
    seen: HashMap<u64, Seen>,
    /// The directory's tenants (an engine that doesn't replicate) and when
    /// they were listed.
    listed: Option<(Instant, Vec<u64>)>,
    /// Every tenant is to be looked at again (the feed lagged).
    all_dirty: bool,
}

impl Led {
    /// A view following `svc`'s catalog loads from now on.
    pub(crate) fn new(svc: &TenantServices) -> Self {
        Self {
            loads: svc.cache.loads(),
            seen: HashMap::new(),
            listed: None,
            all_dirty: false,
        }
    }

    /// Waits for the next catalog load here and marks its tenant
    /// (returned); a feed that lagged marks every tenant (`None`: some
    /// loads were missed). Cancel-safe. Never returns once the cache is
    /// gone.
    pub(crate) async fn changed(&mut self) -> Option<u64> {
        match self.loads.recv().await {
            Ok(l) => {
                if let Some(s) = self.seen.get_mut(&l.tenant.0) {
                    s.checked = None;
                }
                Some(l.tenant.0)
            }
            Err(RecvError::Lagged(n)) => {
                tracing::debug!(
                    missed = n,
                    "catalog load feed lagged: every led tenant is looked at again"
                );
                self.all_dirty = true;
                None
            }
            Err(RecvError::Closed) => std::future::pending().await,
        }
    }

    /// Whether the feed lagged since the last call (the runner should look
    /// at everything again); clears the mark for runners that don't use
    /// [`catalogs`](Self::catalogs).
    pub(crate) fn take_lagged(&mut self) -> bool {
        std::mem::take(&mut self.all_dirty)
    }

    /// The tenants led here now, in id order.
    pub(crate) async fn tenants(&mut self, svc: &TenantServices) -> Vec<u64> {
        if let Some(led) = crate::node_engine::led_tenants() {
            return led;
        }
        if let Some((at, list)) = &self.listed
            && at.elapsed() < RECHECK
        {
            return list.clone();
        }
        match directory_tenants(svc).await {
            Ok(list) => {
                self.listed = Some((Instant::now(), list.clone()));
                list
            }
            Err(e) => {
                tracing::debug!(error = %e, "tenant directory not listed; using the last listing");
                self.listed
                    .as_ref()
                    .map(|(_, l)| l.clone())
                    .unwrap_or_default()
            }
        }
    }

    /// The catalogs of the tenants led here that `wants` holds for, each
    /// as fresh as the directory (loaded on demand).
    pub(crate) async fn catalogs<F>(
        &mut self,
        svc: &TenantServices,
        wants: F,
    ) -> Vec<Arc<TenantCatalog>>
    where
        F: Fn(&TenantCatalog) -> bool,
    {
        let led = self.tenants(svc).await;
        let keep: BTreeSet<u64> = led.iter().copied().collect();
        self.seen.retain(|t, _| keep.contains(t));
        if std::mem::take(&mut self.all_dirty) {
            for s in self.seen.values_mut() {
                s.checked = None;
            }
        }
        let mut out = Vec::new();
        for t in led {
            let prev = self.seen.get(&t).copied();
            if let Some(s) = prev
                && !s.wanted
            {
                match s.checked {
                    Some(at) if at.elapsed() < RECHECK => continue,
                    Some(_) => {
                        // Nothing wanted at this version: look again only
                        // when the committed version moved.
                        if committed_version(svc, TenantId(t))
                            .await
                            .is_ok_and(|v| v == s.version)
                        {
                            if let Some(s) = self.seen.get_mut(&t) {
                                s.checked = Some(Instant::now());
                            }
                            continue;
                        }
                    }
                    None => {}
                }
            }
            match svc.cache.get(TenantId(t), Freshness::Latest).await {
                Ok(cat) => {
                    let wanted = wants(&cat);
                    self.seen.insert(
                        t,
                        Seen {
                            version: cat.version,
                            wanted,
                            checked: Some(Instant::now()),
                        },
                    );
                    if wanted {
                        out.push(cat);
                    }
                }
                Err(e) => {
                    // Looked at again on the next tick.
                    tracing::debug!(tenant = t, error = %e, "led tenant's catalog not loaded");
                    self.seen.remove(&t);
                }
            }
        }
        out
    }
}

/// `tenant`'s committed catalog version in this node's records.
async fn committed_version(svc: &TenantServices, tenant: TenantId) -> Result<u64> {
    let mut tx = svc.records.begin(tenant).await?;
    let v = sprig_catalog::records::read_version(
        &mut *tx,
        sprig_catalog::version::VersionKind::Catalog,
    )
    .await;
    let _ = tx.rollback().await;
    v
}

/// The active tenants of this data center in the directory, in id order.
async fn directory_tenants(svc: &TenantServices) -> Result<Vec<u64>> {
    let prefix = svc.global.tier().key("tenants/")?;
    let mut out = Vec::new();
    let mut opts = sprig_meta::ListOpts {
        limit: PAGE,
        ..sprig_meta::ListOpts::default()
    };
    loop {
        let page = svc.global.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            let Some(id) = kv
                .key
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|r| r.parse::<u64>().ok())
            else {
                continue;
            };
            let Ok(r) = sprig_meta::Record::<crate::inherited::DirEntry>::decode(&kv.value) else {
                continue;
            };
            let here = r.body.home_dc.is_empty() || r.body.home_dc == svc.dc.as_str();
            if id != 0 && r.body.state == "active" && here {
                out.push(id);
            }
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    out.sort_unstable();
    Ok(out)
}
