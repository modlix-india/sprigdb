//! `sprig_ctl` connections of a tenant on Sprig 2PC's relay path go
//! through the cluster's relay writer, as the tenant's own sessions do, so
//! the relay fence ends them with the tenant's other connections when a
//! held branch's holder is lost, and refuses them until its recovery is
//! done. Without it, a system writer (a trigger, the mover's rename
//! fence, the orphanage sweep, a lookup build) waiting for a held
//! branch's rows could take them in the moment between the holder's end
//! and the branch's replay.
//!
//! ```text
//! route(tenant, cluster, direct sprig_ctl pool):
//!   the tenant's '%' accounts on the cluster are locked (the gateways'
//!     reconciler locks them for tenants on the relay path), checked every 2 s
//!     no  -> direct
//!   the relay lease: ready -> through it; recovering or free -> retryable error
//! ```
//!
//! `mysql_async` can't send the relay's preamble line before the MySQL
//! handshake, so a route through the relay connects to a forwarder on
//! 127.0.0.1 that opens the relay connection, sends the preamble and then
//! copies bytes both ways. Each tenant, cluster, relay writer and caller's
//! connection options get one forwarder and one pool, shared by every
//! caller in the process with the same options. The pool keeps its
//! connections (min 0, 60 s idle), so the forwarder opens few.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_common::ids::{ClusterId, NodeId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use super::relay::{RelayState, read_relay_state};

/// How long a check of the tenant's path and the relay lease is kept.
const RECHECK: Duration = Duration::from_secs(2);
/// Connections per pool through a relay.
const MAX_CONNS: usize = 8;

/// The relay's preamble for a tenant (`sprig_txn::fence::wire::preamble`;
/// passed in, since this crate doesn't depend on the transaction crate).
pub type Preamble = fn(TenantId) -> Vec<u8>;

/// A route through one relay writer for one caller's options, and its
/// forwarder.
struct Through {
    relay_addr: String,
    /// The caller's options it was made from.
    base: Opts,
    opts: Opts,
    pool: Pool,
    forwarder: JoinHandle<()>,
}

impl Drop for Through {
    fn drop(&mut self) {
        self.forwarder.abort();
        let pool = self.pool.clone();
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                let _ = pool.disconnect().await;
            });
        }
    }
}

#[derive(Default)]
struct Entry {
    checked: Option<Instant>,
    /// `None`: straight to the cluster; `Some((addr, node))`: through the
    /// relay writer at `addr` on `node`; an empty `addr`: through a relay,
    /// none ready (`node` then says why).
    relay: Option<(String, String)>,
    throughs: Vec<Through>,
}

fn entries() -> &'static Mutex<HashMap<(TenantId, ClusterId), Entry>> {
    static E: OnceLock<Mutex<HashMap<(TenantId, ClusterId), Entry>>> = OnceLock::new();
    E.get_or_init(|| Mutex::new(HashMap::new()))
}

fn unavailable(msg: String) -> SprigError {
    let mut e = SprigError::new(SprigCode::BackendUnavailable, msg);
    e.retryable = true;
    e
}

/// A tenant's connections to a cluster through its ready relay writer.
#[derive(Clone)]
pub struct Relayed {
    /// The relay writer's node.
    pub node: NodeId,
    /// The relay's address.
    pub relay_addr: String,
    /// The caller's options, reaching the node through the relay (the
    /// forwarder on 127.0.0.1).
    pub opts: Opts,
    /// A pool over [`Self::opts`].
    pub pool: Pool,
}

impl std::fmt::Debug for Relayed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relayed")
            .field("node", &self.node)
            .field("relay_addr", &self.relay_addr)
            .finish_non_exhaustive()
    }
}

/// Whether `tenant`'s per-user accounts at `'%'` on the cluster behind
/// `direct` are locked: the tenant is on the relay path there.
///
/// # Errors
/// Retryable 39030 when the cluster can't be read.
pub async fn accounts_locked(direct: &Pool, tenant: TenantId) -> Result<bool> {
    let mut c = direct
        .get_conn()
        .await
        .map_err(|e| unavailable(format!("the tenant's cluster is unreachable: {e}")))?;
    let like = format!("t{tenant}\\_u%");
    let n: Option<u64> = c
        .exec_first(
            "SELECT COUNT(*) FROM mysql.global_priv WHERE User LIKE ? AND Host = '%' \
             AND JSON_VALUE(Priv, '$.account_locked') IN ('1', 'true')",
            (&like,),
        )
        .await
        .map_err(|e| unavailable(format!("the tenant's accounts can't be read: {e}")))?;
    Ok(n.unwrap_or(0) > 0)
}

/// Accepts on 127.0.0.1 and forwards each connection to `relay_addr`
/// after `preamble`.
async fn forwarder(
    relay_addr: String,
    preamble: Vec<u8>,
) -> std::io::Result<(u16, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let addr = relay_addr.clone();
            let preamble = preamble.clone();
            tokio::spawn(async move {
                let Ok(mut out) = tokio::net::TcpStream::connect(&addr).await else {
                    return;
                };
                let _ = out.set_nodelay(true);
                let _ = inbound.set_nodelay(true);
                if out.write_all(&preamble).await.is_err() {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut out).await;
            });
        }
    });
    Ok((port, task))
}

/// A route with `base`'s account and session whose connections go
/// through the relay at `relay_addr`.
async fn through(
    base: &Opts,
    tenant: TenantId,
    relay_addr: &str,
    preamble: Preamble,
) -> Result<Through> {
    let (port, task) = forwarder(relay_addr.to_owned(), preamble(tenant))
        .await
        .map_err(|e| unavailable(format!("the relay forwarder can't listen: {e}")))?;
    let mut b = OptsBuilder::from_opts(base.clone())
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .prefer_socket(false);
    if let Some(ssl) = base.ssl_opts() {
        // The certificate is still checked against the node's name.
        b = b.ssl_opts(Some(ssl.clone().with_danger_tls_hostname_override(Some(
            base.ip_or_hostname().to_owned(),
        ))));
    }
    let b = b.pool_opts(
        PoolOpts::new()
            .with_constraints(PoolConstraints::new(0, MAX_CONNS).unwrap_or_default())
            .with_inactive_connection_ttl(Duration::from_secs(60)),
    );
    let opts = Opts::from(b);
    Ok(Through {
        relay_addr: relay_addr.to_owned(),
        base: base.clone(),
        pool: Pool::new(opts.clone()),
        opts,
        forwarder: task,
    })
}

fn relayed(t: &Through, node: &str) -> Result<Relayed> {
    Ok(Relayed {
        node: NodeId::new(node)
            .map_err(|e| SprigError::internal(format!("relay lease node {node}: {e}")))?,
        relay_addr: t.relay_addr.clone(),
        opts: t.opts.clone(),
        pool: t.pool.clone(),
    })
}

/// Where `tenant` reaches `cluster` now: `None` straight, `Some((addr,
/// node))` through the ready relay writer.
async fn relay_of(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    probe: &Pool,
) -> Result<Option<(String, String)>> {
    let key = (tenant, cluster.clone());
    let fresh = {
        let m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        m.get(&key).and_then(|e| {
            e.checked
                .filter(|t| t.elapsed() < RECHECK)
                .map(|_| e.relay.clone())
        })
    };
    let relay = if let Some(r) = fresh {
        r
    } else {
        let relay = if accounts_locked(probe, tenant).await? {
            match read_relay_state(&**dc, cluster).await? {
                RelayState::Ready(l) => Some((l.relay_addr, l.node_id)),
                RelayState::Unavailable(why) => Some((String::new(), why)),
                RelayState::None => None,
            }
        } else {
            None
        };
        let mut m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        let e = m.entry(key).or_default();
        e.checked = Some(Instant::now());
        e.relay.clone_from(&relay);
        // Routes through another writer (or any, when direct) go.
        let keep = relay.as_ref().map(|(a, _)| a.clone()).unwrap_or_default();
        e.throughs.retain(|t| t.relay_addr == keep);
        relay
    };
    match relay {
        Some((a, why)) if a.is_empty() => Err(unavailable(format!(
            "cluster {cluster} has relays but none is ready: {why}"
        ))),
        r => Ok(r),
    }
}

/// How `tenant`'s `sprig_ctl` connections with options `base` reach
/// `cluster` (see the module notes): `None` straight to it, or through
/// its ready relay writer. `probe` is a direct `sprig_ctl` pool to the
/// cluster, for the accounts check; `dc` is the DC tier holding the relay
/// leases.
///
/// # Errors
/// Retryable 39030 while the tenant is on the relay path and the cluster
/// has no ready relay writer (one is recovering, or none holds the lease),
/// or when the cluster or the store can't be read.
pub async fn tenant_route_with(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    probe: &Pool,
    base: &Opts,
    preamble: Preamble,
) -> Result<Option<Relayed>> {
    let Some((relay, node)) = relay_of(dc, tenant, cluster, probe).await? else {
        return Ok(None);
    };
    let key = (tenant, cluster.clone());
    {
        let m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(t) = m.get(&key).and_then(|e| {
            e.throughs
                .iter()
                .find(|t| t.relay_addr == relay && t.base == *base)
        }) {
            return relayed(t, &node).map(Some);
        }
    }
    let t = through(base, tenant, &relay, preamble).await?;
    let mut m = entries().lock().unwrap_or_else(PoisonError::into_inner);
    let e = m.entry(key).or_default();
    // Another caller may have made one meanwhile: use it, drop ours.
    if let Some(x) = e
        .throughs
        .iter()
        .find(|x| x.relay_addr == relay && x.base == *base)
    {
        return relayed(x, &node).map(Some);
    }
    tracing::info!(%tenant, %cluster, relay = %relay,
        "sprig_ctl connections of a tenant on the relay path go through the relay writer");
    let out = relayed(&t, &node)?;
    e.throughs.push(t);
    Ok(Some(out))
}

/// [`tenant_route_with`] with `direct`'s own options.
///
/// # Errors
/// As [`tenant_route_with`].
pub async fn tenant_route(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: &Pool,
    preamble: Preamble,
) -> Result<Option<Relayed>> {
    if relay_of(dc, tenant, cluster, direct).await?.is_none() {
        return Ok(None);
    }
    let base = {
        let c = direct
            .get_conn()
            .await
            .map_err(|e| unavailable(format!("the tenant's cluster is unreachable: {e}")))?;
        c.opts().clone()
    };
    tenant_route_with(dc, tenant, cluster, direct, &base, preamble).await
}

/// The pool `tenant`'s `sprig_ctl` work on `cluster` takes connections
/// from: `direct`, or one with `direct`'s options through the cluster's
/// relay writer ([`tenant_route`]).
///
/// # Errors
/// As [`tenant_route_with`].
pub async fn tenant_pool(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: Pool,
    preamble: Preamble,
) -> Result<Pool> {
    Ok(tenant_route(dc, tenant, cluster, &direct, preamble)
        .await?
        .map_or(direct, |r| r.pool))
}

/// [`tenant_route_with`] and [`tenant_pool`] bound to a DC tier and
/// preamble.
#[derive(Clone)]
pub struct TenantRelays {
    dc: Arc<dyn MetaStore>,
    preamble: Preamble,
}

impl std::fmt::Debug for TenantRelays {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantRelays").finish_non_exhaustive()
    }
}

impl TenantRelays {
    /// Relay leases in `dc` (the DC tier); `preamble` names the tenant to
    /// the relay.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, preamble: Preamble) -> Self {
        Self { dc, preamble }
    }

    /// [`tenant_route_with`].
    ///
    /// # Errors
    /// As [`tenant_route_with`].
    pub async fn route_with(
        &self,
        tenant: TenantId,
        cluster: &ClusterId,
        probe: &Pool,
        base: &Opts,
    ) -> Result<Option<Relayed>> {
        tenant_route_with(&self.dc, tenant, cluster, probe, base, self.preamble).await
    }

    /// [`tenant_pool`].
    ///
    /// # Errors
    /// As [`tenant_route_with`].
    pub async fn pool(&self, tenant: TenantId, cluster: &ClusterId, direct: Pool) -> Result<Pool> {
        tenant_pool(&self.dc, tenant, cluster, direct, self.preamble).await
    }
}
