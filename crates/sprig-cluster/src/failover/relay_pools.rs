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
    /// The caller's pool kind (see [`tenant_pool_of`]).
    kind: &'static str,
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
    /// `None`: straight to the cluster; `Some((addr, node, url))`: through
    /// the relay writer at `addr` on `node`, whose agent's internal service
    /// is `url`; an empty `addr`: through a relay, none ready (`node` then
    /// says why).
    relay: Option<(String, String, String)>,
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
    /// The relay writer's agent's internal service, where a holder's calls
    /// go (`RelayLease::internal_url`).
    pub internal_url: String,
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
                // The greeting names the backend thread (or is the
                // relay's refusal): passed on, and the thread registered
                // for [`sever`].
                let Ok(greeting) = read_packet(&mut out).await else {
                    return;
                };
                if inbound.write_all(&greeting).await.is_err() {
                    return;
                }
                let key = greeting_thread(&greeting).map(|t| (addr.clone(), t));
                let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                if let Some(k) = &key {
                    severs()
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(k.clone(), tx);
                }
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut inbound, &mut out) => {}
                    () = async {
                        if rx.await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    } => {}
                }
                if let Some(k) = &key {
                    let mut m = severs().lock().unwrap_or_else(PoisonError::into_inner);
                    if m.get(k)
                        .is_some_and(tokio::sync::oneshot::Sender::is_closed)
                    {
                        m.remove(k);
                    }
                }
            });
        }
    });
    Ok((port, task))
}

type Severs = Mutex<HashMap<(String, u64), tokio::sync::oneshot::Sender<()>>>;

/// Forwarded connections by relay address and backend thread.
fn severs() -> &'static Severs {
    static S: OnceLock<Severs> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One MySQL packet (header and payload) from `s`.
async fn read_packet(s: &mut tokio::net::TcpStream) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 4];
    s.read_exact(&mut head).await?;
    let len = usize::from(head[0]) | usize::from(head[1]) << 8 | usize::from(head[2]) << 16;
    let mut p = Vec::with_capacity(4 + len);
    p.extend_from_slice(&head);
    p.resize(4 + len, 0);
    s.read_exact(&mut p[4..]).await?;
    Ok(p)
}

/// The connection id of a v10 handshake packet (header included).
fn greeting_thread(p: &[u8]) -> Option<u64> {
    let body = p.get(4..)?;
    if body.first() != Some(&10) {
        return None;
    }
    let end = body.get(1..)?.iter().position(|b| *b == 0)? + 2;
    let id = body.get(end..end + 4)?;
    Some(u64::from(u32::from_le_bytes([id[0], id[1], id[2], id[3]])))
}

/// Cuts the forwarded connection of backend `thread` through the relay at
/// `relay_addr` at once, passing nothing more to the relay: the relay
/// sees its client go away, as when the holder's process dies, never a
/// `COM_QUIT`. A holder giving up a connection whose branch may be held
/// calls it before closing the connection, so the fence keeps the branch
/// (and its locks) for its recovery instead of the server rolling it back
/// while other writers wait for its rows. Whether one was cut.
pub fn sever(relay_addr: &str, thread: u64) -> bool {
    let tx = severs()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(relay_addr.to_owned(), thread));
    tx.is_some_and(|t| t.send(()).is_ok())
}

/// A route with `base`'s account and session whose connections go
/// through the relay at `relay_addr`.
async fn through(
    base: &Opts,
    kind: &'static str,
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
    // The caller's own pool size, when larger (Bolt sessions' branches).
    let max = MAX_CONNS.max(base.pool_opts().constraints().max());
    let b = b.pool_opts(
        PoolOpts::new()
            .with_constraints(PoolConstraints::new(0, max).unwrap_or_default())
            .with_inactive_connection_ttl(Duration::from_secs(60)),
    );
    let opts = Opts::from(b);
    Ok(Through {
        relay_addr: relay_addr.to_owned(),
        base: base.clone(),
        kind,
        pool: Pool::new(opts.clone()),
        opts,
        forwarder: task,
    })
}

fn relayed(t: &Through, node: &str, internal_url: &str) -> Result<Relayed> {
    Ok(Relayed {
        node: NodeId::new(node)
            .map_err(|e| SprigError::internal(format!("relay lease node {node}: {e}")))?,
        relay_addr: t.relay_addr.clone(),
        internal_url: internal_url.to_owned(),
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
) -> Result<Option<(String, String, String)>> {
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
                RelayState::Ready(l) => Some((l.relay_addr, l.node_id, l.internal_url)),
                RelayState::Unavailable(why) => Some((String::new(), why, String::new())),
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
        let keep = relay
            .as_ref()
            .map(|(a, _, _)| a.clone())
            .unwrap_or_default();
        e.throughs.retain(|t| t.relay_addr == keep);
        relay
    };
    match relay {
        Some((a, why, _)) if a.is_empty() => Err(unavailable(format!(
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
    tenant_route_kind(dc, tenant, cluster, probe, base, "", preamble).await
}

/// [`tenant_route_with`] for a pool of its own `kind`: callers whose
/// connections must not come from the same pool (a lock holder's and the
/// work it waits for) pass different kinds with the same options.
///
/// # Errors
/// As [`tenant_route_with`].
pub async fn tenant_route_kind(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    probe: &Pool,
    base: &Opts,
    kind: &'static str,
    preamble: Preamble,
) -> Result<Option<Relayed>> {
    let Some((relay, node, url)) = relay_of(dc, tenant, cluster, probe).await? else {
        return Ok(None);
    };
    let key = (tenant, cluster.clone());
    {
        let m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(t) = m.get(&key).and_then(|e| {
            e.throughs
                .iter()
                .find(|t| t.relay_addr == relay && t.base == *base && t.kind == kind)
        }) {
            return relayed(t, &node, &url).map(Some);
        }
    }
    let t = through(base, kind, tenant, &relay, preamble).await?;
    let mut m = entries().lock().unwrap_or_else(PoisonError::into_inner);
    let e = m.entry(key).or_default();
    // Another caller may have made one meanwhile: use it, drop ours.
    if let Some(x) = e
        .throughs
        .iter()
        .find(|x| x.relay_addr == relay && x.base == *base && x.kind == kind)
    {
        return relayed(x, &node, &url).map(Some);
    }
    tracing::info!(%tenant, %cluster, relay = %relay,
        "sprig_ctl connections of a tenant on the relay path go through the relay writer");
    let out = relayed(&t, &node, &url)?;
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
    route_of_kind(dc, tenant, cluster, direct, "", preamble).await
}

async fn route_of_kind(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: &Pool,
    kind: &'static str,
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
    tenant_route_kind(dc, tenant, cluster, direct, &base, kind, preamble).await
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
    tenant_pool_of(dc, tenant, cluster, direct, "", preamble).await
}

/// [`tenant_pool`] for a pool of its own `kind` (see
/// [`tenant_route_kind`]).
///
/// # Errors
/// As [`tenant_route_with`].
pub async fn tenant_pool_of(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: Pool,
    kind: &'static str,
    preamble: Preamble,
) -> Result<Pool> {
    Ok(route_of_kind(dc, tenant, cluster, &direct, kind, preamble)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_greeting_names_the_backend_thread() {
        // A v10 handshake: version "11.8.3-MariaDB", connection id 0x01020304.
        let mut body = vec![10u8];
        body.extend_from_slice(b"11.8.3-MariaDB\0");
        body.extend_from_slice(&[4, 3, 2, 1]);
        body.extend_from_slice(&[0; 8]);
        let mut p = vec![u8::try_from(body.len()).unwrap(), 0, 0, 0];
        p.extend_from_slice(&body);
        assert_eq!(greeting_thread(&p), Some(0x0102_0304));
        // The relay's refusal (an ERR packet) names none.
        assert_eq!(greeting_thread(&[9, 0, 0, 0, 0xff, 0x18, 0x9a]), None);
        assert_eq!(greeting_thread(&[1, 0, 0, 0, 10]), None);
    }

    #[tokio::test]
    async fn sever_cuts_a_forwarded_connection_without_passing_anything_on() {
        use tokio::io::AsyncReadExt;
        // A stand-in relay: reads the preamble, sends a greeting for
        // thread 77, then records what else arrives until the client goes.
        let relay = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = relay.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut s, _) = relay.accept().await.unwrap();
            let mut pre = [0u8; 15];
            s.read_exact(&mut pre).await.unwrap();
            let mut body = vec![10u8];
            body.extend_from_slice(b"x\0");
            body.extend_from_slice(&77u32.to_le_bytes());
            let mut p = vec![u8::try_from(body.len()).unwrap(), 0, 0, 0];
            p.extend_from_slice(&body);
            s.write_all(&p).await.unwrap();
            let mut rest = Vec::new();
            let _ = s.read_to_end(&mut rest).await;
            rest
        });
        let (port, task) = forwarder(addr.clone(), b"SPRIGRELAY 1 7\n".to_vec())
            .await
            .unwrap();
        let mut c = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let g = read_packet(&mut c).await.unwrap();
        assert_eq!(greeting_thread(&g), Some(77));
        // Wait until the forwarder registered the thread, then cut it.
        let mut cut = false;
        for _ in 0..100 {
            if sever(&addr, 77) {
                cut = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(cut);
        // A COM_QUIT sent after the cut never reaches the relay.
        let _ = c.write_all(&[1, 0, 0, 0, 1]).await;
        let rest = server.await.unwrap();
        assert!(rest.is_empty(), "the relay got {rest:?}");
        assert!(!sever(&addr, 77));
        task.abort();
    }
}
