//! After-commit triggers of a tenant on Sprig 2PC's relay path reach its
//! shards through the cluster's relay writer, as the tenant's gateway
//! sessions do, so the relay fence ends their connections with the
//! tenant's others when a held branch's holder is lost. Without it, a
//! trigger's write waiting for a held branch's rows could take them in the
//! moment between the holder's end and the branch's replay.
//!
//! ```text
//! pool(tenant, cluster, direct sprig_ctl pool):
//!   the tenant's '%' accounts on the cluster are locked (the gateways'
//!     reconciler locks them for tenants on the relay path), checked every 2 s
//!     no  -> the direct pool
//!   the relay lease: ready -> a pool through it; recovering -> retry later
//! ```
//!
//! `mysql_async` can't send the relay's preamble line before the MySQL
//! handshake, so a pool through the relay connects to a forwarder on
//! 127.0.0.1 that opens the relay connection, sends the preamble and then
//! copies bytes both ways. The pool keeps its connections (min 0, 60 s
//! idle), so the forwarder opens few.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_cluster::failover::relay::{RelayState, read_relay_state};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

/// How long a check of the tenant's path and the relay lease is kept.
const RECHECK: Duration = Duration::from_secs(2);
/// Connections per pool through a relay.
const MAX_CONNS: usize = 8;

/// A pool through one relay writer, and its forwarder.
struct Through {
    relay_addr: String,
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
    /// `None`: straight to the cluster; `Some(addr)`: through the relay
    /// writer at `addr`; `Some("")`: through a relay, none ready.
    relay: Option<String>,
    through: Option<Through>,
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

/// Whether the tenant's per-user accounts at `'%'` on the cluster behind
/// `direct` are locked: the tenant is on the relay path there.
async fn locked(direct: &Pool, tenant: TenantId) -> Result<bool> {
    let mut c = direct
        .get_conn()
        .await
        .map_err(|e| unavailable(format!("the tenant's cluster is unreachable: {e}")))?;
    let like = format!("t{tenant}\\_u%");
    let n: Option<u64> = c
        .exec_first(
            "SELECT COUNT(*) FROM mysql.global_priv WHERE User LIKE ? AND Host = '%' \
             AND JSON_VALUE(Priv, '$.account_locked') = 'true'",
            (&like,),
        )
        .await
        .map_err(|e| unavailable(format!("the tenant's accounts can't be read: {e}")))?;
    Ok(n.unwrap_or(0) > 0)
}

/// Accepts on 127.0.0.1 and forwards each connection to `relay_addr`
/// after the preamble naming `tenant`.
async fn forwarder(relay_addr: String, tenant: TenantId) -> std::io::Result<(u16, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let addr = relay_addr.clone();
            tokio::spawn(async move {
                let Ok(mut out) = tokio::net::TcpStream::connect(&addr).await else {
                    return;
                };
                let _ = out.set_nodelay(true);
                let _ = inbound.set_nodelay(true);
                if out
                    .write_all(&sprig_txn::fence::wire::preamble(tenant))
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut out).await;
            });
        }
    });
    Ok((port, task))
}

/// A pool like `direct` whose connections go through the relay at
/// `relay_addr`.
async fn through(direct: &Pool, tenant: TenantId, relay_addr: &str) -> Result<Through> {
    let opts = {
        let c = direct
            .get_conn()
            .await
            .map_err(|e| unavailable(format!("the tenant's cluster is unreachable: {e}")))?;
        c.opts().clone()
    };
    let (port, task) = forwarder(relay_addr.to_owned(), tenant)
        .await
        .map_err(|e| unavailable(format!("the relay forwarder can't listen: {e}")))?;
    let mut b = OptsBuilder::from_opts(opts.clone())
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .prefer_socket(false);
    if let Some(ssl) = opts.ssl_opts() {
        // The certificate is still checked against the node's name.
        b = b.ssl_opts(Some(ssl.clone().with_danger_tls_hostname_override(Some(
            opts.ip_or_hostname().to_owned(),
        ))));
    }
    let pool = Pool::new(
        b.pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, MAX_CONNS).unwrap_or_default())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    );
    Ok(Through {
        relay_addr: relay_addr.to_owned(),
        pool,
        forwarder: task,
    })
}

/// The pool a trigger of `tenant` takes `sprig_ctl` connections to
/// `cluster` from: `direct`, or one through the cluster's relay writer for
/// a tenant on the relay path (see the module notes).
///
/// # Errors
/// Retryable 39101 while the cluster has relays but none is ready, or
/// when the cluster or the store can't be read.
pub(crate) async fn shard_pool(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: Pool,
) -> Result<Pool> {
    let key = (tenant, cluster.clone());
    let fresh = {
        let m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        m.get(&key).and_then(|e| {
            e.checked
                .filter(|t| t.elapsed() < RECHECK)
                .map(|_| (e.relay.clone(), e.through.as_ref().map(|t| t.pool.clone())))
        })
    };
    let relay = if let Some((relay, pool)) = fresh {
        match (relay, pool) {
            (None, _) => return Ok(direct),
            (Some(a), Some(p)) if !a.is_empty() => return Ok(p),
            (Some(a), _) => a,
        }
    } else {
        let relay = if locked(&direct, tenant).await? {
            match read_relay_state(&**dc, cluster).await? {
                RelayState::Ready(l) => Some(l.relay_addr),
                RelayState::Unavailable(_) => Some(String::new()),
                RelayState::None => None,
            }
        } else {
            None
        };
        let mut m = entries().lock().unwrap_or_else(PoisonError::into_inner);
        let e = m.entry(key.clone()).or_default();
        e.checked = Some(Instant::now());
        e.relay.clone_from(&relay);
        match &relay {
            None => {
                e.through = None;
                return Ok(direct);
            }
            Some(a) if a.is_empty() => {}
            Some(a) => {
                if let Some(t) = e.through.as_ref().filter(|t| t.relay_addr == *a) {
                    return Ok(t.pool.clone());
                }
            }
        }
        relay.unwrap_or_default()
    };
    if relay.is_empty() {
        return Err(unavailable(format!(
            "cluster {cluster} has relays but none is ready (a relay writer is recovering)"
        )));
    }
    let t = through(&direct, tenant, &relay).await?;
    let pool = t.pool.clone();
    let mut m = entries().lock().unwrap_or_else(PoisonError::into_inner);
    let e = m.entry(key).or_default();
    // Another trigger may have made one meanwhile: use it, drop ours.
    if let Some(x) = e.through.as_ref().filter(|x| x.relay_addr == relay) {
        return Ok(x.pool.clone());
    }
    tracing::info!(%tenant, %cluster, relay = %relay, "triggers of a tenant on the relay path connect through the relay writer");
    e.through = Some(t);
    Ok(pool)
}
