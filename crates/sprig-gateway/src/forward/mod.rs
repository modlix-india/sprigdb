//! Whole-connection forwarding: any node accepts any client connection.
//!
//! A tenant's sessions run on the node that leads its unit, so `KILL`,
//! `SHOW PROCESSLIST`, locks, temporary tables and prepared statements keep
//! one server's behaviour. Each front end serves a connection where it was
//! accepted until it has read the client's login message, which names the
//! tenant. Before answering it, the front end asks [`route`] where the
//! tenant is served; when that is another node it hands the connection over
//! with [`splice`]: the bytes read from the login message on, and a small
//! JSON of what it had already established with the client (its "resume"),
//! go to that node over the peer listener (`NodeForward.Splice`, mutual
//! TLS), whose front end continues from the login message as if the client
//! had connected there and checks the password itself. From then on this
//! node only copies bytes both ways.
//!
//! The receiving side is [`service`]: it serves the connection unless it
//! knows another node leads the tenant's unit, in which case it answers with
//! that node and the sender tries it (at most [`MAX_HOPS`] times).

pub mod io;
pub mod service;
pub mod watch;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_api::proto::internal::node_forward_client::NodeForwardClient;
use sprig_api::proto::internal::{SpliceOpen, SpliceTls, SpliceUp, splice_down, splice_up};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::frontend::{Protocol, TlsInfo};
use crate::listener::ListenerManager;
use crate::node_engine::{self, UnitLeader};
use crate::peer::{PeerTls, Peers};

/// How many times a connection may be sent on to another node before the
/// node it reaches serves it whatever it knows.
pub const MAX_HOPS: u32 = 3;

/// How long a login waits for an election to name a leader before it goes
/// to the unit's home node.
const LEADER_WAIT: Duration = Duration::from_secs(2);

/// How long node addresses read from the registry are reused.
const ADDR_TTL: Duration = Duration::from_secs(2);

/// Bytes read from the client per up frame.
const CHUNK: usize = 32 * 1024;

/// What forwarding needs from the running node.
pub struct Host {
    /// The client listeners: a forwarded connection is served by the
    /// listener of the same name here.
    pub listeners: Arc<ListenerManager>,
    /// The other nodes' registration records (address, peer port, Raft id).
    pub peers: Arc<Peers>,
    /// The peer service's mutual TLS identity (`[tls]`); `None` in
    /// development on loopback.
    pub tls: Option<PeerTls>,
}

static HOST: OnceLock<Host> = OnceLock::new();

/// Sets the node's forwarding host, once, after its listeners exist.
pub fn init(host: Host) {
    let _ = HOST.set(host);
}

pub(crate) fn host() -> Option<&'static Host> {
    HOST.get()
}

/// Connection ids this node greeted and handed to another node, with that
/// node: `KILL` of such an id is relayed there (the serving node's session
/// has the same id).
static HANDED: OnceLock<Mutex<HashMap<u32, u64>>> = OnceLock::new();

fn handed() -> &'static Mutex<HashMap<u32, u64>> {
    HANDED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Records a connection id as handed to node `to` until dropped.
pub(crate) struct HandedOff(u32);

impl HandedOff {
    pub(crate) fn new(id: u32, to: u64) -> Self {
        handed()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, to);
        Self(id)
    }
}

impl Drop for HandedOff {
    fn drop(&mut self) {
        handed()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.0);
    }
}

/// `KILL [QUERY] id` of a connection this node handed to another node:
/// relayed to that node, whose session has the same id. `None` when `id`
/// wasn't handed away.
pub(crate) async fn kill_handed(
    id: sprig_common::ids::ConnId,
    query_only: bool,
    tenant: Option<sprig_common::ids::TenantId>,
    user: &str,
) -> Option<Result<crate::peer::KillOutcome>> {
    let node = handed()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&id.0)
        .copied()?;
    let host = HOST.get()?;
    Some(
        host.peers
            .kill_on_node(node, id, query_only, tenant, user)
            .await,
    )
}

/// Whether forwarded bytes that are ready together travel together (one
/// down frame, one flush to the client): `SPRIGDB_FORWARD_COALESCE`,
/// default on; `off` sends each write as it comes (for comparisons).
pub(crate) fn coalesce() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("SPRIGDB_FORWARD_COALESCE").is_ok_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "off" | "false" | "0"
            )
        })
    })
}

/// Where a tenant's sessions are served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// On this node.
    Here,
    /// On another node, by Raft id.
    Node(u64),
}

/// Whether this process forwards at all: a replicated engine node with the
/// peer service's host set.
#[must_use]
pub fn enabled() -> bool {
    HOST.get().is_some() && node_engine::raft_id_from_env().is_some()
}

/// Where `tenant`'s sessions for `protocol` are served, as this node knows
/// now: the node leading its unit. During an election this waits up to 2 s
/// for a leader, then names the unit's home node. Kafka connections are
/// served by any node that holds a replica of the tenant (Metadata sends
/// clients on to each partition's leader); only other nodes forward them.
pub async fn route(tenant: u64, protocol: Protocol) -> Route {
    if !enabled() || tenant == 0 {
        return Route::Here;
    }
    let Some(me) = node_engine::raft_id_from_env() else {
        return Route::Here;
    };
    let descriptor = node_engine::unit_directory().and_then(|d| d.get(tenant));
    if protocol == Protocol::Kafka {
        match &descriptor {
            Some(d) if !d.voters.contains(&me) => {}
            _ => return Route::Here,
        }
    }
    let deadline = Instant::now() + LEADER_WAIT;
    loop {
        match node_engine::tenant_leader(tenant) {
            UnitLeader::Here | UnitLeader::Any => return Route::Here,
            UnitLeader::Node(n) if n == me => return Route::Here,
            UnitLeader::Node(n) => return Route::Node(n),
            UnitLeader::Unknown if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            UnitLeader::Unknown => {
                return match descriptor
                    .or_else(|| node_engine::unit_directory().and_then(|d| d.get(tenant)))
                {
                    Some(d) if d.home != me => Route::Node(d.home),
                    _ => Route::Here,
                };
            }
        }
    }
}

/// Whether this node holds a replica (a voter or a learner) of
/// `tenant`'s unit: where a reader session may be served. True where units
/// aren't placed per tenant (every node holds every unit) and on a node
/// that doesn't replicate.
#[must_use]
pub fn holds_replica(tenant: u64) -> bool {
    let Some(me) = node_engine::raft_id_from_env() else {
        return true;
    };
    match node_engine::unit_directory().and_then(|d| d.get(tenant)) {
        Some(d) => d.voters.contains(&me) || d.learners.contains(&me),
        None => node_engine::unit_directory().is_none(),
    }
}

/// Where a reader session of `tenant` (one that reads from copies:
/// [`crate::reader`]) is served: here when this node holds a replica of
/// the tenant's unit; otherwise a replica that doesn't lead it, chosen by
/// `pick` (the client's port, so connections spread over the copies), or
/// the leader when it is the only replica known.
pub async fn route_reader(tenant: u64, protocol: Protocol, pick: u64) -> Route {
    if !enabled() || tenant == 0 || holds_replica(tenant) {
        return Route::Here;
    }
    match reader_target(tenant, pick) {
        Some(n) => Route::Node(n),
        None => route(tenant, protocol).await,
    }
}

/// A replica of `tenant`'s unit other than its leader (learners first),
/// chosen by `pick`; `None` when no such replica is known.
#[must_use]
pub fn reader_target(tenant: u64, pick: u64) -> Option<u64> {
    let me = node_engine::raft_id_from_env()?;
    let d = node_engine::unit_directory()?.get(tenant)?;
    let leader = match node_engine::tenant_leader(tenant) {
        UnitLeader::Node(n) => Some(n),
        UnitLeader::Here => Some(me),
        UnitLeader::Any | UnitLeader::Unknown => None,
    };
    let copies: Vec<u64> = d
        .learners
        .iter()
        .chain(d.voters.iter())
        .copied()
        .filter(|n| Some(*n) != leader && *n != me)
        .collect();
    if copies.is_empty() {
        return None;
    }
    let i = usize::try_from(pick % copies.len() as u64).unwrap_or(0);
    Some(copies[i])
}

/// A connection to hand over.
#[derive(Debug)]
pub struct Handoff {
    /// The listener the client connected to.
    pub listener: &'static str,
    /// Its protocol (metrics).
    pub protocol: Protocol,
    /// The client address.
    pub peer: SocketAddr,
    /// The client's TLS here.
    pub tls: Option<TlsInfo>,
    /// The tenant the login names.
    pub tenant: u64,
    /// The protocol's resume JSON.
    pub resume: String,
    /// The client's bytes from its login message on.
    pub prelude: Bytes,
    /// The first node to try.
    pub to: u64,
}

fn failed(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ForwardFailed, msg)
}

/// Peer-service channels for splices: no per-call timeout (a splice lasts
/// as long as its connection), one HTTP/2 connection per node.
struct Channels {
    addrs: Mutex<(Option<Instant>, HashMap<u64, String>)>,
    channels: Mutex<HashMap<String, Channel>>,
}

static CHANNELS: OnceLock<Channels> = OnceLock::new();

fn channels() -> &'static Channels {
    CHANNELS.get_or_init(|| Channels {
        addrs: Mutex::new((None, HashMap::new())),
        channels: Mutex::new(HashMap::new()),
    })
}

/// The peer address (`host:port`) of the node with Raft id `node`.
async fn peer_addr(host: &Host, node: u64) -> Result<String> {
    let c = channels();
    {
        let a = c.addrs.lock().unwrap_or_else(PoisonError::into_inner);
        if a.0.is_some_and(|t| t.elapsed() < ADDR_TTL)
            && let Some(s) = a.1.get(&node)
        {
            return Ok(s.clone());
        }
    }
    let recs = host.peers.gateways().await?;
    let map: HashMap<u64, String> = recs
        .iter()
        .filter_map(|r| {
            let port = r.listeners.get("peer")?;
            Some((r.raft_node?, format!("{}:{port}", r.addr)))
        })
        .collect();
    let found = map.get(&node).cloned();
    *c.addrs.lock().unwrap_or_else(PoisonError::into_inner) = (Some(Instant::now()), map);
    found.ok_or_else(|| failed(format!("node {node} has no peer listener registered")))
}

fn channel(host: &Host, addr: &str) -> Result<Channel> {
    let c = channels();
    let scheme = if host.tls.is_some() { "https" } else { "http" };
    let uri = format!("{scheme}://{addr}");
    let mut chans = c.channels.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(ch) = chans.get(&uri) {
        return Ok(ch.clone());
    }
    let mut ep = Endpoint::from_shared(uri.clone())
        .map_err(|e| failed(format!("peer address {addr}: {e}")))?
        .connect_timeout(Duration::from_secs(2))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if let Some(t) = &host.tls {
        let domain = addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_owned();
        let cfg = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&t.ca_pem))
            .identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
            .domain_name(domain);
        ep = ep
            .tls_config(cfg)
            .map_err(|e| failed(format!("peer TLS: {e}")))?;
    }
    let ch = ep.connect_lazy();
    chans.insert(uri, ch.clone());
    Ok(ch)
}

fn forget_channel(addr: &str) {
    let c = channels();
    c.channels
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|k, _| !k.ends_with(addr));
    c.addrs.lock().unwrap_or_else(PoisonError::into_inner).0 = None;
}

fn splice_tls(t: &TlsInfo) -> SpliceTls {
    let (subject, issuer) = t
        .client_dn
        .as_ref()
        .map(|d| (d.subject.clone(), d.issuer.clone()))
        .unwrap_or_default();
    SpliceTls {
        version: t.version.to_owned(),
        cipher: t.cipher.to_owned(),
        sni: t.sni.clone().unwrap_or_default(),
        client_cert: t.client_cert,
        client_subject: subject,
        client_issuer: issuer,
    }
}

/// Counts a hand-off's outcome.
fn count(protocol: Protocol, result: &'static str) {
    metrics::counter!("sprig_node_forwarded_connections_total", "protocol" => protocol.as_str(), "result" => result)
        .increment(1);
}

/// Counts a connection served on this node that came straight from a
/// client (the other half of `sprig_node_forwarded_ratio`).
pub fn count_direct(protocol: Protocol) {
    metrics::counter!("sprig_node_direct_connections_total", "protocol" => protocol.as_str())
        .increment(1);
}

type Opened = (
    mpsc::Sender<SpliceUp>,
    tonic::Streaming<sprig_api::proto::internal::SpliceDown>,
);

/// Opens the splice for `h`: from `h.to`, and when that node can't be
/// reached (it died; this node's idea of the leader is stale), from each
/// other voter of the tenant's unit in turn, any of which redirects to the
/// leader it knows.
async fn dial(host: &Host, h: &Handoff) -> Result<Opened> {
    let me = node_engine::raft_id_from_env().unwrap_or(0);
    let voters = node_engine::unit_directory()
        .and_then(|d| d.get(h.tenant))
        .map(|d| d.voters)
        .unwrap_or_default();
    let mut tried = vec![h.to];
    let mut first: Option<SprigError> = None;
    let mut to = h.to;
    loop {
        match dial_from(host, h, to).await {
            Ok(o) => return Ok(o),
            Err(e) if e.code() == Some(SprigCode::ForwardFailed) => {
                let next = voters
                    .iter()
                    .copied()
                    .find(|v| *v != me && !tried.contains(v));
                let e = first.take().unwrap_or(e);
                let Some(n) = next else { return Err(e) };
                tracing::debug!(tenant = h.tenant, from = to, to = n, error = %e,
                    "forwarding: node unreachable; trying another voter");
                first = Some(e);
                tried.push(n);
                to = n;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Opens the splice for `h` at node `start`, following redirects: the up
/// sender and the down stream once a node accepted.
async fn dial_from(host: &Host, h: &Handoff, start: u64) -> Result<Opened> {
    let me = node_engine::raft_id_from_env().unwrap_or(0);
    let mut to = start;
    let mut hops = 0;
    loop {
        let addr = match peer_addr(host, to).await {
            Ok(a) => a,
            Err(e) => {
                count(h.protocol, "unreachable");
                return Err(e);
            }
        };
        let (tx, rx) = mpsc::channel::<SpliceUp>(8);
        let open = SpliceUp {
            msg: Some(splice_up::Msg::Open(SpliceOpen {
                listener: h.listener.to_owned(),
                client_addr: h.peer.to_string(),
                tls: h.tls.as_ref().map(splice_tls),
                tenant_id: h.tenant,
                hops,
                from_node: me,
                resume_json: h.resume.clone(),
                prelude: h.prelude.to_vec(),
            })),
        };
        let _ = tx.send(open).await;
        let mut client_rpc = NodeForwardClient::new(channel(host, &addr)?)
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(64 << 20);
        let req =
            futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|m| (m, rx)) });
        let resp = match tokio::time::timeout(Duration::from_secs(5), client_rpc.splice(req)).await
        {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(e)) => {
                forget_channel(&addr);
                count(h.protocol, "unreachable");
                return Err(failed(format!("node {to} ({addr}): {}", e.message())));
            }
            Err(_) => {
                forget_channel(&addr);
                count(h.protocol, "unreachable");
                return Err(failed(format!("node {to} ({addr}) didn't answer in 5 s")));
            }
        };
        let mut resp = resp;
        let first = match tokio::time::timeout(Duration::from_secs(5), resp.message()).await {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => {
                count(h.protocol, "unreachable");
                return Err(failed(format!("node {to} ended the hand-off")));
            }
            Ok(Err(e)) => {
                count(h.protocol, "unreachable");
                return Err(failed(format!("node {to}: {}", e.message())));
            }
            Err(_) => {
                count(h.protocol, "unreachable");
                return Err(failed(format!("node {to} didn't accept in 5 s")));
            }
        };
        match first.msg {
            Some(splice_down::Msg::Accepted(_)) => return Ok((tx, resp)),
            Some(splice_down::Msg::Redirect(n)) if hops + 1 < MAX_HOPS && n != me => {
                tracing::debug!(
                    tenant = h.tenant,
                    from = to,
                    to = n,
                    "forwarded connection redirected"
                );
                to = n;
                hops += 1;
            }
            Some(splice_down::Msg::Redirect(n)) => {
                count(h.protocol, "redirect_loop");
                return Err(failed(format!(
                    "the nodes disagree about who serves this tenant (last: node {n}); retry"
                )));
            }
            Some(splice_down::Msg::Refused(r)) => {
                count(h.protocol, "refused");
                let code = u16::try_from(r.code)
                    .ok()
                    .and_then(SprigCode::from_number)
                    .unwrap_or(SprigCode::ForwardFailed);
                return Err(SprigError::new(code, r.message));
            }
            Some(splice_down::Msg::Data(_)) | None => {
                count(h.protocol, "unreachable");
                return Err(failed(format!(
                    "node {to} answered the hand-off out of turn"
                )));
            }
        }
    }
}

/// Hands `client` over as `h` says and copies bytes both ways until either
/// side ends. Nothing has been written to the client by then on failure, so
/// the caller can answer in its protocol.
///
/// # Errors
/// 39788 (retryable) when no node accepted the connection; the receiving
/// node's refusal (its code) otherwise. Errors after the hand-off end the
/// connection and are returned only for the log.
pub async fn splice<S>(client: S, h: Handoff) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let host = HOST
        .get()
        .ok_or_else(|| failed("forwarding is not set up on this node"))?;
    let (up_tx, mut down) = dial(host, &h).await?;
    count(h.protocol, "ok");
    tracing::debug!(tenant = h.tenant, to = h.to, listener = h.listener, peer = %h.peer, "connection forwarded");
    let (mut rd, mut wr) = tokio::io::split(client);
    let upward = async move {
        let mut buf = vec![0u8; CHUNK];
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let m = SpliceUp {
                        msg: Some(splice_up::Msg::Data(buf[..n].to_vec())),
                    };
                    if up_tx.send(m).await.is_err() {
                        break;
                    }
                }
            }
        }
        // Dropping the sender ends the up stream: the home node sees the
        // client's end of file.
    };
    let downward = async move {
        'down: while let Ok(Some(m)) = down.message().await {
            if let Some(splice_down::Msg::Data(d)) = m.msg
                && wr.write_all(&d).await.is_err()
            {
                break;
            }
            // Frames already here go out with this one: one flush (one TLS
            // record) for a result set the serving node wrote in pieces.
            while coalesce() {
                match futures::FutureExt::now_or_never(down.message()) {
                    Some(Ok(Some(m))) => {
                        if let Some(splice_down::Msg::Data(d)) = m.msg
                            && wr.write_all(&d).await.is_err()
                        {
                            break 'down;
                        }
                    }
                    Some(Ok(None) | Err(_)) => {
                        let _ = wr.flush().await;
                        break 'down;
                    }
                    None => break,
                }
            }
            if wr.flush().await.is_err() {
                break;
            }
        }
        let _ = wr.shutdown().await;
    };
    let mut downward = std::pin::pin!(downward);
    tokio::select! {
        () = upward => {
            // The client stopped sending: let the home node's last answer
            // (and its close) through, briefly.
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut downward).await;
        }
        () = &mut downward => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handed_off_ids_are_known_while_the_splice_lasts() {
        let at = |id| {
            handed()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&id)
                .copied()
        };
        {
            let _h = HandedOff::new(0x0010_0077, 3);
            assert_eq!(at(0x0010_0077), Some(3));
        }
        assert_eq!(at(0x0010_0077), None);
    }
}
