//! Frames over TCP, optionally with mutual TLS, and with node identity:
//! each node's certificate names it (`node-<id>`), dialled peers must be
//! the node asked for, and accepted peers may send only as themselves.
//!
//! Each node keeps one outgoing connection per peer node and accepts the
//! peers' connections on its listener. A connection carries length-prefixed
//! frames (`u32` little-endian length, then the frame). The writer drains
//! every frame queued for the peer into one write, so the frames of several
//! driver turns travel together under load. A full queue (a peer that is
//! down) drops new frames: Raft resends what matters.
//!
//! Certificates change without a restart: [`TcpTransport::set_tls`] puts
//! a new configuration in place, and every outgoing connection reconnects
//! with it after its current write (frames queued meanwhile go on the new
//! connection). [`TcpTransport::set_revoked`] lists revoked certificates
//! by serial number: a peer presenting one is refused, and every live
//! connection to or from it is closed at once.
//!
//! The listener may be shared with other protocols (the gateway's peer
//! listener): a Raft connection offers the ALPN protocol [`ALPN`] over TLS,
//! or writes [`PREFACE`] first in plaintext ([`TcpTransport::set_preface`]),
//! and the shared listener hands it over with [`TcpTransport::serve_tls`]
//! or [`TcpTransport::serve_plain`]. The bytes after that are the same.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use crate::NodeId;
use crate::node::Node;
use crate::transport::Transport;

/// The ALPN protocol a Raft connection offers over TLS.
pub const ALPN: &[u8] = b"sprig-raft/1";
/// What a plaintext Raft connection to a shared listener writes first.
pub const PREFACE: &[u8] = b"SPRIG-RAFT/1\n";

/// The largest frame accepted. A unit snapshot travels as one message in
/// one frame, so this bounds the unit a new or lagging replica can be sent
/// (until snapshots are streamed in chunks).
pub const MAX_FRAME: usize = 1 << 30;
/// Frames queued per peer before new ones are dropped.
const QUEUE: usize = 4096;
/// Bytes gathered into one write.
const WRITE_CHUNK: usize = 1 << 20;

/// Mutual TLS for the transport: the client side presents this node's
/// certificate, the server side requires the peer's (the configs say how
/// certificates are checked).
#[derive(Clone)]
pub struct TlsConfig {
    /// For outgoing connections.
    pub client: Arc<rustls::ClientConfig>,
    /// For the listener.
    pub server: Arc<rustls::ServerConfig>,
    /// The name every peer's certificate is checked against (unless
    /// `node_names`).
    pub server_name: ServerName<'static>,
    /// Node identity: every node's certificate names it (a subjectAltName
    /// `node-<id>`, [`cert_node_id`]). An outgoing connection checks the
    /// peer's certificate names the node it dials; the listener takes a
    /// peer's id from its certificate and drops every frame that claims
    /// another sender, so a node can't speak for another.
    pub node_names: bool,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .field("node_names", &self.node_names)
            .finish_non_exhaustive()
    }
}

/// The name node `id`'s certificate carries.
#[must_use]
pub fn node_name(id: NodeId) -> String {
    format!("node-{id}")
}

impl TlsConfig {
    /// Mutual TLS with node identity from PEM: `ca` the internal CA that
    /// signs every node's certificate, `chain` and `key` this node's
    /// (whose certificate must name node `me`).
    ///
    /// # Errors
    /// The PEM doesn't parse, or the certificate doesn't name `me`.
    pub fn node_identity(me: NodeId, ca: &[u8], chain: &[u8], key: &[u8]) -> io::Result<Self> {
        use rustls::pki_types::pem::PemObject as _;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let bad = |what: &str, e: &dyn std::fmt::Display| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("raft TLS: {what}: {e}"),
            )
        };
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(c.map_err(|e| bad("the CA", &e))?)
                .map_err(|e| bad("the CA", &e))?;
        }
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain)
            .collect::<Result<_, _>>()
            .map_err(|e| bad("the certificate", &e))?;
        let Some(first) = certs.first() else {
            return Err(bad("the certificate", &"none in the file"));
        };
        if cert_node_id(first) != Some(me) {
            return Err(bad(
                "the certificate",
                &format!("it doesn't name this node ({})", node_name(me)),
            ));
        }
        let key = || PrivateKeyDer::from_pem_slice(key).map_err(|e| bad("the key", &e));
        let roots = Arc::new(roots);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::clone(&roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| bad("the client verifier", &e))?;
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("the server config", &e))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key()?)
            .map_err(|e| bad("the server config", &e))?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("the client config", &e))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key()?)
            .map_err(|e| bad("the client config", &e))?;
        // A shared listener tells Raft connections apart by it.
        client.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            server_name: ServerName::try_from(node_name(me))
                .map_err(|e| bad("the node name", &e))?,
            node_names: true,
        })
    }
}

/// One DER element: its tag, its contents and what follows.
fn tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = b.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |a, x| (a << 8) | usize::from(*x));
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

/// The node id a DER certificate names: a subjectAltName DNS name
/// `node-<id>`, or `node-<id>.<domain>`. `None` when it names none.
#[must_use]
pub fn cert_node_id(der: &[u8]) -> Option<NodeId> {
    const SAN: &[u8] = &[0x55, 0x1d, 0x11];
    let (_, cert, _) = tlv(der)?;
    let (_, mut tbs, _) = tlv(cert)?;
    // The TBS fields up to the extensions ([3]).
    let exts = loop {
        let (tag, body, rest) = tlv(tbs)?;
        if tag == 0xa3 {
            break body;
        }
        tbs = rest;
    };
    let (_, mut list, _) = tlv(exts)?;
    while !list.is_empty() {
        let (_, ext, rest) = tlv(list)?;
        list = rest;
        let (tag, oid, mut fields) = tlv(ext)?;
        if tag != 0x06 || oid != SAN {
            continue;
        }
        // critical BOOLEAN, optional; then the OCTET STRING.
        let (mut t, mut v, mut after) = tlv(fields)?;
        if t == 0x01 {
            fields = after;
            (t, v, after) = tlv(fields)?;
        }
        let _ = after;
        if t != 0x04 {
            return None;
        }
        let (_, mut names, _) = tlv(v)?;
        while !names.is_empty() {
            let (t, name, rest) = tlv(names)?;
            names = rest;
            if t != 0x82 {
                continue;
            }
            let name = std::str::from_utf8(name).ok()?;
            let id = name.strip_prefix("node-")?;
            let id = id.split_once('.').map_or(id, |(i, _)| i);
            if let Ok(id) = id.parse::<NodeId>() {
                return Some(id);
            }
        }
    }
    None
}

/// The serial number of a DER certificate: its TBS's INTEGER contents.
#[must_use]
pub fn cert_serial(der: &[u8]) -> Option<Vec<u8>> {
    let (_, cert, _) = tlv(der)?;
    let (_, tbs, _) = tlv(cert)?;
    let (mut tag, mut body, rest) = tlv(tbs)?;
    if tag == 0xa0 {
        (tag, body, _) = tlv(rest)?;
    }
    (tag == 0x02).then(|| body.to_vec())
}

/// What live connections must follow: the TLS configuration's generation
/// (a connection made under an older one reconnects) and the revoked
/// certificates' serial numbers.
#[derive(Debug, Clone, Default)]
struct Policy {
    generation: u64,
    revoked: Arc<BTreeSet<Vec<u8>>>,
}

/// Resolves once `serial` is revoked (never for a connection without a
/// certificate).
async fn revoked(mut rx: watch::Receiver<Policy>, serial: Option<Vec<u8>>) {
    stale(&mut rx, None, serial.as_deref()).await;
}

/// Resolves once the TLS configuration is no longer generation `generation`
/// (when given) or `serial` is revoked.
async fn stale(rx: &mut watch::Receiver<Policy>, generation: Option<u64>, serial: Option<&[u8]>) {
    loop {
        {
            let p = rx.borrow_and_update();
            if generation.is_some_and(|g| g != p.generation)
                || serial.is_some_and(|s| p.revoked.contains(s))
            {
                return;
            }
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// The TCP transport of one node.
pub struct TcpTransport {
    me: NodeId,
    handle: tokio::runtime::Handle,
    tls: Arc<RwLock<Option<TlsConfig>>>,
    policy: watch::Sender<Policy>,
    peers: Mutex<BTreeMap<NodeId, SocketAddr>>,
    queues: Mutex<BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Peers cut off (a test's network partition): frames to them and
    /// from them are dropped.
    cut: Arc<RwLock<BTreeSet<NodeId>>>,
    /// Plaintext connections start with [`PREFACE`] (peers listen on a
    /// shared listener).
    preface: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl TcpTransport {
    /// A transport for node `me` whose connections run on `handle`.
    #[must_use]
    pub fn new(me: NodeId, handle: tokio::runtime::Handle, tls: Option<TlsConfig>) -> Arc<Self> {
        Arc::new(Self {
            me,
            handle,
            tls: Arc::new(RwLock::new(tls)),
            policy: watch::Sender::new(Policy::default()),
            peers: Mutex::new(BTreeMap::new()),
            queues: Mutex::new(BTreeMap::new()),
            cut: Arc::new(RwLock::new(BTreeSet::new())),
            preface: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Peers listen on a listener shared with other protocols: plaintext
    /// connections start with [`PREFACE`] (TLS ones offer [`ALPN`]
    /// anyway). Set before the first frame is sent.
    pub fn set_preface(&self, on: bool) {
        self.preface.store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// The TLS server configuration in place, for a shared listener to
    /// finish the handshake of a connection that offered [`ALPN`]; `None`
    /// without TLS.
    #[must_use]
    pub fn server_config(&self) -> Option<Arc<rustls::ServerConfig>> {
        self.tls
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|t| Arc::clone(&t.server))
    }

    /// Whether the transport runs with TLS.
    #[must_use]
    pub fn has_tls(&self) -> bool {
        self.tls
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Serves a Raft connection a shared listener accepted over TLS (its
    /// handshake finished with [`Self::server_config`]): frames go to
    /// `node` until the connection ends or its certificate is revoked.
    pub async fn serve_tls<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        s: tokio_rustls::server::TlsStream<S>,
        node: &Node,
    ) {
        let node_names = self
            .tls
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|t| t.node_names);
        serve_accepted(s, node, node_names, self.policy.subscribe(), &self.cut).await;
    }

    /// Serves a plaintext Raft connection a shared listener accepted: it
    /// must start with [`PREFACE`]; refused when this transport runs with
    /// TLS.
    pub async fn serve_plain<S: AsyncRead + Unpin>(&self, mut s: S, node: &Node) {
        if self.has_tls() {
            tracing::warn!(
                node = node.id(),
                "raft: a plaintext connection to a node that runs Raft over TLS; refused"
            );
            return;
        }
        let mut pre = [0u8; PREFACE.len()];
        if s.read_exact(&mut pre).await.is_err() || pre != PREFACE {
            return;
        }
        read_frames(s, node, None, &self.cut, std::future::pending()).await;
    }

    /// Puts new certificates in place (renewed ones): new connections use
    /// them, and every outgoing connection reconnects with them after its
    /// current write.
    ///
    /// # Errors
    /// The transport was started without TLS.
    pub fn set_tls(&self, t: TlsConfig) -> io::Result<()> {
        {
            let mut cur = self.tls.write().unwrap_or_else(PoisonError::into_inner);
            if cur.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "raft tcp: the transport runs without TLS",
                ));
            }
            *cur = Some(t);
        }
        self.policy.send_modify(|p| p.generation += 1);
        Ok(())
    }

    /// How many times [`Self::set_tls`] has replaced the configuration.
    #[must_use]
    pub fn tls_generation(&self) -> u64 {
        self.policy.borrow().generation
    }

    /// Revoked certificates by serial number ([`cert_serial`]): peers
    /// presenting one are refused, and live connections to or from them
    /// close at once. Replaces the previous list.
    pub fn set_revoked(&self, serials: BTreeSet<Vec<u8>>) {
        self.policy.send_if_modified(|p| {
            if *p.revoked == serials {
                return false;
            }
            p.revoked = Arc::new(serials);
            true
        });
    }

    /// Cuts this node off from `peers` (and heals every other link): from
    /// now on frames to them aren't sent and frames from them are dropped,
    /// as a network partition would, while connections stay open. For
    /// fault tests; an empty set heals everything.
    pub fn set_cut(&self, peers: BTreeSet<NodeId>) {
        *self.cut.write().unwrap_or_else(PoisonError::into_inner) = peers;
    }

    /// The peers this node is cut off from ([`TcpTransport::set_cut`]).
    #[must_use]
    pub fn cut(&self) -> BTreeSet<NodeId> {
        self.cut
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Where peer `id` listens.
    pub fn set_peer(&self, id: NodeId, addr: SocketAddr) {
        self.peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, addr);
    }

    /// Listens on `addr` and hands every frame received to `node`. Returns
    /// the bound address.
    ///
    /// # Errors
    /// The address could not be bound.
    pub async fn listen(self: &Arc<Self>, addr: SocketAddr, node: Node) -> io::Result<SocketAddr> {
        let l = TcpListener::bind(addr).await?;
        let bound = l.local_addr()?;
        let tls = Arc::clone(&self.tls);
        let policy = self.policy.clone();
        let cut = Arc::clone(&self.cut);
        self.handle.spawn(async move {
            loop {
                let Ok((s, _)) = l.accept().await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let _ = s.set_nodelay(true);
                let node = node.clone();
                // The configuration in place now (certificates are renewed
                // while the node runs).
                let tls = tls.read().unwrap_or_else(PoisonError::into_inner).clone();
                let policy = policy.subscribe();
                let cut = Arc::clone(&cut);
                tokio::spawn(async move {
                    match tls {
                        Some(t) => {
                            let acc = tokio_rustls::TlsAcceptor::from(Arc::clone(&t.server));
                            let Ok(s) = acc.accept(s).await else {
                                return;
                            };
                            serve_accepted(s, &node, t.node_names, policy, &cut).await;
                        }
                        None => {
                            read_frames(s, &node, None, &cut, std::future::pending()).await;
                        }
                    }
                });
            }
        });
        Ok(bound)
    }

    fn queue(&self, to: NodeId) -> Option<mpsc::Sender<Vec<u8>>> {
        let preface = self.preface.load(std::sync::atomic::Ordering::Relaxed);
        let mut qs = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(q) = qs.get(&to)
            && !q.is_closed()
        {
            return Some(q.clone());
        }
        let addr = *self
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&to)?;
        let (tx, rx) = mpsc::channel(QUEUE);
        qs.insert(to, tx.clone());
        self.handle.spawn(write_frames(
            addr,
            Arc::clone(&self.tls),
            self.policy.subscribe(),
            rx,
            (self.me, to, preface),
        ));
        Some(tx)
    }
}

impl Transport for TcpTransport {
    fn send(&self, to: NodeId, frame: Vec<u8>) {
        if self
            .cut
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&to)
        {
            return;
        }
        if let Some(q) = self.queue(to) {
            let _ = q.try_send(frame);
        }
    }
}

/// An accepted TLS connection: refused when its certificate is revoked or,
/// with node identity, names no node; otherwise its frames go to `node`.
async fn serve_accepted<S: AsyncRead + AsyncWrite + Unpin>(
    s: tokio_rustls::server::TlsStream<S>,
    node: &Node,
    node_names: bool,
    policy: watch::Receiver<Policy>,
    cut: &RwLock<BTreeSet<NodeId>>,
) {
    let leaf = s
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .map(|c| c.to_vec());
    let serial = leaf.as_deref().and_then(cert_serial);
    if let Some(sn) = &serial
        && policy.borrow().revoked.contains(sn)
    {
        tracing::warn!(node = node.id(), peer = ?leaf.as_deref().and_then(cert_node_id),
            "raft tcp: a peer's certificate is revoked; connection refused");
        return;
    }
    // With node identity, the peer is the node its certificate names, and
    // only that node.
    let peer = if node_names {
        let Some(id) = leaf.as_deref().and_then(cert_node_id) else {
            tracing::warn!(
                node = node.id(),
                "raft tcp: a peer's certificate names no node; connection refused"
            );
            return;
        };
        Some(id)
    } else {
        None
    };
    read_frames(s, node, peer, cut, revoked(policy, serial)).await;
}

/// The sender a frame claims (its header's `from`).
fn frame_sender(frame: &[u8]) -> Option<NodeId> {
    let b: [u8; 8] = frame.get(2..10)?.try_into().ok()?;
    Some(NodeId::from_le_bytes(b))
}

/// Reads frames until the connection ends or `gone` resolves (its
/// certificate was revoked).
async fn read_frames<S: AsyncRead + Unpin>(
    mut s: S,
    node: &Node,
    peer: Option<NodeId>,
    cut: &RwLock<BTreeSet<NodeId>>,
    gone: impl Future<Output = ()>,
) {
    tokio::pin!(gone);
    let mut len = [0u8; 4];
    loop {
        tokio::select! {
            r = s.read_exact(&mut len) => if r.is_err() { return },
            () = &mut gone => {
                tracing::warn!(node = node.id(), ?peer, "raft tcp: a peer's certificate was revoked; connection closed");
                return;
            }
        }
        let n = u32::from_le_bytes(len) as usize;
        if n > MAX_FRAME {
            return;
        }
        let mut buf = vec![0u8; n];
        tokio::select! {
            r = s.read_exact(&mut buf) => if r.is_err() { return },
            () = &mut gone => return,
        }
        if let Some(p) = peer
            && frame_sender(&buf) != Some(p)
        {
            tracing::warn!(node = node.id(), peer = p, claimed = ?frame_sender(&buf),
                "raft tcp: a frame claiming another sender than its certificate names; connection closed");
            return;
        }
        if frame_sender(&buf).is_some_and(|f| {
            cut.read()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&f)
        }) {
            continue;
        }
        node.deliver(buf);
    }
}

async fn connect(
    addr: SocketAddr,
    tls: Option<&TlsConfig>,
    to: NodeId,
    preface: bool,
) -> io::Result<Conn> {
    let mut s = TcpStream::connect(addr).await?;
    s.set_nodelay(true)?;
    if preface && tls.is_none() {
        s.write_all(PREFACE).await?;
    }
    Ok(match tls {
        Some(t) => {
            let c = tokio_rustls::TlsConnector::from(Arc::clone(&t.client));
            // With node identity, the peer must be the node dialled.
            let name = if t.node_names {
                ServerName::try_from(node_name(to))
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            } else {
                t.server_name.clone()
            };
            Conn::Tls(Box::new(c.connect(name, s).await?))
        }
        None => Conn::Plain(s),
    })
}

enum Conn {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Conn {
    /// The serial number of the peer's certificate.
    fn peer_serial(&self) -> Option<Vec<u8>> {
        match self {
            Conn::Plain(_) => None,
            Conn::Tls(s) => s
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|c| c.first())
                .and_then(|c| cert_serial(c)),
        }
    }

    async fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
        match self {
            Conn::Plain(s) => write_flush(s, b).await,
            Conn::Tls(s) => write_flush(s.as_mut(), b).await,
        }
    }
}

async fn write_flush<S: AsyncWrite + Unpin>(s: &mut S, b: &[u8]) -> io::Result<()> {
    s.write_all(b).await?;
    s.flush().await
}

async fn write_frames(
    addr: SocketAddr,
    tls: Arc<RwLock<Option<TlsConfig>>>,
    mut policy: watch::Receiver<Policy>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    (me, to, preface): (NodeId, NodeId, bool),
) {
    let mut backoff = Duration::from_millis(50);
    let mut buf = Vec::with_capacity(64 << 10);
    loop {
        let generation = policy.borrow_and_update().generation;
        let t = tls.read().unwrap_or_else(PoisonError::into_inner).clone();
        let connected = connect(addr, t.as_ref(), to, preface).await.and_then(|c| {
            let serial = c.peer_serial();
            if serial
                .as_ref()
                .is_some_and(|s| policy.borrow().revoked.contains(s))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "the peer's certificate is revoked",
                ));
            }
            Ok((c, serial))
        });
        let (mut conn, serial) = match connected {
            Ok(c) => {
                backoff = Duration::from_millis(50);
                c
            }
            Err(e) => {
                tracing::debug!(node = me, peer = to, error = %e, "raft tcp: connect failed");
                // Frames queued while the peer is unreachable are stale.
                while rx.try_recv().is_ok() {}
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
                continue;
            }
        };
        // Renewed certificates (reconnect with them) or the peer's revoked.
        let mut watch = policy.clone();
        let renewed = stale(&mut watch, Some(generation), serial.as_deref());
        tokio::pin!(renewed);
        loop {
            let f = tokio::select! {
                f = rx.recv() => f,
                () = &mut renewed => break,
            };
            let Some(f) = f else {
                return;
            };
            buf.clear();
            push_frame(&mut buf, &f);
            while buf.len() < WRITE_CHUNK {
                match rx.try_recv() {
                    Ok(f) => push_frame(&mut buf, &f),
                    Err(_) => break,
                }
            }
            if conn.write_all(&buf).await.is_err() {
                break;
            }
        }
    }
}

fn push_frame(buf: &mut Vec<u8>, f: &[u8]) {
    buf.extend_from_slice(&u32::try_from(f.len()).unwrap_or(u32::MAX).to_le_bytes());
    buf.extend_from_slice(f);
}
