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

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::NodeId;
use crate::node::Node;
use crate::transport::Transport;

/// The largest frame accepted.
pub const MAX_FRAME: usize = 64 << 20;
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
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("the server config", &e))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key()?)
            .map_err(|e| bad("the server config", &e))?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| bad("the client config", &e))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key()?)
            .map_err(|e| bad("the client config", &e))?;
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

/// The TCP transport of one node.
pub struct TcpTransport {
    me: NodeId,
    handle: tokio::runtime::Handle,
    tls: Option<TlsConfig>,
    peers: Mutex<BTreeMap<NodeId, SocketAddr>>,
    queues: Mutex<BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>>,
    /// Peers cut off (a test's network partition): frames to them and
    /// from them are dropped.
    cut: Arc<RwLock<BTreeSet<NodeId>>>,
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
            tls,
            peers: Mutex::new(BTreeMap::new()),
            queues: Mutex::new(BTreeMap::new()),
            cut: Arc::new(RwLock::new(BTreeSet::new())),
        })
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
        let tls = self.tls.clone();
        let cut = Arc::clone(&self.cut);
        self.handle.spawn(async move {
            loop {
                let Ok((s, _)) = l.accept().await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let _ = s.set_nodelay(true);
                let node = node.clone();
                let tls = tls.clone();
                let cut = Arc::clone(&cut);
                tokio::spawn(async move {
                    match tls {
                        Some(t) => {
                            let acc = tokio_rustls::TlsAcceptor::from(Arc::clone(&t.server));
                            let Ok(s) = acc.accept(s).await else {
                                return;
                            };
                            // With node identity, the peer is the node its
                            // certificate names, and only that node.
                            let peer = if t.node_names {
                                let named = s
                                    .get_ref()
                                    .1
                                    .peer_certificates()
                                    .and_then(|c| c.first())
                                    .and_then(|c| cert_node_id(c));
                                let Some(id) = named else {
                                    tracing::warn!(node = node.id(), "raft tcp: a peer's certificate names no node; connection refused");
                                    return;
                                };
                                Some(id)
                            } else {
                                None
                            };
                            read_frames(s, &node, peer, &cut).await;
                        }
                        None => read_frames(s, &node, None, &cut).await,
                    }
                });
            }
        });
        Ok(bound)
    }

    fn queue(&self, to: NodeId) -> Option<mpsc::Sender<Vec<u8>>> {
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
        self.handle
            .spawn(write_frames(addr, self.tls.clone(), rx, self.me, to));
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

/// The sender a frame claims (its header's `from`).
fn frame_sender(frame: &[u8]) -> Option<NodeId> {
    let b: [u8; 8] = frame.get(2..10)?.try_into().ok()?;
    Some(NodeId::from_le_bytes(b))
}

async fn read_frames<S: AsyncRead + Unpin>(
    mut s: S,
    node: &Node,
    peer: Option<NodeId>,
    cut: &RwLock<BTreeSet<NodeId>>,
) {
    let mut len = [0u8; 4];
    loop {
        if s.read_exact(&mut len).await.is_err() {
            return;
        }
        let n = u32::from_le_bytes(len) as usize;
        if n > MAX_FRAME {
            return;
        }
        let mut buf = vec![0u8; n];
        if s.read_exact(&mut buf).await.is_err() {
            return;
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

async fn connect(addr: SocketAddr, tls: Option<&TlsConfig>, to: NodeId) -> io::Result<Conn> {
    let s = TcpStream::connect(addr).await?;
    s.set_nodelay(true)?;
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
    tls: Option<TlsConfig>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    me: NodeId,
    to: NodeId,
) {
    let mut backoff = Duration::from_millis(50);
    let mut buf = Vec::with_capacity(64 << 10);
    loop {
        let mut conn = match connect(addr, tls.as_ref(), to).await {
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
        loop {
            let Some(f) = rx.recv().await else {
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
