//! Frames over TCP, optionally with mutual TLS.
//!
//! Each node keeps one outgoing connection per peer node and accepts the
//! peers' connections on its listener. A connection carries length-prefixed
//! frames (`u32` little-endian length, then the frame). The writer drains
//! every frame queued for the peer into one write, so the frames of several
//! driver turns travel together under load. A full queue (a peer that is
//! down) drops new frames: Raft resends what matters.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
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
    /// The name every peer's certificate is checked against.
    pub server_name: ServerName<'static>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// The TCP transport of one node.
pub struct TcpTransport {
    me: NodeId,
    handle: tokio::runtime::Handle,
    tls: Option<TlsConfig>,
    peers: Mutex<BTreeMap<NodeId, SocketAddr>>,
    queues: Mutex<BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>>,
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
        })
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
        self.handle.spawn(async move {
            loop {
                let Ok((s, _)) = l.accept().await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let _ = s.set_nodelay(true);
                let node = node.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    match tls {
                        Some(t) => {
                            let acc = tokio_rustls::TlsAcceptor::from(Arc::clone(&t.server));
                            if let Ok(s) = acc.accept(s).await {
                                read_frames(s, &node).await;
                            }
                        }
                        None => read_frames(s, &node).await,
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
        if let Some(q) = self.queue(to) {
            let _ = q.try_send(frame);
        }
    }
}

async fn read_frames<S: AsyncRead + Unpin>(mut s: S, node: &Node) {
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
        node.deliver(buf);
    }
}

async fn connect(addr: SocketAddr, tls: Option<&TlsConfig>) -> io::Result<Conn> {
    let s = TcpStream::connect(addr).await?;
    s.set_nodelay(true)?;
    Ok(match tls {
        Some(t) => {
            let c = tokio_rustls::TlsConnector::from(Arc::clone(&t.client));
            Conn::Tls(Box::new(c.connect(t.server_name.clone(), s).await?))
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
        let mut conn = match connect(addr, tls.as_ref()).await {
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
