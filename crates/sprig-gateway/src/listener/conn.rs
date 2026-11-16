//! One accepted connection, up to the front end: the PROXY header, TLS and
//! the hand-over to the protocol's front end.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::proxy::{self, ProxyError};
use super::{Active, TlsMode, count_handshake};
use crate::frontend::{ClientConn, Frontend, GatewayContext, InProtocolTls, MaybeTls, Protocol};

/// The first byte of a TLS record carrying a handshake message: every
/// `ClientHello` starts with it.
pub(crate) const TLS_HANDSHAKE_RECORD: u8 = 0x16;

/// Sent on a `required` HTTP listener to a plaintext request.
const HTTP_TLS_REQUIRED: &[u8] = b"HTTP/1.1 400 Bad Request\r\n\
content-type: text/plain\r\ncontent-length: 25\r\nconnection: close\r\n\r\n\
This listener needs TLS.\n";

/// How long the HTTP refusal waits for the client to close, so the
/// response isn't lost to a reset.
const HTTP_REJECT_LINGER: Duration = Duration::from_millis(500);

/// Whether a connection that starts with `head` (its first bytes, at most
/// [`TLS_SNIFF`]) starts a TLS handshake: a handshake record (`0x16`) of
/// protocol version 3.x (SSL 3.0 to TLS 1.3 put `03 00` to `03 04` there)
/// whose first message is a `ClientHello` (`01`). The first byte alone
/// isn't enough: a MongoDB message starts with its length in little-endian
/// order, so a plaintext `hello` of 278, 534 or 790 bytes also starts with
/// `0x16` (the Rust driver's does, depending on its client metadata).
pub(crate) fn starts_tls(head: &[u8]) -> bool {
    match head {
        [TLS_HANDSHAKE_RECORD, 0x03, minor, _, _, hello, ..] => *minor <= 0x04 && *hello == 0x01,
        _ => false,
    }
}

/// How many first bytes [`starts_tls`] looks at.
pub(crate) const TLS_SNIFF: usize = 6;

/// Keeps `sprig_gateway_connections{listener, state="open"}` in step.
struct OpenGuard(&'static str);

impl OpenGuard {
    fn new(listener: &'static str) -> Self {
        metrics::gauge!("sprig_gateway_connections", "listener" => listener, "state" => "open")
            .increment(1.0);
        Self(listener)
    }
}

impl Drop for OpenGuard {
    fn drop(&mut self) {
        metrics::gauge!("sprig_gateway_connections", "listener" => self.0, "state" => "open")
            .decrement(1.0);
    }
}

fn reject(listener: &'static str, reason: &'static str) {
    metrics::counter!("sprig_gateway_connections_rejected_total", "listener" => listener, "reason" => reason).increment(1);
}

/// The connection task: prepares the connection and runs the front end
/// until it returns or the listener retires the connection.
pub(super) async fn run(
    sock: TcpStream,
    peer: SocketAddr,
    active: Arc<Active>,
    fe: Arc<dyn Frontend>,
    ctx: Arc<GatewayContext>,
    permit: Option<crate::limits::ConnPermit>,
) {
    let _permit = permit;
    let name = active.spec.name;
    let _open = OpenGuard::new(name);
    // This connection's place in its generation: its own close tokens, and
    // its transport, which a stricter listener change checks.
    let slot = active.group.join();
    let close = slot.close.clone();
    let prepared = tokio::select! {
        biased;
        () = close.cancelled() => return,
        r = tokio::time::timeout(active.spec.handshake_timeout, prepare(sock, peer, &active, &slot)) => r,
    };
    let conn = match prepared {
        Ok(Ok(conn)) => conn,
        Ok(Err(reason)) => {
            tracing::debug!(listener = name, %peer, reason, "connection refused");
            return reject(name, reason);
        }
        Err(_) => {
            tracing::debug!(listener = name, %peer, "connection phase timed out");
            return reject(name, "handshake_timeout");
        }
    };
    let peer = conn.peer;
    tokio::select! {
        biased;
        () = close.cancelled() => {
            tracing::debug!(listener = name, %peer, "connection closed: listener retired");
        }
        r = fe.serve(conn, ctx) => {
            if let Err(e) = r {
                tracing::debug!(listener = name, %peer, error = %e, "connection ended with an error");
            }
        }
    }
}

/// The PROXY header, then TLS by mode.
async fn prepare(
    mut sock: TcpStream,
    peer: SocketAddr,
    active: &Active,
    slot: &super::ConnSlot,
) -> Result<ClientConn, &'static str> {
    let spec = &active.spec;
    let speaks_first = spec.protocol.client_speaks_first();
    let peer = proxy::accept(&mut sock, peer, &spec.proxy, speaks_first)
        .await
        .map_err(|e| match e {
            ProxyError::Missing => "proxy_missing",
            ProxyError::Malformed(_) => "proxy_malformed",
            ProxyError::Io(_) => "io",
        })?;
    let _ = sock.set_nodelay(true);

    let (stream, in_protocol_tls) = if !speaks_first {
        // The MySQL family negotiates TLS after the greeting.
        (MaybeTls::Plain(sock), InProtocolTls::for_mode(spec.tls))
    } else if spec.tls == TlsMode::Off {
        (MaybeTls::Plain(sock), InProtocolTls::NotOffered)
    } else {
        let mut head = [0u8; TLS_SNIFF];
        let mut n = sock.peek(&mut head).await.map_err(|_| "io")?;
        if n == 0 {
            return Err("closed");
        }
        // A first byte of 0x16 may be TLS: wait briefly for the rest of the
        // record header (a `ClientHello` always has it) before deciding.
        let mut waited = 0;
        while head[0] == TLS_HANDSHAKE_RECORD && n < TLS_SNIFF && waited < 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            waited += 1;
            n = sock.peek(&mut head).await.map_err(|_| "io")?;
        }
        if starts_tls(&head[..n]) {
            let acceptor = active.acceptor.clone().ok_or("no_certificate")?;
            let Ok(tls) = acceptor.accept(sock).await else {
                count_handshake(spec.name, None, "error");
                return Err("tls_handshake");
            };
            let stream = MaybeTls::Tls(Box::new(tls));
            count_handshake(spec.name, stream.tls_info().as_ref(), "ok");
            (stream, InProtocolTls::NotOffered)
        } else if spec.tls == TlsMode::Required {
            if spec.protocol == Protocol::Http {
                refuse_http(sock).await;
            }
            return Err("tls_required");
        } else {
            (MaybeTls::Plain(sock), InProtocolTls::NotOffered)
        }
    };
    slot.transport.set(stream.tls_info().as_ref());
    let acceptor = match in_protocol_tls {
        InProtocolTls::NotOffered => None,
        InProtocolTls::Allowed | InProtocolTls::Required => active.acceptor.clone(),
    };
    Ok(ClientConn {
        stream,
        peer,
        listener: spec.clone(),
        generation: active.generation,
        in_protocol_tls,
        retired: slot.close.clone(),
        close_soon: slot.soft.clone(),
        transport: Arc::clone(&slot.transport),
        acceptor,
    })
}

/// Refuses a connection over `gateway.max_connections`: the MySQL family
/// gets ERR 1040 in place of the greeting, plaintext HTTP a 503; the rest
/// are closed.
pub(super) async fn refuse_too_many(mut sock: TcpStream, protocol: Protocol, tls: TlsMode) {
    match protocol {
        Protocol::MySql84 | Protocol::MariaDb => {
            let p = sprig_proto_mysql::ErrPacket::new(1040, *b"08004", "Too many connections")
                .encode(sprig_proto_mysql::Caps::empty());
            let mut frame = Vec::with_capacity(p.len() + 4);
            let len = u32::try_from(p.len()).unwrap_or(0).to_le_bytes();
            frame.extend_from_slice(&len[..3]);
            frame.push(0);
            frame.extend_from_slice(&p);
            let _ = sock.write_all(&frame).await;
        }
        Protocol::Http if tls != TlsMode::Required => {
            let _ = sock
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\ncontent-type: text/plain\r\n\
                      retry-after: 1\r\ncontent-length: 21\r\nconnection: close\r\n\r\n\
                      Too many connections\n",
                )
                .await;
        }
        _ => {}
    }
    let _ = sock.shutdown().await;
}

/// Refuses a connection while the DC returns from an outage:
/// MySQL gets 39531 in place of the greeting, HTTP a 503 with
/// `Retry-After`.
pub(super) async fn refuse_admission(
    mut sock: TcpStream,
    protocol: Protocol,
    tls: TlsMode,
    wait: std::time::Duration,
) {
    match protocol {
        Protocol::MySql84 | Protocol::MariaDb => {
            let p = crate::failover::admission_err_packet(wait);
            let mut frame = Vec::with_capacity(p.len() + 4);
            let len = u32::try_from(p.len()).unwrap_or(0).to_le_bytes();
            frame.extend_from_slice(&len[..3]);
            frame.push(0);
            frame.extend_from_slice(&p);
            let _ = sock.write_all(&frame).await;
        }
        Protocol::Http if tls != TlsMode::Required => {
            let secs = wait.as_secs().max(1);
            let body = "Admission backoff: the data center is returning from an outage\n";
            let head = format!(
                "HTTP/1.1 503 Service Unavailable\r\ncontent-type: text/plain\r\n\
                 retry-after: {secs}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(head.as_bytes()).await;
        }
        _ => {}
    }
    let _ = sock.shutdown().await;
}

/// Answers a plaintext HTTP request on a `required` listener with 400.
async fn refuse_http(mut sock: TcpStream) {
    if sock.write_all(HTTP_TLS_REQUIRED).await.is_err() || sock.shutdown().await.is_err() {
        return;
    }
    // Read what the client sent until it closes, so closing with unread
    // data doesn't reset the connection before the response is read.
    let mut sink = [0u8; 1024];
    let drain = async { while matches!(sock.read(&mut sink).await, Ok(n) if n > 0) {} };
    let _ = tokio::time::timeout(HTTP_REJECT_LINGER, drain).await;
}
