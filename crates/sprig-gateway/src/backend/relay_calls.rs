//! Sprig 2PC's holder calls from the gateway to the relay writer's agent:
//! prepare, finished and abandon (`sprig_txn::fence::wire`), on the
//! agent's internal service, over mutual TLS with the process's `[tls]`.
//!
//! Connections are kept per agent URL and reused one call at a time
//! (HTTP/1.1 keep-alive), so a held branch's prepare doesn't pay a TLS
//! handshake.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::client::conn::http1::SendRequest;
use sprig_cluster::failover::internal_tls::{InternalTls, split_url};
use sprig_txn::fence::wire::{HolderCall, decode_answer};
use sprig_txn::{DbError, DbResult};

/// How long one holder call may take.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);

static TLS: OnceLock<Option<InternalTls>> = OnceLock::new();
/// Idle keep-alive connections, per agent URL.
type Idle = Mutex<HashMap<String, Vec<SendRequest<Full<Bytes>>>>>;

static IDLE: OnceLock<Idle> = OnceLock::new();

/// Sets the TLS the calls use (the process's `[tls]`), once at start-up.
///
/// # Errors
/// 39998 for unreadable TLS files.
pub fn init(tls: Option<&sprig_common::config::TlsConfig>) -> sprig_common::Result<()> {
    let t = tls.map(InternalTls::from_config).transpose()?;
    let _ = TLS.set(t);
    Ok(())
}

fn idle() -> &'static Idle {
    IDLE.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn connect(url: &str) -> DbResult<SendRequest<Full<Bytes>>> {
    let lost = |e: String| DbError::lost(format!("relay agent {url}: {e}"));
    let (https, host, port) = split_url(url).map_err(|e| lost(e.message))?;
    if https {
        let tls = TLS
            .get()
            .and_then(Option::as_ref)
            .ok_or_else(|| lost("needs [tls], and this gateway has none".into()))?;
        let stream = tls
            .connect(&host, port)
            .await
            .map_err(|e| lost(e.message))?;
        let (tx, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|e| lost(e.to_string()))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(tx)
    } else {
        let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| lost(e.to_string()))?;
        let _ = tcp.set_nodelay(true);
        let (tx, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
            .await
            .map_err(|e| lost(e.to_string()))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(tx)
    }
}

async fn send(url: &str, call: &HolderCall) -> DbResult<()> {
    let lost = |e: String| DbError::lost(format!("relay agent {url}: {e}"));
    let reused = idle()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_mut(url)
        .and_then(Vec::pop);
    let mut tx = match reused {
        Some(t) if !t.is_closed() => t,
        _ => connect(url).await?,
    };
    tx.ready().await.map_err(|e| lost(e.to_string()))?;
    let (_, host, port) = split_url(url).map_err(|e| lost(e.message))?;
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(call.route())
        .header(hyper::header::HOST, format!("{host}:{port}"))
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from(call.encode())))
        .map_err(|e| lost(e.to_string()))?;
    let resp = tx
        .send_request(req)
        .await
        .map_err(|e| lost(e.to_string()))?;
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| lost(e.to_string()))?
        .to_bytes();
    idle()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(url.to_owned())
        .or_default()
        .push(tx);
    decode_answer(&String::from_utf8_lossy(&body))
}

/// Makes `call` on the agent at `url` (its internal service).
///
/// # Errors
/// The agent's answer (39448 while the tenant is fenced, the insert's
/// error); a lost-connection error when the agent can't be reached or the
/// call timed out (its outcome is then unknown).
pub async fn call(url: &str, call: &HolderCall) -> DbResult<()> {
    let started = std::time::Instant::now();
    let r = tokio::time::timeout(CALL_TIMEOUT, send(url, call))
        .await
        .unwrap_or_else(|_| Err(DbError::lost(format!("relay agent {url}: timed out"))));
    let kind = match call {
        HolderCall::Prepare { .. } => "prepare",
        HolderCall::Finished { .. } => "finished",
        HolderCall::Abandon { .. } => "abandon",
    };
    metrics::histogram!("sprig_txn_relay_call_seconds", "call" => kind)
        .record(started.elapsed().as_secs_f64());
    r
}
