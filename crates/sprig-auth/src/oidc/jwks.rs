//! Issuers' key sets: fetched, cached per process, refreshed.
//!
//! ```text
//! keys(source, refresh):
//!   cached and younger than refresh            → the cached keys
//!   else, unless a fetch started under a minute ago: fetch
//!      ok    → cache and use
//!      error → the cached keys while younger than 2 × refresh; then none (1045)
//! unknown kid: one refetch, at most once a minute per source
//! ```
//!
//! A source is the issuer's `jwks_url`, or, when it has none, OIDC
//! discovery: `<issuer>/.well-known/openid-configuration`, whose `issuer`
//! must equal the configured one and whose `jwks_uri` names the key set.
//! Key sets are fetched over HTTPS, checked against the Mozilla roots; plain
//! HTTP is accepted for loopback addresses only (a local identity provider,
//! and tests).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::Request;
use hyper_util::rt::TokioIo;

use super::jwt::{Jwk, parse_jwks};

/// Least time between two fetches of one source.
pub const MIN_REFETCH: Duration = Duration::from_secs(60);
/// A fetch's deadline.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
/// The largest document read.
pub const MAX_DOC_BYTES: usize = 1 << 20;

/// Fetches a document by URL.
#[async_trait]
pub trait Fetcher: Send + Sync + 'static {
    /// The body of a `200` answer to `GET url`.
    ///
    /// # Errors
    /// A message naming what failed.
    async fn get(&self, url: &str) -> Result<Vec<u8>, String>;
}

/// Whether `url` may be fetched: `https://`, or `http://` to a loopback
/// address.
///
/// # Errors
/// A message for any other URL.
pub fn check_url(url: &str) -> Result<(), String> {
    let uri: hyper::Uri = url.parse().map_err(|e| format!("{url}: {e}"))?;
    let host = uri.host().ok_or_else(|| format!("{url}: no host"))?;
    match uri.scheme_str() {
        Some("https") => Ok(()),
        Some("http") if is_loopback(host) => Ok(()),
        Some("http") => Err(format!(
            "{url}: plain http is accepted for loopback addresses only; use https"
        )),
        _ => Err(format!("{url}: not an http(s) URL")),
    }
}

fn is_loopback(host: &str) -> bool {
    let h = host.trim_start_matches('[').trim_end_matches(']');
    h.eq_ignore_ascii_case("localhost") || h.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The process's fetcher: hyper over HTTP/1.1, rustls with the Mozilla
/// roots for HTTPS.
pub struct HttpFetcher {
    tls: Arc<rustls::ClientConfig>,
}

impl std::fmt::Debug for HttpFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpFetcher").finish_non_exhaustive()
    }
}

impl Default for HttpFetcher {
    fn default() -> Self {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map(|b| b.with_root_certificates(roots).with_no_client_auth())
        .unwrap_or_else(|_| {
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth()
        });
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self { tls: Arc::new(cfg) }
    }
}

#[async_trait]
impl Fetcher for HttpFetcher {
    async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        check_url(url)?;
        let uri: hyper::Uri = url.parse().map_err(|e| format!("{url}: {e}"))?;
        let req = Request::get(uri.clone())
            .header(hyper::header::ACCEPT, "application/json")
            .header(hyper::header::USER_AGENT, "sprigdb")
            .body(Full::new(Bytes::new()))
            .map_err(|e| format!("{url}: {e}"))?;
        let fut = async {
            let resp = if uri.scheme_str() == Some("https") {
                sprig_common::https::send(&self.tls, req).await?
            } else {
                plain(req).await?
            };
            if resp.status() != hyper::StatusCode::OK {
                return Err(format!("{url}: HTTP {}", resp.status().as_u16()));
            }
            let body = Limited::new(resp.into_body(), MAX_DOC_BYTES)
                .collect()
                .await
                .map_err(|e| format!("{url}: {e}"))?;
            Ok(body.to_bytes().to_vec())
        };
        tokio::time::timeout(FETCH_TIMEOUT, fut)
            .await
            .map_err(|_| format!("{url}: no answer in {} s", FETCH_TIMEOUT.as_secs()))?
    }
}

async fn plain(
    req: Request<Full<Bytes>>,
) -> Result<hyper::Response<hyper::body::Incoming>, String> {
    let uri = req.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| format!("{uri}: no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(80);
    let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let (mut parts, body) = req.into_parts();
    if let Ok(v) = hyper::header::HeaderValue::from_str(&format!("{host}:{port}")) {
        parts.headers.insert(hyper::header::HOST, v);
    }
    if let Some(pq) = uri.path_and_query()
        && let Ok(u) = pq.as_str().parse()
    {
        parts.uri = u;
    }
    sender
        .send_request(Request::from_parts(parts, body))
        .await
        .map_err(|e| format!("{host}:{port}: {e}"))
}

/// Where an issuer's keys come from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Source {
    /// A configured `jwks_url`.
    Url(String),
    /// OIDC discovery from the issuer.
    Discovery(String),
}

impl Source {
    fn key(&self) -> String {
        match self {
            Source::Url(u) => format!("url {u}"),
            Source::Discovery(i) => format!("discovery {i}"),
        }
    }
}

#[derive(Default)]
struct State {
    keys: Option<Arc<Vec<Jwk>>>,
    fetched: Option<Instant>,
    attempted: Option<Instant>,
}

#[derive(Default)]
struct Entry {
    state: Mutex<State>,
    fetching: tokio::sync::Mutex<()>,
}

/// Why no keys are available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unavailable(pub String);

/// The process's key sets, by source.
pub struct JwksCache {
    fetcher: Arc<dyn Fetcher>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    min_refetch: Duration,
}

impl std::fmt::Debug for JwksCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwksCache").finish_non_exhaustive()
    }
}

impl JwksCache {
    /// Key sets fetched by `fetcher`.
    #[must_use]
    pub fn new(fetcher: Arc<dyn Fetcher>) -> Self {
        Self {
            fetcher,
            entries: Mutex::new(HashMap::new()),
            min_refetch: MIN_REFETCH,
        }
    }

    /// The same cache refetching at most every `d` (tests).
    #[must_use]
    pub fn with_min_refetch(mut self, d: Duration) -> Self {
        self.min_refetch = d;
        self
    }

    fn entry(&self, src: &Source) -> Arc<Entry> {
        let mut m = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(m.entry(src.key()).or_default())
    }

    /// The keys of `src`, refreshed when older than `refresh`; with
    /// `need_kid`, refetched once (at most every minute) when no key has
    /// that id.
    ///
    /// # Errors
    /// [`Unavailable`] when there are no keys: never fetched, or the last
    /// good fetch is older than twice `refresh` and fetching fails.
    pub async fn keys(
        &self,
        src: &Source,
        refresh: Duration,
        need_kid: Option<&str>,
    ) -> Result<Arc<Vec<Jwk>>, Unavailable> {
        let e = self.entry(src);
        let lookup = |s: &State, now: Instant| -> (Option<Arc<Vec<Jwk>>>, bool, bool) {
            let age = s.fetched.map(|t| now.saturating_duration_since(t));
            let fresh = age.is_some_and(|a| a < refresh);
            let usable = age.is_some_and(|a| a < refresh.saturating_mul(2));
            let has_kid = match (need_kid, &s.keys) {
                (Some(k), Some(keys)) => keys.iter().any(|j| j.kid.as_deref() == Some(k)),
                _ => true,
            };
            let may_fetch = s
                .attempted
                .is_none_or(|t| now.saturating_duration_since(t) >= self.min_refetch);
            let want = (!fresh || !has_kid) && may_fetch;
            (s.keys.clone().filter(|_| usable), want, has_kid)
        };
        {
            let s = e.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (keys, want, _) = lookup(&s, Instant::now());
            if !want {
                return keys.ok_or_else(|| Unavailable("no key set fetched yet".into()));
            }
        }
        // One fetch at a time per source; others wait and reuse it.
        let _g = e.fetching.lock().await;
        {
            let s = e.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (keys, want, _) = lookup(&s, Instant::now());
            if !want {
                return keys.ok_or_else(|| Unavailable("no key set fetched yet".into()));
            }
        }
        e.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .attempted = Some(Instant::now());
        let r = self.fetch(src).await;
        let mut s = e.state.lock().unwrap_or_else(PoisonError::into_inner);
        match r {
            Ok(keys) => {
                metrics::counter!("sprig_auth_jwks_fetches_total", "outcome" => "ok").increment(1);
                let keys = Arc::new(keys);
                s.keys = Some(Arc::clone(&keys));
                s.fetched = Some(Instant::now());
                Ok(keys)
            }
            Err(why) => {
                metrics::counter!("sprig_auth_jwks_fetches_total", "outcome" => "error")
                    .increment(1);
                tracing::warn!(source = %src.key(), error = %why, "JWKS fetch failed");
                let (keys, _, _) = lookup(&s, Instant::now());
                keys.ok_or(Unavailable(why))
            }
        }
    }

    /// Fetches the key set (discovery first, every time, so a moved
    /// `jwks_uri` is followed).
    async fn fetch(&self, src: &Source) -> Result<Vec<Jwk>, String> {
        let url = match src {
            Source::Url(u) => u.clone(),
            Source::Discovery(issuer) => self.discover(issuer).await?,
        };
        let doc = self.fetcher.get(&url).await?;
        let keys = parse_jwks(&doc)?;
        if keys.is_empty() {
            return Err(format!("{url}: no signing key SprigDB can use"));
        }
        Ok(keys)
    }

    async fn discover(&self, issuer: &str) -> Result<String, String> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        let doc = self.fetcher.get(&url).await?;
        let v: serde_json::Value =
            serde_json::from_slice(&doc).map_err(|e| format!("{url}: {e}"))?;
        if v.get("issuer").and_then(serde_json::Value::as_str) != Some(issuer) {
            return Err(format!("{url}: its issuer isn't {issuer}"));
        }
        let uri = v
            .get("jwks_uri")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("{url}: no jwks_uri"))?;
        check_url(uri)?;
        Ok(uri.to_owned())
    }
}
