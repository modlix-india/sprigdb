//! Catalog records reached through the engine node's peer service
//! (`sprig.internal.v1.CatalogPeer`), for processes that don't open the
//! engine themselves: the controller and workers.
//!
//! One record transaction is one `Txn` stream: the gateway runs every
//! operation of the stream in one engine transaction, and the stream
//! ending before a commit (this process crashed, or dropped the
//! transaction) rolls it back.
//!
//! **Which gateway.** `SPRIGDB_CATALOG_NODE` (`host:port` of the engine
//! node's peer listener) when set; otherwise the one ready gateway with a
//! peer port registered in this DC. None, or several without the
//! variable, is 39070 (retryable).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_api::proto::internal::catalog_op::Op;
use sprig_api::proto::internal::catalog_peer_client::CatalogPeerClient;
use sprig_api::proto::internal::{CatalogEntry, CatalogOp, CatalogReply, CatalogScan};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use tokio::sync::mpsc;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use super::{RecordStore, RecordTx, unavailable};

/// The variable naming the engine node's peer listener.
pub const NODE_VAR: &str = "SPRIGDB_CATALOG_NODE";

/// Deadline of one operation (a lock wait is shorter: 10 s).
const OP_TIMEOUT: Duration = Duration::from_secs(30);

/// The process identity for the peer service's mutual TLS.
#[derive(Clone)]
pub struct PeerPems {
    /// The internal CA.
    pub ca: Vec<u8>,
    /// This process's chain.
    pub cert: Vec<u8>,
    /// Its key.
    pub key: Vec<u8>,
}

impl std::fmt::Debug for PeerPems {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerPems").finish_non_exhaustive()
    }
}

impl PeerPems {
    /// Reads `[tls]`.
    ///
    /// # Errors
    /// 39998 for an unreadable file.
    pub fn from_config(cfg: &sprig_common::config::TlsConfig) -> Result<Self> {
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[tls] {}: {e}", p.display()),
                )
            })
        };
        Ok(Self {
            ca: read(&cfg.ca)?,
            cert: read(&cfg.cert)?,
            key: read(&cfg.key)?,
        })
    }
}

/// The parts of a gateway's registration record needed here.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GwRecord {
    gateway_id: String,
    addr: String,
    #[serde(default)]
    listeners: BTreeMap<String, u16>,
    state: String,
}

impl Versioned for GwRecord {
    const V: u32 = 1;
}

/// Catalog records on the engine node.
pub struct RemoteRecords {
    dc: Option<Arc<dyn MetaStore>>,
    fixed: Option<String>,
    tls: Option<PeerPems>,
    /// The channel and the address it goes to, found at most every 2 s.
    channel: Mutex<Option<(Instant, String, Channel)>>,
}

impl std::fmt::Debug for RemoteRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRecords")
            .field("fixed", &self.fixed)
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

impl RemoteRecords {
    /// Records on the gateway named by `SPRIGDB_CATALOG_NODE`, or found in
    /// `dc`'s gateway registrations; reached with `tls` (`None`:
    /// plaintext, development).
    #[must_use]
    pub fn new(dc: Option<Arc<dyn MetaStore>>, tls: Option<PeerPems>) -> Self {
        let fixed = std::env::var(NODE_VAR)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty());
        Self {
            dc,
            fixed,
            tls,
            channel: Mutex::new(None),
        }
    }

    /// Records on the peer listener at `addr` (`host:port`).
    #[must_use]
    pub fn at(addr: impl Into<String>, tls: Option<PeerPems>) -> Self {
        Self {
            dc: None,
            fixed: Some(addr.into()),
            tls,
            channel: Mutex::new(None),
        }
    }

    async fn node(&self) -> Result<(String, String)> {
        if let Some(a) = &self.fixed {
            let host = a.rsplit_once(':').map_or(a.as_str(), |(h, _)| h).to_owned();
            return Ok((a.clone(), host));
        }
        let Some(dc) = &self.dc else {
            return Err(unavailable(format!(
                "no engine node: set {NODE_VAR} to its peer address"
            )));
        };
        let prefix = dc.tier().key("gateways/")?;
        let page = dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await
            .map_err(|e| unavailable(format!("gateway registrations: {e}")))?;
        let ready: Vec<GwRecord> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<GwRecord>::decode(&kv.value).ok())
            .map(|r| r.body)
            .filter(|g| g.state == "ready" && g.listeners.contains_key("peer"))
            .collect();
        match ready.as_slice() {
            [g] => {
                let port = g.listeners.get("peer").copied().unwrap_or(7403);
                Ok((format!("{}:{port}", g.addr), g.addr.clone()))
            }
            [] => Err(unavailable(
                "no ready gateway with a peer port in this DC holds the catalog",
            )),
            _ => Err(unavailable(format!(
                "{} ready gateways in this DC: set {NODE_VAR} to the engine node's peer address",
                ready.len()
            ))),
        }
    }

    async fn client(&self) -> Result<CatalogPeerClient<Channel>> {
        if let Some((at, _, ch)) = &*self.channel.lock().unwrap_or_else(PoisonError::into_inner)
            && (self.fixed.is_some() || at.elapsed() < Duration::from_secs(2))
        {
            return Ok(CatalogPeerClient::new(ch.clone()));
        }
        let (addr, host) = self.node().await?;
        let mut slot = self.channel.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((at, a, ch)) = slot.as_mut()
            && *a == addr
        {
            *at = Instant::now();
            return Ok(CatalogPeerClient::new(ch.clone()));
        }
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{addr}");
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unavailable(format!("peer address {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(5));
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca))
                .identity(Identity::from_pem(&t.cert, &t.key))
                .domain_name(host);
            ep = ep
                .tls_config(cfg)
                .map_err(|e| unavailable(format!("peer TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        *slot = Some((Instant::now(), addr, ch.clone()));
        Ok(CatalogPeerClient::new(ch))
    }

    fn forget(&self) {
        *self.channel.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

#[async_trait::async_trait]
impl RecordStore for RemoteRecords {
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        let mut client = self.client().await?;
        let (send, recv) = mpsc::channel::<CatalogOp>(4);
        let ops = futures::stream::unfold(recv, next_op);
        let mut tx = RemoteTx {
            send: Some(send),
            replies: None,
        };
        // The first message must be on its way before the call returns
        // its response stream.
        tx.queue(Op::BeginTenant(tenant.0)).await?;
        let replies = match tokio::time::timeout(OP_TIMEOUT, client.txn(ops)).await {
            Ok(Ok(r)) => r.into_inner(),
            Ok(Err(s)) => {
                self.forget();
                return Err(unavailable(format!("engine node: {}", s.message())));
            }
            Err(_) => {
                self.forget();
                return Err(unavailable("engine node didn't answer"));
            }
        };
        tx.replies = Some(replies);
        tx.reply().await?;
        Ok(Box::new(tx))
    }
}

/// The next operation to send, for the request stream.
async fn next_op(mut r: mpsc::Receiver<CatalogOp>) -> Option<(CatalogOp, mpsc::Receiver<CatalogOp>)> {
    r.recv().await.map(|op| (op, r))
}

struct RemoteTx {
    send: Option<mpsc::Sender<CatalogOp>>,
    replies: Option<tonic::Streaming<CatalogReply>>,
}

impl RemoteTx {
    async fn queue(&mut self, op: Op) -> Result<()> {
        let Some(s) = &self.send else {
            return Err(unavailable("the catalog transaction has ended"));
        };
        s.send(CatalogOp { op: Some(op) })
            .await
            .map_err(|_| unavailable("the engine node closed the catalog transaction"))
    }

    async fn reply(&mut self) -> Result<CatalogReply> {
        let Some(r) = &mut self.replies else {
            return Err(unavailable("the catalog transaction has ended"));
        };
        let m = match tokio::time::timeout(OP_TIMEOUT, r.message()).await {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => {
                return Err(unavailable(
                    "the engine node ended the catalog transaction",
                ));
            }
            Ok(Err(s)) => return Err(unavailable(format!("engine node: {}", s.message()))),
            Err(_) => return Err(unavailable("engine node didn't answer")),
        };
        if m.error_code != 0 {
            let code = u16::try_from(m.error_code)
                .ok()
                .and_then(SprigCode::from_number)
                .unwrap_or(SprigCode::Internal);
            return Err(SprigError::new(code, m.error_message));
        }
        Ok(m)
    }

    async fn call(&mut self, op: Op) -> Result<CatalogReply> {
        self.queue(op).await?;
        self.reply().await
    }
}

fn found(r: CatalogReply) -> Option<Vec<u8>> {
    r.found.then_some(r.value)
}

#[async_trait::async_trait]
impl RecordTx for RemoteTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.call(Op::Get(key.to_vec())).await.map(found)
    }

    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.call(Op::GetForUpdate(key.to_vec())).await.map(found)
    }

    async fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let r = self
            .call(Op::Scan(CatalogScan {
                lo: lo.to_vec(),
                hi: hi.to_vec(),
                limit: u32::try_from(limit).unwrap_or(u32::MAX),
            }))
            .await?;
        Ok(r.entries.into_iter().map(|e| (e.key, e.value)).collect())
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.call(Op::Put(CatalogEntry {
            key: key.to_vec(),
            value: value.to_vec(),
        }))
        .await
        .map(drop)
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.call(Op::Delete(key.to_vec())).await.map(drop)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let r = self.call(Op::Commit(true)).await;
        self.send = None;
        r.map(drop)
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        let r = self.call(Op::Rollback(true)).await;
        self.send = None;
        r.map(drop)
    }
}
