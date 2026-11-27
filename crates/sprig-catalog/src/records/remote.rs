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
//! node's peer listener) when set. Otherwise, among the ready gateways with
//! a peer port registered in this DC, the one leading the tenant's unit:
//! a gateway that doesn't lead it refuses the transaction with 39110 and,
//! when it knows, the leader's Raft id, so this side tries the node that
//! last accepted the tenant, the unit's home node and its other voters (or
//! every replicated gateway when the unit has no descriptor), following
//! hints, for up to 15 s. A DC whose engines don't replicate must have
//! exactly one ready gateway: none, or several, is 39070 (retryable).

use std::collections::{BTreeMap, HashMap};
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
use crate::units::UnitDescriptor;

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
    /// Its engine's Raft id, when its engine is one replica of several.
    #[serde(default)]
    raft_node: Option<u64>,
}

impl Versioned for GwRecord {
    const V: u32 = 1;
}

/// One peer listener to try.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    /// `host:port`.
    addr: String,
    /// The host, for TLS.
    host: String,
    /// The gateway's Raft id (`None`: a one-node engine).
    raft: Option<u64>,
}

/// How long a listing of the DC's gateways is reused.
const GATEWAYS_TTL: Duration = Duration::from_secs(2);
/// How long a tenant's unit descriptor is reused (absent: shorter).
const UNIT_TTL: Duration = Duration::from_secs(10);
const NO_UNIT_TTL: Duration = Duration::from_secs(1);
/// How long `begin` keeps looking for the tenant's leader.
const FIND_LEADER: Duration = Duration::from_secs(15);

/// The node a "not the leader" answer names (`... leader node <id> ...`).
#[must_use]
pub fn leader_hint(message: &str) -> Option<u64> {
    let at = message.find("leader node ")? + "leader node ".len();
    let digits: String = message[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// Catalog records on the engine node.
pub struct RemoteRecords {
    dc: Option<Arc<dyn MetaStore>>,
    fixed: Option<String>,
    tls: Option<PeerPems>,
    /// A channel per peer listener.
    channels: Mutex<HashMap<String, Channel>>,
    /// The DC's ready gateways with a peer port, and when they were read.
    gateways: Mutex<Option<(Instant, Vec<GwRecord>)>>,
    /// Per tenant: its unit's descriptor, and when it was read.
    units: Mutex<HashMap<u64, (Instant, Option<UnitDescriptor>)>>,
    /// Per tenant: the Raft id of the node that last accepted its catalog
    /// transactions (its unit's leader then).
    leaders: Mutex<HashMap<u64, u64>>,
}

impl std::fmt::Debug for RemoteRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRecords")
            .field("fixed", &self.fixed)
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl RemoteRecords {
    /// Records on the gateway named by `SPRIGDB_CATALOG_NODE`, or on the
    /// node leading each tenant's unit among `dc`'s gateway registrations;
    /// reached with `tls` (`None`: plaintext, development).
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
            channels: Mutex::new(HashMap::new()),
            gateways: Mutex::new(None),
            units: Mutex::new(HashMap::new()),
            leaders: Mutex::new(HashMap::new()),
        }
    }

    /// Records on the peer listener at `addr` (`host:port`).
    #[must_use]
    pub fn at(addr: impl Into<String>, tls: Option<PeerPems>) -> Self {
        Self {
            dc: None,
            fixed: Some(addr.into()),
            tls,
            channels: Mutex::new(HashMap::new()),
            gateways: Mutex::new(None),
            units: Mutex::new(HashMap::new()),
            leaders: Mutex::new(HashMap::new()),
        }
    }

    /// The DC's ready gateways with a peer port (listed at most every
    /// [`GATEWAYS_TTL`]).
    async fn gateways(&self, dc: &Arc<dyn MetaStore>, fresh: bool) -> Result<Vec<GwRecord>> {
        if !fresh
            && let Some((at, g)) = &*lock(&self.gateways)
            && at.elapsed() < GATEWAYS_TTL
        {
            return Ok(g.clone());
        }
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
        *lock(&self.gateways) = Some((Instant::now(), ready.clone()));
        Ok(ready)
    }

    /// `tenant`'s unit descriptor (read at most every [`UNIT_TTL`]; with
    /// `fresh`, an absent one is read again at once: a new tenant's
    /// descriptor appears between its first catalog calls).
    async fn unit(
        &self,
        dc: &Arc<dyn MetaStore>,
        tenant: u64,
        fresh: bool,
    ) -> Option<UnitDescriptor> {
        if let Some((at, d)) = lock(&self.units).get(&tenant)
            && at.elapsed() < if d.is_some() { UNIT_TTL } else { NO_UNIT_TTL }
            && !(fresh && d.is_none())
        {
            return d.clone();
        }
        match crate::units::read(dc.as_ref(), tenant).await {
            Ok(d) => {
                lock(&self.units).insert(tenant, (Instant::now(), d.clone()));
                d
            }
            Err(e) => {
                tracing::debug!(tenant, error = %e, "catalog: unit descriptor unreadable");
                None
            }
        }
    }

    /// The peer listeners to try for `tenant`, the likeliest leader first:
    /// the node that last accepted it, the unit's home, its other voters;
    /// without a descriptor, every replicated gateway. A DC of one-node
    /// engines must have exactly one.
    async fn targets(&self, tenant: u64, fresh: bool) -> Result<Vec<Target>> {
        if let Some(a) = &self.fixed {
            let host = a.rsplit_once(':').map_or(a.as_str(), |(h, _)| h).to_owned();
            return Ok(vec![Target {
                addr: a.clone(),
                host,
                raft: None,
            }]);
        }
        let Some(dc) = &self.dc else {
            return Err(unavailable(format!(
                "no engine node: set {NODE_VAR} to its peer address"
            )));
        };
        let ready = self.gateways(dc, fresh).await?;
        let target = |g: &GwRecord| Target {
            addr: format!(
                "{}:{}",
                g.addr,
                g.listeners.get("peer").copied().unwrap_or(7403)
            ),
            host: g.addr.clone(),
            raft: g.raft_node,
        };
        let replicated: Vec<&GwRecord> = ready.iter().filter(|g| g.raft_node.is_some()).collect();
        if replicated.is_empty() {
            return match ready.as_slice() {
                [g] => Ok(vec![target(g)]),
                [] => Err(unavailable(
                    "no ready gateway with a peer port in this DC holds the catalog",
                )),
                _ => Err(unavailable(format!(
                    "{} ready gateways in this DC, none replicated: set {NODE_VAR} to the engine node's peer address",
                    ready.len()
                ))),
            };
        }
        // Raft ids in the order to try.
        let mut order: Vec<u64> = Vec::new();
        if let Some(l) = lock(&self.leaders).get(&tenant) {
            order.push(*l);
        }
        if let Some(d) = self.unit(dc, tenant, fresh).await {
            order.push(d.home);
            order.extend(d.voters.iter().copied());
            order.retain(|id| d.voters.contains(id));
        } else {
            let mut ids: Vec<u64> = replicated.iter().filter_map(|g| g.raft_node).collect();
            ids.sort_unstable();
            order.extend(ids);
        }
        let mut seen = Vec::new();
        order.retain(|id| {
            let new = !seen.contains(id);
            seen.push(*id);
            new
        });
        let out: Vec<Target> = order
            .iter()
            .filter_map(|id| replicated.iter().find(|g| g.raft_node == Some(*id)))
            .map(|g| target(g))
            .collect();
        if out.is_empty() {
            return Err(unavailable(format!(
                "none of the nodes holding tenant {tenant}'s catalog is ready"
            )));
        }
        Ok(out)
    }

    fn client(&self, t: &Target) -> Result<CatalogPeerClient<Channel>> {
        if let Some(ch) = lock(&self.channels).get(&t.addr) {
            return Ok(CatalogPeerClient::new(ch.clone()));
        }
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}", t.addr);
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unavailable(format!("peer address {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(5));
        if let Some(p) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&p.ca))
                .identity(Identity::from_pem(&p.cert, &p.key))
                .domain_name(t.host.clone());
            ep = ep
                .tls_config(cfg)
                .map_err(|e| unavailable(format!("peer TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        lock(&self.channels).insert(t.addr.clone(), ch.clone());
        Ok(CatalogPeerClient::new(ch))
    }

    fn forget(&self, t: &Target) {
        lock(&self.channels).remove(&t.addr);
    }

    /// Opens a transaction on `tenant` at `t`.
    async fn begin_at(&self, t: &Target, tenant: TenantId) -> Result<RemoteTx> {
        let mut client = self.client(t)?;
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
                self.forget(t);
                return Err(unavailable(format!(
                    "engine node {}: {}",
                    t.addr,
                    s.message()
                )));
            }
            Err(_) => {
                self.forget(t);
                return Err(unavailable(format!("engine node {} didn't answer", t.addr)));
            }
        };
        tx.replies = Some(replies);
        tx.reply().await?;
        Ok(tx)
    }
}

#[async_trait::async_trait]
impl RecordStore for RemoteRecords {
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        let deadline = Instant::now() + FIND_LEADER;
        let mut fresh = false;
        loop {
            let targets = self.targets(tenant.0, fresh).await?;
            let mut last: Option<SprigError> = None;
            for t in &targets {
                match self.begin_at(t, tenant).await {
                    Ok(tx) => {
                        if let Some(r) = t.raft {
                            lock(&self.leaders).insert(tenant.0, r);
                        }
                        return Ok(Box::new(tx));
                    }
                    // Not the unit's leader: the answer names it when known.
                    Err(e) if e.code() == Some(SprigCode::ShardMoved) => {
                        match leader_hint(&e.message) {
                            Some(l) => lock(&self.leaders).insert(tenant.0, l),
                            None => lock(&self.leaders).remove(&tenant.0),
                        };
                        last = Some(e);
                    }
                    Err(e) if e.code() == Some(SprigCode::CatalogUnavailable) => {
                        last = Some(e);
                    }
                    Err(e) => return Err(e),
                }
                // One node only: its answer is final.
                if targets.len() == 1 && t.raft.is_none() {
                    break;
                }
            }
            let only_one = targets.len() == 1 && targets[0].raft.is_none();
            if only_one || Instant::now() >= deadline {
                return Err(last.unwrap_or_else(|| unavailable("no engine node answered")));
            }
            // An election may be running: look again shortly.
            fresh = true;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The next operation to send, for the request stream.
async fn next_op(
    mut r: mpsc::Receiver<CatalogOp>,
) -> Option<(CatalogOp, mpsc::Receiver<CatalogOp>)> {
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
                return Err(unavailable("the engine node ended the catalog transaction"));
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
