//! The AMQP listeners on 5672 and 5671: [`AmqpFrontend`]
//! runs `sprig-proto-amqp`'s server on each connection. The protocol
//! header picks AMQP 0-9-1 or 1.0; TLS on 5671 is finished by the listener
//! before the header is read.
//!
//! - [`identity`]: SASL `PLAIN` and `AMQPLAIN` onto the gateway's logins,
//!   with the tenant from the TLS server name, a `tenant.user` name, or
//!   `listener.amqp.tenant`.
//! - [`GatewayQueues`]: the queue engine. With catalog logins it is
//!   `sprig-queues`' [`QueueEngine`] over [`queues::GatewayMq`] (the tenant
//!   catalog, the shard schemas, the tenant's grants) and
//!   [`queues::EtcdLeases`]; without them (no tenants) `sprig-proto-amqp`'s
//!   in-memory engine. Either way a vhost maps to the tenant's logical
//!   database (`/` to `amqp.default_vhost_database`), and vhosts whose
//!   database doesn't exist are refused.

pub mod identity;
pub mod peer;
pub mod queues;

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use sprig_catalog::Freshness;
use sprig_common::Result;
use sprig_common::ids::{DcId, TenantId};
use sprig_proto_amqp::auth::{ConnInfo, Principal};
use sprig_proto_amqp::backend::{ConnKey, EventSender, QueueBackend, Vhost};
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::mem::{MemBackend, default_database};
use sprig_proto_amqp::reply_to::DirectReplyTo;
use sprig_proto_amqp::server::{Config, Services, serve_resumed};
use sprig_queues::{EngineDeps, QueueEngine};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::frontend::{ClientConn, Frontend, GatewayContext, Protocol};
use crate::tenant::TenantServices;

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 50,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `listener.amqp.tenant`: binds the `amqp` and `amqps` listeners to one
    /// tenant, for plain user names.
    LISTENER_TENANT: Text = "listener.amqp.tenant", Cow::Borrowed(""), Scopes::PROCESS,
    HardBounds::NONE, "Tenant of plain user names on the AMQP listeners"
);
key!(
    /// `amqp.default_vhost_database`: the database of vhost `/`.
    DEFAULT_VHOST_DATABASE: Text = "amqp.default_vhost_database", Cow::Borrowed("mq"), TENANT_GLOBAL,
    HardBounds::NONE, "Database for the AMQP virtual host /"
);
key!(
    /// `amqp.sasl_mechanisms`: offered SASL mechanisms, in order.
    SASL_MECHANISMS: Text = "amqp.sasl_mechanisms", Cow::Borrowed("PLAIN,AMQPLAIN,EXTERNAL"), Scopes::PROCESS,
    HardBounds::NONE, "Offered SASL mechanisms on the AMQP listeners"
);
key!(
    /// `amqp.cert_login_field`: what of a client certificate names the
    /// user for `EXTERNAL`.
    CERT_LOGIN_FIELD: Text = "amqp.cert_login_field", Cow::Borrowed("common_name"), Scopes::PROCESS,
    HardBounds::one_of(&["common_name", "distinguished_name"]),
    "Certificate field that names the user for SASL EXTERNAL"
);
key!(
    /// `amqp.heartbeat_seconds`: the heartbeat the server proposes.
    HEARTBEAT_SECONDS: u64 = "amqp.heartbeat_seconds", 60, Scopes::PROCESS, HardBounds::range(0, 3600),
    "Heartbeat the AMQP server proposes, in seconds"
);
key!(
    /// `amqp.frame_max`: the largest frame.
    FRAME_MAX: u64 = "amqp.frame_max", 131_072, Scopes::PROCESS, HardBounds::range(4096, 16_777_216),
    "Largest AMQP frame, in bytes"
);
key!(
    /// `amqp.channel_max`: channels per connection.
    CHANNEL_MAX: u64 = "amqp.channel_max", 2047, Scopes::PROCESS, HardBounds::range(1, 65_535),
    "AMQP channels per connection"
);
key!(
    /// `amqp.max_message_bytes`: the largest message.
    MAX_MESSAGE_BYTES: u64 = "amqp.max_message_bytes", 16_777_216, Scopes::ALL,
    HardBounds::range(1, 536_870_912), "Largest AMQP message, in bytes"
);
key!(
    /// `amqp.max_unacked_per_consumer`: the cap for prefetch 0.
    MAX_UNACKED_PER_CONSUMER: u64 = "amqp.max_unacked_per_consumer", 65_535, TENANT_GLOBAL,
    HardBounds::range(1, 1_000_000), "Unacknowledged deliveries per consumer with prefetch 0"
);
key!(
    /// `amqp10.link_credit`: credit granted to publishing links.
    LINK_CREDIT: u64 = "amqp10.link_credit", 256, Scopes::PROCESS, HardBounds::range(1, 65_535),
    "Credit granted to AMQP 1.0 publishing links"
);
key!(
    /// `amqp10.legacy_addresses`: accept bare and v1 addresses.
    LEGACY_ADDRESSES: bool = "amqp10.legacy_addresses", false, Scopes::PROCESS, HardBounds::NONE,
    "Accept bare queue names and AMQP address v1 forms"
);

/// The temporary-queue registry, relay and placement of the engine.
type RelayParts = (
    Arc<dyn sprig_queues::engine::TempRegistry>,
    Option<Arc<dyn sprig_queues::relay::PeerTransport>>,
    Option<Arc<dyn sprig_queues::relay::Placement>>,
);

/// The queue engine of the AMQP listeners: `sprig-queues` with catalog
/// logins, else the in-memory engine.
pub struct GatewayQueues {
    mem: MemBackend,
    tenants: Option<Arc<TenantServices>>,
    live: LiveSettings,
    dc: DcId,
    engine: Option<Arc<QueueEngine>>,
    /// The ready gateways' 0-9-1 listeners by their engine's Raft id, as
    /// last read (at most once a second).
    relays: std::sync::Mutex<Option<(std::time::Instant, Relays)>>,
}

/// 0-9-1 listeners (`host:port`) by engine Raft id.
type Relays = Vec<(u64, String)>;

impl std::fmt::Debug for GatewayQueues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayQueues").finish_non_exhaustive()
    }
}

impl GatewayQueues {
    /// The engine; vhosts are checked against `tenants`' catalogs.
    #[must_use]
    pub fn new(tenants: Option<Arc<TenantServices>>, live: LiveSettings, dc: DcId) -> Self {
        let (l, d) = (live.clone(), dc.clone());
        let map = Arc::new(move |_: &Principal, v: &str| {
            Ok(default_database(
                v,
                &l.get(&DEFAULT_VHOST_DATABASE, &SettingCtx::process(&d)),
            ))
        });
        Self {
            // Per-object privileges come with the
            // MariaDB-backed engine; a login that may use the vhost's
            // database may use its queues.
            mem: MemBackend::new(map, Arc::new(|_, _, _| true)),
            tenants,
            live,
            dc,
            engine: None,
            relays: std::sync::Mutex::new(None),
        }
    }

    /// The `sprig-queues` engine over `tenants`:
    /// owner leases in `dc_store` as gateway `gateway_id`, stopped (and
    /// the leases revoked) at `stop`.
    ///
    /// With `relay` (the peer service is on), queues are shared by every
    /// gateway of the tenant's home DC: owners are placed by rendezvous
    /// hashing and follow their consumers, other gateways relay to them,
    /// and temporary queues are published in etcd.
    #[must_use]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)] // the engine's footing, one each
    pub fn with_engine(
        tenants: Arc<TenantServices>,
        live: LiveSettings,
        dc: DcId,
        dc_store: Arc<dyn sprig_meta::MetaStore>,
        gateway_id: String,
        slot: u32,
        relay: Option<peer::GatewayRelay>,
        stop: sprig_common::CancellationToken,
    ) -> Self {
        let leases = Arc::new(queues::EtcdLeases::new(
            Arc::clone(&dc_store),
            gateway_id.clone(),
        ));
        let (temps, peers, placement): RelayParts = match relay {
            Some(r) => {
                let temps = Arc::new(peer::EtcdTemps::new(Arc::clone(&dc_store), gateway_id));
                tokio::spawn(Arc::clone(&temps).run(stop.clone()));
                (
                    temps,
                    Some(Arc::new(r)),
                    Some(Arc::new(peer::GatewayPlacement::new(
                        Arc::clone(&dc_store),
                        peer::home_of(Arc::clone(&tenants.locator)),
                    ))),
                )
            }
            None => (Arc::new(sprig_queues::engine::NoTemps), None, None),
        };
        let engine = Arc::new(QueueEngine::new(EngineDeps {
            catalog: Arc::new(queues::GatewayMq::new(Arc::clone(&tenants), live.clone())),
            leases: Arc::clone(&leases) as Arc<dyn sprig_queues::engine::Leases>,
            temps,
            clock: Arc::new(sprig_queues::owner::SystemClock),
            slot,
            peers,
            placement,
        }));
        {
            let e = Arc::clone(&engine);
            let stop2 = stop.clone();
            tokio::spawn(async move {
                stop2.cancelled().await;
                e.shutdown();
            });
        }
        // Fan-in consumers' ready index.
        engine.set_ready_hook(crate::inherited::fanin_mq::ReadyIndex::start(
            Arc::clone(&tenants),
            stop.clone(),
        ));
        tokio::spawn(leases.keep(stop));
        let _ = tenants.mq.set(Arc::clone(&engine));
        let mut g = Self::new(Some(tenants), live, dc);
        g.engine = Some(engine);
        g
    }

    /// The queue engine, when this gateway runs `sprig-queues`.
    #[must_use]
    pub fn engine(&self) -> Option<&Arc<QueueEngine>> {
        self.engine.as_ref()
    }

    fn database(&self, vhost: &str) -> String {
        default_database(
            vhost,
            &self
                .live
                .get(&DEFAULT_VHOST_DATABASE, &SettingCtx::process(&self.dc)),
        )
    }
}

impl GatewayQueues {
    /// The 0-9-1 listener of the gateway whose engine has Raft id `node`.
    async fn listener_of(&self, node: u64) -> Option<String> {
        let fresh = |c: &Option<(std::time::Instant, Relays)>| {
            c.as_ref()
                .filter(|(at, _)| at.elapsed() < std::time::Duration::from_secs(1))
                .map(|(_, v)| v.clone())
        };
        let cached = fresh(
            &self
                .relays
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let list = if let Some(v) = cached {
            v
        } else {
            {
                let peers = self.tenants.as_ref()?.peers.get()?;
                let v: Relays = peers
                    .gateways()
                    .await
                    .ok()?
                    .into_iter()
                    .filter(|r| r.state == crate::registry::GatewayState::Ready)
                    .filter_map(|r| {
                        let port = r.listeners.get("amqp")?;
                        Some((r.raft_node?, format!("{}:{port}", r.addr)))
                    })
                    .collect();
                *self
                    .relays
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((std::time::Instant::now(), v.clone()));
                v
            }
        };
        list.into_iter().find(|(n, _)| *n == node).map(|(_, a)| a)
    }
}

#[async_trait]
impl QueueBackend for GatewayQueues {
    /// On replicated engines a tenant's queues, catalog changes and
    /// messages commit only where its unit is led: a connection that
    /// logged in elsewhere is relayed to that gateway.
    async fn serve_elsewhere(&self, principal: &Principal) -> Option<String> {
        self.engine.as_ref()?;
        let crate::node_engine::UnitLeader::Node(node) =
            crate::node_engine::tenant_leader(principal.tenant_id)
        else {
            return None;
        };
        self.listener_of(node).await
    }

    async fn open_vhost(
        &self,
        principal: &Principal,
        vhost: &str,
        conn: ConnKey,
        events: EventSender,
    ) -> std::result::Result<Arc<dyn Vhost>, MqError> {
        if let Some(e) = &self.engine {
            // A member's inherited exchanges, queues and
            // bindings are made on its first use of the vhost, and
            // publishes into them are stamped (or targeted, from above).
            let db = self.database(vhost);
            if let Some(t) = &self.tenants {
                Box::pin(crate::inherited::mq::before_open(
                    t,
                    TenantId(principal.tenant_id),
                    &db,
                ))
                .await;
            }
            let v = e.open_vhost(principal, vhost, conn, events.clone()).await?;
            if let Some(t) = &self.tenants {
                return Box::pin(crate::inherited::mq::wrap_vhost(
                    t,
                    principal,
                    &db,
                    v,
                    Some(events),
                ))
                .await;
            }
            return Ok(v);
        }
        let db = self.database(vhost);
        if let Some(t) = &self.tenants {
            let cat = t
                .cache
                .get(TenantId(principal.tenant_id), Freshness::Latest)
                .await
                .map_err(|e| MqError::not_found(format!("vhost '{vhost}': {e}")))?;
            if !cat.databases.iter().any(|d| d.name == db) {
                return Err(MqError::not_found(format!(
                    "vhost '{vhost}' not found: the tenant has no database '{db}' (amqp.default_vhost_database maps '/')"
                )));
            }
        }
        self.mem.open_vhost(principal, vhost, conn, events).await
    }

    async fn max_message_bytes(&self, principal: &Principal, vhost: &str) -> Option<u64> {
        let t = self.tenants.as_ref()?;
        let tenant = TenantId(principal.tenant_id);
        let cat = match t.cache.cached(tenant) {
            Some(c) => c,
            None => t.cache.get(tenant, Freshness::Latest).await.ok()?,
        };
        Some(max_message_bytes(t, Some(&cat), vhost))
    }
}

/// `amqp.max_message_bytes` for a publish into vhost `vhost` of tenant
/// `cat`: at the scope of the vhost's database, so its value, the
/// tenant's, the DC's and the global one apply in that order (the AMQP
/// listeners and `sprig.mq_publish` both read it here). Without a
/// catalog, the DC's and the global one.
#[must_use]
pub fn max_message_bytes(
    svc: &TenantServices,
    cat: Option<&sprig_catalog::TenantCatalog>,
    vhost: &str,
) -> u64 {
    let Some(cat) = cat else {
        return svc
            .settings
            .get(&MAX_MESSAGE_BYTES, &SettingCtx::process(&svc.dc), None);
    };
    let db = default_database(
        vhost,
        &svc.settings.get(
            &DEFAULT_VHOST_DATABASE,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        ),
    );
    let mut ctx = cat.setting_ctx(&svc.dc);
    if let Some(d) = cat
        .databases
        .by_name(&db)
        .or_else(|| cat.databases.by_name(&db.to_lowercase()))
    {
        ctx = ctx.with_db(d.db_id);
    }
    svc.settings
        .get(&MAX_MESSAGE_BYTES, &ctx, Some(&cat.settings))
}

/// The AMQP front end: one server for both listeners.
pub struct AmqpFrontend {
    services: Services,
    live: LiveSettings,
    dc: DcId,
    node: String,
}

impl std::fmt::Debug for AmqpFrontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpFrontend").finish_non_exhaustive()
    }
}

impl AmqpFrontend {
    /// The front end over `identity` and `backend`; `slot` numbers this
    /// gateway in direct reply-to names.
    #[must_use]
    pub fn new(
        identity: Arc<identity::AmqpIdentity>,
        backend: Arc<dyn QueueBackend>,
        live: LiveSettings,
        dc: DcId,
        node: String,
        slot: u32,
    ) -> Self {
        Self::with_replies(identity, backend, live, dc, node, DirectReplyTo::new(slot))
    }

    /// The front end with a given direct reply-to registry (a key shared
    /// by the DC's gateways and a relay to them, [`peer::reply_key`]).
    #[must_use]
    pub fn with_replies(
        identity: Arc<identity::AmqpIdentity>,
        backend: Arc<dyn QueueBackend>,
        live: LiveSettings,
        dc: DcId,
        node: String,
        replies: Arc<DirectReplyTo>,
    ) -> Self {
        Self {
            services: Services {
                identity,
                backend,
                replies,
            },
            live,
            dc,
            node,
        }
    }

    fn config(&self, handshake: std::time::Duration) -> Config {
        let ctx = SettingCtx::process(&self.dc);
        let n = |s: &'static Setting<u64>| self.live.get(s, &ctx);
        Config {
            product: "SprigDB".into(),
            version: "4.1.0".into(),
            node: self.node.clone(),
            mechanisms: self
                .live
                .get(&SASL_MECHANISMS, &ctx)
                .split(',')
                .map(|m| m.trim().to_ascii_uppercase())
                .filter(|m| !m.is_empty())
                .collect(),
            heartbeat: u16::try_from(n(&HEARTBEAT_SECONDS)).unwrap_or(60),
            frame_max: u32::try_from(n(&FRAME_MAX)).unwrap_or(131_072),
            channel_max: u16::try_from(n(&CHANNEL_MAX)).unwrap_or(2047),
            max_message_bytes: n(&MAX_MESSAGE_BYTES),
            max_unacked_per_consumer: u32::try_from(n(&MAX_UNACKED_PER_CONSUMER)).unwrap_or(65_535),
            handshake_timeout: handshake,
            link_credit: u32::try_from(n(&LINK_CREDIT)).unwrap_or(256),
            legacy_addresses: self.live.get(&LEGACY_ADDRESSES, &ctx),
        }
    }
}

#[async_trait]
impl Frontend for AmqpFrontend {
    fn protocol(&self) -> Protocol {
        Protocol::Amqp
    }

    async fn serve(&self, conn: ClientConn, _ctx: Arc<GatewayContext>) -> Result<()> {
        let tls = conn.stream.tls_info();
        // A verified client certificate (`listener.amqps.client_cert`):
        // its names for the login's `REQUIRE SUBJECT` and `ISSUER`, and its
        // login name for `EXTERNAL`.
        let dn = tls
            .as_ref()
            .filter(|t| t.client_cert)
            .and_then(|t| t.client_dn.clone());
        let field = self
            .live
            .get(&CERT_LOGIN_FIELD, &SettingCtx::process(&self.dc));
        let info = ConnInfo {
            peer: conn.peer,
            tls: tls.is_some(),
            sni: tls.as_ref().and_then(|t| t.sni.clone()),
            cert_login: dn
                .as_ref()
                .and_then(|d| identity::cert_login(&d.subject, &field)),
            client_dn: dn.map(|d| (d.subject, d.issuer)),
        };
        let cfg = self.config(conn.listener.handshake_timeout);
        let close = conn.close_soon.clone();
        let peer = conn.peer;
        let mut conn = conn;
        // A connection another node accepted and handed here: which AMQP
        // it speaks (its header and the server's first frame were
        // exchanged there).
        let resumed = match &conn.stream {
            crate::frontend::MaybeTls::Forwarded(f) => f
                .take_resume()
                .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok())
                .and_then(|v| v.get("v10").and_then(serde_json::Value::as_bool)),
            _ => None,
        };
        let h = match serve_resumed(&mut conn.stream, info, &self.services, cfg, close, resumed)
            .await
        {
            Ok(Some(h)) => h,
            Ok(None) => return Ok(()),
            Err(e) => {
                tracing::debug!(%peer, error = %e, "AMQP connection ended");
                return Ok(());
            }
        };
        let handoff = crate::forward::Handoff {
            listener: conn.listener.name,
            protocol: Protocol::Amqp,
            peer,
            tls,
            tenant: h.tenant,
            resume: serde_json::json!({ "v10": h.v10 }).to_string(),
            prelude: h.prelude.into(),
            to: h.node,
        };
        // On failure the client sees the connection close and reconnects.
        if let Err(e) = crate::forward::splice(&mut conn.stream, handoff).await {
            tracing::debug!(%peer, error = %e, "AMQP connection not handed over");
        }
        Ok(())
    }
}
