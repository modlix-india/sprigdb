//! Protocol tests for both AMQP versions over TCP, with this crate's own
//! clients (no AMQP client library is installed on the build machines).
//!
//! By default each test starts the server in process on 127.0.0.1 with
//! the in-memory engine (`sprig-queues` runs the same suite, in
//! `protocol/suite.rs`, against the queue engine). With `SPRIG_AMQP_ADDR` set, the same tests run
//! against a gateway's `amqp` listener instead (the live test):
//!
//! ```text
//! SPRIG_AMQP_ADDR=127.0.0.1:35672 SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=... \
//! [SPRIG_AMQP_TLS_ADDR=127.0.0.1:35671 SPRIG_AMQP_CA=deploy/compose/gateway-certs/ca.pem] \
//! cargo test -p sprig-proto-amqp --test protocol -- --nocapture
//! ```
//!
//! Tests that need the in-memory engine's test hooks (permissions,
//! `connection.blocked`, a small `max_message_bytes`) only run in process.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc as EngineArc;

use sprig_proto_amqp::backend::QueueBackend;
use sprig_proto_amqp::mem::{Authorizer, MemBackend, VhostMap};

/// Sends `connection.blocked` (true) or unblocked (false).
type Blocker = EngineArc<dyn Fn(bool) + Send + Sync>;

/// The engine the suite runs against: `sprig-proto-amqp`'s in-memory one.
fn engine_under_test(map: VhostMap, authz: Authorizer) -> (EngineArc<dyn QueueBackend>, Blocker) {
    let mem = EngineArc::new(MemBackend::new(map, authz));
    let m = EngineArc::clone(&mem);
    (mem, EngineArc::new(move |b| m.set_blocked(b)))
}

include!("protocol/suite.rs");

/// The in-memory engine, with vhost `other` holding a limit of its own
/// (100 bytes, as a tenant's or a vhost database's
/// `amqp.max_message_bytes` does in the gateway); `/` keeps the
/// listener's.
struct Limited(EngineArc<MemBackend>);

#[async_trait]
impl QueueBackend for Limited {
    async fn open_vhost(
        &self,
        principal: &Principal,
        vhost: &str,
        conn: sprig_proto_amqp::backend::ConnKey,
        events: sprig_proto_amqp::backend::EventSender,
    ) -> Result<EngineArc<dyn sprig_proto_amqp::backend::Vhost>, sprig_proto_amqp::error::MqError>
    {
        self.0.open_vhost(principal, vhost, conn, events).await
    }

    async fn max_message_bytes(&self, _principal: &Principal, vhost: &str) -> Option<u64> {
        (vhost == "other").then_some(100)
    }
}

#[tokio::test]
async fn a_vhost_limit_replaces_the_listener_one() {
    let mem = MemBackend::new(Arc::new(|_, v| Ok(v.to_owned())), Arc::new(|_, _, _| true));
    let svc = Services {
        identity: Arc::new(TestIdentity),
        backend: Arc::new(Limited(EngineArc::new(mem))),
        replies: DirectReplyTo::new(1),
    };
    let cfg = Config {
        handshake_timeout: Duration::from_secs(3),
        ..Config::default()
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (s, peer) = l.accept().await.unwrap();
            let (svc, cfg) = (svc.clone(), cfg.clone());
            tokio::spawn(async move {
                let info = ConnInfo {
                    peer,
                    tls: false,
                    sni: None,
                    cert_login: None,
                    client_dn: None,
                };
                let _ = serve(s, info, &svc, cfg, CancellationToken::new()).await;
            });
        }
    });
    for (vhost, refused) in [("other", true), ("/", false)] {
        let mut c = Client::connect(
            TcpStream::connect(addr).await.unwrap(),
            "PLAIN",
            plain("acme.app", "secret"),
            vhost,
            default_capabilities(),
            None,
        )
        .await
        .unwrap();
        open(&mut c, 1).await;
        let q = uniq("limit");
        declare_queue(&mut c, 1, &q, FieldTable::new()).await;
        c.publish(1, "", &q, false, &text("bin"), &[7u8; 200])
            .await
            .unwrap();
        if refused {
            let t = expect_channel_close(&mut c, 1, 406).await;
            assert!(t.contains("39257") && t.contains("100"), "{t}");
            open(&mut c, 1).await;
        } else {
            let started = std::time::Instant::now();
            while declare_queue(&mut c, 1, &q, FieldTable::new())
                .await
                .message_count
                == 0
            {
                assert!(started.elapsed() < Duration::from_secs(5), "never queued");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        delete_queue(&mut c, 1, &q).await;
        c.close().await.unwrap();
    }
}
