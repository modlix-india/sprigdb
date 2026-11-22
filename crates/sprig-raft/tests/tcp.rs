//! Three nodes over the TCP transport, plain and with mutual TLS.

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use sprig_common::time::SystemClock;
use sprig_engine::log::Durability;
use sprig_raft::tcp::{TcpTransport, TlsConfig};
use sprig_raft::toy::{self, KvMachine};
use sprig_raft::{MemLogStore, Node, NodeOptions, RaftConfig};

fn fast() -> RaftConfig {
    RaftConfig {
        tick: Duration::from_millis(10),
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        ..RaftConfig::default()
    }
}

struct Pki {
    roots: Arc<rustls::RootCertStore>,
    issuer: Issuer<'static, KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name
            .push(DnType::CommonName, "raft test CA");
        let key = KeyPair::generate().unwrap();
        let ca = p.self_signed(&key).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        Self {
            roots: Arc::new(roots),
            issuer: Issuer::new(p, key),
        }
    }

    fn tls(&self, cn: &str) -> TlsConfig {
        let mut p = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        p.distinguished_name.push(DnType::CommonName, cn);
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &self.issuer).unwrap();
        let chain: Vec<CertificateDer<'static>> = vec![cert.der().clone()];
        let pk = || PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.serialize_der()));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::clone(&self.roots),
            Arc::clone(&provider),
        )
        .build()
        .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), pk())
            .unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(Arc::clone(&self.roots))
            .with_client_auth_cert(chain, pk())
            .unwrap();
        TlsConfig {
            client: Arc::new(client),
            server: Arc::new(server),
            server_name: ServerName::try_from("localhost").unwrap(),
        }
    }
}

fn run_cluster(tls: impl Fn(u64) -> Option<TlsConfig>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut nodes = Vec::new();
    let mut transports = Vec::new();
    let mut threads = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for id in 1..=3u64 {
        let t = TcpTransport::new(id, rt.handle().clone(), tls(id));
        let node = Node::open(NodeOptions {
            id,
            config: fast(),
            store: MemLogStore::new(),
            transport: Arc::clone(&t) as Arc<dyn sprig_raft::Transport>,
            clock: Arc::new(SystemClock),
            machines: Arc::new(KvMachine::factory()),
            observer: None,
            seed: id,
        })
        .unwrap();
        let addr = rt
            .block_on(t.listen("127.0.0.1:0".parse().unwrap(), node.clone()))
            .unwrap();
        addrs.push(addr);
        transports.push(t);
        threads.push(node.spawn().unwrap());
        nodes.push(node);
    }
    for t in &transports {
        for (i, a) in addrs.iter().enumerate() {
            t.set_peer(i as u64 + 1, *a);
        }
    }
    for n in &nodes {
        let s = n.create_group(1, &[1, 2, 3]).unwrap();
        assert_eq!(s.wait_timeout(Duration::from_secs(5)), Some(Ok(())));
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut written = 0;
    while written < 20 {
        assert!(Instant::now() < deadline, "no progress over TCP");
        let Some(l) = nodes
            .iter()
            .find(|n| n.status(1).is_some_and(|s| s.is_leader))
        else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if let Ok(slot) = l.propose(1, toy::put(1, written), Durability::Strict)
            && let Some(Ok(_)) = slot.wait_timeout(Duration::from_secs(5))
        {
            written += 1;
        }
    }
    let l = nodes
        .iter()
        .find(|n| n.status(1).is_some_and(|s| s.is_leader))
        .unwrap();
    let got = l
        .read(1, toy::get(1))
        .unwrap()
        .wait_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(toy::value(&got), Some(19));
    // Every replica gets there.
    let want = l.query_local(1, &toy::history(1)).unwrap().1;
    for n in &nodes {
        let deadline = Instant::now() + Duration::from_secs(10);
        while n.query_local(1, &toy::history(1)).unwrap().1 != want {
            assert!(Instant::now() < deadline, "replica {} behind", n.id());
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    for n in &nodes {
        n.close();
    }
    for t in threads {
        t.join().unwrap();
    }
    rt.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn replicates_over_plain_tcp() {
    run_cluster(|_| None);
}

#[test]
fn replicates_over_mutual_tls() {
    let pki = Pki::new();
    run_cluster(|id| Some(pki.tls(&format!("node{id}"))));
}

#[test]
fn a_peer_without_a_trusted_certificate_gets_nothing_through() {
    // Node 3's certificate comes from another authority: the others refuse
    // its connections, so groups still elect among 1 and 2 only.
    let pki = Pki::new();
    let other = Pki::new();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let t1 = TcpTransport::new(1, rt.handle().clone(), Some(pki.tls("node1")));
    let t3 = TcpTransport::new(3, rt.handle().clone(), Some(other.tls("node3")));
    let n1 = Node::open(NodeOptions {
        id: 1,
        config: fast(),
        store: MemLogStore::new(),
        transport: Arc::clone(&t1) as Arc<dyn sprig_raft::Transport>,
        clock: Arc::new(SystemClock),
        machines: Arc::new(KvMachine::factory()),
        observer: None,
        seed: 1,
    })
    .unwrap();
    let a1 = rt
        .block_on(t1.listen("127.0.0.1:0".parse().unwrap(), n1.clone()))
        .unwrap();
    let h1 = n1.spawn().unwrap();
    t3.set_peer(1, a1);
    // Frames from 3 never reach node 1's inbox.
    for _ in 0..50 {
        sprig_raft::Transport::send(&*t3, 1, vec![0; 16]);
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(n1.stats().frames_received, 0);
    n1.close();
    h1.join().unwrap();
    rt.shutdown_timeout(Duration::from_secs(1));
}
