//! Three nodes over the TCP transport, plain and with mutual TLS.

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use sprig_common::time::SystemClock;
use sprig_engine::log::Durability;
use sprig_raft::tcp::{self, TcpTransport, TlsConfig};
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
    ca_pem: String,
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
            ca_pem: ca.pem(),
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
            node_names: false,
        }
    }

    /// PEM files of a certificate naming node `name_id` (subjectAltName
    /// `node-<id>`), signed by this authority: (ca, chain, key).
    fn node_pems(&self, name_id: u64) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut p = CertificateParams::new(vec![tcp::node_name(name_id)]).unwrap();
        p.distinguished_name
            .push(DnType::CommonName, tcp::node_name(name_id));
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &self.issuer).unwrap();
        (
            self.ca_pem.clone().into_bytes(),
            cert.pem().into_bytes(),
            key.serialize_pem().into_bytes(),
        )
    }
}

/// A listener shared with other protocols, as the gateway's peer listener
/// is: Raft connections are told apart by their ALPN protocol or preface
/// and handed to the transport; anything else is dropped here.
async fn shared_listener(t: Arc<TcpTransport>, node: Node) -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = l.accept().await else {
                continue;
            };
            let (t, node) = (Arc::clone(&t), node.clone());
            tokio::spawn(async move {
                let mut b = [0u8; 1];
                if s.peek(&mut b).await.ok() != Some(1) {
                    return;
                }
                if b[0] == tcp::PREFACE[0] {
                    t.serve_plain(s, &node).await;
                } else if b[0] == 0x16 {
                    let acc = tokio_rustls::LazyConfigAcceptor::new(
                        rustls::server::Acceptor::default(),
                        s,
                    );
                    let Ok(start) = acc.await else { return };
                    let raft = start
                        .client_hello()
                        .alpn()
                        .is_some_and(|mut a| a.any(|p| p == tcp::ALPN));
                    if let (true, Some(cfg)) = (raft, t.server_config())
                        && let Ok(tls) = start.into_stream(cfg).await
                    {
                        t.serve_tls(tls, &node).await;
                    }
                }
            });
        }
    });
    addr
}

fn run_cluster(tls: impl Fn(u64) -> Option<TlsConfig>) {
    run_cluster_on(tls, false);
}

fn run_cluster_on(tls: impl Fn(u64) -> Option<TlsConfig>, shared: bool) {
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
        let addr = if shared {
            t.set_preface(true);
            rt.block_on(shared_listener(Arc::clone(&t), node.clone()))
        } else {
            rt.block_on(t.listen("127.0.0.1:0".parse().unwrap(), node.clone()))
                .unwrap()
        };
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

fn identity(pki: &Pki, me: u64, named: u64) -> TlsConfig {
    let (ca, chain, key) = pki.node_pems(named);
    TlsConfig::node_identity(me, &ca, &chain, &key).unwrap()
}

#[test]
fn certificates_name_their_node() {
    let pki = Pki::new();
    for id in [1, 7, 123_456_789] {
        let (_, chain, _) = pki.node_pems(id);
        let der = rustls::pki_types::CertificateDer::from_pem_slice(&chain).unwrap();
        assert_eq!(tcp::cert_node_id(&der), Some(id));
    }
    // A certificate that names no node.
    let mut p = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    p.distinguished_name.push(DnType::CommonName, "node-4");
    let key = KeyPair::generate().unwrap();
    let cert = p.signed_by(&key, &pki.issuer).unwrap();
    assert_eq!(tcp::cert_node_id(cert.der()), None);
    // A node refuses to start with a certificate that names another node.
    let (ca, chain, key) = pki.node_pems(2);
    assert!(TlsConfig::node_identity(3, &ca, &chain, &key).is_err());
    assert!(TlsConfig::node_identity(2, &ca, &chain, &key).is_ok());
}

#[test]
fn replicates_over_mutual_tls_with_node_identity() {
    let pki = Pki::new();
    run_cluster(|id| Some(identity(&pki, id, id)));
}

#[test]
fn replicates_through_a_shared_listener_in_plaintext() {
    run_cluster_on(|_| None, true);
}

#[test]
fn replicates_through_a_shared_listener_with_node_identity() {
    let pki = Pki::new();
    run_cluster_on(|id| Some(identity(&pki, id, id)), true);
}

#[test]
fn a_node_cannot_speak_for_another() {
    // Node 3 holds node 2's certificate (trusted, but naming node 2): node
    // 1 refuses its frames, which claim to be from 3, and node 1 never
    // reaches it as node 3.
    let pki = Pki::new();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let t1 = TcpTransport::new(1, rt.handle().clone(), Some(identity(&pki, 1, 1)));
    let mut stolen = identity(&pki, 2, 2);
    stolen.node_names = true;
    let t3 = TcpTransport::new(3, rt.handle().clone(), Some(stolen));
    let open = |id: u64, t: &Arc<TcpTransport>| {
        Node::open(NodeOptions {
            id,
            config: fast(),
            store: MemLogStore::new(),
            transport: Arc::clone(t) as Arc<dyn sprig_raft::Transport>,
            clock: Arc::new(SystemClock),
            machines: Arc::new(KvMachine::factory()),
            observer: None,
            seed: id,
        })
        .unwrap()
    };
    let n1 = open(1, &t1);
    let n3 = open(3, &t3);
    let a1 = rt
        .block_on(t1.listen("127.0.0.1:0".parse().unwrap(), n1.clone()))
        .unwrap();
    let a3 = rt
        .block_on(t3.listen("127.0.0.1:0".parse().unwrap(), n3.clone()))
        .unwrap();
    let (h1, h3) = (n1.spawn().unwrap(), n3.spawn().unwrap());
    t1.set_peer(3, a3);
    t3.set_peer(1, a1);
    for n in [&n1, &n3] {
        n.create_group(1, &[1, 3]).unwrap();
    }
    let _ = n3.campaign(1);
    let _ = n1.campaign(1);
    std::thread::sleep(Duration::from_millis(800));
    // Neither hears the other: no leader, nothing received.
    assert_eq!(n1.stats().frames_received, 0);
    assert_eq!(n3.stats().frames_received, 0);
    assert!(!n1.status(1).unwrap().is_leader);
    assert!(!n3.status(1).unwrap().is_leader);
    n1.close();
    n3.close();
    h1.join().unwrap();
    h3.join().unwrap();
    rt.shutdown_timeout(Duration::from_secs(1));
}

/// A node cut off from both others (in-process, as a partition would):
/// the others elect a leader of their own and go on; healed, the cut node
/// follows that leader and catches up.
#[test]
fn a_cut_off_leader_is_replaced_and_rejoins_when_healed() {
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
        let t = TcpTransport::new(id, rt.handle().clone(), None);
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
        addrs.push(
            rt.block_on(t.listen("127.0.0.1:0".parse().unwrap(), node.clone()))
                .unwrap(),
        );
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
    let leader = |except: Option<u64>| {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(Instant::now() < deadline, "no leader");
            if let Some(n) = nodes
                .iter()
                .find(|n| Some(n.id()) != except && n.status(1).is_some_and(|s| s.is_leader))
            {
                return n.clone();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    let write = |n: &Node, v: u64| {
        n.propose(1, toy::put(1, v), Durability::Strict)
            .ok()
            .and_then(|s| s.wait_timeout(Duration::from_secs(2)))
            .is_some_and(|r| r.is_ok())
    };
    let old = leader(None);
    assert!(write(&old, 1));
    let cut_id = old.id();
    let others: std::collections::BTreeSet<u64> = (1..=3).filter(|i| *i != cut_id).collect();
    let cut = &transports[nodes.iter().position(|n| n.id() == cut_id).unwrap()];
    cut.set_cut(others.clone());
    assert_eq!(cut.cut(), others);
    // The cut leader's write can't commit; the others elect a new leader.
    assert!(!write(&old, 2));
    let new = leader(Some(cut_id));
    let deadline = Instant::now() + Duration::from_secs(10);
    while !write(&new, 3) {
        assert!(Instant::now() < deadline, "the new leader can't write");
    }
    cut.set_cut(std::collections::BTreeSet::new());
    let want = new.query_local(1, &toy::history(1)).unwrap().1;
    let deadline = Instant::now() + Duration::from_secs(10);
    while old.query_local(1, &toy::history(1)).unwrap().1 != want
        || old.status(1).is_some_and(|s| s.is_leader)
    {
        assert!(Instant::now() < deadline, "the healed node didn't catch up");
        std::thread::sleep(Duration::from_millis(10));
    }
    for n in &nodes {
        n.close();
    }
    for t in threads {
        t.join().unwrap();
    }
    rt.shutdown_timeout(Duration::from_secs(1));
}

/// Certificates renewed while the cluster runs take over without a
/// restart, with no failed write; then a revoked node is cut off at once:
/// its live connections close, it is refused, and the other two go on.
#[test]
#[allow(clippy::too_many_lines)] // one scenario, step by step
fn renewed_certificates_take_over_and_a_revoked_node_is_cut_off() {
    let pki = Pki::new();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let serial_of =
        |chain: &[u8]| tcp::cert_serial(&CertificateDer::from_pem_slice(chain).unwrap()).unwrap();
    let mut nodes = Vec::new();
    let mut transports = Vec::new();
    let mut threads = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    // Every certificate node 3 has held: revocation lists them all, as the
    // CA's does (a peer's connection may still be on an older one).
    let mut held_by_3 = std::collections::BTreeSet::new();
    for id in 1..=3u64 {
        let (ca, chain, key) = pki.node_pems(id);
        if id == 3 {
            held_by_3.insert(serial_of(&chain));
        }
        let tls = TlsConfig::node_identity(id, &ca, &chain, &key).unwrap();
        let t = TcpTransport::new(id, rt.handle().clone(), Some(tls));
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
        addrs.push(
            rt.block_on(t.listen("127.0.0.1:0".parse().unwrap(), node.clone()))
                .unwrap(),
        );
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
    let leader = |among: &[u64]| {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(Instant::now() < deadline, "no leader");
            if let Some(n) = nodes
                .iter()
                .find(|n| among.contains(&n.id()) && n.status(1).is_some_and(|s| s.is_leader))
            {
                return n.clone();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    let mut v = 0u64;
    let mut write = |among: &[u64]| {
        v += 1;
        let l = leader(among);
        l.propose(1, toy::put(1, v), Durability::Strict)
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    };
    for _ in 0..10 {
        write(&[1, 2, 3]);
    }
    // Renew every node's certificate, twice, writing all along.
    for round in 1..=2u64 {
        for (i, t) in transports.iter().enumerate() {
            let id = i as u64 + 1;
            let (ca, chain, key) = pki.node_pems(id);
            if id == 3 {
                held_by_3.insert(serial_of(&chain));
            }
            t.set_tls(TlsConfig::node_identity(id, &ca, &chain, &key).unwrap())
                .unwrap();
            write(&[1, 2, 3]);
        }
        for t in &transports {
            assert_eq!(t.tls_generation(), round);
        }
        for _ in 0..10 {
            write(&[1, 2, 3]);
        }
    }
    // Revoke node 3's certificate on nodes 1 and 2.
    assert_eq!(held_by_3.len(), 3);
    transports[0].set_revoked(held_by_3.clone());
    transports[1].set_revoked(held_by_3);
    std::thread::sleep(Duration::from_millis(300));
    let before = nodes[2].stats().frames_received;
    for _ in 0..10 {
        write(&[1, 2]);
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        nodes[2].stats().frames_received,
        before,
        "the revoked node still hears its peers"
    );
    assert!(
        !nodes[2].status(1).unwrap().is_leader,
        "the revoked node leads"
    );
    for n in &nodes {
        n.close();
    }
    for t in threads {
        t.join().unwrap();
    }
    rt.shutdown_timeout(Duration::from_secs(1));
}
