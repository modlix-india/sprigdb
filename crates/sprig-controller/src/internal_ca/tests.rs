#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, UnixTime};
use sprig_common::ids::DcId;
use sprig_meta::MemStore;

use super::*;

fn stores() -> (Arc<dyn MetaStore>, Arc<dyn MetaStore>) {
    (
        Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-a").unwrap()))),
        Arc::new(MemStore::new(Tier::Global)),
    )
}

fn ca(validity: u64) -> InternalCa {
    InternalCa::new(SealKey::new([9u8; 32]), validity)
}

fn t(s: u64) -> SystemTime {
    at(1_800_000_000 + s)
}

/// A node's key, as a gateway makes it: its public point in a request.
async fn request(dc: &dyn MetaStore, node: u64) -> KeyPair {
    let k = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let req = KeyRequest {
        node,
        gateway_id: format!("gw{node}"),
        public_key: node_ca::hex(k.public_key_raw()),
        at: 0,
    };
    dc.txn(sprig_meta::Txn {
        when: Vec::new(),
        then: vec![Op::Put {
            key: node_ca::request_key(dc, node).unwrap(),
            value: Record::new(req).encode().unwrap(),
            lease: None,
        }],
        otherwise: Vec::new(),
    })
    .await
    .unwrap();
    k
}

/// Checks `chain` the way a node's TLS does: against the root, as a client
/// certificate, at `now`.
fn verifies(root_pem: &str, chain: &str, now: SystemTime) -> bool {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(root_pem.as_bytes()).unwrap())
        .unwrap();
    let v = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain.as_bytes())
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    v.verify_client_cert(
        &certs[0],
        &certs[1..],
        UnixTime::since_unix_epoch(now.duration_since(UNIX_EPOCH).unwrap()),
    )
    .is_ok()
}

#[tokio::test]
async fn init_makes_the_root_and_the_issuing_ca_once() {
    let (dc, g) = stores();
    let c = ca(NODE_CERT_DEFAULT_SECONDS);
    let a = c.init(dc.as_ref(), g.as_ref(), t(0)).await.unwrap();
    assert!(a.root_created && a.issuing_created);
    assert_eq!(a.issuing.subject, "SprigDB issuing CA dc-a");
    let b = c.init(dc.as_ref(), g.as_ref(), t(10)).await.unwrap();
    assert!(!b.root_created && !b.issuing_created);
    assert_eq!(a.root.fingerprint, b.root.fingerprint);
    assert_eq!(a.issuing.fingerprint, b.issuing.fingerprint);
    // The keys are stored sealed: no PEM key in the records.
    let (root, _) = node_ca::read::<CaRecord>(g.as_ref(), node_ca::ROOT)
        .await
        .unwrap()
        .unwrap();
    assert!(!root.sealed_key.contains("PRIVATE"));
    assert!(root.cert_pem.contains("BEGIN CERTIFICATE"));
}

#[tokio::test]
async fn node_certificates_chain_to_the_root_and_name_their_node() {
    let (dc, g) = stores();
    let c = ca(NODE_CERT_DEFAULT_SECONDS);
    assert_eq!(
        c.issue(dc.as_ref(), g.as_ref(), 5, false, t(0))
            .await
            .unwrap(),
        IssueOutcome::NoRequest
    );
    let _k = request(dc.as_ref(), 5).await;
    let IssueOutcome::Issued(cert) = c
        .issue(dc.as_ref(), g.as_ref(), 5, false, t(0))
        .await
        .unwrap()
    else {
        panic!("not issued")
    };
    assert_eq!(cert.not_after, 1_800_000_000 + NODE_CERT_DEFAULT_SECONDS);
    assert_eq!(cert.serial.len(), 32);
    let (root, _) = node_ca::read::<CaRecord>(g.as_ref(), node_ca::ROOT)
        .await
        .unwrap()
        .unwrap();
    assert!(verifies(&root.cert_pem, &cert.chain_pem, t(60)));
    assert!(
        !verifies(
            &root.cert_pem,
            &cert.chain_pem,
            t(NODE_CERT_DEFAULT_SECONDS + 60)
        ),
        "expired"
    );
    let leaf = node_ca::first_der(&cert.chain_pem).unwrap();
    assert!(leaf.windows(6).any(|w| w == b"node-5"));
    // The serial is the DER integer's content.
    assert!(leaf.windows(16).any(|w| node_ca::hex(w) == cert.serial));
    // Current until a third of the validity is left, or forced.
    assert!(matches!(
        c.issue(dc.as_ref(), g.as_ref(), 5, false, t(3600))
            .await
            .unwrap(),
        IssueOutcome::Current(_)
    ));
    let IssueOutcome::Issued(again) = c
        .issue(dc.as_ref(), g.as_ref(), 5, true, t(3600))
        .await
        .unwrap()
    else {
        panic!("not forced")
    };
    assert_ne!(again.serial, cert.serial);
    assert_eq!(again.history.len(), 2);
}

#[tokio::test]
async fn renewal_comes_when_a_third_is_left_and_on_a_new_key() {
    let (dc, g) = stores();
    let c = ca(120);
    let _k = request(dc.as_ref(), 2).await;
    let IssueOutcome::Issued(first) = c
        .issue(dc.as_ref(), g.as_ref(), 2, false, t(0))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(
        c.issue(dc.as_ref(), g.as_ref(), 2, false, t(79))
            .await
            .unwrap(),
        IssueOutcome::Current(_)
    ));
    let IssueOutcome::Issued(second) = c
        .issue(dc.as_ref(), g.as_ref(), 2, false, t(81))
        .await
        .unwrap()
    else {
        panic!("not renewed")
    };
    assert_ne!(first.serial, second.serial);
    // The node made a new key: a certificate for it at once.
    let _k2 = request(dc.as_ref(), 2).await;
    assert!(matches!(
        c.issue(dc.as_ref(), g.as_ref(), 2, false, t(82))
            .await
            .unwrap(),
        IssueOutcome::Issued(_)
    ));
}

#[tokio::test]
async fn revoked_nodes_get_nothing_until_readmitted() {
    let (dc, g) = stores();
    let c = ca(NODE_CERT_DEFAULT_SECONDS);
    let _k = request(dc.as_ref(), 3).await;
    c.issue(dc.as_ref(), g.as_ref(), 3, false, t(0))
        .await
        .unwrap();
    c.issue(dc.as_ref(), g.as_ref(), 3, true, t(1))
        .await
        .unwrap();
    let serials = revoke(dc.as_ref(), g.as_ref(), 3, "removed", t(2))
        .await
        .unwrap();
    assert_eq!(serials.len(), 2);
    let listed = node_ca::list::<Revoked>(g.as_ref(), node_ca::REVOKED)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|r| r.node == 3 && r.dc == "dc-a"));
    assert_eq!(
        c.issue(dc.as_ref(), g.as_ref(), 3, true, t(3))
            .await
            .unwrap(),
        IssueOutcome::Revoked
    );
    let v = view(Some(&c), dc.as_ref(), g.as_ref(), t(4)).await.unwrap();
    assert!(v.nodes.iter().any(|n| n.node == 3 && n.revoked));
    assert_eq!(v.revoked.len(), 2);
    assert!(readmit(dc.as_ref(), 3).await.unwrap());
    assert!(matches!(
        c.issue(dc.as_ref(), g.as_ref(), 3, false, t(5))
            .await
            .unwrap(),
        IssueOutcome::Issued(_)
    ));
    // Revocations go once their certificates have expired.
    assert_eq!(
        prune_revoked(g.as_ref(), t(NODE_CERT_DEFAULT_SECONDS + 10))
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn another_installations_key_opens_nothing() {
    let (dc, g) = stores();
    ca(NODE_CERT_DEFAULT_SECONDS)
        .init(dc.as_ref(), g.as_ref(), t(0))
        .await
        .unwrap();
    let _k = request(dc.as_ref(), 1).await;
    let other = InternalCa::new(SealKey::new([1u8; 32]), NODE_CERT_DEFAULT_SECONDS);
    let e = other
        .issue(dc.as_ref(), g.as_ref(), 1, false, t(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
}

#[test]
fn loop_keys_come_from_requests_and_node_records() {
    assert_eq!(node_of("ca/requests/12"), Some(12));
    assert_eq!(node_of("nodes/7"), Some(7));
    assert_eq!(node_of("ca/certs/7"), None);
}
