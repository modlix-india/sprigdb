//! Makes an internal CA and a certificate per node for the Raft transport's
//! mutual TLS with node identity (development and tests; each node's
//! certificate names it, subjectAltName `node-<id>`, valid 30 days).
//!
//! ```text
//! cargo run -p sprig-raft --example node_certs -- <dir> 1 2 3
//! ```
//!
//! writes `<dir>/ca.pem`, `<dir>/ca-key.pem` and, per node,
//! `<dir>/node-<id>/{ca.pem,node.pem,node-key.pem}`: point a gateway's
//! `SPRIGDB_ENGINE_RAFT_TLS_DIR` at its node's directory.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().expect("usage: node_certs <dir> <id>..."));
    let ids: Vec<u64> = args
        .map(|a| a.parse().expect("node ids are numbers"))
        .collect();
    std::fs::create_dir_all(&dir).unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name
        .push(DnType::CommonName, "SprigDB internal CA (development)");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca.self_signed(&ca_key).unwrap();
    std::fs::write(dir.join("ca.pem"), ca_cert.pem()).unwrap();
    write_private(&dir.join("ca-key.pem"), &ca_key.serialize_pem());
    let issuer = Issuer::new(ca, ca_key);
    for id in ids {
        let name = sprig_raft::tcp::node_name(id);
        let mut p = CertificateParams::new(vec![name.clone()]).unwrap();
        p.distinguished_name.push(DnType::CommonName, name.clone());
        let now = std::time::SystemTime::now();
        p.not_before = now.into();
        p.not_after = (now + std::time::Duration::from_secs(30 * 24 * 3600)).into();
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &issuer).unwrap();
        let d = dir.join(&name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("ca.pem"), ca_cert.pem()).unwrap();
        std::fs::write(d.join("node.pem"), cert.pem()).unwrap();
        write_private(&d.join("node-key.pem"), &key.serialize_pem());
        println!("{}", d.display());
    }
}

fn write_private(path: &std::path::Path, pem: &str) {
    use std::io::Write as _;
    #[cfg(unix)]
    let mut f = {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .unwrap()
    };
    #[cfg(not(unix))]
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(pem.as_bytes()).unwrap();
}
