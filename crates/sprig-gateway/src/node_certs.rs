//! Issued node certificates for the Raft transport
//! (`SPRIGDB_ENGINE_RAFT_TLS_ISSUED=on`, with `SPRIGDB_ENGINE_RAFT_TLS_DIR`).
//!
//! ```text
//! start, before Raft:
//!   node-key.pem in the directory, or make one (P-256, PKCS#8, mode 0600)
//!   publish its public key: /sprig/dc/<dc>/ca/requests/<raft id>
//!   wait for /sprig/dc/<dc>/ca/certs/<raft id> certifying that key, then write
//!   node.pem (the certificate and the DC's issuing CA) and ca.pem (the root)
//!   (files already there and naming this node: start with them after 10 s)
//! then, every 2 s:
//!   a renewed certificate  -> write the files, and the transport reloads them
//!   revoked certificates   -> the transport refuses them and closes their connections
//! ```
//!
//! The private key never leaves the node: the controller sees only the
//! public key.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ring::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair as _};
use sprig_catalog::node_ca::{self, CaRecord, KeyRequest, NodeCert, Revoked};
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_meta::{MetaStore, Op, Record};

use crate::node_engine;

/// The variable that turns issued certificates on.
pub const ISSUED_VAR: &str = "SPRIGDB_ENGINE_RAFT_TLS_ISSUED";
/// How often a running node looks for a renewed certificate and for
/// revocations.
const FOLLOW_EVERY: Duration = Duration::from_secs(2);
/// How long a starting node waits for a certificate before it starts with
/// the files it already has.
const START_WAIT_WITH_FILES: Duration = Duration::from_secs(10);

fn on(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "on" | "true" | "1" | "yes"
    )
}

/// Whether this node takes its Raft certificates from the internal CA: the
/// switch is on, it replicates and it has a certificate directory.
#[must_use]
pub fn issued_from_env() -> bool {
    std::env::var(ISSUED_VAR).is_ok_and(|v| on(&v))
        && dir().is_some()
        && node_engine::raft_id_from_env().is_some()
}

fn dir() -> Option<PathBuf> {
    std::env::var(node_engine::RAFT_TLS_DIR_VAR)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn err(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("node certificates: {what}: {e}"))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Writes `body` to `path` through a temporary file and a rename, so a
/// reader never sees half a file; `private` files get mode 0600.
fn write_atomic(path: &Path, body: &[u8], private: bool) -> Result<()> {
    use std::io::Write as _;
    let tmp = path.with_extension("tmp");
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt as _;
        o.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut f = o
        .open(&tmp)
        .map_err(|e| err(&tmp.display().to_string(), e))?;
    f.write_all(body)
        .and_then(|()| f.sync_all())
        .map_err(|e| err(&tmp.display().to_string(), e))?;
    std::fs::rename(&tmp, path).map_err(|e| err(&path.display().to_string(), e))
}

fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let b = base64::engine::general_purpose::STANDARD.encode(der);
    let mut s = format!("-----BEGIN {label}-----\n");
    for line in b.as_bytes().chunks(64) {
        s.push_str(&String::from_utf8_lossy(line));
        s.push('\n');
    }
    let _ = writeln!(s, "-----END {label}-----");
    s
}

/// This node's key (`node-key.pem` in `dir`), made when missing. Returns
/// its public key (the uncompressed P-256 point).
///
/// # Errors
/// The directory or the key file can't be read or written, or the key
/// isn't a P-256 PKCS#8 key.
pub fn node_key(dir: &Path) -> Result<Vec<u8>> {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject as _;
    let path = dir.join("node-key.pem");
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = if path.exists() {
        let text = std::fs::read(&path).map_err(|e| err(&path.display().to_string(), e))?;
        match PrivateKeyDer::from_pem_slice(&text)
            .map_err(|e| err(&path.display().to_string(), e))?
        {
            PrivateKeyDer::Pkcs8(k) => k.secret_pkcs8_der().to_vec(),
            _ => {
                return Err(err(
                    &path.display().to_string(),
                    "not a PKCS#8 key (issued certificates need the P-256 key the node made)",
                ));
            }
        }
    } else {
        std::fs::create_dir_all(dir).map_err(|e| err(&dir.display().to_string(), e))?;
        let doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
            .map_err(|e| err("making the node key", e))?;
        write_atomic(&path, pem("PRIVATE KEY", doc.as_ref()).as_bytes(), true)?;
        tracing::info!(path = %path.display(), "node certificates: node key made");
        doc.as_ref().to_vec()
    };
    let kp = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &pkcs8, &rng)
        .map_err(|e| err(&path.display().to_string(), e))?;
    Ok(kp.public_key().as_ref().to_vec())
}

/// Publishes `public` as node `id`'s key, unless it is published already.
async fn publish(dc: &dyn MetaStore, id: u64, gateway_id: &str, public: &str) -> Result<()> {
    let path = format!("{}{id}", node_ca::REQUESTS);
    if node_ca::read::<KeyRequest>(dc, &path)
        .await?
        .is_some_and(|(r, _)| r.public_key == public)
    {
        return Ok(());
    }
    dc.txn(sprig_meta::Txn {
        when: Vec::new(),
        then: vec![Op::Put {
            key: node_ca::request_key(dc, id)?,
            value: Record::new(KeyRequest {
                node: id,
                gateway_id: gateway_id.to_owned(),
                public_key: public.to_owned(),
                at: unix_now(),
            })
            .encode()?,
            lease: None,
        }],
        otherwise: Vec::new(),
    })
    .await?;
    tracing::info!(node = id, "node certificates: public key published");
    Ok(())
}

/// Node `id`'s current certificate when it certifies `public`, is
/// unexpired and not revoked, with the root's PEM.
async fn fetch(
    dc: &dyn MetaStore,
    global: &dyn MetaStore,
    id: u64,
    public: &str,
) -> Result<Option<(NodeCert, String)>> {
    let Some((c, _)) = node_ca::read::<NodeCert>(dc, &format!("{}{id}", node_ca::CERTS)).await?
    else {
        return Ok(None);
    };
    if c.revoked || c.public_key != public || c.not_after <= unix_now() || c.chain_pem.is_empty() {
        return Ok(None);
    }
    let Some((root, _)) = node_ca::read::<CaRecord>(global, node_ca::ROOT).await? else {
        return Ok(None);
    };
    Ok(Some((c, root.cert_pem)))
}

/// Writes the certificate files that differ; returns whether any did.
fn write_files(dir: &Path, chain: &str, root: &str) -> Result<bool> {
    let mut changed = false;
    for (name, body) in [("ca.pem", root), ("node.pem", chain)] {
        let path = dir.join(name);
        if std::fs::read(&path).is_ok_and(|b| b == body.as_bytes()) {
            continue;
        }
        write_atomic(&path, body.as_bytes(), false)?;
        changed = true;
    }
    Ok(changed)
}

/// Before Raft starts: this node's key, published, and its certificate
/// files from the controller (see the module docs). Returns when the files
/// are in place, or at `stop`.
///
/// # Errors
/// The key can't be made or read, or the files can't be written.
pub async fn obtain(
    dc: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    gateway_id: &str,
    stop: &CancellationToken,
) -> Result<()> {
    let (Some(dir), Some(id)) = (dir(), node_engine::raft_id_from_env()) else {
        return Ok(());
    };
    let public = node_ca::hex(&node_key(&dir)?);
    let have_files = dir.join("node.pem").exists() && dir.join("ca.pem").exists();
    let started = Instant::now();
    let mut said = Instant::now();
    loop {
        let r = async {
            publish(dc.as_ref(), id, gateway_id, &public).await?;
            fetch(dc.as_ref(), global.as_ref(), id, &public).await
        };
        match tokio::time::timeout(Duration::from_secs(5), r).await {
            Ok(Ok(Some((c, root)))) => {
                write_files(&dir, &c.chain_pem, &root)?;
                tracing::info!(node = id, serial = %c.serial, not_after = c.not_after, "node certificates: in place");
                return Ok(());
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => tracing::debug!(error = %e, "node certificates: not read yet"),
            Err(_) => tracing::debug!("node certificates: the control plane is slow"),
        }
        if have_files && started.elapsed() >= START_WAIT_WITH_FILES {
            tracing::warn!(
                node = id,
                "node certificates: none issued yet; starting with the files in {}",
                dir.display()
            );
            return Ok(());
        }
        if said.elapsed() >= Duration::from_secs(10) {
            tracing::warn!(
                node = id,
                "node certificates: waiting for the controller to issue this node's certificate (is SPRIGDB_CA_KEY_FILE set on the controllers?)"
            );
            said = Instant::now();
        }
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

/// Every revoked serial number (bytes) in the global tier.
async fn revoked_serials(global: &dyn MetaStore) -> Result<BTreeSet<Vec<u8>>> {
    Ok(node_ca::list::<Revoked>(global, node_ca::REVOKED)
        .await?
        .iter()
        .filter_map(|r| node_ca::unhex(&r.serial))
        .collect())
}

/// While the node runs: renewed certificates into the files and the
/// transport, and revocations into the transport, every 2 s until `stop`.
pub fn follow(
    dc: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    gateway_id: String,
    stop: CancellationToken,
) {
    let (Some(dir), Some(id)) = (dir(), node_engine::raft_id_from_env()) else {
        return;
    };
    tokio::spawn(async move {
        let public = match node_key(&dir) {
            Ok(p) => node_ca::hex(&p),
            Err(e) => {
                tracing::error!(error = %e, "node certificates: renewals stop");
                return;
            }
        };
        let mut last_serial = String::new();
        let mut last_revoked: Option<BTreeSet<Vec<u8>>> = None;
        loop {
            let pass = async {
                publish(dc.as_ref(), id, &gateway_id, &public).await?;
                if let Some((c, root)) = fetch(dc.as_ref(), global.as_ref(), id, &public).await?
                    && c.serial != last_serial
                {
                    if write_files(&dir, &c.chain_pem, &root)? {
                        match node_engine::reload_raft_tls() {
                            Ok(true) => {
                                tracing::info!(node = id, serial = %c.serial, not_after = c.not_after, "node certificates: renewed; the Raft transport took them");
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!(error = %e, "node certificates: the renewed files don't load");
                            }
                        }
                    }
                    last_serial = c.serial;
                }
                let revoked = revoked_serials(global.as_ref()).await?;
                if last_revoked.as_ref() != Some(&revoked) {
                    if !revoked.is_empty() {
                        tracing::info!(
                            n = revoked.len(),
                            "node certificates: revocations in force"
                        );
                    }
                    node_engine::set_raft_revoked(revoked.clone());
                    last_revoked = Some(revoked);
                }
                Ok::<(), SprigError>(())
            };
            if let Ok(Err(e)) = tokio::time::timeout(Duration::from_secs(10), pass).await {
                tracing::debug!(error = %e, "node certificates: a pass failed");
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(FOLLOW_EVERY) => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn the_node_key_is_made_once_and_kept_private() {
        let d = std::env::temp_dir().join(format!("sprig-node-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let a = node_key(&d).unwrap();
        assert_eq!(a.len(), 65);
        assert_eq!(a[0], 4);
        assert_eq!(node_key(&d).unwrap(), a, "the same key next time");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(d.join("node-key.pem"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0);
        }
        assert!(write_files(&d, "chain", "root").unwrap());
        assert!(!write_files(&d, "chain", "root").unwrap(), "unchanged");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn the_switch_reads_like_the_others() {
        assert!(on(" ON "));
        assert!(on("true"));
        assert!(!on("off"));
    }
}
