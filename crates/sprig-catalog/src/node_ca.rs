//! The internal CA's records: the root, each DC's issuing CA, the nodes'
//! key requests and certificates, and revoked certificates.
//!
//! ```text
//! /sprig/g/ca/root                      CaRecord   the installation's root (sealed key)
//! /sprig/g/ca/revoked/<serial>          Revoked    a revoked node certificate (until it expires)
//! /sprig/dc/<dc_id>/ca/issuing          CaRecord   this DC's issuing CA, signed by the root
//! /sprig/dc/<dc_id>/ca/requests/<node>  KeyRequest a node's public key (the node made the key)
//! /sprig/dc/<dc_id>/ca/certs/<node>     NodeCert   the node's current certificate and history
//! ```
//!
//! Each node makes its own key and publishes only the public half; the DC
//! leader's controller signs a certificate naming the node (`node-<id>`)
//! with the DC's issuing CA. Every node trusts the root, so nodes of every
//! DC trust each other, and one DC's issuing key signs only its own
//! nodes. The CAs' private keys are stored sealed (AES-256-GCM) under the
//! installation's CA key, which only controllers hold. Revocations live in
//! the global tier, so every DC refuses a revoked certificate.

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_meta::{Consistency, Key, ListOpts, MetaStore, Record, Versioned};

/// The key of the installation's root CA (global tier).
pub const ROOT: &str = "ca/root";
/// The key of a DC's issuing CA (DC tier).
pub const ISSUING: &str = "ca/issuing";
/// The prefix of node key requests (DC tier).
pub const REQUESTS: &str = "ca/requests/";
/// The prefix of node certificates (DC tier).
pub const CERTS: &str = "ca/certs/";
/// The prefix of revoked certificates (global tier).
pub const REVOKED: &str = "ca/revoked/";

/// A CA certificate and its sealed private key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaRecord {
    /// The certificate's subject common name (the issuer name of what it
    /// signs).
    pub subject: String,
    /// The certificate, PEM.
    pub cert_pem: String,
    /// The private key (PKCS#8 PEM) sealed under the installation's CA key,
    /// hex.
    pub sealed_key: String,
    /// The certificate's SHA-256 fingerprint, hex.
    pub fingerprint: String,
    /// Valid until (Unix seconds).
    pub not_after: u64,
    /// Made at (Unix seconds).
    pub created_at: u64,
}

impl Versioned for CaRecord {
    const V: u32 = 1;
}

/// A node's public key, published by the node itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRequest {
    /// The node (its Raft id).
    pub node: u64,
    /// The gateway process running it.
    #[serde(default)]
    pub gateway_id: String,
    /// The P-256 public key: the uncompressed point (65 bytes), hex.
    pub public_key: String,
    /// Published at (Unix seconds).
    pub at: u64,
}

impl Versioned for KeyRequest {
    const V: u32 = 1;
}

/// One certificate issued to a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Issued {
    /// Its serial number, hex.
    pub serial: String,
    /// Valid until (Unix seconds).
    pub not_after: u64,
}

/// A node's certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCert {
    /// The node (its Raft id).
    pub node: u64,
    /// The current certificate's serial number, hex.
    pub serial: String,
    /// Valid from (Unix seconds).
    pub not_before: u64,
    /// Valid until (Unix seconds).
    pub not_after: u64,
    /// The public key it certifies (as in its [`KeyRequest`]).
    pub public_key: String,
    /// The certificate followed by the DC's issuing CA certificate, PEM.
    pub chain_pem: String,
    /// Issued at (Unix seconds).
    pub issued_at: u64,
    /// Every certificate issued to the node that hasn't expired, the
    /// current one included (revocation lists them all).
    #[serde(default)]
    pub history: Vec<Issued>,
    /// Revoked: the node was removed; it gets no new certificate until an
    /// operator readmits it.
    #[serde(default)]
    pub revoked: bool,
}

impl Versioned for NodeCert {
    const V: u32 = 1;
}

/// A revoked certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revoked {
    /// Its serial number, hex.
    pub serial: String,
    /// The DC that issued it.
    pub dc: String,
    /// The node it named.
    pub node: u64,
    /// Valid until (Unix seconds): the entry can go after that.
    pub not_after: u64,
    /// Revoked at (Unix seconds).
    pub at: u64,
    /// Why.
    #[serde(default)]
    pub reason: String,
}

impl Versioned for Revoked {
    const V: u32 = 1;
}

/// `node`'s request key in `store`'s tier.
///
/// # Errors
/// The tier's key error.
pub fn request_key(store: &dyn MetaStore, node: u64) -> Result<Key> {
    store.tier().key(&format!("{REQUESTS}{node}"))
}

/// `node`'s certificate key in `store`'s tier.
///
/// # Errors
/// The tier's key error.
pub fn cert_key(store: &dyn MetaStore, node: u64) -> Result<Key> {
    store.tier().key(&format!("{CERTS}{node}"))
}

/// Reads one record of type `T` at `path` (relative to `store`'s tier),
/// with its mod revision.
///
/// # Errors
/// The store's; a record that doesn't decode.
pub async fn read<T: Versioned + serde::de::DeserializeOwned>(
    store: &dyn MetaStore,
    path: &str,
) -> Result<Option<(T, sprig_meta::Revision)>> {
    let key = store.tier().key(path)?;
    match store.get(&key, Consistency::Linearizable).await? {
        Some(kv) => Ok(Some((
            Record::<T>::decode(&kv.value)?.body,
            kv.mod_revision,
        ))),
        None => Ok(None),
    }
}

/// Every record of type `T` under `prefix` (relative to `store`'s tier);
/// records that don't decode are skipped.
///
/// # Errors
/// The store's.
pub async fn list<T: Versioned + serde::de::DeserializeOwned>(
    store: &dyn MetaStore,
    prefix: &str,
) -> Result<Vec<T>> {
    let prefix = store.tier().key(prefix)?;
    let mut out = Vec::new();
    let mut opts = ListOpts {
        limit: 5_000,
        ..ListOpts::default()
    };
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        out.extend(
            page.kvs
                .iter()
                .filter_map(|kv| Record::<T>::decode(&kv.value).ok())
                .map(|r| r.body),
        );
        match (page.more, page.kvs.last()) {
            (true, Some(last)) => {
                opts.at = Some(page.revision);
                opts.after = Some(last.key.clone());
            }
            _ => return Ok(out),
        }
    }
}

/// Lower-case hex of `b`.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

/// The bytes of a hex string; `None` when it isn't hex.
#[must_use]
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The SHA-256 fingerprint (hex) of the first certificate in `pem`; empty
/// when there is none.
#[must_use]
pub fn fingerprint(pem: &str) -> String {
    use sha2::{Digest, Sha256};
    first_der(pem).map_or_else(String::new, |d| hex(&Sha256::digest(d)))
}

/// The DER of the first PEM block in `pem`.
#[must_use]
pub fn first_der(pem: &str) -> Option<Vec<u8>> {
    let start = pem.find("-----BEGIN ")?;
    let body = &pem[start..];
    let body = &body[body.find('\n')? + 1..];
    let end = body.find("-----END ")?;
    let b64: String = body[..end].chars().filter(|c| !c.is_whitespace()).collect();
    b64_decode(&b64)
}

/// Standard base64 (with padding) decoding.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        }))
    };
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        let mut n = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            n |= val(*c)? << (18 - 6 * i);
        }
        let bytes = n.to_be_bytes();
        let take = match chunk.len() {
            4 => 3,
            3 => 2,
            2 => 1,
            _ => return None,
        };
        out.extend_from_slice(&bytes[1..=take]);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn hex_round_trips() {
        let b = [0u8, 1, 0x7f, 0x80, 0xff];
        assert_eq!(hex(&b), "00017f80ff");
        assert_eq!(unhex("00017f80ff").unwrap(), b);
        assert!(unhex("0g").is_none());
        assert!(unhex("abc").is_none());
    }

    #[test]
    fn pem_bodies_decode() {
        let pem = "junk\n-----BEGIN CERTIFICATE-----\nAAECAwQF\nBgc=\n-----END CERTIFICATE-----\n";
        assert_eq!(first_der(pem).unwrap(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(fingerprint("no pem").len(), 0);
        assert_eq!(fingerprint(pem).len(), 64);
    }
}
