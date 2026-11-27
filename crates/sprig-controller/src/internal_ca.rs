//! The internal CA: one root for the installation, one issuing CA per DC,
//! and a short-lived certificate for every engine node, issued and renewed
//! by the DC leader's controller (records: `sprig_catalog::node_ca`).
//!
//! ```text
//! the controller holds the installation's CA key (SPRIGDB_CA_KEY_FILE: 64 hex digits)
//! tick, every minute on the DC leader:
//!   root missing           -> make it (P-256, 10 years), its key sealed under the CA key
//!   issuing CA missing or
//!   under 90 days left     -> make this DC's, signed by the root (2 years)
//!   revocations expired    -> delete them
//! node n (its key request or node record changed, or its renewal is due):
//!   node removed           -> revoke every unexpired certificate it holds
//!   key request, and no certificate, another key, or under a third of the
//!   validity left         -> sign `node-<n>` (SPRIGDB_CA_NODE_CERT_SECONDS, 30 days)
//! ```
//!
//! Nodes make their own keys: the controller sees only public keys and
//! never a node's private key. Without the CA key file the loop does
//! nothing (nodes then use certificates made by hand, as before).

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PublicKeyData, SerialNumber, SignatureAlgorithm,
};
use serde::{Deserialize, Serialize};
use sprig_auth::sealed::SealKey;
use sprig_catalog::node_ca::{self, CaRecord, Issued, KeyRequest, NodeCert, Revoked};
use sprig_catalog::units::NodeRecord;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, MetaStore, Op, Record, Tier};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// The variable naming the file with the installation's CA key.
pub const KEY_FILE_VAR: &str = "SPRIGDB_CA_KEY_FILE";
/// The variable setting node certificates' validity in seconds (tests use
/// minutes; at least 60).
pub const NODE_CERT_SECONDS_VAR: &str = "SPRIGDB_CA_NODE_CERT_SECONDS";
const DAY: u64 = 86_400;
/// Node certificates' validity by default.
pub const NODE_CERT_DEFAULT_SECONDS: u64 = 30 * DAY;
const ROOT_VALIDITY: u64 = 10 * 365 * DAY;
const ISSUING_VALIDITY: u64 = 2 * 365 * DAY;
const ISSUING_RENEW_BEFORE: u64 = 90 * DAY;
const ROOT_SUBJECT: &str = "SprigDB internal root CA";
/// How far back a certificate's validity starts, for clocks a little
/// behind.
const BACKDATE: u64 = 3600;

fn unix(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn at(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

#[allow(clippy::needless_pass_by_value)] // the shape `map_err` hands over
fn rc(e: rcgen::Error) -> SprigError {
    SprigError::internal(format!("internal CA: {e}"))
}

fn aad(subject: &str) -> Vec<u8> {
    format!("sprig-ca:{subject}").into_bytes()
}

/// The name a node's certificate carries (subjectAltName and common name).
#[must_use]
pub fn node_name(node: u64) -> String {
    format!("node-{node}")
}

fn dc_name(store: &dyn MetaStore) -> String {
    match store.tier() {
        Tier::Dc(d) => d.to_string(),
        Tier::Global => "global".to_owned(),
    }
}

fn ca_params(subject: &str, path_len: u8) -> CertificateParams {
    let mut p = CertificateParams::default();
    p.distinguished_name = DistinguishedName::new();
    p.distinguished_name.push(DnType::CommonName, subject);
    p.is_ca = IsCa::Ca(BasicConstraints::Constrained(path_len));
    p.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    p
}

/// 16 random bytes, positive and without a leading zero, so the DER
/// integer's content is exactly these bytes (nodes compare serials by
/// them).
fn serial() -> Result<Vec<u8>> {
    let mut b = sprig_auth::sealed::random::<16>()?;
    b[0] = (b[0] & 0x7f) | 0x40;
    Ok(b.to_vec())
}

/// A node's P-256 public key, as its key request names it.
struct NodeKey(Vec<u8>);

impl PublicKeyData for NodeKey {
    fn der_bytes(&self) -> &[u8] {
        &self.0
    }
    fn algorithm(&self) -> &'static SignatureAlgorithm {
        &rcgen::PKCS_ECDSA_P256_SHA256
    }
}

/// What [`InternalCa::issue`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueOutcome {
    /// A new certificate.
    Issued(NodeCert),
    /// The current one is good for a while.
    Current(NodeCert),
    /// The node published no key.
    NoRequest,
    /// The node's certificates are revoked.
    Revoked,
    /// Another controller wrote the node's record meanwhile; read again.
    Raced,
}

/// `POST /admin/v1/ca:init`'s answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitView {
    /// The root CA.
    pub root: CaSummary,
    /// The root was made by this call.
    pub root_created: bool,
    /// This DC's issuing CA.
    pub issuing: CaSummary,
    /// The issuing CA was made by this call.
    pub issuing_created: bool,
}

/// A CA certificate, without its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaSummary {
    /// Its subject.
    pub subject: String,
    /// Its SHA-256 fingerprint, hex.
    pub fingerprint: String,
    /// Valid until (Unix seconds).
    pub not_after: u64,
}

impl From<&CaRecord> for CaSummary {
    fn from(r: &CaRecord) -> Self {
        Self {
            subject: r.subject.clone(),
            fingerprint: r.fingerprint.clone(),
            not_after: r.not_after,
        }
    }
}

/// One node in [`CaView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    /// The node.
    pub node: u64,
    /// It published a key.
    pub requested: bool,
    /// Its current certificate's serial, hex (empty: none).
    pub serial: String,
    /// Valid until (Unix seconds; 0: none).
    pub not_after: u64,
    /// When the controller renews it (Unix seconds; 0: none).
    pub renew_at: u64,
    /// Its certificate certifies the key it published.
    pub key_matches: bool,
    /// Revoked (the node was removed).
    pub revoked: bool,
}

/// `GET /admin/v1/ca`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaView {
    /// This controller holds the CA key (`SPRIGDB_CA_KEY_FILE`).
    pub enabled: bool,
    /// Node certificates' validity, seconds.
    pub node_cert_seconds: u64,
    /// The root CA.
    pub root: Option<CaSummary>,
    /// This DC's issuing CA.
    pub issuing: Option<CaSummary>,
    /// This DC's nodes.
    pub nodes: Vec<NodeView>,
    /// Revoked certificates not yet expired (every DC's).
    pub revoked: Vec<Revoked>,
}

/// The internal CA as one controller holds it: the sealing key and the
/// node certificates' validity.
#[derive(Clone)]
pub struct InternalCa {
    key: Arc<SealKey>,
    node_validity: u64,
}

impl fmt::Debug for InternalCa {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InternalCa")
            .field("node_validity", &self.node_validity)
            .finish_non_exhaustive()
    }
}

impl InternalCa {
    /// The CA sealing its keys under `key`, issuing node certificates
    /// valid `node_validity_secs` (at least 60).
    #[must_use]
    pub fn new(key: SealKey, node_validity_secs: u64) -> Self {
        Self {
            key: Arc::new(key),
            node_validity: node_validity_secs.max(60),
        }
    }

    /// The CA from [`KEY_FILE_VAR`] and [`NODE_CERT_SECONDS_VAR`]; `None`
    /// when no key file is set.
    ///
    /// # Errors
    /// 39998 for a key file that is missing, writable by everyone or not 64
    /// hex digits.
    pub fn from_env() -> Result<Option<Self>> {
        let Some(path) = std::env::var(KEY_FILE_VAR)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
        else {
            return Ok(None);
        };
        let key = SealKey::from_file(std::path::Path::new(&path), KEY_FILE_VAR)?;
        let secs = std::env::var(NODE_CERT_SECONDS_VAR)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(NODE_CERT_DEFAULT_SECONDS);
        Ok(Some(Self::new(key, secs)))
    }

    /// Node certificates' validity, seconds.
    #[must_use]
    pub fn node_validity(&self) -> u64 {
        self.node_validity
    }

    /// A node certificate is renewed once less than this is left: a third
    /// of its validity (10 of 30 days).
    #[must_use]
    pub fn renew_before(&self) -> u64 {
        self.node_validity / 3
    }

    fn make_ca(
        &self,
        subject: &str,
        path_len: u8,
        validity: u64,
        parent: Option<&CaRecord>,
        now: u64,
    ) -> Result<CaRecord> {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(rc)?;
        let mut p = ca_params(subject, path_len);
        p.not_before = at(now.saturating_sub(BACKDATE)).into();
        p.not_after = at(now + validity).into();
        p.serial_number = Some(SerialNumber::from_slice(&serial()?));
        let cert = match parent {
            None => p.self_signed(&key),
            Some(par) => {
                p.use_authority_key_identifier_extension = true;
                let issuer = self.issuer_of(par)?;
                p.signed_by(&key, &issuer)
            }
        }
        .map_err(rc)?;
        let pem = cert.pem();
        let sealed = self
            .key
            .seal(&aad(subject), key.serialize_pem().as_bytes())?;
        Ok(CaRecord {
            subject: subject.to_owned(),
            fingerprint: node_ca::fingerprint(&pem),
            cert_pem: pem,
            sealed_key: node_ca::hex(&sealed),
            not_after: now + validity,
            created_at: now,
        })
    }

    /// The issuer that signs with `rec`'s key.
    fn issuer_of(&self, rec: &CaRecord) -> Result<Issuer<'static, KeyPair>> {
        let sealed = node_ca::unhex(&rec.sealed_key)
            .ok_or_else(|| SprigError::internal("internal CA: a sealed key isn't hex"))?;
        let pem = self.key.open(&aad(&rec.subject), &sealed).map_err(|_| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!(
                    "internal CA: the key of {:?} doesn't open with {KEY_FILE_VAR}'s key (another installation's key file?)",
                    rec.subject
                ),
            )
        })?;
        let pem = std::str::from_utf8(&pem)
            .map_err(|_| SprigError::internal("internal CA: a sealed key isn't PEM"))?;
        let kp = KeyPair::from_pem(pem).map_err(rc)?;
        Ok(Issuer::new(ca_params(&rec.subject, 0), kp))
    }

    /// The root, made when missing. Returns it and whether this call made
    /// it.
    async fn root(&self, global: &dyn MetaStore, now: u64) -> Result<(CaRecord, bool)> {
        if let Some((r, _)) = node_ca::read::<CaRecord>(global, node_ca::ROOT).await? {
            return Ok((r, false));
        }
        let rec = self.make_ca(ROOT_SUBJECT, 1, ROOT_VALIDITY, None, now)?;
        let key = global.tier().key(node_ca::ROOT)?;
        let r = global
            .txn(sprig_meta::Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            tracing::info!(fingerprint = %rec.fingerprint, "internal CA: root made");
            return Ok((rec, true));
        }
        node_ca::read::<CaRecord>(global, node_ca::ROOT)
            .await?
            .map(|(r, _)| (r, false))
            .ok_or_else(|| SprigError::internal("internal CA: the root vanished"))
    }

    /// The root and this DC's issuing CA, each made when missing (the
    /// issuing CA also when under 90 days are left). Returns them and
    /// whether this call made each.
    async fn issuing(
        &self,
        dc: &dyn MetaStore,
        global: &dyn MetaStore,
        now: u64,
    ) -> Result<(CaRecord, bool, CaRecord, bool)> {
        let (root, root_made) = self.root(global, now).await?;
        let cur = node_ca::read::<CaRecord>(dc, node_ca::ISSUING).await?;
        if let Some((c, _)) = &cur
            && c.not_after > now + ISSUING_RENEW_BEFORE
        {
            return Ok((root, root_made, c.clone(), false));
        }
        let subject = format!("SprigDB issuing CA {}", dc_name(dc));
        let rec = self.make_ca(&subject, 0, ISSUING_VALIDITY, Some(&root), now)?;
        let key = dc.tier().key(node_ca::ISSUING)?;
        let when = match &cur {
            Some((_, rev)) => Cmp::ModRev(key.clone(), *rev),
            None => Cmp::Missing(key.clone()),
        };
        let r = dc
            .txn(sprig_meta::Txn {
                when: vec![when],
                then: vec![Op::Put {
                    key,
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            tracing::info!(subject, fingerprint = %rec.fingerprint, "internal CA: issuing CA made");
            return Ok((root, root_made, rec, true));
        }
        let (c, _) = node_ca::read::<CaRecord>(dc, node_ca::ISSUING)
            .await?
            .ok_or_else(|| SprigError::internal("internal CA: the issuing CA vanished"))?;
        Ok((root, root_made, c, false))
    }

    /// Makes the root and this DC's issuing CA where they are missing.
    ///
    /// # Errors
    /// The store's; 39998 when the stored keys don't open with this key.
    pub async fn init(
        &self,
        dc: &dyn MetaStore,
        global: &dyn MetaStore,
        now: SystemTime,
    ) -> Result<InitView> {
        let (root, root_created, issuing, issuing_created) =
            self.issuing(dc, global, unix(now)).await?;
        Ok(InitView {
            root: CaSummary::from(&root),
            root_created,
            issuing: CaSummary::from(&issuing),
            issuing_created,
        })
    }

    /// Issues node `node` a certificate for the key it published, when it
    /// has none, has published another key, is within
    /// [`Self::renew_before`] of expiry, or `force`.
    ///
    /// # Errors
    /// The store's; `Internal` for a malformed key request.
    pub async fn issue(
        &self,
        dc: &dyn MetaStore,
        global: &dyn MetaStore,
        node: u64,
        force: bool,
        now: SystemTime,
    ) -> Result<IssueOutcome> {
        let now = unix(now);
        let Some((req, _)) =
            node_ca::read::<KeyRequest>(dc, &format!("{}{node}", node_ca::REQUESTS)).await?
        else {
            return Ok(IssueOutcome::NoRequest);
        };
        let cur = node_ca::read::<NodeCert>(dc, &format!("{}{node}", node_ca::CERTS)).await?;
        if let Some((c, _)) = &cur {
            if c.revoked {
                return Ok(IssueOutcome::Revoked);
            }
            if !force && c.public_key == req.public_key && c.not_after > now + self.renew_before() {
                return Ok(IssueOutcome::Current(c.clone()));
            }
        }
        let pk = node_ca::unhex(&req.public_key)
            .filter(|b| b.len() == 65 && b[0] == 4)
            .ok_or_else(|| {
                SprigError::internal(format!(
                    "internal CA: node {node}'s key request isn't a P-256 public key"
                ))
            })?;
        let (_, _, issuing, _) = self.issuing(dc, global, now).await?;
        let issuer = self.issuer_of(&issuing)?;
        let name = node_name(node);
        let mut p = CertificateParams::new(vec![name.clone()]).map_err(rc)?;
        p.distinguished_name = DistinguishedName::new();
        p.distinguished_name.push(DnType::CommonName, name);
        let not_after = (now + self.node_validity).min(issuing.not_after);
        p.not_before = at(now.saturating_sub(BACKDATE)).into();
        p.not_after = at(not_after).into();
        let serial = serial()?;
        p.serial_number = Some(SerialNumber::from_slice(&serial));
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        p.use_authority_key_identifier_extension = true;
        let cert = p.signed_by(&NodeKey(pk), &issuer).map_err(rc)?;
        let serial = node_ca::hex(&serial);
        let mut history: Vec<Issued> = cur
            .as_ref()
            .map(|(c, _)| c.history.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|i| i.not_after > now)
            .collect();
        history.push(Issued {
            serial: serial.clone(),
            not_after,
        });
        let rec = NodeCert {
            node,
            serial,
            not_before: now.saturating_sub(BACKDATE),
            not_after,
            public_key: req.public_key.clone(),
            chain_pem: format!("{}{}", cert.pem(), issuing.cert_pem),
            issued_at: now,
            history,
            revoked: false,
        };
        if !put_cert(dc, node, &rec, cur.as_ref().map(|(_, r)| *r)).await? {
            return Ok(IssueOutcome::Raced);
        }
        tracing::info!(node, serial = %rec.serial, not_after, "internal CA: node certificate issued");
        Ok(IssueOutcome::Issued(rec))
    }
}

/// Writes `node`'s certificate record if it is still at `rev` (`None`:
/// missing). Returns whether it was written.
async fn put_cert(
    dc: &dyn MetaStore,
    node: u64,
    rec: &NodeCert,
    rev: Option<sprig_meta::Revision>,
) -> Result<bool> {
    let key = node_ca::cert_key(dc, node)?;
    let when = match rev {
        Some(r) => Cmp::ModRev(key.clone(), r),
        None => Cmp::Missing(key.clone()),
    };
    Ok(dc
        .txn(sprig_meta::Txn {
            when: vec![when],
            then: vec![Op::Put {
                key,
                value: Record::new(rec.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?
        .succeeded)
}

/// Revokes every unexpired certificate of node `node` (in the global tier,
/// so every DC's nodes refuse them and close the node's connections) and
/// marks the node revoked: it gets no new certificate until
/// [`readmit`]. Returns the serials revoked.
///
/// # Errors
/// The store's.
pub async fn revoke(
    dc: &dyn MetaStore,
    global: &dyn MetaStore,
    node: u64,
    reason: &str,
    now: SystemTime,
) -> Result<Vec<String>> {
    let now = unix(now);
    for _ in 0..5 {
        let cur = node_ca::read::<NodeCert>(dc, &format!("{}{node}", node_ca::CERTS)).await?;
        let mut rec = cur.as_ref().map_or_else(
            || NodeCert {
                node,
                serial: String::new(),
                not_before: 0,
                not_after: 0,
                public_key: String::new(),
                chain_pem: String::new(),
                issued_at: 0,
                history: Vec::new(),
                revoked: true,
            },
            |(c, _)| c.clone(),
        );
        let mut serials: Vec<Issued> = rec
            .history
            .iter()
            .filter(|i| i.not_after > now)
            .cloned()
            .collect();
        if !rec.serial.is_empty() && !serials.iter().any(|i| i.serial == rec.serial) {
            serials.push(Issued {
                serial: rec.serial.clone(),
                not_after: rec.not_after,
            });
        }
        let mut ops = Vec::new();
        for i in &serials {
            ops.push(Op::Put {
                key: global
                    .tier()
                    .key(&format!("{}{}", node_ca::REVOKED, i.serial))?,
                value: Record::new(Revoked {
                    serial: i.serial.clone(),
                    dc: dc_name(dc),
                    node,
                    not_after: i.not_after,
                    at: now,
                    reason: reason.to_owned(),
                })
                .encode()?,
                lease: None,
            });
        }
        if !ops.is_empty() {
            global
                .txn(sprig_meta::Txn {
                    when: Vec::new(),
                    then: ops,
                    otherwise: Vec::new(),
                })
                .await?;
        }
        rec.revoked = true;
        if put_cert(dc, node, &rec, cur.as_ref().map(|(_, r)| *r)).await? {
            tracing::warn!(
                node,
                serials = serials.len(),
                reason,
                "internal CA: node certificates revoked"
            );
            return Ok(serials.into_iter().map(|i| i.serial).collect());
        }
    }
    Err(SprigError::internal(format!(
        "internal CA: node {node}'s record kept changing; revoke again"
    )))
}

/// Lets a revoked node have certificates again (its revoked ones stay
/// revoked). Returns whether it was revoked.
///
/// # Errors
/// The store's.
pub async fn readmit(dc: &dyn MetaStore, node: u64) -> Result<bool> {
    for _ in 0..5 {
        let Some((mut rec, rev)) =
            node_ca::read::<NodeCert>(dc, &format!("{}{node}", node_ca::CERTS)).await?
        else {
            return Ok(false);
        };
        if !rec.revoked {
            return Ok(false);
        }
        rec.revoked = false;
        // Renewed at once: the old certificates are revoked.
        rec.not_after = 0;
        if put_cert(dc, node, &rec, Some(rev)).await? {
            return Ok(true);
        }
    }
    Err(SprigError::internal(format!(
        "internal CA: node {node}'s record kept changing; readmit again"
    )))
}

/// Deletes revocations of certificates that have expired.
///
/// # Errors
/// The store's.
pub async fn prune_revoked(global: &dyn MetaStore, now: SystemTime) -> Result<usize> {
    let now = unix(now);
    let all = node_ca::list::<Revoked>(global, node_ca::REVOKED).await?;
    let mut n = 0;
    for r in all.iter().filter(|r| r.not_after < now) {
        global
            .txn(sprig_meta::Txn {
                when: Vec::new(),
                then: vec![Op::Delete(global.tier().key(&format!(
                    "{}{}",
                    node_ca::REVOKED,
                    r.serial
                ))?)],
                otherwise: Vec::new(),
            })
            .await?;
        n += 1;
    }
    Ok(n)
}

/// The CA's state for `sprigctl ca show`.
///
/// # Errors
/// The store's.
pub async fn view(
    ca: Option<&InternalCa>,
    dc: &dyn MetaStore,
    global: &dyn MetaStore,
    now: SystemTime,
) -> Result<CaView> {
    let now = unix(now);
    let root = node_ca::read::<CaRecord>(global, node_ca::ROOT).await?;
    let issuing = node_ca::read::<CaRecord>(dc, node_ca::ISSUING).await?;
    let reqs = node_ca::list::<KeyRequest>(dc, node_ca::REQUESTS).await?;
    let certs = node_ca::list::<NodeCert>(dc, node_ca::CERTS).await?;
    let renew_before = ca.map_or(NODE_CERT_DEFAULT_SECONDS / 3, InternalCa::renew_before);
    let mut nodes: std::collections::BTreeMap<u64, NodeView> = std::collections::BTreeMap::new();
    for c in &certs {
        nodes.insert(
            c.node,
            NodeView {
                node: c.node,
                requested: false,
                serial: c.serial.clone(),
                not_after: c.not_after,
                renew_at: if c.not_after == 0 {
                    0
                } else {
                    c.not_after.saturating_sub(renew_before)
                },
                key_matches: false,
                revoked: c.revoked,
            },
        );
    }
    for r in &reqs {
        let v = nodes.entry(r.node).or_insert_with(|| NodeView {
            node: r.node,
            requested: true,
            serial: String::new(),
            not_after: 0,
            renew_at: 0,
            key_matches: false,
            revoked: false,
        });
        v.requested = true;
        v.key_matches = certs
            .iter()
            .any(|c| c.node == r.node && c.public_key == r.public_key);
    }
    let mut revoked = node_ca::list::<Revoked>(global, node_ca::REVOKED).await?;
    revoked.retain(|r| r.not_after >= now);
    Ok(CaView {
        enabled: ca.is_some(),
        node_cert_seconds: ca.map_or(NODE_CERT_DEFAULT_SECONDS, InternalCa::node_validity),
        root: root.as_ref().map(|(r, _)| CaSummary::from(r)),
        issuing: issuing.as_ref().map(|(r, _)| CaSummary::from(r)),
        nodes: nodes.into_values().collect(),
        revoked,
    })
}

/// The loop's keys: the CAs themselves, or one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaKey {
    /// The root and this DC's issuing CA, and expired revocations.
    Cas,
    /// One node's certificate.
    Node(u64),
}

impl fmt::Display for CaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cas => f.write_str("cas"),
            Self::Node(n) => write!(f, "node-{n}"),
        }
    }
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct CaLoop {
    ca: Option<InternalCa>,
}

impl CaLoop {
    /// The loop for `ca` (`None`: the CA is off and the loop idles).
    #[must_use]
    pub fn new(ca: Option<InternalCa>) -> Self {
        Self { ca }
    }
}

fn node_of(path: &str) -> Option<u64> {
    path.strip_prefix(node_ca::REQUESTS)
        .or_else(|| path.strip_prefix("nodes/"))
        .and_then(|n| n.parse().ok())
}

/// How long until `c` is due for renewal (at least a second, at most an
/// hour, so a changed validity setting is seen).
fn until_renewal(ca: &InternalCa, c: &NodeCert, now: SystemTime) -> Duration {
    let due = c.not_after.saturating_sub(ca.renew_before());
    Duration::from_secs(due.saturating_sub(unix(now)).clamp(1, 3600))
}

#[async_trait::async_trait]
impl Reconciler for CaLoop {
    type Key = CaKey;

    fn name(&self) -> &'static str {
        "internal_ca"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc(node_ca::REQUESTS), WatchSpec::dc("nodes/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<CaKey> {
        node_of(ev.path()).map(CaKey::Node).into_iter().collect()
    }

    async fn all_keys(&self, cx: &LoopCx) -> Result<Vec<CaKey>> {
        if self.ca.is_none() {
            return Ok(Vec::new());
        }
        let mut keys = vec![CaKey::Cas];
        let mut ids: Vec<u64> = node_ca::list::<KeyRequest>(cx.dc.as_ref(), node_ca::REQUESTS)
            .await?
            .iter()
            .map(|r| r.node)
            .collect();
        ids.extend(
            sprig_catalog::units::nodes(cx.dc.as_ref())
                .await?
                .iter()
                .map(|n| n.id),
        );
        ids.sort_unstable();
        ids.dedup();
        keys.extend(ids.into_iter().map(CaKey::Node));
        Ok(keys)
    }

    async fn reconcile(&self, cx: &LoopCx, key: &CaKey) -> Result<Outcome> {
        let Some(ca) = &self.ca else {
            return Ok(Outcome::Done);
        };
        cx.leader()?;
        let now = cx.clock.wall();
        match *key {
            CaKey::Cas => {
                ca.init(cx.dc.as_ref(), cx.global.as_ref(), now).await?;
                let n = prune_revoked(cx.global.as_ref(), now).await?;
                if n > 0 {
                    tracing::info!(n, "internal CA: expired revocations deleted");
                }
                Ok(Outcome::RequeueAfter(Duration::from_secs(60)))
            }
            CaKey::Node(node) => {
                let rec = node_ca::read::<NodeRecord>(cx.dc.as_ref(), &format!("nodes/{node}"))
                    .await?
                    .map(|(r, _)| r);
                if rec.as_ref().is_some_and(|r| r.state == "removed") {
                    let revoked = node_ca::read::<NodeCert>(
                        cx.dc.as_ref(),
                        &format!("{}{node}", node_ca::CERTS),
                    )
                    .await?
                    .is_some_and(|(c, _)| c.revoked);
                    if !revoked {
                        revoke(
                            cx.dc.as_ref(),
                            cx.global.as_ref(),
                            node,
                            "the node was removed",
                            now,
                        )
                        .await?;
                    }
                    return Ok(Outcome::Done);
                }
                match ca
                    .issue(cx.dc.as_ref(), cx.global.as_ref(), node, false, now)
                    .await?
                {
                    IssueOutcome::Issued(c) | IssueOutcome::Current(c) => {
                        Ok(Outcome::RequeueAfter(until_renewal(ca, &c, now)))
                    }
                    IssueOutcome::Raced => Ok(Outcome::RequeueAfter(Duration::from_secs(1))),
                    IssueOutcome::NoRequest | IssueOutcome::Revoked => Ok(Outcome::Done),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
