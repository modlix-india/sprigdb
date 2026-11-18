//! The RSA key pair of `caching_sha2_password`'s full path without TLS.
//!
//! A client that logs in over plain TCP, and whose account isn't in the
//! fast cache yet, must send its password. MySQL lets it do that without
//! TLS by encrypting it with the server's RSA public key (RSA-OAEP with
//! SHA-1): the client asks for the key (`--get-server-public-key`; MariaDB's
//! client always asks) or has it in a file (`--server-public-key-path`).
//! Without a key the gateway refused that login with a TLS hint, and every
//! plain-TCP client failed (`ERROR 2061`) until a TLS login of the same
//! account filled the fast cache.
//!
//! Where the key comes from (`tls.caching_sha2_rsa_key`):
//!
//! - **empty** (the default): the gateway generates a 2048-bit pair when it
//!   starts, as MySQL does (`caching_sha2_password_auto_generate_rsa_keys`).
//!   Clients that fetch the key work with any gateway; a client pinned to a
//!   key file doesn't, since each gateway has its own key.
//! - **a path**: a PEM RSA private key (PKCS#1 `RSA PRIVATE KEY` or
//!   unencrypted PKCS#8 `PRIVATE KEY`), the same file on every gateway of a
//!   DC, so a pinned client reaches any of them. The file is checked again
//!   (size and modification time) at each login that needs the key, and
//!   read again when it changed. A file that can't be read or parsed is
//!   logged once per change, and plain-TCP full logins are refused with the
//!   TLS hint until it's fixed: the generated key is never used in its place,
//!   as a pinned client's key wouldn't match it.
//!
//! Decryption uses the Chinese remainder theorem with blinding (a random
//! factor folded into the ciphertext and taken out of the result), so its
//! timing doesn't follow the private exponent, and checks its result with
//! the public exponent. A failed decryption costs the client the same hash
//! work and error as a wrong password (`sprig_proto_mysql::auth`). Every
//! attempt is a login attempt, under the login rate limits and lockouts.
//!
//! The arithmetic is `num-bigint`'s (through `mysql_common`, already a
//! dependency); SHA-1 for OAEP is `sha1`'s, randomness `ring`'s. The
//! variables keep RSA's usual one-letter names (`n`, `e`, `d`, `p`, `q`).
#![allow(clippy::many_single_char_names)]

use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use mysql_common::num_bigint::BigUint;
use ring::rand::{SecureRandom, SystemRandom};
use sha1::{Digest, Sha1};
use sprig_proto_mysql::auth::ServerRsaKey;
use zeroize::Zeroizing;

/// The modulus size of a generated key, in bits.
pub const GENERATED_BITS: u64 = 2048;

/// SHA-1's output length (OAEP's `hLen`).
const HLEN: usize = 20;

/// The DER of OID 1.2.840.113549.1.1.1 (`rsaEncryption`).
const RSA_ENCRYPTION_OID: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01];

/// An RSA key pair, with the CRT values decryption uses.
pub struct RsaKeyPair {
    n: BigUint,
    e: BigUint,
    p: BigUint,
    q: BigUint,
    dp: BigUint,
    dq: BigUint,
    qinv: BigUint,
    size: usize,
    pem: Bytes,
}

impl std::fmt::Debug for RsaKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RsaKeyPair")
            .field("bits", &self.n.bits())
            .finish_non_exhaustive()
    }
}

fn random_bytes(n: usize) -> Option<Vec<u8>> {
    let mut b = vec![0u8; n];
    SystemRandom::new().fill(&mut b).ok()?;
    Some(b)
}

/// A random number in `[0, bound)` (slightly biased, which neither the
/// primality test's bases nor blinding mind).
fn random_below(bound: &BigUint) -> Option<BigUint> {
    let len = usize::try_from(bound.bits().div_ceil(8)).ok()? + 8;
    Some(BigUint::from_bytes_be(&random_bytes(len)?) % bound)
}

/// The odd primes below 2,000, for trial division of candidates.
fn small_primes() -> &'static [u32] {
    static PRIMES: OnceLock<Vec<u32>> = OnceLock::new();
    PRIMES.get_or_init(|| {
        (3u32..2000)
            .step_by(2)
            .filter(|n| {
                (3u32..)
                    .step_by(2)
                    .take_while(|d| d * d <= *n)
                    .all(|d| n % d != 0)
            })
            .collect()
    })
}

/// Miller–Rabin with `rounds` random bases (`n` odd and above the small
/// primes).
fn probably_prime(n: &BigUint, rounds: usize) -> Option<bool> {
    let one = BigUint::from(1u32);
    let two = BigUint::from(2u32);
    let n1 = n - &one;
    let s = n1.trailing_zeros()?;
    let d = &n1 >> s;
    let span = n - 3u32;
    'rounds: for _ in 0..rounds {
        let a = random_below(&span)? + 2u32;
        let mut x = a.modpow(&d, n);
        if x == one || x == n1 {
            continue;
        }
        for _ in 1..s {
            x = x.modpow(&two, n);
            if x == n1 {
                continue 'rounds;
            }
        }
        return Some(false);
    }
    Some(true)
}

/// A random prime of `bits` bits with its top two bits set (so two of them
/// make a modulus of exactly `2 * bits` bits) and `p - 1` prime to `e`.
fn random_prime(bits: u64, e: &BigUint) -> Option<BigUint> {
    let primes = small_primes();
    let len = usize::try_from(bits / 8).ok()?;
    loop {
        let mut b = random_bytes(len)?;
        b[0] |= 0xC0;
        b[len - 1] |= 1;
        // Residues of the start, so each step costs no division of the big
        // number.
        let residues: Vec<u32> = primes
            .iter()
            .map(|&p| {
                b.iter()
                    .fold(0u64, |r, &x| (r * 256 + u64::from(x)) % u64::from(p))
            })
            .map(|r| u32::try_from(r).unwrap_or(0))
            .collect();
        let start = BigUint::from_bytes_be(&b);
        for delta in (0u32..1 << 16).step_by(2) {
            if primes
                .iter()
                .zip(&residues)
                .any(|(&p, &r)| (u64::from(r) + u64::from(delta)) % u64::from(p) == 0)
            {
                continue;
            }
            let c = &start + delta;
            if c.bits() != bits {
                break;
            }
            if (&c - 1u32).modinv(e).is_none() {
                continue;
            }
            if probably_prime(&c, 8)? {
                return Some(c);
            }
        }
    }
}

impl RsaKeyPair {
    /// A new key pair of `bits` bits (even, at least 1024), exponent 65537.
    /// CPU-heavy (about a second in a debug build): run it off the async
    /// runtime. `None` only if the system's random source fails.
    #[must_use]
    pub fn generate(bits: u64) -> Option<Self> {
        let e = BigUint::from(65_537u32);
        loop {
            let p = random_prime(bits / 2, &e)?;
            let q = random_prime(bits / 2, &e)?;
            if p == q {
                continue;
            }
            let phi = (&p - 1u32) * (&q - 1u32);
            let d = e.modinv(&phi)?;
            let n = &p * &q;
            if let Ok(k) = Self::from_parts(n, e.clone(), &d, p, q) {
                return Some(k);
            }
        }
    }

    /// Reads a PEM private key: PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`)
    /// or unencrypted PKCS#8 (`-----BEGIN PRIVATE KEY-----`).
    ///
    /// # Errors
    /// A text naming what's wrong (no key, an encrypted key, not RSA, a
    /// modulus under 2048 bits, inconsistent values).
    pub fn from_pem(pem: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(pem).map_err(|_| "not a PEM text file".to_owned())?;
        if text.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----") {
            return Err("the key is encrypted; give an unencrypted PEM key".into());
        }
        let (label, der) = pem_body(text, "RSA PRIVATE KEY")
            .map(|d| ("RSA PRIVATE KEY", d))
            .or_else(|| pem_body(text, "PRIVATE KEY").map(|d| ("PRIVATE KEY", d)))
            .ok_or_else(|| {
                "no \"-----BEGIN RSA PRIVATE KEY-----\" or \"-----BEGIN PRIVATE KEY-----\" block"
                    .to_owned()
            })?;
        let der = Zeroizing::new(der?);
        let pkcs1: &[u8] = if label == "PRIVATE KEY" {
            pkcs8_inner(&der).ok_or("not an RSA PKCS#8 private key")?
        } else {
            &der
        };
        let ints = pkcs1_ints(pkcs1).ok_or("not a two-prime PKCS#1 RSA private key")?;
        let [n, e, d, p, q] = ints;
        if n.bits() < 2048 {
            return Err(format!(
                "a {}-bit key is too small; use 2048 bits or more",
                n.bits()
            ));
        }
        Self::from_parts(n, e, &d, p, q)
    }

    fn from_parts(
        n: BigUint,
        e: BigUint,
        d: &BigUint,
        p: BigUint,
        q: BigUint,
    ) -> Result<Self, String> {
        let bad = || "the key's values don't fit together".to_owned();
        if &p * &q != n || p.bits() < 2 || q.bits() < 2 {
            return Err(bad());
        }
        let dp = d % (&p - 1u32);
        let dq = d % (&q - 1u32);
        let qinv = q.modinv(&p).ok_or_else(bad)?;
        let size = usize::try_from(n.bits().div_ceil(8)).map_err(|_| bad())?;
        let pem = public_pem(&n, &e);
        let k = Self {
            n,
            e,
            p,
            q,
            dp,
            dq,
            qinv,
            size,
            pem,
        };
        // One round trip proves d, p and q belong to n and e.
        let m = BigUint::from(0x5352_4947u32);
        let c = m.modpow(&k.e, &k.n);
        if k.raw_decrypt(&c).as_ref() != Some(&m) {
            return Err(bad());
        }
        Ok(k)
    }

    /// The public key as PEM (X.509 `SubjectPublicKeyInfo`).
    #[must_use]
    pub fn public_pem(&self) -> Bytes {
        self.pem.clone()
    }

    /// The modulus length in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.size
    }

    /// `c^d mod n` by the CRT, blinded, checked with `e`. `None` when `c`
    /// isn't below `n`, or the system's random source fails.
    fn raw_decrypt(&self, c: &BigUint) -> Option<BigUint> {
        if c >= &self.n {
            return None;
        }
        let (r, rinv) = loop {
            let r = random_below(&self.n)?;
            if r.bits() < 2 {
                continue;
            }
            if let Some(i) = r.modinv(&self.n) {
                break (r, i);
            }
        };
        let blinded = (c * r.modpow(&self.e, &self.n)) % &self.n;
        let m1 = blinded.modpow(&self.dp, &self.p);
        let m2 = blinded.modpow(&self.dq, &self.q);
        let m2p = &m2 % &self.p;
        let diff = if m1 >= m2p {
            m1 - m2p
        } else {
            m1 + &self.p - m2p
        };
        let h = (&self.qinv * diff) % &self.p;
        let m = ((m2 + h * &self.q) * rinv) % &self.n;
        (m.modpow(&self.e, &self.n) == *c).then_some(m)
    }

    /// RSA-OAEP decryption (SHA-1, MGF1-SHA-1, empty label) of a
    /// ciphertext of exactly [`Self::size`] bytes.
    #[must_use]
    pub fn decrypt(&self, ciphertext: &[u8]) -> Option<Zeroizing<Vec<u8>>> {
        if ciphertext.len() != self.size {
            return None;
        }
        let m = self.raw_decrypt(&BigUint::from_bytes_be(ciphertext))?;
        let bytes = Zeroizing::new(m.to_bytes_be());
        let mut em = Zeroizing::new(vec![0u8; self.size]);
        let pad = self.size.checked_sub(bytes.len())?;
        em[pad..].copy_from_slice(&bytes);
        oaep_decode(&em)
    }
}

/// MGF1 with SHA-1: `len` bytes from `seed`.
fn mgf1(seed: &[u8], len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + HLEN);
    let mut counter: u32 = 0;
    while out.len() < len {
        let mut h = Sha1::new();
        h.update(seed);
        h.update(counter.to_be_bytes());
        out.extend_from_slice(&h.finalize());
        counter += 1;
    }
    out.truncate(len);
    out
}

/// EME-OAEP decoding (RFC 8017, 7.1.2 step 3) with SHA-1 and the empty
/// label. Every check runs on every input, and they are combined before
/// the one branch, so the time taken doesn't say which failed.
fn oaep_decode(em: &[u8]) -> Option<Zeroizing<Vec<u8>>> {
    let k = em.len();
    if k < 2 * HLEN + 2 {
        return None;
    }
    let (y, rest) = (em[0], &em[1..]);
    let (masked_seed, masked_db) = rest.split_at(HLEN);
    let seed: Zeroizing<Vec<u8>> = Zeroizing::new(
        masked_seed
            .iter()
            .zip(mgf1(masked_db, HLEN))
            .map(|(a, b)| a ^ b)
            .collect(),
    );
    let db: Zeroizing<Vec<u8>> = Zeroizing::new(
        masked_db
            .iter()
            .zip(mgf1(&seed, k - HLEN - 1))
            .map(|(a, b)| a ^ b)
            .collect(),
    );
    let l_hash = Sha1::digest([]);
    let mut bad = u32::from(y);
    for (a, b) in db[..HLEN].iter().zip(l_hash.iter()) {
        bad |= u32::from(a ^ b);
    }
    // PS (zeros), then 0x01, then the message.
    let mut found: u32 = 0;
    let mut start: usize = 0;
    for (i, &b) in db[HLEN..].iter().enumerate() {
        let is_one = u32::from(b == 1);
        let is_zero = u32::from(b == 0);
        let first_one = is_one & (found ^ 1);
        start |= i & (usize::from(first_one == 1)).wrapping_neg();
        bad |= (found ^ 1) & (is_one ^ 1) & (is_zero ^ 1);
        found |= is_one;
    }
    bad |= found ^ 1;
    if bad != 0 {
        return None;
    }
    Some(Zeroizing::new(db[HLEN + start + 1..].to_vec()))
}

/// The DER length octets of `len`.
fn der_len(len: usize) -> Vec<u8> {
    if len < 0x80 {
        return vec![u8::try_from(len).unwrap_or(0)];
    }
    let bytes: Vec<u8> = len
        .to_be_bytes()
        .into_iter()
        .skip_while(|&b| b == 0)
        .collect();
    let mut out = vec![0x80 | u8::try_from(bytes.len()).unwrap_or(0)];
    out.extend(bytes);
    out
}

fn der_tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend(der_len(content.len()));
    out.extend_from_slice(content);
    out
}

fn der_uint(v: &BigUint) -> Vec<u8> {
    let mut b = v.to_bytes_be();
    if b.first().is_some_and(|&x| x & 0x80 != 0) {
        b.insert(0, 0);
    }
    der_tlv(0x02, &b)
}

/// The `SubjectPublicKeyInfo` PEM of `(n, e)`.
fn public_pem(n: &BigUint, e: &BigUint) -> Bytes {
    let rsa_public = der_tlv(0x30, &[der_uint(n), der_uint(e)].concat());
    let mut bits = vec![0u8];
    bits.extend(rsa_public);
    let alg = der_tlv(
        0x30,
        &[der_tlv(0x06, RSA_ENCRYPTION_OID), vec![0x05, 0x00]].concat(),
    );
    let spki = der_tlv(0x30, &[alg, der_tlv(0x03, &bits)].concat());
    let b64 = STANDARD.encode(spki);
    let mut pem = String::from("-----BEGIN PUBLIC KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        pem.push('\n');
    }
    pem.push_str("-----END PUBLIC KEY-----\n");
    Bytes::from(pem)
}

/// The decoded body of the first `-----BEGIN <label>-----` block, if any.
fn pem_body(text: &str, label: &str) -> Option<Result<Vec<u8>, String>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let from = text.find(&begin)? + begin.len();
    let Some(len) = text[from..].find(&end) else {
        return Some(Err(format!("no \"{end}\" line")));
    };
    let b64: String = text[from..from + len]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    Some(
        STANDARD
            .decode(b64)
            .map_err(|e| format!("bad base64 in the PEM block: {e}")),
    )
}

/// One DER element: its tag and contents, and what follows.
fn der_read(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7F);
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |l, &b| (l << 8) | usize::from(b));
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

/// The PKCS#1 key inside a PKCS#8 `PrivateKeyInfo` of an RSA key.
fn pkcs8_inner(der: &[u8]) -> Option<&[u8]> {
    let (0x30, body, _) = der_read(der)? else {
        return None;
    };
    let (0x02, _version, rest) = der_read(body)? else {
        return None;
    };
    let (0x30, alg, rest) = der_read(rest)? else {
        return None;
    };
    let (0x06, oid, _) = der_read(alg)? else {
        return None;
    };
    if oid != RSA_ENCRYPTION_OID {
        return None;
    }
    let (0x04, inner, _) = der_read(rest)? else {
        return None;
    };
    Some(inner)
}

/// `n, e, d, p, q` of a two-prime PKCS#1 `RSAPrivateKey` (version 0).
fn pkcs1_ints(der: &[u8]) -> Option<[BigUint; 5]> {
    let (0x30, mut body, _) = der_read(der)? else {
        return None;
    };
    let mut ints = Vec::with_capacity(9);
    while !body.is_empty() && ints.len() < 9 {
        let (tag, v, rest) = der_read(body)?;
        if tag != 0x02 {
            return None;
        }
        ints.push(BigUint::from_bytes_be(v));
        body = rest;
    }
    if ints.len() != 9 || ints[0] != BigUint::from(0u32) {
        return None;
    }
    let mut it = ints.into_iter().skip(1);
    Some([it.next()?, it.next()?, it.next()?, it.next()?, it.next()?])
}

/// A key pair as the auth plugin uses it: decryption runs on the blocking
/// pool.
#[derive(Debug, Clone)]
pub struct GatewayRsaKey(Arc<RsaKeyPair>);

#[async_trait]
impl ServerRsaKey for GatewayRsaKey {
    fn public_pem(&self) -> Bytes {
        self.0.public_pem()
    }

    fn size(&self) -> usize {
        self.0.size()
    }

    async fn decrypt(&self, ciphertext: Bytes) -> Option<Zeroizing<Vec<u8>>> {
        let key = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || key.decrypt(&ciphertext))
            .await
            .ok()
            .flatten()
    }
}

/// The key generated at start, for every listener of the process.
static GENERATED: OnceLock<Option<Arc<RsaKeyPair>>> = OnceLock::new();

fn generated_now() -> Option<Arc<RsaKeyPair>> {
    GENERATED
        .get_or_init(|| {
            let t = std::time::Instant::now();
            let k = RsaKeyPair::generate(GENERATED_BITS).map(Arc::new);
            if k.is_some() {
                tracing::info!(
                    bits = GENERATED_BITS,
                    ms = t.elapsed().as_millis(),
                    "generated the RSA key for caching_sha2_password logins without TLS"
                );
            } else {
                tracing::warn!(
                    "couldn't generate the RSA key for caching_sha2_password logins without TLS: \
                     the system's random source failed; those logins need TLS"
                );
            }
            k
        })
        .clone()
}

/// Starts generating the process's key in the background, so the first
/// plain-TCP login doesn't wait for it.
pub fn start_generating() {
    if GENERATED.get().is_none() {
        tokio::task::spawn_blocking(generated_now);
    }
}

/// A key file as last read.
struct FileKey {
    path: String,
    stamp: (Option<SystemTime>, u64),
    key: Option<Arc<RsaKeyPair>>,
}

static FILE: Mutex<Option<FileKey>> = Mutex::new(None);

fn stamp_of(path: &str) -> std::io::Result<(Option<SystemTime>, u64)> {
    let m = std::fs::metadata(path)?;
    Ok((m.modified().ok(), m.len()))
}

fn load_file(path: &str) -> Option<Arc<RsaKeyPair>> {
    let stamp = match stamp_of(path) {
        Ok(s) => s,
        Err(e) => {
            let mut f = FILE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stamp = (None, u64::MAX);
            if !f
                .as_ref()
                .is_some_and(|k| k.path == path && k.stamp == stamp)
            {
                tracing::warn!(
                    path,
                    error = %e,
                    "tls.caching_sha2_rsa_key: can't read the key file; caching_sha2_password \
                     logins that need the full path must use TLS"
                );
                *f = Some(FileKey {
                    path: path.to_owned(),
                    stamp,
                    key: None,
                });
            }
            return None;
        }
    };
    {
        let f = FILE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(k) = f.as_ref().filter(|k| k.path == path && k.stamp == stamp) {
            return k.key.clone();
        }
    }
    let key = match std::fs::read(path)
        .map_err(|e| e.to_string())
        .and_then(|pem| RsaKeyPair::from_pem(&Zeroizing::new(pem)))
    {
        Ok(k) => {
            tracing::info!(
                path,
                bits = k.n.bits(),
                "tls.caching_sha2_rsa_key: loaded the RSA key for caching_sha2_password logins \
                 without TLS"
            );
            Some(Arc::new(k))
        }
        Err(e) => {
            tracing::warn!(
                path,
                error = %e,
                "tls.caching_sha2_rsa_key: the key file can't be used; caching_sha2_password \
                 logins that need the full path must use TLS"
            );
            None
        }
    };
    *FILE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(FileKey {
        path: path.to_owned(),
        stamp,
        key: key.clone(),
    });
    key
}

/// The key for a login on a connection without TLS (`None` with TLS,
/// where it isn't used), under `settings`' `tls.caching_sha2_rsa_key`.
pub async fn key(
    tls: bool,
    settings: &super::settings::SettingsSource,
) -> Option<Arc<dyn ServerRsaKey>> {
    if tls {
        return None;
    }
    key_for(&settings.current().rsa_key_path).await
}

/// The key for a login without TLS, under `tls.caching_sha2_rsa_key` =
/// `path` (empty: the generated key, waited for if it isn't ready yet).
pub async fn key_for(path: &str) -> Option<Arc<dyn ServerRsaKey>> {
    let key = if path.is_empty() {
        match GENERATED.get() {
            Some(k) => k.clone(),
            None => tokio::task::spawn_blocking(generated_now)
                .await
                .ok()
                .flatten(),
        }
    } else {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || load_file(&path))
            .await
            .ok()
            .flatten()
    }?;
    Some(Arc::new(GatewayRsaKey(key)))
}

#[cfg(test)]
mod tests;
