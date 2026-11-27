//! Small secrets sealed under a 256-bit key with AES-256-GCM: the
//! internal CA's private keys, sealed under the installation's CA key.
//!
//! A sealed value is `nonce (12) | ciphertext and tag`; the caller's
//! associated data binds it to what it is (a CA's name), so a sealed key
//! can't be moved to another record.

use std::path::Path;

use ring::aead;
use sprig_common::{Result, SprigCode, SprigError};
use zeroize::Zeroizing;

/// A 256-bit sealing key, wiped when dropped.
pub struct SealKey(Zeroizing<[u8; 32]>);

impl std::fmt::Debug for SealKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SealKey(..)")
    }
}

impl SealKey {
    /// The key `b`.
    #[must_use]
    pub fn new(b: [u8; 32]) -> Self {
        Self(Zeroizing::new(b))
    }

    /// The key in `path`: 64 hex digits (whitespace around them is
    /// ignored), as `openssl rand -hex 32` writes them.
    ///
    /// # Errors
    /// 39998 for a missing, everyone-writable or malformed file.
    pub fn from_file(path: &Path, what: &str) -> Result<Self> {
        sprig_common::secret_file::check(path, what)?;
        let text = Zeroizing::new(std::fs::read_to_string(path).map_err(|e| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("{what} {}: {e}", path.display()),
            )
        })?);
        let t = text.trim();
        let bad = || {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!(
                    "{what} {} must hold 64 hex digits (a 256-bit key, such as `openssl rand -hex 32` writes)",
                    path.display()
                ),
            )
        };
        if t.len() != 64 {
            return Err(bad());
        }
        let mut k = Zeroizing::new([0u8; 32]);
        for (i, b) in k.iter_mut().enumerate() {
            *b = t
                .get(2 * i..2 * i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(bad)?;
        }
        Ok(Self(k))
    }

    /// [`seal`] under this key.
    ///
    /// # Errors
    /// As [`seal`].
    pub fn seal(&self, aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
        seal(&self.0, aad, plain)
    }

    /// [`open`] under this key.
    ///
    /// # Errors
    /// As [`open`].
    pub fn open(&self, aad: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        open(&self.0, aad, sealed)
    }
}

/// `N` bytes from the system's random source.
///
/// # Errors
/// `Internal` if the source fails.
pub fn random<const N: usize>() -> Result<[u8; N]> {
    let mut b = [0u8; N];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b)
        .map_err(|_| SprigError::internal("the system's random source failed"))?;
    Ok(b)
}

/// Seals `plain` under `key`, bound to `aad`.
///
/// # Errors
/// `Internal` if the system's random source or the cipher fails.
pub fn seal(key: &[u8; 32], aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, key)
            .map_err(|_| SprigError::internal("sealing key"))?,
    );
    let nonce = random::<12>()?;
    let mut buf = plain.to_vec();
    k.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(aad),
        &mut buf,
    )
    .map_err(|_| SprigError::internal("sealing a secret"))?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buf);
    Ok(out)
}

/// The secret in `sealed`, sealed by [`seal`] under `key` with `aad`.
///
/// # Errors
/// `Internal` for a damaged seal, another key or other associated data.
pub fn open(key: &[u8; 32], aad: &[u8], sealed: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if sealed.len() < 12 + 16 {
        return Err(SprigError::internal("a sealed secret is too short"));
    }
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, key)
            .map_err(|_| SprigError::internal("sealing key"))?,
    );
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&sealed[..12]);
    let mut buf = Zeroizing::new(sealed[12..].to_vec());
    let n = k
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            &mut buf,
        )
        .map_err(|_| SprigError::internal("a sealed secret doesn't open with this key"))?
        .len();
    buf.truncate(n);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn seals_and_opens_only_with_its_key_and_name() {
        let key = [7u8; 32];
        let s = seal(&key, b"root", b"secret key").unwrap();
        assert_eq!(open(&key, b"root", &s).unwrap().as_slice(), b"secret key");
        assert!(open(&[8u8; 32], b"root", &s).is_err(), "another key");
        assert!(open(&key, b"issuing", &s).is_err(), "another name");
        let mut bad = s.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(open(&key, b"root", &bad).is_err(), "damaged");
        assert_ne!(
            seal(&key, b"root", b"secret key").unwrap(),
            s,
            "fresh nonce"
        );
    }
}
