//! Encryption of everything a backup writes, and the names of its chunks.
//!
//! [`KeyProvider`] hands out one data key per tenant. [`KeyFile`] is the
//! provider for now: a per-installation 32-byte key in a file (created
//! with mode 0600 on first use), from which each tenant's key is derived
//! with HKDF-SHA256. A key service with per-tenant keys wrapped by a data
//! centre key can replace it behind the same trait.
//!
//! A sealed object is `magic "SPRBKE1\0" | key id (8) | nonce (12) |
//! AES-256-GCM ciphertext and tag`, with the object's name as the
//! additional data, so an object moved to another name doesn't open.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::hkdf;
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use sprig_common::{Result, SprigError};

const MAGIC: &[u8; 8] = b"SPRBKE1\0";
const HEAD: usize = 8 + 8 + 12;

/// One tenant's keys: one for sealing, one for chunk names.
pub struct DataKey {
    seal: LessSafeKey,
    name: hmac::Key,
    id: [u8; 8],
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataKey")
            .field("id", &hex(&self.id))
            .finish_non_exhaustive()
    }
}

/// Where backups get their keys.
pub trait KeyProvider: Send + Sync {
    /// The keys of `tenant`.
    ///
    /// # Errors
    /// The key can't be read.
    fn key_for(&self, tenant: u64) -> Result<DataKey>;
}

/// A per-installation key in a file.
pub struct KeyFile {
    master: [u8; 32],
    path: PathBuf,
}

impl std::fmt::Debug for KeyFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyFile")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn err(m: impl Into<String>) -> SprigError {
    SprigError::internal(format!("backup key: {}", m.into()))
}

pub(crate) fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

impl KeyFile {
    /// Reads the key at `path`, or creates it (64 hex characters, mode
    /// 0600) when the file doesn't exist.
    ///
    /// # Errors
    /// The file can't be read or written, or doesn't hold a key.
    pub fn open_or_create(path: impl Into<PathBuf>) -> Result<KeyFile> {
        let path = path.into();
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let b = unhex(&s).filter(|b| b.len() == 32).ok_or_else(|| {
                    err(format!("{} doesn't hold a 32-byte hex key", path.display()))
                })?;
                let mut master = [0u8; 32];
                master.copy_from_slice(&b);
                Ok(KeyFile { master, path })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut master = [0u8; 32];
                SystemRandom::new()
                    .fill(&mut master)
                    .map_err(|_| err("no randomness"))?;
                write_new_key(&path, &master)?;
                tracing::info!(path = %path.display(), "created the backup key file");
                Ok(KeyFile { master, path })
            }
            Err(e) => Err(err(format!("read {}: {e}", path.display()))),
        }
    }

    /// The file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn write_new_key(path: &Path, master: &[u8; 32]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d).map_err(|e| err(format!("create {}: {e}", d.display())))?;
    }
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|e| err(format!("create {}: {e}", tmp.display())))?;
    f.write_all(hex(master).as_bytes())
        .and_then(|()| f.sync_all())
        .map_err(|e| err(format!("write {}: {e}", tmp.display())))?;
    // A key another process created meanwhile wins: never replace one.
    std::fs::hard_link(&tmp, path)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
        .map_err(|e| err(format!("create {}: {e}", path.display())))?;
    let _ = std::fs::remove_file(&tmp);
    Ok(())
}

struct Len(usize);

impl hkdf::KeyType for Len {
    fn len(&self) -> usize {
        self.0
    }
}

impl KeyProvider for KeyFile {
    fn key_for(&self, tenant: u64) -> Result<DataKey> {
        derive(&self.master, tenant)
    }
}

/// Derives a tenant's keys from a master key.
///
/// # Errors
/// Never in practice (the lengths are fixed).
pub fn derive(master: &[u8; 32], tenant: u64) -> Result<DataKey> {
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, b"sprigdb backup v1").extract(master);
    let t = tenant.to_be_bytes();
    let mut seal = [0u8; 32];
    let mut name = [0u8; 32];
    prk.expand(&[b"seal", &t], Len(32))
        .and_then(|o| o.fill(&mut seal))
        .map_err(|_| err("derive"))?;
    prk.expand(&[b"name", &t], Len(32))
        .and_then(|o| o.fill(&mut name))
        .map_err(|_| err("derive"))?;
    let digest = ring::digest::digest(&ring::digest::SHA256, &seal);
    let mut id = [0u8; 8];
    id.copy_from_slice(&digest.as_ref()[..8]);
    Ok(DataKey {
        seal: LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &seal).map_err(|_| err("key"))?),
        name: hmac::Key::new(hmac::HMAC_SHA256, &name),
        id,
    })
}

impl DataKey {
    /// Seals `plain` as the object `name`.
    ///
    /// # Errors
    /// No randomness for the nonce.
    pub fn seal(&self, name: &str, plain: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| err("no randomness"))?;
        let mut out = Vec::with_capacity(HEAD + plain.len() + 16);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&nonce);
        let mut body = plain.to_vec();
        self.seal
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(name.as_bytes()),
                &mut body,
            )
            .map_err(|_| err("seal"))?;
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Opens what [`DataKey::seal`] wrote for `name`.
    ///
    /// # Errors
    /// Not a sealed object, another key, another name, or changed bytes.
    pub fn open(&self, name: &str, sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < HEAD + 16 || &sealed[..8] != MAGIC {
            return Err(err(format!("{name} is not a sealed backup object")));
        }
        if sealed[8..16] != self.id {
            return Err(err(format!(
                "{name} was sealed with another key (key id {}, this installation's {})",
                hex(&sealed[8..16]),
                hex(&self.id)
            )));
        }
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&sealed[16..HEAD]);
        let mut body = sealed[HEAD..].to_vec();
        let n = self
            .seal
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(name.as_bytes()),
                &mut body,
            )
            .map_err(|_| err(format!("{name} doesn't open: changed or damaged")))?
            .len();
        body.truncate(n);
        Ok(body)
    }

    /// The content name of a chunk: a keyed hash, so equal chunks get one
    /// name and names reveal nothing about the content.
    #[must_use]
    pub fn chunk_name(&self, content: &[u8]) -> String {
        hex(&hmac::sign(&self.name, content).as_ref()[..16])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_and_tamper() {
        let k = derive(&[7u8; 32], 5).unwrap();
        let s = k.seal("a/b", b"hello").unwrap();
        assert_eq!(k.open("a/b", &s).unwrap(), b"hello");
        assert!(k.open("a/c", &s).is_err(), "another name");
        let mut t = s.clone();
        let last = t.len() - 1;
        t[last] ^= 1;
        assert!(k.open("a/b", &t).is_err(), "changed bytes");
        let other = derive(&[7u8; 32], 6).unwrap();
        assert!(other.open("a/b", &s).is_err(), "another tenant's key");
        assert_eq!(k.chunk_name(b"x"), k.chunk_name(b"x"));
        assert_ne!(k.chunk_name(b"x"), other.chunk_name(b"x"));
    }

    #[test]
    fn key_file_is_created_once() {
        use std::os::unix::fs::PermissionsExt as _;
        let d = std::env::temp_dir().join(format!("sprig-bk-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let p = d.join("backup.key");
        let a = KeyFile::open_or_create(&p).unwrap();
        let b = KeyFile::open_or_create(&p).unwrap();
        assert_eq!(a.master, b.master);
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&d);
    }
}
