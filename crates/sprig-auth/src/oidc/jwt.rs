//! Compact JWS tokens from external issuers: parsing and signature checks.
//!
//! Only asymmetric algorithms are accepted: `RS256`, `RS384`, `RS512`,
//! `PS256`, `PS384`, `PS512`, `ES256`, `ES384` and `EdDSA` (Ed25519).
//! `none` and the HMAC family never verify, whatever an issuer's allow-list
//! says: an issuer's public keys are no secret, so an HMAC "signature" made
//! with one proves nothing.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey};
use serde::Deserialize;
use serde_json::Value;

/// The longest token accepted, in bytes: its claims become the session's
/// `SPRIG_CLAIM()` values, which travel with every statement of a session
/// under row-level security.
pub const MAX_TOKEN_BYTES: usize = 16 * 1024;

/// A signature algorithm SprigDB verifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Alg {
    /// RSASSA-PKCS1-v1_5 with SHA-256.
    Rs256,
    /// RSASSA-PKCS1-v1_5 with SHA-384.
    Rs384,
    /// RSASSA-PKCS1-v1_5 with SHA-512.
    Rs512,
    /// RSASSA-PSS with SHA-256.
    Ps256,
    /// RSASSA-PSS with SHA-384.
    Ps384,
    /// RSASSA-PSS with SHA-512.
    Ps512,
    /// ECDSA on P-256 with SHA-256.
    Es256,
    /// ECDSA on P-384 with SHA-384.
    Es384,
    /// Ed25519.
    EdDsa,
}

impl Alg {
    /// Every supported algorithm.
    pub const ALL: [Alg; 9] = [
        Alg::Rs256,
        Alg::Rs384,
        Alg::Rs512,
        Alg::Ps256,
        Alg::Ps384,
        Alg::Ps512,
        Alg::Es256,
        Alg::Es384,
        Alg::EdDsa,
    ];

    /// The JOSE name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Alg::Rs256 => "RS256",
            Alg::Rs384 => "RS384",
            Alg::Rs512 => "RS512",
            Alg::Ps256 => "PS256",
            Alg::Ps384 => "PS384",
            Alg::Ps512 => "PS512",
            Alg::Es256 => "ES256",
            Alg::Es384 => "ES384",
            Alg::EdDsa => "EdDSA",
        }
    }

    /// The algorithm named `s` (case-sensitive, as JOSE names are); `None`
    /// for `none`, the HMAC family and anything unknown.
    #[must_use]
    pub fn parse(s: &str) -> Option<Alg> {
        Alg::ALL.into_iter().find(|a| a.name() == s)
    }

    /// The JWK key type the algorithm needs.
    #[must_use]
    pub const fn kty(self) -> &'static str {
        match self {
            Alg::Rs256 | Alg::Rs384 | Alg::Rs512 | Alg::Ps256 | Alg::Ps384 | Alg::Ps512 => "RSA",
            Alg::Es256 | Alg::Es384 => "EC",
            Alg::EdDsa => "OKP",
        }
    }
}

/// A public key from a JWKS.
#[derive(Clone, PartialEq, Eq)]
pub enum PublicKey {
    /// RSA: modulus and exponent, big-endian.
    Rsa {
        /// `n`.
        n: Vec<u8>,
        /// `e`.
        e: Vec<u8>,
    },
    /// An EC point (`0x04 || x || y`) on P-256 or P-384.
    Ec {
        /// `crv`: `P-256` or `P-384`.
        p384: bool,
        /// The uncompressed point.
        point: Vec<u8>,
    },
    /// An Ed25519 public key.
    Ed25519(Vec<u8>),
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublicKey::Rsa { n, .. } => write!(f, "Rsa({} bits)", n.len() * 8),
            PublicKey::Ec { p384, .. } => {
                write!(f, "Ec({})", if *p384 { "P-384" } else { "P-256" })
            }
            PublicKey::Ed25519(_) => f.write_str("Ed25519"),
        }
    }
}

impl PublicKey {
    /// Whether this key can check `alg`'s signatures.
    #[must_use]
    pub fn fits(&self, alg: Alg) -> bool {
        match self {
            PublicKey::Rsa { .. } => alg.kty() == "RSA",
            PublicKey::Ec { p384, .. } => {
                (alg == Alg::Es256 && !p384) || (alg == Alg::Es384 && *p384)
            }
            PublicKey::Ed25519(_) => alg == Alg::EdDsa,
        }
    }

    /// Whether `sig` is `alg`'s signature of `msg` under this key.
    #[must_use]
    pub fn verify(&self, alg: Alg, msg: &[u8], sig: &[u8]) -> bool {
        if !self.fits(alg) {
            return false;
        }
        match self {
            PublicKey::Rsa { n, e } => {
                let params: &signature::RsaParameters = match alg {
                    Alg::Rs256 => &signature::RSA_PKCS1_2048_8192_SHA256,
                    Alg::Rs384 => &signature::RSA_PKCS1_2048_8192_SHA384,
                    Alg::Rs512 => &signature::RSA_PKCS1_2048_8192_SHA512,
                    Alg::Ps256 => &signature::RSA_PSS_2048_8192_SHA256,
                    Alg::Ps384 => &signature::RSA_PSS_2048_8192_SHA384,
                    Alg::Ps512 => &signature::RSA_PSS_2048_8192_SHA512,
                    _ => return false,
                };
                RsaPublicKeyComponents { n, e }
                    .verify(params, msg, sig)
                    .is_ok()
            }
            PublicKey::Ec { p384, point } => {
                let a: &dyn signature::VerificationAlgorithm = if *p384 {
                    &signature::ECDSA_P384_SHA384_FIXED
                } else {
                    &signature::ECDSA_P256_SHA256_FIXED
                };
                UnparsedPublicKey::new(a, point).verify(msg, sig).is_ok()
            }
            PublicKey::Ed25519(k) => UnparsedPublicKey::new(&signature::ED25519, k)
                .verify(msg, sig)
                .is_ok(),
        }
    }
}

/// One key of a JWKS, as published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jwk {
    /// `kid`, if any.
    pub kid: Option<String>,
    /// `alg`, if the issuer pinned one to the key.
    pub alg: Option<String>,
    /// The key.
    pub key: PublicKey,
}

#[derive(Deserialize)]
struct RawJwk {
    kty: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default, rename = "use")]
    use_: Option<String>,
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

fn b64(s: Option<&String>) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(s?.trim_end_matches('=').as_bytes())
        .ok()
}

impl Jwk {
    fn from_raw(r: RawJwk) -> Option<Jwk> {
        if r.use_.as_deref().is_some_and(|u| u != "sig") {
            return None;
        }
        let key = match r.kty.as_str() {
            "RSA" => {
                let n = b64(r.n.as_ref())?;
                let e = b64(r.e.as_ref())?;
                (n.len() >= 256 && !e.is_empty()).then_some(PublicKey::Rsa { n, e })?
            }
            "EC" => {
                let (p384, len) = match r.crv.as_deref()? {
                    "P-256" => (false, 32),
                    "P-384" => (true, 48),
                    _ => return None,
                };
                let x = b64(r.x.as_ref())?;
                let y = b64(r.y.as_ref())?;
                if x.len() != len || y.len() != len {
                    return None;
                }
                let mut point = Vec::with_capacity(1 + 2 * len);
                point.push(4);
                point.extend_from_slice(&x);
                point.extend_from_slice(&y);
                PublicKey::Ec { p384, point }
            }
            "OKP" if r.crv.as_deref() == Some("Ed25519") => {
                let x = b64(r.x.as_ref())?;
                (x.len() == 32).then_some(PublicKey::Ed25519(x))?
            }
            _ => return None,
        };
        Some(Jwk {
            kid: r.kid,
            alg: r.alg,
            key,
        })
    }
}

/// The signing keys of a JWKS document (`{"keys": [...]}`). Keys of
/// unsupported types, encryption keys (`use` other than `sig`) and keys
/// that don't parse are left out.
///
/// # Errors
/// A message when the document isn't a JWKS.
pub fn parse_jwks(doc: &[u8]) -> Result<Vec<Jwk>, String> {
    #[derive(Deserialize)]
    struct Doc {
        keys: Vec<Value>,
    }
    let d: Doc = serde_json::from_slice(doc).map_err(|e| format!("not a JWKS: {e}"))?;
    Ok(d.keys
        .into_iter()
        .filter_map(|k| serde_json::from_value::<RawJwk>(k).ok())
        .filter_map(Jwk::from_raw)
        .collect())
}

/// A parsed, unverified token.
#[derive(Debug, Clone)]
pub struct Jws {
    /// The header's `alg` as sent.
    pub alg: String,
    /// The header's `kid`, if any.
    pub kid: Option<String>,
    /// The claims.
    pub claims: Value,
    /// `header.payload`, the signed bytes.
    pub signed: Vec<u8>,
    /// The signature.
    pub signature: Vec<u8>,
}

impl Jws {
    /// Whether `s` has a JWT's shape: three base64url parts, the first a
    /// JSON object (`eyJ`). A cheap test before treating a password as a
    /// token.
    #[must_use]
    pub fn looks_like(s: &[u8]) -> bool {
        s.starts_with(b"eyJ")
            && s.len() <= MAX_TOKEN_BYTES
            && s.split(|&b| b == b'.').count() == 3
            && s.iter()
                .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    }

    /// Parses `token`. The claims must be a JSON object.
    ///
    /// # Errors
    /// A message for a token that isn't a compact JWS, is too long, has a
    /// `crit` header (no extension is understood) or claims that aren't an
    /// object.
    pub fn parse(token: &str) -> Result<Jws, String> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(format!("longer than {MAX_TOKEN_BYTES} bytes"));
        }
        let mut parts = token.split('.');
        let (Some(h), Some(p), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("not a compact JWS".into());
        };
        let dec = |x: &str| {
            URL_SAFE_NO_PAD
                .decode(x)
                .map_err(|_| "bad base64url".to_owned())
        };
        let header: Value =
            serde_json::from_slice(&dec(h)?).map_err(|_| "header isn't JSON".to_owned())?;
        let claims: Value =
            serde_json::from_slice(&dec(p)?).map_err(|_| "claims aren't JSON".to_owned())?;
        if !claims.is_object() {
            return Err("claims aren't an object".into());
        }
        if header.get("crit").is_some() {
            return Err("crit header".into());
        }
        let alg = header
            .get("alg")
            .and_then(Value::as_str)
            .ok_or_else(|| "no alg".to_owned())?
            .to_owned();
        let kid = header.get("kid").and_then(Value::as_str).map(str::to_owned);
        Ok(Jws {
            alg,
            kid,
            claims,
            signed: token.as_bytes()[..h.len() + 1 + p.len()].to_vec(),
            signature: dec(s)?,
        })
    }

    /// A string claim.
    #[must_use]
    pub fn str_claim(&self, k: &str) -> Option<&str> {
        self.claims.get(k).and_then(Value::as_str)
    }

    /// A `NumericDate` claim (seconds; fractions dropped).
    #[must_use]
    pub fn time_claim(&self, k: &str) -> Option<i64> {
        let v = self.claims.get(k)?;
        #[allow(clippy::cast_possible_truncation)]
        v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))
    }

    /// The audiences: `aud` as a string or an array of strings.
    #[must_use]
    pub fn audiences(&self) -> Vec<&str> {
        match self.claims.get("aud") {
            Some(Value::String(s)) => vec![s.as_str()],
            Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        }
    }
}
