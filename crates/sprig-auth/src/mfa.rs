//! Second factors: TOTP with
//! recovery codes, kept in the tenant catalog's `user_factors` (catalog
//! records, `m/auth/user_factors/<user>/<factor>`), and the account
//! lookups the second-factor challenge and impersonation need.
//!
//! ```text
//! enrol(user):    a 20-byte seed, sealed (AES-256-GCM under the tenant's
//!                 factor key), stored `pending`; shown once as otpauth://
//! confirm(code):  the code is right → the factor is `active` and 10
//!                 recovery codes (SHA-256 only) replace any earlier ones,
//!                 in one auth change
//! challenge:      TOTP code: RFC 6238, HMAC-SHA-1, 6 digits, 30 s, one
//!                 step either way, the step must be newer than the last
//!                 accepted one (compare-and-set: a code works once);
//!                 recovery code: its row is deleted (works once)
//! reset(user):    every factor deleted and every session revoked, in one
//!                 auth change (an administrator's reset, or the user's own
//!                 "remove")
//! ```
//!
//! Seeds are sealed with a per-tenant key the caller supplies
//! ([`FactorKeys`]): the gateway derives it from its tenant-data-key file
//! (the tenant data keys), so no seed is ever
//! stored in the clear and a catalog copy alone can't produce codes.

use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::aead;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sprig_catalog::records::{self, Pk, RecordTx};
use sprig_catalog::{AuthCatalog, ChangeOpts, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::Persona;
use subtle::ConstantTimeEq as _;

use crate::cache::TenantAuth;
use crate::mysql::{CatalogLogins, Principal, locked, temp_locked};
use crate::rec::{USER_FACTORS, to_micros};
use crate::store::RequireTls;

/// TOTP step.
pub const STEP: u64 = 30;
/// Seed bytes.
pub const SEED_BYTES: usize = 20;
/// Recovery codes per enrolment.
pub const RECOVERY_CODES: usize = 10;
/// The purpose the factor key is derived for.
pub const KEY_PURPOSE: &[u8] = b"sprig-auth-totp-seed";

// ---- TOTP (RFC 6238 over RFC 4226) ----

/// The 6-digit code of `seed` at `step`.
#[must_use]
pub fn code_at(seed: &[u8], step: u64) -> u32 {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, seed);
    let tag = ring::hmac::sign(&key, &step.to_be_bytes());
    let h = tag.as_ref();
    let off = usize::from(h[h.len() - 1] & 0x0f);
    let bin = (u32::from(h[off]) & 0x7f) << 24
        | u32::from(h[off + 1]) << 16
        | u32::from(h[off + 2]) << 8
        | u32::from(h[off + 3]);
    bin % 1_000_000
}

/// The step of `t`.
#[must_use]
pub fn step_of(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) / STEP
}

/// The step `code` matches at `now` (one step of drift either way) and
/// that is newer than `last`; `None` otherwise.
#[must_use]
pub fn check(seed: &[u8], code: &str, now: SystemTime, last: Option<u64>) -> Option<u64> {
    let code = code.trim().replace(' ', "");
    if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let want: u32 = code.parse().ok()?;
    let s = step_of(now);
    [s.saturating_sub(1), s, s + 1]
        .into_iter()
        .find(|&st| last.is_none_or(|l| st > l) && bool::from(code_at(seed, st).ct_eq(&want)))
}

/// RFC 4648 base32 without padding (authenticator apps' secret form).
#[must_use]
pub fn base32(b: &[u8]) -> String {
    const A: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::new();
    let (mut buf, mut bits) = (0u32, 0u32);
    for &x in b {
        buf = (buf << 8) | u32::from(x);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(char::from(A[((buf >> bits) & 31) as usize]));
        }
    }
    if bits > 0 {
        out.push(char::from(A[((buf << (5 - bits)) & 31) as usize]));
    }
    out
}

fn uri_escape(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// `otpauth://totp/<issuer>:<account>?secret=...&issuer=...`.
#[must_use]
pub fn otpauth_uri(issuer: &str, account: &str, seed: &[u8]) -> String {
    format!(
        "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm=SHA1&digits=6&period={STEP}",
        uri_escape(issuer),
        uri_escape(account),
        base32(seed),
        uri_escape(issuer)
    )
}

fn random<const N: usize>() -> Result<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b)
        .map_err(|e| SprigError::internal(format!("no randomness for a factor: {e}")))?;
    Ok(b)
}

/// Ten recovery codes, `xxxxx-xxxxx` in base32 (50 bits each).
///
/// # Errors
/// `Internal` without randomness.
pub fn recovery_codes() -> Result<Vec<String>> {
    (0..RECOVERY_CODES)
        .map(|_| {
            let b = base32(&random::<7>()?).to_lowercase();
            Ok(format!("{}-{}", &b[..5], &b[5..10]))
        })
        .collect()
}

/// The stored form of a recovery code: SHA-256 of its normalized text.
#[must_use]
pub fn recovery_hash(code: &str) -> [u8; 32] {
    let n: String = code
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    Sha256::digest(n.as_bytes()).into()
}

// ---- sealing ----

/// The per-tenant keys that seal seeds.
pub trait FactorKeys: Send + Sync {
    /// The current key version and key for `tenant`.
    fn current(&self, tenant: TenantId) -> Option<(u32, [u8; 32])>;
    /// The key `version` for `tenant`.
    fn key(&self, tenant: TenantId, version: u32) -> Option<[u8; 32]>;
}

fn aad(tenant: TenantId, user: UserId) -> Vec<u8> {
    let mut a = b"sprig-totp:".to_vec();
    a.extend_from_slice(&tenant.0.to_be_bytes());
    a.extend_from_slice(&user.0.to_be_bytes());
    a
}

fn no_key() -> SprigError {
    SprigError::new(
        SprigCode::AuthUnavailable,
        "second factors need a factor key: set [gateway] workload_key_file (tenant data keys)",
    )
}

/// `version (4) | nonce (12) | ciphertext and tag`.
///
/// # Errors
/// 39301 without a key; `Internal`.
pub fn seal(keys: &dyn FactorKeys, tenant: TenantId, user: UserId, seed: &[u8]) -> Result<Vec<u8>> {
    let (version, key) = keys.current(tenant).ok_or_else(no_key)?;
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .map_err(|_| SprigError::internal("factor key"))?,
    );
    let nonce = random::<12>()?;
    let mut buf = seed.to_vec();
    k.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(aad(tenant, user)),
        &mut buf,
    )
    .map_err(|_| SprigError::internal("sealing a seed"))?;
    let mut out = version.to_be_bytes().to_vec();
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&buf);
    Ok(out)
}

/// The seed in `sealed`.
///
/// # Errors
/// 39301 without the key; `Internal` for a damaged or foreign seal.
pub fn open(
    keys: &dyn FactorKeys,
    tenant: TenantId,
    user: UserId,
    sealed: &[u8],
) -> Result<Vec<u8>> {
    if sealed.len() < 4 + 12 + 16 {
        return Err(SprigError::internal("a sealed seed is too short"));
    }
    let version = u32::from_be_bytes([sealed[0], sealed[1], sealed[2], sealed[3]]);
    let key = keys.key(tenant, version).ok_or_else(no_key)?;
    let k = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, &key)
            .map_err(|_| SprigError::internal("factor key"))?,
    );
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&sealed[4..16]);
    let mut buf = sealed[16..].to_vec();
    let plain = k
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad(tenant, user)),
            &mut buf,
        )
        .map_err(|_| SprigError::internal("a sealed seed doesn't open with its key"))?;
    Ok(plain.to_vec())
}

// ---- storage ----

/// A factor row (no secret).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Factor {
    /// The id.
    pub id: u64,
    /// `totp`, `webauthn`, `recovery`.
    pub kind: String,
    /// `pending` or `active`.
    pub state: String,
    /// When it was made, Unix milliseconds.
    pub created_ms: u64,
}

/// `user_factors.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum FactorKind {
    Totp,
    Webauthn,
    Recovery,
}

impl FactorKind {
    const fn as_str(self) -> &'static str {
        match self {
            FactorKind::Totp => "totp",
            FactorKind::Webauthn => "webauthn",
            FactorKind::Recovery => "recovery",
        }
    }
}

/// `user_factors.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum FactorState {
    Pending,
    Active,
}

impl FactorState {
    const fn as_str(self) -> &'static str {
        match self {
            FactorState::Pending => "pending",
            FactorState::Active => "active",
        }
    }
}

/// A `user_factors` row (no `Debug`: it holds the sealed seed).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FactorRow {
    factor_id: u64,
    user_id: UserId,
    kind: FactorKind,
    state: FactorState,
    secret_enc: Option<Vec<u8>>,
    last_step: Option<u64>,
    credential_id: Option<Vec<u8>>,
    public_key: Option<Vec<u8>>,
    sign_count: Option<u64>,
    code_sha256: Option<[u8; 32]>,
    #[serde(with = "crate::rec::micros")]
    created_at: SystemTime,
    /// 0 while pending; the auth version of the change that wrote it.
    changed_version: u64,
}

/// The id counter of factors.
const FACTOR_COUNTER: &str = "user_factors";

fn factor_pk(user: UserId, id: u64) -> Pk {
    Pk::new().u32(user.0).u64(id)
}

/// The user's factors, by id.
async fn factors_of(tx: &mut dyn RecordTx, user: UserId) -> Result<Vec<FactorRow>> {
    USER_FACTORS.rows(tx, &Pk::new().u32(user.0)).await
}

impl FactorRow {
    /// A new factor of `user` (its id comes with the insert).
    fn new(
        user: UserId,
        kind: FactorKind,
        state: FactorState,
        now: SystemTime,
        version: u64,
    ) -> Self {
        Self {
            factor_id: 0,
            user_id: user,
            kind,
            state,
            secret_enc: None,
            last_step: None,
            credential_id: None,
            public_key: None,
            sign_count: None,
            code_sha256: None,
            created_at: to_micros(now),
            changed_version: version,
        }
    }
}

/// Inserts `row` with a new id (counter `user_factors`); returns the id.
async fn insert_factor(tx: &mut dyn RecordTx, mut row: FactorRow) -> Result<u64> {
    let id = records::next_ids(tx, FACTOR_COUNTER, 1).await?;
    row.factor_id = id;
    USER_FACTORS
        .put(tx, &factor_pk(row.user_id, id), &row)
        .await?;
    Ok(id)
}

/// `user_factors` in the tenant catalogs.
#[derive(Clone)]
pub struct FactorStore {
    cat: AuthCatalog,
}

impl std::fmt::Debug for FactorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FactorStore").finish_non_exhaustive()
    }
}

impl FactorStore {
    /// Factors written through `cat`.
    #[must_use]
    pub fn new(cat: AuthCatalog) -> Self {
        Self { cat }
    }

    /// A plain (unversioned) record transaction on `tenant`'s records.
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        self.cat.backend().store().begin(tenant).await
    }

    /// The user's factor rows as committed, by id.
    async fn rows(&self, tenant: TenantId, user: UserId) -> Result<Vec<FactorRow>> {
        let mut tx = self.begin(tenant).await?;
        let rows = factors_of(&mut *tx, user).await;
        let _ = tx.rollback().await;
        rows
    }

    /// The user's factors, newest first.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn list(&self, tenant: TenantId, user: UserId) -> Result<Vec<Factor>> {
        Ok(self
            .rows(tenant, user)
            .await?
            .iter()
            .rev()
            .map(|r| Factor {
                id: r.factor_id,
                kind: r.kind.as_str().to_owned(),
                state: r.state.as_str().to_owned(),
                created_ms: u64::try_from(crate::store::micros(r.created_at)).unwrap_or(0) / 1000,
            })
            .collect())
    }

    /// Whether the user has an active TOTP factor.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn has_active(&self, tenant: TenantId, user: UserId) -> Result<bool> {
        Ok(self
            .list(tenant, user)
            .await?
            .iter()
            .any(|f| f.kind == "totp" && f.state == "active"))
    }

    /// The id, sealed seed and last step of the user's newest active (or,
    /// with `pending`, pending) TOTP factor.
    async fn totp(
        &self,
        tenant: TenantId,
        user: UserId,
        pending: bool,
    ) -> Result<Option<(u64, Vec<u8>, Option<u64>)>> {
        let want = if pending {
            FactorState::Pending
        } else {
            FactorState::Active
        };
        Ok(self
            .rows(tenant, user)
            .await?
            .into_iter()
            .rev()
            .find(|r| r.kind == FactorKind::Totp && r.state == want)
            .map(|r| (r.factor_id, r.secret_enc.unwrap_or_default(), r.last_step)))
    }

    /// Starts an enrolment: a new seed, stored sealed and `pending` (an
    /// earlier pending one is replaced; not an auth change). Returns the
    /// seed, to show once.
    ///
    /// # Errors
    /// 39301 without a factor key; the catalog's.
    pub async fn enrol(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        now: SystemTime,
    ) -> Result<Vec<u8>> {
        let seed = random::<SEED_BYTES>()?.to_vec();
        let sealed = seal(keys, tenant, user, &seed)?;
        let mut tx = self.begin(tenant).await?;
        for r in factors_of(&mut *tx, user).await? {
            if r.kind == FactorKind::Totp && r.state == FactorState::Pending {
                USER_FACTORS
                    .delete(&mut *tx, &factor_pk(user, r.factor_id))
                    .await?;
            }
        }
        let mut row = FactorRow::new(user, FactorKind::Totp, FactorState::Pending, now, 0);
        row.secret_enc = Some(sealed);
        insert_factor(&mut *tx, row).await?;
        tx.commit().await?;
        Ok(seed)
    }

    /// Completes an enrolment with a code from the pending seed: the
    /// factor becomes active (replacing an earlier active one) and new
    /// recovery codes replace the old, in one auth change. Returns the
    /// recovery codes, to show once; `None` for a wrong code.
    ///
    /// # Errors
    /// 39301; the catalog's.
    pub async fn confirm(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        submitted: &str,
        now: SystemTime,
    ) -> Result<Option<Vec<String>>> {
        let Some((id, sealed, _)) = self.totp(tenant, user, true).await? else {
            return Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                "no enrolment is pending: start one first",
            ));
        };
        let seed = open(keys, tenant, user, &sealed)?;
        let Some(step) = check(&seed, submitted, now, None) else {
            return Ok(None);
        };
        let codes = recovery_codes()?;
        let mut ch = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = ch.version();
        let r = async {
            let tx = ch.tx().records();
            for r in factors_of(tx, user).await? {
                let other_totp = r.kind == FactorKind::Totp && r.factor_id != id;
                if r.kind == FactorKind::Recovery || other_totp {
                    USER_FACTORS
                        .delete(tx, &factor_pk(user, r.factor_id))
                        .await?;
                }
            }
            // The pending factor, if it is still there (a newer enrolment
            // may have replaced it meanwhile: then nothing is activated).
            let pk = factor_pk(user, id);
            if let Some(mut f) = USER_FACTORS.get_for_update::<FactorRow>(tx, &pk).await? {
                f.state = FactorState::Active;
                f.last_step = Some(step);
                f.changed_version = version;
                USER_FACTORS.put(tx, &pk, &f).await?;
            }
            for code in &codes {
                let mut row = FactorRow::new(
                    user,
                    FactorKind::Recovery,
                    FactorState::Active,
                    now,
                    version,
                );
                row.code_sha256 = Some(recovery_hash(code));
                insert_factor(tx, row).await?;
            }
            Ok::<_, SprigError>(())
        }
        .await;
        match r {
            Ok(()) => {
                ch.commit().await?;
                Ok(Some(codes))
            }
            Err(e) => {
                let _ = ch.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }

    /// A second factor at login: a TOTP code (accepted once: the step is
    /// stored with a compare-and-set) or a recovery code (its row is
    /// deleted). `Some("otp" | "rec")` when it passes. Not an auth change.
    ///
    /// # Errors
    /// 39301; the catalog's.
    pub async fn verify(
        &self,
        keys: &dyn FactorKeys,
        tenant: TenantId,
        user: UserId,
        code: &str,
        now: SystemTime,
    ) -> Result<Option<&'static str>> {
        let digits = code.trim().replace(' ', "");
        if digits.len() == 6 && digits.bytes().all(|b| b.is_ascii_digit()) {
            let Some((id, sealed, last)) = self.totp(tenant, user, false).await? else {
                return Ok(None);
            };
            let seed = open(keys, tenant, user, &sealed)?;
            let Some(step) = check(&seed, &digits, now, last) else {
                return Ok(None);
            };
            // Compare-and-set: the step moves only forward, so a code
            // works once however many logins race with it.
            let mut tx = self.begin(tenant).await?;
            let pk = factor_pk(user, id);
            let found = USER_FACTORS
                .get_for_update::<FactorRow>(&mut *tx, &pk)
                .await?;
            return match found {
                Some(mut f) if f.last_step.is_none_or(|l| l < step) => {
                    f.last_step = Some(step);
                    USER_FACTORS.put(&mut *tx, &pk, &f).await?;
                    tx.commit().await?;
                    Ok(Some("otp"))
                }
                _ => {
                    let _ = tx.rollback().await;
                    Ok(None)
                }
            };
        }
        let want = recovery_hash(code);
        let mut tx = self.begin(tenant).await?;
        let found = factors_of(&mut *tx, user).await?.into_iter().find(|r| {
            r.kind == FactorKind::Recovery
                && r.code_sha256.is_some_and(|h| bool::from(h.ct_eq(&want)))
        });
        let Some(f) = found else {
            let _ = tx.rollback().await;
            return Ok(None);
        };
        // Single use: only the transaction that still finds the row (its
        // latest value, locked) deletes it.
        let pk = factor_pk(user, f.factor_id);
        if USER_FACTORS
            .get_for_update::<FactorRow>(&mut *tx, &pk)
            .await?
            .is_none()
        {
            let _ = tx.rollback().await;
            return Ok(None);
        }
        USER_FACTORS.delete(&mut *tx, &pk).await?;
        tx.commit().await?;
        Ok(Some("rec"))
    }

    /// Recovery codes left.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn recovery_left(&self, tenant: TenantId, user: UserId) -> Result<usize> {
        Ok(self
            .list(tenant, user)
            .await?
            .iter()
            .filter(|f| f.kind == "recovery")
            .count())
    }

    /// Deletes every factor of the user and revokes the user's sessions,
    /// in one auth change; the number of factors deleted.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn reset(&self, tenant: TenantId, user: UserId, now: SystemTime) -> Result<u64> {
        let mut ch = self
            .cat
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let version = ch.version();
        let r = async {
            let tx = ch.tx().records();
            let mut n = 0;
            for r in factors_of(tx, user).await? {
                USER_FACTORS
                    .delete(tx, &factor_pk(user, r.factor_id))
                    .await?;
                n += 1;
            }
            crate::token::sessions::revoke_user_sessions(tx, user, version, now).await?;
            Ok::<_, SprigError>(n)
        }
        .await;
        match r {
            Ok(n) => {
                ch.commit().await?;
                Ok(n)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                let _ = self.cat.settle(tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }
}

// ---- accounts without a password (a completed challenge, impersonation) ----

fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

/// The principal of `tenant`'s user `user` (by id, or by name and host),
/// with the checks a login makes after the password: the tenant can log
/// in and isn't suspended, the account exists and isn't locked, TLS.
///
/// # Errors
/// 1045, 3118, 3955, 39303; 39301.
pub async fn account_principal(
    cl: &CatalogLogins,
    tenant: &str,
    user: AccountRef<'_>,
    client_ip: IpAddr,
    tls: bool,
    now: SystemTime,
) -> Result<Principal> {
    let t = match cl.tenants().lookup(tenant).await {
        Ok(Some(t)) if t.can_log_in => t,
        Ok(_) => return Err(denied("no such tenant")),
        Err(e) => {
            tracing::warn!(error = %e, "mfa: tenant directory unreadable");
            return Err(SprigError::new(
                SprigCode::AuthUnavailable,
                "authentication unavailable; retry",
            ));
        }
    };
    let catalog = cl.catalogs().catalog(t.id).await.map_err(|e| {
        tracing::warn!(error = %e, "mfa: catalog unreadable");
        SprigError::new(
            SprigCode::AuthUnavailable,
            "authentication unavailable; retry",
        )
    })?;
    let Some(auth) = TenantAuth::of(&catalog) else {
        return Err(denied("the tenant has no accounts"));
    };
    let u = match user {
        AccountRef::Id(id) => auth.user(id).cloned(),
        AccountRef::Name(name, host) => auth
            .accounts(name)
            .iter()
            .find(|a| host.is_none_or(|h| a.row.host == h))
            .cloned(),
    }
    .ok_or_else(|| denied("no such account"))?;
    let (name, host) = (u.row.name.as_str(), u.row.host.as_str());
    if let Some(lock) = auth
        .temp_lock(u.row.user_id)
        .filter(|l| l.locked_until > now)
    {
        let p = temp_locked(Persona::MySql84, name, host, &lock, now);
        return Err(SprigError::standard(p.code, p.sqlstate, p.message));
    }
    if t.suspended {
        return Err(SprigError::new(
            SprigCode::TenantSuspended,
            format!("tenant {} is suspended", t.name),
        ));
    }
    if u.row.account_locked {
        let p = locked(Persona::MySql84, name, host);
        return Err(SprigError::standard(p.code, p.sqlstate, p.message));
    }
    let need_tls = u.row.require_tls != RequireTls::None || cl.require_tls(&catalog);
    if (need_tls && !tls) || u.row.require_tls == RequireTls::X509 {
        return Err(denied("the account requires TLS"));
    }
    let backend = cl.backend_of(&t, &u)?;
    Ok(Principal {
        tenant: Some(t.id),
        tenant_name: Some(t.name.clone()),
        user_id: Some(u.row.user_id),
        user: u.row.name.clone(),
        host: u.row.host.clone(),
        client_ip,
        must_change_password: false,
        limits: u.row.limits,
        auth_version: catalog.auth_version,
        backend,
    })
}

/// Which account [`account_principal`] looks up.
#[derive(Debug, Clone, Copy)]
pub enum AccountRef<'a> {
    /// By id.
    Id(UserId),
    /// By name, and host when given (else the first account of the name).
    Name(&'a str, Option<&'a str>),
}

/// How long a challenge lives, by default (`auth.mfa_challenge_seconds`).
pub const CHALLENGE_DEFAULT: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6238 appendix B, SHA-1 with the 20-byte ASCII seed: the last
    /// six digits of the eight-digit values.
    #[test]
    fn rfc6238_vectors() {
        let seed = b"12345678901234567890";
        for (t, want) in [
            (59u64, 94_287_082u32),
            (1_111_111_109, 7_081_804),
            (1_111_111_111, 14_050_471),
            (1_234_567_890, 89_005_924),
            (2_000_000_000, 69_279_037),
        ] {
            assert_eq!(code_at(seed, t / STEP), want % 1_000_000, "t = {t}");
        }
    }

    #[test]
    fn codes_work_once_and_within_one_step() {
        let seed = b"12345678901234567890";
        let now = UNIX_EPOCH + Duration::from_secs(1_111_111_111);
        let s = step_of(now);
        let code = format!("{:06}", code_at(seed, s));
        assert_eq!(check(seed, &code, now, None), Some(s));
        // Used: the same step again is refused.
        assert_eq!(check(seed, &code, now, Some(s)), None);
        // The previous step's code is still accepted (drift), once.
        let prev = format!("{:06}", code_at(seed, s - 1));
        assert_eq!(check(seed, &prev, now, None), Some(s - 1));
        // Two steps away: refused.
        let old = format!("{:06}", code_at(seed, s - 2));
        assert_eq!(check(seed, &old, now, None), None);
        for bad in ["", "12345", "1234567", "abcdef"] {
            assert_eq!(check(seed, bad, now, None), None);
        }
    }

    #[test]
    fn base32_and_uri() {
        assert_eq!(base32(b"foobar"), "MZXW6YTBOI");
        assert_eq!(
            base32(b"12345678901234567890"),
            "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
        );
        let u = otpauth_uri("SprigDB acme", "acme.app", b"12345678901234567890");
        assert!(u.starts_with("otpauth://totp/SprigDB%20acme:acme.app?secret=GEZDGNBV"));
        assert!(u.contains("&digits=6&period=30"));
    }

    #[test]
    fn recovery_codes_normalize() {
        let codes = recovery_codes().unwrap();
        assert_eq!(codes.len(), RECOVERY_CODES);
        assert!(
            codes
                .iter()
                .all(|c| c.len() == 11 && c.as_bytes()[5] == b'-')
        );
        let c = &codes[0];
        assert_eq!(
            recovery_hash(c),
            recovery_hash(&c.to_uppercase().replace('-', " "))
        );
        assert_ne!(recovery_hash(&codes[0]), recovery_hash(&codes[1]));
    }

    struct Fixed;
    impl FactorKeys for Fixed {
        fn current(&self, t: TenantId) -> Option<(u32, [u8; 32])> {
            Some((2, [u8::try_from(t.0 % 200).unwrap(); 32]))
        }
        fn key(&self, t: TenantId, v: u32) -> Option<[u8; 32]> {
            (v == 2).then(|| [u8::try_from(t.0 % 200).unwrap(); 32])
        }
    }

    use crate::token::sessions::tests::{T, auth_version, catalog};

    /// The code of `seed` at `now`.
    fn code_now(seed: &[u8], now: SystemTime) -> String {
        format!("{:06}", code_at(seed, step_of(now)))
    }

    fn t(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_800_000_000 + secs)
    }

    #[tokio::test]
    async fn an_enrolment_replaces_a_pending_seed_without_a_version() {
        let (recs, cat) = catalog().await;
        let f = FactorStore::new(cat);
        let u = UserId(3);
        let first = f.enrol(&Fixed, T, u, t(0)).await.unwrap();
        let second = f.enrol(&Fixed, T, u, t(1)).await.unwrap();
        assert_ne!(first, second);
        let list = f.list(T, u).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(
            (list[0].kind.as_str(), list[0].state.as_str()),
            ("totp", "pending")
        );
        assert_eq!(list[0].created_ms, 1_800_000_001_000);
        assert!(!f.has_active(T, u).await.unwrap());
        assert_eq!(auth_version(&recs).await, 0);
        // Another user's factors are apart.
        assert!(f.list(T, UserId(4)).await.unwrap().is_empty());
        // The pending seed is the second one: the first one's code fails.
        let now = t(60);
        let wrong = f
            .confirm(&Fixed, T, u, &code_now(&first, now), now)
            .await
            .unwrap();
        assert!(wrong.is_none() || code_now(&first, now) == code_now(&second, now));
    }

    #[tokio::test]
    async fn a_confirmation_activates_and_replaces_recovery_codes_in_one_change() {
        let (recs, cat) = catalog().await;
        let f = FactorStore::new(cat);
        let u = UserId(3);
        let e = f.confirm(&Fixed, T, u, "000000", t(0)).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AdminRequestInvalid));
        let seed = f.enrol(&Fixed, T, u, t(0)).await.unwrap();
        let now = t(30);
        let codes = f
            .confirm(&Fixed, T, u, &code_now(&seed, now), now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(codes.len(), RECOVERY_CODES);
        let v = auth_version(&recs).await;
        assert!(v > 0);
        assert!(f.has_active(T, u).await.unwrap());
        assert_eq!(f.recovery_left(T, u).await.unwrap(), RECOVERY_CODES);
        let list = f.list(T, u).await.unwrap();
        // Newest first: the recovery codes, then the factor.
        assert_eq!(list.last().unwrap().kind, "totp");
        assert!(list.windows(2).all(|w| w[0].id > w[1].id));
        // The step of the confirming code is used up.
        let again = f.verify(&Fixed, T, u, &code_now(&seed, now), now).await;
        assert_eq!(again.unwrap(), None);
        // A new enrolment and confirmation replace the factor and the codes.
        let seed2 = f.enrol(&Fixed, T, u, t(100)).await.unwrap();
        let later = t(200);
        let codes2 = f
            .confirm(&Fixed, T, u, &code_now(&seed2, later), later)
            .await
            .unwrap()
            .unwrap();
        let list = f.list(T, u).await.unwrap();
        assert_eq!(list.len(), RECOVERY_CODES + 1);
        assert_eq!(list.iter().filter(|x| x.kind == "totp").count(), 1);
        assert!(auth_version(&recs).await > v);
        // The old codes are gone, the new ones work.
        assert_eq!(
            f.verify(&Fixed, T, u, &codes[0], later).await.unwrap(),
            None
        );
        assert_eq!(
            f.verify(&Fixed, T, u, &codes2[0], later).await.unwrap(),
            Some("rec")
        );
    }

    #[tokio::test]
    async fn totp_codes_and_recovery_codes_work_once() {
        let (recs, cat) = catalog().await;
        let f = FactorStore::new(cat);
        let u = UserId(3);
        let seed = f.enrol(&Fixed, T, u, t(0)).await.unwrap();
        let codes = f
            .confirm(&Fixed, T, u, &code_now(&seed, t(30)), t(30))
            .await
            .unwrap()
            .unwrap();
        let v = auth_version(&recs).await;
        let now = t(90);
        let code = code_now(&seed, now);
        assert_eq!(
            f.verify(&Fixed, T, u, &code, now).await.unwrap(),
            Some("otp")
        );
        assert_eq!(f.verify(&Fixed, T, u, &code, now).await.unwrap(), None);
        // An older step after a newer one: refused.
        let prev = format!("{:06}", code_at(&seed, step_of(now) - 1));
        if prev != code {
            assert_eq!(f.verify(&Fixed, T, u, &prev, now).await.unwrap(), None);
        }
        // Recovery codes, in any case and spacing, once.
        let spaced = codes[3].to_uppercase().replace('-', " ");
        assert_eq!(
            f.verify(&Fixed, T, u, &spaced, now).await.unwrap(),
            Some("rec")
        );
        assert_eq!(f.verify(&Fixed, T, u, &codes[3], now).await.unwrap(), None);
        assert_eq!(f.recovery_left(T, u).await.unwrap(), RECOVERY_CODES - 1);
        assert_eq!(
            f.verify(&Fixed, T, u, "nope-nope", now).await.unwrap(),
            None
        );
        // Neither is an auth change.
        assert_eq!(auth_version(&recs).await, v);
        // Another user's codes don't work for this one.
        assert_eq!(
            f.verify(&Fixed, T, UserId(4), &codes[4], now)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_reset_deletes_every_factor_in_one_change() {
        let (recs, cat) = catalog().await;
        let f = FactorStore::new(cat);
        let u = UserId(3);
        let seed = f.enrol(&Fixed, T, u, t(0)).await.unwrap();
        f.confirm(&Fixed, T, u, &code_now(&seed, t(30)), t(30))
            .await
            .unwrap()
            .unwrap();
        f.enrol(&Fixed, T, UserId(4), t(0)).await.unwrap();
        let v = auth_version(&recs).await;
        let n = f.reset(T, u, t(60)).await.unwrap();
        assert_eq!(n, u64::try_from(RECOVERY_CODES).unwrap() + 1);
        assert!(f.list(T, u).await.unwrap().is_empty());
        assert_eq!(f.list(T, UserId(4)).await.unwrap().len(), 1);
        assert!(auth_version(&recs).await > v);
        // Factor ids are never reused.
        let again = f.enrol(&Fixed, T, u, t(70)).await.unwrap();
        let _ = again;
        let id = f.list(T, u).await.unwrap()[0].id;
        assert!(id > u64::try_from(RECOVERY_CODES).unwrap() + 2);
    }

    #[test]
    fn seeds_seal_per_tenant_and_user() {
        let seed = random::<SEED_BYTES>().unwrap();
        let s = seal(&Fixed, TenantId(7), UserId(3), &seed).unwrap();
        assert_eq!(&s[..4], &2u32.to_be_bytes());
        assert_eq!(open(&Fixed, TenantId(7), UserId(3), &s).unwrap(), seed);
        // Another user's or tenant's row can't be swapped in.
        assert!(open(&Fixed, TenantId(7), UserId(4), &s).is_err());
        assert!(open(&Fixed, TenantId(8), UserId(3), &s).is_err());
        let mut bad = s.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(open(&Fixed, TenantId(7), UserId(3), &bad).is_err());
    }
}
