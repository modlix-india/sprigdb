//! Token sessions (the catalog's `token_sessions`): where a session lives, so
//! any gateway can verify, refresh and end it.
//!
//! [`CatalogSessions`] keeps them in the tenant catalog's `token_sessions`
//! (catalog records, `m/auth/token_sessions/<session id>`):
//!
//! - **create** is an auth change (the row with the change's version), so
//!   every gateway's [`TenantAuth`] learns the session;
//! - **verify** looks the session up in the cached [`TenantAuth`] (not
//!   revoked, not expired), then checks the account as an API key does
//!   (live, not locked, tenant not suspended, TLS, password unchanged since
//!   the session began). A session too new for this gateway's snapshot is
//!   read once from the catalog records;
//! - **refresh** is a plain record transaction: the row read for update
//!   serializes refreshes from any gateway, so the token
//!   presented is compared with the *current* `refresh_sha256` wherever the
//!   last refresh happened. Equal: both hashes rotate and `last_used_at`
//!   moves (not an auth change: nothing a cache holds changed). Equal to
//!   `refresh_prev`: reuse means theft, so the session is revoked in an
//!   auth change and every gateway refuses its access tokens within
//!   moments;
//! - **logout** revokes in an auth change;
//! - **`last_used_at`** moves on refresh and, at most once a minute per
//!   session and gateway, when an access token is used.
//!
//! [`MemorySessions`] keeps them in the process, for logins that don't come
//! from a tenant catalog (the accounts file) and for tests.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordTx};
use sprig_catalog::{AuthCatalog, ChangeOpts, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use subtle::ConstantTimeEq as _;

use crate::cache::TenantAuth;
use crate::mysql::{LoginService, LoginSource, Principal, locked, temp_locked};
use crate::rec::{TOKEN_SESSIONS, duplicate, to_micros};
use crate::store::RequireTls;
use sprig_proto_mysql::Persona;

/// A session as caches keep it (no refresh hashes: those are read from the
/// catalog records when a refresh needs them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSession {
    /// The `sid` claim.
    pub sid: [u8; 16],
    /// The user.
    pub user_id: UserId,
    /// Authentication methods (`pwd`).
    pub amr: Vec<String>,
    /// `studio`, `api` or `cli`.
    pub client: String,
    /// When the session ends at the latest.
    pub expires_at: SystemTime,
    /// When it was revoked.
    pub revoked_at: Option<SystemTime>,
    /// When it began.
    pub created_at: SystemTime,
    /// The auth version of its last versioned change.
    pub changed_version: u64,
}

impl TokenSession {
    /// Not revoked and not past `expires_at`.
    #[must_use]
    pub fn live(&self, now: SystemTime) -> bool {
        self.revoked_at.is_none() && self.expires_at > now
    }
}

/// The members of `amr` (MariaDB's `SET('pwd','otp','hwk','rec')`), in
/// the set's order.
const AMR: [&str; 4] = ["pwd", "otp", "hwk", "rec"];
/// The values of `client` (MariaDB's `ENUM('studio','api','cli')`).
const CLIENTS: [&str; 3] = ["studio", "api", "cli"];

/// MariaDB's strict-mode refusal of a value its column can't hold.
fn truncated(column: &str) -> SprigError {
    SprigError::backend(
        1265,
        *b"01000",
        format!("catalog statement failed (1265): data truncated for column {column}"),
    )
}

/// `amr` as the `SET` column kept it: each member once, in the set's
/// order.
///
/// # Errors
/// 1265 for a value that isn't a member.
fn stored_amr(amr: &[String]) -> Result<Vec<String>> {
    let joined = amr.join(",");
    let given: Vec<&str> = joined.split(',').filter(|m| !m.is_empty()).collect();
    if given.iter().any(|m| !AMR.contains(m)) {
        return Err(truncated("amr"));
    }
    Ok(AMR
        .iter()
        .filter(|m| given.contains(m))
        .map(|m| (*m).to_owned())
        .collect())
}

/// A `token_sessions` row. `Debug` hides the refresh hashes.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionRow {
    session_id: [u8; 16],
    user_id: UserId,
    amr: Vec<String>,
    refresh_sha256: [u8; 32],
    refresh_prev: Option<[u8; 32]>,
    impersonator_tenant: Option<u64>,
    impersonator_user: Option<u32>,
    reason: Option<String>,
    #[serde(with = "crate::rec::micros")]
    expires_at: SystemTime,
    #[serde(with = "crate::rec::micros_opt")]
    revoked_at: Option<SystemTime>,
    client: String,
    #[serde(with = "crate::rec::micros_opt")]
    last_used_at: Option<SystemTime>,
    #[serde(with = "crate::rec::micros")]
    created_at: SystemTime,
    changed_version: u64,
}

impl std::fmt::Debug for SessionRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRow")
            .field("user_id", &self.user_id)
            .field("revoked_at", &self.revoked_at)
            .field("changed_version", &self.changed_version)
            .finish_non_exhaustive()
    }
}

impl SessionRow {
    fn cached(&self) -> TokenSession {
        TokenSession {
            sid: self.session_id,
            user_id: self.user_id,
            amr: self.amr.clone(),
            client: self.client.clone(),
            expires_at: self.expires_at,
            revoked_at: self.revoked_at,
            created_at: self.created_at,
            changed_version: self.changed_version,
        }
    }
}

/// The primary key of session `sid`.
fn session_pk(sid: &[u8; 16]) -> Pk {
    Pk::new().raw(sid)
}

/// Sessions changed after `since`; with `None`, every live one (not
/// revoked, not expired now). By session id.
///
/// # Errors
/// The store's; `Internal` for a malformed row.
pub async fn sessions_since(
    tx: &mut dyn RecordTx,
    since: Option<u64>,
) -> Result<Vec<TokenSession>> {
    let rows: Vec<SessionRow> = TOKEN_SESSIONS.rows(tx, &Pk::new()).await?;
    let now = SystemTime::now();
    Ok(rows
        .iter()
        .filter(|r| match since {
            Some(v) => r.changed_version > v,
            None => r.revoked_at.is_none() && r.expires_at > now,
        })
        .map(SessionRow::cached)
        .collect())
}

/// Revokes every live session of `user` in the caller's auth change
/// (`version`), as of `now`. Returns how many.
///
/// # Errors
/// The store's.
pub(crate) async fn revoke_user_sessions(
    tx: &mut dyn RecordTx,
    user: UserId,
    version: u64,
    now: SystemTime,
) -> Result<u64> {
    let rows: Vec<SessionRow> = TOKEN_SESSIONS.rows(tx, &Pk::new()).await?;
    let mut n = 0;
    for sid in rows
        .iter()
        .filter(|r| r.user_id == user && r.revoked_at.is_none())
        .map(|r| r.session_id)
    {
        if revoke_row(tx, &sid, version, now).await? {
            n += 1;
        }
    }
    Ok(n)
}

/// Revokes session `sid` when it is there and not revoked yet (its latest
/// value, locked: refreshes write it outside auth changes). Returns
/// whether it did.
async fn revoke_row(
    tx: &mut dyn RecordTx,
    sid: &[u8; 16],
    version: u64,
    now: SystemTime,
) -> Result<bool> {
    let pk = session_pk(sid);
    let Some(mut row) = TOKEN_SESSIONS.get_for_update::<SessionRow>(tx, &pk).await? else {
        return Ok(false);
    };
    if row.revoked_at.is_some() {
        return Ok(false);
    }
    row.revoked_at = Some(to_micros(now));
    row.changed_version = version;
    TOKEN_SESSIONS.put(tx, &pk, &row).await?;
    Ok(true)
}

/// A session to create.
#[derive(Debug, Clone)]
pub struct NewSession {
    /// The `sid`.
    pub sid: [u8; 16],
    /// Who signed in.
    pub principal: Principal,
    /// `SHA256` of the first refresh token.
    pub refresh_sha256: [u8; 32],
    /// `pwd`, ...
    pub amr: Vec<String>,
    /// `studio`, `api` or `cli`.
    pub client: String,
    /// The session's end at the latest.
    pub expires_at: SystemTime,
    /// Now.
    pub now: SystemTime,
    /// For an impersonation session: who acts, why.
    pub impersonator: Option<Impersonator>,
}

/// The administrator behind an impersonation session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impersonator {
    /// The administrator's tenant id.
    pub tenant_id: u64,
    /// The administrator's user id.
    pub user_id: u32,
    /// The reason given (required).
    pub reason: String,
}

/// What a refresh found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rotated {
    /// The token was the current one: rotated to the new hash.
    Ok {
        /// The tenant, `0` for none.
        tenant_id: u64,
        /// The user.
        user_id: Option<UserId>,
        /// When the session began.
        created_at: SystemTime,
        /// `amr`.
        amr: Vec<String>,
        /// `client`.
        client: String,
    },
    /// The token was the previous one: the session is now revoked.
    Reused,
    /// No such live session, or neither hash matches.
    Invalid,
    /// Idle longer than the limit: the session is now revoked.
    Idle,
    /// Older than the limit: the session is now revoked.
    Expired,
}

/// The checks a refresh makes besides the token.
#[derive(Debug, Clone, Copy)]
pub struct RefreshRules {
    /// Refused when `last_used_at` is older than this.
    pub idle: Duration,
    /// Refused when the session is older than this.
    pub max_age: Duration,
}

/// Where sessions live.
#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    /// Stores a new session.
    ///
    /// # Errors
    /// The store's; 39301 when the catalog writer can't be reached.
    async fn create(&self, s: &NewSession) -> Result<()>;

    /// Rotates `tenant`'s session `sid` from `presented` to `next` when
    /// `presented` is its current refresh hash (see [`Rotated`]).
    ///
    /// # Errors
    /// The store's.
    async fn rotate(
        &self,
        tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated>;

    /// Ends a session. Returns whether a live one ended.
    ///
    /// # Errors
    /// The store's.
    async fn revoke(&self, tenant: &str, sid: [u8; 16], now: SystemTime) -> Result<bool>;

    /// The principal of a live session, its account checked now: `tenant`
    /// and `tenant_id` from the token, `user_id` and `sid` from it too.
    ///
    /// # Errors
    /// 1045 when the session or account no longer allows it; 39303
    /// suspended; 3118 locked; 39301 when the catalog can't be read.
    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)>;

    /// Records a use of the session (at most once a minute per gateway is
    /// enough; the caller limits).
    async fn touch(&self, tenant: &str, sid: [u8; 16], now: SystemTime);
}

/// Who a verified token names.
#[derive(Debug, Clone, Copy)]
pub struct Who<'a> {
    /// The tenant's name (`-` for none).
    pub tenant: &'a str,
    /// Its id, `0` for none.
    pub tenant_id: u64,
    /// The user's id, `0` for none.
    pub user_id: u32,
    /// The session.
    pub sid: [u8; 16],
    /// The client's address.
    pub client_ip: IpAddr,
    /// Over TLS.
    pub tls: bool,
}

pub(crate) fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}

fn packet_error(p: sprig_proto_mysql::ErrPacket) -> SprigError {
    SprigError::standard(p.code, p.sqlstate, p.message)
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    bool::from(a.ct_eq(b))
}

// ---- in memory ----

#[derive(Clone)]
struct MemSession {
    principal: Principal,
    refresh: [u8; 32],
    refresh_prev: Option<[u8; 32]>,
    amr: Vec<String>,
    client: String,
    created_at: SystemTime,
    expires_at: SystemTime,
    last_used: SystemTime,
}

/// Sessions in this process: a token works only here, and a restart ends
/// them. For the accounts file and tests.
#[derive(Default)]
pub struct MemorySessions {
    inner: Mutex<HashMap<[u8; 16], MemSession>>,
}

impl std::fmt::Debug for MemorySessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySessions")
            .field("open", &self.len())
            .finish()
    }
}

/// Sessions kept at most in memory; the least recently used go first.
pub const MAX_MEMORY_SESSIONS: usize = 100_000;

impl MemorySessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 16], MemSession>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// None open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl SessionStore for MemorySessions {
    async fn create(&self, s: &NewSession) -> Result<()> {
        let mut g = self.lock();
        g.retain(|_, m| m.expires_at > s.now);
        if g.len() >= MAX_MEMORY_SESSIONS
            && let Some(oldest) = g.iter().min_by_key(|(_, m)| m.last_used).map(|(k, _)| *k)
        {
            g.remove(&oldest);
        }
        g.insert(
            s.sid,
            MemSession {
                principal: s.principal.clone(),
                refresh: s.refresh_sha256,
                refresh_prev: None,
                amr: s.amr.clone(),
                client: s.client.clone(),
                created_at: s.now,
                expires_at: s.expires_at,
                last_used: s.now,
            },
        );
        Ok(())
    }

    async fn rotate(
        &self,
        _tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated> {
        let mut g = self.lock();
        let Some(m) = g.get_mut(&sid) else {
            return Ok(Rotated::Invalid);
        };
        if m.refresh_prev.is_some_and(|p| ct_eq(&p, &presented)) {
            g.remove(&sid);
            return Ok(Rotated::Reused);
        }
        if !ct_eq(&m.refresh, &presented) {
            return Ok(Rotated::Invalid);
        }
        if now.duration_since(m.created_at).unwrap_or_default() >= rules.max_age
            || m.expires_at <= now
        {
            g.remove(&sid);
            return Ok(Rotated::Expired);
        }
        if now.duration_since(m.last_used).unwrap_or_default() >= rules.idle {
            g.remove(&sid);
            return Ok(Rotated::Idle);
        }
        m.refresh_prev = Some(m.refresh);
        m.refresh = next;
        m.last_used = now;
        Ok(Rotated::Ok {
            tenant_id: m.principal.tenant.map_or(0, |t| t.0),
            user_id: m.principal.user_id,
            created_at: m.created_at,
            amr: m.amr.clone(),
            client: m.client.clone(),
        })
    }

    async fn revoke(&self, _tenant: &str, sid: [u8; 16], _now: SystemTime) -> Result<bool> {
        Ok(self.lock().remove(&sid).is_some())
    }

    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)> {
        let g = self.lock();
        match g.get(&who.sid) {
            Some(m) if m.expires_at > now => {
                let mut p = m.principal.clone();
                p.client_ip = who.client_ip;
                Ok((p, m.created_at))
            }
            _ => Err(denied("the session ended; sign in again")),
        }
    }

    async fn touch(&self, _tenant: &str, sid: [u8; 16], now: SystemTime) {
        if let Some(m) = self.lock().get_mut(&sid) {
            m.last_used = now;
        }
    }
}

// ---- in the tenant catalog ----

/// Sessions in the tenant catalogs (catalog records, `token_sessions`).
pub struct CatalogSessions {
    login: Arc<LoginService>,
    cat: AuthCatalog,
}

impl std::fmt::Debug for CatalogSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSessions").finish_non_exhaustive()
    }
}

fn unavailable(what: &str, e: &SprigError) -> SprigError {
    tracing::warn!(error = %e, "token sessions: {what}");
    SprigError::new(
        SprigCode::AuthUnavailable,
        "authentication unavailable; retry",
    )
}

/// Ends an auth change: commits when `r` says a row changed, else rolls
/// back and settles the version.
async fn finish(
    cat: &AuthCatalog,
    tenant: TenantId,
    c: sprig_catalog::AuthTx,
    r: Result<bool>,
) -> Result<bool> {
    match r {
        Ok(true) => {
            c.commit().await?;
            Ok(true)
        }
        Ok(false) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Ok(false)
        }
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Stores session `s` of `tenant` in an auth change, first purging up to
/// 100 rows that expired or were revoked more than a day ago.
async fn insert(cat: &AuthCatalog, tenant: TenantId, s: &NewSession) -> Result<()> {
    let user = s
        .principal
        .user_id
        .ok_or_else(|| denied("token sessions need a catalog user"))?;
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        // Expired and long-revoked rows go now and then (no version:
        // caches never treat a missing row as live). A failure here
        // doesn't stop the sign-in.
        let _ = purge(tx, s.now).await;
        let pk = session_pk(&s.sid);
        if TOKEN_SESSIONS.get::<SessionRow>(tx, &pk).await?.is_some() {
            return Err(duplicate("session id"));
        }
        if !CLIENTS.contains(&s.client.as_str()) {
            return Err(truncated("client"));
        }
        let now = to_micros(s.now);
        let row = SessionRow {
            session_id: s.sid,
            user_id: user,
            amr: stored_amr(&s.amr)?,
            refresh_sha256: s.refresh_sha256,
            refresh_prev: None,
            impersonator_tenant: s.impersonator.as_ref().map(|i| i.tenant_id),
            impersonator_user: s.impersonator.as_ref().map(|i| i.user_id),
            reason: s
                .impersonator
                .as_ref()
                .map(|i| i.reason.chars().take(256).collect::<String>()),
            expires_at: to_micros(s.expires_at),
            revoked_at: None,
            client: s.client.clone(),
            last_used_at: Some(now),
            created_at: now,
            changed_version: version,
        };
        TOKEN_SESSIONS.put(tx, &pk, &row).await
    }
    .await;
    match r {
        Ok(()) => {
            c.commit().await?;
            Ok(())
        }
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Rows purged per sign-in at most.
const PURGE_LIMIT: usize = 100;

/// Deletes up to [`PURGE_LIMIT`] sessions that expired, or were revoked,
/// more than a day before `now`. Returns how many.
async fn purge(tx: &mut dyn RecordTx, now: SystemTime) -> Result<usize> {
    let cut = now
        .checked_sub(Duration::from_secs(86_400))
        .unwrap_or(std::time::UNIX_EPOCH);
    let rows: Vec<SessionRow> = TOKEN_SESSIONS.rows(tx, &Pk::new()).await?;
    let old: Vec<[u8; 16]> = rows
        .iter()
        .filter(|r| r.expires_at < cut || r.revoked_at.is_some_and(|t| t < cut))
        .map(|r| r.session_id)
        .take(PURGE_LIMIT)
        .collect();
    for sid in &old {
        TOKEN_SESSIONS.delete(tx, &session_pk(sid)).await?;
    }
    Ok(old.len())
}

/// Revokes `tenant`'s session `sid` in an auth change. Returns whether a
/// live one ended (nothing changes, and no version is used, otherwise).
async fn revoke_in_change(
    cat: &AuthCatalog,
    tenant: TenantId,
    sid: [u8; 16],
    now: SystemTime,
) -> Result<bool> {
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = revoke_row(c.tx().records(), &sid, version, now).await;
    finish(cat, tenant, c, r).await
}

/// The session row as committed, for a session newer than this gateway's
/// snapshot.
async fn read_session(
    cat: &AuthCatalog,
    tenant: TenantId,
    sid: [u8; 16],
) -> Result<Option<TokenSession>> {
    let mut tx = cat.backend().store().begin(tenant).await?;
    let row = TOKEN_SESSIONS
        .get::<SessionRow>(&mut *tx, &session_pk(&sid))
        .await;
    let _ = tx.rollback().await;
    Ok(row?.as_ref().map(SessionRow::cached))
}

/// Rotates `tenant`'s session `sid` (see [`SessionStore::rotate`]): the
/// row read for update serializes refreshes from every gateway; the
/// rotation itself is not an auth change, the revocation on reuse, idle
/// or age is.
async fn rotate_in(
    cat: &AuthCatalog,
    tid: TenantId,
    sid: [u8; 16],
    presented: [u8; 32],
    next: [u8; 32],
    rules: RefreshRules,
    now: SystemTime,
) -> Result<Rotated> {
    let mut tx = cat
        .backend()
        .store()
        .begin(tid)
        .await
        .map_err(|e| unavailable("catalog writer unreachable", &e))?;
    let pk = session_pk(&sid);
    let found = TOKEN_SESSIONS
        .get_for_update::<SessionRow>(&mut *tx, &pk)
        .await?;
    let Some(mut row) = found else {
        let _ = tx.rollback().await;
        return Ok(Rotated::Invalid);
    };
    if row.revoked_at.is_some() {
        let _ = tx.rollback().await;
        return Ok(Rotated::Invalid);
    }
    let last_used = row.last_used_at.unwrap_or(row.created_at);
    let outcome = if row.refresh_prev.is_some_and(|p| ct_eq(&p, &presented)) {
        Some(Rotated::Reused)
    } else if !ct_eq(&row.refresh_sha256, &presented) {
        let _ = tx.rollback().await;
        return Ok(Rotated::Invalid);
    } else if row.expires_at <= now
        || now.duration_since(row.created_at).unwrap_or_default() >= rules.max_age
    {
        Some(Rotated::Expired)
    } else if now.duration_since(last_used).unwrap_or_default() >= rules.idle {
        Some(Rotated::Idle)
    } else {
        None
    };
    if let Some(end) = outcome {
        let _ = tx.rollback().await;
        revoke_in_change(cat, tid, sid, now).await?;
        return Ok(end);
    }
    row.refresh_prev = Some(row.refresh_sha256);
    row.refresh_sha256 = next;
    row.last_used_at = Some(to_micros(now));
    TOKEN_SESSIONS.put(&mut *tx, &pk, &row).await?;
    tx.commit().await?;
    Ok(Rotated::Ok {
        tenant_id: tid.0,
        user_id: Some(row.user_id),
        created_at: row.created_at,
        amr: row.amr,
        client: row.client,
    })
}

/// Moves `last_used_at` of `tenant`'s session `sid` to `now` when it is
/// not revoked (not an auth change).
async fn touch_in(cat: &AuthCatalog, tid: TenantId, sid: [u8; 16], now: SystemTime) -> Result<()> {
    let mut tx = cat.backend().store().begin(tid).await?;
    let pk = session_pk(&sid);
    let found = TOKEN_SESSIONS
        .get_for_update::<SessionRow>(&mut *tx, &pk)
        .await?;
    match found {
        Some(mut row) if row.revoked_at.is_none() => {
            row.last_used_at = Some(to_micros(now));
            TOKEN_SESSIONS.put(&mut *tx, &pk, &row).await?;
            tx.commit().await
        }
        _ => tx.rollback().await,
    }
}

impl CatalogSessions {
    /// Sessions of the tenants `login` knows, written through `cat`.
    #[must_use]
    pub fn new(login: Arc<LoginService>, cat: AuthCatalog) -> Self {
        Self { login, cat }
    }

    fn logins(&self) -> Result<&Arc<crate::mysql::CatalogLogins>> {
        match self.login.source() {
            LoginSource::Catalog(c) => Ok(c),
            LoginSource::File(_) => Err(denied("token sessions need tenant catalogs")),
        }
    }

    async fn tenant_id(&self, name: &str) -> Result<TenantId> {
        match self.logins()?.tenants().lookup(name).await {
            Ok(Some(t)) => Ok(t.id),
            Ok(None) => Err(denied("the tenant no longer exists")),
            Err(e) => Err(unavailable("tenant directory unreadable", &e)),
        }
    }
}

#[async_trait]
impl SessionStore for CatalogSessions {
    async fn create(&self, s: &NewSession) -> Result<()> {
        let tenant = s
            .principal
            .tenant
            .ok_or_else(|| denied("token sessions need a tenant login"))?;
        insert(&self.cat, tenant, s).await
    }

    async fn rotate(
        &self,
        tenant: &str,
        sid: [u8; 16],
        presented: [u8; 32],
        next: [u8; 32],
        rules: RefreshRules,
        now: SystemTime,
    ) -> Result<Rotated> {
        let tid = self.tenant_id(tenant).await?;
        rotate_in(&self.cat, tid, sid, presented, next, rules, now).await
    }

    async fn revoke(&self, tenant: &str, sid: [u8; 16], now: SystemTime) -> Result<bool> {
        let tid = self.tenant_id(tenant).await?;
        revoke_in_change(&self.cat, tid, sid, now).await
    }

    #[allow(clippy::too_many_lines)] // the checks, in order
    async fn principal(&self, who: &Who<'_>, now: SystemTime) -> Result<(Principal, SystemTime)> {
        let cl = self.logins()?;
        let tenant = match cl.tenants().lookup(who.tenant).await {
            Ok(Some(t)) if t.can_log_in && t.id.0 == who.tenant_id => t,
            Ok(_) => return Err(denied("the tenant no longer exists")),
            Err(e) => return Err(unavailable("tenant directory unreadable", &e)),
        };
        let catalog = cl
            .catalogs()
            .catalog(tenant.id)
            .await
            .map_err(|e| unavailable("catalog unreadable", &e))?;
        let Some(auth) = TenantAuth::of(&catalog) else {
            return Err(denied("the tenant has no accounts"));
        };
        let session = match auth.session(&who.sid) {
            Some(s) => (**s).clone(),
            // Newer than this snapshot (signed in on another gateway a
            // moment ago), or not there at all.
            None => read_session(&self.cat, tenant.id, who.sid)
                .await
                .map_err(|e| unavailable("catalog writer unreachable", &e))?
                .ok_or_else(|| denied("the session ended; sign in again"))?,
        };
        if !session.live(now) || session.user_id.0 != who.user_id {
            return Err(denied("the session ended; sign in again"));
        }
        let Some(user) = auth.user(session.user_id).cloned() else {
            return Err(denied("the account no longer exists"));
        };
        if user
            .row
            .password_changed_at
            .is_some_and(|t| t > session.created_at)
        {
            return Err(denied(
                "the password changed since the session began; sign in again",
            ));
        }
        let (name, host) = (user.row.name.as_str(), user.row.host.as_str());
        if let Some(lock) = auth
            .temp_lock(user.row.user_id)
            .filter(|l| l.locked_until > now)
        {
            return Err(packet_error(temp_locked(
                Persona::MySql84,
                name,
                host,
                &lock,
                now,
            )));
        }
        if tenant.suspended {
            return Err(SprigError::new(
                SprigCode::TenantSuspended,
                format!("tenant {} is suspended", tenant.name),
            ));
        }
        if user.row.account_locked {
            return Err(packet_error(locked(Persona::MySql84, name, host)));
        }
        let need_tls = user.row.require_tls != RequireTls::None || cl.require_tls(&catalog);
        if (need_tls && !who.tls) || user.row.require_tls == RequireTls::X509 {
            return Err(denied("the account requires TLS"));
        }
        let backend = cl.backend_of(&tenant, &user)?;
        Ok((
            Principal {
                tenant: Some(tenant.id),
                tenant_name: Some(tenant.name.clone()),
                user_id: Some(user.row.user_id),
                user: user.row.name.clone(),
                host: user.row.host.clone(),
                client_ip: who.client_ip,
                must_change_password: false,
                limits: user.row.limits,
                auth_version: catalog.auth_version,
                backend,
            },
            session.created_at,
        ))
    }

    async fn touch(&self, tenant: &str, sid: [u8; 16], now: SystemTime) {
        let r = async {
            let tid = self.tenant_id(tenant).await?;
            touch_in(&self.cat, tid, sid, now).await
        };
        if let Err(e) = r.await {
            tracing::debug!(error = %e, "token session: last_used_at not written");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
