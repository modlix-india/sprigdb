//! The auth tables of the tenant catalog, with typed rows and access: the
//! users, their verifiers and their login state, as catalog records in
//! the storage engine ([`crate::rec`]).
//!
//! **Versioning.** `users` and `verifiers` are versioned by the tenant's
//! auth version ([`VersionKind::Auth`](sprig_catalog::VersionKind)).
//! `verifiers` has no `changed_version`, so every change of a user's
//! verifiers also rewrites the user's row with the change's version; the
//! cache reloads a changed user's verifiers with it. `login_state` is not
//! versioned (it is read on demand); only lock transitions bump the auth
//! version, and the cache re-reads every locked row on each load.
//!
//! **Formats.** The rows start at the newest shape (what MariaDB's format 7
//! had); the `auth` module has no migrations ([`AUTH_MODULE`]).

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{self, Pk, RecordTx};
use sprig_catalog::{Migration, Module};
use sprig_common::ids::UserId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::rec::{LOGIN_STATE, USERS, VERIFIERS, duplicate, host_eq};

/// The module name in the catalog's format records.
pub const MODULE: &str = "auth";

/// The auth module's migrations: none. Its rows are engine records, which
/// start at the newest shape; the module stays registered so older
/// catalogs' format records of it are recognised.
pub static AUTH_MIGRATIONS: &[Migration] = &[];

/// The auth module, for [`Migrator::new`](sprig_catalog::Migrator::new).
pub const AUTH_MODULE: Module = Module {
    name: MODULE,
    migrations: AUTH_MIGRATIONS,
};

/// The id counter of users.
pub const USER_COUNTER: &str = "user";

/// `locked_until` of an `UNBOUNDED` lock: 9999-12-31.
pub const UNBOUNDED_LOCK_MICROS: i64 = 253_402_214_400_000_000;

macro_rules! sql_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// The SQL text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the SQL text.
            ///
            /// # Errors
            /// `Internal` for any other text.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    _ => Err(SprigError::internal(format!(
                        concat!("auth catalog: bad ", stringify!($name), " {:?}"), s
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                $name::parse(&s).map_err(|e| serde::de::Error::custom(e.message))
            }
        }
    };
}

sql_enum! {
    /// `users.kind`.
    UserKind {
        /// A person or application logging in with a password.
        User = "user",
        /// An account meant for API keys.
        Service = "service",
    }
}

sql_enum! {
    /// `users.require_tls`: MySQL's `REQUIRE` clause.
    RequireTls {
        /// No requirement.
        None = "none",
        /// `REQUIRE SSL`: the connection must use TLS.
        Ssl = "ssl",
        /// `REQUIRE X509`: TLS with a verified client certificate.
        X509 = "x509",
    }
}

sql_enum! {
    /// `users.require_current`: `PASSWORD REQUIRE CURRENT`.
    RequireCurrent {
        /// From `auth.password_require_current`.
        Default = "default",
        /// Always.
        Yes = "yes",
        /// `PASSWORD REQUIRE CURRENT OPTIONAL`.
        Optional = "optional",
    }
}

sql_enum! {
    /// `users.source`: how the user was created.
    UserSource {
        /// `CREATE USER` over MySQL.
        Sql = "sql",
        /// `createUser` over MongoDB.
        Mongo = "mongo",
        /// `ACL SETUSER` over Redis.
        Redis = "redis",
        /// The HTTP or gRPC API.
        Api = "api",
        /// Just-in-time from an external token.
        OidcJit = "oidc_jit",
        /// Tenant bootstrap (the admin user) or tools.
        Bootstrap = "bootstrap",
    }
}

sql_enum! {
    /// `users.state`. A dropped user is a tombstone: its name is free and
    /// its id never reused.
    UserState {
        /// Can log in (subject to the statuses).
        Active = "active",
        /// Dropped: logs in as an unknown user.
        Dropped = "dropped",
    }
}

sql_enum! {
    /// `verifiers.mechanism`.
    VerifierKind {
        /// `caching_sha2_password`: `$A$RRR$` + salt + SHA-256 crypt.
        CachingSha2 = "caching_sha2_password",
        /// `mysql_native_password`: `*` + HEX(SHA1(SHA1(password))).
        Native = "mysql_native_password",
        /// SCRAM-SHA-256 (RFC 7677).
        ScramSha256 = "scram_sha_256",
        /// SCRAM-SHA-1 (MongoDB's variant).
        ScramSha1 = "scram_sha_1",
        /// SCRAM-SHA-512 (Kafka; format 3).
        ScramSha512 = "scram_sha_512",
    }
}

sql_enum! {
    /// `verifiers.slot`: MySQL's dual passwords plus history.
    Slot {
        /// The current password.
        Primary = "primary",
        /// The retained previous password (`RETAIN CURRENT PASSWORD`);
        /// logins accept it until `DISCARD OLD PASSWORD`.
        Secondary = "secondary",
        /// Earlier passwords, for `PASSWORD HISTORY` and `REUSE INTERVAL`
        /// checks only; never accepted at login.
        History = "history",
    }
}

sql_enum! {
    /// `verifiers.login_form`: which login name a SCRAM-SHA-1 verifier was
    /// computed over.
    LoginForm {
        /// Any form (every mechanism but SCRAM-SHA-1).
        Any = "any",
        /// The qualified form, `acme.alice`, of shared endpoints.
        Qualified = "qualified",
        /// The plain form, `alice`, of tenant endpoints.
        Plain = "plain",
    }
}

/// `users.auth_methods`: the login methods an account accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthMethods(u8);

impl AuthMethods {
    /// Passwords (every password mechanism).
    pub const PASSWORD: AuthMethods = AuthMethods(1);
    /// Tokens (SprigDB and external).
    pub const TOKEN: AuthMethods = AuthMethods(2);
    /// API keys.
    pub const API_KEY: AuthMethods = AuthMethods(4);
    const NAMES: [(&'static str, AuthMethods); 3] = [
        ("password", AuthMethods::PASSWORD),
        ("token", AuthMethods::TOKEN),
        ("api_key", AuthMethods::API_KEY),
    ];

    /// No method.
    #[must_use]
    pub const fn empty() -> Self {
        AuthMethods(0)
    }

    /// Whether every method of `other` is in `self`.
    #[must_use]
    pub const fn contains(self, other: AuthMethods) -> bool {
        self.0 & other.0 == other.0
    }

    /// Both sets.
    #[must_use]
    pub const fn union(self, other: AuthMethods) -> Self {
        AuthMethods(self.0 | other.0)
    }

    /// The SQL `SET` text.
    #[must_use]
    pub fn to_sql(self) -> String {
        Self::NAMES
            .iter()
            .filter(|(_, m)| self.contains(*m))
            .map(|(n, _)| *n)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parses the SQL `SET` text.
    ///
    /// # Errors
    /// `Internal` for an unknown member.
    pub fn parse(s: &str) -> Result<Self> {
        let mut out = AuthMethods::empty();
        for part in s.split(',').filter(|p| !p.is_empty()) {
            let (_, m) = Self::NAMES
                .iter()
                .find(|(n, _)| *n == part)
                .ok_or_else(|| {
                    SprigError::internal(format!("auth catalog: bad auth method {part:?}"))
                })?;
            out = out.union(*m);
        }
        Ok(out)
    }
}

impl Default for AuthMethods {
    fn default() -> Self {
        AuthMethods::PASSWORD
    }
}

/// An account's resource limits (`MAX_QUERIES_PER_HOUR` and the others);
/// 0 means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountLimits {
    /// `MAX_QUERIES_PER_HOUR`.
    pub max_queries_per_hour: u32,
    /// `MAX_UPDATES_PER_HOUR`.
    pub max_updates_per_hour: u32,
    /// `MAX_CONNECTIONS_PER_HOUR`.
    pub max_connections_per_hour: u32,
    /// `MAX_USER_CONNECTIONS`.
    pub max_user_connections: u32,
}

/// A `users` row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserRow {
    /// The id; never reused. The backend account is `t<tenant>_u<user_id>`.
    pub user_id: UserId,
    /// The user name, compared exactly.
    pub name: String,
    /// The host pattern, as MySQL writes it (`'%'`, `'10.%'`,
    /// `'10.0.0.0/8'`, ...).
    pub host: String,
    /// User or service account.
    pub kind: UserKind,
    /// Accepted login methods.
    pub auth_methods: AuthMethods,
    /// The account's MySQL plugin (`caching_sha2_password` by default).
    pub plugin: String,
    /// MongoDB authentication database.
    pub mongo_db: Option<String>,
    /// The home space (data inheritance).
    pub home_space_id: u64,
    /// `REQUIRE NONE | SSL | X509`.
    pub require_tls: RequireTls,
    /// `ACCOUNT LOCK`.
    pub account_locked: bool,
    /// `PASSWORD EXPIRE` (the flag; the lifetime is checked separately).
    pub password_expired: bool,
    /// When the password last changed.
    #[serde(with = "crate::rec::micros_opt")]
    pub password_changed_at: Option<SystemTime>,
    /// `PASSWORD EXPIRE INTERVAL n DAY`: `None` from `auth.*` settings,
    /// `Some(0)` never.
    pub password_lifetime: Option<i16>,
    /// `PASSWORD HISTORY n`.
    pub password_history: Option<i16>,
    /// `PASSWORD REUSE INTERVAL n DAY`.
    pub password_reuse_days: Option<i16>,
    /// `PASSWORD REQUIRE CURRENT`.
    pub require_current: RequireCurrent,
    /// `FAILED_LOGIN_ATTEMPTS n`; `None`: `auth.failed_login_attempts`.
    pub failed_login_attempts: Option<u16>,
    /// `PASSWORD_LOCK_TIME` in minutes; `None`: setting; -1: unbounded.
    pub password_lock_minutes: Option<i32>,
    /// Resource limits.
    pub limits: AccountLimits,
    /// `ATTRIBUTE` / `COMMENT` JSON, for `SPRIG_USER_ATTR()`.
    pub attributes: Option<serde_json::Value>,
    /// How the user was created.
    pub source: UserSource,
    /// 0: created by this tenant; else the ancestor tenant that did.
    pub created_by_tenant: u64,
    /// Active or dropped.
    pub state: UserState,
    /// When created.
    #[serde(with = "crate::rec::micros")]
    pub created_at: SystemTime,
    /// The auth version of the last change to the row or its verifiers.
    pub changed_version: u64,
    /// `REQUIRE SUBJECT '...'`: the verified client certificate's subject
    /// must be exactly this (`/C=../CN=..`).
    pub require_subject: Option<String>,
    /// `REQUIRE ISSUER '...'`: its issuer must be exactly this.
    pub require_issuer: Option<String>,
}

impl UserRow {
    /// A new active user with every policy column at its default.
    #[must_use]
    pub fn new(
        user_id: UserId,
        name: impl Into<String>,
        host: impl Into<String>,
        source: UserSource,
        created_at: SystemTime,
        changed_version: u64,
    ) -> Self {
        Self {
            user_id,
            name: name.into(),
            host: host.into(),
            kind: UserKind::User,
            auth_methods: AuthMethods::PASSWORD,
            plugin: "caching_sha2_password".to_owned(),
            mongo_db: None,
            home_space_id: 1,
            require_tls: RequireTls::None,
            account_locked: false,
            password_expired: false,
            password_changed_at: None,
            password_lifetime: None,
            password_history: None,
            password_reuse_days: None,
            require_current: RequireCurrent::Default,
            failed_login_attempts: None,
            password_lock_minutes: None,
            limits: AccountLimits::default(),
            attributes: None,
            source,
            created_by_tenant: 0,
            state: UserState::Active,
            created_at,
            changed_version,
            require_subject: None,
            require_issuer: None,
        }
    }

    /// The account as MySQL prints it: `'alice'@'%'`.
    #[must_use]
    pub fn account(&self) -> String {
        format!("'{}'@'{}'", self.name, self.host)
    }

    /// Whether the row is live (not a tombstone).
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.state == UserState::Active
    }
}

/// A `verifiers` row. `Debug` hides the secret-derived columns.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifierRow {
    /// The id (`AUTO_INCREMENT`; 0 when inserting), never reused: the fast
    /// cache keys on it.
    pub verifier_id: u64,
    /// The user.
    pub user_id: UserId,
    /// The mechanism.
    pub kind: VerifierKind,
    /// Primary, secondary or history.
    pub slot: Slot,
    /// The login name form (SCRAM-SHA-1 only; else `Any`).
    pub login_form: LoginForm,
    /// The stored string of `caching_sha2_password` and
    /// `mysql_native_password`.
    pub auth_string: Option<Vec<u8>>,
    /// SCRAM salt.
    pub salt: Option<Vec<u8>>,
    /// SCRAM iterations.
    pub iterations: Option<u32>,
    /// SCRAM `StoredKey`.
    pub stored_key: Option<Vec<u8>>,
    /// SCRAM `ServerKey`.
    pub server_key: Option<Vec<u8>>,
    /// Increases with every password change of the user.
    pub generation: u64,
    /// When created.
    #[serde(with = "crate::rec::micros")]
    pub created_at: SystemTime,
}

impl fmt::Debug for VerifierRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifierRow")
            .field("verifier_id", &self.verifier_id)
            .field("user_id", &self.user_id)
            .field("kind", &self.kind)
            .field("slot", &self.slot)
            .field("login_form", &self.login_form)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl VerifierRow {
    /// A `caching_sha2_password` or `mysql_native_password` verifier with
    /// its stored string.
    #[must_use]
    pub fn mysql(
        user_id: UserId,
        kind: VerifierKind,
        slot: Slot,
        auth_string: Vec<u8>,
        generation: u64,
        created_at: SystemTime,
    ) -> Self {
        Self {
            verifier_id: 0,
            user_id,
            kind,
            slot,
            login_form: LoginForm::Any,
            auth_string: Some(auth_string),
            salt: None,
            iterations: None,
            stored_key: None,
            server_key: None,
            generation,
            created_at,
        }
    }
}

/// A `login_state` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginStateRow {
    /// The user.
    pub user_id: UserId,
    /// Wrong credentials in a row (saturates).
    pub consecutive_failures: u32,
    /// The temporary lock's end; 9999-12-31 for `UNBOUNDED`.
    #[serde(with = "crate::rec::micros_opt")]
    pub locked_until: Option<SystemTime>,
    /// Last failure.
    #[serde(with = "crate::rec::micros_opt")]
    pub last_failure_at: Option<SystemTime>,
    /// Last success.
    #[serde(with = "crate::rec::micros_opt")]
    pub last_success_at: Option<SystemTime>,
    /// The last successful login's address (4 or 16 bytes).
    pub last_success_ip: Option<Vec<u8>>,
    /// Last write.
    #[serde(with = "crate::rec::micros")]
    pub updated_at: SystemTime,
}

/// A temporary lock as the cache keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempLock {
    /// The user.
    pub user_id: UserId,
    /// When it ends (9999-12-31: at `ACCOUNT UNLOCK`).
    pub locked_until: SystemTime,
    /// Failures in a row that caused it.
    pub consecutive_failures: u32,
}

/// Typed access to the auth tables of one tenant's catalog records. Every
/// method takes the record transaction to run in: an auth change's
/// ([`AuthTx`](sprig_catalog::AuthTx)`.tx().records()`) for versioned
/// writes, a snapshot's for cache loads, or a plain record transaction for
/// the unversioned login-state writes.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthStore;

/// The primary key of user `id`.
fn user_pk(id: UserId) -> Pk {
    Pk::new().u32(id.0)
}

/// The primary key of a verifier.
fn verifier_pk(user: UserId, id: u64) -> Pk {
    Pk::new().u32(user.0).u64(id)
}

/// A row with every time truncated to microseconds, as MariaDB stored it.
fn stored_user(r: &UserRow) -> UserRow {
    let mut r = r.clone();
    r.created_at = crate::rec::to_micros(r.created_at);
    r.password_changed_at = r.password_changed_at.map(crate::rec::to_micros);
    r
}

impl AuthStore {
    /// The auth tables.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    // ---- users ----

    /// Reserves the next user id (counter `user`). Inside a change's
    /// transaction the reservation commits or rolls back with the change,
    /// so a failed create leaves no gap; ids of users that existed
    /// (dropped ones included) are never handed out again.
    ///
    /// # Errors
    /// The store's; `Internal` when ids are exhausted.
    pub async fn next_user_id(&self, tx: &mut dyn RecordTx) -> Result<UserId> {
        let id = records::next_ids(tx, USER_COUNTER, 1).await?;
        u32::try_from(id)
            .map(UserId)
            .map_err(|_| SprigError::internal("user ids exhausted"))
    }

    /// Whether a live user other than `except` holds account
    /// `name`@`host`.
    async fn live_holder(
        &self,
        tx: &mut dyn RecordTx,
        name: &str,
        host: &str,
        except: UserId,
    ) -> Result<bool> {
        Ok(self.users_since(tx, None).await?.iter().any(|u| {
            u.user_id != except && u.is_live() && u.name == name && host_eq(&u.host, host)
        }))
    }

    /// Inserts a user. A live `(name, host)` already in use is a duplicate
    /// key (1062, as MariaDB's unique key reported it).
    ///
    /// # Errors
    /// 1062; the store's.
    pub async fn insert_user(&self, tx: &mut dyn RecordTx, r: &UserRow) -> Result<()> {
        if USERS
            .get::<UserRow>(tx, &user_pk(r.user_id))
            .await?
            .is_some()
        {
            return Err(duplicate("user id"));
        }
        if r.is_live() && self.live_holder(tx, &r.name, &r.host, r.user_id).await? {
            return Err(duplicate("account"));
        }
        USERS.put(tx, &user_pk(r.user_id), &stored_user(r)).await
    }

    /// Rewrites every column of the user `r.user_id`. Returns whether the
    /// row exists (nothing is written when it doesn't).
    ///
    /// # Errors
    /// 1062 for a live name in use by another user; the store's.
    pub async fn update_user(&self, tx: &mut dyn RecordTx, r: &UserRow) -> Result<bool> {
        if USERS
            .get::<UserRow>(tx, &user_pk(r.user_id))
            .await?
            .is_none()
        {
            return Ok(false);
        }
        if r.is_live() && self.live_holder(tx, &r.name, &r.host, r.user_id).await? {
            return Err(duplicate("account"));
        }
        USERS.put(tx, &user_pk(r.user_id), &stored_user(r)).await?;
        Ok(true)
    }

    /// Sets the user's `changed_version` to `version`, so the cache
    /// reloads it and its verifiers. Returns whether the user exists.
    ///
    /// # Errors
    /// The store's.
    pub async fn touch_user(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        version: u64,
    ) -> Result<bool> {
        match self.user(tx, user).await? {
            Some(mut u) => {
                u.changed_version = version;
                USERS.put(tx, &user_pk(user), &u).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The user `id`, live or dropped.
    ///
    /// # Errors
    /// The store's; `Internal` for a malformed row.
    pub async fn user(&self, tx: &mut dyn RecordTx, id: UserId) -> Result<Option<UserRow>> {
        USERS.get(tx, &user_pk(id)).await
    }

    /// The live account `'name'@'host'` (host compared as MySQL does,
    /// case-insensitively).
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn user_by_account(
        &self,
        tx: &mut dyn RecordTx,
        name: &str,
        host: &str,
    ) -> Result<Option<UserRow>> {
        Ok(self
            .users_since(tx, None)
            .await?
            .into_iter()
            .find(|u| u.is_live() && u.name == name && host_eq(&u.host, host)))
    }

    /// Users changed after `since` (every user, dropped ones included, for
    /// `None`), by id.
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn users_since(
        &self,
        tx: &mut dyn RecordTx,
        since: Option<u64>,
    ) -> Result<Vec<UserRow>> {
        let all: Vec<UserRow> = USERS.rows(tx, &Pk::new()).await?;
        Ok(match since {
            Some(v) => all.into_iter().filter(|u| u.changed_version > v).collect(),
            None => all,
        })
    }

    // ---- verifiers ----

    /// Inserts a verifier (its id from counter `verifier`) and returns its
    /// id.
    ///
    /// # Errors
    /// The store's.
    pub async fn insert_verifier(&self, tx: &mut dyn RecordTx, r: &VerifierRow) -> Result<u64> {
        let id = records::next_ids(tx, "verifier", 1).await?;
        let mut row = r.clone();
        row.verifier_id = id;
        row.created_at = crate::rec::to_micros(row.created_at);
        VERIFIERS.put(tx, &verifier_pk(r.user_id, id), &row).await?;
        Ok(id)
    }

    /// The verifiers of one user, by id.
    async fn verifiers_of_one(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
    ) -> Result<Vec<VerifierRow>> {
        VERIFIERS.rows(tx, &Pk::new().u32(user.0)).await
    }

    /// The verifiers of `users`, by user, slot and id.
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn verifiers_of(
        &self,
        tx: &mut dyn RecordTx,
        users: &[UserId],
    ) -> Result<Vec<VerifierRow>> {
        let mut ids = users.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut out = Vec::new();
        for u in ids {
            out.extend(self.verifiers_of_one(tx, u).await?);
        }
        sort_verifiers(&mut out);
        Ok(out)
    }

    /// Every verifier of every user (a full cache load).
    ///
    /// # Errors
    /// As [`user`](Self::user).
    pub async fn all_verifiers(&self, tx: &mut dyn RecordTx) -> Result<Vec<VerifierRow>> {
        let mut out: Vec<VerifierRow> = VERIFIERS.rows(tx, &Pk::new()).await?;
        sort_verifiers(&mut out);
        Ok(out)
    }

    /// Deletes the user's verifiers in `slot`. Returns how many.
    ///
    /// # Errors
    /// The store's.
    pub async fn delete_verifiers(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        slot: Slot,
    ) -> Result<u64> {
        let mut n = 0;
        for v in self.verifiers_of_one(tx, user).await? {
            if v.slot == slot {
                VERIFIERS
                    .delete(tx, &verifier_pk(user, v.verifier_id))
                    .await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Moves the user's verifiers from slot `from` to `to`. Returns how
    /// many.
    ///
    /// # Errors
    /// The store's.
    pub async fn move_verifiers(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        from: Slot,
        to: Slot,
    ) -> Result<u64> {
        let mut n = 0;
        for mut v in self.verifiers_of_one(tx, user).await? {
            if v.slot == from {
                v.slot = to;
                VERIFIERS
                    .put(tx, &verifier_pk(user, v.verifier_id), &v)
                    .await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Deletes the user's history verifiers beyond the newest `keep`
    /// generations. Returns how many.
    ///
    /// # Errors
    /// The store's.
    pub async fn trim_history(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        keep: u64,
    ) -> Result<u64> {
        let history: Vec<VerifierRow> = self
            .verifiers_of_one(tx, user)
            .await?
            .into_iter()
            .filter(|v| v.slot == Slot::History)
            .collect();
        let mut gens: Vec<u64> = history.iter().map(|v| v.generation).collect();
        gens.sort_unstable_by(|a, b| b.cmp(a));
        gens.dedup();
        let Some(&cut) = usize::try_from(keep).ok().and_then(|k| gens.get(k)) else {
            return Ok(0);
        };
        let mut n = 0;
        for v in history {
            if v.generation <= cut {
                VERIFIERS
                    .delete(tx, &verifier_pk(user, v.verifier_id))
                    .await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// The user's newest verifier generation, 0 if none.
    ///
    /// # Errors
    /// The store's.
    pub async fn max_generation(&self, tx: &mut dyn RecordTx, user: UserId) -> Result<u64> {
        Ok(self
            .verifiers_of_one(tx, user)
            .await?
            .iter()
            .map(|v| v.generation)
            .max()
            .unwrap_or(0))
    }

    // ---- login_state ----

    /// The user's login state, if any was written.
    ///
    /// # Errors
    /// The store's; `Internal` for a malformed row.
    pub async fn login_state(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
    ) -> Result<Option<LoginStateRow>> {
        LOGIN_STATE.get(tx, &user_pk(user)).await
    }

    /// The user's login state, locked for update in the caller's
    /// transaction (the failure counter's read): its latest value.
    ///
    /// # Errors
    /// As [`login_state`](Self::login_state); 39071 when the lock wait
    /// times out.
    pub async fn login_state_for_update(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
    ) -> Result<Option<LoginStateRow>> {
        LOGIN_STATE.get_for_update(tx, &user_pk(user)).await
    }

    /// Clears the user's temporary lock and failure count (`ACCOUNT
    /// UNLOCK`, or an expired lock). Returns whether a row changed.
    ///
    /// # Errors
    /// As [`login_state_for_update`](Self::login_state_for_update).
    pub async fn clear_login_lock(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        now: SystemTime,
    ) -> Result<bool> {
        let Some(mut r) = self.login_state_for_update(tx, user).await? else {
            return Ok(false);
        };
        if r.locked_until.is_none() && r.consecutive_failures == 0 {
            return Ok(false);
        }
        r.locked_until = None;
        r.consecutive_failures = 0;
        r.updated_at = now;
        self.put_login_state(tx, &r).await?;
        Ok(true)
    }

    /// Resets a non-zero failure count of an unlocked user (a successful
    /// login). Returns whether a row changed.
    ///
    /// # Errors
    /// As [`login_state_for_update`](Self::login_state_for_update).
    pub async fn reset_failures(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        now: SystemTime,
    ) -> Result<bool> {
        // Most logins find nothing to reset: a plain read first, so they
        // take no lock.
        match self.login_state(tx, user).await? {
            Some(r) if r.consecutive_failures > 0 && r.locked_until.is_none() => {}
            _ => return Ok(false),
        }
        let Some(mut r) = self.login_state_for_update(tx, user).await? else {
            return Ok(false);
        };
        if r.consecutive_failures == 0 || r.locked_until.is_some() {
            return Ok(false);
        }
        r.consecutive_failures = 0;
        r.updated_at = now;
        self.put_login_state(tx, &r).await?;
        Ok(true)
    }

    /// Writes a login state row (the lockout counters use it; tests too).
    ///
    /// # Errors
    /// The store's.
    pub async fn put_login_state(&self, tx: &mut dyn RecordTx, r: &LoginStateRow) -> Result<()> {
        let mut row = r.clone();
        row.locked_until = row.locked_until.map(crate::rec::to_micros);
        row.last_failure_at = row.last_failure_at.map(crate::rec::to_micros);
        row.last_success_at = row.last_success_at.map(crate::rec::to_micros);
        row.updated_at = crate::rec::to_micros(row.updated_at);
        LOGIN_STATE.put(tx, &user_pk(r.user_id), &row).await
    }

    /// Every user with a temporary lock set (expired ones included: the
    /// login unlocks them).
    ///
    /// # Errors
    /// As [`login_state`](Self::login_state).
    pub async fn temp_locks(&self, tx: &mut dyn RecordTx) -> Result<Vec<TempLock>> {
        let rows: Vec<LoginStateRow> = LOGIN_STATE.rows(tx, &Pk::new()).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.locked_until.map(|locked_until| TempLock {
                    user_id: r.user_id,
                    locked_until,
                    consecutive_failures: r.consecutive_failures,
                })
            })
            .collect())
    }
}

/// MariaDB's order of verifier reads: by user, slot (in the column's
/// order: primary, secondary, history) and id.
fn sort_verifiers(v: &mut [VerifierRow]) {
    let slot = |s: Slot| Slot::ALL.iter().position(|x| *x == s).unwrap_or(usize::MAX);
    v.sort_by_key(|r| (r.user_id, slot(r.slot), r.verifier_id));
}

/// Microseconds since the epoch; times before it clamp to 0.
pub(crate) fn micros(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// The time `m` microseconds after the epoch.
pub(crate) fn from_micros(m: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(m).unwrap_or(0))
}

/// Maps a driver error the way `sprig-catalog` does: lock waits and
/// deadlocks are 39071, other server errors pass through with their number
/// (the driver's text, which names the physical schema, stays in the
/// source), anything else is 39070.
pub(crate) fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

#[cfg(test)]
mod tests;
