//! The auth tables as catalog records in the storage engine
//! ([`sprig_catalog::records`]): one record per row under
//! `m/auth/<table>/<primary key>`, the row's JSON as its value.
//!
//! What MariaDB enforced is kept in code:
//! - **Unique keys** (an account's live name and host, a role's name, a
//!   privilege's target, a policy's name, an issuer's URL) are checked by a
//!   scan inside the change. Every write of those tables runs in an auth
//!   change, which holds the auth version record, so no two checks race.
//!   A duplicate is reported as MariaDB's 1062 was ([`duplicate`]), so the
//!   callers' handling stays as it was.
//! - **Ids** (auto-increment columns, `MAX(id) + 1`, the user counter) come
//!   from counters (`ctr/<table>`), read for update in the same
//!   transaction: never reused.
//! - **Times** are stored as microseconds since the epoch, as the
//!   `DATETIME(6)` columns held them, so a row reads back exactly as it
//!   did from MariaDB.

use std::time::SystemTime;

use sprig_catalog::records::RowTable;
use sprig_common::SprigError;

/// The module of every auth table in the catalog records.
pub const MODULE: &str = "auth";

/// `users`, by user id.
pub const USERS: RowTable = RowTable::new(MODULE, "users");
/// `verifiers`, by user id and verifier id.
pub const VERIFIERS: RowTable = RowTable::new(MODULE, "verifiers");
/// `login_state`, by user id.
pub const LOGIN_STATE: RowTable = RowTable::new(MODULE, "login_state");
/// `api_keys`, by key id.
pub const API_KEYS: RowTable = RowTable::new(MODULE, "api_keys");
/// `token_sessions`, by session id.
pub const TOKEN_SESSIONS: RowTable = RowTable::new(MODULE, "token_sessions");
/// `user_factors`, by user id and factor id.
pub const USER_FACTORS: RowTable = RowTable::new(MODULE, "user_factors");
/// `oidc_issuers`, by issuer id.
pub const OIDC_ISSUERS: RowTable = RowTable::new(MODULE, "oidc_issuers");
/// `claim_mappings`, by issuer id and mapping id.
pub const CLAIM_MAPPINGS: RowTable = RowTable::new(MODULE, "claim_mappings");
/// `roles`, by role id.
pub const ROLES: RowTable = RowTable::new(MODULE, "roles");
/// `role_edges`, by grantee kind, grantee id and role id.
pub const ROLE_EDGES: RowTable = RowTable::new(MODULE, "role_edges");
/// `privileges`, by privilege id.
pub const PRIVILEGES: RowTable = RowTable::new(MODULE, "privileges");
/// `row_policies`, by policy id.
pub const ROW_POLICIES: RowTable = RowTable::new(MODULE, "row_policies");
/// `row_security`, by object id.
pub const ROW_SECURITY: RowTable = RowTable::new(MODULE, "row_security");

/// MariaDB's duplicate-key error (1062), as the catalog's SQL reported it:
/// callers that mapped it to a client error (1396 and others) keep doing so.
#[must_use]
pub fn duplicate(what: &str) -> SprigError {
    SprigError::backend(
        1062,
        *b"23000",
        format!("catalog statement failed (1062): duplicate {what}"),
    )
}

/// Whether `e` is a duplicate key ([`duplicate`]).
#[must_use]
pub fn is_duplicate(e: &SprigError) -> bool {
    matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1062, .. }
            | sprig_common::ErrorKind::Standard { errno: 1062, .. }
    )
}

/// Host patterns compare as MySQL compares them (ASCII, without case).
#[must_use]
pub fn host_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// The time truncated to microseconds, as a `DATETIME(6)` column keeps it.
#[must_use]
pub fn to_micros(t: SystemTime) -> SystemTime {
    crate::store::from_micros(crate::store::micros(t))
}

/// Serde for a time stored as microseconds since the epoch.
pub mod micros {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer};

    /// Writes `t` as microseconds.
    ///
    /// # Errors
    /// The serializer's.
    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(crate::store::micros(*t))
    }

    /// Reads microseconds.
    ///
    /// # Errors
    /// The deserializer's.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        Ok(crate::store::from_micros(i64::deserialize(d)?))
    }
}

/// Serde for an optional time stored as microseconds since the epoch.
pub mod micros_opt {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer};

    /// Writes `t` as microseconds, or null.
    ///
    /// # Errors
    /// The serializer's.
    #[allow(clippy::ref_option)]
    pub fn serialize<S: Serializer>(t: &Option<SystemTime>, s: S) -> Result<S::Ok, S::Error> {
        match t {
            Some(t) => s.serialize_some(&crate::store::micros(*t)),
            None => s.serialize_none(),
        }
    }

    /// Reads microseconds or null.
    ///
    /// # Errors
    /// The deserializer's.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SystemTime>, D::Error> {
        Ok(Option::<i64>::deserialize(d)?.map(crate::store::from_micros))
    }
}
