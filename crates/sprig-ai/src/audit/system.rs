//! The administration assistant's conversations: the rules its store keeps.
//! The store itself is [`super::rec::RecAudit::system`], in the DC's system
//! catalog records (tenant id 0, module `ai_admin`).
//!
//! - **The same records** as a tenant's `ai` module, except that a row
//!   belongs to an operator's login (`tenant.user`, lower case,
//!   [`login_key`]) instead of a tenant user id, and `role` is `admin` or
//!   `triage` ([`system_role`]).
//! - **Whose.** A system store is opened for one operator's login; it
//!   lists, reads and rates only that operator's conversations. The
//!   `UserId` arguments of [`AuditStore`](super::AuditStore) are ignored.
//! - **Usage and budgets.** `usage_today` counts every operator's tokens
//!   today as the "tenant" total and the operator's own as the user's, so
//!   the global `ai.budget.tenant_tokens_day` caps all administration turns
//!   of the DC and `ai.budget.user_tokens_day` each operator.
//! - **Retention.** The controller drops payloads older than the global
//!   `ai.audit_payload_days` every hour, keeping their hashes, as for the
//!   tenants.

use sprig_common::{Result, SprigError};

use crate::role::Role;

/// The longest operator login kept.
pub const MAX_LOGIN: usize = 200;

/// `Ok` for the roles kept here: `admin` and `triage`.
pub(crate) fn system_role(role: Role) -> Result<&'static str> {
    if role.is_tenant() {
        Err(SprigError::internal(
            "tenant assistants keep their conversations in the tenant's catalog",
        ))
    } else {
        Ok(role.as_str())
    }
}

/// An operator's login as it is kept: trimmed, lower case, at most
/// [`MAX_LOGIN`] characters.
#[must_use]
pub fn login_key(login: &str) -> String {
    login
        .trim()
        .to_lowercase()
        .chars()
        .take(MAX_LOGIN)
        .collect()
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::UserId;

    use super::*;
    use crate::audit::AuditStore as _;

    #[test]
    fn logins_and_roles() {
        assert!(system_role(Role::Admin).is_ok() && system_role(Role::Triage).is_ok());
        assert!(system_role(Role::Data).is_err() && system_role(Role::Dba).is_err());
        assert_eq!(login_key("  Acme.Alice "), "acme.alice");
        assert_eq!(login_key(&"x".repeat(300)).len(), MAX_LOGIN);
    }

    #[tokio::test]
    async fn the_memory_store_keeps_admin_conversations_only() {
        let m = crate::MemoryAudit::system();
        assert!(m.create(UserId(1), Role::Data, "x").await.is_err());
        let c = m.create(UserId(1), Role::Admin, "disk").await.unwrap();
        assert_eq!(
            m.get(UserId(1), c).await.unwrap().unwrap().role,
            Role::Admin
        );
        assert!(m.get(UserId(2), c).await.unwrap().is_none());
        let t = crate::MemoryAudit::default();
        assert!(t.create(UserId(1), Role::Admin, "x").await.is_err());
    }
}
