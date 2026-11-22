//! The `authz` module in the gateway's catalog cache: [`AuthzState`], the
//! tenant's roles, role edges and privileges at one auth version, loaded
//! into every snapshot by [`AuthzExtension`] in the snapshot's own
//! transaction.
//!
//! The tables are small (a tenant has users and roles, not rows), so every
//! load reads them whole from the snapshot's catalog records; the cache
//! reloads a tenant when its auth version moves, which every gateway learns
//! from the directory watch (the same path settings changes take). The
//! rows are engine records, so every load is of a migrated module at its
//! newest format; [`AuthzState::unmigrated`] (every user holds everything)
//! remains for snapshots without the module.

use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use sprig_catalog::{
    CatalogExtension, ExtState, Migration, RecordTx, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::Result;
use sprig_common::ids::{RoleId, UserId};

use super::store::{
    AUTHZ_MIGRATIONS, AuthzRows, AuthzStore, EdgeRow, Grantee, MODULE, PrivRow, RoleRow,
};

/// One tenant's authorization rows at one auth version, indexed.
#[derive(Debug, Clone, Default)]
pub struct AuthzState {
    migrated: bool,
    /// The module's format in the catalog (0: unknown or none).
    format: u32,
    rows: Arc<AuthzRows>,
    privs: HashMap<Grantee, Vec<usize>>,
    edges: HashMap<Grantee, Vec<EdgeRow>>,
    roles: HashMap<RoleId, usize>,
    role_names: HashMap<(String, String), RoleId>,
}

impl ExtState for AuthzState {
    fn bytes(&self) -> usize {
        256 + self.rows.privileges.len() * 128
            + self.rows.edges.len() * 48
            + self.rows.roles.len() * 96
    }
}

impl AuthzState {
    /// The state of a catalog without the module: every user holds
    /// everything.
    #[must_use]
    pub fn unmigrated() -> Self {
        Self::default()
    }

    /// The state of `rows`.
    #[must_use]
    pub fn new(rows: AuthzRows) -> Self {
        let mut privs: HashMap<Grantee, Vec<usize>> = HashMap::new();
        for (i, p) in rows.privileges.iter().enumerate() {
            privs.entry(p.grantee).or_default().push(i);
        }
        let mut edges: HashMap<Grantee, Vec<EdgeRow>> = HashMap::new();
        for e in &rows.edges {
            edges.entry(e.grantee).or_default().push(*e);
        }
        let roles = rows
            .roles
            .iter()
            .enumerate()
            .map(|(i, r)| (r.role_id, i))
            .collect();
        let role_names = rows
            .roles
            .iter()
            .map(|r| ((r.name.clone(), r.host.to_ascii_lowercase()), r.role_id))
            .collect();
        Self {
            migrated: true,
            format: super::store::AUTHZ_FORMAT,
            rows: Arc::new(rows),
            privs,
            edges,
            roles,
            role_names,
        }
    }

    /// The tenant's state in `catalog`; `None` if the module isn't
    /// registered with the cache.
    #[must_use]
    pub fn of(catalog: &TenantCatalog) -> Option<&AuthzState> {
        catalog.ext.get::<AuthzState>(MODULE)
    }

    /// Whether the catalog has the module's tables.
    #[must_use]
    pub fn is_migrated(&self) -> bool {
        self.migrated
    }

    /// Whether the catalog is at the module's latest format (always, for
    /// a loaded state: the rows start at it).
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.migrated && self.format >= super::store::AUTHZ_FORMAT
    }

    /// The same state with the catalog's format.
    #[must_use]
    pub fn with_format(mut self, format: u32) -> Self {
        self.format = format;
        self
    }

    /// All rows.
    #[must_use]
    pub fn rows(&self) -> &AuthzRows {
        &self.rows
    }

    /// The privilege rows `grantee` holds directly.
    pub fn privileges_of(&self, grantee: Grantee) -> impl Iterator<Item = &PrivRow> {
        self.privs
            .get(&grantee)
            .into_iter()
            .flatten()
            .map(|i| &self.rows.privileges[*i])
    }

    /// The roles `grantee` holds directly.
    #[must_use]
    pub fn edges_of(&self, grantee: Grantee) -> &[EdgeRow] {
        self.edges.get(&grantee).map_or(&[], Vec::as_slice)
    }

    /// The role `id`.
    #[must_use]
    pub fn role(&self, id: RoleId) -> Option<&RoleRow> {
        self.roles.get(&id).map(|i| &self.rows.roles[*i])
    }

    /// The role `'name'@'host'` (host compared case-insensitively).
    #[must_use]
    pub fn role_named(&self, name: &str, host: &str) -> Option<RoleId> {
        self.role_names
            .get(&(name.to_owned(), host.to_ascii_lowercase()))
            .copied()
    }

    /// `roles` and every role they contain, breadth first, each once.
    #[must_use]
    pub fn closure(&self, roles: &[RoleId]) -> Vec<RoleId> {
        let mut seen: HashSet<RoleId> = HashSet::new();
        let mut out = Vec::new();
        let mut q: VecDeque<RoleId> = roles.iter().copied().collect();
        while let Some(r) = q.pop_front() {
            if !seen.insert(r) || self.role(r).is_none() {
                continue;
            }
            out.push(r);
            for e in self.edges_of(Grantee::Role(r)) {
                q.push_back(e.role);
            }
        }
        out
    }

    /// The roles granted to `user` directly.
    #[must_use]
    pub fn granted_roles(&self, user: UserId) -> Vec<RoleId> {
        self.edges_of(Grantee::User(user))
            .iter()
            .filter(|e| self.role(e.role).is_some())
            .map(|e| e.role)
            .collect()
    }

    /// `user`'s default roles.
    #[must_use]
    pub fn default_roles(&self, user: UserId) -> Vec<RoleId> {
        self.edges_of(Grantee::User(user))
            .iter()
            .filter(|e| e.is_default && self.role(e.role).is_some())
            .map(|e| e.role)
            .collect()
    }

    /// Whether granting `role` to role `to` would close a cycle (`to` is
    /// `role` or contained in it).
    #[must_use]
    pub fn would_cycle(&self, role: RoleId, to: RoleId) -> bool {
        role == to || self.closure(&[role]).contains(&to)
    }
}

/// `sprig-auth`'s authorization catalog module: the tables' migrations, and
/// [`AuthzState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthzExtension;

#[async_trait::async_trait]
impl CatalogExtension for AuthzExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        AUTHZ_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Auth)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        Ok(Arc::new(load_state(tx.records()?).await?))
    }
}

/// The tenant's [`AuthzState`] from its catalog records: every role, role
/// edge and privilege, at the module's format.
///
/// # Errors
/// The store's, or `Internal` for a malformed row.
pub async fn load_state(tx: &mut dyn RecordTx) -> Result<AuthzState> {
    let store = AuthzStore::new();
    let rows = store.load(tx).await?;
    Ok(AuthzState::new(rows).with_format(store.format()))
}

/// `prev` as an [`AuthzState`], for tests.
#[must_use]
pub fn downcast(prev: &Arc<dyn ExtState>) -> Option<&AuthzState> {
    let any: &dyn Any = &**prev;
    any.downcast_ref::<AuthzState>()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_catalog::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    use super::super::privilege::{Action, ActionSet};
    use super::super::store::PrivTarget;
    use super::*;

    #[tokio::test]
    async fn every_load_is_migrated_and_current() {
        let recs = MemRecords::new();
        let mut tx = recs.begin(TenantId(42)).await.unwrap();
        let empty = load_state(&mut *tx).await.unwrap();
        assert!(empty.is_migrated() && empty.is_current());
        assert!(empty.rows().privileges.is_empty());
        let store = AuthzStore::new();
        let alice = Grantee::User(UserId(7));
        let sel = ActionSet::of(&[Action::Select]);
        store
            .grant(&mut *tx, alice, &PrivTarget::Tenant, sel, None)
            .await
            .unwrap();
        let r = store
            .create_role(&mut *tx, "reader", "%", None)
            .await
            .unwrap()
            .unwrap();
        store
            .grant_role(&mut *tx, alice, r, false, None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let mut tx = recs.begin(TenantId(42)).await.unwrap();
        let st = load_state(&mut *tx).await.unwrap();
        assert!(st.is_current());
        assert_eq!(st.privileges_of(alice).count(), 1);
        assert_eq!(st.granted_roles(UserId(7)), vec![r]);
        assert_eq!(st.role_named("reader", "%"), Some(r));
        // Another tenant's records are its own.
        let mut other = recs.begin(TenantId(43)).await.unwrap();
        assert!(
            load_state(&mut *other)
                .await
                .unwrap()
                .rows()
                .roles
                .is_empty()
        );
    }
}
