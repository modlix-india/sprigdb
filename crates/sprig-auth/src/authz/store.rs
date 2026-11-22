//! The authorization catalog tables: `roles`, `role_edges` and
//! `privileges`, as catalog records in the storage engine
//! ([`crate::rec`]), with typed rows and access.
//!
//! The `authz` module stays registered under its own name, but has no
//! migrations: its rows are engine records, which start at the newest
//! shape ([`AUTHZ_FORMAT`]). `row_policies` and `foreign_grants` arrive
//! with the features that use them.
//!
//! **Rows.** Unused key parts are `0` or `''`, never `NULL`. Grants on a
//! database keep its *name* in `name_pattern` (`db_id` 0): MySQL keeps
//! database grants by name, so a grant on a database that doesn't exist
//! yet works, and one survives `DROP DATABASE` and `CREATE DATABASE`.
//! Object and column grants keep `db_id` and `object_id`: MySQL refuses
//! them on a missing table (1146), and they go with the object.
//!
//! **Keys.** `roles/<role_id>`, `role_edges/<grantee kind>/<grantee
//! id>/<role_id>` (the kind as its old `ENUM` index, so a scan returns
//! edges in the old `ORDER BY grantee_kind, grantee_id, role_id`) and
//! `privileges/<priv_id>`. The unique keys the tables had (a role's kind,
//! name, host and database; a privilege's full target) are checked by a
//! scan inside the auth change; ids come from the counters `roles` and
//! `privileges`.
//!
//! **Versioning.** Every change commits with a new auth version
//! ([`VersionKind::Auth`](sprig_catalog::VersionKind)); the gateway's
//! [`AuthzExtension`](super::state::AuthzExtension) reloads all three
//! tables (they are small) at each version.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{self, Pk, RecordTx};
use sprig_catalog::{Migration, Module};
use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};
use sprig_common::{Result, SprigError};

use super::privilege::{ALL_TENANT, Action, ActionSet, Level};
use crate::rec::{PRIVILEGES, ROLE_EDGES, ROLES, host_eq};

/// The module name in the catalog's format records.
pub const MODULE: &str = "authz";

/// What every user that existed before this module holds: `ALL` and
/// `SPRIG_TENANT_ADMIN` on `*.*` `WITH GRANT OPTION`.
pub const LEGACY_ACTIONS: ActionSet = ALL_TENANT
    .with(Action::TenantAdmin)
    .with(Action::GrantOption);

/// The module's latest format: the shape every row has.
pub const AUTHZ_FORMAT: u32 = 2;

/// The module's migrations: none (its rows are engine records).
pub static AUTHZ_MIGRATIONS: &[Migration] = &[];

/// The `authz` module, for [`Migrator::new`](sprig_catalog::Migrator::new).
pub const AUTHZ_MODULE: Module = Module {
    name: MODULE,
    migrations: AUTHZ_MIGRATIONS,
};

/// The id counter of roles.
pub const ROLE_COUNTER: &str = "roles";

/// The id counter of privileges.
pub const PRIV_COUNTER: &str = "privileges";

/// `roles.kind` of the roles this module creates and loads.
const SQL_ROLE: &str = "sql";

/// Who holds a privilege or a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Grantee {
    /// A user of this tenant.
    User(UserId),
    /// A role of this tenant.
    Role(RoleId),
}

/// `grantee_kind` as stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum GranteeKind {
    User,
    Role,
}

impl GranteeKind {
    /// The key byte: the old `ENUM('user','role')` index.
    const fn byte(self) -> u8 {
        match self {
            GranteeKind::User => 1,
            GranteeKind::Role => 2,
        }
    }
}

impl Grantee {
    fn kind(self) -> GranteeKind {
        match self {
            Grantee::User(_) => GranteeKind::User,
            Grantee::Role(_) => GranteeKind::Role,
        }
    }

    fn id(self) -> u32 {
        match self {
            Grantee::User(u) => u.0,
            Grantee::Role(r) => r.0,
        }
    }

    fn from_parts(kind: GranteeKind, id: u32) -> Self {
        match kind {
            GranteeKind::User => Grantee::User(UserId(id)),
            GranteeKind::Role => Grantee::Role(RoleId(id)),
        }
    }
}

/// Where a privilege row points (the supported levels).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrivTarget {
    /// `*.*`.
    Tenant,
    /// `db.*`, by the database's name key.
    Database(String),
    /// `db.obj`.
    Object {
        /// The database.
        db: DbId,
        /// The table, view or routine.
        object: ObjectId,
    },
    /// `db.t (c)`.
    Column {
        /// The database.
        db: DbId,
        /// The table or view.
        object: ObjectId,
        /// The column name, lower case (MySQL compares column names
        /// case-insensitively).
        column: String,
    },
    /// A MySQL partial revoke (`auth.partial_revokes`): the actions
    /// a tenant-level (`*.*`) grant of the same grantee doesn't give on
    /// this database, by its name key. A `database` row with `restriction
    /// = 1`.
    Restriction(String),
    /// `db.t` or `db.t (c)` held by name: the grant of a table or view that
    /// was dropped with `auth.grants_on_drop = keep`. It applies to the
    /// relation with that name whenever one exists (see the module docs).
    Named {
        /// The database's name key.
        db_key: String,
        /// The relation's name key.
        name_key: String,
        /// The column, lower case; `None` for the table itself.
        column: Option<String>,
    },
    /// One label (`NODES L`) or relationship type (`RELATIONSHIPS T`) of a
    /// graph: a `graph_element` row with the graph in
    /// `object_id`, bit 31 of `element` set for relationship types, and
    /// the label or type's name in `name_pattern` (by name,
    /// so a grant may name a label no node carries yet, as in Neo4j).
    GraphElement {
        /// The database.
        db: DbId,
        /// The graph.
        graph: ObjectId,
        /// A relationship type (else a label).
        rels: bool,
        /// The label or type, case-sensitive.
        name: String,
    },
}

/// `name_pattern` of a [`PrivTarget::Named`] row: `` `db`.`t` ``.
#[must_use]
pub fn named_pattern(db_key: &str, name_key: &str) -> String {
    format!(
        "`{}`.`{}`",
        db_key.replace('`', "``"),
        name_key.replace('`', "``")
    )
}

/// The database and relation keys of a `name_pattern` written by
/// [`named_pattern`].
#[must_use]
pub fn parse_named(pattern: &str) -> Option<(String, String)> {
    fn ident(s: &str) -> Option<(String, &str)> {
        let mut rest = s.strip_prefix('`')?;
        let mut out = String::new();
        loop {
            let i = rest.find('`')?;
            out.push_str(&rest[..i]);
            rest = &rest[i + 1..];
            match rest.strip_prefix('`') {
                Some(r) => {
                    out.push('`');
                    rest = r;
                }
                None => return Some((out, rest)),
            }
        }
    }
    let (db, rest) = ident(pattern)?;
    let (name, rest) = ident(rest.strip_prefix('.')?)?;
    rest.is_empty().then_some((db, name))
}

impl PrivTarget {
    /// The level.
    #[must_use]
    pub fn level(&self) -> Level {
        match self {
            PrivTarget::Tenant => Level::Tenant,
            PrivTarget::Database(_) | PrivTarget::Restriction(_) => Level::Database,
            PrivTarget::Object { .. } | PrivTarget::Named { column: None, .. } => Level::Object,
            PrivTarget::Column { .. }
            | PrivTarget::Named {
                column: Some(_), ..
            } => Level::Column,
            PrivTarget::GraphElement { .. } => Level::GraphElement,
        }
    }

    /// The `element` column: bit 31 for a relationship type.
    fn element(&self) -> u32 {
        match self {
            PrivTarget::GraphElement { rels: true, .. } => 1 << 31,
            _ => 0,
        }
    }

    /// The `restriction` column: 1 for a partial revoke.
    fn restriction(&self) -> u8 {
        u8::from(matches!(self, PrivTarget::Restriction(_)))
    }

    /// `(db_id, object_id, column_name, name_pattern)`.
    fn parts(&self) -> (u32, u64, String, String) {
        match self {
            PrivTarget::Tenant => (0, 0, String::new(), String::new()),
            PrivTarget::Database(n) | PrivTarget::Restriction(n) => {
                (0, 0, String::new(), n.clone())
            }
            PrivTarget::Object { db, object } => (db.0, object.0, String::new(), String::new()),
            PrivTarget::Column { db, object, column } => {
                (db.0, object.0, column.clone(), String::new())
            }
            PrivTarget::Named {
                db_key,
                name_key,
                column,
            } => (
                0,
                0,
                column.clone().unwrap_or_default(),
                named_pattern(db_key, name_key),
            ),
            PrivTarget::GraphElement {
                db, graph, name, ..
            } => (db.0, graph.0, String::new(), name.clone()),
        }
    }
}

/// A `privileges` row (the supported levels, this tenant's grantees).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivRow {
    /// The id.
    pub priv_id: u64,
    /// Who holds it.
    pub grantee: Grantee,
    /// Where.
    pub target: PrivTarget,
    /// What.
    pub actions: ActionSet,
}

/// A `roles` row of kind `sql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRow {
    /// The id; the backend role would be `t<tenant>_r<role_id>`.
    pub role_id: RoleId,
    /// The role name.
    pub name: String,
    /// The host part (MySQL roles are accounts).
    pub host: String,
}

/// A `role_edges` row: `grantee` holds `role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeRow {
    /// The user or role holding the role.
    pub grantee: Grantee,
    /// The role held.
    pub role: RoleId,
    /// `WITH ADMIN OPTION`.
    pub admin_option: bool,
    /// One of the grantee's default roles.
    pub is_default: bool,
}

/// Everything the module stores for one tenant (this tenant's grantees,
/// the supported levels and SQL roles).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzRows {
    /// SQL roles.
    pub roles: Vec<RoleRow>,
    /// Role grants.
    pub edges: Vec<EdgeRow>,
    /// Privileges.
    pub privileges: Vec<PrivRow>,
}

/// A `roles` row as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RoleRec {
    role_id: u32,
    /// `sql` (the only kind written so far).
    kind: String,
    /// Compared exactly.
    name: String,
    /// Compared as MySQL compares hosts (ASCII, without case).
    host: String,
    db_id: u32,
    members_hash: Option<u64>,
    created_by: Option<u32>,
    #[serde(with = "crate::rec::micros")]
    created_at: SystemTime,
    #[serde(with = "crate::rec::micros_opt")]
    last_used_at: Option<SystemTime>,
}

/// A `role_edges` row as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EdgeRec {
    /// Always 0 (this tenant's grantees); not part of the key.
    grantee_tenant_id: u64,
    grantee_kind: GranteeKind,
    grantee_id: u32,
    role_id: u32,
    admin_option: bool,
    is_default: bool,
    granted_by: Option<u32>,
    granted_by_tenant_id: u64,
    #[serde(with = "crate::rec::micros")]
    granted_at: SystemTime,
}

/// A `privileges` row as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PrivRec {
    priv_id: u64,
    grantee_tenant_id: u64,
    grantee_kind: GranteeKind,
    grantee_id: u32,
    /// The level's text ([`Level::as_str`]; other texts are skipped on
    /// load).
    level: String,
    db_id: u32,
    object_id: u64,
    column_name: String,
    kv_prefix: Vec<u8>,
    element: u32,
    name_pattern: String,
    actions: u64,
    restriction: u8,
    detail: Option<serde_json::Value>,
    granted_by: Option<u32>,
    granted_by_tenant_id: u64,
    #[serde(with = "crate::rec::micros")]
    granted_at: SystemTime,
}

/// A privilege's unique key (the old `uk_priv_target`): grantee tenant,
/// kind and id, level, database, object, column, key-value prefix,
/// element, name pattern, restriction. Every text part compares exactly,
/// as the catalog schema's binary collation did.
type PrivKey<'a> = (
    u64,
    GranteeKind,
    u32,
    &'a str,
    u32,
    u64,
    &'a str,
    &'a [u8],
    u32,
    &'a str,
    u8,
);

impl PrivRec {
    /// A new row of `grantee` at `target` (id 0 until allocated).
    fn new(
        grantee: Grantee,
        target: &PrivTarget,
        actions: ActionSet,
        granted_by: Option<UserId>,
    ) -> Self {
        let (db, object, column, pattern) = target.parts();
        Self {
            priv_id: 0,
            grantee_tenant_id: 0,
            grantee_kind: grantee.kind(),
            grantee_id: grantee.id(),
            level: target.level().as_str().to_owned(),
            db_id: db,
            object_id: object,
            column_name: column,
            kv_prefix: Vec::new(),
            element: target.element(),
            name_pattern: pattern,
            actions: actions.0,
            restriction: target.restriction(),
            detail: None,
            granted_by: granted_by.map(|u| u.0),
            granted_by_tenant_id: 0,
            granted_at: now_ms(),
        }
    }

    fn key(&self) -> PrivKey<'_> {
        (
            self.grantee_tenant_id,
            self.grantee_kind,
            self.grantee_id,
            &self.level,
            self.db_id,
            self.object_id,
            &self.column_name,
            &self.kv_prefix,
            self.element,
            &self.name_pattern,
            self.restriction,
        )
    }

    fn held_by(&self, grantee: Grantee) -> bool {
        self.grantee_tenant_id == 0
            && self.grantee_kind == grantee.kind()
            && self.grantee_id == grantee.id()
    }

    fn is_object_or_column(&self) -> bool {
        self.level == Level::Object.as_str() || self.level == Level::Column.as_str()
    }

    /// The column limits the old table enforced (strict mode: a longer
    /// value failed the statement).
    fn check_lengths(&self) -> Result<()> {
        check_chars("column_name", &self.column_name, 64)?;
        check_chars("name_pattern", &self.name_pattern, 255)
    }
}

/// The levels [`AuthzStore::load`] reads.
const LOADED_LEVELS: [Level; 5] = [
    Level::Tenant,
    Level::Database,
    Level::Object,
    Level::Column,
    Level::GraphElement,
];

/// The time now, truncated to milliseconds as a `DATETIME(3)` column kept
/// it.
fn now_ms() -> SystemTime {
    let m = crate::store::micros(SystemTime::now());
    crate::store::from_micros(m - m % 1000)
}

/// MariaDB's "data too long" (1406) under strict mode, which the old
/// tables' column limits raised.
pub(crate) fn too_long(column: &str) -> SprigError {
    SprigError::backend(
        1406,
        *b"22001",
        format!("catalog statement failed (1406): data too long for column {column}"),
    )
}

/// 1406 when `v` has more than `max` characters.
pub(crate) fn check_chars(column: &str, v: &str, max: usize) -> Result<()> {
    if v.chars().count() > max {
        Err(too_long(column))
    } else {
        Ok(())
    }
}

fn role_pk(id: u32) -> Pk {
    Pk::new().u32(id)
}

fn edge_pk(grantee: Grantee, role: RoleId) -> Pk {
    Pk::new()
        .u8(grantee.kind().byte())
        .u32(grantee.id())
        .u32(role.0)
}

fn priv_pk(id: u64) -> Pk {
    Pk::new().u64(id)
}

/// The module's tables in the tenant's catalog records. Every method takes
/// the record transaction to run in: an auth change's
/// ([`AuthTx`](sprig_catalog::AuthTx)`.tx().records()`) for writes, a
/// snapshot's for loads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthzStore;

impl AuthzStore {
    /// The tables.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Whether the module's tables exist: always (engine records need no
    /// migration).
    #[must_use]
    pub const fn migrated(&self) -> bool {
        true
    }

    /// The module's format: the newest, which every row has.
    #[must_use]
    pub const fn format(&self) -> u32 {
        AUTHZ_FORMAT
    }

    async fn roles(&self, tx: &mut dyn RecordTx) -> Result<Vec<RoleRec>> {
        ROLES.rows(tx, &Pk::new()).await
    }

    async fn privileges(&self, tx: &mut dyn RecordTx) -> Result<Vec<PrivRec>> {
        PRIVILEGES.rows(tx, &Pk::new()).await
    }

    /// Deletes every privilege row `which` picks.
    async fn delete_privileges(
        &self,
        tx: &mut dyn RecordTx,
        which: impl Fn(&PrivRec) -> bool + Send,
    ) -> Result<()> {
        for p in self.privileges(tx).await? {
            if which(&p) {
                PRIVILEGES.delete(tx, &priv_pk(p.priv_id)).await?;
            }
        }
        Ok(())
    }

    /// Writes `row` (by its unique key): an existing row gains its actions
    /// (and, with `regrant`, its grantor and time); else it is inserted
    /// with a new id.
    async fn upsert_privilege(
        &self,
        tx: &mut dyn RecordTx,
        mut row: PrivRec,
        regrant: bool,
    ) -> Result<()> {
        row.check_lengths()?;
        let existing = self
            .privileges(tx)
            .await?
            .into_iter()
            .find(|p| p.key() == row.key());
        if let Some(mut p) = existing {
            p.actions |= row.actions;
            if regrant {
                p.granted_by = row.granted_by;
                p.granted_at = row.granted_at;
            }
            return PRIVILEGES.put(tx, &priv_pk(p.priv_id), &p).await;
        }
        row.priv_id = records::next_ids(tx, PRIV_COUNTER, 1).await?;
        PRIVILEGES.put(tx, &priv_pk(row.priv_id), &row).await
    }

    /// Every row the gateway uses: SQL roles, this tenant's role edges and
    /// privileges at the supported levels. Rows this build doesn't know are
    /// skipped.
    ///
    /// # Errors
    /// The store's, or `Internal` for a malformed row.
    pub async fn load(&self, tx: &mut dyn RecordTx) -> Result<AuthzRows> {
        let roles = self
            .roles(tx)
            .await?
            .into_iter()
            .filter(|r| r.kind == SQL_ROLE)
            .map(|r| RoleRow {
                role_id: RoleId(r.role_id),
                name: r.name,
                host: r.host,
            })
            .collect();
        let edges = ROLE_EDGES
            .rows::<EdgeRec>(tx, &Pk::new())
            .await?
            .into_iter()
            .filter(|e| e.grantee_tenant_id == 0)
            .map(|e| EdgeRow {
                grantee: Grantee::from_parts(e.grantee_kind, e.grantee_id),
                role: RoleId(e.role_id),
                admin_option: e.admin_option,
                is_default: e.is_default,
            })
            .collect();
        let mut privileges = Vec::new();
        for r in self.privileges(tx).await? {
            let Some(level) = Level::parse(&r.level).filter(|l| LOADED_LEVELS.contains(l)) else {
                continue;
            };
            if r.grantee_tenant_id != 0 || (r.restriction != 0 && level != Level::Database) {
                continue;
            }
            let db = DbId(r.db_id);
            let object = ObjectId(r.object_id);
            let named = if object.0 == 0 && level != Level::GraphElement {
                parse_named(&r.name_pattern)
            } else {
                None
            };
            let target = match (level, named) {
                (Level::Tenant, _) => PrivTarget::Tenant,
                (Level::Database, _) if r.restriction != 0 => {
                    PrivTarget::Restriction(r.name_pattern.clone())
                }
                (Level::Database, _) => PrivTarget::Database(r.name_pattern.clone()),
                (Level::Object | Level::Column, Some((db_key, name_key))) => PrivTarget::Named {
                    db_key,
                    name_key,
                    column: (!r.column_name.is_empty()).then(|| r.column_name.clone()),
                },
                (Level::Object, None) => PrivTarget::Object { db, object },
                (Level::Column, None) => PrivTarget::Column {
                    db,
                    object,
                    column: r.column_name.clone(),
                },
                (Level::GraphElement, _) => PrivTarget::GraphElement {
                    db,
                    graph: object,
                    rels: r.element & (1 << 31) != 0,
                    name: r.name_pattern.clone(),
                },
            };
            privileges.push(PrivRow {
                priv_id: r.priv_id,
                grantee: Grantee::from_parts(r.grantee_kind, r.grantee_id),
                target,
                actions: ActionSet(r.actions),
            });
        }
        Ok(AuthzRows {
            roles,
            edges,
            privileges,
        })
    }

    /// Adds `actions` to `grantee`'s row at `target` (creating it); an
    /// existing row also takes the new grantor and time.
    ///
    /// # Errors
    /// The store's; 1406 for a column or name longer than the table kept.
    pub async fn grant(
        &self,
        tx: &mut dyn RecordTx,
        grantee: Grantee,
        target: &PrivTarget,
        actions: ActionSet,
        granted_by: Option<UserId>,
    ) -> Result<()> {
        let row = PrivRec::new(grantee, target, actions, granted_by);
        self.upsert_privilege(tx, row, true).await
    }

    /// Removes `actions` from `grantee`'s row at `target`, deleting the
    /// row when nothing is left. Returns the actions the row held before
    /// (empty when there was no row).
    ///
    /// # Errors
    /// The store's.
    pub async fn revoke(
        &self,
        tx: &mut dyn RecordTx,
        grantee: Grantee,
        target: &PrivTarget,
        actions: ActionSet,
    ) -> Result<ActionSet> {
        let want = PrivRec::new(grantee, target, ActionSet::EMPTY, None);
        let found = self
            .privileges(tx)
            .await?
            .into_iter()
            .find(|p| p.key() == want.key());
        let Some(mut row) = found else {
            return Ok(ActionSet::EMPTY);
        };
        let before = ActionSet(row.actions);
        let left = before.minus(actions);
        if left.is_empty() {
            PRIVILEGES.delete(tx, &priv_pk(row.priv_id)).await?;
        } else {
            row.actions = left.0;
            PRIVILEGES.put(tx, &priv_pk(row.priv_id), &row).await?;
        }
        Ok(before)
    }

    /// Deletes every privilege and role edge `grantee` holds, and, for a
    /// role, every edge granting it.
    ///
    /// # Errors
    /// The store's.
    pub async fn forget(&self, tx: &mut dyn RecordTx, grantee: Grantee) -> Result<()> {
        self.forget_privileges(tx, grantee).await?;
        let under = Pk::new().u8(grantee.kind().byte()).u32(grantee.id());
        for (pk, e) in ROLE_EDGES.scan::<EdgeRec>(tx, &under).await? {
            if e.grantee_tenant_id == 0 {
                ROLE_EDGES.delete(tx, &pk).await?;
            }
        }
        if let Grantee::Role(r) = grantee {
            for (pk, e) in ROLE_EDGES.scan::<EdgeRec>(tx, &Pk::new()).await? {
                if e.role_id == r.0 {
                    ROLE_EDGES.delete(tx, &pk).await?;
                }
            }
        }
        Ok(())
    }

    /// Deletes every privilege row of `grantee` (`REVOKE ALL PRIVILEGES,
    /// GRANT OPTION`); its roles stay.
    ///
    /// # Errors
    /// The store's.
    pub async fn forget_privileges(&self, tx: &mut dyn RecordTx, grantee: Grantee) -> Result<()> {
        self.delete_privileges(tx, |p| p.held_by(grantee)).await
    }

    /// Deletes every privilege on `object` (it was dropped).
    ///
    /// # Errors
    /// The store's.
    pub async fn forget_object(
        &self,
        tx: &mut dyn RecordTx,
        db: DbId,
        object: ObjectId,
    ) -> Result<()> {
        self.delete_privileges(tx, |p| p.db_id == db.0 && p.object_id == object.0)
            .await
    }

    /// `auth.grants_on_drop = keep` for a dropped table or view: its object
    /// and column rows become [`PrivTarget::Named`] rows for
    /// `` `db_key`.`name_key` `` (merged into any already there: their
    /// actions are added, their grantor kept). Returns whether there were
    /// rows.
    ///
    /// # Errors
    /// The store's; 1406 for a name pattern longer than the table kept.
    pub async fn keep_by_name(
        &self,
        tx: &mut dyn RecordTx,
        db: DbId,
        object: ObjectId,
        db_key: &str,
        name_key: &str,
    ) -> Result<bool> {
        let rows: Vec<PrivRec> = self
            .privileges(tx)
            .await?
            .into_iter()
            .filter(|p| {
                p.db_id == db.0
                    && p.object_id == object.0
                    && p.is_object_or_column()
                    && p.restriction == 0
            })
            .collect();
        if rows.is_empty() {
            return Ok(false);
        }
        let pattern = named_pattern(db_key, name_key);
        let now = now_ms();
        for r in rows {
            let kept = PrivRec {
                priv_id: 0,
                grantee_tenant_id: r.grantee_tenant_id,
                grantee_kind: r.grantee_kind,
                grantee_id: r.grantee_id,
                level: r.level,
                db_id: 0,
                object_id: 0,
                column_name: r.column_name,
                kv_prefix: Vec::new(),
                element: 0,
                name_pattern: pattern.clone(),
                actions: r.actions,
                restriction: 0,
                detail: None,
                granted_by: r.granted_by,
                granted_by_tenant_id: r.granted_by_tenant_id,
                granted_at: now,
            };
            self.upsert_privilege(tx, kept, false).await?;
        }
        self.forget_object(tx, db, object).await?;
        Ok(true)
    }

    /// Deletes the [`PrivTarget::Named`] rows of relation `name_key` in
    /// database `db_key`, or of every relation in it when `name_key` is
    /// `None`.
    ///
    /// # Errors
    /// The store's.
    pub async fn forget_named(
        &self,
        tx: &mut dyn RecordTx,
        db_key: &str,
        name_key: Option<&str>,
    ) -> Result<()> {
        let held_by_name = |p: &PrivRec| p.is_object_or_column() && p.object_id == 0;
        if let Some(n) = name_key {
            let pattern = named_pattern(db_key, n);
            self.delete_privileges(tx, move |p| held_by_name(p) && p.name_pattern == pattern)
                .await
        } else {
            let prefix = format!("`{}`.", db_key.replace('`', "``"));
            self.delete_privileges(tx, move |p| {
                held_by_name(p) && p.name_pattern.starts_with(&prefix)
            })
            .await
        }
    }

    /// `auth.grants_on_drop = drop` for a dropped database: deletes its
    /// `db.*` rows (by name key) and every object and column row of its
    /// objects (by id).
    ///
    /// # Errors
    /// The store's.
    pub async fn forget_database(
        &self,
        tx: &mut dyn RecordTx,
        db: DbId,
        db_key: &str,
    ) -> Result<()> {
        self.delete_privileges(tx, |p| {
            p.level == Level::Database.as_str() && p.db_id == 0 && p.name_pattern == db_key
        })
        .await?;
        self.delete_privileges(tx, |p| p.db_id == db.0 && p.is_object_or_column())
            .await
    }

    /// Creates the SQL role `'name'@'host'` and returns its id (from the
    /// `roles` counter); `None` if it exists (host compared without case).
    ///
    /// # Errors
    /// The store's; 1406 for a name or host over 255 characters.
    pub async fn create_role(
        &self,
        tx: &mut dyn RecordTx,
        name: &str,
        host: &str,
        created_by: Option<UserId>,
    ) -> Result<Option<RoleId>> {
        check_chars("name", name, 255)?;
        check_chars("host", host, 255)?;
        let taken = self.roles(tx).await?.iter().any(|r| {
            r.kind == SQL_ROLE && r.name == name && host_eq(&r.host, host) && r.db_id == 0
        });
        if taken {
            return Ok(None);
        }
        let id = records::next_ids(tx, ROLE_COUNTER, 1).await?;
        let id = u32::try_from(id).map_err(|_| SprigError::internal("role ids exhausted"))?;
        let row = RoleRec {
            role_id: id,
            kind: SQL_ROLE.to_owned(),
            name: name.to_owned(),
            host: host.to_owned(),
            db_id: 0,
            members_hash: None,
            created_by: created_by.map(|u| u.0),
            created_at: now_ms(),
            last_used_at: None,
        };
        ROLES.put(tx, &role_pk(id), &row).await?;
        Ok(Some(RoleId(id)))
    }

    /// The SQL role `'name'@'host'` (host compared without case), read in
    /// the transaction.
    ///
    /// # Errors
    /// The store's.
    pub async fn role(
        &self,
        tx: &mut dyn RecordTx,
        name: &str,
        host: &str,
    ) -> Result<Option<RoleId>> {
        Ok(self
            .roles(tx)
            .await?
            .iter()
            .find(|r| r.kind == SQL_ROLE && r.name == name && host_eq(&r.host, host))
            .map(|r| RoleId(r.role_id)))
    }

    /// Deletes the role row (its grants with [`Self::forget`]).
    ///
    /// # Errors
    /// The store's.
    pub async fn delete_role(&self, tx: &mut dyn RecordTx, role: RoleId) -> Result<()> {
        ROLES.delete(tx, &role_pk(role.0)).await
    }

    /// `GRANT role TO grantee [WITH ADMIN OPTION]`. An existing edge keeps
    /// its default flag, grantor and time, and gains the admin option if
    /// asked.
    ///
    /// # Errors
    /// The store's.
    pub async fn grant_role(
        &self,
        tx: &mut dyn RecordTx,
        grantee: Grantee,
        role: RoleId,
        admin_option: bool,
        granted_by: Option<UserId>,
    ) -> Result<()> {
        let pk = edge_pk(grantee, role);
        let row = match ROLE_EDGES.get::<EdgeRec>(tx, &pk).await? {
            Some(mut e) => {
                e.admin_option = e.admin_option || admin_option;
                e
            }
            None => EdgeRec {
                grantee_tenant_id: 0,
                grantee_kind: grantee.kind(),
                grantee_id: grantee.id(),
                role_id: role.0,
                admin_option,
                is_default: false,
                granted_by: granted_by.map(|u| u.0),
                granted_by_tenant_id: 0,
                granted_at: now_ms(),
            },
        };
        ROLE_EDGES.put(tx, &pk, &row).await
    }

    /// `REVOKE role FROM grantee`; whether there was an edge.
    ///
    /// # Errors
    /// The store's.
    pub async fn revoke_role(
        &self,
        tx: &mut dyn RecordTx,
        grantee: Grantee,
        role: RoleId,
    ) -> Result<bool> {
        let pk = edge_pk(grantee, role);
        let existed = ROLE_EDGES.get::<EdgeRec>(tx, &pk).await?.is_some();
        if existed {
            ROLE_EDGES.delete(tx, &pk).await?;
        }
        Ok(existed)
    }

    /// `SET DEFAULT ROLE`: exactly `roles` (each already granted) become
    /// `user`'s defaults.
    ///
    /// # Errors
    /// The store's.
    pub async fn set_default_roles(
        &self,
        tx: &mut dyn RecordTx,
        user: UserId,
        roles: &[RoleId],
    ) -> Result<()> {
        let grantee = Grantee::User(user);
        let under = Pk::new().u8(grantee.kind().byte()).u32(grantee.id());
        for (pk, mut e) in ROLE_EDGES.scan::<EdgeRec>(tx, &under).await? {
            if e.grantee_tenant_id != 0 {
                continue;
            }
            let default = roles.contains(&RoleId(e.role_id));
            if e.is_default != default {
                e.is_default = default;
                ROLE_EDGES.put(tx, &pk, &e).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
