//! Definers of stored code.
//!
//! MariaDB runs a `SQL SECURITY DEFINER` routine, every trigger, every
//! event and a `SQL SECURITY DEFINER` view with its definer's rights. SprigDB
//! creates stored programs as the cluster's `sprig_ctl` account (binary
//! logging refuses them from accounts without `SUPER`, error 1419), so
//! before the fix their definer was `sprig_ctl`, which holds `ALL` on
//! every tenant's schemas: dynamic SQL in a procedure or event body (which
//! the gateway never sees) could read any tenant. Now every stored program
//! names the creating session's backend account as `DEFINER` (the gateway
//! adds it; `sprig_ctl` needs MariaDB's `SET USER` privilege for that),
//! and this module moves code created before the fix:
//!
//! ```text
//! migrate(tenant T, cluster C):                     # as sprig_ctl, once per
//!   found := routines, triggers, events and views   # gateway start
//!            in t<T>_d% schemas whose definer is not one of T's accounts
//!   for each: definer := the account of the database's creator if live,
//!             else the lowest-id live user holding SPRIG_TENANT_ADMIN,
//!             else t<T>_u0 (no such account: the code fails, closed)
//!     SHOW CREATE, then CREATE OR REPLACE with DEFINER = that account, in
//!     the object's sql_mode, character set and time zone
//! ```
//!
//! **Share views run as their parent's share definer.** A child tenant's
//! read-through share view (`__sprig_share_<id>`, chapter 42,
//! `sprig_inherit::upstream`) lives in the child's schema but runs as the
//! parent's share definer `t<parent>_sprig_share`
//! ([`sprig_common::names::share_definer_account`], decision RL70), a
//! locked account with `SELECT` only on the parent's relations its share
//! views read, so it can read the parent's shared table and nothing of the
//! child's. [`migrate`] never moves one to a child account (every read of
//! the inherited table would fail with 1356, RL68). [`repair_shares`]
//! (part of [`migrate`], and the drift sweep's check) re-points a share
//! view whose definer is anything else (the share's creator
//! `t<parent>_u<id>` before RL70, a child account an older build moved it
//! to) to the share definer, creating the account when it is missing and
//! granting it `SELECT` on exactly the parent's relations the view's text
//! names:
//!
//! ```text
//! repair_shares(tenant T, parent P):
//!   for each view __sprig_share_<id> in t<T>_d% schemas:
//!     reads := every `t<P>_d...`.`name` pair in its definition
//!     CREATE USER IF NOT EXISTS t<P>_sprig_share ACCOUNT LOCK  (if missing)
//!     GRANT SELECT ON each of reads the account lacks
//!     definer not t<P>_sprig_share → CREATE OR REPLACE with that definer
//! ```

use std::collections::BTreeMap;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::shard_schemas;
use sprig_catalog::store::DbState;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::names::{AccountName, SchemaName, share_definer_account};
use sprig_common::{Result, SprigError};

use super::privilege::Action;
use super::state::AuthzState;
use super::store::{Grantee, PrivTarget};
use crate::cache::TenantAuth;

/// What kind of stored code an object is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StoredKind {
    /// `PROCEDURE`.
    Procedure,
    /// `FUNCTION`.
    Function,
    /// `TRIGGER`.
    Trigger,
    /// `EVENT`.
    Event,
    /// `VIEW`.
    View,
}

impl StoredKind {
    /// The SQL keyword.
    #[must_use]
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Procedure => "PROCEDURE",
            Self::Function => "FUNCTION",
            Self::Trigger => "TRIGGER",
            Self::Event => "EVENT",
            Self::View => "VIEW",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "PROCEDURE" => Self::Procedure,
            "FUNCTION" => Self::Function,
            "TRIGGER" => Self::Trigger,
            "EVENT" => Self::Event,
            "VIEW" => Self::View,
            _ => return None,
        })
    }
}

/// A stored program or view found on a cluster.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoredObject {
    /// Its kind.
    pub kind: StoredKind,
    /// Its schema.
    pub schema: String,
    /// Its name.
    pub name: String,
    /// Its definer, as `information_schema` writes it (`user@host`).
    pub definer: String,
}

/// Whether `definer` (`user@host`) is one of `tenant`'s backend accounts.
#[must_use]
pub fn is_tenant_account(tenant: TenantId, definer: &str) -> bool {
    let user = definer.rsplit_once('@').map_or(definer, |(u, _)| u);
    user.parse::<AccountName>()
        .is_ok_and(|a| a.tenant() == tenant && matches!(a, AccountName::User { .. }))
}

/// The name prefix of a cross-tenant share view (`__sprig_share_<id>`).
pub const SHARE_VIEW_PREFIX: &str = "__sprig_share_";

/// Whether `kind` and `name` are a share view's: a view named
/// `__sprig_share_<digits>`.
#[must_use]
pub fn is_share_view(kind: StoredKind, name: &str) -> bool {
    kind == StoredKind::View
        && name
            .strip_prefix(SHARE_VIEW_PREFIX)
            .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
}

/// Whether `definer` (`user@host`) is `parent`'s share definer
/// (`t<parent>_sprig_share@%`): the definer a share view in a child of
/// `parent` must have (decision RL70).
#[must_use]
pub fn is_share_definer(parent: Option<TenantId>, definer: &str) -> bool {
    let (user, host) = definer.rsplit_once('@').unwrap_or((definer, "%"));
    parent.is_some_and(|p| user == share_definer_account(p) && host == "%")
}

/// The relations of `parent`'s data schemas a view's definition names
/// (`` `t<parent>_d...`.`name` ``), each once, in order: what its definer
/// needs `SELECT` on.
#[must_use]
pub fn parent_reads(parent: TenantId, definition: &str) -> Vec<(String, String)> {
    let prefix = format!("t{parent}_d");
    let mut out: Vec<(String, String)> = Vec::new();
    let b = definition.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'`' {
            i += 1;
            continue;
        }
        let Some(end) = quoted_end(&definition[i..]) else {
            break;
        };
        let first = unquote(&definition[i..i + end]);
        let rest = &definition[i + end..];
        if let Some(r) = rest.strip_prefix('.')
            && let Some(e2) = quoted_end(r)
        {
            let second = unquote(&r[..e2]);
            let ours = first.starts_with(&prefix)
                && first.parse::<SchemaName>().is_ok_and(|s| {
                    s.tenant() == parent && !matches!(s, SchemaName::Catalog { .. })
                });
            if ours && !out.iter().any(|(a, n)| *a == first && *n == second) {
                out.push((first, second));
            }
        }
        i += end;
    }
    out
}

fn unquote(q: &str) -> String {
    q[1..q.len() - 1].replace("``", "`")
}

/// The account stored code of each of the tenant's schemas moves to (see
/// the module docs): schema name to account user name (host `%`).
#[must_use]
pub fn wanted(
    cat: &TenantCatalog,
    state: &AuthzState,
    auth: &TenantAuth,
) -> BTreeMap<String, String> {
    let tenant = cat.tenant;
    let live = |u: UserId| auth.user(u).is_some();
    let admin = auth
        .users()
        .map(|u| u.row.user_id)
        .filter(|u| {
            !state.is_migrated()
                || state.privileges_of(Grantee::User(*u)).any(|p| {
                    matches!(p.target, PrivTarget::Tenant) && p.actions.has(Action::TenantAdmin)
                })
        })
        .min();
    let fallback = admin.unwrap_or(UserId(0));
    let mut out = BTreeMap::new();
    for d in cat.databases.iter().filter(|d| d.state == DbState::Active) {
        let user = d.created_by.filter(|u| live(*u)).unwrap_or(fallback);
        let acct = AccountName::User { tenant, user }.to_string();
        for s in shard_schemas(tenant, d) {
            out.insert(s.to_string(), acct.clone());
        }
    }
    out
}

/// What [`scan`] found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Scan {
    /// Stored programs and views whose definer isn't one of the tenant's
    /// accounts (share views excepted).
    pub foreign: Vec<StoredObject>,
    /// Share views whose definer isn't the parent's share definer.
    pub stale_shares: Vec<StoredObject>,
    /// Share views whose definer is the parent's share definer.
    pub shares: Vec<StoredObject>,
}

/// The tenant's stored programs and views on the cluster behind `conn`
/// whose definer isn't one of its accounts, and its share views, by
/// whether their definer is the share definer of the tenant's parent
/// (the last of `ancestors`, root first).
///
/// # Errors
/// A backend error.
pub async fn scan(conn: &mut Conn, tenant: TenantId, ancestors: &[TenantId]) -> Result<Scan> {
    let like = format!("t{tenant}\\_d%");
    // Routines and events come from mysql.proc and mysql.event. Triggers
    // and views are read one schema at a time: a LIKE on their schema
    // reads every table of every tenant on the server (see
    // `sprig_catalog::infoschema`).
    let mut rows: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT ROUTINE_TYPE, ROUTINE_SCHEMA, ROUTINE_NAME, DEFINER \
               FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA LIKE ? \
             UNION ALL SELECT 'EVENT', EVENT_SCHEMA, EVENT_NAME, DEFINER \
               FROM information_schema.EVENTS WHERE EVENT_SCHEMA LIKE ?",
            (&like, &like),
        )
        .await
        .map_err(crate::store::map_err)?;
    let schemas = sprig_catalog::infoschema::schemas_like(conn, &like).await?;
    rows.extend(
        sprig_catalog::infoschema::read_per_schema::<(String, String, String, String)>(
            conn,
            "SELECT 'TRIGGER', TRIGGER_SCHEMA, TRIGGER_NAME, DEFINER FROM information_schema.TRIGGERS",
            "EVENT_OBJECT_SCHEMA",
            &schemas,
            "",
        )
        .await?,
    );
    rows.extend(
        sprig_catalog::infoschema::read_per_schema::<(String, String, String, String)>(
            conn,
            "SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME, DEFINER FROM information_schema.VIEWS",
            "TABLE_SCHEMA",
            &schemas,
            "",
        )
        .await?,
    );
    Ok(classify(tenant, ancestors, rows))
}

/// [`scan`]'s decision on the rows it read (kind, schema, name, definer).
fn classify(
    tenant: TenantId,
    ancestors: &[TenantId],
    rows: Vec<(String, String, String, String)>,
) -> Scan {
    let parent = ancestors.last().copied();
    let mut out = Scan::default();
    for (k, schema, name, definer) in rows {
        let Some(kind) = StoredKind::parse(&k) else {
            continue;
        };
        let o = StoredObject {
            kind,
            schema,
            name,
            definer,
        };
        if is_share_view(kind, &o.name) {
            if is_share_definer(parent, &o.definer) {
                out.shares.push(o);
            } else {
                out.stale_shares.push(o);
            }
        } else if !is_tenant_account(tenant, &o.definer) {
            out.foreign.push(o);
        }
    }
    out.foreign.sort();
    out.stale_shares.sort();
    out.shares.sort();
    out
}

/// `create` (a `SHOW CREATE` text MariaDB wrote: `CREATE [ALGORITHM=...]
/// DEFINER=`u`@`h` ...`) as `CREATE OR REPLACE ... DEFINER=`account`@`%`
/// ...`; `None` when it has no such definer clause.
#[must_use]
pub fn redefine(create: &str, account: &str) -> Option<String> {
    let head = create.get(..7)?;
    if !head.eq_ignore_ascii_case("CREATE ") {
        return None;
    }
    let rest = &create[7..];
    let at = rest.find("DEFINER=`")?;
    // Only the clause MariaDB writes before a view's definer.
    let before = &rest[..at];
    let algorithm = before
        .strip_prefix("ALGORITHM=")
        .and_then(|a| a.strip_suffix(' '))
        .is_some_and(|a| !a.is_empty() && a.bytes().all(|c| c.is_ascii_alphabetic()));
    if !before.is_empty() && !algorithm {
        return None;
    }
    let after = &rest[at + "DEFINER=".len()..];
    let user_end = quoted_end(after)?;
    let host = after[user_end..].strip_prefix('@')?;
    let host_end = quoted_end(host)?;
    let tail = &host[host_end..];
    Some(format!(
        "CREATE OR REPLACE {}DEFINER=`{}`@`%`{tail}",
        &rest[..at],
        account.replace('`', "``")
    ))
}

/// One past the closing backtick of the identifier `s` starts with.
fn quoted_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    if b.first() != Some(&b'`') {
        return None;
    }
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'`' {
            if b.get(i + 1) == Some(&b'`') {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// What [`migrate`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DefinerReport {
    /// Objects given a tenant account as definer.
    pub moved: Vec<StoredObject>,
    /// Objects left as they were, with the reason.
    pub failed: Vec<(StoredObject, String)>,
    /// Share views whose definer wasn't the parent's share definer, or
    /// whose share definer was missing or lacked a grant, before
    /// [`repair_shares`] (with the definer they had).
    pub stale_shares: Vec<StoredObject>,
    /// Of those, the views [`repair_shares`] re-pointed or completed.
    pub repaired: Vec<StoredObject>,
}

fn text(row: &Row, name: &str) -> Option<String> {
    let i = row
        .columns_ref()
        .iter()
        .position(|c| c.name_str() == name)?;
    match row.as_ref(i)? {
        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// Moves the tenant's stored code on the cluster behind `conn` (a
/// `sprig_ctl` connection to its writer) to tenant accounts: `wanted`
/// maps each schema to its account ([`wanted`]); share views are left
/// alone (see the module docs), `ancestors` being the tenant's. The
/// connection's session state is reset afterwards.
///
/// # Errors
/// A backend error listing the objects; single objects that fail are
/// reported in [`DefinerReport::failed`].
pub async fn migrate(
    conn: &mut Conn,
    tenant: TenantId,
    ancestors: &[TenantId],
    wanted: &BTreeMap<String, String>,
) -> Result<DefinerReport> {
    let scanned = scan(conn, tenant, ancestors).await?;
    let found = scanned.foreign.clone();
    let mut rep = repair(conn, ancestors, scanned).await?;
    if found.is_empty() {
        return Ok(rep);
    }
    let fallback = AccountName::User {
        tenant,
        user: UserId(0),
    }
    .to_string();
    for o in found {
        let account = wanted.get(&o.schema).unwrap_or(&fallback);
        match migrate_one(conn, &o, account).await {
            Ok(()) => rep.moved.push(o),
            Err(e) => rep.failed.push((o, e)),
        }
    }
    if let Err(e) = conn.reset().await {
        return Err(crate::store::map_err(e));
    }
    Ok(rep)
}

/// Checks the tenant's share views on the cluster behind `conn` (a
/// `sprig_ctl` connection to its writer) and repairs them (module docs):
/// each runs as its parent's share definer, which exists and may read
/// what the view reads. `ancestors`: the tenant's, root first. The
/// connection's session state is reset afterwards when anything was
/// repaired.
///
/// # Errors
/// A backend error reading the views; single views that fail are
/// reported in [`DefinerReport::failed`].
pub async fn repair_shares(
    conn: &mut Conn,
    tenant: TenantId,
    ancestors: &[TenantId],
) -> Result<DefinerReport> {
    let scanned = scan(conn, tenant, ancestors).await?;
    repair(conn, ancestors, scanned).await
}

async fn repair(conn: &mut Conn, ancestors: &[TenantId], scanned: Scan) -> Result<DefinerReport> {
    let mut rep = DefinerReport::default();
    let Some(parent) = ancestors.last().copied() else {
        return Ok(rep);
    };
    if scanned.shares.is_empty() && scanned.stale_shares.is_empty() {
        return Ok(rep);
    }
    let account = share_definer_account(parent);
    let exists: Option<String> = conn
        .exec_first(
            "SELECT User FROM mysql.global_priv WHERE User = ? AND Host = '%'",
            (&account,),
        )
        .await
        .map_err(crate::store::map_err)?;
    let granted: Vec<(String, String)> = conn
        .exec(
            "SELECT Db, Table_name FROM mysql.tables_priv WHERE User = ? AND Host = '%' \
             AND FIND_IN_SET('Select', Table_priv) > 0",
            (&account,),
        )
        .await
        .map_err(crate::store::map_err)?;
    let mut created = exists.is_some();
    let mut touched = false;
    let views = scanned
        .stale_shares
        .into_iter()
        .map(|o| (o, false))
        .chain(scanned.shares.into_iter().map(|o| (o, true)));
    for (o, definer_ok) in views {
        let definition: Option<String> = conn
            .exec_first(
                "SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ?",
                (&o.schema, &o.name),
            )
            .await
            .map_err(crate::store::map_err)?;
        let Some(definition) = definition else {
            continue;
        };
        let missing: Vec<(String, String)> = parent_reads(parent, &definition)
            .into_iter()
            .filter(|(d, t)| {
                !granted
                    .iter()
                    .any(|(gd, gt)| gd == d && gt.eq_ignore_ascii_case(t))
            })
            .collect();
        if definer_ok && created && missing.is_empty() {
            continue;
        }
        rep.stale_shares.push(o.clone());
        let r = async {
            if !created {
                conn.query_drop(format!(
                    "CREATE USER IF NOT EXISTS '{account}'@'%' ACCOUNT LOCK"
                ))
                .await
                .map_err(|e| e.to_string())?;
                created = true;
            }
            for (d, t) in &missing {
                conn.query_drop(format!(
                    "GRANT SELECT ON {}.{} TO '{account}'@'%'",
                    quote(d),
                    quote(t)
                ))
                .await
                .map_err(|e| e.to_string())?;
            }
            if !definer_ok {
                migrate_one(conn, &o, &account).await?;
            }
            Ok::<_, String>(())
        }
        .await;
        touched = true;
        match r {
            Ok(()) => rep.repaired.push(o),
            Err(e) => rep.failed.push((o, e)),
        }
    }
    if touched && let Err(e) = conn.reset().await {
        return Err(crate::store::map_err(e));
    }
    Ok(rep)
}

async fn migrate_one(
    conn: &mut Conn,
    o: &StoredObject,
    account: &str,
) -> std::result::Result<(), String> {
    let show = format!(
        "SHOW CREATE {} {}.{}",
        o.kind.keyword(),
        quote(&o.schema),
        quote(&o.name)
    );
    let row: Option<Row> = conn.query_first(show).await.map_err(|e| e.to_string())?;
    let row = row.ok_or_else(|| "gone".to_owned())?;
    let create_col = match o.kind {
        StoredKind::Procedure => "Create Procedure",
        StoredKind::Function => "Create Function",
        StoredKind::Trigger => "SQL Original Statement",
        StoredKind::Event => "Create Event",
        StoredKind::View => "Create View",
    };
    let create =
        text(&row, create_col).ok_or_else(|| "no definition (no privilege?)".to_owned())?;
    let sql = redefine(&create, account).ok_or_else(|| "no DEFINER clause".to_owned())?;
    let mut set = Vec::new();
    if let Some(m) = text(&row, "sql_mode") {
        set.push(format!("sql_mode = '{}'", m.replace('\'', "''")));
    }
    if let Some(z) = text(&row, "time_zone") {
        set.push(format!("time_zone = '{}'", z.replace('\'', "''")));
    }
    if let Some(c) = text(&row, "character_set_client") {
        set.push(format!(
            "character_set_client = '{}'",
            c.replace('\'', "''")
        ));
    }
    if let Some(c) = text(&row, "collation_connection") {
        set.push(format!(
            "collation_connection = '{}'",
            c.replace('\'', "''")
        ));
    }
    if !set.is_empty() {
        conn.query_drop(format!("SET SESSION {}", set.join(", ")))
            .await
            .map_err(|e| e.to_string())?;
    }
    conn.query_drop(format!("USE {}", quote(&o.schema)))
        .await
        .map_err(|e| e.to_string())?;
    conn.query_drop(sql).await.map_err(|e| e.to_string())
}

/// The definer error a stored program's creation is refused with when
/// MariaDB recorded another definer than the session's account.
#[must_use]
pub fn mismatch(kind: &str, name: &str, got: &str) -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::DefinerRefused,
        format!(
            "{kind} {name} was not created with the session's account as its definer \
             (MariaDB recorded {got}); it was dropped"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redefines_what_mariadb_writes() {
        assert_eq!(
            redefine(
                "CREATE DEFINER=`root`@`%` PROCEDURE `p`() select 1",
                "t1_u1"
            )
            .as_deref(),
            Some("CREATE OR REPLACE DEFINER=`t1_u1`@`%` PROCEDURE `p`() select 1")
        );
        assert_eq!(
            redefine(
                "CREATE ALGORITHM=UNDEFINED DEFINER=`sprig_ctl`@`localhost` SQL SECURITY DEFINER VIEW `v` AS select 1",
                "t1_u2"
            )
            .as_deref(),
            Some(
                "CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`t1_u2`@`%` SQL SECURITY DEFINER VIEW `v` AS select 1"
            )
        );
        assert_eq!(
            redefine(
                "CREATE DEFINER=`a``b`@`h` TRIGGER t AFTER INSERT ON x FOR EACH ROW SET @a=1",
                "t1_u1"
            )
            .as_deref(),
            Some(
                "CREATE OR REPLACE DEFINER=`t1_u1`@`%` TRIGGER t AFTER INSERT ON x FOR EACH ROW SET @a=1"
            )
        );
        assert_eq!(
            redefine("CREATE PROCEDURE p() select 'DEFINER=`x`@`y`'", "a"),
            None
        );
        assert_eq!(redefine("SELECT 1", "a"), None);
    }

    #[test]
    fn share_views_run_as_their_parents_share_definer() {
        let row = |k: &str, n: &str, d: &str| {
            (k.to_owned(), "t5_d1".to_owned(), n.to_owned(), d.to_owned())
        };
        let got = classify(
            TenantId(5),
            &[TenantId(2), TenantId(3)],
            vec![
                // The parent's share definer: kept.
                row("VIEW", "__sprig_share_4", "t3_sprig_share@%"),
                // The share's creator (before RL70), a grandparent's
                // account or share definer, the child's own account (an
                // older build moved it), a role, sprig_ctl, another host:
                // re-pointed.
                row("VIEW", "__sprig_share_9", "t3_u7@%"),
                row("VIEW", "__sprig_share_11", "t2_sprig_share@%"),
                row("VIEW", "__sprig_share_6", "t5_u1@%"),
                row("VIEW", "__sprig_share_8", "t3_r1@%"),
                row("VIEW", "__sprig_share_10", "sprig_ctl@localhost"),
                row("VIEW", "__sprig_share_12", "t3_sprig_share@localhost"),
                // Not a share view's name: the usual rule.
                row("VIEW", "__sprig_share_x", "t3_u7@%"),
                row("PROCEDURE", "__sprig_share_4", "t3_u7@%"),
                row("VIEW", "v", "t5_u1@%"),
            ],
        );
        let names = |v: &[StoredObject]| v.iter().map(|o| o.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&got.shares), ["__sprig_share_4"]);
        assert_eq!(
            names(&got.stale_shares),
            [
                "__sprig_share_10",
                "__sprig_share_11",
                "__sprig_share_12",
                "__sprig_share_6",
                "__sprig_share_8",
                "__sprig_share_9"
            ]
        );
        assert_eq!(
            names(&got.foreign),
            ["__sprig_share_4", "__sprig_share_x"],
            "a procedure and a view not named like a share view move as before"
        );
        assert!(is_share_view(StoredKind::View, "__sprig_share_12"));
        assert!(!is_share_view(StoredKind::View, "__sprig_share_"));
        assert!(!is_share_definer(None, "t3_sprig_share@%"));
        // A root tenant has no parent: none of its views is a share view
        // it may keep.
        let root = classify(
            TenantId(5),
            &[],
            vec![row("VIEW", "__sprig_share_4", "t3_sprig_share@%")],
        );
        assert_eq!(names(&root.stale_shares), ["__sprig_share_4"]);
    }

    #[test]
    fn a_share_view_reads_what_its_text_names_in_the_parent() {
        let text = "select `__sprig_v`.`id` AS `id` from (select `l`.`id` AS `id` from \
                    `t3_d1_g1_s1`.`docs` `__sprig_lyr` union all select `u`.`id` from \
                    `t3_d1_g1_s1`.`__sprig_share_2` `u` join `t3_d1_g1_s1`.`docs` `x` join \
                    `t30_d1_g1_s1`.`docs` `y` join `t3_sprig`.`objects` `z` join \
                    `t3_d1_ref`.`a``b` `w`) `__sprig_v` where `__sprig_v`.`id` > 0";
        assert_eq!(
            parent_reads(TenantId(3), text),
            [
                ("t3_d1_g1_s1".to_owned(), "docs".to_owned()),
                ("t3_d1_g1_s1".to_owned(), "__sprig_share_2".to_owned()),
                ("t3_d1_ref".to_owned(), "a`b".to_owned()),
            ],
            "another tenant's, the catalog and aliases are never granted"
        );
        assert!(parent_reads(TenantId(3), "select 1").is_empty());
        assert!(is_share_definer(Some(TenantId(3)), "t3_sprig_share@%"));
        assert!(!is_share_definer(Some(TenantId(3)), "t30_sprig_share@%"));
    }

    #[test]
    fn tenant_accounts() {
        assert!(is_tenant_account(TenantId(1), "t1_u7@%"));
        assert!(!is_tenant_account(TenantId(1), "t12_u7@%"));
        assert!(!is_tenant_account(TenantId(1), "root@%"));
        assert!(!is_tenant_account(TenantId(1), "sprig_ctl@localhost"));
        assert!(!is_tenant_account(TenantId(1), "t1_r3@%"));
    }
}
