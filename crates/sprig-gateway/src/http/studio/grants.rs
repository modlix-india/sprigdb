//! What a user's `SHOW GRANTS` lines allow, as far as Studio's routes ask
//! (the privileges, read the way a client would read them).
//!
//! Studio hides what a user can't do as a courtesy; the gateway decides
//! every statement. The one place the gateway itself uses this is the
//! indexing actions (approve, reject, freeze, drop), which go to the
//! controller rather than through a statement: Studio lets only users who
//! could run the `CREATE INDEX` approve, so the caller needs `INDEX`,
//! `ALTER` or `ALL` on the database (or `*.*`), or `SPRIG_TENANT_ADMIN`.

use serde_json::{Value, json};

/// The privileges Studio's views and routes look at.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // one flag per privilege, as WhoAmI shows them
pub struct Privileges {
    /// `SPRIG_TENANT_ADMIN` (or `ALL` on `*.*`).
    pub tenant_admin: bool,
    /// `SPRIG_SETTINGS_ADMIN` (or tenant admin).
    pub settings_admin: bool,
    /// `CREATE USER` (or tenant admin).
    pub create_user: bool,
    /// `GRANT OPTION` on something.
    pub grant_option: bool,
    /// `INDEX`, `ALTER` or `ALL` on `*.*`.
    pub index_all: bool,
    /// Databases with `INDEX`, `ALTER` or `ALL` on `` `db`.* ``.
    pub index_dbs: Vec<String>,
}

/// Removes the backquotes around a name and undoes doubled ones.
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('`') && t.ends_with('`') {
        t[1..t.len() - 1].replace("``", "`")
    } else {
        t.to_owned()
    }
}

/// The `ON` target of a grant: `(db, table)`, `*` for wildcards.
fn target(on: &str) -> (String, String) {
    // `db`.`t`, `db`.*, *.* (a backquoted name may hold a dot).
    let mut in_q = false;
    let mut split = None;
    for (i, c) in on.char_indices() {
        match c {
            '`' => in_q = !in_q,
            '.' if !in_q => {
                split = Some(i);
                break;
            }
            _ => {}
        }
    }
    match split {
        Some(i) => (unquote(&on[..i]), unquote(&on[i + 1..])),
        None => (unquote(on), "*".to_owned()),
    }
}

impl Privileges {
    /// Reads `SHOW GRANTS` lines.
    #[must_use]
    pub fn of(lines: &[String]) -> Self {
        let mut p = Self::default();
        for line in lines {
            let u = line.trim();
            let Some(rest) = u
                .strip_prefix("GRANT ")
                .or_else(|| u.strip_prefix("grant "))
            else {
                continue;
            };
            let upper = rest.to_ascii_uppercase();
            // Role grants (`GRANT r TO u`) have no ON.
            let Some(on_at) = upper.find(" ON ") else {
                continue;
            };
            let privs = &upper[..on_at];
            let after = &rest[on_at + 4..];
            let to_at = after
                .to_ascii_uppercase()
                .find(" TO ")
                .unwrap_or(after.len());
            let (db, table) = target(after[..to_at].trim());
            if upper.contains("WITH GRANT OPTION") {
                p.grant_option = true;
            }
            let list: Vec<&str> = privs.split(',').map(str::trim).collect();
            let all = list.iter().any(|x| *x == "ALL" || *x == "ALL PRIVILEGES");
            let has = |name: &str| all || list.contains(&name);
            if db == "*" {
                if has("SPRIG_TENANT_ADMIN") || all {
                    p.tenant_admin = true;
                }
                if has("SPRIG_SETTINGS_ADMIN") {
                    p.settings_admin = true;
                }
                if has("CREATE USER") {
                    p.create_user = true;
                }
                if has("INDEX") || has("ALTER") {
                    p.index_all = true;
                }
            } else if table == "*" && (has("INDEX") || has("ALTER")) && !p.index_dbs.contains(&db) {
                p.index_dbs.push(db);
            }
        }
        if p.tenant_admin {
            p.settings_admin = true;
            p.create_user = true;
            p.index_all = true;
        }
        p
    }

    /// Whether the user could create an index in `db`.
    #[must_use]
    pub fn may_index(&self, db: &str) -> bool {
        self.index_all || self.index_dbs.iter().any(|d| d.eq_ignore_ascii_case(db))
    }

    /// `WhoAmI`'s `privileges`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "tenant_admin": self.tenant_admin,
            "settings_admin": self.settings_admin,
            "create_user": self.create_user,
            "grant_option": self.grant_option,
            "index_all": self.index_all,
            "index_dbs": self.index_dbs,
        })
    }
}

/// `WhoAmI`'s `capabilities`: what the user can do, summed up for Studio
/// to choose which workspaces to offer. It is a summary for display only;
/// nothing on the server checks it, and the gateway decides every
/// statement as before.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // one flag per capability, as WhoAmI shows them
pub struct Capabilities {
    /// `SELECT` (or `ALL`) on anything.
    pub read_data: bool,
    /// `CREATE`, `CREATE VIEW`, `ALTER` or `DROP` (or `ALL`) on anything.
    /// `INDEX` alone doesn't count: approving indexes isn't designing schemas.
    pub change_schemas: bool,
    /// `CREATE ROUTINE` or `ALTER ROUTINE` (or `ALL`) on anything.
    pub manage_logic: bool,
    /// `CREATE USER` on `*.*`, or tenant admin.
    pub manage_users: bool,
    /// Can approve index and shard changes for every database:
    /// `INDEX`, `ALTER` or `ALL` on `*.*`, or tenant admin.
    pub approve_changes: bool,
    /// `SPRIG_TENANT_ADMIN` (or `ALL` on `*.*`).
    pub administer_tenant: bool,
    /// Holds platform operator rights in Studio.
    pub operator: bool,
}

/// The privilege names of a grant's list, without column lists:
/// `SELECT (a, b), INSERT` → `SELECT`, `INSERT`.
fn privilege_names(list: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for c in list.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => out.push(std::mem::take(&mut cur)),
            _ if depth == 0 => cur.push(c),
            _ => {}
        }
    }
    out.push(cur);
    out.into_iter()
        .map(|p| p.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|p| !p.is_empty())
        .collect()
}

impl Capabilities {
    /// Sums up `SHOW GRANTS` lines (`privs` read from the same lines) and
    /// whether the user holds operator rights.
    #[must_use]
    pub fn of(lines: &[String], privs: &Privileges, operator: bool) -> Self {
        let mut c = Self {
            manage_users: privs.create_user,
            approve_changes: privs.index_all,
            administer_tenant: privs.tenant_admin,
            operator,
            ..Self::default()
        };
        for line in lines {
            let u = line.trim().to_ascii_uppercase();
            let Some(rest) = u.strip_prefix("GRANT ") else {
                continue;
            };
            let Some(on_at) = rest.find(" ON ") else {
                continue;
            };
            let names = privilege_names(&rest[..on_at]);
            let has = |want: &[&str]| {
                names
                    .iter()
                    .any(|n| n == "ALL" || n == "ALL PRIVILEGES" || want.contains(&n.as_str()))
            };
            c.read_data |= has(&["SELECT"]);
            c.change_schemas |= has(&["CREATE", "CREATE VIEW", "ALTER", "DROP"]);
            c.manage_logic |= has(&["CREATE ROUTINE", "ALTER ROUTINE"]);
        }
        if c.administer_tenant {
            c.read_data = true;
            c.change_schemas = true;
            c.manage_logic = true;
        }
        c
    }

    /// The JSON `WhoAmI` returns.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "read_data": self.read_data,
            "change_schemas": self.change_schemas,
            "manage_logic": self.manage_logic,
            "manage_users": self.manage_users,
            "approve_changes": self.approve_changes,
            "administer_tenant": self.administer_tenant,
            "operator": self.operator,
        })
    }
}

/// The roles granted to the user, from `SHOW GRANTS` lines of the form
/// ``GRANT `role` TO `user`@`host` `` (a role grant has no `ON`).
#[must_use]
pub fn roles_of(lines: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for line in lines {
        let t = line.trim();
        let Some(rest) = t
            .get(..6)
            .filter(|h| h.eq_ignore_ascii_case("GRANT "))
            .map(|_| &t[6..])
        else {
            continue;
        };
        let upper = rest.to_ascii_uppercase();
        if upper.contains(" ON ") {
            continue;
        }
        let Some(to_at) = upper.find(" TO ") else {
            continue;
        };
        for role in rest[..to_at].split(',') {
            // `role` or `role`@`host` (an account used as a role).
            let name = match role.trim().find("`@") {
                Some(at) => &role.trim()[..=at],
                None => role.trim(),
            };
            let name = unquote(name);
            if !name.is_empty() && !out.contains(&name) {
                out.push(name);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(lines: &[&str]) -> Privileges {
        Privileges::of(&lines.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
    }

    #[test]
    fn reads_the_usual_lines() {
        let p = of(&[
            "GRANT USAGE ON *.* TO `app`@`%`",
            "GRANT SELECT, INSERT, INDEX ON `shop`.* TO `app`@`%`",
            "GRANT SELECT ON `crm`.`leads` TO `app`@`%`",
        ]);
        assert!(!p.tenant_admin);
        assert!(p.may_index("shop"));
        assert!(p.may_index("SHOP"));
        assert!(!p.may_index("crm"));
        assert!(!p.create_user);
    }

    #[test]
    fn all_and_admin() {
        let p = of(&["GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"]);
        assert!(p.tenant_admin && p.settings_admin && p.create_user && p.grant_option);
        assert!(p.may_index("anything"));
        let p = of(&["GRANT SPRIG_TENANT_ADMIN ON *.* TO `a`@`%`"]);
        assert!(p.tenant_admin && p.may_index("x"));
        let p = of(&["GRANT SPRIG_SETTINGS_ADMIN, CREATE USER ON *.* TO `a`@`%`"]);
        assert!(!p.tenant_admin && p.settings_admin && p.create_user);
        assert!(!p.may_index("x"));
    }

    fn caps(lines: &[&str], operator: bool) -> Capabilities {
        let lines: Vec<String> = lines.iter().map(|s| (*s).to_owned()).collect();
        Capabilities::of(&lines, &Privileges::of(&lines), operator)
    }

    #[test]
    fn capabilities_sum_up_the_grants() {
        // Read only, through a column grant.
        let c = caps(
            &[
                "GRANT USAGE ON *.* TO `ann`@`%`",
                "GRANT SELECT (`id`, `name`), INSERT ON `shop`.`orders` TO `ann`@`%`",
            ],
            false,
        );
        assert!(c.read_data);
        assert!(!c.change_schemas && !c.manage_logic && !c.manage_users);
        assert!(!c.approve_changes && !c.administer_tenant && !c.operator);

        // A developer on one database: schemas and logic, but approving
        // changes for every database needs the grant on *.*.
        let c = caps(
            &["GRANT SELECT, ALTER, CREATE ROUTINE ON `shop`.* TO `dev`@`%`"],
            false,
        );
        assert!(c.read_data && c.change_schemas && c.manage_logic);
        assert!(!c.approve_changes);
        let c = caps(&["GRANT INDEX ON *.* TO `dba`@`%`"], false);
        assert!(!c.change_schemas && c.approve_changes && !c.read_data);
        let c = caps(&["GRANT ALTER ON *.* TO `dba`@`%`"], false);
        assert!(c.change_schemas && c.approve_changes);

        // Logic alone.
        let c = caps(&["GRANT ALTER ROUTINE ON `shop`.* TO `l`@`%`"], false);
        assert!(c.manage_logic && !c.change_schemas);

        // User management and the tenant administrator.
        let c = caps(&["GRANT CREATE USER ON *.* TO `sec`@`%`"], true);
        assert!(c.manage_users && c.operator && !c.administer_tenant);
        let c = caps(&["GRANT SPRIG_TENANT_ADMIN ON *.* TO `boss`@`%`"], false);
        assert!(c.administer_tenant && c.manage_users && c.approve_changes);
        assert!(c.read_data && c.change_schemas && c.manage_logic);
        let c = caps(&["GRANT ALL PRIVILEGES ON `shop`.* TO `o`@`%`"], false);
        assert!(c.read_data && c.change_schemas && c.manage_logic && !c.administer_tenant);

        // Nothing.
        assert_eq!(
            caps(&["GRANT USAGE ON *.* TO `x`@`%`"], false),
            Capabilities::default()
        );
        let j = caps(&[], true).to_json();
        assert_eq!(j["operator"], true);
        assert_eq!(j.as_object().map(serde_json::Map::len), Some(7));
    }

    #[test]
    fn roles_come_from_role_grants() {
        let lines: Vec<String> = [
            "GRANT `dev` TO `a`@`%`",
            "GRANT `ops`@`%`, `Auditors` TO `a`@`%` WITH ADMIN OPTION",
            "GRANT SELECT ON `shop`.* TO `a`@`%`",
            "grant `dev` to `a`@`%`",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        assert_eq!(roles_of(&lines), ["dev", "ops", "Auditors"]);
        assert_eq!(
            privilege_names("SELECT (a, b), INSERT,  CREATE   ROUTINE"),
            ["SELECT", "INSERT", "CREATE ROUTINE"]
        );
    }

    #[test]
    fn odd_names_and_role_grants() {
        let p = of(&[
            "GRANT `dev`@`%` TO `a`@`%`",
            "GRANT ALTER ON `we.ird``db`.* TO `a`@`%`",
        ]);
        assert_eq!(p.index_dbs, ["we.ird`db"]);
        assert_eq!(target("`a`.`b.c`"), ("a".to_owned(), "b.c".to_owned()));
    }
}
