//! Studio's workspaces in the TUI: the same workspace setting (the user's
//! own choice, kept in the tenant's catalog, else `studio.default_workspace`,
//! else the widest the user qualifies for) and the same sections, as far as
//! a terminal has them.
//!
//! | Studio section | In the TUI |
//! |---|---|
//! | Explorer | the explorer pane (the analyst's lists tables and views only) |
//! | Query, Saved queries, History | the data tabs and the palette |
//! | Performance | the Inbox screen (approvals) |
//! | Operator mode | the Dashboard and Admin screens |
//! | Dashboards, Messaging, Logic, Inheritance, Access control, Settings, Docs | browser pages: not in the TUI |
//!
//! A workspace changes what the TUI shows, never what the user may do: the
//! gateway checks every statement. The analyst's tabs start read-only, which
//! stops writes before they are sent until the user turns it off.

use serde_json::Value;

use crate::lang::{Class, Lang, classify_sql, top_words};

/// A tenant workspace (operator mode is the Dashboard and Admin screens).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workspace {
    /// The tenant administrator's.
    TenantAdmin,
    /// The database administrator's.
    DbAdmin,
    /// The security administrator's.
    SecurityAdmin,
    /// The developer's.
    Developer,
    /// The analyst's.
    Analyst,
}

/// Widest first, as Studio orders them.
pub const ALL: [Workspace; 5] = [
    Workspace::TenantAdmin,
    Workspace::DbAdmin,
    Workspace::SecurityAdmin,
    Workspace::Developer,
    Workspace::Analyst,
];

/// `WhoAmI`'s capability summary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // one flag per capability, as WhoAmI has them
pub struct Caps {
    /// Can read data.
    pub read_data: bool,
    /// Can change schemas.
    pub change_schemas: bool,
    /// Can manage logic.
    pub manage_logic: bool,
    /// Can manage users.
    pub manage_users: bool,
    /// Can approve changes for every database.
    pub approve_changes: bool,
    /// Administers the tenant.
    pub administer_tenant: bool,
    /// Holds operator rights.
    pub operator: bool,
}

impl Caps {
    /// The summary in a `WhoAmI` reply; `None` from a gateway without one.
    #[must_use]
    pub fn of(whoami: &Value) -> Option<Self> {
        let c = whoami.get("capabilities")?.as_object()?;
        let b = |k: &str| c.get(k).and_then(Value::as_bool).unwrap_or(false);
        Some(Self {
            read_data: b("read_data"),
            change_schemas: b("change_schemas"),
            manage_logic: b("manage_logic"),
            manage_users: b("manage_users"),
            approve_changes: b("approve_changes"),
            administer_tenant: b("administer_tenant"),
            operator: b("operator"),
        })
    }
}

impl Workspace {
    /// The name Studio and `studio.default_workspace` use.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::TenantAdmin => "tenant_admin",
            Self::DbAdmin => "db_admin",
            Self::SecurityAdmin => "security_admin",
            Self::Developer => "developer",
            Self::Analyst => "analyst",
        }
    }

    /// The name shown.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::TenantAdmin => "Tenant administrator",
            Self::DbAdmin => "Database administrator",
            Self::SecurityAdmin => "Security administrator",
            Self::Developer => "Developer",
            Self::Analyst => "Analyst",
        }
    }

    /// The workspace named `id`.
    #[must_use]
    pub fn from_id(id: &str) -> Option<Self> {
        ALL.into_iter().find(|w| w.id() == id)
    }

    /// Whether `caps` open this workspace.
    #[must_use]
    pub fn opens(self, caps: &Caps) -> bool {
        match self {
            Self::TenantAdmin => caps.administer_tenant,
            Self::DbAdmin => caps.approve_changes,
            Self::SecurityAdmin => caps.manage_users,
            Self::Developer => caps.change_schemas || caps.manage_logic,
            Self::Analyst => caps.read_data,
        }
    }

    /// Whether the Inbox screen (Studio's Performance section) shows.
    #[must_use]
    pub fn shows_inbox(self) -> bool {
        matches!(self, Self::TenantAdmin | Self::DbAdmin | Self::Developer)
    }

    /// Whether the explorer pane (Studio's Explorer in the activity bar) shows.
    #[must_use]
    pub fn shows_explorer(self) -> bool {
        self != Self::SecurityAdmin
    }

    /// The analyst's simplifications: tables and views only in the tree,
    /// tabs that start read-only.
    #[must_use]
    pub fn simple(self) -> bool {
        self == Self::Analyst
    }
}

/// The workspaces `caps` open, widest first; the analyst's when none does.
#[must_use]
pub fn open(caps: &Caps) -> Vec<Workspace> {
    let v: Vec<Workspace> = ALL.into_iter().filter(|w| w.opens(caps)).collect();
    if v.is_empty() {
        vec![Workspace::Analyst]
    } else {
        v
    }
}

/// The workspace to start in: the user's own last choice, then the default
/// set for them, then the widest open; choices not open are skipped.
#[must_use]
pub fn choose(caps: &Caps, chosen: Option<&str>, default: Option<&str>) -> Workspace {
    let open = open(caps);
    let pick = |id: Option<&str>| id.and_then(Workspace::from_id).filter(|w| open.contains(w));
    pick(chosen).or_else(|| pick(default)).unwrap_or(open[0])
}

/// From a `WhoAmI` reply: the workspace to start in, those open, and the
/// capabilities; `None` from a gateway without workspaces.
#[must_use]
pub fn from_whoami(v: &Value) -> Option<(Workspace, Vec<Workspace>, Caps)> {
    let caps = Caps::of(v)?;
    let ws = v.get("workspace");
    let s = |k: &str| ws.and_then(|w| w.get(k)).and_then(Value::as_str);
    Some((choose(&caps, s("chosen"), s("default")), open(&caps), caps))
}

const CYPHER_WRITES: &[&str] = &[
    "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "DROP", "LOAD", "CALL", "FOREACH",
];

const REDIS_READS: &[&str] = &[
    "GET",
    "MGET",
    "STRLEN",
    "GETRANGE",
    "EXISTS",
    "TTL",
    "PTTL",
    "TYPE",
    "KEYS",
    "SCAN",
    "HGET",
    "HMGET",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HEXISTS",
    "HSCAN",
    "LRANGE",
    "LLEN",
    "LINDEX",
    "SMEMBERS",
    "SISMEMBER",
    "SCARD",
    "SSCAN",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZREVRANGE",
    "ZSCORE",
    "ZCARD",
    "ZCOUNT",
    "ZRANK",
    "ZSCAN",
    "PING",
    "ECHO",
    "INFO",
    "DBSIZE",
    "TIME",
    "FT.SEARCH",
    "FT.INFO",
    "FT._LIST",
    "FT.AGGREGATE",
    "SELECT",
];

const MONGO_READS: &[&str] = &[
    ".find(",
    ".findOne(",
    ".countDocuments(",
    ".estimatedDocumentCount(",
    ".distinct(",
    ".getIndexes(",
    ".getCollectionNames(",
    ".stats(",
];

/// Whether a read-only tab may send `stmt`: reads, transaction control,
/// `USE` and `SET`; anything the TUI can't tell is a read is stopped. This
/// is the TUI's switch, not an access check.
#[must_use]
pub fn reads_only(stmt: &str, lang: Lang) -> bool {
    match lang {
        Lang::Sql => match classify_sql(stmt) {
            Class::Read | Class::Transaction => true,
            Class::Other => matches!(
                top_words(stmt, false).first().map(String::as_str),
                Some("USE" | "SET")
            ),
            Class::Write | Class::Ddl | Class::Account => false,
        },
        Lang::Cypher => !top_words(stmt, true)
            .iter()
            .any(|w| CYPHER_WRITES.contains(&w.as_str())),
        Lang::Redis => stmt
            .split_whitespace()
            .next()
            .is_some_and(|c| REDIS_READS.contains(&c.to_ascii_uppercase().as_str())),
        Lang::Mongo => {
            let t = stmt.trim();
            t.starts_with("show ")
                || (MONGO_READS.iter().any(|m| t.contains(m))
                    && !t.contains("$out")
                    && !t.contains("$merge"))
                || (t.contains(".aggregate(") && !t.contains("$out") && !t.contains("$merge"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn caps(f: impl Fn(&mut Caps)) -> Caps {
        let mut c = Caps::default();
        f(&mut c);
        c
    }

    #[test]
    fn each_single_capability_opens_one_workspace() {
        let one = |c: Caps| open(&c);
        assert_eq!(one(caps(|c| c.read_data = true)), [Workspace::Analyst]);
        assert_eq!(
            one(caps(|c| c.change_schemas = true)),
            [Workspace::Developer]
        );
        assert_eq!(one(caps(|c| c.manage_logic = true)), [Workspace::Developer]);
        assert_eq!(
            one(caps(|c| c.approve_changes = true)),
            [Workspace::DbAdmin]
        );
        assert_eq!(
            one(caps(|c| c.manage_users = true)),
            [Workspace::SecurityAdmin]
        );
        assert_eq!(
            one(caps(|c| c.administer_tenant = true)),
            [Workspace::TenantAdmin]
        );
        assert_eq!(one(Caps::default()), [Workspace::Analyst]);
    }

    #[test]
    fn the_start_follows_studios_order() {
        let c = caps(|c| {
            c.read_data = true;
            c.change_schemas = true;
            c.approve_changes = true;
        });
        assert_eq!(
            choose(&c, Some("analyst"), Some("developer")),
            Workspace::Analyst
        );
        assert_eq!(choose(&c, None, Some("developer")), Workspace::Developer);
        assert_eq!(choose(&c, Some("tenant_admin"), None), Workspace::DbAdmin);
        assert_eq!(
            choose(&c, Some("operator"), Some("wizard")),
            Workspace::DbAdmin
        );
        let v = json!({
            "capabilities": {"read_data": true, "manage_users": true},
            "workspace": {"chosen": null, "default": "security_admin"},
        });
        let (ws, open, caps) = from_whoami(&v).expect("workspaces");
        assert_eq!(ws, Workspace::SecurityAdmin);
        assert_eq!(open, [Workspace::SecurityAdmin, Workspace::Analyst]);
        assert!(caps.manage_users && !caps.operator);
        assert!(from_whoami(&json!({"principal": {}})).is_none());
    }

    #[test]
    fn sections_per_workspace() {
        let inbox: Vec<&str> = ALL
            .into_iter()
            .filter(|w| w.shows_inbox())
            .map(Workspace::id)
            .collect();
        assert_eq!(inbox, ["tenant_admin", "db_admin", "developer"]);
        assert!(!Workspace::SecurityAdmin.shows_explorer());
        assert!(Workspace::Analyst.shows_explorer() && Workspace::Analyst.simple());
        assert!(!Workspace::Developer.simple());
        for w in ALL {
            assert_eq!(Workspace::from_id(w.id()), Some(w));
        }
    }

    #[test]
    fn read_only_tabs_send_only_reads() {
        assert!(reads_only("SELECT * FROM t", Lang::Sql));
        assert!(reads_only("show tables", Lang::Sql));
        assert!(reads_only("BEGIN", Lang::Sql));
        assert!(reads_only("USE shop", Lang::Sql));
        assert!(!reads_only("INSERT INTO t VALUES (1)", Lang::Sql));
        assert!(!reads_only("DROP TABLE t", Lang::Sql));
        assert!(!reads_only("GRANT SELECT ON *.* TO u", Lang::Sql));
        assert!(!reads_only("KILL 12", Lang::Sql));
        assert!(reads_only("MATCH (n) RETURN n LIMIT 5", Lang::Cypher));
        assert!(!reads_only("MATCH (n) SET n.x = 1", Lang::Cypher));
        assert!(reads_only("get k", Lang::Redis));
        assert!(!reads_only("SET k v", Lang::Redis));
        assert!(!reads_only("FLUSHDB", Lang::Redis));
        assert!(reads_only("db.orders.find({})", Lang::Mongo));
        assert!(reads_only(
            "db.orders.aggregate([{$match: {}}])",
            Lang::Mongo
        ));
        assert!(!reads_only(
            "db.orders.aggregate([{$out: 'x'}])",
            Lang::Mongo
        ));
        assert!(!reads_only("db.orders.insertOne({a: 1})", Lang::Mongo));
    }
}
