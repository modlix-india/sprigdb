//! Redis databases read from SQL: the keys of Redis database `n` of the
//! tenant, and their elements, read as five read-only tables of the SQL
//! database named by `redis.logical_database` (`redis` by default).
//!
//! ```text
//! db<n>          k VARBINARY, type VARCHAR ('string', 'hash', 'list', 'set', 'zset'),
//!                v LONGBLOB (a string's value, else NULL),
//!                expires_at DATETIME(3) (UTC, NULL: no expiry)   PRIMARY (k)
//! db<n>__hash    k, field VARBINARY, value LONGBLOB              PRIMARY (k, field)
//! db<n>__list    k, idx BIGINT UNSIGNED (0 = the head), item     PRIMARY (k, idx)
//! db<n>__set     k, member VARBINARY                             PRIMARY (k, member)
//! db<n>__zset    k, member, score DOUBLE (NULL: an infinity),
//!                score_class TINYINT (-1, 0, 1)                  PRIMARY (k, member)
//! ```
//!
//! - **At the statement's view.** The rows are read from the Redis
//!   database's keys in the statement's engine transaction
//!   (`general::storage::redis`), so a join with tables, collections or
//!   another Redis table reads one snapshot.
//! - **Hidden:** keys and hash fields whose expiry has passed.
//! - **Read-only.** A write naming one answers 39729; a locking read is
//!   refused as for a stream.
//! - **Which names.** `db0` up to `db<redis.databases - 1>`, with or without
//!   a suffix, in the SQL database of `redis.logical_database`, when the
//!   tenant has an SQL database of that name; a table, view, sequence,
//!   collection or stream of the same name comes first. Privileges are as
//!   for a stream: `SELECT` on the table's name or on the database.
//! - **Rows** come in primary-key order (keys in byte order; a key's
//!   elements by field, position or member).

use std::sync::{Arc, PoisonError, RwLock};

use sprig_common::ids::{DbId, ObjectId};
use sprig_redis_engine::layout::Space;

use super::streamtable::StreamSource;
use super::table;

/// Where Redis tables' objects are numbered (apart from tables',
/// collections', streams', queues' and temporary tables').
const REDIS_BASE: u64 = 0x9000_0000;

/// One of a Redis database's tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rel {
    /// `db<n>`: one row per key.
    Keys,
    /// `db<n>__hash`: one row per hash field.
    Hash,
    /// `db<n>__list`: one row per list item.
    List,
    /// `db<n>__set`: one row per set member.
    Set,
    /// `db<n>__zset`: one row per sorted-set member.
    ZSet,
}

impl Rel {
    const ALL: [(Rel, &'static str); 5] = [
        (Rel::Keys, ""),
        (Rel::Hash, "__hash"),
        (Rel::List, "__list"),
        (Rel::Set, "__set"),
        (Rel::ZSet, "__zset"),
    ];

    fn number(self) -> u64 {
        match self {
            Rel::Keys => 0,
            Rel::Hash => 1,
            Rel::List => 2,
            Rel::Set => 3,
            Rel::ZSet => 4,
        }
    }

    /// The columns after `k`, and the primary key's other columns.
    fn columns(self) -> (&'static str, &'static str) {
        match self {
            Rel::Keys => (
                "  `type` varchar(6) NOT NULL,\n  `v` longblob DEFAULT NULL,\n  `expires_at` datetime(3) DEFAULT NULL,\n",
                "",
            ),
            Rel::Hash => (
                "  `field` varbinary(2024) NOT NULL,\n  `value` longblob DEFAULT NULL,\n",
                ",`field`",
            ),
            Rel::List => (
                "  `idx` bigint(20) unsigned NOT NULL,\n  `item` longblob DEFAULT NULL,\n",
                ",`idx`",
            ),
            Rel::Set => ("  `member` varbinary(2024) NOT NULL,\n", ",`member`"),
            Rel::ZSet => (
                "  `member` varbinary(2024) NOT NULL,\n  `score` double DEFAULT NULL,\n  `score_class` tinyint(4) NOT NULL,\n",
                ",`member`",
            ),
        }
    }
}

/// A Redis table as a statement reads it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RedisTable {
    /// The Redis database's keyspace.
    pub space: Space,
    /// Which of its tables.
    pub rel: Rel,
}

/// `redis.logical_database` and `redis.databases`, as the Redis front end
/// last read them.
fn config() -> &'static RwLock<(String, u32)> {
    static C: std::sync::OnceLock<RwLock<(String, u32)>> = std::sync::OnceLock::new();
    C.get_or_init(|| RwLock::new(("redis".to_owned(), 16)))
}

/// The Redis front end's settings: the SQL database the Redis databases
/// show in, and how many there are.
pub(crate) fn note_config(logical_database: &str, databases: u32) {
    let mut g = config().write().unwrap_or_else(PoisonError::into_inner);
    *g = (logical_database.to_owned(), databases);
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn table_text(name: &str, rel: Rel) -> String {
    let (cols, pk) = rel.columns();
    format!(
        "CREATE TABLE {} (\n  `k` varbinary(1024) NOT NULL,\n{cols}  PRIMARY KEY (`k`{pk})\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        quote(name)
    )
}

/// The database number and table of name `name` (`db3`, `db3__hash`);
/// `None` when it names none.
fn parse_name(name: &str, lower_case: bool, databases: u32) -> Option<(u32, Rel)> {
    let lower = name.to_ascii_lowercase();
    let n = if lower_case { lower.as_str() } else { name };
    let rest = n.strip_prefix("db")?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (num, suffix) = rest.split_at(digits);
    // `db0`, `db12`; never `db`, `db01` or `db+1`.
    if num.is_empty() || (num.len() > 1 && num.starts_with('0')) || num.len() > 4 {
        return None;
    }
    let db: u32 = num.parse().ok()?;
    if db >= databases {
        return None;
    }
    let rel = Rel::ALL
        .iter()
        .find(|(_, s)| *s == suffix)
        .map(|(r, _)| *r)?;
    Some((db, rel))
}

/// Table `name` of SQL database `db` (named `db_name`) of `tenant`, when it
/// is one of the tenant's Redis databases' tables.
pub(crate) fn find(
    tenant: u64,
    db: DbId,
    db_name: &str,
    name: &str,
    lower_case: bool,
) -> Option<Arc<StreamSource>> {
    let (home, databases) = config()
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let same_db = if lower_case {
        db_name.eq_ignore_ascii_case(&home)
    } else {
        db_name == home
    };
    if !same_db {
        return None;
    }
    let (n, rel) = parse_name(name, lower_case, databases)?;
    let object = ObjectId(REDIS_BASE + (u64::from(n) << 3) + rel.number());
    let def = table::parse(object, 1, db, &table_text(name, rel)).ok()?;
    let t = RedisTable {
        space: Space::redis(tenant, n),
        rel,
    };
    Some(Arc::new(StreamSource {
        def,
        storage: Arc::new(Unread),
        redis: Some(t),
    }))
}

/// The storage a Redis table's source carries: the statement's storage
/// reads Redis tables itself, in its transaction (`general::storage`), so
/// this is never asked for rows.
struct Unread;

fn unread() -> sprig_exec::SqlError {
    sprig_exec::SqlError::new(
        1105,
        "HY000",
        "engine: a Redis table read outside a statement's transaction",
    )
}

impl sprig_plan::access::Storage for Unread {
    fn table_rows(&self, _table: &sprig_plan::catalog::TableDef) -> u64 {
        0
    }

    fn rows_in_range(
        &self,
        _table: &sprig_plan::catalog::TableDef,
        _key: sprig_plan::catalog::KeyId,
        _range: &sprig_plan::access::KeyRange,
    ) -> u64 {
        0
    }

    fn rows_per_key(
        &self,
        _table: &sprig_plan::catalog::TableDef,
        _key: sprig_plan::catalog::KeyId,
        _parts: usize,
    ) -> Option<f64> {
        None
    }

    fn get(
        &self,
        _table: &sprig_plan::catalog::TableDef,
        _key: &[sprig_engine::row::Value],
        _counters: &sprig_plan::access::Counters,
    ) -> Result<Option<sprig_exec::ops::Row>, sprig_exec::SqlError> {
        Err(unread())
    }

    fn scan(
        &self,
        _table: &sprig_plan::catalog::TableDef,
        _key: sprig_plan::catalog::KeyId,
        _ranges: Vec<sprig_plan::access::KeyRange>,
        _reverse: bool,
        _fetch: sprig_plan::access::Fetch,
        _counters: Arc<sprig_plan::access::Counters>,
    ) -> Result<Box<dyn sprig_plan::access::RowCursor>, sprig_exec::SqlError> {
        Err(unread())
    }
}

/// 39729: a write naming a Redis database's table.
pub(crate) fn read_only(name: &str) -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::KvSqlReadOnly,
        format!(
            "'{name}' is a Redis database read as a table: it is read-only from SQL (write it with Redis commands)"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_of_redis_tables() {
        assert_eq!(parse_name("db0", true, 16), Some((0, Rel::Keys)));
        assert_eq!(parse_name("DB15__Hash", true, 16), Some((15, Rel::Hash)));
        assert_eq!(parse_name("DB15__Hash", false, 16), None);
        assert_eq!(parse_name("db3__zset", false, 16), Some((3, Rel::ZSet)));
        assert_eq!(parse_name("db16", true, 16), None);
        assert_eq!(parse_name("db01", true, 16), None);
        assert_eq!(parse_name("db", true, 16), None);
        assert_eq!(parse_name("db1__other", true, 16), None);
        assert_eq!(parse_name("orders", true, 16), None);
    }

    #[test]
    fn tables_parse() {
        for (rel, _) in Rel::ALL {
            let def = table::parse(ObjectId(REDIS_BASE), 1, DbId(1), &table_text("db0", rel))
                .expect("parses");
            assert_eq!(def.columns[0].name, "k");
            assert!(!def.pk.is_empty());
        }
    }
}
