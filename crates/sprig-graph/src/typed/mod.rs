//! Typed graph storage (`graphs.storage = 2`): a table per label set and
//! per relationship type, a typed column per property key and value type,
//! and no JSON in the graph's tables.
//!
//! **Tables, in each shard schema of the graph's table group:**
//!
//! ```text
//! __sprig_gn_<g>_<cls>   the nodes of one label set (`cls`): id, part, _v, _x,
//!                        _sprig_rk, then p<key>_<code> per property key and type
//! __sprig_ge_<g>_<type>  the relationships of one type, on their source's shard:
//!                        src, dst, id, src_cls, dst_cls, dst_rk, _v, _x, _sprig_rk,
//!                        typed properties; PRIMARY KEY (src, dst, id), KEY (dst)
//! __sprig_gm_<g>_<type>  their mirrors, on the destination's shard: dst, src, id,
//!                        src_cls, src_rk, _v, _sprig_rk; PRIMARY KEY (dst, src, id)
//! __sprig_gd_<g>         the directory: id, cls (a node's label set by id)
//! __sprig_gv_<g>         values outside the columns: lists (one row per item) and
//!                        scalars whose column isn't there (yet); `_x = 1` on the
//!                        element's row says it has some
//! ```
//!
//! - **Labels** are not stored on rows: the table is the label set. A
//!   label change moves the row to the table of the new set ([`codec`]).
//! - **Columns** are `p<key id>_<code>`: one per property key and value
//!   type (`i` integer, `f` float, `s` string, `b` boolean, `d`/`lt`/`t`/
//!   `ldt`/`dt`/`du` the temporal types, `by` bytes, `pt` points, `fx`
//!   NaN and the infinities), so a key that holds values of two types has
//!   two columns, and a read takes the one that isn't `NULL`.
//! - **New keys never wait for DDL inside a data transaction.** A table
//!   made for a new label set or type gets the columns its first rows
//!   need. A column a later write needs is added at once only if the
//!   transaction hasn't touched the table (`ALTER … NOWAIT`, outside the
//!   transaction: otherwise it would wait for the transaction's own
//!   metadata lock); else the value goes to `__sprig_gv_` until a
//!   column is added after the commit.
//! - The schema (label sets, keys, columns) is catalog rows
//!   (`graph_label_sets`, `graph_prop_keys`, `graph_columns`), loaded into
//!   every snapshot ([`TypedSchema`]).

pub mod catalog;
pub mod codec;
pub mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::ast::Entity;

/// The layout stamp (`graphs.storage`) of typed storage.
pub const STORAGE_TYPED: u8 = 2;

/// Most columns a table gets; values past it go to `__sprig_gv_`.
pub const MAX_COLUMNS: usize = 480;

/// A value type: the column suffix and its SQL type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Code {
    /// 64-bit integer.
    Int,
    /// Finite float.
    Float,
    /// NaN or an infinity (as `NaN`, `Infinity`, `-Infinity`).
    FloatX,
    /// Boolean.
    Bool,
    /// String.
    Str,
    /// A temporal, point or bytes value: its `_types` name.
    Typed(TypedKind),
}

/// The kinds `_types` names (other than `float`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypedKind {
    /// `date`.
    Date,
    /// `localtime`.
    LocalTime,
    /// `time`.
    Time,
    /// `localdatetime`.
    LocalDateTime,
    /// `datetime`.
    DateTime,
    /// `duration`.
    Duration,
    /// `bytes`.
    Bytes,
    /// `point`.
    Point,
}

impl TypedKind {
    /// The `_types` name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::LocalTime => "localtime",
            Self::Time => "time",
            Self::LocalDateTime => "localdatetime",
            Self::DateTime => "datetime",
            Self::Duration => "duration",
            Self::Bytes => "bytes",
            Self::Point => "point",
        }
    }

    /// The kind of a `_types` name.
    #[must_use]
    pub fn of(name: &str) -> Option<Self> {
        Some(match name {
            "date" => Self::Date,
            "localtime" => Self::LocalTime,
            "time" => Self::Time,
            "localdatetime" => Self::LocalDateTime,
            "datetime" => Self::DateTime,
            "duration" => Self::Duration,
            "bytes" => Self::Bytes,
            "point" => Self::Point,
            _ => return None,
        })
    }
}

impl Code {
    /// The column suffix (and the catalog's code).
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Int => "i",
            Self::Float => "f",
            Self::FloatX => "fx",
            Self::Bool => "b",
            Self::Str => "s",
            Self::Typed(k) => match k {
                TypedKind::Date => "d",
                TypedKind::LocalTime => "lt",
                TypedKind::Time => "t",
                TypedKind::LocalDateTime => "ldt",
                TypedKind::DateTime => "dt",
                TypedKind::Duration => "du",
                TypedKind::Bytes => "by",
                TypedKind::Point => "pt",
            },
        }
    }

    /// The code of a suffix.
    #[must_use]
    pub fn of(s: &str) -> Option<Self> {
        Some(match s {
            "i" => Self::Int,
            "f" => Self::Float,
            "fx" => Self::FloatX,
            "b" => Self::Bool,
            "s" => Self::Str,
            "d" => Self::Typed(TypedKind::Date),
            "lt" => Self::Typed(TypedKind::LocalTime),
            "t" => Self::Typed(TypedKind::Time),
            "ldt" => Self::Typed(TypedKind::LocalDateTime),
            "dt" => Self::Typed(TypedKind::DateTime),
            "du" => Self::Typed(TypedKind::Duration),
            "by" => Self::Typed(TypedKind::Bytes),
            "pt" => Self::Typed(TypedKind::Point),
            _ => return None,
        })
    }

    /// The column's SQL type.
    #[must_use]
    pub fn sql_type(self) -> &'static str {
        match self {
            Self::Int => "BIGINT",
            Self::Float => "DOUBLE",
            Self::Bool => "TINYINT(1)",
            Self::FloatX => "VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin",
            Self::Str => "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            Self::Typed(TypedKind::Bytes) => "LONGBLOB",
            Self::Typed(_) => "VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        }
    }
}

/// A table's owner: a label set (`Entity::Node`, its class id) or a
/// relationship type (`Entity::Relationship`, the type id).
pub type Owner = (Entity, u32);

/// One graph's typed schema, as the catalog has it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypedSchema {
    /// Label sets by class id: their sorted label ids.
    pub classes: BTreeMap<u32, Vec<u32>>,
    /// Property keys: name to id.
    pub keys: BTreeMap<String, u32>,
    /// Columns per table: (key id, code).
    pub columns: BTreeMap<Owner, BTreeSet<(u32, Code)>>,
    /// Relationship types whose tables exist.
    pub types: BTreeSet<u32>,
    /// The classes a type's relationships may end at: (type, `true` for
    /// the destination's class, `false` for the source's, class). Recorded
    /// before the rows that need them, so a superset.
    pub ends: BTreeSet<(u32, bool, u32)>,
}

impl TypedSchema {
    /// The class of a label set (sorted ids), if it has one.
    #[must_use]
    pub fn class_of(&self, labels: &[u32]) -> Option<u32> {
        let mut l = labels.to_vec();
        l.sort_unstable();
        l.dedup();
        self.classes
            .iter()
            .find_map(|(c, ls)| (*ls == l).then_some(*c))
    }

    /// The classes whose label sets contain `label` (every class for
    /// `None`).
    #[must_use]
    pub fn classes_with(&self, label: Option<u32>) -> Vec<u32> {
        self.classes
            .iter()
            .filter(|(_, ls)| label.is_none_or(|l| ls.contains(&l)))
            .map(|(c, _)| *c)
            .collect()
    }

    /// The key name of an id.
    #[must_use]
    pub fn key_name(&self, id: u32) -> Option<&str> {
        self.keys
            .iter()
            .find_map(|(n, k)| (*k == id).then_some(n.as_str()))
    }

    /// The columns of a table, in order.
    #[must_use]
    pub fn columns_of(&self, owner: Owner) -> Vec<(u32, Code)> {
        self.columns
            .get(&owner)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }
    /// Adds what `other` has: the schema only grows (rows are added once
    /// their tables and columns exist, and stay), so two loads of it at
    /// different times merge into the later one.
    pub fn merge(&mut self, other: &Self) {
        for (c, ls) in &other.classes {
            self.classes.entry(*c).or_insert_with(|| ls.clone());
        }
        for (k, id) in &other.keys {
            self.keys.entry(k.clone()).or_insert(*id);
        }
        for (o, cols) in &other.columns {
            self.columns
                .entry(*o)
                .or_default()
                .extend(cols.iter().copied());
        }
        self.types.extend(other.types.iter().copied());
        self.ends.extend(other.ends.iter().copied());
    }

    /// The classes relationships of type `t` may lead to: their
    /// destinations' (`to_dst`) or their sources'.
    #[must_use]
    pub fn far_classes(&self, t: u32, to_dst: bool) -> Vec<u32> {
        self.ends
            .range((t, to_dst, 0)..=(t, to_dst, u32::MAX))
            .map(|(_, _, c)| *c)
            .collect()
    }
}

/// A column's name.
#[must_use]
pub fn column(key: u32, code: Code) -> String {
    format!("p{key}_{}", code.suffix())
}

/// The node table of class `cls` of graph `g` in `schema`.
#[must_use]
pub fn node_table(schema: &str, g: u64, cls: u32) -> String {
    qualified(schema, &format!("__sprig_gn_{g}_{cls}"))
}

/// The edge table of type `t`.
#[must_use]
pub fn edge_table(schema: &str, g: u64, t: u32) -> String {
    qualified(schema, &format!("__sprig_ge_{g}_{t}"))
}

/// The mirror table of type `t`.
#[must_use]
pub fn mirror_table(schema: &str, g: u64, t: u32) -> String {
    qualified(schema, &format!("__sprig_gm_{g}_{t}"))
}

/// The directory table.
#[must_use]
pub fn dir_table(schema: &str, g: u64) -> String {
    qualified(schema, &format!("__sprig_gd_{g}"))
}

/// The values table (lists, and scalars outside the columns).
#[must_use]
pub fn values_table(schema: &str, g: u64) -> String {
    qualified(schema, &format!("__sprig_gv_{g}"))
}

fn qualified(schema: &str, name: &str) -> String {
    format!("`{}`.`{name}`", schema.replace('`', "``"))
}

/// The table of `owner` in `schema`.
#[must_use]
pub fn table_of(schema: &str, g: u64, owner: Owner) -> String {
    match owner.0 {
        Entity::Node => node_table(schema, g, owner.1),
        Entity::Relationship => edge_table(schema, g, owner.1),
    }
}

/// `CREATE TABLE IF NOT EXISTS` for the directory and the values table of
/// one shard.
#[must_use]
pub fn base_ddl(schema: &str, g: u64) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  id BIGINT UNSIGNED NOT NULL, cls INT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (id), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
            dir_table(schema, g)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  kind CHAR(1) CHARACTER SET ascii NOT NULL, id BIGINT UNSIGNED NOT NULL,
  key_id INT UNSIGNED NOT NULL, ord INT UNSIGNED NOT NULL,
  code VARCHAR(4) CHARACTER SET ascii NOT NULL,
  v_i BIGINT NULL, v_f DOUBLE NULL, v_s LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
  v_b LONGBLOB NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (kind, id, key_id, ord), KEY by_key (kind, key_id, id), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
            values_table(schema, g)
        ),
        // Tables made before the key was: its values found by key.
        format!(
            "ALTER TABLE {} ADD INDEX IF NOT EXISTS by_key (kind, key_id, id)",
            values_table(schema, g)
        ),
    ]
}

fn key_defs(keys: &[String]) -> String {
    let mut out = String::new();
    for k in keys {
        let _ = write!(out, ", KEY {k}");
    }
    out
}

fn column_defs(cols: &[(u32, Code)]) -> String {
    let mut out = String::new();
    for (key, code) in cols {
        let _ = write!(out, ", {} {} NULL", column(*key, *code), code.sql_type());
    }
    out
}

/// `CREATE TABLE IF NOT EXISTS` for a node class's table with `columns`.
#[must_use]
pub fn class_ddl(
    schema: &str,
    g: u64,
    cls: u32,
    columns: &[(u32, Code)],
    keys: &[String],
) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (
  id BIGINT UNSIGNED NOT NULL, part BIGINT UNSIGNED NOT NULL,
  _v BIGINT UNSIGNED NOT NULL, _x TINYINT(1) NOT NULL DEFAULT 0,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE{},
  PRIMARY KEY (id), KEY _sprig_rk (_sprig_rk){}
) ENGINE=InnoDB",
        node_table(schema, g, cls),
        column_defs(columns),
        key_defs(keys)
    )
}

/// `CREATE TABLE IF NOT EXISTS` for a type's edge and mirror tables.
#[must_use]
pub fn type_ddl(
    schema: &str,
    g: u64,
    t: u32,
    cols: &[(u32, Code)],
    keys: &[String],
) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  src BIGINT UNSIGNED NOT NULL, dst BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  src_cls INT UNSIGNED NOT NULL, dst_cls INT UNSIGNED NOT NULL, dst_rk BIGINT UNSIGNED NOT NULL,
  _v BIGINT UNSIGNED NOT NULL, _x TINYINT(1) NOT NULL DEFAULT 0,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE{},
  PRIMARY KEY (src, dst, id), UNIQUE KEY uk_id (id), KEY rev (dst, src), KEY _sprig_rk (_sprig_rk){}
) ENGINE=InnoDB",
            edge_table(schema, g, t),
            column_defs(cols),
            key_defs(keys)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  dst BIGINT UNSIGNED NOT NULL, src BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  src_cls INT UNSIGNED NOT NULL, src_rk BIGINT UNSIGNED NOT NULL, _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (dst, src, id), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
            mirror_table(schema, g, t)
        ),
    ]
}

/// `ALTER TABLE … NOWAIT` adding `cols` to `owner`'s table (instant:
/// nullable columns at the end). `NOWAIT`: a table another transaction
/// holds isn't waited for; the caller keeps the values aside instead.
#[must_use]
pub fn add_columns_ddl(schema: &str, g: u64, owner: Owner, cols: &[(u32, Code)]) -> String {
    let parts: Vec<String> = cols
        .iter()
        .map(|(k, c)| {
            format!(
                "ADD COLUMN IF NOT EXISTS {} {} NULL",
                column(*k, *c),
                c.sql_type()
            )
        })
        .collect();
    format!(
        "ALTER TABLE {} NOWAIT {}, ALGORITHM=INSTANT",
        table_of(schema, g, owner),
        parts.join(", ")
    )
}

/// Whether index `d` covers `owner`'s table: a node index on a label
/// the class's set has, or a relationship index on the type.
#[must_use]
pub fn index_covers(s: &TypedSchema, owner: Owner, d: &crate::store::IndexDef) -> bool {
    if d.entity != owner.0 {
        return false;
    }
    match owner.0 {
        Entity::Node => s
            .classes
            .get(&owner.1)
            .is_some_and(|ls| ls.contains(&d.token_id)),
        Entity::Relationship => owner.1 == d.token_id,
    }
}

/// The indexes `d` needs on `owner`'s table with columns `cols`: one per
/// column of its first property (a prefix of long text and bytes), as
/// `ix<id>_<code> (<column>)`.
#[must_use]
pub fn index_specs(
    s: &TypedSchema,
    owner: Owner,
    cols: &[(u32, Code)],
    d: &crate::store::IndexDef,
) -> Vec<String> {
    if !index_covers(s, owner, d) {
        return Vec::new();
    }
    let Some(key) = d.props.first().and_then(|p| s.keys.get(p)) else {
        return Vec::new();
    };
    cols.iter()
        .filter(|(k, _)| k == key)
        .map(|(k, c)| {
            let prefix = match c {
                Code::Str | Code::Typed(TypedKind::Bytes) => "(255)",
                _ => "",
            };
            format!("ix{}_{} ({}{prefix})", d.id, c.suffix(), column(*k, *c))
        })
        .collect()
}

/// The `ADD INDEX` clauses index `d` needs on `owner`'s table.
#[must_use]
pub fn index_clauses(s: &TypedSchema, owner: Owner, d: &crate::store::IndexDef) -> Vec<String> {
    index_specs(s, owner, &s.columns_of(owner), d)
        .into_iter()
        .map(|x| format!("ADD INDEX IF NOT EXISTS {x}"))
        .collect()
}

/// `ALTER TABLE` adding `clauses` to `owner`'s table without blocking
/// writers (`NOWAIT` when `nowait`: a busy table answers 1205 at once).
#[must_use]
pub fn add_index_ddl(
    schema: &str,
    g: u64,
    owner: Owner,
    clauses: &[String],
    nowait: bool,
) -> String {
    format!(
        "ALTER TABLE {}{} {}, LOCK=NONE",
        table_of(schema, g, owner),
        if nowait { " NOWAIT" } else { "" },
        clauses.join(", ")
    )
}

/// Statements copying `owner`'s values of key `key` with type `code` that
/// went to the values table into its column (once it exists), on one
/// shard: rows a later write rewrote already have it.
#[must_use]
pub fn promote_sql(schema: &str, g: u64, owner: Owner, key: u32, code: Code) -> String {
    let kind = match owner.0 {
        Entity::Node => 'n',
        Entity::Relationship => 'e',
    };
    let v = match code {
        Code::Int | Code::Bool => "v_i",
        Code::Float => "v_f",
        Code::Typed(TypedKind::Bytes) => "v_b",
        _ => "v_s",
    };
    format!(
        "UPDATE {} t JOIN {} v ON v.kind = '{kind}' AND v.id = t.id AND v.key_id = {key} \
         AND v.ord = {} AND v.code = '{}' SET t.{} = v.{v}",
        table_of(schema, g, owner),
        values_table(schema, g),
        codec::SCALAR,
        code.suffix(),
        column(key, code)
    )
}

/// Statements removing values-table scalars of `key`/`code` whose column
/// holds them now, and clearing `_x` of rows left without values-table
/// rows (once every reader knows the column).
#[must_use]
pub fn cleanup_sql(schema: &str, g: u64, owner: Owner, key: u32, code: Code) -> Vec<String> {
    let kind = match owner.0 {
        Entity::Node => 'n',
        Entity::Relationship => 'e',
    };
    let t = table_of(schema, g, owner);
    let vt = values_table(schema, g);
    vec![
        format!(
            "DELETE v FROM {vt} v JOIN {t} t ON t.id = v.id WHERE v.kind = '{kind}' \
             AND v.key_id = {key} AND v.ord = {} AND v.code = '{}' AND t.{} IS NOT NULL",
            codec::SCALAR,
            code.suffix(),
            column(key, code)
        ),
        format!(
            "UPDATE {t} t SET t._x = 0 WHERE t._x = 1 AND NOT EXISTS \
             (SELECT 1 FROM {vt} v WHERE v.kind = '{kind}' AND v.id = t.id)"
        ),
    ]
}

/// `ALTER TABLE` dropping index `id`'s indexes from `owner`'s table.
#[must_use]
pub fn drop_index_ddl(schema: &str, g: u64, owner: Owner, id: u32) -> String {
    let codes = [
        Code::Int,
        Code::Float,
        Code::FloatX,
        Code::Bool,
        Code::Str,
        Code::Typed(TypedKind::Date),
        Code::Typed(TypedKind::LocalTime),
        Code::Typed(TypedKind::Time),
        Code::Typed(TypedKind::LocalDateTime),
        Code::Typed(TypedKind::DateTime),
        Code::Typed(TypedKind::Duration),
        Code::Typed(TypedKind::Bytes),
        Code::Typed(TypedKind::Point),
    ];
    let parts: Vec<String> = codes
        .iter()
        .map(|c| format!("DROP INDEX IF EXISTS ix{id}_{}", c.suffix()))
        .collect();
    format!(
        "ALTER TABLE {} {}",
        table_of(schema, g, owner),
        parts.join(", ")
    )
}

/// `DROP TABLE IF EXISTS` for every typed table of a graph in one shard
/// schema: the names come from the schema, plus the base tables.
#[must_use]
pub fn drop_ddl(schema: &str, g: u64, s: &TypedSchema) -> Vec<String> {
    let mut v = vec![
        format!("DROP TABLE IF EXISTS {}", dir_table(schema, g)),
        format!("DROP TABLE IF EXISTS {}", values_table(schema, g)),
    ];
    for c in s.classes.keys() {
        v.push(format!(
            "DROP TABLE IF EXISTS {}",
            node_table(schema, g, *c)
        ));
    }
    for t in &s.types {
        v.push(format!(
            "DROP TABLE IF EXISTS {}",
            edge_table(schema, g, *t)
        ));
        v.push(format!(
            "DROP TABLE IF EXISTS {}",
            mirror_table(schema, g, *t)
        ));
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip_and_name_columns() {
        for c in [
            Code::Int,
            Code::Float,
            Code::FloatX,
            Code::Bool,
            Code::Str,
            Code::Typed(TypedKind::Date),
            Code::Typed(TypedKind::Point),
            Code::Typed(TypedKind::Bytes),
        ] {
            assert_eq!(Code::of(c.suffix()), Some(c));
        }
        assert_eq!(column(12, Code::Int), "p12_i");
        let mut s = TypedSchema::default();
        s.classes.insert(1, vec![10, 12]);
        s.classes.insert(2, vec![11, 12]);
        s.classes.insert(3, vec![14]);
        assert_eq!(s.class_of(&[12, 10]), Some(1));
        assert_eq!(s.class_of(&[12]), None);
        assert_eq!(s.classes_with(Some(12)), [1, 2]);
        assert_eq!(s.classes_with(None), [1, 2, 3]);
        let d = class_ddl(
            "s1",
            9,
            3,
            &[(1, Code::Int), (2, Code::Str)],
            &["ix4_i (p1_i)".into()],
        );
        assert!(d.contains(", KEY ix4_i (p1_i)"));
        assert!(d.contains("`s1`.`__sprig_gn_9_3`"));
        assert!(d.contains(", p1_i BIGINT NULL, p2_s LONGTEXT"));
        let a = add_columns_ddl("s1", 9, (Entity::Relationship, 4), &[(5, Code::Float)]);
        assert_eq!(
            a,
            "ALTER TABLE `s1`.`__sprig_ge_9_4` NOWAIT ADD COLUMN IF NOT EXISTS p5_f DOUBLE NULL, ALGORITHM=INSTANT"
        );
    }

    #[test]
    fn indexes_cover_the_classes_with_their_label() {
        let mut s = TypedSchema::default();
        s.classes.insert(1, vec![10, 12]);
        s.classes.insert(2, vec![11]);
        s.keys.insert("id".into(), 3);
        s.columns.entry((Entity::Node, 1)).or_default().extend([
            (3, Code::Int),
            (3, Code::Str),
            (4, Code::Int),
        ]);
        let d = crate::store::IndexDef {
            id: 7,
            name: "i".into(),
            unique: false,
            kind: crate::ast::IndexKind::Range,
            entity: Entity::Node,
            token: "L".into(),
            token_id: 12,
            props: vec!["id".into()],
            state: "online".into(),
        };
        assert_eq!(
            index_clauses(&s, (Entity::Node, 1), &d),
            [
                "ADD INDEX IF NOT EXISTS ix7_i (p3_i)",
                "ADD INDEX IF NOT EXISTS ix7_s (p3_s(255))"
            ]
        );
        assert!(index_clauses(&s, (Entity::Node, 2), &d).is_empty());
        assert_eq!(
            add_index_ddl(
                "s1",
                9,
                (Entity::Node, 1),
                &["ADD INDEX IF NOT EXISTS ix7_i (p3_i)".into()],
                true
            ),
            "ALTER TABLE `s1`.`__sprig_gn_9_1` NOWAIT ADD INDEX IF NOT EXISTS ix7_i (p3_i), LOCK=NONE"
        );
        assert!(
            drop_index_ddl("s1", 9, (Entity::Node, 1), 7).contains("DROP INDEX IF EXISTS ix7_dt")
        );
    }
}
