//! What the planner knows about tables: columns, the primary key,
//! secondary indexes, and where they are found by name.

use std::sync::Arc;

use sprig_engine::keys::typed::KeyColumn;
use sprig_engine::row::ColumnType;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column's name as declared.
    pub name: String,
    /// The declared type.
    pub ty: ColumnType,
    /// Whether the column can hold `NULL`.
    pub nullable: bool,
    /// `INVISIBLE`: not part of `*`.
    pub invisible: bool,
    /// A character column whose character set is `utf8mb3` (its type
    /// holds the collation's `utf8mb4` twin).
    pub mb3: bool,
}

impl ColumnDef {
    /// A nullable column.
    #[must_use]
    pub fn new(name: &str, ty: ColumnType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            nullable: true,
            invisible: false,
            mb3: false,
        }
    }

    /// The same column, `NOT NULL`.
    #[must_use]
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

/// One column of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexPart {
    /// The column's position in the table.
    pub column: usize,
    /// The index prefix length (`col(10)`), in characters or bytes.
    pub prefix: Option<u32>,
    /// Whether the key orders the column descending.
    pub desc: bool,
}

impl IndexPart {
    /// The whole column, ascending.
    #[must_use]
    pub fn asc(column: usize) -> Self {
        Self {
            column,
            prefix: None,
            desc: false,
        }
    }

    /// The whole column, descending.
    #[must_use]
    pub fn desc(column: usize) -> Self {
        Self {
            column,
            prefix: None,
            desc: true,
        }
    }

    /// The first `len` characters (or bytes) of the column, ascending.
    #[must_use]
    pub fn prefix(column: usize, len: u32) -> Self {
        Self {
            column,
            prefix: Some(len),
            desc: false,
        }
    }
}

/// A key part over a value inside a JSON column: the part's column holds
/// documents, and the key orders the value at `path` in each of them (a
/// MongoDB collection's index on a field, read from SQL).
///
/// The planner uses such a part for conditions on `JSON_VALUE(col, path)`,
/// `JSON_EXTRACT(col, path)` or `col->>path` (`JSON_UNQUOTE(JSON_EXTRACT(...))`)
/// compared with constants of the part's type `ty` (a number type for
/// numbers, a string type for strings): it hands storage ranges of such
/// constants, and storage returns every row whose value could compare so
/// (more rows are fine). The condition is always checked again on the rows
/// read. Such a key never serves an order, a lookup by an earlier table's
/// rows, or a covering read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPart {
    /// The JSON path, as written in the condition (`$.a.b`).
    pub path: String,
    /// The type the part's bounds are given in.
    pub ty: ColumnType,
}

/// A key of a table: the primary key or a secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// The key's name (`PRIMARY` for the primary key).
    pub name: String,
    /// Whether two rows can't share the key's values.
    pub unique: bool,
    /// The key's columns, in order.
    pub parts: Vec<IndexPart>,
    /// MariaDB's `IGNORED` (MySQL's `INVISIBLE`): every write keeps the
    /// key's entries, and no plan reads them. [`TableDef::keys`] leaves
    /// such a key out.
    pub ignored: bool,
    /// Per part, the JSON path it orders (see [`PathPart`]); empty when no
    /// part has one. A key with a path part is left out of
    /// [`TableDef::keys`] and listed by [`TableDef::path_keys`].
    pub paths: Vec<Option<PathPart>>,
}

impl IndexDef {
    /// A key from its parts.
    #[must_use]
    pub fn new(name: &str, unique: bool, parts: Vec<IndexPart>) -> Self {
        Self {
            name: name.to_owned(),
            unique,
            parts,
            ignored: false,
            paths: Vec::new(),
        }
    }

    /// The same key with these path parts (one entry per part).
    #[must_use]
    pub fn with_paths(mut self, paths: Vec<Option<PathPart>>) -> Self {
        self.paths = paths;
        self
    }

    /// Whether a part orders a value inside a JSON column.
    #[must_use]
    pub fn has_paths(&self) -> bool {
        self.paths.iter().any(Option::is_some)
    }

    /// The same key, ignored by plans when `ignored`.
    #[must_use]
    pub fn ignored(mut self, ignored: bool) -> Self {
        self.ignored = ignored;
        self
    }
}

/// Which key of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyId {
    /// The primary key: the order rows are stored in.
    Primary,
    /// The secondary index at this position of [`TableDef::indexes`].
    Index(usize),
}

/// A table as the planner sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// The database the table is in.
    pub schema: String,
    /// The table's name.
    pub name: String,
    /// The columns, in declaration order: a row has one value per column.
    pub columns: Vec<ColumnDef>,
    /// The primary key. A table without one stores its rows in insertion
    /// order and has no key to look rows up by.
    pub primary: Option<IndexDef>,
    /// The secondary indexes.
    pub indexes: Vec<IndexDef>,
}

impl TableDef {
    /// The position of the column called `name` (names compare without
    /// case, as on both servers).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The key `id` names.
    #[must_use]
    pub fn key(&self, id: KeyId) -> Option<&IndexDef> {
        match id {
            KeyId::Primary => self.primary.as_ref(),
            KeyId::Index(i) => self.indexes.get(i),
        }
    }

    /// Every key of columns a plan may read, the primary key first:
    /// ignored secondary indexes and keys with path parts are left out.
    pub fn keys(&self) -> impl Iterator<Item = (KeyId, &IndexDef)> {
        self.primary.iter().map(|k| (KeyId::Primary, k)).chain(
            self.indexes
                .iter()
                .enumerate()
                .filter(|(_, k)| !k.ignored && !k.has_paths())
                .map(|(i, k)| (KeyId::Index(i), k)),
        )
    }

    /// The keys with path parts ([`PathPart`]) a plan may read: only for
    /// ranges of the conditions on their paths.
    pub fn path_keys(&self) -> impl Iterator<Item = (KeyId, &IndexDef)> {
        self.indexes
            .iter()
            .enumerate()
            .filter(|(_, k)| !k.ignored && k.has_paths())
            .map(|(i, k)| (KeyId::Index(i), k))
    }

    /// The parts an entry of key `id` is ordered by: the key's own parts
    /// and, for a secondary index, the primary-key parts after them (an
    /// index entry carries the row's primary key).
    #[must_use]
    pub fn ordering_parts(&self, id: KeyId) -> Vec<IndexPart> {
        let Some(key) = self.key(id) else {
            return Vec::new();
        };
        let mut parts = key.parts.clone();
        if let (KeyId::Index(_), Some(pk)) = (id, &self.primary) {
            parts.extend(pk.parts.iter().copied());
        }
        parts
    }

    /// How a key part is encoded.
    #[must_use]
    pub fn key_column(&self, part: &IndexPart) -> KeyColumn {
        let col = &self.columns[part.column];
        KeyColumn {
            ty: col.ty,
            nullable: col.nullable,
            desc: part.desc,
            prefix: part.prefix,
        }
    }

    /// Whether a unique key's parts are all `NOT NULL` whole columns, so
    /// that a lookup of all its parts finds at most one row.
    #[must_use]
    pub fn is_strict_unique(&self, id: KeyId) -> bool {
        self.key(id).is_some_and(|k| {
            k.unique
                && k.parts
                    .iter()
                    .all(|p| p.prefix.is_none() && !self.columns[p.column].nullable)
        })
    }
}

/// Where tables are found by name.
pub trait Catalog {
    /// The table `name` of database `schema`; `None` when there is none.
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>>;

    /// The session's current database, used for unqualified table names.
    fn default_schema(&self) -> Option<String>;

    /// A copy the plan can keep, for subqueries planned again while the
    /// statement runs; `None` when the catalog can't give one (such
    /// subqueries are then refused).
    fn shared(&self) -> Option<Arc<dyn Catalog>> {
        None
    }

    /// The sequence `name` of database `schema` (MariaDB's sequences), as
    /// the identity the expression engine's sequence functions take.
    ///
    /// # Errors
    /// 4091 when there is no such sequence, 4089 when `name` is a table.
    fn sequence(&self, schema: &str, name: &str) -> Result<String, sprig_exec::SqlError> {
        let _ = schema;
        Err(sprig_exec::SqlError::new(
            4091,
            "42S02",
            format!("Unknown SEQUENCE: '{name}'"),
        ))
    }
}

/// A catalog of a fixed list of tables: `(database, table, definition)`.
/// A name matches exactly first, then without case when only one table
/// matches so.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The tables: database, table name, definition.
    pub tables: Vec<(String, String, Arc<TableDef>)>,
    /// The current database.
    pub default: Option<String>,
}

impl Catalog for Snapshot {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>> {
        let same_db = |d: &String| d.eq_ignore_ascii_case(schema);
        if let Some((_, _, t)) = self.tables.iter().find(|(d, n, _)| same_db(d) && n == name) {
            return Some(t.clone());
        }
        let mut hits = self
            .tables
            .iter()
            .filter(|(d, n, _)| same_db(d) && n.eq_ignore_ascii_case(name));
        match (hits.next(), hits.next()) {
            (Some((_, _, t)), None) => Some(t.clone()),
            _ => None,
        }
    }

    fn default_schema(&self) -> Option<String> {
        self.default.clone()
    }

    fn shared(&self) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(self.clone()))
    }
}
