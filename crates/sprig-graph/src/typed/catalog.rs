//! The typed schema's catalog rows, in `t<tenant>_sprig`, and the DDL that
//! makes the tables they name ([`SqlDdl`], an implementation of
//! [`TypedDdl`]).
//!
//! ```text
//! graph_label_sets (graph_id, cls, labels)            labels: sorted label ids, "10,12"
//! graph_prop_keys  (graph_id, key_id, name)
//! graph_columns    (graph_id, entity, owner, key_id, code)
//!                  owner: the class (nodes) or the type (relationships);
//!                  a type's row with key_id 0 and code '-' says its tables exist
//! ```
//!
//! Rows are written outside the statement's transaction, on their own
//! connection, and only after the tables or columns they name exist on
//! every shard: a reader that sees a row finds its table or column.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use super::{Code, Owner, TypedSchema};
use crate::ast::Entity;
use crate::catalog::backend_error;

/// The catalog tables of the typed schema.
pub const DDL: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS graph_label_sets (
  graph_id BIGINT UNSIGNED NOT NULL, cls INT UNSIGNED NOT NULL,
  labels VARCHAR(1024) CHARACTER SET ascii NOT NULL,
  PRIMARY KEY (graph_id, cls), UNIQUE KEY uk_labels (graph_id, labels)
) ENGINE=InnoDB",
    "CREATE TABLE IF NOT EXISTS graph_prop_keys (
  graph_id BIGINT UNSIGNED NOT NULL, key_id INT UNSIGNED NOT NULL,
  name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  PRIMARY KEY (graph_id, key_id), UNIQUE KEY uk_name (graph_id, name)
) ENGINE=InnoDB",
    "CREATE TABLE IF NOT EXISTS graph_columns (
  graph_id BIGINT UNSIGNED NOT NULL, entity ENUM('node','relationship') NOT NULL,
  owner INT UNSIGNED NOT NULL, key_id INT UNSIGNED NOT NULL,
  code VARCHAR(4) CHARACTER SET ascii NOT NULL,
  PRIMARY KEY (graph_id, entity, owner, key_id, code)
) ENGINE=InnoDB",
];

fn entity_name(e: Entity) -> &'static str {
    match e {
        Entity::Node => "node",
        Entity::Relationship => "relationship",
    }
}

/// Reads graph `g`'s typed schema from catalog schema `cat`.
///
/// # Errors
/// A backend error.
pub async fn load<Q: Queryable + Send>(q: &mut Q, cat: &str, g: u64) -> Result<TypedSchema> {
    let mut s = TypedSchema::default();
    let rows: Vec<(u32, String)> = q
        .exec(
            format!(
                "SELECT ls.cls, ls.labels FROM {0}.graph_label_sets ls WHERE ls.graph_id = ? \
                 AND EXISTS (SELECT 1 FROM {0}.graph_columns c WHERE c.graph_id = ls.graph_id \
                 AND c.entity = 'node' AND c.owner = ls.cls AND c.key_id = 0 AND c.code = '-')",
                quote(cat)
            ),
            (g,),
        )
        .await
        .map_err(backend_error)?;
    for (c, l) in rows {
        let ls: Vec<u32> = l
            .split(',')
            .filter(|x| !x.is_empty())
            .filter_map(|x| x.parse().ok())
            .collect();
        s.classes.insert(c, ls);
    }
    let rows: Vec<(u32, String)> = q
        .exec(
            format!(
                "SELECT key_id, name FROM {}.graph_prop_keys WHERE graph_id = ?",
                quote(cat)
            ),
            (g,),
        )
        .await
        .map_err(backend_error)?;
    for (k, n) in rows {
        s.keys.insert(n, k);
    }
    let rows: Vec<(String, u32, u32, String)> = q
        .exec(
            format!(
                "SELECT CAST(entity AS CHAR), owner, key_id, code FROM {}.graph_columns WHERE graph_id = ?",
                quote(cat)
            ),
            (g,),
        )
        .await
        .map_err(backend_error)?;
    for (e, o, k, c) in rows {
        let ent = if e == "node" {
            Entity::Node
        } else {
            Entity::Relationship
        };
        if ent == Entity::Relationship && k == 0 && c == "-" {
            s.types.insert(o);
            continue;
        }
        if ent == Entity::Relationship && (c == ">" || c == "<") {
            s.ends.insert((o, c == ">", k));
            continue;
        }
        if let Some(code) = Code::of(&c) {
            s.columns.entry((ent, o)).or_default().insert((k, code));
        }
    }
    Ok(s)
}

/// What a write needs that the schema may lack.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Want {
    /// Property keys.
    pub keys: BTreeSet<String>,
    /// Label sets (sorted ids) and the columns their first rows need.
    pub classes: BTreeMap<Vec<u32>, BTreeSet<(String, Code)>>,
    /// Relationship types and the columns their first rows need.
    pub types: BTreeMap<u32, BTreeSet<(String, Code)>>,
    /// Columns of existing tables the statement's transaction hasn't
    /// touched (added with `NOWAIT`, or not at all).
    pub columns: BTreeMap<Owner, BTreeSet<(String, Code)>>,
    /// Classes relationships end at ([`TypedSchema::ends`]).
    pub ends: BTreeSet<(u32, bool, u32)>,
    /// The graph's indexes: new tables and columns they cover get them.
    pub indexes: Vec<crate::store::IndexDef>,
}

impl Want {
    /// Nothing wanted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
            && self.classes.is_empty()
            && self.types.is_empty()
            && self.columns.is_empty()
            && self.ends.is_empty()
    }
}

/// Makes the schema a write needs, outside the statement's transaction.
#[async_trait]
pub trait TypedDdl: Send + Sync {
    /// Adds what `want` names that isn't there yet: keys, label-set and
    /// type tables (on every shard, with their first columns) and columns
    /// (each added with `NOWAIT` on every shard; a column that can't be
    /// added now is left out). Answers the schema now.
    ///
    /// # Errors
    /// A backend error.
    async fn ensure(&self, want: &Want) -> Result<TypedSchema>;

    /// Columns a write needed but couldn't have (its transaction had
    /// touched `owner`'s table), whose values went to the values table:
    /// the implementation may add them after the commit and move the
    /// values in.
    fn defer(&self, _owner: Owner, _cols: BTreeSet<(String, Code)>) {}
}

/// Where a graph's shard schemas are: (cluster, schema) for each shard.
pub type ShardSchemas = Vec<(String, String)>;

/// Connections for [`SqlDdl`]: the catalog's and each cluster's.
#[async_trait]
pub trait DdlConns: Send + Sync {
    /// A connection to the tenant catalog's writer.
    async fn catalog(&self) -> Result<mysql_async::Conn>;
    /// A connection to `cluster`'s writer, outside any transaction.
    async fn cluster(&self, cluster: &str) -> Result<mysql_async::Conn>;
    /// Called after catalog rows changed (the gateway raises the
    /// catalog's version, so other gateways read them).
    async fn changed(&self) -> Result<()> {
        Ok(())
    }
    /// Graph `graph`'s indexes and constraints as the catalog has them now
    /// (token names left empty). The default: none (a store with no
    /// catalog records, as in tests).
    async fn indexes(&self, _graph: u64) -> Result<Vec<crate::store::IndexDef>> {
        Ok(Vec::new())
    }
}

/// [`TypedDdl`] over plain SQL connections.
pub struct SqlDdl {
    /// Catalog schema name.
    pub cat: String,
    /// The graph.
    pub graph: u64,
    /// Its shards.
    pub shards: ShardSchemas,
    /// Connections.
    pub conns: Arc<dyn DdlConns>,
}

impl std::fmt::Debug for SqlDdl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlDdl")
            .field("graph", &self.graph)
            .finish_non_exhaustive()
    }
}

/// MariaDB answers a `NOWAIT` that can't lock with these.
fn lock_busy(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if matches!(s.code, 1205 | 1213 | 4144))
}

impl SqlDdl {
    async fn key_ids(&self, c: &mut mysql_async::Conn, keys: &BTreeSet<String>) -> Result<()> {
        let t = format!("{}.graph_prop_keys", quote(&self.cat));
        for k in keys {
            if k.len() > 255 {
                return Err(SprigError::new(
                    sprig_common::SprigCode::GraphDefinitionInvalid,
                    format!("property key '{k}' is longer than 255 bytes"),
                ));
            }
            c.exec_drop(
                format!(
                    "INSERT IGNORE INTO {t} (graph_id, key_id, name) \
                     SELECT ?, COALESCE(MAX(key_id), 0) + 1, ? FROM {t} WHERE graph_id = ?"
                ),
                (self.graph, k.as_str(), self.graph),
            )
            .await
            .map_err(backend_error)?;
        }
        Ok(())
    }

    async fn on_shards(&self, sql: impl Fn(&str) -> Vec<String>) -> Result<()> {
        for (cluster, schema) in &self.shards {
            let mut c = self.conns.cluster(cluster).await?;
            for s in sql(schema) {
                c.query_drop(s).await.map_err(backend_error)?;
            }
        }
        Ok(())
    }

    /// Records `cols` of `owner` (and, with `ready`, the row that makes a
    /// new class or type table visible) in one transaction: a reader sees
    /// a new table with all its first columns or not at all.
    async fn record_columns(
        &self,
        c: &mut mysql_async::Conn,
        owner: Owner,
        cols: &[(u32, Code)],
        ready: bool,
    ) -> Result<()> {
        let mut rows: Vec<(u32, &str)> = cols.iter().map(|(k, code)| (*k, code.suffix())).collect();
        if ready {
            rows.push((0, "-"));
        }
        if rows.is_empty() {
            return Ok(());
        }
        let marks = vec!["(?, ?, ?, ?, ?)"; rows.len()].join(", ");
        let mut params: Vec<mysql_async::Value> = Vec::with_capacity(rows.len() * 5);
        for (k, code) in rows {
            params.extend([
                self.graph.into(),
                entity_name(owner.0).into(),
                owner.1.into(),
                k.into(),
                code.into(),
            ]);
        }
        c.exec_drop(
            format!(
                "INSERT IGNORE INTO {}.graph_columns (graph_id, entity, owner, key_id, code) VALUES {marks}",
                quote(&self.cat)
            ),
            params,
        )
        .await
        .map_err(backend_error)?;
        Ok(())
    }

    /// Adds `cols` to `owner`'s table on every shard, waiting up to a few
    /// seconds for each table's writers (not `NOWAIT`: the columns of
    /// values a write had to put aside, added after its commit), with the
    /// indexes covering them; records them. Answers the schema now.
    ///
    /// # Errors
    /// 1205 when a table stayed busy; backend errors.
    pub async fn add_columns_wait(
        &self,
        owner: Owner,
        cols: &BTreeSet<(String, Code)>,
    ) -> Result<TypedSchema> {
        let mut c = self.conns.catalog().await?;
        let mut s = load(&mut c, &self.cat, self.graph).await?;
        let missing: BTreeSet<String> = cols
            .iter()
            .map(|(k, _)| k.clone())
            .filter(|k| !s.keys.contains_key(k))
            .collect();
        if !missing.is_empty() {
            self.key_ids(&mut c, &missing).await?;
            s = load(&mut c, &self.cat, self.graph).await?;
        }
        let have = s.columns.get(&owner).cloned().unwrap_or_default();
        let add: Vec<(u32, Code)> = cols
            .iter()
            .filter_map(|(k, code)| s.keys.get(k).map(|id| (*id, *code)))
            .filter(|x| !have.contains(x))
            .collect();
        if add.is_empty() || have.len() + add.len() > super::MAX_COLUMNS {
            return Ok(s);
        }
        let indexes = self.indexes_now(&Want::default()).await?;
        let mut with = s.clone();
        with.columns
            .entry(owner)
            .or_default()
            .extend(add.iter().copied());
        let specs: Vec<String> = indexes
            .iter()
            .flat_map(|d| super::index_specs(&with, owner, &add, d))
            .collect();
        for (cluster, schema) in &self.shards {
            let mut sc = self.conns.cluster(cluster).await?;
            let mut parts: Vec<String> = add
                .iter()
                .map(|(k, code)| {
                    format!(
                        "ADD COLUMN IF NOT EXISTS {} {} NULL",
                        super::column(*k, *code),
                        code.sql_type()
                    )
                })
                .collect();
            parts.extend(specs.iter().map(|x| format!("ADD INDEX IF NOT EXISTS {x}")));
            sc.query_drop(format!(
                "SET STATEMENT lock_wait_timeout = 5 FOR ALTER TABLE {} {}",
                super::table_of(schema, self.graph, owner),
                parts.join(", ")
            ))
            .await
            .map_err(backend_error)?;
        }
        self.record_columns(&mut c, owner, &add, false).await?;
        s = load(&mut c, &self.cat, self.graph).await?;
        self.conns.changed().await?;
        Ok(s)
    }

    /// The graph's indexes as the catalog has them now (a writer's may be
    /// a moment old).
    async fn indexes_now(&self, want: &Want) -> Result<Vec<crate::store::IndexDef>> {
        let mut v: Vec<crate::store::IndexDef> = self.conns.indexes(self.graph).await?;
        for d in &want.indexes {
            if !v.iter().any(|x| x.id == d.id) {
                v.push(d.clone());
            }
        }
        Ok(v)
    }

    /// Makes `owner`'s table on every shard with `cols` and the indexes
    /// covering them, whoever made it first: `CREATE TABLE IF NOT EXISTS`,
    /// then the columns and indexes another maker's table may lack
    /// (waiting for its makers, not `NOWAIT`: no writer uses the table
    /// before its ready row), then the catalog rows.
    async fn make_table(
        &self,
        c: &mut mysql_async::Conn,
        s: &TypedSchema,
        owner: Owner,
        cols: &[(u32, Code)],
        indexes: &[crate::store::IndexDef],
    ) -> Result<()> {
        let g = self.graph;
        let specs: Vec<String> = indexes
            .iter()
            .flat_map(|d| super::index_specs(s, owner, cols, d))
            .collect();
        self.on_shards(|schema| {
            let mut v = match owner.0 {
                Entity::Node => vec![super::class_ddl(schema, g, owner.1, cols, &specs)],
                Entity::Relationship => super::type_ddl(schema, g, owner.1, cols, &specs),
            };
            let mut alter: Vec<String> = cols
                .iter()
                .map(|(k, code)| {
                    format!(
                        "ADD COLUMN IF NOT EXISTS {} {} NULL",
                        super::column(*k, *code),
                        code.sql_type()
                    )
                })
                .collect();
            alter.extend(specs.iter().map(|x| format!("ADD INDEX IF NOT EXISTS {x}")));
            if !alter.is_empty() {
                v.push(format!(
                    "ALTER TABLE {} {}",
                    super::table_of(schema, g, owner),
                    alter.join(", ")
                ));
            }
            v
        })
        .await?;
        self.record_columns(c, owner, cols, true).await
    }
}

#[async_trait]
impl TypedDdl for SqlDdl {
    async fn ensure(&self, want: &Want) -> Result<TypedSchema> {
        let mut c = self.conns.catalog().await?;
        let mut changed = false;
        let mut grown: BTreeSet<Owner> = BTreeSet::new();
        let mut keys = want.keys.clone();
        for cols in want
            .classes
            .values()
            .chain(want.types.values())
            .chain(want.columns.values())
        {
            keys.extend(cols.iter().map(|(k, _)| k.clone()));
        }
        let mut s = load(&mut c, &self.cat, self.graph).await?;
        let missing: BTreeSet<String> = keys
            .iter()
            .filter(|k| !s.keys.contains_key(*k))
            .cloned()
            .collect();
        if !missing.is_empty() {
            self.key_ids(&mut c, &missing).await?;
            s = load(&mut c, &self.cat, self.graph).await?;
            changed = true;
        }
        let ids = |s: &TypedSchema, cols: &BTreeSet<(String, Code)>| -> Vec<(u32, Code)> {
            cols.iter()
                .filter_map(|(k, code)| s.keys.get(k).map(|id| (*id, *code)))
                .collect()
        };
        let indexes = self.indexes_now(want).await?;
        // Columns of tables that exist (made meanwhile by another writer).
        let mut more: BTreeMap<Owner, BTreeSet<(String, Code)>> = BTreeMap::new();
        for (labels, cols) in &want.classes {
            if let Some(cls) = s.class_of(labels) {
                more.entry((Entity::Node, cls))
                    .or_default()
                    .extend(cols.iter().cloned());
                continue;
            }
            let t = format!("{}.graph_label_sets", quote(&self.cat));
            let text: Vec<String> = labels.iter().map(ToString::to_string).collect();
            let text = text.join(",");
            c.exec_drop(
                format!(
                    "INSERT IGNORE INTO {t} (graph_id, cls, labels) \
                     SELECT ?, COALESCE(MAX(cls), 0) + 1, ? FROM {t} WHERE graph_id = ?"
                ),
                (self.graph, text.as_str(), self.graph),
            )
            .await
            .map_err(backend_error)?;
            let cls: Option<u32> = c
                .exec_first(
                    format!("SELECT cls FROM {t} WHERE graph_id = ? AND labels = ?"),
                    (self.graph, text.as_str()),
                )
                .await
                .map_err(backend_error)?;
            let Some(cls) = cls else {
                return Err(SprigError::internal("label set missing after insert"));
            };
            // The schema with the new class, for the index specs.
            let mut with = s.clone();
            with.classes.insert(cls, labels.clone());
            self.make_table(&mut c, &with, (Entity::Node, cls), &ids(&s, cols), &indexes)
                .await?;
            s = load(&mut c, &self.cat, self.graph).await?;
            changed = true;
        }
        for (t, cols) in &want.types {
            if s.types.contains(t) {
                more.entry((Entity::Relationship, *t))
                    .or_default()
                    .extend(cols.iter().cloned());
                continue;
            }
            self.make_table(
                &mut c,
                &s,
                (Entity::Relationship, *t),
                &ids(&s, cols),
                &indexes,
            )
            .await?;
            s = load(&mut c, &self.cat, self.graph).await?;
            changed = true;
        }
        for (owner, cols) in &want.columns {
            more.entry(*owner).or_default().extend(cols.iter().cloned());
        }
        for (owner, cols) in &more {
            let have = s.columns.get(owner).cloned().unwrap_or_default();
            let add: Vec<(u32, Code)> = ids(&s, cols)
                .into_iter()
                .filter(|x| !have.contains(x))
                .collect();
            if add.is_empty() || have.len() + add.len() > super::MAX_COLUMNS {
                continue;
            }
            let mut ok = true;
            for (cluster, schema) in &self.shards {
                let mut sc = self.conns.cluster(cluster).await?;
                match sc
                    .query_drop(super::add_columns_ddl(schema, self.graph, *owner, &add))
                    .await
                {
                    Ok(()) => {}
                    Err(e) if lock_busy(&e) => {
                        ok = false;
                        break;
                    }
                    Err(e) => return Err(backend_error(e)),
                }
            }
            if ok {
                self.record_columns(&mut c, *owner, &add, false).await?;
                grown.insert(*owner);
                changed = true;
            }
        }
        for (t, to_dst, cls) in &want.ends {
            if s.ends.contains(&(*t, *to_dst, *cls)) {
                continue;
            }
            c.exec_drop(
                format!(
                    "INSERT IGNORE INTO {}.graph_columns (graph_id, entity, owner, key_id, code) \
                     VALUES (?, 'relationship', ?, ?, ?)",
                    quote(&self.cat)
                ),
                (self.graph, *t, *cls, if *to_dst { ">" } else { "<" }),
            )
            .await
            .map_err(backend_error)?;
            changed = true;
        }
        if changed {
            s = load(&mut c, &self.cat, self.graph).await?;
            // Indexes covering the tables and columns just made, where
            // the table isn't busy (else reads go without them).
            for owner in &grown {
                let clauses: Vec<String> = indexes
                    .iter()
                    .flat_map(|d| super::index_clauses(&s, *owner, d))
                    .collect();
                if clauses.is_empty() {
                    continue;
                }
                for (cluster, schema) in &self.shards {
                    let mut sc = self.conns.cluster(cluster).await?;
                    match sc
                        .query_drop(super::add_index_ddl(
                            schema, self.graph, *owner, &clauses, true,
                        ))
                        .await
                    {
                        Ok(()) => {}
                        Err(e) if lock_busy(&e) => break,
                        Err(e) => return Err(backend_error(e)),
                    }
                }
            }
            self.conns.changed().await?;
        }
        Ok(s)
    }
}
