//! Retrieval indexes in the tenant catalog: the `ai_retrieval` module, one
//! `ai_retrieval_indexes` row per index, created on first use.
//!
//! A row records the source table (its object id), the text, title and
//! filter columns, the source's key columns, the chunking, the model alias
//! and its pinned identity, and the index's state:
//!
//! ```text
//! building → ready → rebuilding → ready
//!          ↘ failed (until the cause is gone)
//! ```
//!
//! `rebuilding` means a shard of the source has no chunk table yet, or
//! one being filled again (a split, a merge or a move put the shard's
//! rows in a new schema); searches meanwhile miss that shard's rows.
//! `progress` holds the fill's position per shard schema (its last key,
//! or `"done"`), and its keys are the shards the index has: the gateway
//! searches those.

use mysql_async::prelude::Queryable as _;
use serde_json::Value;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use super::{FilterColumn, KeyColumn};
use crate::embedding::ModelRef;

/// The catalog module.
pub const MODULE: &str = "ai_retrieval";

/// The table.
pub const TABLE: &str = "ai_retrieval_indexes";

const DDL: &str = "CREATE TABLE IF NOT EXISTS ai_retrieval_indexes (
  index_id      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_id         INT UNSIGNED NOT NULL,
  name          VARCHAR(64) NOT NULL,
  name_key      VARCHAR(64) NOT NULL,
  object_id     BIGINT UNSIGNED NOT NULL,
  text_column   VARCHAR(64) NOT NULL,
  title_column  VARCHAR(64) NULL,
  filters       JSON NOT NULL,
  key_columns   JSON NOT NULL,
  chunking      VARCHAR(32) NOT NULL,
  model_alias   VARCHAR(128) NOT NULL,
  provider      VARCHAR(64) NOT NULL,
  model         VARCHAR(255) NOT NULL,
  dimensions    SMALLINT UNSIGNED NOT NULL,
  state         ENUM('building','ready','rebuilding','failed') NOT NULL,
  progress      JSON NULL,
  rows_indexed  BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_error    TEXT NULL,
  created_by    INT UNSIGNED NULL,
  created_at    DATETIME(3) NOT NULL,
  updated_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_retrieval_index (db_id, name_key),
  KEY by_object (object_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_retrieval_indexes",
    statements: &[DDL],
}];

/// The module, for migrators.
pub const RETRIEVAL_MODULE: Module = Module {
    name: MODULE,
    migrations: MIGRATIONS,
};

/// An index's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// The source's rows are being chunked and embedded; changes are
    /// followed meanwhile.
    Building,
    /// Every shard was filled once; changes are followed.
    Ready,
    /// A shard is being filled again.
    Rebuilding,
    /// Stopped on an error that isn't transient, with `last_error`; the
    /// job tries again every 30 to 60 seconds.
    Failed,
}

impl State {
    /// The column value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            State::Building => "building",
            State::Ready => "ready",
            State::Rebuilding => "rebuilding",
            State::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "ready" => State::Ready,
            "rebuilding" => State::Rebuilding,
            "failed" => State::Failed,
            _ => State::Building,
        }
    }
}

/// One index.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalIndex {
    /// The id (its chunk tables are `__sprig_rx_<id>`).
    pub id: u64,
    /// Its database.
    pub db_id: u32,
    /// Its name as declared.
    pub name: String,
    /// The source table's object id.
    pub object_id: u64,
    /// The text column.
    pub text_column: String,
    /// The title column.
    pub title_column: Option<String>,
    /// The filter columns.
    pub filters: Vec<FilterColumn>,
    /// The source's primary key.
    pub key_columns: Vec<KeyColumn>,
    /// `headings` or `tokens:<n>`.
    pub chunking: String,
    /// The model.
    pub model: ModelRef,
    /// The state.
    pub state: State,
    /// The fill's position per shard schema.
    pub progress: Value,
    /// Source rows that have their chunks now, over the index's live
    /// shards (rows without text have none). The worker sets it: exact
    /// once a fill or check ends, kept per row in between.
    pub rows_indexed: u64,
    /// Why it failed, or the last transient error.
    pub last_error: Option<String>,
    /// Who created it.
    pub created_by: Option<u32>,
}

impl RetrievalIndex {
    /// The shard schemas the index has (the keys of `progress`), sorted.
    #[must_use]
    pub fn shards(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .progress
            .as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();
        v.sort();
        v
    }
}

/// A new index.
#[derive(Debug, Clone)]
pub struct NewIndex {
    /// Its database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// The source table.
    pub object_id: u64,
    /// The text column.
    pub text_column: String,
    /// The title column.
    pub title_column: Option<String>,
    /// The filter columns.
    pub filters: Vec<FilterColumn>,
    /// The source's key.
    pub key_columns: Vec<KeyColumn>,
    /// The chunking.
    pub chunking: String,
    /// The model.
    pub model: ModelRef,
    /// The initial shards' progress (`{schema: null}`).
    pub progress: Value,
    /// Who creates it.
    pub created_by: Option<u32>,
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
}

const COLUMNS: &str = "index_id, db_id, name, object_id, text_column, title_column, \
     CAST(filters AS CHAR), CAST(key_columns AS CHAR), chunking, model_alias, provider, model, \
     dimensions, state, CAST(progress AS CHAR), rows_indexed, last_error, created_by";

fn json_list<T: serde::de::DeserializeOwned>(s: Option<String>) -> Vec<T> {
    s.and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn row_of(r: &mysql_async::Row) -> Result<RetrievalIndex> {
    let bad = || SprigError::internal("an unreadable ai_retrieval_indexes row");
    macro_rules! g {
        ($i:expr) => {
            r.get_opt($i).and_then(|v| v.ok()).ok_or_else(bad)?
        };
    }
    let filters: Option<String> = g!(6);
    let keys: Option<String> = g!(7);
    let state: String = g!(13);
    let progress: Option<String> = g!(14);
    Ok(RetrievalIndex {
        id: g!(0),
        db_id: g!(1),
        name: g!(2),
        object_id: g!(3),
        text_column: g!(4),
        title_column: g!(5),
        filters: json_list(filters),
        key_columns: json_list(keys),
        chunking: g!(8),
        model: ModelRef {
            alias: g!(9),
            provider: g!(10),
            model: g!(11),
            dimensions: g!(12),
        },
        state: State::parse(&state),
        progress: progress
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or(Value::Null),
        rows_indexed: g!(15),
        last_error: g!(16),
        created_by: g!(17),
    })
}

/// The indexes in one tenant catalog schema.
#[derive(Debug, Clone)]
pub struct Store {
    schema: String,
}

impl Store {
    /// Over catalog schema `schema` (`t<tenant>_sprig`).
    #[must_use]
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
        }
    }

    fn t(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote(TABLE))
    }

    /// Whether the module's table exists.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn exists(&self, c: &mut mysql_async::Conn) -> Result<bool> {
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (self.schema.as_str(), TABLE),
            )
            .await
            .map_err(unavailable)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Creates the module's table when it is missing.
    ///
    /// # Errors
    /// The catalog is unreachable, or the migration failed.
    pub async fn ensure(&self, c: &mut mysql_async::Conn) -> Result<()> {
        if !self.exists(c).await? {
            Migrator::new(&[RETRIEVAL_MODULE])?
                .run(c, &self.schema, &MigrateOpts::existing())
                .await?;
        }
        Ok(())
    }

    async fn select(
        &self,
        c: &mut mysql_async::Conn,
        filter: &str,
        params: mysql_async::Params,
    ) -> Result<Vec<RetrievalIndex>> {
        if !self.exists(c).await? {
            return Ok(Vec::new());
        }
        let rows: Vec<mysql_async::Row> = c
            .exec(
                format!(
                    "SELECT {COLUMNS} FROM {} {filter} ORDER BY index_id",
                    self.t()
                ),
                params,
            )
            .await
            .map_err(unavailable)?;
        rows.iter().map(row_of).collect()
    }

    /// Every index of the tenant.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn all(&self, c: &mut mysql_async::Conn) -> Result<Vec<RetrievalIndex>> {
        self.select(c, "", mysql_async::Params::Empty).await
    }

    /// The indexes of database `db_id`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn of_db(
        &self,
        c: &mut mysql_async::Conn,
        db_id: u32,
    ) -> Result<Vec<RetrievalIndex>> {
        self.select(c, "WHERE db_id = ?", (db_id,).into()).await
    }

    /// The indexes over table `object`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn of_object(
        &self,
        c: &mut mysql_async::Conn,
        object: u64,
    ) -> Result<Vec<RetrievalIndex>> {
        self.select(c, "WHERE object_id = ?", (object,).into())
            .await
    }

    /// Index `name` of database `db_id` (any case).
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn by_name(
        &self,
        c: &mut mysql_async::Conn,
        db_id: u32,
        name: &str,
    ) -> Result<Option<RetrievalIndex>> {
        Ok(self
            .select(
                c,
                "WHERE db_id = ? AND name_key = ?",
                (db_id, name.to_lowercase()).into(),
            )
            .await?
            .pop())
    }

    /// Index `id`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn get(&self, c: &mut mysql_async::Conn, id: u64) -> Result<Option<RetrievalIndex>> {
        Ok(self
            .select(c, "WHERE index_id = ?", (id,).into())
            .await?
            .pop())
    }

    /// Records a new index (state `building`); its id. `None` when the
    /// database has an index of that name already.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn insert(&self, c: &mut mysql_async::Conn, n: &NewIndex) -> Result<Option<u64>> {
        self.ensure(c).await?;
        let r = c
            .exec_drop(
                format!(
                    "INSERT INTO {} (db_id, name, name_key, object_id, text_column, title_column, \
                     filters, key_columns, chunking, model_alias, provider, model, dimensions, state, \
                     progress, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, \
                     ?, ?, ?, ?, 'building', ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                    self.t()
                ),
                mysql_async::Params::Positional(vec![
                    n.db_id.into(),
                    n.name.as_str().into(),
                    n.name.to_lowercase().into(),
                    n.object_id.into(),
                    n.text_column.as_str().into(),
                    n.title_column.clone().into(),
                    json(&n.filters).into(),
                    json(&n.key_columns).into(),
                    n.chunking.as_str().into(),
                    n.model.alias.as_str().into(),
                    n.model.provider.as_str().into(),
                    n.model.model.as_str().into(),
                    n.model.dimensions.into(),
                    n.progress.to_string().into(),
                    n.created_by.into(),
                ]),
            )
            .await;
        match r {
            Ok(()) => Ok(Some(c.last_insert_id().unwrap_or(0))),
            Err(mysql_async::Error::Server(s)) if s.code == 1062 => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }

    async fn update(
        &self,
        c: &mut mysql_async::Conn,
        set: &str,
        id: u64,
        mut params: Vec<mysql_async::Value>,
    ) -> Result<()> {
        params.push(id.into());
        c.exec_drop(
            format!(
                "UPDATE {} SET {set}, updated_at = UTC_TIMESTAMP(3) WHERE index_id = ?",
                self.t()
            ),
            mysql_async::Params::Positional(params),
        )
        .await
        .map_err(unavailable)
    }

    /// Sets the state and the last error.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn set_state(
        &self,
        c: &mut mysql_async::Conn,
        id: u64,
        state: State,
        error: Option<&str>,
    ) -> Result<()> {
        self.update(
            c,
            "state = ?, last_error = ?",
            id,
            vec![state.as_str().into(), error.into()],
        )
        .await
    }

    /// Records the fill's position and the rows indexed (rows with chunks
    /// over the live shards).
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn set_progress(
        &self,
        c: &mut mysql_async::Conn,
        id: u64,
        progress: &Value,
        rows: u64,
    ) -> Result<()> {
        self.update(
            c,
            "progress = ?, rows_indexed = ?",
            id,
            vec![progress.to_string().into(), rows.into()],
        )
        .await
    }

    /// Forgets index `id`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn delete(&self, c: &mut mysql_async::Conn, id: u64) -> Result<()> {
        if !self.exists(c).await? {
            return Ok(());
        }
        c.exec_drop(
            format!("DELETE FROM {} WHERE index_id = ?", self.t()),
            (id,),
        )
        .await
        .map_err(unavailable)
    }
}

/// A value as JSON text.
fn json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip() {
        for s in [
            State::Building,
            State::Ready,
            State::Rebuilding,
            State::Failed,
        ] {
            assert_eq!(State::parse(s.as_str()), s);
        }
        let ix = RetrievalIndex {
            id: 1,
            db_id: 1,
            name: "kb".into(),
            object_id: 9,
            text_column: "body".into(),
            title_column: None,
            filters: Vec::new(),
            key_columns: Vec::new(),
            chunking: "tokens:400".into(),
            model: ModelRef::local(),
            state: State::Building,
            progress: serde_json::json!({"t1_d1_g2_s1": "done", "t1_d1_g2_s0": null}),
            rows_indexed: 0,
            last_error: None,
            created_by: None,
        };
        assert_eq!(ix.shards(), ["t1_d1_g2_s0", "t1_d1_g2_s1"]);
    }
}
