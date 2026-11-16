//! Embedded columns in the tenant catalog: the `ai_embeddings` module,
//! one `ai_embedded_columns` row per embedded column.
//!
//! A row records the source expression, the model alias and its pinned
//! identity (provider, model, dimensions), the mode, the generation (which
//! a model or source change raises) and the column's state:
//!
//! ```text
//! backfilling → ready → reembedding (generation g+1) → ready
//!            ↘ failed (until resumed)
//! ```
//!
//! `pending` holds the next generation while a re-embedding fills its
//! shadow columns; `progress` holds the backfill's position per shard, so
//! a restarted job carries on where it stopped.

use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use super::ModelRef;

/// The catalog module.
pub const MODULE: &str = "ai_embeddings";

/// The table.
pub const TABLE: &str = "ai_embedded_columns";

const DDL: &str = "CREATE TABLE IF NOT EXISTS ai_embedded_columns (
  embedding_id     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  db_id            INT UNSIGNED NOT NULL,
  object_id        BIGINT UNSIGNED NOT NULL,
  column_name      VARCHAR(64) NOT NULL,
  source_expr      TEXT NOT NULL,
  model_alias      VARCHAR(128) NOT NULL,
  provider         VARCHAR(64) NOT NULL,
  model            VARCHAR(255) NOT NULL,
  dimensions       SMALLINT UNSIGNED NOT NULL,
  mode             ENUM('async','sync') NOT NULL,
  max_input_tokens SMALLINT UNSIGNED NULL,
  generation       SMALLINT UNSIGNED NOT NULL,
  state            ENUM('backfilling','ready','reembedding','failed') NOT NULL,
  pending          JSON NULL,
  progress         JSON NULL,
  rows_embedded    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_error       TEXT NULL,
  created_by       INT UNSIGNED NULL,
  created_at       DATETIME(3) NOT NULL,
  updated_at       DATETIME(3) NOT NULL,
  UNIQUE KEY uk_embedded_column (object_id, column_name),
  KEY by_db (db_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "ai_embedded_columns",
    statements: &[DDL],
}];

/// The module, for migrators.
pub const EMBEDDING_MODULE: Module = Module {
    name: MODULE,
    migrations: MIGRATIONS,
};

/// An embedded column's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Existing rows are being embedded; changes are followed meanwhile.
    Backfilling,
    /// Every row was embedded once; changes are followed.
    Ready,
    /// A new generation fills its shadow columns.
    Reembedding,
    /// Stopped on an error that isn't transient; resumed by a new
    /// declaration or by the job once the cause is gone.
    Failed,
}

impl State {
    /// The column value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            State::Backfilling => "backfilling",
            State::Ready => "ready",
            State::Reembedding => "reembedding",
            State::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "ready" => State::Ready,
            "reembedding" => State::Reembedding,
            "failed" => State::Failed,
            _ => State::Backfilling,
        }
    }
}

/// The generation a re-embedding fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    /// Its number.
    pub generation: u32,
    /// Its source expression.
    pub source: String,
    /// Whether the source differs from the current one (the shadow has its
    /// own source column).
    pub new_source: bool,
    /// Its model.
    pub model: ModelRef,
    /// `WITH (max_input_tokens = n)`.
    #[serde(default)]
    pub max_input_tokens: Option<u32>,
}

/// One row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedColumn {
    /// The id.
    pub id: u64,
    /// The database.
    pub db_id: u32,
    /// The table's object id.
    pub object_id: u64,
    /// The column.
    pub column: String,
    /// The source expression as declared.
    pub source: String,
    /// The model.
    pub model: ModelRef,
    /// `async` or `sync`.
    pub mode: String,
    /// `WITH (max_input_tokens = n)`.
    pub max_input_tokens: Option<u32>,
    /// The generation in the column now.
    pub generation: u32,
    /// The state.
    pub state: State,
    /// The generation being filled.
    pub pending: Option<Pending>,
    /// The backfill's position per shard schema.
    pub progress: Value,
    /// Rows embedded so far.
    pub rows_embedded: u64,
    /// Why the column failed, or the last transient error.
    pub last_error: Option<String>,
    /// Who declared it.
    pub created_by: Option<u32>,
}

/// A new row.
#[derive(Debug, Clone)]
pub struct NewColumn {
    /// The database.
    pub db_id: u32,
    /// The table.
    pub object_id: u64,
    /// The column.
    pub column: String,
    /// The source.
    pub source: String,
    /// The model.
    pub model: ModelRef,
    /// `async` or `sync`.
    pub mode: String,
    /// `WITH (max_input_tokens = n)`.
    pub max_input_tokens: Option<u32>,
    /// Who declares it.
    pub created_by: Option<u32>,
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
}

type RowTuple = (
    u64,
    u32,
    u64,
    String,
    String,
    String,
    String,
    String,
    u32,
    String,
    Option<u32>,
    u32,
);
type RowTail = (String, Option<String>, Option<String>, u64, Option<String>, Option<u32>);

const COLUMNS: &str = "embedding_id, db_id, object_id, column_name, source_expr, model_alias, provider, \
     model, dimensions, mode, max_input_tokens, generation";
const TAIL: &str = "state, pending, progress, rows_embedded, last_error, created_by";

fn row_of(head: RowTuple, tail: RowTail) -> EmbeddedColumn {
    let (id, db_id, object_id, column, source, alias, provider, model, dimensions, mode, mit, generation) =
        head;
    let (state, pending, progress, rows_embedded, last_error, created_by) = tail;
    EmbeddedColumn {
        id,
        db_id,
        object_id,
        column,
        source,
        model: ModelRef {
            alias,
            provider,
            model,
            dimensions,
        },
        mode,
        max_input_tokens: mit,
        generation,
        state: State::parse(&state),
        pending: pending.and_then(|p| serde_json::from_str(&p).ok()),
        progress: progress
            .and_then(|p| serde_json::from_str(&p).ok())
            .unwrap_or(Value::Null),
        rows_embedded,
        last_error,
        created_by,
    }
}

/// The rows in one tenant catalog schema.
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
            Migrator::new(&[EMBEDDING_MODULE])?
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
    ) -> Result<Vec<EmbeddedColumn>> {
        if !self.exists(c).await? {
            return Ok(Vec::new());
        }
        let heads: Vec<mysql_async::Row> = c
            .exec(
                format!(
                    "SELECT {COLUMNS}, {TAIL} FROM {} {filter} ORDER BY embedding_id",
                    self.t()
                ),
                params,
            )
            .await
            .map_err(unavailable)?;
        let mut out = Vec::with_capacity(heads.len());
        for r in heads {
            let bad = || SprigError::internal("an unreadable ai_embedded_columns row");
            let head: RowTuple = (
                r.get(0).ok_or_else(bad)?,
                r.get(1).ok_or_else(bad)?,
                r.get(2).ok_or_else(bad)?,
                r.get(3).ok_or_else(bad)?,
                r.get(4).ok_or_else(bad)?,
                r.get(5).ok_or_else(bad)?,
                r.get(6).ok_or_else(bad)?,
                r.get(7).ok_or_else(bad)?,
                r.get(8).ok_or_else(bad)?,
                r.get(9).ok_or_else(bad)?,
                r.get(10).ok_or_else(bad)?,
                r.get(11).ok_or_else(bad)?,
            );
            let tail: RowTail = (
                r.get(12).ok_or_else(bad)?,
                r.get(13).ok_or_else(bad)?,
                r.get(14).ok_or_else(bad)?,
                r.get(15).ok_or_else(bad)?,
                r.get(16).ok_or_else(bad)?,
                r.get(17).ok_or_else(bad)?,
            );
            out.push(row_of(head, tail));
        }
        Ok(out)
    }

    /// Every embedded column of the tenant.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn all(&self, c: &mut mysql_async::Conn) -> Result<Vec<EmbeddedColumn>> {
        self.select(c, "", mysql_async::Params::Empty).await
    }

    /// The embedded columns of table `object`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn of_object(
        &self,
        c: &mut mysql_async::Conn,
        object: u64,
    ) -> Result<Vec<EmbeddedColumn>> {
        self.select(c, "WHERE object_id = ?", (object,).into()).await
    }

    /// Embedded column `id`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn get(&self, c: &mut mysql_async::Conn, id: u64) -> Result<Option<EmbeddedColumn>> {
        Ok(self
            .select(c, "WHERE embedding_id = ?", (id,).into())
            .await?
            .pop())
    }

    /// Records a new embedded column (generation 1, backfilling); its id.
    ///
    /// # Errors
    /// The catalog is unreachable, or the column is already recorded
    /// (39997).
    pub async fn insert(&self, c: &mut mysql_async::Conn, n: &NewColumn) -> Result<u64> {
        self.ensure(c).await?;
        let r = c
            .exec_drop(
                format!(
                    "INSERT INTO {} (db_id, object_id, column_name, source_expr, model_alias, provider, \
                     model, dimensions, mode, max_input_tokens, generation, state, created_by, \
                     created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'backfilling', ?, \
                     UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                    self.t()
                ),
                mysql_async::Params::Positional(vec![
                    n.db_id.into(),
                    n.object_id.into(),
                    n.column.as_str().into(),
                    n.source.as_str().into(),
                    n.model.alias.as_str().into(),
                    n.model.provider.as_str().into(),
                    n.model.model.as_str().into(),
                    n.model.dimensions.into(),
                    n.mode.as_str().into(),
                    n.max_input_tokens.into(),
                    n.created_by.into(),
                ]),
            )
            .await;
        match r {
            Ok(()) => Ok(c.last_insert_id().unwrap_or(0)),
            Err(mysql_async::Error::Server(s)) if s.code == 1062 => Err(super::failed(format!(
                "column {} is already an embedded column",
                n.column
            ))),
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
                "UPDATE {} SET {set}, updated_at = UTC_TIMESTAMP(3) WHERE embedding_id = ?",
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

    /// Records the backfill's position and adds `rows` to the rows
    /// embedded.
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
            "progress = ?, rows_embedded = rows_embedded + ?",
            id,
            vec![progress.to_string().into(), rows.into()],
        )
        .await
    }

    /// Starts a re-embedding: `pending`, state `reembedding`, the
    /// backfill's position cleared.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn start_reembedding(
        &self,
        c: &mut mysql_async::Conn,
        id: u64,
        pending: &Pending,
    ) -> Result<()> {
        let p = serde_json::to_string(pending).map_err(|e| SprigError::internal(e.to_string()))?;
        self.update(
            c,
            "pending = ?, state = 'reembedding', progress = NULL, last_error = NULL",
            id,
            vec![p.into()],
        )
        .await
    }

    /// Ends a re-embedding: the pending generation becomes the column's.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn finish_reembedding(
        &self,
        c: &mut mysql_async::Conn,
        id: u64,
        pending: &Pending,
    ) -> Result<()> {
        self.update(
            c,
            "source_expr = ?, model_alias = ?, provider = ?, model = ?, dimensions = ?, \
             max_input_tokens = ?, generation = ?, pending = NULL, progress = NULL, \
             state = 'ready', last_error = NULL",
            id,
            vec![
                pending.source.as_str().into(),
                pending.model.alias.as_str().into(),
                pending.model.provider.as_str().into(),
                pending.model.model.as_str().into(),
                pending.model.dimensions.into(),
                pending.max_input_tokens.into(),
                pending.generation.into(),
            ],
        )
        .await
    }

    /// Forgets embedded column `id`.
    ///
    /// # Errors
    /// The catalog is unreachable.
    pub async fn delete(&self, c: &mut mysql_async::Conn, id: u64) -> Result<()> {
        if !self.exists(c).await? {
            return Ok(());
        }
        c.exec_drop(
            format!("DELETE FROM {} WHERE embedding_id = ?", self.t()),
            (id,),
        )
        .await
        .map_err(unavailable)
    }
}
