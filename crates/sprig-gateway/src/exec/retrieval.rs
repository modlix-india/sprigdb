//! Retrieval indexes in a tenant session, on 3306 and 3307
//! ([`sprig_sql::retrieval`] parses the statements):
//!
//! ```text
//! CREATE RETRIEVAL INDEX kb ON [db.]t (body) [TITLE c] [FILTERS (c, ...)]
//!     [CHUNK BY HEADINGS | TOKENS n] [EMBED WITH MODEL 'alias']
//!   INDEX on the table; the alias in ai.embedding_models, a local model
//!   (a provider's: 39997, not built yet); the columns read through the
//!   session's information_schema: the text and title are strings, the
//!   filters plain values, the table has a primary key
//!   then an ai_retrieval_indexes row (building) and a feed subscription
//!   rx:i<id> at the feed's position now; the worker's ai_retrieval job
//!   makes the chunk tables and fills them
//! DROP RETRIEVAL INDEX [IF EXISTS] [db.]kb: INDEX on the table; the row
//!   goes, so searches stop at once; the job drops the chunk tables and
//!   the subscription when it next reads the row
//! SHOW CREATE RETRIEVAL INDEX, SHOW RETRIEVAL INDEXES [FROM db]: indexes
//!   over tables the caller may read
//! ... FROM RETRIEVE([db.]kb, 'query', limit [, 'filter']) ...
//!   search (below), then the call replaced by a derived table of the
//!   hits: the key columns (named as in the table), chunk_no, title,
//!   chunk, score, `rank`, bm25_rank, vector_rank; the statement runs as
//!   any other
//! ```
//!
//! **Search** (the docs search's hybrid search, over shards):
//!
//! ```text
//! q_vec := embed(query) with the bundled model, here    -- none: keyword only
//! per shard of the index (as sprig_ctl, on the chunk table):
//!   bm25 := MATCH(text, title) AGAINST (query) top C [AND filter]
//!   vec  := VEC_DISTANCE_COSINE(embedding, q_vec) top C [AND filter]
//! the lists merged across shards by score (and by distance)
//! visible := the candidates' rows the caller's session reads:
//!   SELECT key, text IS NULL FROM db.t WHERE key IN (...) [AND filter]
//!   (privileges, column grants and row-level security apply; a deleted
//!   row is gone)
//! hits := RRF(bm25 weighted keyword_weight(query), vec) over the visible,
//!   k = 60; at most 2 per row; the first `limit`
//! fewer than `limit` while a list was full: again with C × 4 (C ≤ 1000)
//! ```
//!
//! C is `ai.retrieval.candidates` (40). The filter names only `FILTERS`
//! columns ([`sprig_sql::retrieval::compile_filter`]); it runs on the
//! chunk table's copies and again on the table itself.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use mysql_async::prelude::Queryable as _;
use serde_json::json;
use sprig_ai::embedding::{self, failed};
use sprig_ai::retrieval::store::{NewIndex, RetrievalIndex, Store};
use sprig_ai::retrieval::{self as rx, FilterColumn, KeyColumn, metrics};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::CatalogLocator as _;
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_common::ids::{DbId, GroupId, ObjectId, ShardId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_feed::{ResumeToken, StartAt, TailOptions, TenantTail};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::ai_fn::string_literal;
use sprig_sql::retrieval::{self as sqlrx, Call, Chunking, Create, Shape, Stmt};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};
use crate::tenant::TenantServices;

/// The comment that asks for the key as one JSON column `key` (the REST
/// route's statements; honoured only for the gateway's own).
pub(crate) const KEY_JSON_HINT: &str = "/* sprig-retrieve:key-json */";

/// The most candidates per list and shard a search widens to.
const MAX_CANDIDATES: usize = 1000;

fn no_db() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn no_index(db: &str, name: &str) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!("Retrieval index '{db}.{name}' doesn't exist"),
    )
}

fn sql_err(code: u16, m: impl Into<String>) -> SprigError {
    let state = match code {
        1054 => *b"42S22",
        1064 | 1235 => *b"42000",
        _ => *b"HY000",
    };
    SprigError::standard(code, state, m)
}

/// One chunk found on a shard.
#[derive(Debug, Clone)]
struct Candidate {
    key: Vec<Option<String>>,
    chunk_no: u32,
    title: Option<String>,
    text: String,
}

/// One hit.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hit {
    /// The source row's key, as text (binary columns hexadecimal).
    pub key: Vec<Option<String>>,
    /// The chunk's number in its row.
    pub chunk_no: u32,
    /// The title (and headings) of the chunk.
    pub title: Option<String>,
    /// The chunk.
    pub text: String,
    /// The fused score.
    pub score: f64,
    /// Rank in the full-text list, from 1.
    pub bm25_rank: Option<usize>,
    /// Rank in the vector list, from 1.
    pub vector_rank: Option<usize>,
}

/// A search's answer.
pub(crate) struct Found {
    index: RetrievalIndex,
    db: String,
    table: String,
    hits: Vec<Hit>,
}

/// The id of a candidate in fusion, and the row it belongs to.
fn cand_id(c: &Candidate) -> (Vec<u8>, String) {
    let row = c
        .key
        .iter()
        .map(|k| k.as_deref().unwrap_or("\u{0}"))
        .collect::<Vec<_>>()
        .join("\u{2}");
    (format!("{row}\u{1}{}", c.chunk_no).into_bytes(), row)
}

/// The reciprocal-rank fusion of the lists (best first), at most
/// [`rx::MAX_PER_ROW`] chunks per row, `limit` hits.
fn fuse(keyword: &[Candidate], vector: &[Candidate], weight: f64, limit: usize) -> Vec<Hit> {
    let mut rows: HashMap<Vec<u8>, String> = HashMap::new();
    let mut by_id: HashMap<Vec<u8>, &Candidate> = HashMap::new();
    let mut lists: [Vec<Vec<u8>>; 2] = [Vec::new(), Vec::new()];
    for (n, list) in [keyword, vector].into_iter().enumerate() {
        for c in list {
            let (id, row) = cand_id(c);
            rows.insert(id.clone(), row);
            by_id.entry(id.clone()).or_insert(c);
            lists[n].push(id);
        }
    }
    let [k, v] = lists;
    crate::docs::search::fuse(
        &k,
        &v,
        weight,
        |id| rows.get(id).map_or("", String::as_str),
        limit,
    )
    .into_iter()
    .filter_map(|(id, score, bm25_rank, vector_rank)| {
        let c = by_id.get(&id)?;
        Some(Hit {
            key: c.key.clone(),
            chunk_no: c.chunk_no,
            title: c.title.clone(),
            text: c.text.clone(),
            score,
            bm25_rank,
            vector_rank,
        })
    })
    .collect()
}

/// A key value (text, binary columns hexadecimal) as a SQL literal for a
/// session whose `NO_BACKSLASH_ESCAPES` is `nbe`.
fn key_literal(k: &KeyColumn, v: Option<&str>, nbe: bool) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(h) if k.binary && h.bytes().all(|b| b.is_ascii_hexdigit()) => format!("X'{h}'"),
        Some(s) if k.numeric && !s.is_empty() && s.parse::<f64>().is_ok() => {
            if s.bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                s.to_owned()
            } else {
                string_literal(s, nbe)
            }
        }
        Some(s) => string_literal(s, nbe),
    }
}

/// The key as a JSON object (REST): numbers of numeric columns as
/// numbers, the rest as strings.
fn key_json(keys: &[KeyColumn], v: &[Option<String>]) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for (k, x) in keys.iter().zip(v) {
        let val = match x {
            None => serde_json::Value::Null,
            Some(s) if k.numeric => serde_json::from_str::<serde_json::Number>(s)
                .map_or_else(|_| json!(s), serde_json::Value::Number),
            Some(s) => json!(s),
        };
        m.insert(k.name.clone(), val);
    }
    serde_json::Value::Object(m)
}

/// The derived table a call becomes: one row per hit.
fn derived_table(
    found: &Found,
    alias: Option<&str>,
    key_as_json: bool,
    nbe: bool,
) -> String {
    let keys = &found.index.key_columns;
    let lit = |s: &str| string_literal(s, nbe);
    let rows: Vec<String> = found
        .hits
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let mut cols: Vec<String> = if key_as_json {
                vec![format!(
                    "{} AS `key`",
                    lit(&key_json(keys, &h.key).to_string())
                )]
            } else {
                keys.iter()
                    .zip(&h.key)
                    .map(|(k, v)| format!("{} AS {}", key_literal(k, v.as_deref(), nbe), quote(&k.name)))
                    .collect()
            };
            cols.push(format!("{} AS `chunk_no`", h.chunk_no));
            cols.push(format!(
                "{} AS `title`",
                h.title.as_deref().map_or_else(|| "CAST(NULL AS CHAR)".to_owned(), lit)
            ));
            cols.push(format!("{} AS `chunk`", lit(&h.text)));
            cols.push(format!("CAST({:e} AS DOUBLE) AS `score`", h.score));
            cols.push(format!("{} AS `rank`", i + 1));
            let r = |x: Option<usize>| x.map_or_else(|| "CAST(NULL AS SIGNED)".to_owned(), |n| n.to_string());
            cols.push(format!("{} AS `bm25_rank`", r(h.bm25_rank)));
            cols.push(format!("{} AS `vector_rank`", r(h.vector_rank)));
            format!("SELECT {}", cols.join(", "))
        })
        .collect();
    let body = if rows.is_empty() {
        let mut cols: Vec<String> = if key_as_json {
            vec!["CAST(NULL AS CHAR) AS `key`".to_owned()]
        } else {
            keys.iter()
                .map(|k| format!("NULL AS {}", quote(&k.name)))
                .collect()
        };
        cols.extend(
            [
                "CAST(NULL AS UNSIGNED) AS `chunk_no`",
                "CAST(NULL AS CHAR) AS `title`",
                "CAST(NULL AS CHAR) AS `chunk`",
                "CAST(NULL AS DOUBLE) AS `score`",
                "CAST(NULL AS SIGNED) AS `rank`",
                "CAST(NULL AS SIGNED) AS `bm25_rank`",
                "CAST(NULL AS SIGNED) AS `vector_rank`",
            ]
            .map(str::to_owned),
        );
        format!("SELECT {} FROM DUAL WHERE 0 = 1", cols.join(", "))
    } else {
        rows.join(" UNION ALL ")
    };
    match alias {
        Some(a) => format!("({body}) AS {}", quote(a)),
        None => format!("({body})"),
    }
}

impl PassSession {
    fn rx_services(&self) -> Result<(Arc<TenantServices>, TenantId, Option<u32>)> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        Ok((
            Arc::clone(ts.services()),
            ts.tenant,
            ts.principal.user_id.map(|u| u.0),
        ))
    }

    async fn rx_catalog(&mut self) -> Result<(Arc<TenantCatalog>, bool)> {
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let lctn = ts.lower_case_table_names();
        Ok((ts.refresh().await?, lctn))
    }

    async fn rx_store(&self) -> Result<(mysql_async::Conn, Store)> {
        let (svc, tenant, _) = self.rx_services()?;
        let ep = svc.locator.writer(tenant).await?;
        let conn = ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok((conn, Store::new(ep.store.schema())))
    }

    /// Whether the session's user holds `a` on table `object` of `db`.
    fn rx_allowed(&self, cat: &TenantCatalog, lctn: bool, db: &str, db_id: DbId, object: u64, a: Action) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return false;
        };
        let Some(user) = ts.principal.user_id else {
            return true;
        };
        let g = crate::tenant::authz::grants_of(ts.services(), cat, Some(user));
        if g.unrestricted() || g.allows(a, Target::Tenant) {
            return true;
        }
        let db_key = database_key(db, lctn);
        g.allows(a, Target::Database { db_key: &db_key })
            || g.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db: db_id,
                    object: ObjectId(object),
                },
            )
    }

    fn rx_denied(&self, a: &str, db: &str, table: &str) -> SprigError {
        let user = self
            .tenant
            .as_deref()
            .map(|t| t.principal.user.clone())
            .unwrap_or_default();
        SprigError::standard(
            1142,
            *b"42000",
            format!("{a} command denied to user '{user}' for table '{db}.{table}'"),
        )
    }

    /// Retrieval index statements: `None` when the statement isn't one.
    pub(super) async fn tenant_retrieval_ddl(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !sqlrx::mentions_ddl(text) || self.tenant.as_deref().is_none_or(|ts| ts.types.internal)
        {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        let stmt = match sqlrx::parse(text, &lex) {
            None => return Ok(None),
            Some(Err(m)) => return Ok(Some(self.fail(out, &sql_err(1064, m)).await?)),
            Some(Ok(s)) => s,
        };
        let wire = out.wire();
        let r = match stmt {
            Stmt::Create(c) => Box::pin(self.rx_create(cx, wire, &c)).await.map(|()| None),
            Stmt::Drop {
                db,
                name,
                if_exists,
            } => self.rx_drop(db, &name, if_exists).await.map(|()| None),
            Stmt::ShowCreate { db, name } => self.rx_show_create(db, &name).await.map(Some),
            Stmt::Show { db } => self.rx_show(db).await.map(Some),
        };
        match r {
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Some(select)) => {
                let t = Bytes::from(select);
                Box::pin(self.tenant_text(cx, &t, attrs, more, format, out))
                    .await
                    .map(Some)
            }
            Ok(None) => {
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                out.ok(OkPacket::new(status)).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
        }
    }

    /// `db`.`table`'s database and object ids.
    fn rx_table(cat: &TenantCatalog, db: &str, table: &str, lctn: bool) -> Option<(DbId, u64)> {
        let d = cat.database(&database_key(db, lctn))?;
        let o = cat
            .object(
                d.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, table, lctn),
            )
            .filter(|o| o.state == ObjectState::Active)?;
        Some((d.db_id, o.object_id.0))
    }

    #[allow(clippy::too_many_lines)] // one check per part of the statement
    async fn rx_create(&mut self, cx: &mut ExecCtx<'_>, wire: Wire, c: &Create) -> Result<()> {
        let db = c.db.clone().or_else(|| self.current_logical()).ok_or_else(no_db)?;
        let (svc, tenant, user) = self.rx_services()?;
        let (cat, lctn) = self.rx_catalog().await?;
        let Some((db_id, object)) = Self::rx_table(&cat, &db, &c.table, lctn) else {
            return Err(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{db}.{}' doesn't exist", c.table),
            ));
        };
        if !self.rx_allowed(&cat, lctn, &db, db_id, object, Action::Index) {
            return Err(self.rx_denied("INDEX", &db, &c.table));
        }
        // The model.
        let alias = c.model.clone().unwrap_or_else(|| embedding::LOCAL_ALIAS.to_owned());
        let models = svc.settings.get(
            &embedding::MODELS,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        );
        let model = embedding::resolve(&alias, &models)?;
        if !model.is_local() {
            return Err(failed(format!(
                "retrieval index {}: '{alias}' is a provider's model; retrieval indexes use \
                 local models for now ({})",
                c.name,
                embedding::LOCAL_ALIAS
            )));
        }
        // The columns, through the session.
        let lit = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
        let cols = self
            .sub_rows(
                cx,
                wire,
                &format!(
                    "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                    lit(&db),
                    lit(&c.table)
                ),
            )
            .await?;
        let s = |v: &Option<Vec<u8>>| String::from_utf8_lossy(v.as_deref().unwrap_or(b"")).into_owned();
        let columns: Vec<(String, String)> = cols.iter().map(|r| (s(&r[0]), s(&r[1]))).collect();
        let type_of = |name: &str| -> Option<(String, String)> {
            columns
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .cloned()
        };
        let col_err = |what: &str, name: &str| {
            sql_err(1054, format!("Unknown column '{name}' in '{what}'"))
        };
        let (text_column, text_type) =
            type_of(&c.column).ok_or_else(|| col_err("RETRIEVAL INDEX", &c.column))?;
        if !rx::is_text_type(&text_type) {
            return Err(failed(format!(
                "retrieval index {}: column {text_column} is {text_type}; the text column must be \
                 CHAR, VARCHAR or a TEXT type",
                c.name
            )));
        }
        let title_column = match &c.title {
            None => None,
            Some(t) => {
                let (n, ty) = type_of(t).ok_or_else(|| col_err("TITLE", t))?;
                if !rx::is_text_type(&ty) {
                    return Err(failed(format!(
                        "retrieval index {}: TITLE column {n} is {ty}; it must be CHAR, VARCHAR \
                         or a TEXT type",
                        c.name
                    )));
                }
                Some(n)
            }
        };
        let mut filters = Vec::new();
        for f in &c.filters {
            let (n, ty) = type_of(f).ok_or_else(|| col_err("FILTERS", f))?;
            if !rx::is_filter_type(&ty) || n.chars().count() > 62 {
                return Err(failed(format!(
                    "retrieval index {}: FILTERS column {n} ({ty}) can't be a filter: filters are \
                     numbers, dates, short strings, ENUM or SET, with names of at most 62 characters",
                    c.name
                )));
            }
            filters.push(FilterColumn {
                name: n,
                column_type: ty,
            });
        }
        // The primary key, in key order.
        let pk = self
            .sub_rows(
                cx,
                wire,
                &format!(
                    "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {} \
                     AND TABLE_NAME = {} AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
                    lit(&db),
                    lit(&c.table)
                ),
            )
            .await?;
        let mut key_columns = Vec::new();
        for r in &pk {
            let name = s(&r[0]);
            if name.to_ascii_lowercase().starts_with("_sprig_") {
                continue;
            }
            let (n, ty) = type_of(&name).ok_or_else(|| col_err("PRIMARY KEY", &name))?;
            if n.chars().count() > 62 {
                return Err(failed(format!(
                    "retrieval index {}: key column {n} has a name longer than 62 characters",
                    c.name
                )));
            }
            key_columns.push(KeyColumn {
                name: n,
                binary: rx::is_binary_type(&ty),
                numeric: rx::is_numeric_type(&ty),
                column_type: ty,
            });
        }
        if key_columns.is_empty() {
            return Err(failed(format!(
                "retrieval index {}: {db}.{} has no primary key; a retrieval index needs one",
                c.name, c.table
            )));
        }
        let (mut conn, store) = self.rx_store().await?;
        let id = store
            .insert(
                &mut conn,
                &NewIndex {
                    db_id: db_id.0,
                    name: c.name.clone(),
                    object_id: object,
                    text_column,
                    title_column,
                    filters,
                    key_columns,
                    chunking: c.chunking.as_catalog(),
                    model,
                    progress: json!({}),
                    created_by: user,
                },
            )
            .await?;
        let Some(id) = id else {
            if c.if_not_exists {
                return Ok(());
            }
            return Err(SprigError::standard(
                1061,
                *b"42000",
                format!("Duplicate key name '{}': {db} has a retrieval index of that name", c.name),
            ));
        };
        if let Err(e) = register(&svc, tenant, id, &db, &c.table, object).await {
            let _ = store.delete(&mut conn, id).await;
            return Err(e);
        }
        tracing::info!(%tenant, %db, table = %c.table, index = id, name = %c.name, "retrieval index created");
        Ok(())
    }

    /// The index `[db.]name` names, with its database's name and id.
    async fn rx_find(
        &mut self,
        db: Option<String>,
        name: &str,
    ) -> Result<(Option<RetrievalIndex>, String, Arc<TenantCatalog>, bool)> {
        let db = db.or_else(|| self.current_logical()).ok_or_else(no_db)?;
        let (cat, lctn) = self.rx_catalog().await?;
        let Some(d) = cat.database(&database_key(&db, lctn)) else {
            return Ok((None, db, cat, lctn));
        };
        let db_id = d.db_id.0;
        let (mut conn, store) = self.rx_store().await?;
        let ix = store.by_name(&mut conn, db_id, name).await?;
        Ok((ix, db, cat, lctn))
    }

    fn rx_table_name(cat: &TenantCatalog, ix: &RetrievalIndex) -> String {
        cat.objects
            .get(ObjectId(ix.object_id))
            .map_or_else(|| format!("#{}", ix.object_id), |o| o.name.clone())
    }

    async fn rx_drop(&mut self, db: Option<String>, name: &str, if_exists: bool) -> Result<()> {
        let (ix, db, cat, lctn) = self.rx_find(db, name).await?;
        let Some(ix) = ix else {
            if if_exists {
                return Ok(());
            }
            return Err(no_index(&db, name));
        };
        let table = Self::rx_table_name(&cat, &ix);
        if !self.rx_allowed(&cat, lctn, &db, DbId(ix.db_id), ix.object_id, Action::Index) {
            return Err(self.rx_denied("INDEX", &db, &table));
        }
        let (mut conn, store) = self.rx_store().await?;
        store.delete(&mut conn, ix.id).await?;
        let (_, tenant, _) = self.rx_services()?;
        tracing::info!(%tenant, %db, index = ix.id, %name, "retrieval index dropped");
        Ok(())
    }

    fn rx_shape<'a>(ix: &'a RetrievalIndex, db: &'a str, table: &'a str) -> (Shape<'a>, Vec<String>) {
        let filters: Vec<String> = ix.filters.iter().map(|f| f.name.clone()).collect();
        (
            Shape {
                name: &ix.name,
                db,
                table,
                column: &ix.text_column,
                title: ix.title_column.as_deref(),
                filters: &[],
                chunking: Chunking::from_catalog(&ix.chunking)
                    .unwrap_or(Chunking::Tokens(sqlrx::DEFAULT_CHUNK_TOKENS)),
                model: &ix.model.alias,
            },
            filters,
        )
    }

    fn rx_render(ix: &RetrievalIndex, db: &str, table: &str) -> String {
        let (shape, filters) = Self::rx_shape(ix, db, table);
        sqlrx::render(&Shape {
            filters: &filters,
            ..shape
        })
    }

    async fn rx_show_create(&mut self, db: Option<String>, name: &str) -> Result<String> {
        let (ix, db, cat, lctn) = self.rx_find(db, name).await?;
        let Some(ix) = ix else {
            return Err(no_index(&db, name));
        };
        let table = Self::rx_table_name(&cat, &ix);
        if !self.rx_allowed(&cat, lctn, &db, DbId(ix.db_id), ix.object_id, Action::Select) {
            return Err(self.rx_denied("SELECT", &db, &table));
        }
        let (lex, _) = self.lex_opts();
        let nbe = lex.no_backslash_escapes;
        Ok(format!(
            "SELECT {} AS `Retrieval Index`, {} AS `Create Retrieval Index`",
            string_literal(&ix.name, nbe),
            string_literal(&Self::rx_render(&ix, &db, &table), nbe)
        ))
    }

    async fn rx_show(&mut self, db: Option<String>) -> Result<String> {
        let db = db.or_else(|| self.current_logical()).ok_or_else(no_db)?;
        let (cat, lctn) = self.rx_catalog().await?;
        let Some(d) = cat.database(&database_key(&db, lctn)) else {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        };
        let db_id = d.db_id;
        let (mut conn, store) = self.rx_store().await?;
        let all = store.of_db(&mut conn, db_id.0).await?;
        let (lex, _) = self.lex_opts();
        let nbe = lex.no_backslash_escapes;
        let l = |s: &str| string_literal(s, nbe);
        let mut rows = Vec::new();
        for ix in all {
            if !self.rx_allowed(&cat, lctn, &db, db_id, ix.object_id, Action::Select) {
                continue;
            }
            let table = Self::rx_table_name(&cat, &ix);
            rows.push(format!(
                "SELECT {} AS `Index`, {} AS `Table`, {} AS `Column`, {} AS `Model`, {} AS `State`, \
                 {} AS `Rows`, {} AS `Shards`, {} AS `Error`",
                l(&ix.name),
                l(&table),
                l(&ix.text_column),
                l(&ix.model.alias),
                l(ix.state.as_str()),
                ix.rows_indexed,
                ix.shards().len(),
                ix.last_error.as_deref().map_or_else(|| "CAST(NULL AS CHAR)".to_owned(), l)
            ));
        }
        if rows.is_empty() {
            return Ok("SELECT '' AS `Index`, '' AS `Table`, '' AS `Column`, '' AS `Model`, \
                       '' AS `State`, 0 AS `Rows`, 0 AS `Shards`, CAST(NULL AS CHAR) AS `Error` \
                       FROM DUAL WHERE 0 = 1"
                .to_owned());
        }
        Ok(rows.join(" UNION ALL "))
    }

    /// `... FROM RETRIEVE(...)`: searches each call and runs the statement
    /// with the calls replaced by their hits; `None` when the text has
    /// none.
    pub(super) async fn tenant_retrieve(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !sqlrx::mentions(text) {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.principal.must_change_password {
            return Ok(None);
        }
        let internal = ts.types.internal;
        let (lex, _) = self.lex_opts();
        let calls = match sqlrx::calls(text, &lex) {
            Ok(c) if c.is_empty() => return Ok(None),
            Ok(c) => c,
            Err(e) => return Ok(Some(self.fail(out, &sql_err(e.code, e.message)).await?)),
        };
        if format == RowFormat::Binary {
            let e = SprigError::new(
                SprigCode::NameRewriteUnsupported,
                "RETRIEVE() in a prepared statement: run it as a plain statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let key_json = internal && sql.trim_start().starts_with(KEY_JSON_HINT);
        let wire = out.wire();
        let mut edits = Vec::with_capacity(calls.len());
        for c in &calls {
            let found = match Box::pin(self.retrieval_search(cx, wire, c)).await {
                Ok(f) => f,
                Err(e) => return Ok(Some(self.fail(out, &e).await?)),
            };
            let alias = (!c.aliased).then_some("retrieve");
            edits.push((
                c.span.clone(),
                derived_table(&found, alias, key_json, lex.no_backslash_escapes),
            ));
            tracing::debug!(db = %found.db, table = %found.table, hits = found.hits.len(), "RETRIEVE answered");
        }
        let rewritten = Bytes::from(sprig_sql::embedcol::splice(sql, edits));
        Box::pin(self.tenant_text(cx, &rewritten, attrs, more, format, out))
            .await
            .map(Some)
    }

    /// One `RETRIEVE()`'s search (see the module docs), timed and counted.
    async fn retrieval_search(&mut self, cx: &mut ExecCtx<'_>, wire: Wire, call: &Call) -> Result<Found> {
        let started = Instant::now();
        let r = Box::pin(self.retrieval_search_inner(cx, wire, call)).await;
        match &r {
            Ok((f, mode)) => {
                metrics::searches(if f.hits.is_empty() { "empty" } else { "ok" });
                metrics::search_seconds(mode, started.elapsed().as_secs_f64());
            }
            Err(_) => metrics::searches("error"),
        }
        r.map(|(f, _)| f)
    }

    #[allow(clippy::too_many_lines)] // the search of the module docs
    async fn retrieval_search_inner(
        &mut self,
        cx: &mut ExecCtx<'_>,
        wire: Wire,
        call: &Call,
    ) -> Result<(Found, &'static str)> {
        let (ix, db, cat, _) = self.rx_find(call.db.clone(), &call.index).await?;
        let Some(ix) = ix else {
            return Err(no_index(call.db.as_deref().unwrap_or(&db), &call.index));
        };
        let table = cat
            .objects
            .get(ObjectId(ix.object_id))
            .filter(|o| o.state == ObjectState::Active)
            .map(|o| o.name.clone())
            .ok_or_else(|| no_index(&db, &call.index))?;
        let (svc, tenant, _) = self.rx_services()?;
        let (lex, _) = self.lex_opts();
        let nbe = lex.no_backslash_escapes;
        // The filter, for the chunk tables and for the table.
        let filter_names: Vec<String> = ix.filters.iter().map(|f| f.name.clone()).collect();
        let (shard_filter, table_filter) = match &call.filter {
            None => (None, None),
            Some(f) => {
                let on_chunks = sqlrx::compile_filter(
                    f,
                    &lex,
                    &filter_names,
                    &|c| quote(&rx::filter_column(c)),
                    false,
                )
                .map_err(|m| sql_err(1054, m))?;
                let on_table = sqlrx::compile_filter(f, &lex, &filter_names, &|c| quote(c), nbe)
                    .map_err(|m| sql_err(1054, m))?;
                (Some(on_chunks), Some(on_table))
            }
        };
        if !ix.model.is_local() {
            return Err(failed(format!(
                "retrieval index {}: '{}' is a provider's model; not available yet",
                ix.name, ix.model.alias
            )));
        }
        // The query's embedding: the bundled model, in this process; a
        // model that doesn't load leaves the search keyword-only.
        let q_vec: Option<Vec<f32>> = match crate::docs::service().map(|d| d.model_dir()) {
            None => None,
            Some(dir) => {
                let q = call.query.clone();
                tokio::task::spawn_blocking(move || {
                    let m = sprig_embed::local::shared(&dir)?;
                    m.embed_query(&q).map(|v| v.to_vec()).map_err(|e| e.to_string())
                })
                .await
                .ok()
                .and_then(|r| {
                    r.map_err(|why| tracing::warn!(%why, "retrieval search without the query embedding"))
                        .ok()
                })
            }
        };
        // The index's shards and their clusters.
        let maps = crate::exec::router::map_cache(&svc, tenant).ok();
        let mut shards = Vec::new();
        for schema in ix.shards() {
            let Ok(SchemaName::Shard { group, shard, .. }) = schema.parse::<SchemaName>() else {
                continue;
            };
            let mapped = maps
                .as_ref()
                .and_then(|m| m.get(tenant, GroupId(group.0)).ok())
                .and_then(|m| m.shard(ShardId(shard.0)).map(|r| r.cluster.clone()));
            let cluster = match mapped {
                Some(c) => c,
                None => svc.locator.cluster(tenant).await?,
            };
            shards.push((schema, cluster));
        }
        let keys = ix.key_columns.clone();
        let nk = keys.len();
        let kcols: Vec<String> = keys
            .iter()
            .map(|k| quote(&rx::key_column(&k.name)))
            .collect();
        let q_lit = string_literal(&call.query, false);
        let v_lit = q_vec.as_ref().map(|v| {
            let parts: Vec<String> = v.iter().map(ToString::to_string).collect();
            format!("'[{}]'", parts.join(","))
        });
        let candidates_setting = svc.settings.get(
            &rx::CANDIDATES,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        );
        let mut per_list = usize::try_from(candidates_setting).unwrap_or(40).max(1);
        let limit = usize::try_from(call.limit).unwrap_or(10);
        let weight = sprig_docs::search::keyword_weight(&call.query);
        let and_filter = shard_filter
            .as_deref()
            .map_or_else(String::new, |f| format!(" AND ({f})"));
        let where_filter = shard_filter
            .as_deref()
            .map_or_else(String::new, |f| format!(" WHERE ({f})"));
        loop {
            // The two lists, per shard, merged.
            let mut kw: Vec<(f64, Candidate)> = Vec::new();
            let mut vecs: Vec<(f64, Candidate)> = Vec::new();
            let mut full = false;
            for (schema, cluster) in &shards {
                let pool = svc.locator.ctl_pool(cluster).await?;
                let mut c = pool.get_conn_bounded().await.map_err(|e| {
                    SprigError::new(SprigCode::BackendUnavailable, format!("retrieval: {e}"))
                })?;
                let t = format!("{}.{}", quote(schema), quote(&rx::table_name(ix.id)));
                let head = format!("{}, chunk_no, title, text", kcols.join(", "));
                let mut queries = vec![(
                    true,
                    format!(
                        "SELECT {head}, MATCH(text, title) AGAINST ({q_lit} IN NATURAL LANGUAGE MODE) AS s \
                         FROM {t} WHERE MATCH(text, title) AGAINST ({q_lit} IN NATURAL LANGUAGE MODE){and_filter} \
                         ORDER BY s DESC LIMIT {per_list}"
                    ),
                )];
                if let Some(v) = &v_lit {
                    queries.push((
                        false,
                        format!(
                            "SELECT {head}, VEC_DISTANCE_COSINE(embedding, VEC_FromText({v})) AS d \
                             FROM {t}{where_filter} ORDER BY d LIMIT {per_list}"
                        ),
                    ));
                }
                for (keyword, q) in queries {
                    let rows: Vec<mysql_async::Row> = match c.query(q).await {
                        Ok(r) => r,
                        // A shard whose chunk table isn't made yet.
                        Err(mysql_async::Error::Server(s)) if s.code == 1146 => break,
                        Err(e) => {
                            return Err(SprigError::new(
                                SprigCode::BackendUnavailable,
                                format!("retrieval search on {schema}: {e}"),
                            ));
                        }
                    };
                    if rows.len() >= per_list {
                        full = true;
                    }
                    for r in rows {
                        let b = |i: usize| match r.as_ref(i) {
                            Some(mysql_async::Value::Bytes(b)) => Some(b.clone()),
                            _ => None,
                        };
                        let text_of = |i: usize| b(i).map(|x| String::from_utf8_lossy(&x).into_owned());
                        let cand = Candidate {
                            key: (0..nk).map(|i| keys[i].text(b(i).as_deref())).collect(),
                            chunk_no: text_of(nk).and_then(|s| s.parse().ok()).unwrap_or(0),
                            title: text_of(nk + 1),
                            text: text_of(nk + 2).unwrap_or_default(),
                        };
                        let score: f64 = text_of(nk + 3).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        if keyword {
                            kw.push((score, cand));
                        } else {
                            vecs.push((score, cand));
                        }
                    }
                }
            }
            kw.sort_by(|a, b| b.0.total_cmp(&a.0));
            vecs.sort_by(|a, b| a.0.total_cmp(&b.0));
            // The rows the caller may see, through the session.
            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            let mut seen: HashSet<Vec<Option<String>>> = HashSet::new();
            for (_, c) in kw.iter().chain(&vecs) {
                if seen.insert(c.key.clone()) {
                    rows.push(c.key.clone());
                }
            }
            let mut visible: HashSet<Vec<Option<String>>> = HashSet::new();
            for part in rows.chunks(200) {
                let pred: Vec<String> = part
                    .iter()
                    .map(|k| {
                        let eq: Vec<String> = keys
                            .iter()
                            .zip(k)
                            .map(|(c, v)| format!("{} = {}", quote(&c.name), key_literal(c, v.as_deref(), nbe)))
                            .collect();
                        format!("({})", eq.join(" AND "))
                    })
                    .collect();
                let sel: Vec<String> = keys
                    .iter()
                    .map(|k| {
                        if k.binary {
                            format!("LOWER(HEX({}))", quote(&k.name))
                        } else {
                            quote(&k.name)
                        }
                    })
                    .collect();
                let mut readable = vec![format!("{} IS NULL", quote(&ix.text_column))];
                if let Some(t) = &ix.title_column {
                    readable.push(format!("{} IS NULL", quote(t)));
                }
                let sql = format!(
                    "SELECT {}, {} FROM {}.{} WHERE ({}){}",
                    sel.join(", "),
                    readable.join(", "),
                    quote(&db),
                    quote(&table),
                    pred.join(" OR "),
                    table_filter
                        .as_deref()
                        .map_or_else(String::new, |f| format!(" AND ({f})"))
                );
                for r in self.sub_rows(cx, wire, &sql).await? {
                    visible.insert(
                        r.iter()
                            .take(nk)
                            .map(|v| v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                            .collect(),
                    );
                }
            }
            let kw: Vec<Candidate> = kw
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| visible.contains(&c.key))
                .collect();
            let vecs: Vec<Candidate> = vecs
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| visible.contains(&c.key))
                .collect();
            let hits = fuse(&kw, &vecs, weight, limit);
            let hidden = rows.len() > visible.len();
            if hits.len() >= limit || !full || !hidden || per_list >= MAX_CANDIDATES {
                return Ok((
                    Found {
                        index: ix,
                        db,
                        table,
                        hits,
                    },
                    if q_vec.is_some() { "hybrid" } else { "keyword" },
                ));
            }
            per_list = (per_list * 4).min(MAX_CANDIDATES);
        }
    }
}

/// Registers index `id`'s feed subscription at the feed's position now:
/// the worker's job misses no change committed after the index was
/// created, and its fill covers the rows before.
async fn register(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    id: u64,
    db: &str,
    table: &str,
    object: u64,
) -> Result<()> {
    let subs = Subscriptions::new(svc.locator.dc_store());
    let (clusters, placement, names) = crate::http::changes::feed(svc)?;
    let tail = TenantTail::start(
        tenant,
        StartAt::Now,
        clusters,
        Arc::clone(&placement),
        names,
        TailOptions {
            progress_every: std::time::Duration::from_millis(500),
            ..TailOptions::default()
        },
    )
    .await?;
    let segment = placement
        .history(tenant)
        .await?
        .last()
        .map_or(0, |h| h.map_version);
    let token = ResumeToken {
        tenant,
        clamp: 0,
        cluster: tail.cluster().clone(),
        segment,
        pos: tail.position().clone(),
        until: None,
        progress: true,
    };
    drop(tail);
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0);
    let sub = Subscription {
        tenant,
        name: rx::sub_name(id),
        kind: rx::KIND.to_owned(),
        ordering: "merged".to_owned(),
        owner_ref: Some(
            json!({"retrieval": id, "db": db, "table": table, "object": object}).to_string(),
        ),
        state: "active".to_owned(),
        created_at_ms: now_ms,
    };
    subs.register_at(&sub, &token, now_ms)
        .await
        .map_err(|e| SprigError::internal(format!("the retrieval index's subscription: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(k: &str, no: u32) -> Candidate {
        Candidate {
            key: vec![Some(k.to_owned())],
            chunk_no: no,
            title: None,
            text: format!("{k}/{no}"),
        }
    }

    #[test]
    fn fusion_ranks_and_caps_rows() {
        let kw = vec![cand("1", 0), cand("1", 1), cand("1", 2), cand("2", 0)];
        let v = vec![cand("2", 0), cand("1", 0), cand("3", 0)];
        let hits = fuse(&kw, &v, 1.0, 10);
        // Row 1 keeps two chunks; 1/0 is in both lists and first.
        assert_eq!(hits[0].text, "1/0");
        assert_eq!(hits[0].bm25_rank, Some(1));
        assert_eq!(hits[0].vector_rank, Some(2));
        assert_eq!(hits.iter().filter(|h| h.key[0].as_deref() == Some("1")).count(), 2);
        assert!(hits.iter().any(|h| h.text == "3/0" && h.bm25_rank.is_none()));
        assert_eq!(fuse(&kw, &v, 1.0, 1).len(), 1);
    }

    #[test]
    fn hits_become_a_derived_table() {
        let keys = vec![KeyColumn {
            name: "id".into(),
            column_type: "int(11)".into(),
            binary: false,
            numeric: true,
        }];
        let ix = RetrievalIndex {
            id: 1,
            db_id: 1,
            name: "kb".into(),
            object_id: 2,
            text_column: "body".into(),
            title_column: None,
            filters: Vec::new(),
            key_columns: keys.clone(),
            chunking: "tokens:400".into(),
            model: embedding::ModelRef::local(),
            state: sprig_ai::retrieval::store::State::Ready,
            progress: json!({}),
            rows_indexed: 0,
            last_error: None,
            created_by: None,
        };
        let mut found = Found {
            index: ix,
            db: "d".into(),
            table: "t".into(),
            hits: vec![Hit {
                key: vec![Some("7".into())],
                chunk_no: 0,
                title: None,
                text: "it's".into(),
                score: 0.5,
                bm25_rank: Some(1),
                vector_rank: None,
            }],
        };
        let sql = derived_table(&found, Some("retrieve"), false, false);
        assert!(sql.starts_with("(SELECT 7 AS `id`, 0 AS `chunk_no`, CAST(NULL AS CHAR) AS `title`, 'it''s' AS `chunk`"), "{sql}");
        assert!(sql.ends_with(") AS `retrieve`"));
        let js = derived_table(&found, None, true, false);
        assert!(js.contains(r#"'{"id":7}' AS `key`"#), "{js}");
        found.hits.clear();
        assert!(derived_table(&found, None, false, false).contains("WHERE 0 = 1"));
        // Keys as literals.
        assert_eq!(key_literal(&keys[0], Some("1 OR 1"), false), "'1 OR 1'");
        assert_eq!(key_literal(&keys[0], Some("-3"), false), "-3");
        assert_eq!(key_json(&keys, &[Some("7".into())]), json!({"id": 7}));
    }
}
