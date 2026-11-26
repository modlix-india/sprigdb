//! Row images checked against a session's privileges and row-level
//! security: what a change stream over gRPC, SSE or WebSocket, or a
//! Redis keyspace notification, may show its subscriber.
//!
//! A caller in this process (the API's `Changes.Watch`, the Redis front
//! end) sends one marked statement through a session of the subscriber,
//! `/* sprig rls images <nonce> */ <ImagesRequest as JSON>`; the nonce is
//! this process's own, so no client can send it. The answer is one row per
//! image, `(t, i, v)`:
//!
//! ```text
//! table t of the request, image i:
//!   no SELECT on the table (or no such table)  → v = 0
//!   `watch` asked and no WATCH on the table     → v = 0
//!   image null                                 → v = 1 (the privilege alone)
//!   the table's policies don't bind the session → v = 1
//!   bound: the session's SELECT policies, rendered with its session values
//!     (sprig_gateway::tenant::rls_streams::record_filter), evaluated by
//!     MariaDB over the image as a row of the table → v = 0 or 1
//! ```

use std::sync::OnceLock;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::StatusFlags;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

const MARK: &str = "/* sprig rls images ";

fn nonce() -> &'static str {
    static N: OnceLock<String> = OnceLock::new();
    N.get_or_init(|| {
        format!(
            "{}{}",
            uuid::Uuid::now_v7().simple(),
            uuid::Uuid::now_v7().simple()
        )
    })
}

/// One table's images.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableImages {
    /// The logical database.
    pub db: String,
    /// The table (a collection's or key-value namespace's table too).
    pub table: String,
    /// Each image as a JSON object of column values, or `null` for the
    /// privilege alone.
    pub images: Vec<Option<serde_json::Map<String, serde_json::Value>>>,
}

/// What is asked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ImagesRequest {
    /// The tables.
    pub tables: Vec<TableImages>,
    /// Binary string columns carry their bytes as plain JSON strings (a
    /// key-value key), not base64 as change records do.
    #[serde(default)]
    pub plain_binary: bool,
    /// Also `WATCH` on each table (an application
    /// change stream needs it); `Changes.Watch` sets it, keyspace
    /// notifications don't.
    #[serde(default)]
    pub watch: bool,
}

/// The marked statement of `req`.
#[must_use]
pub(crate) fn statement(req: &ImagesRequest) -> String {
    format!(
        "{MARK}{} */ {}",
        nonce(),
        serde_json::to_string(req).unwrap_or_default()
    )
}

/// The answer as `[table][image] → visible`, from the statement's rows
/// `(t, i, v)` as text.
#[must_use]
pub(crate) fn answer(req: &ImagesRequest, rows: &[Vec<Option<String>>]) -> Vec<Vec<bool>> {
    let mut out: Vec<Vec<bool>> = req
        .tables
        .iter()
        .map(|t| vec![false; t.images.len()])
        .collect();
    for r in rows {
        let n = |i: usize| {
            r.get(i)
                .cloned()
                .flatten()
                .and_then(|v| v.parse::<usize>().ok())
        };
        if let (Some(t), Some(i), Some(v)) = (n(0), n(1), n(2))
            && let Some(slot) = out.get_mut(t).and_then(|x| x.get_mut(i))
        {
            *slot = v == 1;
        }
    }
    out
}

impl PassSession {
    /// The marked statement (see the module docs): `None` for any other.
    pub(super) async fn tenant_rls_images(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !text.starts_with(MARK.as_bytes()) || self.tenant.is_none() {
            return Ok(None);
        }
        let s = String::from_utf8_lossy(&text[MARK.len()..]).into_owned();
        let Some(req) = s
            .split_once(" */ ")
            .filter(|(n, _)| *n == nonce())
            .and_then(|(_, j)| serde_json::from_str::<ImagesRequest>(j).ok())
        else {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: an image check isn't a client statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        };
        let rows = match self.check_images(cx, &req).await {
            Ok(r) => r,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let col = |n: &str| crate::pipeline::text_column(n, cx.proto.collation);
        let rows = sprig_types::Rows {
            columns: vec![col("t"), col("i"), col("v")],
            rows: rows
                .into_iter()
                .map(|(t, i, v)| {
                    vec![
                        Some(t.to_string().into_bytes()),
                        Some(i.to_string().into_bytes()),
                        Some(if v { b"1".to_vec() } else { b"0".to_vec() }),
                    ]
                })
                .collect(),
        };
        super::types::send_rows(out, &rows, RowFormat::Text, status).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// `(table, image, visible)` for every image of `req`.
    #[allow(clippy::too_many_lines)] // one pass per table, in order
    pub(super) async fn check_images(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &ImagesRequest,
    ) -> Result<Vec<(usize, usize, bool)>> {
        let (lex, _) = self.lex_opts();
        let current = self.current_logical();
        let mut out = Vec::new();
        for (ti, t) in req.tables.iter().enumerate() {
            let none = |out: &mut Vec<(usize, usize, bool)>| {
                for i in 0..t.images.len() {
                    out.push((ti, i, false));
                }
            };
            let (cat, lctn) = {
                let ts = self
                    .tenant
                    .as_deref_mut()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                (ts.refresh().await?, ts.lower_case_table_names())
            };
            let db_key = database_key(&t.db, lctn);
            let Some(db) = cat
                .database(&db_key)
                .filter(|d| d.state == DbState::Active)
                .cloned()
            else {
                none(&mut out);
                continue;
            };
            let Some(obj) = cat
                .object(
                    db.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, &t.table, lctn),
                )
                .filter(|o| o.state == ObjectState::Active)
                .cloned()
            else {
                none(&mut out);
                continue;
            };
            let (filter, slot) = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                let g = crate::tenant::authz::privileges(ts, &cat);
                let on = Target::Object {
                    db_key: &db_key,
                    db: db.db_id,
                    object: obj.object_id,
                };
                if !g.unrestricted()
                    && (!g.allows(Action::Select, on)
                        || (req.watch && !g.allows(Action::Watch, on)))
                {
                    none(&mut out);
                    continue;
                }
                let bound = !g.unrestricted() && g.row_filter(on, Action::Select).is_some();
                let slot = if bound && engine_table(&cat, obj.object_id) {
                    None
                } else if bound {
                    ts.schema_of(Some(&db.name))
                        .map(|tm| (tm, self.target.cluster.clone()))
                } else {
                    None
                };
                (bound, slot)
            };
            if !filter {
                for i in 0..t.images.len() {
                    out.push((ti, i, true));
                }
                continue;
            }
            if engine_table(&cat, obj.object_id) {
                let seen = self
                    .engine_images(cx, req, t, &cat, &db.name, obj.object_id, lex)
                    .await;
                for (i, v) in seen.iter().enumerate() {
                    out.push((ti, i, *v));
                }
                metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "images")
                    .increment(seen.iter().filter(|v| !**v).count() as u64);
                continue;
            }
            let Some((schema, cluster)) = slot else {
                none(&mut out);
                continue;
            };
            let f = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                let columns = crate::tenant::rls_streams::filter_columns(
                    ts.services(),
                    cat.tenant.0,
                    &obj,
                    &cluster,
                    &schema,
                )
                .await?;
                let columns: Vec<_> = if req.plain_binary {
                    columns
                        .iter()
                        .map(|c| match &c.plain {
                            Some(ty) => crate::tenant::rls_streams::FilterColumn {
                                ty: ty.clone(),
                                opaque: false,
                                ..c.clone()
                            },
                            None => c.clone(),
                        })
                        .collect()
                } else {
                    columns.to_vec()
                };
                crate::tenant::rls_streams::record_filter(
                    ts,
                    &cat,
                    &db.name,
                    obj.object_id,
                    &columns,
                    lex,
                    current.as_deref(),
                )
                .await?
            };
            let Some(f) = f else {
                for i in 0..t.images.len() {
                    out.push((ti, i, true));
                }
                continue;
            };
            // Null images are the privilege alone; the others go to MariaDB
            // in one statement.
            let mut parts = Vec::new();
            for (i, img) in t.images.iter().enumerate() {
                match img {
                    None => out.push((ti, i, true)),
                    Some(m) => {
                        let v = serde_json::json!({ "before": m }).to_string();
                        parts.push(format!(
                            "SELECT {i} AS i, {} AS v",
                            sprig_sql::rls::sql_string(&v, lex.no_backslash_escapes)
                        ));
                    }
                }
            }
            if parts.is_empty() {
                continue;
            }
            let sql = format!(
                "SELECT `__sprig_cl`.i, {} FROM ({}) AS `__sprig_cl`",
                sprig_streams::read::image_visible(&f, "$.before"),
                parts.join(" UNION ALL ")
            );
            let c = self.run_checked(cx, &sql, false).await?;
            let mut seen = vec![false; t.images.len()];
            for r in &c.rows {
                let cell = |k: usize| {
                    r.get(k)
                        .cloned()
                        .flatten()
                        .and_then(|b| String::from_utf8(b).ok())
                };
                if let (Some(i), Some(v)) = (cell(0).and_then(|x| x.parse::<usize>().ok()), cell(1))
                    && let Some(slot) = seen.get_mut(i)
                {
                    *slot = v == "1";
                }
            }
            for (i, img) in t.images.iter().enumerate() {
                if img.is_some() {
                    out.push((ti, i, seen[i]));
                }
            }
            metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "images")
                .increment(seen.iter().filter(|v| !**v).count() as u64);
        }
        Ok(out)
    }
}

/// Whether `object` is a table of the storage engine (its rows' images
/// come from the change log, in the engine's JSON forms).
fn engine_table(cat: &sprig_catalog::TenantCatalog, object: sprig_common::ids::ObjectId) -> bool {
    crate::engine::catalog::of(cat).is_some_and(|e| e.tables.contains_key(&object))
}

/// The `JSON_TABLE` columns that read an engine table's change images
/// (`table_changes`' JSON forms), from its `information_schema.COLUMNS`
/// row: `ENUM` and `SET` values are their members' text, so they read as
/// text; a string type's character set is left to the session's (the
/// engine's `JSON_TABLE` takes none); otherwise as on MariaDB.
fn engine_filter_column(
    name: String,
    data_type: &str,
    column_type: &str,
) -> crate::tenant::rls_streams::FilterColumn {
    use crate::tenant::rls_streams::{Carried, FilterColumn, json_table_type};
    let dt = data_type.to_ascii_lowercase();
    let odd = name.contains(['"', '\\', '\'']);
    let (ty, opaque) = match dt.as_str() {
        "enum" | "set" => ("varchar(1024)".to_owned(), false),
        _ => json_table_type(data_type, column_type, None, None),
    };
    let binary = matches!(
        dt.as_str(),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
    );
    FilterColumn {
        plain: (binary && !odd).then(|| column_type.to_ascii_lowercase()),
        carried: if dt == "timestamp" {
            Carried::Utc
        } else {
            Carried::AsIs
        },
        ty,
        opaque: opaque || odd,
        name,
    }
}

impl PassSession {
    /// Which of table `t`'s images (an engine table whose row policies bind
    /// the session) the session's `SELECT` policies show: one statement on
    /// the engine, each image a `JSON_TABLE` of its own literal JSON
    /// (`EXISTS (... WHERE <policies>)`), as the engine's `JSON_TABLE`
    /// doesn't read another table's columns. Null images are the privilege
    /// alone. Fails closed: any failure shows none (logged).
    #[allow(clippy::too_many_arguments)] // one table's check
    async fn engine_images(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &ImagesRequest,
        t: &TableImages,
        cat: &sprig_catalog::TenantCatalog,
        db: &str,
        object: sprig_common::ids::ObjectId,
        lex: sprig_sql::lex::LexOptions,
    ) -> Vec<bool> {
        let mut seen: Vec<bool> = t.images.iter().map(Option::is_none).collect();
        if t.images.iter().all(Option::is_none) {
            return seen;
        }
        match self
            .engine_images_run(cx, req, t, cat, db, object, lex)
            .await
        {
            Ok(v) => {
                for (i, x) in v {
                    if let Some(s) = seen.get_mut(i) {
                        *s = x;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(table = %t.table, error = %e, "row policies over engine change images failed; the images are hidden");
            }
        }
        seen
    }

    #[allow(clippy::too_many_arguments)] // as engine_images
    async fn engine_images_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &ImagesRequest,
        t: &TableImages,
        cat: &sprig_catalog::TenantCatalog,
        db: &str,
        object: sprig_common::ids::ObjectId,
        lex: sprig_sql::lex::LexOptions,
    ) -> Result<Vec<(usize, bool)>> {
        let lit = |v: &str| sprig_sql::rls::sql_string(v, lex.no_backslash_escapes);
        let Some(obj) = cat.objects.get(object) else {
            return Ok(Vec::new());
        };
        let info = self
            .run_checked(
                cx,
                &format!(
                    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                    lit(db),
                    lit(&obj.name)
                ),
                true,
            )
            .await?;
        let text = |b: &Option<Vec<u8>>| {
            b.as_deref()
                .map(|x| String::from_utf8_lossy(x).into_owned())
                .unwrap_or_default()
        };
        let columns: Vec<_> = info
            .rows
            .iter()
            .filter_map(|r| {
                let name = text(r.first()?);
                let (dt, ct) = (text(r.get(1)?), text(r.get(2)?));
                (!name.starts_with("_sprig") && !name.starts_with("__sprig")).then(|| {
                    let col = engine_filter_column(name, &dt, &ct);
                    match (&col.plain, req.plain_binary) {
                        (Some(ty), true) => crate::tenant::rls_streams::FilterColumn {
                            ty: ty.clone(),
                            opaque: false,
                            ..col.clone()
                        },
                        _ => col,
                    }
                })
            })
            .collect();
        if columns.is_empty() {
            return Err(SprigError::internal(format!(
                "table {} has no columns in information_schema",
                obj.name
            )));
        }
        let current = self.current_logical();
        let filter = {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            crate::tenant::rls_streams::record_filter(
                ts,
                cat,
                db,
                object,
                &columns,
                lex,
                current.as_deref(),
            )
            .await?
        };
        let Some(filter) = filter else {
            return Ok((0..t.images.len()).map(|i| (i, true)).collect());
        };
        let parts: Vec<String> = t
            .images
            .iter()
            .enumerate()
            .filter_map(|(i, img)| {
                let m = img.as_ref()?;
                let v = serde_json::Value::Object(m.clone()).to_string();
                Some(format!(
                    "SELECT {i} AS i, EXISTS (SELECT 1 FROM JSON_TABLE({}, '$' COLUMNS ({})) AS {} \
                     WHERE {}) AS v",
                    lit(&v),
                    filter.columns,
                    sprig_streams::read::FILTER_QUAL,
                    filter.predicate
                ))
            })
            .collect();
        let answer = self
            .run_checked(cx, &parts.join(" UNION ALL "), false)
            .await?;
        Ok(answer
            .rows
            .iter()
            .filter_map(|r| {
                let i = text(r.first()?).parse::<usize>().ok()?;
                Some((i, text(r.get(1)?) == "1"))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_and_answers() {
        let req = ImagesRequest {
            tables: vec![TableImages {
                db: "shop".into(),
                table: "docs".into(),
                images: vec![None, Some(serde_json::Map::new())],
            }],
            plain_binary: false,
            watch: true,
        };
        let s = statement(&req);
        assert!(s.starts_with(MARK));
        assert!(s.contains("\"watch\":true"));
        assert!(s.contains("\"table\":\"docs\""));
        let rows = vec![
            vec![Some("0".into()), Some("1".into()), Some("1".into())],
            vec![Some("0".into()), Some("0".into()), Some("0".into())],
        ];
        assert_eq!(answer(&req, &rows), vec![vec![false, true]]);
    }
}
