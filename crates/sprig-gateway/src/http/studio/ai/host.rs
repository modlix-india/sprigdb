//! The assistants' read tools, run as the signed-in caller.
//!
//! [`GatewayHost`] is built for one request from its [`Caller`] and has no
//! rights of its own:
//!
//! - **SQL** goes through the same [`SqlEngine`](crate::http::sql::SqlEngine)
//!   path as `Sql.Execute`, so the caller's grants, row-level security and
//!   name virtualization apply. SQL the model wrote (`run_query`, `explain`
//!   of a read, `column_stats`) runs inside `START TRANSACTION READ ONLY`,
//!   which is rolled back; `run_query` is capped at its `limit` and at
//!   `studio.statement_timeout_ms`. `sprig_ai`'s checks have already
//!   refused anything that isn't one read.
//! - **Routes** that Studio already serves (workload, shards, the
//!   approvals inbox, logic compile, the policy tester, automatic indexes)
//!   are called with the same caller, so their own checks apply.
//! - Results come back unreduced; the egress gate reduces them.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, Response};
use prost_types::value::Kind;
use serde_json::{Map, Value, json};
use sprig_ai::ToolHost;
use sprig_ai::tools::{
    Column, ColumnStats, ProposalCheck, ResultSet, Source, ToolBody, ToolOutput, ToolSpec,
};
use sprig_api::proto::v1::ExecuteResponse;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::mpsc;
use tonic::body::Body;

use super::super::super::Core;
use super::super::super::sql::{CHANNEL_DEPTH, Caller, Caps, SqlCall, collect};
use super::super::{quote_literal, run_sql};

mod skills;

/// Rows `column_stats` reads per table at most.
pub const STATS_SAMPLE_ROWS: u64 = 100_000;
/// Columns whose common values `column_stats` lists at most.
const TOP_COLUMNS: usize = 10;
/// Docs hits `search_docs` returns.
const DOCS_HITS: usize = 5;
/// Characters of a docs hit's text kept.
const DOCS_TEXT: usize = 1500;
/// Objects `catalog` lists at most.
const MAX_OBJECTS: usize = 500;
/// Rows `run_query` returns when the model doesn't say.
const DEFAULT_LIMIT: u64 = 100;

/// Runs read tools as one caller.
pub struct GatewayHost {
    core: Arc<Core>,
    caller: Caller,
    share_data: bool,
    /// The assistant the tools run for: the administration assistant's
    /// `decision_log` and `metrics_trend` read the DC's, the DBA's its
    /// tenant's.
    role: Option<sprig_ai::Role>,
}

impl std::fmt::Debug for GatewayHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayHost")
            .field("user", &self.caller.principal.user)
            .finish_non_exhaustive()
    }
}

fn tool_error(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn clip(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_owned()
    } else {
        let mut c: String = s.chars().take(n).collect();
        c.push('…');
        c
    }
}

/// A protobuf cell as JSON.
fn cell(v: &prost_types::Value) -> Value {
    match &v.kind {
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::NumberValue(n)) => json!(n),
        Some(Kind::BoolValue(b)) => json!(b),
        _ => Value::Null,
    }
}

/// The first result with columns, as a [`ResultSet`].
#[must_use]
pub fn result_set(r: &ExecuteResponse) -> ResultSet {
    let Some(x) = r.results.iter().find(|x| !x.columns.is_empty()) else {
        return ResultSet::default();
    };
    let rows: Vec<Vec<Value>> = x
        .rows
        .iter()
        .map(|row| row.values.iter().map(cell).collect())
        .collect();
    ResultSet {
        columns: x
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                ty: c.r#type.clone(),
            })
            .collect(),
        row_count: rows.len() as u64,
        truncated: x.truncated,
        rows,
    }
}

/// The first result as objects by column name.
fn objects(r: &ExecuteResponse) -> Vec<Map<String, Value>> {
    let rs = result_set(r);
    rs.rows
        .into_iter()
        .map(|row| rs.columns.iter().map(|c| c.name.clone()).zip(row).collect())
        .collect()
}

/// A route's JSON reply.
async fn reply_json(r: Response<Body>) -> Result<Value> {
    let status = r.status().as_u16();
    let bytes = r
        .into_body()
        .collect()
        .await
        .map_err(|e| SprigError::internal(format!("reading a route's reply: {e}")))?
        .to_bytes();
    body_json(status, &bytes)
}

fn body_json(status: u16, bytes: &Bytes) -> Result<Value> {
    let v: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    if status >= 400 {
        let msg = v["message"]
            .as_str()
            .or_else(|| v["error"]["message"].as_str())
            .unwrap_or("the call failed");
        return Err(tool_error(msg.to_owned()));
    }
    Ok(v)
}

fn source(kind: &str, title: impl Into<String>, reference: impl Into<String>) -> Source {
    Source {
        kind: kind.to_owned(),
        title: title.into(),
        reference: reference.into(),
    }
}

/// Characters an admin-API path segment may hold.
fn safe(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

impl GatewayHost {
    /// A host running as `caller`; `share_data` lets value statistics be
    /// computed at all (without it the gate would drop them anyway).
    #[must_use]
    pub fn new(core: Arc<Core>, caller: Caller, share_data: bool) -> Self {
        Self {
            core,
            caller,
            share_data,
            role: None,
        }
    }

    /// The same host for `role`'s turns.
    #[must_use]
    pub fn for_role(mut self, role: sprig_ai::Role) -> Self {
        self.role = Some(role);
        self
    }

    fn admin(&self) -> bool {
        self.role == Some(sprig_ai::Role::Admin)
    }

    async fn sql(&self, db: &str, sql: String) -> Result<ExecuteResponse> {
        run_sql(&self.core, &self.caller, db, sql).await
    }

    /// `sql` in a read-only transaction that is rolled back.
    async fn read_only(&self, db: &str, sql: String, caps: Caps) -> Result<ExecuteResponse> {
        let engine = &self.core.engine;
        let t = engine.begin(&self.caller, db, true).await?;
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        let e2 = Arc::clone(engine);
        let c2 = self.caller.clone();
        let name = t.name.clone();
        let call = SqlCall {
            database: db.to_owned(),
            sql,
            multi: false,
        };
        tokio::spawn(async move { e2.run_capped(&c2, call, Some(&name), caps, tx).await });
        let r = collect(
            rx,
            engine.settings().max_buffered,
            engine.charge(&self.caller),
        )
        .await;
        if let Err(e) = engine.end(&self.caller, &t.name, false).await {
            tracing::debug!(error = %e, "an assistant's read-only transaction didn't roll back");
        }
        r
    }

    fn timeout(&self, db: &str) -> Duration {
        Duration::from_millis(self.core.studio.tenant_get(
            &sprig_studio::settings::STATEMENT_TIMEOUT_MS,
            self.caller.principal.tenant,
            Some(db),
        ))
    }

    fn tenant_name(&self) -> Result<String> {
        self.caller
            .principal
            .tenant_name
            .clone()
            .ok_or_else(|| tool_error("this tool needs a tenant login"))
    }

    async fn search_docs(&self, input: &Value) -> Result<ToolOutput> {
        let svc = crate::docs::service()
            .ok_or_else(|| crate::docs::unavailable("this gateway has no tenant logins"))?;
        let audience = Some(s(input, "audience").to_owned()).filter(|a| !a.is_empty());
        let reply = svc
            .search(
                self.caller.principal.tenant,
                &crate::docs::search::SearchRequest {
                    query: s(input, "query").to_owned(),
                    audience,
                    limit: Some(DOCS_HITS),
                    version: None,
                },
            )
            .await?;
        let mut sources = Vec::new();
        let hits: Vec<Value> = reply
            .hits
            .iter()
            .map(|h| {
                let title = if h.heading_path.is_empty() {
                    h.title.clone()
                } else {
                    format!("{} › {}", h.title, h.heading_path)
                };
                sources.push(source("docs", title.clone(), h.page.clone()));
                json!({"page": h.page, "title": title, "text": clip(&h.text, DOCS_TEXT)})
            })
            .collect();
        Ok(ToolOutput {
            body: ToolBody::Json(json!({"version": reply.version, "hits": hits})),
            sources,
        })
    }

    async fn catalog(&self, input: &Value) -> Result<ToolOutput> {
        let db = s(input, "db");
        if db.is_empty() {
            let r = self
                .sql(
                    "-",
                    "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA \
                     WHERE SCHEMA_NAME NOT IN ('information_schema','mysql','performance_schema','sys') \
                     ORDER BY SCHEMA_NAME"
                        .to_owned(),
                )
                .await?;
            let names: Vec<Value> = objects(&r)
                .into_iter()
                .filter_map(|m| m.get("name").cloned())
                .collect();
            return Ok(ToolOutput::json(json!({"databases": names})));
        }
        let r = self
            .sql(
                "-",
                format!(
                    "SELECT TABLE_NAME AS name, TABLE_TYPE AS type, TABLE_ROWS AS approx_rows, \
                     TABLE_COMMENT AS comment FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME LIMIT {}",
                    quote_literal(db),
                    MAX_OBJECTS + 1
                ),
            )
            .await?;
        let mut objs: Vec<Value> = objects(&r)
            .into_iter()
            .map(|m| {
                let kind = if m
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|t| t.to_ascii_uppercase().contains("VIEW"))
                {
                    "view"
                } else {
                    "table"
                };
                json!({
                    "name": m.get("name"), "kind": kind,
                    "approx_rows": m.get("approx_rows"), "comment": m.get("comment"),
                })
            })
            .collect();
        let more = objs.len() > MAX_OBJECTS;
        objs.truncate(MAX_OBJECTS);
        Ok(ToolOutput {
            body: ToolBody::Json(json!({"db": db, "objects": objs, "more": more})),
            sources: vec![source("database", db, db)],
        })
    }

    async fn describe(&self, input: &Value) -> Result<ToolOutput> {
        let (db, table) = (s(input, "db"), s(input, "table"));
        let r = self
            .sql(
                "-",
                format!(
                    "SELECT COLUMN_NAME AS name, COLUMN_TYPE AS type, IS_NULLABLE AS nullable, \
                     COLUMN_KEY AS `key`, COLUMN_DEFAULT AS `default`, EXTRA AS extra, \
                     COLUMN_COMMENT AS comment FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                    quote_literal(db),
                    quote_literal(table)
                ),
            )
            .await?;
        let columns: Vec<Value> = objects(&r).into_iter().map(Value::Object).collect();
        if columns.is_empty() {
            return Err(tool_error(format!(
                "there is no table {db}.{table} that the user can see"
            )));
        }
        let q = format!("{}.{}", ident(db), ident(table));
        let indexes = self
            .sql("-", format!("SHOW INDEX FROM {q}"))
            .await
            .map(|r| index_list(&objects(&r)))
            .unwrap_or_default();
        let definition = match self.sql("-", format!("SHOW CREATE TABLE {q}")).await {
            Ok(r) => result_set(&r)
                .rows
                .first()
                .and_then(|row| row.get(1))
                .and_then(Value::as_str)
                .map_or(Value::Null, |d| json!(clip(d, 8000))),
            Err(_) => Value::Null,
        };
        Ok(ToolOutput {
            body: ToolBody::Json(json!({
                "db": db, "table": table, "columns": columns,
                "indexes": indexes, "definition": definition,
            })),
            sources: vec![source(
                "table",
                format!("{db}.{table}"),
                format!("{db}.{table}"),
            )],
        })
    }

    async fn explain(&self, input: &Value) -> Result<ToolOutput> {
        let (db, sql) = (s(input, "db"), s(input, "sql"));
        let text = format!("EXPLAIN FORMAT=JSON {sql}");
        // A read is planned inside a read-only transaction; INSERT, UPDATE
        // and DELETE are only planned (EXPLAIN never runs them).
        let r = if sprig_ai::sql_guard::read_only(sql).is_ok() {
            let caps = Caps {
                timeout: Some(self.timeout(db)),
                ..Caps::default()
            };
            self.read_only(db, text, caps).await?
        } else {
            self.sql(db, text).await?
        };
        let plan = result_set(&r)
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_str)
            .map_or(Value::Null, |p| {
                serde_json::from_str::<Value>(p).unwrap_or_else(|_| json!(p))
            });
        Ok(ToolOutput {
            body: ToolBody::Json(json!({"db": db, "plan": plan})),
            sources: vec![source("plan", "EXPLAIN FORMAT=JSON", db)],
        })
    }

    async fn indexes(&self, input: &Value) -> Result<ToolOutput> {
        let (db, table) = (s(input, "db"), s(input, "table"));
        let r = self
            .sql(
                "-",
                format!("SHOW INDEX FROM {}.{}", ident(db), ident(table)),
            )
            .await?;
        Ok(ToolOutput {
            body: ToolBody::Json(
                json!({"db": db, "table": table, "indexes": index_list(&objects(&r))}),
            ),
            sources: vec![source(
                "table",
                format!("{db}.{table}"),
                format!("{db}.{table}"),
            )],
        })
    }

    async fn forward(&self, path: &str) -> Result<Value> {
        let r = self
            .core
            .admin
            .forward_internal(&Method::GET, path, Bytes::new())
            .await?;
        body_json(r.status, &r.body)
    }

    async fn settings(&self, input: &Value) -> Result<ToolOutput> {
        let key = s(input, "key");
        let mut sql = "SHOW SPRIG SETTINGS".to_owned();
        if !key.is_empty() {
            let pat = if key.ends_with('.') || !key.contains('.') {
                format!("{}%", key.replace('%', "\\%").replace('_', "\\_"))
            } else {
                key.to_owned()
            };
            let _ = write!(sql, " LIKE {}", quote_literal(&pat));
        }
        // As the values resolve for a table or a database: where each
        // comes from (its SOURCE_SCOPE) is what a DBA needs before
        // proposing a change at the narrowest scope.
        match (s(input, "db"), s(input, "table")) {
            ("", "") => {}
            ("", _) => return Err(tool_error("table needs db")),
            (db, "") => {
                let _ = write!(sql, " FOR DATABASE {}", ident(db));
            }
            (db, table) => {
                let _ = write!(sql, " FOR TABLE {}.{}", ident(db), ident(table));
            }
        }
        let r = self.sql("-", sql).await?;
        let mut rows = objects(&r);
        // `SHOW SPRIG SETTINGS` lists the keys a tenant may set; the
        // process-wide ones (`backend.*`, `gateway.*`, ...) come from this
        // gateway's live settings, without operator-only keys.
        if !key.is_empty()
            && let super::super::StudioSource::Live { live, dc } = &self.core.studio.source
            && let Ok(reg) = sprig_settings::Registry::global()
        {
            let pat = if key.ends_with('.') || !key.contains('.') {
                format!("{key}%")
            } else {
                key.to_owned()
            };
            let snap = live.snapshot();
            let ctx = sprig_settings::SettingCtx::process(dc);
            let mut process: Vec<_> = reg
                .iter()
                .filter(|d| {
                    !d.operator_only()
                        && !d.scopes().contains(sprig_settings::ScopeKind::Tenant)
                        && sprig_sql::settings_stmt::like_matches(&pat, d.key())
                })
                .collect();
            process.sort_by_key(|d| d.key());
            for d in process {
                if let Ok(res) = snap.resolve(d.key(), &ctx) {
                    let mut m = Map::new();
                    m.insert("SETTING_SCOPE".into(), json!("process"));
                    m.insert("SETTING_NAME".into(), json!(d.key()));
                    m.insert(
                        "VALUE".into(),
                        json!(sprig_settings::show_value(&res.value)),
                    );
                    m.insert("SOURCE_SCOPE".into(), json!(res.source.to_string()));
                    m.insert("DOC".into(), json!(d.doc()));
                    rows.push(m);
                }
            }
        }
        let more = rows.len() > 200;
        rows.truncate(200);
        Ok(ToolOutput {
            body: ToolBody::Json(json!({
                "settings": rows.into_iter().map(Value::Object).collect::<Vec<_>>(),
                "more": more,
            })),
            sources: vec![source(
                "setting",
                if key.is_empty() { "settings" } else { key },
                key,
            )],
        })
    }

    #[allow(clippy::too_many_lines)] // the counts, then each column's values
    async fn column_stats(&self, input: &Value) -> Result<ToolOutput> {
        let (db, table) = (s(input, "db"), s(input, "table"));
        let wanted: Vec<&str> = input["cols"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let r = self
            .sql(
                "-",
                format!(
                    "SELECT COLUMN_NAME AS name, DATA_TYPE AS type FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                    quote_literal(db),
                    quote_literal(table)
                ),
            )
            .await?;
        let known: Vec<(String, String)> = objects(&r)
            .into_iter()
            .map(|m| {
                (
                    m.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    m.get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                )
            })
            .collect();
        let mut cols = Vec::new();
        for w in &wanted {
            match known.iter().find(|(n, _)| n.eq_ignore_ascii_case(w)) {
                Some(c) => cols.push(c.clone()),
                None => {
                    return Err(tool_error(format!(
                        "{db}.{table} has no column {w} that the user can see"
                    )));
                }
            }
        }
        let from = format!(
            "(SELECT {} FROM {}.{} LIMIT {STATS_SAMPLE_ROWS}) s",
            cols.iter()
                .map(|(n, _)| ident(n))
                .collect::<Vec<_>>()
                .join(", "),
            ident(db),
            ident(table)
        );
        let mut parts = vec!["COUNT(*)".to_owned()];
        for (n, _) in &cols {
            let c = ident(n);
            parts.push(format!("SUM({c} IS NULL)"));
            parts.push(format!("COUNT(DISTINCT {c})"));
            if self.share_data {
                parts.push(format!("MIN({c})"));
                parts.push(format!("MAX({c})"));
            }
        }
        let caps = Caps {
            timeout: Some(self.timeout(db)),
            ..Caps::default()
        };
        let r = self
            .read_only(db, format!("SELECT {} FROM {from}", parts.join(", ")), caps)
            .await?;
        let row = result_set(&r).rows.into_iter().next().unwrap_or_default();
        let num = |v: Option<&Value>| -> u64 {
            match v {
                Some(Value::Number(n)) => n.as_u64().unwrap_or_else(|| whole(n.as_f64())),
                Some(Value::String(s)) => s.parse().unwrap_or(0),
                _ => 0,
            }
        };
        let count = num(row.first());
        let per = if self.share_data { 4 } else { 2 };
        let mut stats = Vec::new();
        for (i, (n, ty)) in cols.iter().enumerate() {
            let at = 1 + i * per;
            let mut st = ColumnStats {
                column: n.clone(),
                ty: ty.clone(),
                count,
                nulls: num(row.get(at)),
                distinct: Some(num(row.get(at + 1))),
                ..ColumnStats::default()
            };
            if self.share_data {
                st.min = row.get(at + 2).cloned();
                st.max = row.get(at + 3).cloned();
                if i < TOP_COLUMNS {
                    let c = ident(n);
                    if let Ok(t) = self
                        .read_only(
                            db,
                            format!(
                                "SELECT {c}, COUNT(*) FROM (SELECT {c} FROM {}.{} LIMIT {STATS_SAMPLE_ROWS}) s \
                                 GROUP BY {c} ORDER BY COUNT(*) DESC LIMIT 5",
                                ident(db),
                                ident(table)
                            ),
                            caps,
                        )
                        .await
                    {
                        st.top = result_set(&t)
                            .rows
                            .into_iter()
                            .map(|r| {
                                (
                                    r.first().cloned().unwrap_or(Value::Null),
                                    num(r.get(1)),
                                )
                            })
                            .collect();
                    }
                }
            }
            stats.push(st);
        }
        Ok(ToolOutput {
            body: ToolBody::Stats(stats),
            sources: vec![source(
                "table",
                format!("{db}.{table}"),
                format!("{db}.{table}"),
            )],
        })
    }

    async fn run_query(&self, input: &Value) -> Result<ToolOutput> {
        let (db, sql) = (s(input, "db"), s(input, "sql"));
        let limit = input["limit"]
            .as_u64()
            .unwrap_or(DEFAULT_LIMIT)
            .clamp(1, 1000);
        let caps = Caps {
            max_rows: limit,
            max_bytes: 0,
            timeout: Some(self.timeout(db)),
        };
        let r = self.read_only(db, sql.to_owned(), caps).await?;
        Ok(ToolOutput {
            body: ToolBody::Rows(result_set(&r)),
            sources: vec![source("query", clip(sql, 120), db)],
        })
    }

    async fn test_policy(&self, input: &Value) -> Result<ToolOutput> {
        let b = json!({
            "user": s(input, "as_user"),
            "database": s(input, "db"),
            "sql": s(input, "sql"),
        });
        let v = super::super::policies::run_as(&self.core, &self.caller, &b).await?;
        // Counts and the policies applied; never the rows.
        let rows = v["rows"].as_array().map_or(0, Vec::len);
        Ok(ToolOutput {
            body: ToolBody::Json(json!({
                "as": v["as"], "columns": v["columns"], "row_count": rows,
                "truncated": v["truncated"], "policies": v["policies"],
            })),
            sources: vec![source(
                "policy",
                "row-level security tester",
                s(input, "db"),
            )],
        })
    }

    /// `decision_log`: the DC's log for the administration assistant, the
    /// tenant's own for the DBA.
    async fn decision_log(&self, input: &Value) -> Result<ToolOutput> {
        let subject = s(input, "subject").trim();
        let days = input["days"].as_u64().unwrap_or(30).clamp(1, 90);
        let path = if self.admin() {
            format!(
                "/admin/v1/ai/decisions?subject={}&days={days}",
                super::pct(subject)
            )
        } else {
            let t = self.tenant_name()?;
            if subject.parse::<u64>().is_ok() {
                format!("/admin/v1/tenants/{t}/decisions/{subject}")
            } else {
                format!(
                    "/admin/v1/tenants/{t}/decisions?subject={}&days={days}",
                    super::pct(subject)
                )
            }
        };
        let v = self.forward(&path).await?;
        let mut sources: Vec<Source> = v["entries"]
            .as_array()
            .or_else(|| v["decisions"].as_array())
            .into_iter()
            .flatten()
            .take(10)
            .map(|e| {
                let id = e["id"]
                    .as_str()
                    .map_or_else(|| e["decision_id"].to_string(), str::to_owned);
                source("decision", e["kind"].as_str().unwrap_or("decision"), id)
            })
            .collect();
        if v.get("decision_id").is_some() {
            sources.push(source("decision", subject, subject));
        }
        Ok(ToolOutput {
            body: ToolBody::Json(v),
            sources,
        })
    }

    /// `metrics_trend`: the controller's metric history.
    async fn metrics_trend(&self, input: &Value) -> Result<ToolOutput> {
        let mut q = format!(
            "name={}&days={}",
            super::pct(s(input, "name")),
            input["days"].as_u64().unwrap_or(7).clamp(1, 30)
        );
        for l in input["labels"].as_array().into_iter().flatten() {
            if let Some(l) = l.as_str() {
                let _ = write!(q, "&label={}", super::pct(l));
            }
        }
        Ok(ToolOutput {
            body: ToolBody::Json(self.forward(&format!("/admin/v1/ai/metrics?{q}")).await?),
            sources: vec![source("metric", s(input, "name"), "metrics_trend")],
        })
    }

    async fn compile_logic(&self, input: &Value) -> Result<ToolOutput> {
        let db = s(input, "db");
        let r = super::super::logic::compile(
            &self.core,
            &self.caller,
            db,
            &json!({"sql": draft_declaration(s(input, "source"))}),
        )
        .await?;
        Ok(ToolOutput::json(reply_json(r).await?))
    }
}

/// `compile_logic`'s source as a declaration: a KIRun definition alone
/// is compiled as a function named after it (or `ai_draft`).
fn draft_declaration(source: &str) -> String {
    let t = source.trim();
    if !t.starts_with('{') {
        return source.to_owned();
    }
    let name = serde_json::from_str::<Value>(t)
        .ok()
        .and_then(|v| v["name"].as_str().map(str::to_owned))
        .filter(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or_else(|| "ai_draft".to_owned());
    format!(
        "CREATE FUNCTION {name} LANGUAGE kirun AS {}",
        quote_literal(t)
    )
}

/// A count that came back as a float.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)] // clamped first
fn whole(f: Option<f64>) -> u64 {
    match f {
        Some(x) if x.is_finite() && x > 0.0 => x.min(u64::MAX as f64) as u64,
        _ => 0,
    }
}

/// `SHOW INDEX` rows grouped by index.
fn index_list(rows: &[Map<String, Value>]) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = Vec::new();
    for r in rows {
        let name = r
            .get("Key_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let col = r.get("Column_name").cloned().unwrap_or(Value::Null);
        if let Some((_, v)) = out.iter_mut().find(|(n, _)| *n == name) {
            if let Some(a) = v["columns"].as_array_mut() {
                a.push(col);
            }
            continue;
        }
        let unique = r
            .get("Non_unique")
            .is_some_and(|v| v.as_str() == Some("0") || v.as_f64().is_some_and(|f| f.abs() < 0.5));
        out.push((
            name.clone(),
            json!({
                "name": name, "columns": [col], "unique": unique,
                "automatic": name.starts_with("sprig_auto_"),
            }),
        ));
    }
    out.into_iter().map(|(_, v)| v).collect()
}

#[async_trait]
impl ToolHost for GatewayHost {
    async fn check_proposal(&self, spec: &ToolSpec, input: &Value) -> Result<ProposalCheck> {
        match spec.name {
            "propose_sql" => {
                check_proposed_sql(input)?;
                Ok(ProposalCheck::default())
            }
            "propose_policy" | "propose_logic" | "generate_rows" => {
                self.check_skill(spec.name, input).await
            }
            _ => Ok(ProposalCheck::default()),
        }
    }

    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        match spec.name {
            "search_docs" => self.search_docs(input).await,
            "catalog" => self.catalog(input).await,
            "describe" => self.describe(input).await,
            "workload" => {
                let q = format!("hours={}&limit=20", input["hours"].as_u64().unwrap_or(24));
                let r = super::super::workload::list(&self.core, &self.caller, &q).await?;
                Ok(ToolOutput::json(reply_json(r).await?))
            }
            "fingerprint" => {
                let id = s(input, "id");
                if !safe(id) {
                    return Err(tool_error("fingerprint ids are as workload returns them"));
                }
                let r =
                    super::super::workload::series(&self.core, &self.caller, id, "days=7").await?;
                Ok(ToolOutput::json(
                    json!({"id": id, "series": reply_json(r).await?}),
                ))
            }
            "explain" => self.explain(input).await,
            "indexes" => self.indexes(input).await,
            "auto_indexes" => {
                let t = self.tenant_name()?;
                let db = s(input, "db");
                let q = if db.is_empty() {
                    String::new()
                } else {
                    format!("?db={}", super::pct(db))
                };
                Ok(ToolOutput::json(
                    self.forward(&format!("/admin/v1/tenants/{t}/indexes{q}"))
                        .await?,
                ))
            }
            "settings" => self.settings(input).await,
            "shards" | "advisor_runs" => {
                let path = if spec.name == "shards" {
                    "/v1/shards"
                } else {
                    "/v1/shards/advisor"
                };
                let r = super::super::shards::route(&self.core, &self.caller, path).await?;
                Ok(ToolOutput::json(reply_json(r).await?))
            }
            "approvals" => {
                let mut q = Vec::new();
                match s(input, "state") {
                    "" | "open" => q.push("state=open".to_owned()),
                    "decided" => q.push("state=decided".to_owned()),
                    _ => {}
                }
                if !s(input, "kind").is_empty() {
                    q.push(format!("kind={}", super::pct(s(input, "kind"))));
                }
                q.push("limit=50".to_owned());
                let r = super::super::recommendations::proxy(
                    &self.core,
                    &self.caller,
                    &Method::GET,
                    "",
                    &q.join("&"),
                    b"",
                )
                .await?;
                Ok(ToolOutput {
                    body: ToolBody::Json(body_json(r.status, &r.body)?),
                    sources: vec![source("approval", "approvals inbox", "/v1/recommendations")],
                })
            }
            "column_stats" => self.column_stats(input).await,
            "run_query" => self.run_query(input).await,
            "test_policy" if !s(input, "policy").trim().is_empty() => self.try_policy(input).await,
            "test_policy" => self.test_policy(input).await,
            "sensitive_columns" => self.sensitive_columns(input).await,
            "compile_logic" => self.compile_logic(input).await,
            "decision_log" => self.decision_log(input).await,
            "check_ddl" => Ok(ToolOutput {
                body: ToolBody::Json(super::schema_check::check_ddl_json(
                    s(input, "sql"),
                    s(input, "db"),
                )),
                sources: vec![source("rules", "SprigDB's schema rules", "check_ddl")],
            }),
            "migration_check" => migration_check(input),
            "metrics_trend" if self.admin() => self.metrics_trend(input).await,
            "metrics_trend" => Err(tool_error(
                "the metric history is for the administration assistant",
            )),
            other => Err(tool_error(format!("{other} isn't a read tool"))),
        }
    }
}

/// `migration_check`: a deterministic report, run on the text the model
/// passed; nothing is read from the tenant.
fn migration_check(input: &Value) -> Result<ToolOutput> {
    use super::schema_check as sc;
    let db = Some(s(input, "db"))
        .filter(|d| !d.trim().is_empty())
        .unwrap_or("app");
    let report = match s(input, "source") {
        "mongodb" => {
            let colls: Value = serde_json::from_str(s(input, "schema"))
                .map_err(|e| tool_error(format!("schema isn't JSON: {e}")))?;
            sc::mongo_report(&colls, db)
        }
        source @ ("mysql" | "mariadb") => {
            let ddl = s(input, "schema");
            if ddl.len() > sc::MAX_TEXT {
                return Err(tool_error(format!(
                    "schema is longer than {} bytes; check it in parts",
                    sc::MAX_TEXT
                )));
            }
            sc::mysql_report(ddl, db, source)
        }
        other => return Err(tool_error(format!("unknown source {other}"))),
    };
    Ok(ToolOutput {
        body: ToolBody::Json(report),
        sources: vec![source(
            "rules",
            "SprigDB's migration rules",
            "migration_check",
        )],
    })
}

/// Statement kinds whose text `check_ddl`'s rules apply to.
fn is_schema_change(sql: &str) -> bool {
    let w: Vec<String> = sql
        .split_whitespace()
        .take(2)
        .map(str::to_ascii_uppercase)
        .collect();
    matches!(
        w.first().map(String::as_str),
        Some("CREATE" | "ALTER" | "RENAME")
    ) && !matches!(
        w.get(1).map(String::as_str),
        Some("USER" | "ROLE" | "TENANT" | "SPRIG")
    )
}

/// The check a proposed statement gets before the user sees it: a schema
/// change must pass `check_ddl`'s rules, so a proposed `CREATE TABLE`
/// always parses on SprigDB.
///
/// # Errors
/// What the rules found, for the model to fix.
pub fn check_proposed_sql(input: &Value) -> Result<()> {
    let sql = s(input, "sql");
    if !is_schema_change(sql) {
        return Ok(());
    }
    let r = super::schema_check::check_sql(
        sql,
        s(input, "db"),
        sprig_proto_mysql::packets::Persona::MySql84,
        true,
    );
    if !r.has_errors() {
        return Ok(());
    }
    let errs: Vec<String> = r
        .findings
        .iter()
        .filter(|f| f.level == super::schema_check::Level::Error)
        .map(|f| format!("{} ({}): {}", f.at, f.rule, f.message))
        .collect();
    Err(tool_error(format!(
        "not proposed: the statement fails SprigDB's schema rules. Fix it and propose again. {}",
        errs.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposed_schema_changes_are_checked() {
        let ok = json!({"db": "shop", "sql": "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT)"});
        assert!(check_proposed_sql(&ok).is_ok());
        let bad =
            json!({"db": "shop", "sql": "CREATE TABLE t (id INT PRIMARY KEY) ENGINE=BLACKHOLE"});
        let e = check_proposed_sql(&bad).unwrap_err();
        assert!(e.message.contains("BLACKHOLE"), "{}", e.message);
        let broken = json!({"db": "shop", "sql": "CREATE TABLE t (id INT PRIMARY KEY,, a)"});
        assert!(check_proposed_sql(&broken).is_err());
        // Data changes and other statements aren't held to the DDL rules.
        let dml = json!({"db": "shop", "sql": "DELETE FROM t WHERE id = 1"});
        assert!(check_proposed_sql(&dml).is_ok());
        let pin = json!({"db": "shop", "sql": "SPRIG PIN t SHARD KEY (id)"});
        assert!(check_proposed_sql(&pin).is_ok());
    }

    #[test]
    fn index_rows_group_by_name() {
        let row = |k: &str, c: &str, nu: &str| -> Map<String, Value> {
            serde_json::from_value(json!({"Key_name": k, "Column_name": c, "Non_unique": nu}))
                .unwrap()
        };
        let v = index_list(&[
            row("PRIMARY", "id", "0"),
            row("sprig_auto_1a2b3c4d", "a", "1"),
            row("sprig_auto_1a2b3c4d", "b", "1"),
        ]);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0]["unique"], true);
        assert_eq!(v[1]["columns"], json!(["a", "b"]));
        assert_eq!(v[1]["automatic"], true);
        assert!(safe("fp_1a2b") && !safe("a/b") && !safe(""));
        assert_eq!(clip("abcdef", 3), "abc…");
    }
}
