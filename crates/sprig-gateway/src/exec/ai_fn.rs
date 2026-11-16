//! The AI functions step of a tenant statement: `AI_EMBED`,
//! `AI_CLASSIFY`, `AI_EXTRACT`, `AI_SUMMARIZE` and `AI_COMPLETE`, answered
//! in the gateway (`sprig_sql::ai_fn` finds them, `sprig_ai::functions`
//! answers them).
//!
//! 1. Calls with a literal input are answered first and replaced by their
//!    values, so they may stand anywhere (`ORDER BY VEC_DISTANCE(v,
//!    AI_EMBED('red shoes'))`, `INSERT ... VALUES (AI_EMBED('...'))`).
//! 2. Calls over row values are whole items of the outermost `SELECT`
//!    list: the statement runs through this session with each call's input
//!    expression in its place (names, privileges, row-level security and
//!    the transaction apply as to any statement), the rows are read, the
//!    inputs of each column answered at once, and the rows sent with the
//!    answers in that column.
//!
//! The calls' inputs are the caller's own statement's values, so they are
//! only ever values the caller can read. Settings are the tenant's: the
//! functions need `ai.functions.enabled`, and a provider model needs
//! `ai.share_data` on every database the statement names (and on the
//! session's database; the tenant's value when it names none). The
//! MongoDB engine's statements name their database in a leading
//! `/* sprig-ai-db:<name> */` comment, honoured only for the gateway's own
//! internal statements.
//!
//! Not in prepared statements (39051): run them as plain statements.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_ai::UsageToday;
use sprig_ai::audit::{AuditStore as _, CatalogAudit};
use sprig_ai::functions::{
    self, AiFn, CatalogFnCache, FnCache, FnEnv, FnSettings, LOCAL_MODEL, Output,
};
use sprig_ai::provider::{Provider, Usage};
use sprig_ai::settings::Lookup;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type as ct};
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};
use sprig_settings::{Setting, SettingCtx};
use sprig_sql::ai_fn::{self, Plan};
use tokio::sync::OnceCell;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues};
use crate::tenant::TenantServices;
use crate::tenant::session::LocalWarning;

/// Rows a statement with AI function columns may return at most (they are
/// held in the gateway while their inputs are answered).
const MAX_ROWS: usize = 100_000;

/// The longest text a column announces (`MEDIUMTEXT`'s).
const TEXT_LENGTH: u32 = 16_777_215;

/// The binary collation.
const BINARY: u16 = 63;

/// The tenant's values, at a database's scope when one is given.
struct ExecLookup<'a> {
    svc: &'a TenantServices,
    tenant: TenantId,
    db: Option<&'a str>,
}

impl ExecLookup<'_> {
    fn get<T: sprig_settings::SettingValue>(&self, s: &'static Setting<T>) -> T {
        if let Some(cat) = self.svc.cache.cached(self.tenant) {
            let mut ctx = cat.setting_ctx(&self.svc.dc);
            if let Some(d) = self
                .db
                .filter(|d| !d.is_empty())
                .and_then(|d| cat.database(&d.to_lowercase()).or_else(|| cat.database(d)))
            {
                ctx = ctx.with_db(d.db_id);
            }
            return self.svc.settings.get(s, &ctx, Some(&cat.settings));
        }
        let ctx = SettingCtx::process(&self.svc.dc).for_tenant(self.tenant, &[]);
        self.svc.settings.get(s, &ctx, None)
    }
}

impl Lookup for ExecLookup<'_> {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        self.get(s)
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        self.get(s)
    }
    fn text(&self, s: &'static Setting<std::borrow::Cow<'static, str>>) -> String {
        self.get(s).to_string()
    }
}

/// The gateway's surroundings for [`functions::evaluate`].
struct GatewayEnv {
    svc: Arc<TenantServices>,
    tenant: TenantId,
    user: UserId,
    /// The tenant's catalog writer and schema, found once.
    catalog: OnceCell<(mysql_async::Pool, String)>,
}

impl GatewayEnv {
    async fn catalog(&self) -> Result<&(mysql_async::Pool, String)> {
        self.catalog
            .get_or_try_init(|| async {
                let ep =
                    sprig_catalog::CatalogLocator::writer(&*self.svc.locator, self.tenant).await?;
                Ok::<_, SprigError>((ep.pool.clone(), ep.store.schema().to_owned()))
            })
            .await
    }

    async fn audit(&self) -> Result<CatalogAudit> {
        let (pool, schema) = self.catalog().await?;
        Ok(CatalogAudit::new(pool.clone(), schema.clone()))
    }
}

#[async_trait]
impl FnEnv for GatewayEnv {
    fn tenant(&self) -> Option<TenantId> {
        Some(self.tenant)
    }

    fn user(&self) -> UserId {
        self.user
    }

    async fn provider(&self, model: &str) -> Result<(Arc<dyn Provider>, String)> {
        crate::http::studio::ai::provider_for(model).await
    }

    async fn embed_local(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let failed = |why: String| {
            SprigError::new(
                SprigCode::EmbeddingFailed,
                format!("AI_EMBED with {LOCAL_MODEL}: {why}"),
            )
        };
        let dir = crate::docs::service()
            .ok_or_else(|| failed("this gateway has no tenant logins".into()))?
            .model_dir();
        tokio::task::spawn_blocking(move || {
            let model = sprig_embed::local::shared(&dir)?;
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            model
                .embed_passages(&refs)
                .map(|v| v.into_iter().map(|x| x.to_vec()).collect())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| failed(format!("the embedding task stopped: {e}")))?
        .map_err(failed)
    }

    async fn cache(&self) -> Result<Arc<dyn FnCache>> {
        let (pool, schema) = self.catalog().await?;
        Ok(Arc::new(CatalogFnCache::new(pool.clone(), schema.clone())))
    }

    async fn usage_today(&self) -> Result<UsageToday> {
        self.audit().await?.usage_today(self.user).await
    }

    async fn add_usage(&self, usage: &Usage) -> Result<()> {
        functions::cache::add_usage(&self.audit().await?, self.user, usage).await
    }
}

/// A value as a SQL literal.
fn literal(v: Option<&Output>, nbe: bool) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(Output::Text(t) | Output::Json(t)) => ai_fn::string_literal(t, nbe),
        Some(Output::Vector(v)) => ai_fn::hex_literal(&functions::vector_bytes(v)),
    }
}

/// The column an answer goes out in.
fn column(f: AiFn, name: &str, persona: Persona, charset: u16) -> ColumnDef {
    let mut c = ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        ..ColumnDef::default()
    };
    match f {
        AiFn::Embed => {
            c.col_type = ct::VAR_STRING;
            c.charset = BINARY;
            c.length = u32::try_from(functions::LOCAL_DIM * 4).unwrap_or(u32::MAX);
            c.flags = ColumnFlags::BINARY.bits();
        }
        AiFn::Extract if persona == Persona::MySql84 => {
            // As MySQL 8.4 sends JSON_OBJECT(...).
            c.col_type = ct::JSON;
            c.charset = charset;
            c.length = 4_294_967_292;
            c.flags = ColumnFlags::BINARY.bits();
        }
        _ => {
            c.col_type = ct::VAR_STRING;
            c.charset = charset;
            c.length = TEXT_LENGTH;
        }
    }
    c
}

fn prepared_refused() -> SprigError {
    SprigError::new(
        SprigCode::NameRewriteUnsupported,
        "AI functions in a prepared statement: run it as a plain statement",
    )
}

/// 39051 when `sql`, being prepared, calls an AI function.
///
/// # Errors
/// 39051.
pub(super) fn refuse_prepared(sql: &[u8], lex: sprig_sql::lex::LexOptions) -> Result<()> {
    if ai_fn::mentions(sql)
        && ai_fn::analyze(sql, &lex)
            .is_some_and(|p| p.is_err() || p.is_ok_and(|p| !p.calls.is_empty()))
    {
        return Err(prepared_refused());
    }
    Ok(())
}

impl PassSession {
    /// Answers a statement calling AI functions; `None` for any other.
    #[allow(clippy::too_many_lines)] // constants, then columns
    pub(super) async fn tenant_ai_fn(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !ai_fn::mentions(text) {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.principal.must_change_password {
            return Ok(None);
        }
        let internal = ts.types.internal;
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let user = ts.principal.user_id.unwrap_or(UserId(0));
        let (lex, _) = self.lex_opts();
        let mut plan: Plan = match ai_fn::analyze(text, &lex) {
            None => return Ok(None),
            Some(Err(e)) => return self.fail(out, &e.into()).await.map(Some),
            Some(Ok(p)) => p,
        };
        // The gateway's own statements are left alone, but for the MongoDB
        // engine's, which name their database; a client's hint is a comment.
        match (internal, plan.hint_db.is_some()) {
            (true, false) => return Ok(None),
            (false, true) => plan.hint_db = None,
            _ => {}
        }
        if format == RowFormat::Binary {
            return self.fail(out, &prepared_refused()).await.map(Some);
        }
        // The databases the statement reads.
        let mut dbs: Vec<String> = Vec::new();
        if let Some(h) = &plan.hint_db {
            dbs.push(h.clone());
        } else {
            if let Some(cur) = self.current_logical() {
                dbs.push(cur);
            }
            if let Some(cat) = svc.cache.cached(tenant) {
                for q in &plan.qualifiers {
                    if (cat.database(&q.to_lowercase()).is_some() || cat.database(q).is_some())
                        && !dbs.iter().any(|d| d.eq_ignore_ascii_case(q))
                    {
                        dbs.push(q.clone());
                    }
                }
            }
        }
        let tenant_scope = ExecLookup {
            svc: &svc,
            tenant,
            db: None,
        };
        let share_data = if dbs.is_empty() {
            tenant_scope.flag(&sprig_ai::settings::SHARE_DATA)
        } else {
            dbs.iter().all(|d| {
                ExecLookup {
                    svc: &svc,
                    tenant,
                    db: Some(d),
                }
                .flag(&sprig_ai::settings::SHARE_DATA)
            })
        };
        let settings = FnSettings::read(&tenant_scope, share_data);
        let env = GatewayEnv {
            svc: Arc::clone(&svc),
            tenant,
            user,
            catalog: OnceCell::new(),
        };
        let nbe = lex.no_backslash_escapes;
        let mut warnings: Vec<LocalWarning> = Vec::new();
        let mut note_nulls = |f: AiFn, n: usize| {
            if n > 0 {
                warnings.push(LocalWarning {
                    level: "Warning",
                    code: 39992,
                    message: format!(
                        "{}: {n} values are NULL because the daily AI token budget is used up (ai.functions.on_budget = null)",
                        f.sql_name()
                    ),
                });
            }
        };
        // Every call's arguments, checked before anything runs.
        let mut calls = Vec::with_capacity(plan.calls.len());
        for c in &plan.calls {
            let f = AiFn::from_sql(c.name).ok_or_else(|| SprigError::internal("an AI function"))?;
            match functions::Call::from_args(f, &c.rest, &settings.known_models(f)) {
                Ok(call) => calls.push(call),
                Err(e) => return self.fail(out, &e).await.map(Some),
            }
        }
        // 1. Literal inputs.
        let mut values = Vec::with_capacity(plan.calls.len());
        for (c, call) in plan.calls.iter().zip(&calls) {
            let Some(input) = &c.literal else {
                values.push(String::new());
                continue;
            };
            match Box::pin(functions::evaluate(
                &env,
                &settings,
                call,
                std::slice::from_ref(input),
            ))
            .await
            {
                Ok((v, r)) => {
                    note_nulls(call.f, r.budget_nulls);
                    values.push(literal(v.first().and_then(Option::as_ref), nbe));
                }
                Err(e) => return self.fail(out, &e).await.map(Some),
            }
        }
        let sql = Bytes::from(plan.render(text, &values));
        metrics::counter!("sprig_translator_rules_applied_total", "rule" => "ai_functions")
            .increment(1);
        if !plan.has_columns() {
            let step = Box::pin(self.tenant_text(cx, &sql, attrs, more, format, out)).await?;
            if !warnings.is_empty()
                && !step.errored
                && let Some(ts) = self.tenant.as_deref_mut()
            {
                ts.warnings.get_or_insert_with(Vec::new).extend(warnings);
            }
            return Ok(Some(step));
        }
        // 2. Columns over rows: run, read, answer, send.
        let got = match Box::pin(self.run_checked(cx, &String::from_utf8_lossy(&sql), false)).await
        {
            Ok(g) => g,
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        if got.rows.len() > MAX_ROWS {
            let e = SprigError::standard(
                39601,
                *b"HY000",
                format!(
                    "ER_SPRIG_TRANSLATE_LIMIT: a statement with AI functions over its rows may return at most {MAX_ROWS} rows; add a LIMIT or a narrower WHERE"
                ),
            );
            return self.fail(out, &e).await.map(Some);
        }
        let mut columns = got.defs.clone();
        let mut rows: Vec<Vec<Option<Bytes>>> = got
            .rows
            .into_iter()
            .map(|r| r.into_iter().map(|v| v.map(Bytes::from)).collect())
            .collect();
        let charset = cx.proto.collation;
        for (c, call) in plan.calls.iter().zip(&calls) {
            if c.constant() {
                continue;
            }
            let Some(item) = &c.item else { continue };
            let Some(i) = item.column.index(columns.len()) else {
                let e = SprigError::internal("an AI function's column isn't in the result");
                return self.fail(out, &e).await.map(Some);
            };
            let inputs: Vec<Option<String>> = rows
                .iter()
                .map(|r| {
                    r.get(i)
                        .cloned()
                        .flatten()
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                })
                .collect();
            let answers = match Box::pin(functions::evaluate(&env, &settings, call, &inputs)).await
            {
                Ok((a, r)) => {
                    note_nulls(call.f, r.budget_nulls);
                    a
                }
                Err(e) => return self.fail(out, &e).await.map(Some),
            };
            let name = item
                .alias
                .clone()
                .unwrap_or_else(|| c.written(text).into_owned());
            columns[i] = column(call.f, &name, self.persona, charset);
            for (r, a) in rows.iter_mut().zip(answers) {
                if let Some(cell) = r.get_mut(i) {
                    *cell = a.map(|a| Bytes::from(a.sql_bytes()));
                }
            }
        }
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        out.begin_rows(&columns, RowFormat::Text).await?;
        for r in &rows {
            let vals: Vec<Option<&[u8]>> = r.iter().map(Option::as_deref).collect();
            out.row(RowValues::Text(&vals)).await?;
        }
        out.end_rows(EndOfRows {
            status,
            warnings: u16::try_from(warnings.len()).unwrap_or(u16::MAX),
        })
        .await?;
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.warnings = (!warnings.is_empty()).then_some(warnings);
        }
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_become_literals_and_columns_are_typed() {
        assert_eq!(literal(None, false), "NULL");
        assert_eq!(
            literal(Some(&Output::Text("it's".into())), false),
            "'it''s'"
        );
        assert_eq!(
            literal(Some(&Output::Vector(vec![1.0, 2.0])), false),
            "X'0000803F00000040'"
        );
        let c = column(AiFn::Embed, "e", Persona::MariaDb, 45);
        assert_eq!(
            (c.col_type, c.charset, c.length),
            (ct::VAR_STRING, 63, 1536)
        );
        let c = column(AiFn::Extract, "j", Persona::MySql84, 255);
        assert_eq!((c.col_type, c.charset), (ct::JSON, 255));
        let c = column(AiFn::Classify, "k", Persona::MySql84, 255);
        assert_eq!((c.col_type, &c.name[..]), (ct::VAR_STRING, &b"k"[..]));
    }
}
