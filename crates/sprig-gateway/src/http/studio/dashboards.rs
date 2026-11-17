//! Dashboards and their tiles ([`sprig_ai::dashboards`]), on the HTTP
//! listeners.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/dashboards` | The caller's own dashboards and the tenant's shared ones |
//! | `POST /v1/dashboards` | Makes one `{name, shared?, filters?}` |
//! | `POST /v1/dashboards:run` | Runs a query not saved yet `{database, sql, filters?, params?}`, read-only |
//! | `GET /v1/dashboards/{id}` | One dashboard with its tiles |
//! | `POST /v1/dashboards/{id}` | Changes `{name?, filters?, layout?, shared?, positions?}` |
//! | `POST /v1/dashboards/{id}:share`, `:delete` | Shares or unshares `{shared}` (the owner), deletes |
//! | `POST /v1/dashboards/{id}/tiles` | Adds a tile `{title, database, sql, chart, refresh_seconds?, pos?}` |
//! | `POST /v1/dashboards/{id}/tiles/{tile}`, `:delete` | Replaces or deletes a tile |
//! | `POST /v1/dashboards/{id}/tiles/{tile}:run` | Runs a tile's query as the caller `{params?}` |
//!
//! Every query runs as the signed-in caller, through the same engine as
//! `Sql.Execute`, inside `START TRANSACTION READ ONLY` (rolled back),
//! capped at `studio.max_result_rows` and `studio.statement_timeout_ms`.
//! A viewer of a shared dashboard sees what their own grants and
//! row-level security allow, never what its owner sees.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyper::body::Incoming;
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_ai::dashboards::{self as dash, CatalogDashboards, TileRunner, Who};
use sprig_ai::tools::ResultSet;
use sprig_common::{Result, SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::{Caller, Caps};
use super::super::{ConnInfo, Core};
use super::ai::host::{GatewayHost, result_set};
use super::{hdr, json_response, parse_json, read_body, rows_of, run_sql};

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_dashboards_path(path: &str) -> bool {
    path == "/v1/dashboards"
        || path.starts_with("/v1/dashboards/")
        || path.starts_with("/v1/dashboards:")
}

/// The caller's tenant's dashboards.
///
/// # Errors
/// Not a tenant login; the catalog unreachable.
pub async fn store(core: &Arc<Core>, caller: &Caller) -> Result<CatalogDashboards> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("dashboards need a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    Ok(CatalogDashboards::new(
        ep.pool.clone(),
        ep.store.schema().to_owned(),
    ))
}

/// The caller as dashboards see them; `admin` asks `SHOW GRANTS` only
/// when it matters.
///
/// # Errors
/// Not a tenant user.
pub async fn who(core: &Arc<Core>, caller: &Caller, with_admin: bool) -> Result<Who> {
    let Some(user) = caller.principal.user_id else {
        return Err(invalid("dashboards need a tenant login"));
    };
    let admin = if with_admin {
        let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
        let lines: Vec<String> = rows_of(&r)
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect();
        super::grants::Privileges::of(&lines).tenant_admin
    } else {
        false
    };
    Ok(Who {
        user,
        name: caller.principal.user.clone(),
        admin,
    })
}

/// Runs tile queries as one caller.
pub struct Runner {
    host: GatewayHost,
    core: Arc<Core>,
    caller: Caller,
}

impl Runner {
    /// A runner with `caller`'s rights.
    #[must_use]
    pub fn new(core: &Arc<Core>, caller: &Caller) -> Self {
        Self {
            host: GatewayHost::new(Arc::clone(core), caller.clone(), false),
            core: Arc::clone(core),
            caller: caller.clone(),
        }
    }
}

#[async_trait]
impl TileRunner for Runner {
    async fn run(&self, db: &str, sql: &str) -> Result<ResultSet> {
        let scope = (db != "-").then_some(db);
        let st = &self.core.studio;
        let tenant = self.caller.principal.tenant;
        let caps = Caps {
            max_rows: st.tenant_get(&sprig_studio::settings::MAX_RESULT_ROWS, tenant, scope),
            max_bytes: 0,
            timeout: Some(Duration::from_millis(st.tenant_get(
                &sprig_studio::settings::STATEMENT_TIMEOUT_MS,
                tenant,
                scope,
            ))),
        };
        let r = self.host.read_only(db, sql.to_owned(), caps).await?;
        Ok(result_set(&r))
    }
}

fn id(s: &str) -> Result<u64> {
    dash::id_of(&Value::String(s.to_owned()))
}

/// Answers `/v1/dashboards...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let caller = core
        .caller(
            info,
            hdr(&req, "authorization"),
            hdr(&req, super::super::rest::TENANT_HEADER),
            hdr(&req, "host"),
        )
        .await?;
    let body = if method == "POST" {
        parse_json(&read_body(req).await?)?
    } else {
        json!({})
    };
    let rest = path.trim_start_matches("/v1/dashboards");
    // `/{id}[:verb]` or `/{id}/tiles[/{tile}[:verb]]`.
    let (head, verb) = match rest.rsplit_once(':') {
        Some((h, v)) if !v.contains('/') => (h, v),
        _ => (rest, ""),
    };
    let parts: Vec<&str> = head.trim_start_matches('/').split('/').collect();
    let out = match (method.as_str(), parts.as_slice(), verb) {
        ("POST", [""], "run") => dash::run_unsaved(&Runner::new(core, &caller), &body).await?,
        ("GET", [""], "") => {
            let s = store(core, &caller).await?;
            dash::list(&s, &who(core, &caller, true).await?).await?
        }
        ("POST", [""], "") => {
            let s = store(core, &caller).await?;
            dash::create(&s, &who(core, &caller, false).await?, &body).await?
        }
        ("GET", [d], "") => {
            let s = store(core, &caller).await?;
            dash::get(&s, &who(core, &caller, true).await?, id(d)?).await?
        }
        ("POST", [d], "") => {
            let s = store(core, &caller).await?;
            dash::update(&s, &who(core, &caller, true).await?, id(d)?, &body).await?
        }
        ("POST", [d], "share") => {
            let shared = body
                .get("shared")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("shared is true or false"))?;
            let s = store(core, &caller).await?;
            dash::share(&s, &who(core, &caller, true).await?, id(d)?, shared).await?
        }
        ("POST", [d], "delete") => {
            let s = store(core, &caller).await?;
            dash::delete(&s, &who(core, &caller, true).await?, id(d)?).await?
        }
        ("POST", [d, "tiles"], "") => {
            let s = store(core, &caller).await?;
            dash::add_tile(&s, &who(core, &caller, true).await?, id(d)?, &body).await?
        }
        ("POST", [d, "tiles", t], "") => {
            let s = store(core, &caller).await?;
            dash::update_tile(&s, &who(core, &caller, true).await?, id(d)?, id(t)?, &body).await?
        }
        ("POST", [d, "tiles", t], "delete") => {
            let s = store(core, &caller).await?;
            dash::delete_tile(&s, &who(core, &caller, true).await?, id(d)?, id(t)?).await?
        }
        ("POST", [d, "tiles", t], "run") => {
            let s = store(core, &caller).await?;
            let viewer = who(core, &caller, false).await?;
            dash::run_tile(
                &s,
                &viewer,
                &Runner::new(core, &caller),
                id(d)?,
                id(t)?,
                &body,
            )
            .await?
        }
        _ => return Err(invalid(format!("no route {method} {path}"))),
    };
    Ok(json_response(200, &out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert!(is_dashboards_path("/v1/dashboards"));
        assert!(is_dashboards_path("/v1/dashboards:run"));
        assert!(is_dashboards_path("/v1/dashboards/4/tiles/9:run"));
        assert!(!is_dashboards_path("/v1/dashboardsx"));
        assert!(id("12").is_ok() && id("x").is_err());
    }

    /// The `OpenAPI` document's chart marks are the tile check's.
    #[test]
    fn the_documented_marks_are_the_checked_ones() {
        assert_eq!(
            sprig_studio::api::CHART_MARKS,
            sprig_ai::tools::builtin::MARKS
        );
        let doc = sprig_studio::api::schemas();
        let aggs: Vec<&str> = doc["ChartField"]["properties"]["agg"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(aggs, sprig_ai::tools::builtin::AGGS);
    }
}
