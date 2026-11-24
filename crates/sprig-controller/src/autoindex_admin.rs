//! Automatic indexing in the Admin API: a tenant's automatic indexes and
//! the loop's recent decisions, read from its catalog records
//! (`m/autoindex/...`, written by the gateway that hosts its engine).
//!
//! - `GET /admin/v1/tenants/{name}/indexes`: every index record
//!   (recommended, building, observing, active, and the ended ones), and
//!   the events of the last 30 days (candidates, builds, drops, deferrals),
//!   newest last, at most 500.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use sprig_advisor::autoindex::model::{Event, IndexRecord};
use sprig_advisor::autoindex::policy::DAY_US;
use sprig_advisor::autoindex::store::AutoIndexStore;
use sprig_catalog::RecordStore;
use sprig_common::{Result, SprigError};

use crate::tree::Directory;

/// Events returned at most.
const MAX_EVENTS: usize = 500;

/// `GET /admin/v1/tenants/{name}/indexes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexesResponse {
    /// The tenant's name.
    pub tenant: String,
    /// Its automatic indexes, by name.
    pub indexes: Vec<IndexRecord>,
    /// The loop's decisions of the last 30 days, oldest first.
    pub events: Vec<Event>,
}

/// The routes' state.
pub struct AutoIndexApi {
    dir: Directory,
    records: Arc<dyn RecordStore>,
}

impl std::fmt::Debug for AutoIndexApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoIndexApi").finish_non_exhaustive()
    }
}

impl AutoIndexApi {
    /// Over the tenant directory and the tenants' records.
    #[must_use]
    pub fn new(dir: Directory, records: Arc<dyn RecordStore>) -> Self {
        Self { dir, records }
    }

    /// The indexes and events of tenant `name`; `None` when there is no
    /// such tenant.
    ///
    /// # Errors
    /// The directory's and the records'.
    pub async fn list(&self, name: &str, now_us: u64) -> Result<Option<IndexesResponse>> {
        let Some(entry) = self.dir.by_name(name).await? else {
            return Ok(None);
        };
        let tenant = entry.record.body.tenant_id;
        let mut tx = self.records.begin(tenant).await?;
        let indexes = AutoIndexStore::indexes(&mut *tx).await?;
        let events =
            AutoIndexStore::events(&mut *tx, now_us.saturating_sub(30 * DAY_US), MAX_EVENTS)
                .await?;
        tx.commit().await?;
        Ok(Some(IndexesResponse {
            tenant: name.to_owned(),
            indexes,
            events,
        }))
    }
}

fn json<T: Serialize>(status: StatusCode, v: &T) -> Response {
    let body = serde_json::to_vec(v).unwrap_or_default();
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn error(status: StatusCode, e: &SprigError) -> Response {
    json(status, &serde_json::json!({ "error": e.to_string() }))
}

async fn list(State(api): State<Arc<AutoIndexApi>>, UrlPath(name): UrlPath<String>) -> Response {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
    match api.list(&name, now).await {
        Ok(Some(r)) => json(StatusCode::OK, &r),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            &SprigError::internal(format!("no tenant {name}")),
        ),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The routes.
pub fn router(api: Arc<AutoIndexApi>) -> Router {
    Router::new()
        .route("/admin/v1/tenants/{name}/indexes", get(list))
        .with_state(api)
}
