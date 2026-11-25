//! Automatic indexing in the Admin API: a tenant's automatic indexes and
//! the loop's recent decisions, read from its catalog records
//! (`m/autoindex/...`, written by the gateway that hosts its engine).
//!
//! - `GET /admin/v1/tenants/{name}/indexes`: every index record
//!   (recommended, building, observing, active, and the ended ones), and
//!   the events of the last 30 days (candidates, builds, drops, deferrals),
//!   newest last, at most 500, and the operators' pending requests.
//! - `POST /admin/v1/tenants/{name}/indexes/{index}:{action}`, action
//!   `drop`, `pin` (`freeze`), `unpin` (`unfreeze`) or `restore`, body
//!   `{"reason": "..."}` (the guard adds `actor`): checks the record's
//!   state and writes a request (`m/autoindex/req/<name>`) that the node
//!   running the tenant's loop applies within a minute; 202 with the
//!   request. `{index}` is the index's name, or `table.name` /
//!   `db.table.name`, any case. 39201 (404) for no such index, 39202 (409)
//!   when its state doesn't allow the action.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use sprig_advisor::autoindex::model::{Action, Event};
/// The records the routes answer with (for clients such as `sprigctl`).
pub use sprig_advisor::autoindex::model::{IndexRecord, OperatorRequest};
use sprig_advisor::autoindex::policy::DAY_US;
use sprig_advisor::autoindex::store::AutoIndexStore;
use sprig_catalog::RecordStore;
use sprig_common::{Result, SprigCode, SprigError};

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
    /// Operators' requests the node hasn't applied yet.
    #[serde(default)]
    pub requests: Vec<OperatorRequest>,
}

/// The body of `POST .../indexes/{index}:{action}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionBody {
    /// Why (kept in the event).
    #[serde(default)]
    pub reason: String,
    /// Who: set by the Admin API's guard.
    #[serde(default)]
    pub actor: String,
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
        let requests = AutoIndexStore::requests(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(IndexesResponse {
            tenant: name.to_owned(),
            indexes,
            events,
            requests,
        }))
    }

    /// Asks for `action` on index `index` of tenant `name`; `None` when
    /// there is no such tenant.
    ///
    /// # Errors
    /// 39201 for no such index, 39202 when its state doesn't allow the
    /// action; the directory's and the records'.
    pub async fn request(
        &self,
        name: &str,
        index: &str,
        action: Action,
        body: &ActionBody,
        now_us: u64,
    ) -> Result<Option<OperatorRequest>> {
        let Some(entry) = self.dir.by_name(name).await? else {
            return Ok(None);
        };
        let tenant = entry.record.body.tenant_id;
        // `db.table.name` and `table.name` name the index by its own name,
        // on that table.
        let (qual, short) = match index.rsplit_once('.') {
            Some((q, n)) => (Some(q.to_lowercase()), n),
            None => (None, index),
        };
        let not_found = || {
            SprigError::new(
                SprigCode::AutoIndexNotFound,
                format!("no automatic index {index} in tenant {name}"),
            )
        };
        let mut tx = self.records.begin(tenant).await?;
        let Some(rec) = AutoIndexStore::index(&mut *tx, short).await? else {
            return Err(not_found());
        };
        let key = rec.spec.table_key();
        if !qual.is_none_or(|q| key == q || key.ends_with(&format!(".{q}"))) {
            return Err(not_found());
        }
        if let Err(why) = action.allowed(&rec) {
            return Err(SprigError::new(SprigCode::AutoIndexStateInvalid, why));
        }
        let req = OperatorRequest {
            index: rec.name.clone(),
            action,
            actor: body.actor.clone(),
            reason: body.reason.clone(),
            at_us: now_us,
        };
        AutoIndexStore::put_request(&mut *tx, &req).await?;
        tx.commit().await?;
        Ok(Some(req))
    }
}

fn json<T: Serialize>(status: StatusCode, v: &T) -> Response {
    let body = serde_json::to_vec(v).unwrap_or_default();
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn error(status: StatusCode, e: &SprigError) -> Response {
    json(status, &serde_json::json!({ "error": e.to_string() }))
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

async fn list(State(api): State<Arc<AutoIndexApi>>, UrlPath(name): UrlPath<String>) -> Response {
    match api.list(&name, now_us()).await {
        Ok(Some(r)) => json(StatusCode::OK, &r),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            &SprigError::internal(format!("no tenant {name}")),
        ),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn act(
    State(api): State<Arc<AutoIndexApi>>,
    UrlPath((name, target)): UrlPath<(String, String)>,
    body: Bytes,
) -> Response {
    let bad = |m: String| {
        error(
            StatusCode::BAD_REQUEST,
            &SprigError::new(SprigCode::AdminRequestInvalid, m),
        )
    };
    let Some((index, verb)) = target.rsplit_once(':') else {
        return bad(format!(
            "{target}: expected <index>:drop, :pin, :unpin or :restore"
        ));
    };
    let Some(action) = Action::parse(verb) else {
        return bad(format!(
            "{verb}: not an index action (drop, pin, unpin, restore)"
        ));
    };
    let body: ActionBody = if body.iter().all(u8::is_ascii_whitespace) {
        ActionBody::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(b) => b,
            Err(e) => return bad(format!("request body: {e}")),
        }
    };
    match api.request(&name, index, action, &body, now_us()).await {
        Ok(Some(r)) => json(StatusCode::ACCEPTED, &r),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            &SprigError::internal(format!("no tenant {name}")),
        ),
        Err(e) => {
            let status = StatusCode::from_u16(e.protocol().http_status)
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
            error(status, &e)
        }
    }
}

/// The routes.
pub fn router(api: Arc<AutoIndexApi>) -> Router {
    Router::new()
        .route("/admin/v1/tenants/{name}/indexes", get(list))
        .route("/admin/v1/tenants/{name}/indexes/{target}", post(act))
        .with_state(api)
}
