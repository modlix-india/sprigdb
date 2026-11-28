//! The AI assistants' routes.
//!
//! | Route | Does |
//! |---|---|
//! | `POST /v1/ai/chat` | One turn `{role, message, conversation?, database?, context?}`, streamed as NDJSON ([`chat`]) |
//! | `GET /v1/ai/conversations?limit=` | The caller's conversations, newest first: their tenant's, and for a platform operator their administration conversations too |
//! | `GET /v1/ai/conversations/{id}?payloads=` | One of them with its messages, including what was sent to the provider (`payloads=false` leaves the payloads out) |
//! | `POST /v1/ai/feedback` | `{message_id, vote: up\|down, note?}` on an answer, or `{proposal_id, outcome: accepted\|dismissed\|failed, note?}` |
//! | `GET /v1/ai/glossary?status=` | The tenant glossary (`accepted`, `proposed` or `all`) |
//! | `POST /v1/ai/glossary` | Adds or replaces an accepted term `{term, definition, sql?, tables?}` (tenant administrators) |
//! | `POST /v1/ai/glossary/{term}:accept`, `:reject`, `:delete` | Decides a proposal, or deletes a term (tenant administrators) |
//! | `GET /v1/ai/digests?limit=` | The tenant's digests, newest first (tenant administrators) |
//! | `GET /v1/ai/digests/{id}?payloads=` | One digest: its facts, the model's words, and what was sent to word it (`payloads=true` adds the exact payloads) |
//! | `POST /v1/ai/proposals/{id}:accept`, `:dismiss` | Acts on a proposal `{confirm?, note?}`: runs a statement as the caller, files advice, accepts a term ([`proposals`]) |
//! | `POST`, `DELETE /v1/mcp` | The MCP server: the tenant assistants' tools for outside agents ([`mcp`]) |
//!
//! **Who.** Every route runs as the signed-in caller (a Studio session,
//! Basic or an API key). `dba` and `data` turns need a tenant login and
//! keep their conversations in the tenant's catalog records, readable only by
//! their owner; `admin` turns need a platform operator (a login in
//! `studio.operator_logins` with operator mode on) and keep their
//! conversations in the system store, the DC's system catalog records
//! ([`system`]), readable only by that operator while they are one.
//! Its ids carry an `a` prefix (`a12`). When the system store can't be
//! reached, an `admin` turn still runs, unstored, with a warning. Tools
//! run with the caller's rights ([`host`]).
//!
//! **Before a turn**, in this order: `ai.enabled` (39991), the role
//! (1227 or 39792), context that would send rows with `ai.share_data` off
//! (39995), the role's model and provider (39990), and the daily budgets
//! (39992). These are plain HTTP errors; anything later is an `error`
//! line in the stream. An `admin` turn reads its settings at the global
//! scope, so its budgets are the global `ai.budget.tenant_tokens_day` for
//! all administration turns of the DC and `ai.budget.user_tokens_day` for
//! each operator.
//!
//! **Prompts.** Each turn's system prompt is the role's base prompt, then
//! for tenant roles the tenant's accepted glossary terms and its users'
//! recent feedback ([`sprig_ai::glossary::prompt`],
//! [`sprig_ai::feedback::prompt`]).

pub mod ask;
pub mod chat;
pub mod host;
pub mod mcp;
pub mod proposals;
pub mod schema_check;
pub mod system;

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use hyper::body::Incoming;
use hyper::{Request, Response};
use serde_json::{Value, json};
use sprig_ai::audit::rec::RecAudit;
use sprig_ai::audit::{AuditStore, ConvId};
use sprig_ai::feedback::{self, FeedbackStore, Outcome};
use sprig_ai::glossary::{self, GlossaryStore, NewTerm, RecGlossary, Status};
use sprig_ai::keys::AiKeyring;
use sprig_ai::providers::ProviderStore;
use sprig_ai::settings::{Lookup, TurnSettings};
use sprig_ai::{Agent, Role, ToolRegistry, TurnConfig, TurnRequest};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_settings::Setting;
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{StudioState, hdr, json_response, parse_json, read_body, rows_of, run_sql};

/// The longest message or context item, in bytes.
pub const MAX_MESSAGE: usize = 32 * 1024;
/// Conversations one listing returns at most.
const MAX_LIST: u32 = 200;

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// Percent-encodes a query value.
pub(crate) fn pct(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            o.push(char::from(b));
        } else {
            let _ = write!(o, "%{b:02X}");
        }
    }
    o
}

// ---- the service ----

/// Where providers come from and the keys that open them.
pub struct AiService {
    providers: ProviderStore,
    keys: Arc<dyn AiKeyring>,
}

impl std::fmt::Debug for AiService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiService").finish_non_exhaustive()
    }
}

/// No master key file: only providers without a key (a local server) open.
struct NoKeys;

impl AiKeyring for NoKeys {
    fn current(&self, _owner: TenantId) -> Option<(u32, [u8; 32])> {
        None
    }
    fn key(&self, _owner: TenantId, _version: u32) -> Option<[u8; 32]> {
        None
    }
}

static SERVICE: OnceLock<Arc<AiService>> = OnceLock::new();

/// Makes providers in `global` usable by this process's assistants, with
/// keys opened by the master key file at `key_file` (`[gateway]
/// workload_key_file`). Once; later calls are ignored.
pub fn install(global: Arc<dyn MetaStore>, key_file: Option<&Path>) {
    let providers = match ProviderStore::new(global) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "the AI assistants have no provider store");
            return;
        }
    };
    let keys: Arc<dyn AiKeyring> = match key_file {
        Some(p) => match sprig_workload::keyring::MasterKeyring::from_file(p) {
            Ok(k) => Arc::new(k),
            Err(e) => {
                tracing::warn!(error = %e, "AI provider keys can't be opened on this gateway");
                Arc::new(NoKeys)
            }
        },
        None => Arc::new(NoKeys),
    };
    let _ = SERVICE.set(Arc::new(AiService { providers, keys }));
}

fn service() -> Result<Arc<AiService>, SprigError> {
    SERVICE.get().cloned().ok_or_else(|| {
        SprigError::new(
            SprigCode::AiNotConfigured,
            "this gateway has no AI provider store (it runs without the global tier)",
        )
    })
}

/// The provider and model a `provider/model` value names, for the AI
/// functions in statements.
///
/// # Errors
/// 39990 without a provider store, or when the value names no provider.
pub(crate) async fn provider_for(
    model: &str,
) -> Result<(Arc<dyn sprig_ai::Provider>, String), SprigError> {
    let svc = service()?;
    svc.providers.for_role(Role::Data, model, &*svc.keys).await
}

// ---- settings ----

/// The `ai.*` values for a caller: the tenant's (and database's) scopes,
/// then dc and global.
pub struct GatewayLookup<'a> {
    st: &'a StudioState,
    tenant: Option<TenantId>,
    db: Option<&'a str>,
    /// The tenant's catalog as [`StudioState::load_tenant`] returned it:
    /// read instead of the cache, which may drop the tenant again between
    /// the load and the read. `None`: the cache as it is.
    cat: Option<Arc<sprig_catalog::TenantCatalog>>,
}

impl GatewayLookup<'_> {
    fn get<T: sprig_settings::SettingValue>(&self, s: &'static Setting<T>) -> T {
        match &self.cat {
            Some(cat) => self.st.tenant_get_in(s, Some(cat), self.db),
            None => self.st.tenant_get(s, self.tenant, self.db),
        }
    }
}

impl Lookup for GatewayLookup<'_> {
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

/// `WhoAmI`'s `ai`: whether the assistants are on for the caller's tenant
/// (`ai.enabled`), whether results may leave (`ai.share_data` at the
/// tenant's scope; a database may differ, and each turn's `start` line says
/// what applied), and the roles the caller may chat with that have a model
/// (`ai.model.<role>`) on a gateway with a provider store. Studio shows the
/// assistant's controls only when a role is listed.
pub(crate) fn whoami_json(st: &StudioState, tenant: Option<TenantId>, operator: bool) -> Value {
    let l = GatewayLookup {
        st,
        tenant,
        db: None,
        cat: None,
    };
    let g = GatewayLookup {
        st,
        tenant: None,
        db: None,
        cat: None,
    };
    let model = |l: &GatewayLookup<'_>, r: Role| TurnSettings::read(l, r).model;
    let roles = ai_roles(
        SERVICE.get().is_some(),
        tenant.is_some(),
        &[
            (Role::Data, model(&l, Role::Data)),
            (Role::Dba, model(&l, Role::Dba)),
            (
                Role::Admin,
                if operator {
                    model(&g, Role::Admin)
                } else {
                    String::new()
                },
            ),
        ],
    );
    json!({
        "enabled": tenant.is_some() && l.flag(&sprig_ai::settings::ENABLED),
        "share_data": tenant.is_some() && l.flag(&sprig_ai::settings::SHARE_DATA),
        "roles": roles,
    })
}

/// The roles [`whoami_json`] lists: a tenant role when the caller has a
/// tenant and the role has a model, the administration role when it has
/// one (the caller passes an empty model unless they are an operator);
/// none without a provider store.
pub(crate) fn ai_roles(
    installed: bool,
    tenant: bool,
    models: &[(Role, String)],
) -> Vec<&'static str> {
    models
        .iter()
        .filter(|(r, m)| installed && !m.trim().is_empty() && (tenant || !r.is_tenant()))
        .map(|(r, _)| r.as_str())
        .collect()
}

// ---- tenant stores ----

/// The catalog records the assistants' stores read and write: the
/// services' leader-routed records (`TenantServices::writers`, a
/// [`LeaderRecords`](crate::tenant::catalog_records::LeaderRecords)): a
/// transaction runs on this node when it leads the tenant's unit (or the
/// engine doesn't replicate), else on the unit's leader through its peer
/// service, as auth changes do. A replica that doesn't lead the tenant can
/// then still store a conversation, a vote or a glossary entry.
pub(crate) fn records(svc: &crate::tenant::TenantServices) -> Arc<dyn sprig_catalog::RecordStore> {
    Arc::clone(&svc.writers)
}

/// `local` (this node's records) behind the unit-leader routing, as
/// `TenantServices::writers` is built (tests).
#[cfg(test)]
pub(crate) fn leader_routed(
    local: Arc<dyn sprig_catalog::RecordStore>,
    dc: Arc<dyn sprig_meta::MetaStore>,
) -> Arc<crate::tenant::catalog_records::LeaderRecords> {
    Arc::new(crate::tenant::catalog_records::LeaderRecords::new(
        local, dc,
    ))
}

struct Stores {
    audit: Arc<RecAudit>,
    glossary: Arc<RecGlossary>,
    tenant: TenantId,
    user: UserId,
}

async fn stores(core: &Arc<Core>, caller: &Caller) -> Result<Stores, SprigError> {
    let (Some(svc), Some(tid), Some(user)) = (
        &core.studio.tenants,
        caller.principal.tenant,
        caller.principal.user_id,
    ) else {
        return Err(invalid("the assistants' conversations need a tenant login"));
    };
    // The tenant's catalog records on the engine.
    Ok(Stores {
        audit: Arc::new(RecAudit::tenant(records(svc), tid)),
        glossary: Arc::new(RecGlossary::new(records(svc), tid)),
        tenant: tid,
        user,
    })
}

async fn tenant_admin(core: &Arc<Core>, caller: &Caller) -> Result<bool, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect();
    Ok(super::grants::Privileges::of(&lines).tenant_admin)
}

fn not_admin() -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        "Access denied; changing the glossary needs SPRIG_TENANT_ADMIN",
    )
}

// ---- routes ----

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_ai_path(path: &str) -> bool {
    path.starts_with("/v1/ai/")
}

/// Answers `/v1/ai/...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let caller = core
        .caller(
            info,
            hdr(&req, "authorization"),
            hdr(&req, super::super::rest::TENANT_HEADER),
            hdr(&req, "host"),
        )
        .await?;
    let rest = path.trim_start_matches("/v1/ai/");
    match (method.as_str(), rest) {
        ("POST", "chat") => {
            let b = parse_json(&read_body(req).await?)?;
            chat_route(core, caller, &b).await
        }
        ("GET", "conversations") => list(core, &caller, &query).await,
        ("GET", r) if r.starts_with("conversations/") => {
            let id = r.trim_start_matches("conversations/");
            show(core, &caller, id, &query).await
        }
        ("POST", "feedback") => {
            let b = parse_json(&read_body(req).await?)?;
            feedback_route(core, &caller, &b).await
        }
        ("GET", "glossary") => glossary_list(core, &caller, &query).await,
        ("POST", "glossary") => {
            let b = parse_json(&read_body(req).await?)?;
            glossary_put(core, &caller, &b).await
        }
        ("POST", r) if r.starts_with("glossary/") => {
            let seg = super::catalog::percent_decode(r.trim_start_matches("glossary/"));
            let Some((term, verb)) = seg.rsplit_once(':') else {
                return Err(invalid("POST /v1/ai/glossary/{term}:accept|reject|delete"));
            };
            glossary_verb(core, &caller, term, verb).await
        }
        ("GET", "digests") => digests(core, &caller, None, &query).await,
        ("GET", r) if r.starts_with("digests/") => {
            let id = r.trim_start_matches("digests/");
            digests(core, &caller, Some(id), &query).await
        }
        ("POST", r) if r.starts_with("proposals/") => {
            let seg = r.trim_start_matches("proposals/");
            let Some((id, verb)) = seg.rsplit_once(':') else {
                return Err(invalid("POST /v1/ai/proposals/{id}:accept|dismiss"));
            };
            let raw = read_body(req).await?;
            let b = if raw.iter().all(u8::is_ascii_whitespace) {
                json!({})
            } else {
                parse_json(&raw)?
            };
            proposals::route(core, &caller, id, verb, &b).await
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// The text of a JSON field that may be a string or a number.
fn id_of(v: &Value, k: &str) -> Option<String> {
    match v.get(k) {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_owned()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

/// The conversation a turn continues (the caller's, with the same
/// assistant), or a new one titled by the message. An administration
/// conversation's id may carry its `a` prefix.
async fn open_conversation(
    audit: &dyn AuditStore,
    user: UserId,
    role: Role,
    b: &Value,
    message: &str,
) -> Result<(ConvId, Vec<sprig_ai::Message>), SprigError> {
    let Some(id) = id_of(b, "conversation") else {
        let title = sprig_ai::egress::redact_secrets(message);
        return Ok((audit.create(user, role, &title).await?, Vec::new()));
    };
    let id = match system::parse(&id) {
        Some(n) if !role.is_tenant() => n,
        _ => id
            .parse::<u64>()
            .map_err(|_| invalid("conversation is a conversation id"))?,
    };
    let c = audit
        .get(user, ConvId(id))
        .await?
        .ok_or_else(|| invalid(format!("you have no conversation {id}")))?;
    if c.role != role {
        return Err(invalid(format!(
            "conversation {id} is with the {} assistant",
            c.role
        )));
    }
    let msgs = audit.messages(c.id).await?;
    Ok((c.id, sprig_ai::audit::history(&msgs)))
}

/// What a turn starts from: its store, glossary, conversation and history.
type Prepared = (
    Arc<dyn AuditStore>,
    Option<Arc<dyn GlossaryStore>>,
    Option<ConvId>,
    Vec<sprig_ai::Message>,
);

#[allow(clippy::too_many_lines)] // the checks in the module docs' order
async fn chat_route(
    core: &Arc<Core>,
    caller: Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let asked = b.get("role").and_then(Value::as_str).unwrap_or("data");
    if !matches!(asked.trim(), "data" | "dba" | "admin") {
        return Err(invalid("role is data, dba or admin"));
    }
    let message = b
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    if message.is_empty() || message.len() > MAX_MESSAGE {
        return Err(invalid(format!("message is 1 to {MAX_MESSAGE} bytes")));
    }
    let database = b
        .get("database")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|d| !d.is_empty() && *d != "-")
        .map(str::to_owned);
    let tenant_login = caller.principal.tenant.is_some() && caller.principal.user_id.is_some();
    let tenant = if asked.trim() == "admin" {
        None
    } else {
        caller.principal.tenant
    };
    // The tenant's own settings, never this gateway's defaults for a tenant
    // whose catalog isn't loaded here (a replica that doesn't lead it).
    let cat = core.studio.load_tenant(tenant).await?;
    let settings = TurnSettings::read(
        &GatewayLookup {
            st: &core.studio,
            tenant,
            db: database.as_deref(),
            cat,
        },
        Role::parse(asked.trim()).unwrap_or(Role::Data),
    );
    // 1. Off for the tenant: before anything else.
    if !settings.enabled {
        return Err(sprig_ai::error::disabled());
    }
    // 2. The role.
    let operator = !super::ops::roles(core, &caller).is_empty();
    let role = chat::role_for(asked, tenant_login, operator)?;
    // 3. Context that would send rows.
    let context = chat::context_text(b.get("context"), settings.share_data)?;
    // 4. The model.
    let svc = service()?;
    let (provider, model) = svc
        .providers
        .for_role(role, &settings.model, &*svc.keys)
        .await?;
    let user = caller.principal.user_id.unwrap_or(UserId(0));
    let mut system = sprig_ai::prompt::base(role);
    let (audit, glossary, conv, history): Prepared = if role.is_tenant() {
        let st = stores(core, &caller).await?;
        // 5. The budgets, before the stream starts.
        let spent = st.audit.usage_today(st.user).await?;
        settings.budget.check(spent, 0)?;
        let (conv, history) = open_conversation(&*st.audit, st.user, role, b, &message).await?;
        if let Ok(terms) = st.glossary.list(Some(Status::Accepted)).await
            && let Some(p) = glossary::prompt(&terms)
        {
            system.push(sprig_ai::Block::text(p));
        }
        if let Ok(notes) = st.audit.recent(feedback::PROMPT_NOTES).await
            && let Some(p) = feedback::prompt(&notes)
        {
            system.push(sprig_ai::Block::text(p));
        }
        (
            st.audit as Arc<dyn AuditStore>,
            Some(st.glossary as Arc<dyn GlossaryStore>),
            Some(conv),
            history,
        )
    } else {
        // The administration assistant: its conversations in the system
        // store; unstored (as before it existed) when that can't be reached.
        let sys = match system::store(core, &caller) {
            Ok(s) => {
                let s = Arc::new(s);
                match s.usage_today(user).await {
                    Ok(spent) => Some((s, spent)),
                    Err(e) => {
                        tracing::warn!(error = %e, "an administration turn runs unstored");
                        None
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "an administration turn runs unstored");
                None
            }
        };
        match sys {
            Some((s, spent)) => {
                // 5. The budgets, before the stream starts.
                settings.budget.check(spent, 0)?;
                let (conv, history) = open_conversation(&*s, user, role, b, &message).await?;
                (s as Arc<dyn AuditStore>, None, Some(conv), history)
            }
            None => (
                Arc::new(sprig_ai::MemoryAudit::system()) as Arc<dyn AuditStore>,
                None,
                None,
                Vec::new(),
            ),
        }
    };
    let cfg = TurnConfig::from_settings(role, &settings, model);
    let host = Arc::new(
        host::GatewayHost::new(Arc::clone(core), caller.clone(), settings.share_data)
            .for_role(role),
    );
    let turn = chat::Turn {
        // The chat shows the user every result in full; the model gets
        // what the egress gate lets through.
        agent: Agent::new(provider, ToolRegistry::builtin(), audit).showing_results(),
        cfg,
        req: TurnRequest {
            tenant: if role.is_tenant() {
                caller.principal.tenant
            } else {
                None
            },
            user,
            conv,
            system,
            history,
            message: format!("{message}{context}"),
            host,
            scrubber: None,
            cancel: None,
        },
        glossary,
        author: caller.principal.user.clone(),
    };
    let shown = conv.map(|c| {
        if role.is_tenant() {
            c.0.to_string()
        } else {
            system::label(c.0)
        }
    });
    let (rx, cancel) = chat::start_as(turn, shown.as_deref());
    Ok(chat::response(rx, cancel))
}

/// Conversations and feedback in one store.
pub(crate) trait ConvStore: AuditStore + FeedbackStore {}

impl<T: AuditStore + FeedbackStore> ConvStore for T {}

/// A store opened for the caller, and their user id.
type Opened = (Arc<dyn ConvStore>, UserId);

/// The stores a caller's conversations are in: their tenant's catalog
/// (a tenant login) and the system store (a platform operator; its error
/// when it can't be opened).
pub(crate) struct Views {
    /// The tenant's store and the caller's user id there.
    pub tenant: Option<Opened>,
    /// The administration conversations, and the caller's user id (which
    /// the system store doesn't need: it is opened for the operator).
    pub admin: Option<Result<Opened, SprigError>>,
}

async fn views(core: &Arc<Core>, caller: &Caller) -> Result<Views, SprigError> {
    let operator = !super::ops::roles(core, caller).is_empty();
    let tenant_login = caller.principal.tenant.is_some() && caller.principal.user_id.is_some();
    let tenant = if tenant_login || !operator {
        let st = stores(core, caller).await?;
        Some((st.audit as Arc<dyn ConvStore>, st.user))
    } else {
        None
    };
    let admin = if operator {
        let user = caller.principal.user_id.unwrap_or(UserId(0));
        Some(system::store(core, caller).map(|s| (Arc::new(s) as Arc<dyn ConvStore>, user)))
    } else {
        None
    };
    Ok(Views { tenant, admin })
}

fn conv_json(c: &sprig_ai::audit::Conversation, id: &str) -> Value {
    json!({
        "id": id, "role": c.role, "title": c.title,
        "created_ms": c.created_ms, "updated_ms": c.updated_ms,
    })
}

/// `GET /v1/ai/conversations`: the tenant's and the administration
/// conversations together, newest first, at most `limit`. A system store
/// that can't be reached leaves its conversations out and says why in
/// `admin_unavailable`.
pub(crate) async fn list_json(v: &Views, limit: u32) -> Result<Value, SprigError> {
    // (updated, 0 tenant / 1 administration, id, the entry)
    type Row = (i64, u8, u64, Value);
    let mut all: Vec<Row> = Vec::new();
    if let Some((st, user)) = &v.tenant {
        for c in st.list(*user, limit).await? {
            all.push((c.updated_ms, 0, c.id.0, conv_json(&c, &c.id.0.to_string())));
        }
    }
    let mut out = json!({});
    match &v.admin {
        Some(Ok((st, user))) => match st.list(*user, limit).await {
            Ok(convs) => {
                for c in convs {
                    all.push((
                        c.updated_ms,
                        1,
                        c.id.0,
                        conv_json(&c, &system::label(c.id.0)),
                    ));
                }
            }
            Err(e) => out["admin_unavailable"] = json!(e.message),
        },
        Some(Err(e)) => out["admin_unavailable"] = json!(e.message),
        None => {}
    }
    all.sort_by(|a, b| (b.0, b.1, b.2).cmp(&(a.0, a.1, a.2)));
    out["conversations"] = Value::Array(
        all.into_iter()
            .take(limit as usize)
            .map(|(_, _, _, j)| j)
            .collect(),
    );
    Ok(out)
}

async fn list(
    core: &Arc<Core>,
    caller: &Caller,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let v = views(core, caller).await?;
    let limit = query_value(query, "limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50)
        .clamp(1, MAX_LIST);
    Ok(json_response(200, &list_json(&v, limit).await?))
}

fn hex(h: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in h {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The administration store, or why the caller can't use it.
fn admin_of<'a>(v: &'a Views, id: &str) -> Result<(&'a Arc<dyn ConvStore>, UserId), SprigError> {
    match &v.admin {
        Some(Ok((st, user))) => Ok((st, *user)),
        Some(Err(e)) => Err(SprigError::new(
            e.code().unwrap_or(SprigCode::CatalogUnavailable),
            e.message.clone(),
        )),
        None => Err(invalid(format!("you have no conversation {id}"))),
    }
}

/// `GET /v1/ai/conversations/{id}`: an `a`-prefixed id is an
/// administration conversation, any other the tenant's.
pub(crate) async fn show_json(v: &Views, id: &str, payloads: bool) -> Result<Value, SprigError> {
    let (st, user, n, admin): (&Arc<dyn ConvStore>, UserId, u64, bool) =
        if let Some(n) = system::parse(id) {
            let (st, user) = admin_of(v, id)?;
            (st, user, n, true)
        } else {
            let n: u64 = id
                .parse()
                .map_err(|_| invalid("conversations/{id} takes a conversation id"))?;
            let Some((st, user)) = &v.tenant else {
                return Err(invalid(format!("you have no conversation {id}")));
            };
            (st, *user, n, false)
        };
    let c = st
        .get(user, ConvId(n))
        .await?
        .ok_or_else(|| invalid(format!("you have no conversation {id}")))?;
    let label = |n: u64| {
        if admin {
            system::label(n)
        } else {
            n.to_string()
        }
    };
    let msgs = st.messages(c.id).await?;
    let messages: Vec<Value> = msgs
        .into_iter()
        .map(|m| {
            let mut body = m.body;
            if !payloads && let Some(o) = body.as_object_mut() {
                o.remove("payload");
            }
            json!({
                "id": label(m.id), "kind": m.kind, "body": body,
                "sent_hash": m.sent_hash.as_ref().map(hex), "created_ms": m.created_ms,
            })
        })
        .collect();
    Ok(json!({
        "conversation": conv_json(&c, &label(c.id.0)),
        "messages": messages,
    }))
}

async fn show(
    core: &Arc<Core>,
    caller: &Caller,
    id: &str,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let v = views(core, caller).await?;
    let payloads = query_value(query, "payloads") != Some("false");
    Ok(json_response(200, &show_json(&v, id, payloads).await?))
}

fn proposal_kind(k: &str) -> &'static str {
    match k {
        "sql" => "sql",
        "recommendation" => "recommendation",
        "tile" => "tile",
        "glossary" => "glossary",
        "generate_rows" => "generate_rows",
        _ => "other",
    }
}

/// `POST /v1/ai/feedback`: a vote on an answer (an `a`-prefixed message
/// id is in an administration conversation), or a proposal's outcome
/// (looked up in the tenant's conversations, then the administration
/// ones).
pub(crate) async fn feedback_json(v: &Views, b: &Value) -> Result<Value, SprigError> {
    let note = b.get("note").and_then(Value::as_str);
    if note.is_some_and(|n| n.chars().count() > feedback::MAX_NOTE) {
        return Err(invalid(format!(
            "note is at most {} characters",
            feedback::MAX_NOTE
        )));
    }
    match (id_of(b, "message_id"), id_of(b, "proposal_id")) {
        (Some(m), None) => {
            let vote: i8 = match b.get("vote") {
                Some(Value::String(s)) if s == "up" => 1,
                Some(Value::String(s)) if s == "down" => -1,
                Some(Value::Number(n)) if n.as_i64() == Some(1) => 1,
                Some(Value::Number(n)) if n.as_i64() == Some(-1) => -1,
                _ => return Err(invalid("vote is up or down")),
            };
            let not_found =
                || invalid(format!("message {m} isn't an answer in your conversations"));
            let role = if let Some(n) = system::parse(&m) {
                let (st, user) = admin_of(v, &m).map_err(|_| not_found())?;
                st.vote(user, n, vote, note).await?
            } else {
                let id: u64 = m
                    .parse()
                    .map_err(|_| invalid("message_id is a message id"))?;
                match &v.tenant {
                    Some((st, user)) => st.vote(*user, id, vote, note).await?,
                    None => None,
                }
            }
            .ok_or_else(not_found)?;
            sprig_ai::metrics::feedback(role, if vote > 0 { "up" } else { "down" });
        }
        (None, Some(p)) => {
            let outcome = b
                .get("outcome")
                .and_then(Value::as_str)
                .and_then(Outcome::parse)
                .ok_or_else(|| invalid("outcome is accepted, dismissed or failed"))?;
            let mut found = match &v.tenant {
                Some((st, user)) => st.outcome(*user, &p, outcome, note).await?,
                None => None,
            };
            if found.is_none()
                && let Some(Ok((st, user))) = &v.admin
            {
                found = st.outcome(*user, &p, outcome, note).await?;
            }
            let (_, kind) = found
                .ok_or_else(|| invalid(format!("proposal {p} isn't in your conversations")))?;
            sprig_ai::metrics::proposal(proposal_kind(&kind), outcome.as_str());
        }
        _ => {
            return Err(invalid(
                "send {message_id, vote} or {proposal_id, outcome}, with an optional note",
            ));
        }
    }
    Ok(json!({"recorded": true}))
}

async fn feedback_route(
    core: &Arc<Core>,
    caller: &Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let v = views(core, caller).await?;
    Ok(json_response(200, &feedback_json(&v, b).await?))
}

fn digest_json(d: &sprig_ai::digest::Digest, whole: bool) -> Value {
    let mut v = json!({
        "id": d.id.to_string(),
        "cadence": d.cadence.as_str(),
        "from_ms": d.from_ms,
        "to_ms": d.to_ms,
        "status": d.status.as_str(),
        "model": d.model,
        "note": d.note,
        "conversation": d.conversation.map(|c| c.to_string()),
        "emailed_ms": d.emailed_ms,
        "created_ms": d.created_ms,
        "notable": d.facts.notable(),
    });
    if whole {
        v["body"] = json!(d.body());
        v["facts_text"] = json!(d.facts_text);
        v["text"] = json!(d.text);
        v["facts"] = json!(d.facts);
        v["usage"] = d.usage.clone();
    }
    v
}

/// `GET /v1/ai/digests[/{id}]`: the tenant's digests, for its
/// administrators (a digest covers every database of the tenant).
async fn digests(
    core: &Arc<Core>,
    caller: &Caller,
    id: Option<&str>,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    use sprig_ai::digest::{DigestStore, RecDigests};
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("digests need a tenant login"));
    };
    if !tenant_admin(core, caller).await? {
        return Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; the tenant's digests need SPRIG_TENANT_ADMIN",
        ));
    }
    let store = RecDigests::new(records(svc), tid);
    let Some(id) = id else {
        let limit = query_value(query, "limit")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(20)
            .clamp(1, MAX_LIST);
        let list = store.list(limit).await?;
        return Ok(json_response(
            200,
            &json!({"digests": list.iter().map(|d| digest_json(d, false)).collect::<Vec<_>>()}),
        ));
    };
    let n: u64 = id
        .parse()
        .map_err(|_| invalid("digests/{id} takes a digest id"))?;
    let d = store
        .get(n)
        .await?
        .ok_or_else(|| invalid(format!("there is no digest {id}")))?;
    let mut v = digest_json(&d, true);
    // What was sent to word it: the stored turn's notes.
    if let Some(c) = d.conversation {
        let payloads = query_value(query, "payloads") == Some("true");
        let audit = RecAudit::tenant(records(svc), tid);
        let sent: Vec<Value> = audit
            .messages(ConvId(c))
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|m| m.kind == sprig_ai::MsgKind::SystemNote)
            .map(|m| {
                let mut body = m.body;
                if !payloads && let Some(o) = body.as_object_mut() {
                    o.remove("payload");
                }
                body
            })
            .collect();
        v["sent"] = Value::Array(sent);
    }
    Ok(json_response(200, &v))
}

async fn glossary_list(
    core: &Arc<Core>,
    caller: &Caller,
    query: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    let status = match query_value(query, "status").unwrap_or("all") {
        "all" => None,
        s => Some(Status::parse(s).ok_or_else(|| invalid("status is accepted, proposed or all"))?),
    };
    let terms = st.glossary.list(status).await?;
    let can_edit = tenant_admin(core, caller).await.unwrap_or(false);
    Ok(json_response(
        200,
        &json!({"terms": terms, "can_edit": can_edit}),
    ))
}

fn audit_glossary(tenant: TenantId, caller: &Caller, action: &str, term: &str) {
    sprig_common::audit::record(
        AuditStream::Tenant(tenant),
        "ai.glossary",
        json!({"user": caller.principal.user, "action": action, "term": term}),
    );
}

async fn glossary_put(
    core: &Arc<Core>,
    caller: &Caller,
    b: &Value,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    if !tenant_admin(core, caller).await? {
        return Err(not_admin());
    }
    let t = NewTerm::from_json(b)?;
    st.glossary.put(&t, st.user, &caller.principal.user).await?;
    audit_glossary(st.tenant, caller, "put", &t.term);
    Ok(json_response(
        200,
        &json!({"term": t.term, "status": "accepted"}),
    ))
}

async fn glossary_verb(
    core: &Arc<Core>,
    caller: &Caller,
    term: &str,
    verb: &str,
) -> Result<Response<Body>, SprigError> {
    let st = stores(core, caller).await?;
    if !tenant_admin(core, caller).await? {
        return Err(not_admin());
    }
    let done = match verb {
        "accept" => st.glossary.accept(term, st.user).await?,
        "reject" => st.glossary.reject(term).await?,
        "delete" => st.glossary.delete(term).await?,
        _ => {
            return Err(invalid(
                "the glossary's verbs are accept, reject and delete",
            ));
        }
    };
    if !done {
        return Err(invalid(match verb {
            "delete" => format!("the glossary has no term {term}"),
            _ => format!("there is no proposal for {term}"),
        }));
    }
    audit_glossary(st.tenant, caller, verb, term);
    Ok(json_response(200, &json!({"term": term, "done": verb})))
}
