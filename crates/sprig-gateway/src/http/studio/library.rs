//! Saved queries and query history in the tenant's catalog, so Studio and
//! `sprig` share one library that follows the user between browsers and
//! machines.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/library?history=N` | The caller's saved queries (newest first) and last `N` runs (100; at most `studio.history_max`) |
//! | `POST /v1/library/saved` | Saves `{name, language, database, text}`, replacing a query of that name |
//! | `POST /v1/library/saved:delete` | Deletes `{name}` |
//! | `POST /v1/library/history` | Records a run `{language, database, text, ok, ms, error, client}` |
//! | `POST /v1/library/history:clear` | Clears the caller's history |
//! | `POST /v1/library/prefs` | Keeps the caller's preferences: `{workspace}`, Studio's last workspace (`null` clears it); `WhoAmI` returns it |
//!
//! - **Whose.** Each row belongs to one user of the tenant (`user_id`);
//!   every route reads and writes only the caller's own rows. A tenant
//!   login is needed (not the accounts file).
//! - **Where.** The tenant's catalog records in the storage engine, module
//!   `library`: `m/library/saved/<user><name>`, `m/library/runs/<user><n>`
//!   (the user's run numbers, with the window of kept numbers in
//!   `m/library/run_window/<user>`) and `m/library/prefs/<user><name>`.
//!   Every transaction runs on the tenant unit's leader, so any node can
//!   write.
//!   They aren't versioned catalog state: nothing is cached, every call
//!   reads them.
//! - **Secrets.** Text is redacted here as well as in the clients: a
//!   string after `IDENTIFIED BY`, `PASSWORD`, `SECRET`, `TOKEN` and the
//!   other secret words becomes `'***'`, words that look like API keys or
//!   session tokens become `***`, Redis `AUTH` arguments are masked, and
//!   text with an unclosed string is kept only as a placeholder.
//! - **Limits.** Names of 1 to 128 characters, text up to 64 KiB, errors
//!   cut to 500 characters, and `studio.history_max` runs per user (500;
//!   0 stops recording and clears nothing).
//! - **Left behind.** A dropped user's rows stay until the tenant is
//!   dropped; they are never shown to a new user (ids aren't reused).

use std::sync::Arc;

use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_catalog::migrate::{Migration, Phase};
use sprig_catalog::{Pk, RecordStore, RecordTx, RowTable};
use sprig_common::ids::TenantId;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::{hdr, invalid, json_response, parse_json, read_body};

/// The catalog module.
pub const MODULE: &str = "library";

const DDL_SAVED: &str = "CREATE TABLE IF NOT EXISTS library_saved (
  user_id   INT UNSIGNED NOT NULL,
  name      VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
  language  VARCHAR(16) NOT NULL,
  db_name   VARCHAR(64) NULL,
  body      MEDIUMTEXT NOT NULL,
  saved_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (user_id, name)
) ENGINE=InnoDB";

const DDL_HISTORY: &str = "CREATE TABLE IF NOT EXISTS library_history (
  run_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id   INT UNSIGNED NOT NULL,
  language  VARCHAR(16) NOT NULL,
  db_name   VARCHAR(64) NULL,
  body      MEDIUMTEXT NOT NULL,
  ok        TINYINT(1) NOT NULL,
  ms        INT UNSIGNED NOT NULL,
  error     VARCHAR(512) NULL,
  client    VARCHAR(16) NOT NULL,
  ran_at    DATETIME(3) NOT NULL,
  PRIMARY KEY (run_id),
  KEY by_user (user_id, run_id)
) ENGINE=InnoDB";

const DDL_PREFS: &str = "CREATE TABLE IF NOT EXISTS library_prefs (
  user_id    INT UNSIGNED NOT NULL,
  name       VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
  value      VARCHAR(256) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  PRIMARY KEY (user_id, name)
) ENGINE=InnoDB";

/// The module's migrations, for tenant catalogs on MariaDB made before
/// the library moved to the catalog records (the registry still declares
/// the module); nothing creates these tables now.
pub static LIBRARY_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "library_saved, library_history",
        statements: &[DDL_SAVED, DDL_HISTORY],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "library_prefs",
        statements: &[DDL_PREFS],
    },
];

/// The preference keeping the user's last Studio workspace.
const PREF_WORKSPACE: &str = "workspace";

/// Longest text kept, in bytes.
pub const MAX_TEXT: usize = 64 * 1024;
/// Longest name, in characters.
pub const MAX_NAME: usize = 128;
const MAX_ERROR: usize = 500;
const LANGUAGES: &[&str] = &["sql", "mongo", "redis", "cypher", "gql"];
const CLIENTS: &[&str] = &["studio", "shell", "other"];

// ---- redaction ----

/// Words after which the next string literal is a secret (the shell's
/// list).
const SECRET_WORDS: &[&str] = &[
    "BY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIALS",
    "API_KEY",
    "ACCESS_KEY",
    "SECRET_KEY",
    "SASL_PASSWORD",
    "PWD",
    "AUTH",
];

const TOKEN_PREFIXES: &[&str] = &["sprig_", "sprigat_", "sprigrt_"];

fn looks_like_token(w: &str) -> bool {
    let w = w.trim_matches(|c| c == '\'' || c == '"');
    TOKEN_PREFIXES
        .iter()
        .any(|p| w.starts_with(p) && w.len() > p.len() + 8)
}

/// `text` with its secrets masked; `language` is one of [`LANGUAGES`].
#[must_use]
pub fn redact(text: &str, language: &str) -> String {
    if language == "redis" {
        return redact_redis(text);
    }
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut armed = false;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            let Some(end) = string_end(b, i) else {
                return "-- redacted: a statement with an unclosed string".to_owned();
            };
            let lit = &text[i..end];
            if c != b'`' && (armed || looks_like_token(lit)) {
                out.push_str(if c == b'"' { "\"***\"" } else { "'***'" });
            } else {
                out.push_str(lit);
            }
            if c != b'`' {
                armed = false;
            }
            i = end;
        } else if (c == b'-' && b.get(i + 1) == Some(&b'-'))
            || c == b'#'
            || (c == b'/' && b.get(i + 1) == Some(&b'/'))
        {
            let end = b[i..]
                .iter()
                .position(|&x| x == b'\n')
                .map_or(b.len(), |p| i + p);
            out.push_str(&text[i..end]);
            i = end;
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let end = text[i + 2..].find("*/").map_or(b.len(), |p| i + 2 + p + 2);
            out.push_str(&text[i..end]);
            i = end;
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' {
            let end = b[i..]
                .iter()
                .position(|&x| !(x.is_ascii_alphanumeric() || x == b'_' || x == b'$'))
                .map_or(b.len(), |p| i + p);
            let w = &text[i..end];
            if looks_like_token(w) {
                out.push_str("***");
                armed = false;
            } else {
                let up = w.to_ascii_uppercase();
                armed = SECRET_WORDS.contains(&up.as_str())
                    || (armed && matches!(up.as_str(), "USING" | "WITH"));
                out.push_str(w);
            }
            i = end;
        } else {
            // Spaces, `=`, `:` and `(` keep a secret word armed; anything
            // else ends it.
            if !(c.is_ascii_whitespace() || matches!(c, b'=' | b':' | b'(')) {
                armed = false;
            }
            let n = utf8_len(c);
            out.push_str(&text[i..(i + n).min(b.len())]);
            i += n;
        }
    }
    out
}

fn utf8_len(c: u8) -> usize {
    match c {
        0xF0..=0xFF => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

/// The end (exclusive) of the quoted string starting at `start`, with
/// doubled quotes and backslash escapes; `None` when it isn't closed.
fn string_end(b: &[u8], start: usize) -> Option<usize> {
    let q = b[start];
    let mut i = start + 1;
    while i < b.len() {
        if b[i] == b'\\' && q != b'`' {
            i += 2;
        } else if b[i] == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
            } else {
                return Some(i + 1);
            }
        } else {
            i += 1;
        }
    }
    None
}

fn redact_redis(text: &str) -> String {
    text.lines()
        .map(|line| {
            let words: Vec<&str> = line.split_whitespace().collect();
            let auth = words
                .iter()
                .position(|w| w.eq_ignore_ascii_case("AUTH"))
                .or_else(|| {
                    words
                        .iter()
                        .position(|w| w.eq_ignore_ascii_case("requirepass"))
                        .filter(|_| {
                            words
                                .first()
                                .is_some_and(|w| w.eq_ignore_ascii_case("CONFIG"))
                        })
                });
            match auth {
                Some(at) => words
                    .iter()
                    .enumerate()
                    .map(|(i, w)| if i > at { "***" } else { w })
                    .collect::<Vec<_>>()
                    .join(" "),
                None => words
                    .iter()
                    .map(|w| if looks_like_token(w) { "***" } else { w })
                    .collect::<Vec<_>>()
                    .join(" "),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---- requests ----

/// A save request, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaveReq {
    /// The name.
    pub name: String,
    /// The language.
    pub language: String,
    /// The database, if any.
    pub database: Option<String>,
    /// The redacted text.
    pub text: String,
}

fn language_of(v: &Value) -> Result<String, SprigError> {
    let l = v
        .get("language")
        .and_then(Value::as_str)
        .unwrap_or("sql")
        .to_ascii_lowercase();
    if LANGUAGES.contains(&l.as_str()) {
        Ok(l)
    } else {
        Err(invalid(format!(
            "language must be one of {}",
            LANGUAGES.join(", ")
        )))
    }
}

fn database_of(v: &Value) -> Result<Option<String>, SprigError> {
    match v.get("database").and_then(Value::as_str).map(str::trim) {
        None | Some("") => Ok(None),
        Some(d) if d.chars().count() > 64 => Err(invalid("database is longer than 64 characters")),
        Some(d) => Ok(Some(d.to_owned())),
    }
}

fn text_of(v: &Value, language: &str) -> Result<String, SprigError> {
    let t = v.get("text").and_then(Value::as_str).unwrap_or("").trim();
    if t.is_empty() {
        return Err(invalid("text is empty"));
    }
    if t.len() > MAX_TEXT {
        return Err(SprigError::new(
            SprigCode::RequestTooLarge,
            format!("text is longer than {MAX_TEXT} bytes"),
        ));
    }
    Ok(redact(t, language))
}

/// Checks a save request.
///
/// # Errors
/// A missing or too long name, an unknown language, empty or too long text.
pub fn save_req(v: &Value) -> Result<SaveReq, SprigError> {
    let name = v.get("name").and_then(Value::as_str).unwrap_or("").trim();
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(invalid(format!(
            "a saved query's name must be 1 to {MAX_NAME} characters"
        )));
    }
    let language = language_of(v)?;
    Ok(SaveReq {
        name: name.to_owned(),
        database: database_of(v)?,
        text: text_of(v, &language)?,
        language,
    })
}

/// A run to record, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReq {
    /// The language.
    pub language: String,
    /// The database, if any.
    pub database: Option<String>,
    /// The redacted text.
    pub text: String,
    /// Whether it succeeded.
    pub ok: bool,
    /// How long it took.
    pub ms: u32,
    /// The error, cut to 500 characters and redacted.
    pub error: Option<String>,
    /// `studio`, `shell` or `other`.
    pub client: String,
}

/// Checks a run to record.
///
/// # Errors
/// An unknown language, empty or too long text.
pub fn run_req(v: &Value) -> Result<RunReq, SprigError> {
    let language = language_of(v)?;
    let client = v
        .get("client")
        .and_then(Value::as_str)
        .unwrap_or("other")
        .to_ascii_lowercase();
    let error = v
        .get("error")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| redact(&e.chars().take(MAX_ERROR).collect::<String>(), "sql"));
    Ok(RunReq {
        database: database_of(v)?,
        text: text_of(v, &language)?,
        ok: v
            .get("ok")
            .and_then(Value::as_bool)
            .unwrap_or(error.is_none()),
        ms: v.get("ms").and_then(Value::as_f64).map_or(0, whole_ms),
        error,
        client: if CLIENTS.contains(&client.as_str()) {
            client
        } else {
            "other".to_owned()
        },
        language,
    })
}

/// Checks a preferences request: `{workspace}`, a workspace Studio offers
/// or `null` (or `""`) to forget the choice. Returns the value to keep,
/// `None` to delete it. Which workspaces the user may open isn't checked:
/// a workspace only changes what Studio shows.
///
/// # Errors
/// No known key, or an unknown workspace.
pub fn prefs_req(v: &Value) -> Result<Option<String>, SprigError> {
    let Some(obj) = v.as_object().filter(|o| o.contains_key(PREF_WORKSPACE)) else {
        return Err(invalid("the preferences to keep are {workspace}"));
    };
    if let Some(k) = obj.keys().find(|k| k.as_str() != PREF_WORKSPACE) {
        return Err(invalid(format!("unknown preference {k}")));
    }
    match &obj[PREF_WORKSPACE] {
        Value::Null => Ok(None),
        Value::String(s) if s.trim().is_empty() => Ok(None),
        Value::String(s) => sprig_studio::settings::WORKSPACES
            .iter()
            .find(|w| w.eq_ignore_ascii_case(s.trim()))
            .map(|w| Some((*w).to_owned()))
            .ok_or_else(|| {
                invalid(format!(
                    "workspace must be one of {}",
                    sprig_studio::settings::WORKSPACES.join(", ")
                ))
            }),
        _ => Err(invalid("workspace is a string or null")),
    }
}

/// `m` milliseconds as a whole number, clamped to `u32`.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // clamped first
fn whole_ms(m: f64) -> u32 {
    if m.is_nan() {
        0
    } else {
        m.clamp(0.0, f64::from(u32::MAX)) as u32
    }
}

fn query_u64(query: &str, key: &str) -> Option<u64> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| v.parse().ok()).flatten()
    })
}

// ---- storage ----

/// Saved queries, by user and name.
const SAVED: RowTable = RowTable::new(MODULE, "saved");
/// Runs, by user and the user's own run number (see [`RunWindow`]).
const RUNS: RowTable = RowTable::new(MODULE, "runs");
/// Each user's run window, by user.
const RUN_WINDOWS: RowTable = RowTable::new(MODULE, "run_window");
/// Preferences, by user and name.
const PREFS: RowTable = RowTable::new(MODULE, "prefs");

/// A user's kept runs are numbers `oldest..next`: recording one writes
/// `next` and deletes from `oldest` while more than `studio.history_max`
/// are kept; listing reads the newest `n` by key. Neither reads the other
/// runs.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct RunWindow {
    oldest: u64,
    next: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedRec {
    name: String,
    language: String,
    database: Option<String>,
    text: String,
    saved_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunRec {
    /// The user's run number.
    id: u64,
    language: String,
    database: Option<String>,
    text: String,
    ok: bool,
    ms: u32,
    error: Option<String>,
    client: String,
    at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrefRec {
    value: String,
    updated_ms: i64,
}

struct Ctx {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
    user: u32,
    history_max: u64,
}

impl Ctx {
    async fn begin(&self) -> Result<Box<dyn RecordTx>, SprigError> {
        self.records.begin(self.tenant).await
    }

    fn mine(&self) -> Pk {
        Pk::new().u32(self.user)
    }

    fn named(&self, name: &str) -> Pk {
        Pk::new().u32(self.user).bytes(name.as_bytes())
    }

    fn run(&self, n: u64) -> Pk {
        Pk::new().u32(self.user).u64(n)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn context(core: &Arc<Core>, caller: &Caller) -> Result<Ctx, SprigError> {
    let (Some(svc), Some(tid), Some(user)) = (
        &core.studio.tenants,
        caller.principal.tenant,
        caller.principal.user_id,
    ) else {
        return Err(invalid("saved queries need a tenant login"));
    };
    let history_max = core
        .studio
        .tenant_get(&sprig_studio::settings::HISTORY_MAX, Some(tid), None);
    Ok(Ctx {
        records: Arc::clone(&svc.writers),
        tenant: tid,
        user: user.0,
        history_max,
    })
}

async fn list(c: &Ctx, history: u64) -> Result<Value, SprigError> {
    let mut tx = c.begin().await?;
    let mut saved: Vec<SavedRec> = SAVED.rows(&mut *tx, &c.mine()).await?;
    let mut runs: Vec<RunRec> = Vec::new();
    if history > 0 {
        let w: RunWindow = RUN_WINDOWS
            .get(&mut *tx, &c.mine())
            .await?
            .unwrap_or_default();
        let from = w.oldest.max(w.next.saturating_sub(history));
        for n in (from..w.next).rev() {
            if let Some(r) = RUNS.get(&mut *tx, &c.run(n)).await? {
                runs.push(r);
            }
        }
    }
    drop(tx);
    saved.sort_by(|a, b| b.saved_ms.cmp(&a.saved_ms).then(a.name.cmp(&b.name)));
    Ok(json!({
        "saved": saved.into_iter().map(|r| json!({
            "name": r.name, "language": r.language, "database": r.database, "text": r.text,
            "saved_at_ms": r.saved_ms,
        })).collect::<Vec<_>>(),
        // Newest first.
        "history": runs.into_iter().map(|r| json!({
            "id": r.id.to_string(), "language": r.language, "database": r.database, "text": r.text,
            "ok": r.ok, "ms": r.ms, "error": r.error, "client": r.client, "at_ms": r.at_ms,
        })).collect::<Vec<_>>(),
        "history_max": c.history_max,
    }))
}

async fn save(c: &Ctx, r: &SaveReq) -> Result<(), SprigError> {
    let mut tx = c.begin().await?;
    let row = SavedRec {
        name: r.name.clone(),
        language: r.language.clone(),
        database: r.database.clone(),
        text: r.text.clone(),
        saved_ms: now_ms(),
    };
    SAVED.put(&mut *tx, &c.named(&r.name), &row).await?;
    tx.commit().await
}

async fn delete(c: &Ctx, name: &str) -> Result<bool, SprigError> {
    let mut tx = c.begin().await?;
    let pk = c.named(name);
    let had = SAVED
        .get_for_update::<Value>(&mut *tx, &pk)
        .await?
        .is_some();
    if had {
        SAVED.delete(&mut *tx, &pk).await?;
    }
    tx.commit().await?;
    Ok(had)
}

async fn record(c: &Ctx, r: &RunReq) -> Result<bool, SprigError> {
    if c.history_max == 0 {
        return Ok(false);
    }
    let mut tx = c.begin().await?;
    let mut w: RunWindow = RUN_WINDOWS
        .get_for_update(&mut *tx, &c.mine())
        .await?
        .unwrap_or_default();
    let row = RunRec {
        id: w.next,
        language: r.language.clone(),
        database: r.database.clone(),
        text: r.text.clone(),
        ok: r.ok,
        ms: r.ms,
        error: r.error.clone(),
        client: r.client.clone(),
        at_ms: now_ms(),
    };
    RUNS.put(&mut *tx, &c.run(w.next), &row).await?;
    w.next += 1;
    // Keep the newest `history_max`: delete the oldest by number.
    while w.next - w.oldest > c.history_max {
        RUNS.delete(&mut *tx, &c.run(w.oldest)).await?;
        w.oldest += 1;
    }
    RUN_WINDOWS.put(&mut *tx, &c.mine(), &w).await?;
    tx.commit().await?;
    Ok(true)
}

async fn clear(c: &Ctx) -> Result<(), SprigError> {
    let mut tx = c.begin().await?;
    let mut w: RunWindow = RUN_WINDOWS
        .get_for_update(&mut *tx, &c.mine())
        .await?
        .unwrap_or_default();
    for n in w.oldest..w.next {
        RUNS.delete(&mut *tx, &c.run(n)).await?;
    }
    w.oldest = w.next;
    RUN_WINDOWS.put(&mut *tx, &c.mine(), &w).await?;
    tx.commit().await
}

async fn set_pref(c: &Ctx, name: &str, value: Option<&str>) -> Result<(), SprigError> {
    let mut tx = c.begin().await?;
    let pk = c.named(name);
    match value {
        Some(v) => {
            let row = PrefRec {
                value: v.to_owned(),
                updated_ms: now_ms(),
            };
            PREFS.put(&mut *tx, &pk, &row).await?;
        }
        None => {
            if PREFS
                .get_for_update::<Value>(&mut *tx, &pk)
                .await?
                .is_some()
            {
                PREFS.delete(&mut *tx, &pk).await?;
            }
        }
    }
    tx.commit().await
}

/// The caller's last Studio workspace, for `WhoAmI`. `None` when there is
/// none, for logins without a tenant user, and when the tenant's catalog
/// records can't answer: Studio then picks the default.
pub(super) async fn workspace_pref(core: &Arc<Core>, caller: &Caller) -> Option<String> {
    let c = context(core, caller).ok()?;
    let mut tx = c.begin().await.ok()?;
    let v: Option<PrefRec> = PREFS
        .get(&mut *tx, &c.named(PREF_WORKSPACE))
        .await
        .map_err(|e| tracing::debug!(error = %e, "WhoAmI: the workspace preference wasn't read"))
        .ok()
        .flatten();
    drop(tx);
    v.map(|p| p.value)
        .filter(|w| sprig_studio::settings::WORKSPACES.contains(&w.as_str()))
}

// ---- routes ----

/// Whether `path` is one of the library's.
#[must_use]
pub fn is_library_path(path: &str) -> bool {
    path == "/v1/library" || path.starts_with("/v1/library/")
}

/// Answers `/v1/library...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    let body = if method == "POST" {
        parse_json(&read_body(req).await?)?
    } else {
        json!({})
    };
    match (method.as_str(), path.as_str()) {
        ("GET", "/v1/library") => {
            let c = context(core, &caller)?;
            let n = query_u64(&query, "history")
                .unwrap_or(100)
                .min(c.history_max);
            Ok(json_response(200, &list(&c, n).await?))
        }
        ("POST", "/v1/library/saved") => {
            let r = save_req(&body)?;
            let c = context(core, &caller)?;
            save(&c, &r).await?;
            Ok(json_response(
                200,
                &json!({"name": r.name, "language": r.language, "database": r.database, "text": r.text}),
            ))
        }
        ("POST", "/v1/library/saved:delete") => {
            let name = body
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                return Err(invalid("name is empty"));
            }
            let c = context(core, &caller)?;
            let deleted = delete(&c, name).await?;
            Ok(json_response(200, &json!({"deleted": deleted})))
        }
        ("POST", "/v1/library/history") => {
            let r = run_req(&body)?;
            let c = context(core, &caller)?;
            let recorded = record(&c, &r).await?;
            Ok(json_response(200, &json!({"recorded": recorded})))
        }
        ("POST", "/v1/library/history:clear") => {
            let c = context(core, &caller)?;
            clear(&c).await?;
            Ok(json_response(200, &json!({})))
        }
        ("POST", "/v1/library/prefs") => {
            let workspace = prefs_req(&body)?;
            let c = context(core, &caller)?;
            set_pref(&c, PREF_WORKSPACE, workspace.as_deref()).await?;
            Ok(json_response(200, &json!({ "workspace": workspace })))
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_masked() {
        assert_eq!(
            redact("CREATE USER bob IDENTIFIED BY 'hunter2'", "sql"),
            "CREATE USER bob IDENTIFIED BY '***'"
        );
        assert_eq!(
            redact(
                "ALTER USER bob IDENTIFIED VIA ed25519 USING PASSWORD('x''y')",
                "sql"
            ),
            "ALTER USER bob IDENTIFIED VIA ed25519 USING PASSWORD('***')"
        );
        assert_eq!(
            redact("SELECT 'by' FROM t WHERE a = 'open'", "sql"),
            "SELECT 'by' FROM t WHERE a = 'open'"
        );
        assert_eq!(
            redact(
                "CREATE CONNECTION c WITH (password = 'p', host = 'h')",
                "sql"
            ),
            "CREATE CONNECTION c WITH (password = '***', host = 'h')"
        );
        assert_eq!(
            redact(
                "db.users.insertOne({name: \"a\", password: \"pw\"})",
                "mongo"
            ),
            "db.users.insertOne({name: \"a\", password: \"***\"})"
        );
        assert_eq!(
            redact("SELECT 'sprigat_0123456789abcdef'", "sql"),
            "SELECT '***'"
        );
        assert_eq!(
            redact("SELECT * FROM t -- by 'x'\nWHERE b = 'é'", "sql"),
            "SELECT * FROM t -- by 'x'\nWHERE b = 'é'"
        );
        assert!(redact("CREATE USER a IDENTIFIED BY 'oops", "sql").starts_with("-- redacted"));
        assert_eq!(redact("AUTH alice s3cret", "redis"), "AUTH *** ***");
        assert_eq!(
            redact("CONFIG SET requirepass x", "redis"),
            "CONFIG SET requirepass ***"
        );
        assert_eq!(redact("GET k", "redis"), "GET k");
    }

    #[test]
    fn requests_are_checked() {
        let r = save_req(&json!({"name": " open orders ", "text": "SELECT 1", "database": "shop"}))
            .expect("ok");
        assert_eq!(r.name, "open orders");
        assert_eq!(r.language, "sql");
        assert_eq!(r.database.as_deref(), Some("shop"));
        assert!(save_req(&json!({"name": "", "text": "SELECT 1"})).is_err());
        assert!(save_req(&json!({"name": "x".repeat(129), "text": "SELECT 1"})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "  "})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "x", "language": "cobol"})).is_err());
        assert!(save_req(&json!({"name": "a", "text": "x".repeat(MAX_TEXT + 1)})).is_err());

        let r =
            run_req(&json!({"text": "SELECT 1", "ms": 12.6, "client": "shell", "language": "SQL"}))
                .expect("ok");
        assert!(r.ok);
        assert_eq!(
            (r.ms, r.client.as_str(), r.language.as_str()),
            (12, "shell", "sql")
        );
        let r = run_req(&json!({"text": "SELEC 1", "error": "syntax", "client": "x"})).expect("ok");
        assert!(!r.ok);
        assert_eq!(r.client, "other");
        assert_eq!(r.error.as_deref(), Some("syntax"));
    }

    #[test]
    fn preferences_are_checked() {
        assert_eq!(
            prefs_req(&json!({"workspace": " Developer "})).expect("ok"),
            Some("developer".to_owned())
        );
        assert_eq!(prefs_req(&json!({"workspace": null})).expect("ok"), None);
        assert_eq!(prefs_req(&json!({"workspace": ""})).expect("ok"), None);
        // The operator workspace is entered through operator mode, not kept.
        assert!(prefs_req(&json!({"workspace": "operator"})).is_err());
        assert!(prefs_req(&json!({"workspace": "wizard"})).is_err());
        assert!(prefs_req(&json!({"workspace": 3})).is_err());
        assert!(prefs_req(&json!({})).is_err());
        assert!(prefs_req(&json!({"workspace": "analyst", "theme": "dark"})).is_err());
        assert_eq!(LIBRARY_MIGRATIONS.last().map(|m| m.format), Some(2));
        assert!(DDL_PREFS.contains("library_prefs"));
    }

    #[test]
    fn paths_and_queries() {
        assert!(is_library_path("/v1/library"));
        assert!(is_library_path("/v1/library/saved:delete"));
        assert!(!is_library_path("/v1/libraryx"));
        assert_eq!(query_u64("history=20&x=1", "history"), Some(20));
        assert_eq!(query_u64("x=1", "history"), None);
    }

    /// Saved queries, runs and preferences live in the catalog records,
    /// one user's never another's, the newest `history_max` runs kept.
    #[tokio::test]
    async fn rows_live_in_records() {
        let records: Arc<dyn RecordStore> =
            Arc::new(sprig_catalog::records::mem::MemRecords::new());
        let ctx = |user: u32| Ctx {
            records: Arc::clone(&records),
            tenant: TenantId(7),
            user,
            history_max: 3,
        };
        let (alice, bob) = (ctx(1), ctx(2));
        let query = |name: &str, text: &str| {
            save_req(&json!({"name": name, "text": text, "database": "shop"})).expect("ok")
        };
        save(&alice, &query("open", "SELECT 1")).await.unwrap();
        save(&alice, &query("open", "SELECT 2")).await.unwrap();
        save(&alice, &query("x\0y", "SELECT 3")).await.unwrap();
        save(&bob, &query("open", "SELECT 9")).await.unwrap();
        let listed = list(&alice, 10).await.unwrap();
        let saved = listed["saved"].as_array().unwrap();
        assert_eq!(saved.len(), 2);
        assert!(
            saved
                .iter()
                .any(|r| r["name"] == "open" && r["text"] == "SELECT 2")
        );
        assert_eq!(
            list(&bob, 10).await.unwrap()["saved"][0]["text"],
            "SELECT 9"
        );
        assert!(delete(&alice, "open").await.unwrap());
        assert!(!delete(&alice, "open").await.unwrap());
        assert_eq!(
            list(&bob, 10).await.unwrap()["saved"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        for n in 0..5 {
            let run = run_req(&json!({"text": format!("SELECT {n}"), "ms": 1})).expect("ok");
            assert!(record(&alice, &run).await.unwrap());
        }
        let hist = list(&alice, 10).await.unwrap();
        let texts: Vec<&str> = hist["history"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| r["text"].as_str())
            .collect();
        assert_eq!(texts, ["SELECT 4", "SELECT 3", "SELECT 2"]);
        // Only the kept runs are stored.
        let mut tx = alice.begin().await.unwrap();
        let kept: Vec<(Pk, Value)> = RUNS.scan(&mut *tx, &alice.mine()).await.unwrap();
        assert_eq!(kept.len(), 3);
        drop(tx);
        assert_eq!(
            list(&alice, 1).await.unwrap()["history"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(
            list(&bob, 10).await.unwrap()["history"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        clear(&alice).await.unwrap();
        assert!(
            list(&alice, 10).await.unwrap()["history"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        set_pref(&alice, PREF_WORKSPACE, Some("developer"))
            .await
            .unwrap();
        let mut tx = alice.begin().await.unwrap();
        let pref: Option<PrefRec> = PREFS
            .get(&mut *tx, &alice.named(PREF_WORKSPACE))
            .await
            .unwrap();
        assert_eq!(pref.map(|p| p.value).as_deref(), Some("developer"));
        drop(tx);
        set_pref(&alice, PREF_WORKSPACE, None).await.unwrap();
        set_pref(&alice, PREF_WORKSPACE, None).await.unwrap();
        let mut tx = alice.begin().await.unwrap();
        let pref: Option<PrefRec> = PREFS
            .get(&mut *tx, &alice.named(PREF_WORKSPACE))
            .await
            .unwrap();
        assert!(pref.is_none());
    }
}
