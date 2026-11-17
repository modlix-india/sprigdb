//! Dashboards: tiles of charts over read-only queries, for one user or
//! shared with the whole tenant, in the tenant's catalog (`t<id>_sprig`,
//! module `dashboards`, created the first time a gateway needs it).
//!
//! - **Whose.** A dashboard belongs to the user who made it. Shared, every
//!   user of the tenant sees it. Its owner edits it; a shared one may also
//!   be edited or deleted by the tenant's administrators
//!   (`SPRIG_TENANT_ADMIN`). Only the owner shares or unshares it. A
//!   dashboard someone can't see answers as if it didn't exist.
//! - **Tiles** hold a title, a database, one `SELECT`, a chart spec (the
//!   same spec the data assistant's `propose_tile` writes, checked by
//!   [`check_chart`](crate::tools::builtin::check_chart)), a refresh
//!   interval and a place on a 12-column grid.
//! - **Running a tile** runs its query as the viewer, never as the owner:
//!   the [`TileRunner`] is built for the signed-in caller, so their grants
//!   and row-level security apply. Two people looking at one shared
//!   dashboard can see different numbers.
//! - **Filters** are named values on the dashboard. A tile's query names
//!   one as `{{name}}`, outside quotes; [`bind`] puts the value in as a
//!   literal of the filter's kind (a number, a quoted string or date, a
//!   list for `IN (...)`), escaped, and the result must still be one
//!   read. A viewer may pass other values for a run; only an editor saves
//!   them.
//! - **The assistant's tiles.** Accepting a `propose_tile` proposal
//!   ([`accept_tile`]) adds it to a dashboard the user picks, or to one of
//!   their own by the name the assistant gave, made if missing.

use sprig_catalog::pool_wait::Bounded as _;
use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_common::ids::UserId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OnceCell;

use crate::tools::ResultSet;
use crate::tools::builtin::check_chart;

#[cfg(test)]
mod tests;

/// The catalog module.
pub const MODULE: &str = "dashboards";

/// Grid columns.
pub const GRID_COLUMNS: u32 = 12;
/// The longest dashboard name, in characters.
pub const MAX_NAME: usize = 128;
/// The longest tile title, in characters.
pub const MAX_TITLE: usize = 200;
/// The longest query or chart spec, in bytes.
pub const MAX_TEXT: usize = 64 * 1024;
/// Filters per dashboard.
pub const MAX_FILTERS: usize = 20;
/// Tiles per dashboard.
pub const MAX_TILES: usize = 50;
/// Dashboards one user owns.
pub const MAX_OWNED: usize = 200;
/// Where an accepted tile goes when neither the user nor the assistant
/// named a dashboard: the caller's own dashboard of this name.
pub const DEFAULT_DASHBOARD: &str = "My dashboard";
/// Shortest automatic refresh, in seconds (0 is "only when asked").
pub const MIN_REFRESH: u32 = 30;
/// Longest automatic refresh, in seconds.
pub const MAX_REFRESH: u32 = 86_400;
/// Values in one list filter.
pub const MAX_LIST: usize = 200;
/// The longest text filter value, in characters.
pub const MAX_VALUE: usize = 1000;

const DDL_DASHBOARDS: &str = "CREATE TABLE IF NOT EXISTS dashboards (
  dash_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  owner_id   INT UNSIGNED NOT NULL,
  owner      VARCHAR(64) NOT NULL,
  shared     TINYINT(1) NOT NULL,
  name       VARCHAR(128) NOT NULL,
  filters    JSON NOT NULL,
  layout     JSON NOT NULL,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  KEY by_owner (owner_id),
  KEY by_shared (shared)
) ENGINE=InnoDB";

const DDL_TILES: &str = "CREATE TABLE IF NOT EXISTS dashboard_tiles (
  tile_id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  dash_id         BIGINT UNSIGNED NOT NULL,
  title           VARCHAR(200) NOT NULL,
  db_name         VARCHAR(64) NULL,
  sql_text        MEDIUMTEXT NOT NULL,
  chart           JSON NOT NULL,
  refresh_seconds INT UNSIGNED NOT NULL,
  pos             JSON NOT NULL,
  KEY by_dash (dash_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static DASHBOARD_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "dashboards, dashboard_tiles",
    statements: &[DDL_DASHBOARDS, DDL_TILES],
}];

/// The module, for migrators.
pub const DASHBOARD_MODULE: Module = Module {
    name: MODULE,
    migrations: DASHBOARD_MIGRATIONS,
};

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn denied(m: &str) -> SprigError {
    SprigError::standard(1227, *b"42000", format!("Access denied; {m}"))
}

fn missing(id: u64) -> SprigError {
    invalid(format!(
        "dashboard {id} isn't yours or shared with you, or doesn't exist"
    ))
}

// ---- types ----

/// The signed-in user, as the routes see them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Who {
    /// Their id in the tenant.
    pub user: UserId,
    /// Their user name.
    pub name: String,
    /// `SPRIG_TENANT_ADMIN`.
    pub admin: bool,
}

/// A tile's place on the 12-column grid, in columns and rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pos {
    /// The first column, from 0.
    pub x: u32,
    /// The first row, from 0.
    pub y: u32,
    /// Columns wide, 1 to 12.
    pub w: u32,
    /// Rows high, 1 to 24.
    pub h: u32,
}

impl Pos {
    /// From JSON, checked.
    ///
    /// # Errors
    /// A place off the grid.
    pub fn from_json(v: &Value) -> Result<Self> {
        let n = |k: &str| {
            v.get(k)
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| invalid(format!("pos.{k} is a whole number")))
        };
        let p = Pos {
            x: n("x")?,
            y: n("y")?,
            w: n("w")?,
            h: n("h")?,
        };
        if p.w == 0 || p.w > GRID_COLUMNS || p.x + p.w > GRID_COLUMNS {
            return Err(invalid(format!(
                "a tile is 1 to {GRID_COLUMNS} columns wide and inside the grid"
            )));
        }
        if p.h == 0 || p.h > 24 || p.y > 10_000 {
            return Err(invalid("a tile is 1 to 24 rows high"));
        }
        Ok(p)
    }
}

/// The kinds of filter value.
pub const FILTER_KINDS: &[&str] = &["text", "number", "date", "list"];

/// A dashboard-wide filter: a named value tiles' queries use as
/// `{{name}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    /// The name in queries.
    pub name: String,
    /// What the filter bar shows.
    pub label: String,
    /// `text`, `number`, `date` or `list`.
    pub kind: String,
    /// The saved value; `null` binds `NULL`.
    #[serde(default)]
    pub value: Value,
}

fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Filters from JSON, checked.
///
/// # Errors
/// More than [`MAX_FILTERS`], a bad name, kind or value, a name twice.
pub fn filters_from_json(v: &Value) -> Result<Vec<Filter>> {
    let Some(a) = v.as_array() else {
        return Err(invalid("filters is a list of {name, label, kind, value}"));
    };
    if a.len() > MAX_FILTERS {
        return Err(invalid(format!(
            "a dashboard has at most {MAX_FILTERS} filters"
        )));
    }
    let mut out: Vec<Filter> = Vec::new();
    for f in a {
        let s = |k: &str| f.get(k).and_then(Value::as_str).map_or("", str::trim);
        let name = s("name");
        if !is_name(name) {
            return Err(invalid(format!(
                "a filter's name is a letter or _ then letters, digits or _, at most 64: {name:?}"
            )));
        }
        if out.iter().any(|o| o.name.eq_ignore_ascii_case(name)) {
            return Err(invalid(format!("two filters are named {name}")));
        }
        let kind = if s("kind").is_empty() {
            "text"
        } else {
            s("kind")
        };
        if !FILTER_KINDS.contains(&kind) {
            return Err(invalid(format!(
                "a filter's kind is one of {}",
                FILTER_KINDS.join(", ")
            )));
        }
        let label = if s("label").is_empty() {
            name
        } else {
            s("label")
        };
        let filter = Filter {
            name: name.to_owned(),
            label: label.chars().take(64).collect(),
            kind: kind.to_owned(),
            value: f.get("value").cloned().unwrap_or(Value::Null),
        };
        literal(&filter, &filter.value)?;
        out.push(filter);
    }
    Ok(out)
}

/// A tile as stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tile {
    /// Its id.
    pub id: u64,
    /// Its title.
    pub title: String,
    /// The database its query runs in.
    pub database: Option<String>,
    /// One `SELECT`, which may name filters as `{{name}}`.
    pub sql: String,
    /// The chart spec.
    pub chart: Value,
    /// Seconds between automatic runs; 0 is only when asked.
    pub refresh_seconds: u32,
    /// Its place.
    pub pos: Pos,
}

/// A dashboard as stored, with its tiles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Dashboard {
    /// Its id.
    pub id: u64,
    /// Its name.
    pub name: String,
    /// Its owner's id.
    pub owner_id: u32,
    /// Its owner's user name.
    pub owner: String,
    /// Seen by the whole tenant.
    pub shared: bool,
    /// Its filters.
    pub filters: Vec<Filter>,
    /// Grid settings (`{"columns": 12}`).
    pub layout: Value,
    /// Last change, ms since the epoch.
    pub updated_ms: i64,
    /// Its tiles, top to bottom.
    pub tiles: Vec<Tile>,
}

/// A dashboard in a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// Its id.
    pub id: u64,
    /// Its name.
    pub name: String,
    /// Its owner's id.
    pub owner_id: u32,
    /// Its owner's user name.
    pub owner: String,
    /// Seen by the whole tenant.
    pub shared: bool,
    /// Tiles on it.
    pub tiles: u32,
    /// Last change, ms since the epoch.
    pub updated_ms: i64,
}

/// A tile to store, checked.
#[derive(Debug, Clone, PartialEq)]
pub struct TileReq {
    /// Its title.
    pub title: String,
    /// The database.
    pub database: Option<String>,
    /// The query.
    pub sql: String,
    /// The chart spec.
    pub chart: Value,
    /// Seconds between runs.
    pub refresh_seconds: u32,
    /// Its place, or the next free row.
    pub pos: Option<Pos>,
}

fn text<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).map_or("", str::trim)
}

/// A chart spec from JSON: an object, or JSON text holding one (as
/// `propose_tile` writes it).
///
/// # Errors
/// Anything else.
pub fn chart_of(v: Option<&Value>) -> Result<Value> {
    match v {
        Some(Value::Object(_)) => Ok(v.cloned().unwrap_or(Value::Null)),
        Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
            Ok(o @ Value::Object(_)) => Ok(o),
            _ => Err(invalid("chart is a chart spec object, or JSON text of one")),
        },
        _ => Err(invalid("a tile needs a chart spec")),
    }
}

impl TileReq {
    /// From a JSON body `{title, database, sql, chart, refresh_seconds,
    /// pos}`, checked: the query is one read (with every `{{filter}}` as
    /// `NULL`), the chart spec fits it.
    ///
    /// # Errors
    /// What is wrong.
    pub fn from_json(v: &Value) -> Result<Self> {
        let title = text(v, "title");
        if title.is_empty() || title.chars().count() > MAX_TITLE {
            return Err(invalid(format!(
                "a tile's title is 1 to {MAX_TITLE} characters"
            )));
        }
        let database = match text(v, "database") {
            "" => match text(v, "db") {
                "" => None,
                d => Some(d),
            },
            d => Some(d),
        };
        if database.is_some_and(|d| d.chars().count() > 64) {
            return Err(invalid("database is longer than 64 characters"));
        }
        let sql = text(v, "sql").trim_end_matches(';').trim();
        if sql.is_empty() {
            return Err(invalid("a tile needs a query"));
        }
        if sql.len() > MAX_TEXT {
            return Err(invalid(format!(
                "the query is longer than {MAX_TEXT} bytes"
            )));
        }
        check_query(sql)?;
        let chart = chart_of(v.get("chart"))?;
        let chart_text = chart.to_string();
        if chart_text.len() > MAX_TEXT {
            return Err(invalid(format!(
                "the chart spec is longer than {MAX_TEXT} bytes"
            )));
        }
        check_chart(&chart_text, sql).map_err(invalid)?;
        let refresh = v
            .get("refresh_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let refresh_seconds = u32::try_from(refresh).unwrap_or(u32::MAX);
        if refresh_seconds != 0 && !(MIN_REFRESH..=MAX_REFRESH).contains(&refresh_seconds) {
            return Err(invalid(format!(
                "refresh_seconds is 0 (only when asked) or {MIN_REFRESH} to {MAX_REFRESH}"
            )));
        }
        let pos = match v.get("pos") {
            None | Some(Value::Null) => None,
            Some(p) => Some(Pos::from_json(p)?),
        };
        Ok(TileReq {
            title: title.to_owned(),
            database: database.map(str::to_owned),
            sql: sql.to_owned(),
            chart,
            refresh_seconds,
            pos,
        })
    }
}

/// The query is one read once its filters are bound (to `NULL`).
fn check_query(sql: &str) -> Result<()> {
    let names = placeholders(sql)?;
    let nulls: Vec<Filter> = names
        .iter()
        .map(|n| Filter {
            name: n.clone(),
            label: n.clone(),
            kind: "text".to_owned(),
            value: Value::Null,
        })
        .collect();
    let bound = bind(sql, &nulls, &Map::new())?;
    crate::sql_guard::read_only(&bound)
        .map_err(|e| invalid(format!("a tile's query is one SELECT: {e}")))
}

// ---- filters in queries ----

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lex {
    Code,
    Quote(u8),
    Line,
    Block,
}

/// Walks `sql`, calling `found(start, end, name, in_code)` for each
/// `{{name}}` (end exclusive).
fn scan(sql: &str, mut found: impl FnMut(usize, usize, &str, bool) -> Result<()>) -> Result<()> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut st = Lex::Code;
    while i < b.len() {
        let c = b[i];
        if c == b'{'
            && b.get(i + 1) == Some(&b'{')
            && let Some(close) = sql[i + 2..].find("}}")
            && is_name(sql[i + 2..i + 2 + close].trim())
        {
            let inner = sql[i + 2..i + 2 + close].trim();
            let end = i + 2 + close + 2;
            if st != Lex::Line && st != Lex::Block {
                found(i, end, inner, st == Lex::Code)?;
            }
            i = end;
            continue;
        }
        match st {
            Lex::Code => {
                if c == b'\'' || c == b'"' || c == b'`' {
                    st = Lex::Quote(c);
                } else if (c == b'-' && b.get(i + 1) == Some(&b'-')) || c == b'#' {
                    st = Lex::Line;
                } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
                    st = Lex::Block;
                    i += 1;
                }
            }
            Lex::Quote(q) => {
                if c == b'\\' && q != b'`' {
                    i += 1;
                } else if c == q {
                    if b.get(i + 1) == Some(&q) {
                        i += 1;
                    } else {
                        st = Lex::Code;
                    }
                }
            }
            Lex::Line => {
                if c == b'\n' {
                    st = Lex::Code;
                }
            }
            Lex::Block => {
                if c == b'*' && b.get(i + 1) == Some(&b'/') {
                    st = Lex::Code;
                    i += 1;
                }
            }
        }
        i += 1;
    }
    Ok(())
}

fn quoted_placeholder(name: &str) -> SprigError {
    invalid(format!(
        "{{{{{name}}}}} is inside quotes or a quoted name; write it bare (WHERE day >= {{{{{name}}}}}): SprigDB quotes the value itself"
    ))
}

/// The filter names `sql` uses, in order of first use.
///
/// # Errors
/// A filter inside quotes.
pub fn placeholders(sql: &str) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    scan(sql, |_, _, name, code| {
        if !code {
            return Err(quoted_placeholder(name));
        }
        if !out.iter().any(|o| o == name) {
            out.push(name.to_owned());
        }
        Ok(())
    })?;
    Ok(out)
}

fn quote_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn number(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i.to_string())
            } else if let Some(u) = n.as_u64() {
                Some(u.to_string())
            } else {
                n.as_f64().filter(|f| f.is_finite()).map(|f| f.to_string())
            }
        }
        Value::String(s) => {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                Some(i.to_string())
            } else {
                t.parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .map(|f| f.to_string())
            }
        }
        _ => None,
    }
}

fn is_date(text: &str) -> bool {
    let all_digits = |bytes: &[u8], range: std::ops::Range<usize>| {
        bytes
            .get(range)
            .is_some_and(|x| x.iter().all(u8::is_ascii_digit))
    };
    let day = text.as_bytes();
    if day.len() < 10
        || !all_digits(day, 0..4)
        || day[4] != b'-'
        || !all_digits(day, 5..7)
        || day[7] != b'-'
        || !all_digits(day, 8..10)
    {
        return false;
    }
    let rest = &text[10..];
    if rest.is_empty() {
        return true;
    }
    // ` HH:MM`, then `:SS`, then `.ffffff`.
    if !(rest.starts_with(' ') || rest.starts_with('T')) || rest.len() < 6 {
        return false;
    }
    let time = &rest.as_bytes()[1..];
    if !(all_digits(time, 0..2) && time[2] == b':' && all_digits(time, 3..5)) {
        return false;
    }
    match time.len() {
        5 => true,
        8 => time[5] == b':' && all_digits(time, 6..8),
        len if len > 9 && len <= 15 => {
            time[5] == b':' && all_digits(time, 6..8) && time[8] == b'.' && all_digits(time, 9..len)
        }
        _ => false,
    }
}

/// The SQL literal for `value` as `f`'s kind.
///
/// # Errors
/// A value that isn't of the filter's kind.
pub fn literal(f: &Filter, value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok("NULL".to_owned());
    }
    let bad = || {
        invalid(format!(
            "filter {} is a {} filter; {value} isn't one",
            f.name, f.kind
        ))
    };
    match f.kind.as_str() {
        "number" => number(value).ok_or_else(bad),
        "date" => {
            let s = value.as_str().map(str::trim).ok_or_else(bad)?;
            if is_date(s) {
                Ok(quote_text(&s.replace('T', " ")))
            } else {
                Err(invalid(format!(
                    "filter {} is a date: YYYY-MM-DD, or with a time HH:MM[:SS[.ffffff]]",
                    f.name
                )))
            }
        }
        "list" => {
            let a = value.as_array().ok_or_else(bad)?;
            if a.len() > MAX_LIST {
                return Err(invalid(format!(
                    "filter {} holds at most {MAX_LIST} values",
                    f.name
                )));
            }
            if a.is_empty() {
                return Ok("(NULL)".to_owned());
            }
            let items = a
                .iter()
                .map(|x| match x {
                    Value::Number(_) => number(x).ok_or_else(bad),
                    Value::String(s) if s.chars().count() <= MAX_VALUE => Ok(quote_text(s)),
                    Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_owned()),
                    _ => Err(bad()),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", items.join(", ")))
        }
        _ => {
            let s = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return Err(bad()),
            };
            if s.chars().count() > MAX_VALUE {
                return Err(invalid(format!(
                    "filter {} is at most {MAX_VALUE} characters",
                    f.name
                )));
            }
            Ok(quote_text(&s))
        }
    }
}

/// `sql` with each `{{name}}` replaced by its filter's value: from
/// `params` when the caller passed one, else the saved value. Values
/// become literals ([`literal`]); nothing in them is read as SQL.
///
/// # Errors
/// A filter inside quotes, a name with no filter, a value of the wrong
/// kind.
pub fn bind(sql: &str, filters: &[Filter], params: &Map<String, Value>) -> Result<String> {
    let mut out = String::with_capacity(sql.len());
    let mut from = 0;
    scan(sql, |start, end, name, code| {
        if !code {
            return Err(quoted_placeholder(name));
        }
        let f = filters
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                invalid(format!(
                    "the query uses {{{{{name}}}}}, but the dashboard has no filter named {name}: add one"
                ))
            })?;
        let v = params
            .get(&f.name)
            .or_else(|| params.get(name))
            .unwrap_or(&f.value);
        out.push_str(&sql[from..start]);
        out.push_str(&literal(f, v)?);
        from = end;
        Ok(())
    })?;
    out.push_str(&sql[from..]);
    Ok(out)
}

// ---- storage ----

/// Where dashboards are kept. It checks nothing about who asks: the
/// functions below do.
#[async_trait]
pub trait DashStore: Send + Sync {
    /// The user's own dashboards and every shared one.
    async fn list(&self, user: UserId) -> Result<Vec<Header>>;
    /// One dashboard with its tiles.
    async fn get(&self, id: u64) -> Result<Option<Dashboard>>;
    /// A new dashboard; its id.
    async fn create(
        &self,
        owner: UserId,
        owner_name: &str,
        name: &str,
        shared: bool,
        filters: &[Filter],
    ) -> Result<u64>;
    /// Saves a dashboard's name, sharing, filters and layout (not tiles).
    async fn update(&self, d: &Dashboard) -> Result<()>;
    /// Deletes a dashboard and its tiles.
    async fn delete(&self, id: u64) -> Result<()>;
    /// Adds (`tile` `None`) or replaces a tile; its id.
    async fn put_tile(&self, dash: u64, tile: Option<u64>, t: &TileReq, pos: Pos) -> Result<u64>;
    /// Moves tiles.
    async fn move_tiles(&self, dash: u64, moves: &[(u64, Pos)]) -> Result<()>;
    /// Deletes a tile; whether it was there.
    async fn delete_tile(&self, dash: u64, tile: u64) -> Result<bool>;
}

/// Runs a tile's query for one signed-in caller, with that caller's
/// rights, read-only and capped.
#[async_trait]
pub trait TileRunner: Send + Sync {
    /// Runs `sql` (one read, filters bound) in `db`.
    async fn run(&self, db: &str, sql: &str) -> Result<ResultSet>;
}

/// In memory, for tests.
#[derive(Debug, Default)]
pub struct MemoryDashboards {
    inner: Mutex<(u64, BTreeMap<u64, Dashboard>)>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[async_trait]
impl DashStore for MemoryDashboards {
    async fn list(&self, user: UserId) -> Result<Vec<Header>> {
        let g = lock(&self.inner);
        Ok(g.1
            .values()
            .filter(|d| d.owner_id == user.0 || d.shared)
            .map(|d| Header {
                id: d.id,
                name: d.name.clone(),
                owner_id: d.owner_id,
                owner: d.owner.clone(),
                shared: d.shared,
                tiles: u32::try_from(d.tiles.len()).unwrap_or(u32::MAX),
                updated_ms: d.updated_ms,
            })
            .collect())
    }
    async fn get(&self, id: u64) -> Result<Option<Dashboard>> {
        Ok(lock(&self.inner).1.get(&id).cloned())
    }
    async fn create(
        &self,
        owner: UserId,
        owner_name: &str,
        name: &str,
        shared: bool,
        filters: &[Filter],
    ) -> Result<u64> {
        let mut g = lock(&self.inner);
        g.0 += 1;
        let id = g.0;
        g.1.insert(
            id,
            Dashboard {
                id,
                name: name.to_owned(),
                owner_id: owner.0,
                owner: owner_name.to_owned(),
                shared,
                filters: filters.to_vec(),
                layout: json!({"columns": GRID_COLUMNS}),
                updated_ms: now_ms(),
                tiles: Vec::new(),
            },
        );
        Ok(id)
    }
    async fn update(&self, d: &Dashboard) -> Result<()> {
        let mut g = lock(&self.inner);
        if let Some(have) = g.1.get_mut(&d.id) {
            have.name.clone_from(&d.name);
            have.shared = d.shared;
            have.filters.clone_from(&d.filters);
            have.layout.clone_from(&d.layout);
            have.updated_ms = now_ms();
        }
        Ok(())
    }
    async fn delete(&self, id: u64) -> Result<()> {
        lock(&self.inner).1.remove(&id);
        Ok(())
    }
    async fn put_tile(&self, dash: u64, tile: Option<u64>, t: &TileReq, pos: Pos) -> Result<u64> {
        let mut g = lock(&self.inner);
        g.0 += 1;
        let fresh = g.0;
        let d = g.1.get_mut(&dash).ok_or_else(|| missing(dash))?;
        let id = tile.unwrap_or(fresh);
        let row = Tile {
            id,
            title: t.title.clone(),
            database: t.database.clone(),
            sql: t.sql.clone(),
            chart: t.chart.clone(),
            refresh_seconds: t.refresh_seconds,
            pos,
        };
        match d.tiles.iter_mut().find(|x| x.id == id) {
            Some(x) => *x = row,
            None => d.tiles.push(row),
        }
        d.updated_ms = now_ms();
        Ok(id)
    }
    async fn move_tiles(&self, dash: u64, moves: &[(u64, Pos)]) -> Result<()> {
        let mut g = lock(&self.inner);
        if let Some(d) = g.1.get_mut(&dash) {
            for (id, p) in moves {
                if let Some(t) = d.tiles.iter_mut().find(|t| t.id == *id) {
                    t.pos = *p;
                }
            }
        }
        Ok(())
    }
    async fn delete_tile(&self, dash: u64, tile: u64) -> Result<bool> {
        let mut g = lock(&self.inner);
        let Some(d) = g.1.get_mut(&dash) else {
            return Ok(false);
        };
        let n = d.tiles.len();
        d.tiles.retain(|t| t.id != tile);
        Ok(d.tiles.len() < n)
    }
}

/// In the tenant's catalog.
pub struct CatalogDashboards {
    pool: mysql_async::Pool,
    schema: String,
    ready: OnceCell<()>,
}

impl std::fmt::Debug for CatalogDashboards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDashboards")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

type DashRow = (u64, String, u32, String, bool, String, String, i64);
type TileRow = (u64, String, Option<String>, String, String, u32, String);

impl CatalogDashboards {
    /// The dashboards in `schema` (`t<id>_sprig`), reached through `pool`
    /// (the catalog writer's).
    #[must_use]
    pub fn new(pool: mysql_async::Pool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            ready: OnceCell::new(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(table))
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        let mut conn = self.pool.get_conn_bounded().await.map_err(unavailable)?;
        self.ready
            .get_or_try_init(|| async {
                let have: Option<u64> = conn
                    .exec_first(
                        "SELECT COUNT(*) FROM information_schema.TABLES \
                         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'dashboard_tiles'",
                        (self.schema.as_str(),),
                    )
                    .await
                    .map_err(unavailable)?;
                if have.unwrap_or(0) == 0 {
                    Migrator::new(&[DASHBOARD_MODULE])?
                        .run(&mut conn, &self.schema, &MigrateOpts::existing())
                        .await?;
                }
                Ok::<(), SprigError>(())
            })
            .await?;
        Ok(conn)
    }
}

fn json_text(v: &impl Serialize) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_owned())
}

#[async_trait]
impl DashStore for CatalogDashboards {
    async fn list(&self, user: UserId) -> Result<Vec<Header>> {
        let mut c = self.conn().await?;
        let rows: Vec<(u64, String, u32, String, bool, i64, u64)> = c
            .exec(
                format!(
                    "SELECT d.dash_id, d.name, d.owner_id, d.owner, d.shared != 0, \
                     CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', d.updated_at) DIV 1000 AS SIGNED), \
                     (SELECT COUNT(*) FROM {} t WHERE t.dash_id = d.dash_id) \
                     FROM {} d WHERE d.owner_id = ? OR d.shared = 1 ORDER BY d.name, d.dash_id",
                    self.t("dashboard_tiles"),
                    self.t("dashboards")
                ),
                (user.0,),
            )
            .await
            .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|(id, name, owner_id, owner, shared, at, tiles)| Header {
                id,
                name,
                owner_id,
                owner,
                shared,
                tiles: u32::try_from(tiles).unwrap_or(u32::MAX),
                updated_ms: at,
            })
            .collect())
    }

    async fn get(&self, id: u64) -> Result<Option<Dashboard>> {
        let mut c = self.conn().await?;
        let row: Option<DashRow> = c
            .exec_first(
                format!(
                    "SELECT dash_id, name, owner_id, owner, shared != 0, filters, layout, \
                     CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', updated_at) DIV 1000 AS SIGNED) \
                     FROM {} WHERE dash_id = ?",
                    self.t("dashboards")
                ),
                (id,),
            )
            .await
            .map_err(unavailable)?;
        let Some((id, name, owner_id, owner, shared, filters, layout, at)) = row else {
            return Ok(None);
        };
        let tiles: Vec<TileRow> = c
            .exec(
                format!(
                    "SELECT tile_id, title, db_name, sql_text, chart, refresh_seconds, pos \
                     FROM {} WHERE dash_id = ? ORDER BY tile_id",
                    self.t("dashboard_tiles")
                ),
                (id,),
            )
            .await
            .map_err(unavailable)?;
        let mut tiles: Vec<Tile> = tiles
            .into_iter()
            .filter_map(|(tid, title, db, sql, chart, refresh, pos)| {
                Some(Tile {
                    id: tid,
                    title,
                    database: db,
                    sql,
                    chart: serde_json::from_str(&chart).ok()?,
                    refresh_seconds: refresh,
                    pos: serde_json::from_str(&pos).ok()?,
                })
            })
            .collect();
        tiles.sort_by_key(|t| (t.pos.y, t.pos.x, t.id));
        Ok(Some(Dashboard {
            id,
            name,
            owner_id,
            owner,
            shared,
            filters: serde_json::from_str(&filters).unwrap_or_default(),
            layout: serde_json::from_str(&layout).unwrap_or(Value::Null),
            updated_ms: at,
            tiles,
        }))
    }

    async fn create(
        &self,
        owner: UserId,
        owner_name: &str,
        name: &str,
        shared: bool,
        filters: &[Filter],
    ) -> Result<u64> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (owner_id, owner, shared, name, filters, layout, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("dashboards")
            ),
            (
                owner.0,
                owner_name,
                shared,
                name,
                json_text(&filters),
                json!({"columns": GRID_COLUMNS}).to_string(),
            ),
        )
        .await
        .map_err(unavailable)?;
        c.last_insert_id()
            .ok_or_else(|| SprigError::internal("no id for the new dashboard"))
    }

    async fn update(&self, d: &Dashboard) -> Result<()> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "UPDATE {} SET name = ?, shared = ?, filters = ?, layout = ?, \
                 updated_at = UTC_TIMESTAMP(3) WHERE dash_id = ?",
                self.t("dashboards")
            ),
            (
                &d.name,
                d.shared,
                json_text(&d.filters),
                d.layout.to_string(),
                d.id,
            ),
        )
        .await
        .map_err(unavailable)
    }

    async fn delete(&self, id: u64) -> Result<()> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE dash_id = ?",
                self.t("dashboard_tiles")
            ),
            (id,),
        )
        .await
        .map_err(unavailable)?;
        c.exec_drop(
            format!("DELETE FROM {} WHERE dash_id = ?", self.t("dashboards")),
            (id,),
        )
        .await
        .map_err(unavailable)
    }

    async fn put_tile(&self, dash: u64, tile: Option<u64>, t: &TileReq, pos: Pos) -> Result<u64> {
        let mut c = self.conn().await?;
        let id = if let Some(tile) = tile {
            c.exec_drop(
                format!(
                    "UPDATE {} SET title = ?, db_name = ?, sql_text = ?, chart = ?, \
                     refresh_seconds = ?, pos = ? WHERE tile_id = ? AND dash_id = ?",
                    self.t("dashboard_tiles")
                ),
                (
                    &t.title,
                    &t.database,
                    &t.sql,
                    t.chart.to_string(),
                    t.refresh_seconds,
                    json_text(&pos),
                    tile,
                    dash,
                ),
            )
            .await
            .map_err(unavailable)?;
            tile
        } else {
            c.exec_drop(
                format!(
                    "INSERT INTO {} (dash_id, title, db_name, sql_text, chart, refresh_seconds, pos) \
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    self.t("dashboard_tiles")
                ),
                (
                    dash,
                    &t.title,
                    &t.database,
                    &t.sql,
                    t.chart.to_string(),
                    t.refresh_seconds,
                    json_text(&pos),
                ),
            )
            .await
            .map_err(unavailable)?;
            c.last_insert_id()
                .ok_or_else(|| SprigError::internal("no id for the new tile"))?
        };
        c.exec_drop(
            format!(
                "UPDATE {} SET updated_at = UTC_TIMESTAMP(3) WHERE dash_id = ?",
                self.t("dashboards")
            ),
            (dash,),
        )
        .await
        .map_err(unavailable)?;
        Ok(id)
    }

    async fn move_tiles(&self, dash: u64, moves: &[(u64, Pos)]) -> Result<()> {
        let mut c = self.conn().await?;
        for (id, p) in moves {
            c.exec_drop(
                format!(
                    "UPDATE {} SET pos = ? WHERE tile_id = ? AND dash_id = ?",
                    self.t("dashboard_tiles")
                ),
                (json_text(p), id, dash),
            )
            .await
            .map_err(unavailable)?;
        }
        Ok(())
    }

    async fn delete_tile(&self, dash: u64, tile: u64) -> Result<bool> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE tile_id = ? AND dash_id = ?",
                self.t("dashboard_tiles")
            ),
            (tile, dash),
        )
        .await
        .map_err(unavailable)?;
        Ok(c.affected_rows() > 0)
    }
}

// ---- what the routes do ----

fn can_view(d: &Dashboard, who: &Who) -> bool {
    d.owner_id == who.user.0 || d.shared
}

fn can_edit(d: &Dashboard, who: &Who) -> bool {
    d.owner_id == who.user.0 || (d.shared && who.admin)
}

fn header_json(h: &Header, who: &Who) -> Value {
    let mine = h.owner_id == who.user.0;
    json!({
        "id": h.id.to_string(), "name": h.name, "owner": h.owner, "owner_id": h.owner_id,
        "shared": h.shared, "mine": mine, "can_edit": mine || (h.shared && who.admin),
        "tiles": h.tiles, "updated_ms": h.updated_ms,
    })
}

fn tile_json(t: &Tile) -> Value {
    json!({
        "id": t.id.to_string(), "title": t.title, "database": t.database, "sql": t.sql,
        "chart": t.chart, "refresh_seconds": t.refresh_seconds, "pos": t.pos,
    })
}

/// A dashboard as the routes answer it.
#[must_use]
pub fn dashboard_json(d: &Dashboard, who: &Who) -> Value {
    json!({
        "id": d.id.to_string(), "name": d.name, "owner": d.owner, "owner_id": d.owner_id,
        "shared": d.shared, "mine": d.owner_id == who.user.0, "can_edit": can_edit(d, who),
        "filters": d.filters, "layout": d.layout, "updated_ms": d.updated_ms,
        "tiles": d.tiles.iter().map(tile_json).collect::<Vec<_>>(),
    })
}

/// An id in a path or body: digits, as text or a number.
///
/// # Errors
/// Anything else.
pub fn id_of(v: &Value) -> Result<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
    .ok_or_else(|| invalid("an id is a whole number"))
}

async fn visible(store: &dyn DashStore, who: &Who, id: u64) -> Result<Dashboard> {
    match store.get(id).await? {
        Some(d) if can_view(&d, who) => Ok(d),
        _ => Err(missing(id)),
    }
}

async fn editable(store: &dyn DashStore, who: &Who, id: u64) -> Result<Dashboard> {
    let d = visible(store, who, id).await?;
    if can_edit(&d, who) {
        Ok(d)
    } else {
        Err(denied(
            "a shared dashboard is changed by its owner or a tenant administrator (SPRIG_TENANT_ADMIN)",
        ))
    }
}

fn name_of(v: &Value) -> Result<String> {
    let n = text(v, "name");
    if n.is_empty() || n.chars().count() > MAX_NAME {
        return Err(invalid(format!(
            "a dashboard's name is 1 to {MAX_NAME} characters"
        )));
    }
    Ok(n.to_owned())
}

/// `GET /v1/dashboards`: the caller's own and the shared ones.
///
/// # Errors
/// The store's.
pub async fn list(store: &dyn DashStore, who: &Who) -> Result<Value> {
    let all = store.list(who.user).await?;
    let (mine, shared): (Vec<&Header>, Vec<&Header>) =
        all.iter().partition(|h| h.owner_id == who.user.0);
    Ok(json!({
        "mine": mine.iter().map(|h| header_json(h, who)).collect::<Vec<_>>(),
        "shared": shared.iter().map(|h| header_json(h, who)).collect::<Vec<_>>(),
    }))
}

/// `GET /v1/dashboards/{id}`.
///
/// # Errors
/// One the caller can't see.
pub async fn get(store: &dyn DashStore, who: &Who, id: u64) -> Result<Value> {
    Ok(dashboard_json(&visible(store, who, id).await?, who))
}

/// `POST /v1/dashboards {name, shared?, filters?}`.
///
/// # Errors
/// A bad name or filter; more than [`MAX_OWNED`] dashboards.
pub async fn create(store: &dyn DashStore, who: &Who, b: &Value) -> Result<Value> {
    let name = name_of(b)?;
    let filters = match b.get("filters") {
        None | Some(Value::Null) => Vec::new(),
        Some(f) => filters_from_json(f)?,
    };
    let shared = b.get("shared").and_then(Value::as_bool).unwrap_or(false);
    let owned = store
        .list(who.user)
        .await?
        .iter()
        .filter(|h| h.owner_id == who.user.0)
        .count();
    if owned >= MAX_OWNED {
        return Err(invalid(format!(
            "you have {MAX_OWNED} dashboards already; delete one first"
        )));
    }
    let id = store
        .create(who.user, &who.name, &name, shared, &filters)
        .await?;
    get(store, who, id).await
}

/// `POST /v1/dashboards/{id}`: `{name?, filters?, layout?, shared?,
/// positions?: [{tile, pos}]}`. Only the owner changes `shared`.
///
/// # Errors
/// One the caller can't edit; bad values.
pub async fn update(store: &dyn DashStore, who: &Who, id: u64, b: &Value) -> Result<Value> {
    let mut d = editable(store, who, id).await?;
    if b.get("name").is_some() {
        d.name = name_of(b)?;
    }
    if let Some(f) = b.get("filters") {
        d.filters = filters_from_json(f)?;
    }
    if let Some(l) = b.get("layout").filter(|l| l.is_object()) {
        d.layout = l.clone();
    }
    if let Some(s) = b.get("shared").and_then(Value::as_bool)
        && s != d.shared
    {
        if d.owner_id != who.user.0 {
            return Err(denied("only a dashboard's owner shares or unshares it"));
        }
        d.shared = s;
    }
    if let Some(moves) = b.get("positions") {
        let moves = moves
            .as_array()
            .ok_or_else(|| invalid("positions is a list of {tile, pos}"))?
            .iter()
            .map(|m| Ok((id_of(&m["tile"])?, Pos::from_json(&m["pos"])?)))
            .collect::<Result<Vec<_>>>()?;
        store.move_tiles(id, &moves).await?;
    }
    store.update(&d).await?;
    get(store, who, id).await
}

/// `POST /v1/dashboards/{id}:share {shared}`: the owner only.
///
/// # Errors
/// Anyone but the owner.
pub async fn share(store: &dyn DashStore, who: &Who, id: u64, shared: bool) -> Result<Value> {
    update(store, who, id, &json!({"shared": shared})).await
}

/// `POST /v1/dashboards/{id}:delete`.
///
/// # Errors
/// One the caller can't edit.
pub async fn delete(store: &dyn DashStore, who: &Who, id: u64) -> Result<Value> {
    editable(store, who, id).await?;
    store.delete(id).await?;
    Ok(json!({"deleted": true}))
}

fn next_row(d: &Dashboard) -> u32 {
    d.tiles.iter().map(|t| t.pos.y + t.pos.h).max().unwrap_or(0)
}

fn default_pos(d: &Dashboard, chart: &Value) -> Pos {
    let mark = chart.get("mark").and_then(Value::as_str).unwrap_or("bar");
    let (w, h) = match mark {
        "kpi" | "gauge" => (3, 3),
        "text-table" | "pivot" | "highlight-table" => (6, 5),
        _ => (6, 4),
    };
    // Beside the last tile when the row has room, else below everything.
    if let Some(last) = d.tiles.iter().max_by_key(|t| (t.pos.y, t.pos.x + t.pos.w))
        && last.pos.y + last.pos.h == next_row(d)
        && last.pos.x + last.pos.w + w <= GRID_COLUMNS
    {
        return Pos {
            x: last.pos.x + last.pos.w,
            y: last.pos.y,
            w,
            h,
        };
    }
    Pos {
        x: 0,
        y: next_row(d),
        w,
        h,
    }
}

fn check_filters(d: &Dashboard, sql: &str) -> Result<()> {
    for n in placeholders(sql)? {
        if !d.filters.iter().any(|f| f.name.eq_ignore_ascii_case(&n)) {
            return Err(invalid(format!(
                "the query uses {{{{{n}}}}}, but the dashboard has no filter named {n}: add the filter first"
            )));
        }
    }
    Ok(())
}

async fn put_tile_checked(
    store: &dyn DashStore,
    d: &Dashboard,
    tile: Option<u64>,
    t: &TileReq,
) -> Result<u64> {
    check_filters(d, &t.sql)?;
    let pos = match (t.pos, tile) {
        (Some(p), _) => p,
        (None, Some(id)) => d
            .tiles
            .iter()
            .find(|x| x.id == id)
            .map_or_else(|| default_pos(d, &t.chart), |x| x.pos),
        (None, None) => default_pos(d, &t.chart),
    };
    store.put_tile(d.id, tile, t, pos).await
}

/// `POST /v1/dashboards/{id}/tiles`: adds a tile.
///
/// # Errors
/// One the caller can't edit; a bad tile; too many tiles.
pub async fn add_tile(store: &dyn DashStore, who: &Who, id: u64, b: &Value) -> Result<Value> {
    let d = editable(store, who, id).await?;
    let t = TileReq::from_json(b)?;
    if d.tiles.len() >= MAX_TILES {
        return Err(invalid(format!(
            "a dashboard holds at most {MAX_TILES} tiles"
        )));
    }
    let tile = put_tile_checked(store, &d, None, &t).await?;
    Ok(json!({"tile": tile.to_string(), "dashboard": get(store, who, id).await?}))
}

/// `POST /v1/dashboards/{id}/tiles/{tile}`: replaces a tile.
///
/// # Errors
/// One the caller can't edit; an unknown tile; a bad tile.
pub async fn update_tile(
    store: &dyn DashStore,
    who: &Who,
    id: u64,
    tile: u64,
    b: &Value,
) -> Result<Value> {
    let d = editable(store, who, id).await?;
    if !d.tiles.iter().any(|t| t.id == tile) {
        return Err(invalid(format!("dashboard {id} has no tile {tile}")));
    }
    let t = TileReq::from_json(b)?;
    put_tile_checked(store, &d, Some(tile), &t).await?;
    Ok(json!({"tile": tile.to_string(), "dashboard": get(store, who, id).await?}))
}

/// `POST /v1/dashboards/{id}/tiles/{tile}:delete`.
///
/// # Errors
/// One the caller can't edit.
pub async fn delete_tile(store: &dyn DashStore, who: &Who, id: u64, tile: u64) -> Result<Value> {
    editable(store, who, id).await?;
    let deleted = store.delete_tile(id, tile).await?;
    Ok(json!({"deleted": deleted}))
}

fn params_of(b: &Value) -> Result<Map<String, Value>> {
    match b.get("params") {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(m)) => Ok(m.clone()),
        Some(_) => Err(invalid("params is an object of filter values")),
    }
}

fn run_json(r: &ResultSet, ms: u128) -> Value {
    json!({
        "columns": r.columns, "rows": r.rows, "row_count": r.row_count,
        "truncated": r.truncated, "ms": u64::try_from(ms).unwrap_or(u64::MAX),
    })
}

/// `POST /v1/dashboards/{id}/tiles/{tile}:run {params?}`: the tile's
/// query, with the dashboard's filters (or `params`) bound, run by
/// `runner`, which is the viewer's.
///
/// # Errors
/// One the caller can't see; a bad value; the query's own error.
pub async fn run_tile(
    store: &dyn DashStore,
    who: &Who,
    runner: &dyn TileRunner,
    id: u64,
    tile: u64,
    b: &Value,
) -> Result<Value> {
    let d = visible(store, who, id).await?;
    let t = d
        .tiles
        .iter()
        .find(|t| t.id == tile)
        .ok_or_else(|| invalid(format!("dashboard {id} has no tile {tile}")))?;
    let sql = bind(&t.sql, &d.filters, &params_of(b)?)?;
    crate::sql_guard::read_only(&sql)
        .map_err(|e| invalid(format!("a tile's query is one SELECT: {e}")))?;
    let started = std::time::Instant::now();
    let r = runner
        .run(t.database.as_deref().unwrap_or("-"), &sql)
        .await?;
    Ok(run_json(&r, started.elapsed().as_millis()))
}

/// `POST /v1/dashboards:run {database, sql, filters?, params?}`: a query
/// that isn't saved yet (the chart editor, a proposal's preview), read
/// only, by `runner`.
///
/// # Errors
/// Not one read; a bad filter or value; the query's own error.
pub async fn run_unsaved(runner: &dyn TileRunner, b: &Value) -> Result<Value> {
    let sql = text(b, "sql").trim_end_matches(';').trim();
    if sql.is_empty() || sql.len() > MAX_TEXT {
        return Err(invalid(format!("sql is 1 to {MAX_TEXT} bytes")));
    }
    let filters = match b.get("filters") {
        None | Some(Value::Null) => Vec::new(),
        Some(f) => filters_from_json(f)?,
    };
    let bound = bind(sql, &filters, &params_of(b)?)?;
    crate::sql_guard::read_only(&bound)
        .map_err(|e| invalid(format!("a tile's query is one SELECT: {e}")))?;
    let db = match text(b, "database") {
        "" => "-",
        d => d,
    };
    let started = std::time::Instant::now();
    let r = runner.run(db, &bound).await?;
    Ok(run_json(&r, started.elapsed().as_millis()))
}

/// Accepts a `propose_tile` proposal (its stored input, `proposal`) into
/// a dashboard. `choice` is the user's: `{dashboard?: id, new_dashboard?:
/// name, title?, chart?, sql?}`; without either dashboard field, the
/// proposal's own `dashboard` (an id, or a name of one of the caller's
/// dashboards, made when missing) is used, else the caller's
/// [`DEFAULT_DASHBOARD`]. The tile is checked as any other. Answers the
/// dashboard and the tile's ids.
///
/// # Errors
/// One the caller can't edit; a bad tile.
pub async fn accept_tile(
    store: &dyn DashStore,
    who: &Who,
    proposal: &Value,
    choice: &Value,
) -> Result<(u64, u64)> {
    let pick = |k: &str| {
        choice
            .get(k)
            .filter(|v| !v.is_null())
            .or_else(|| proposal.get(k))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let req = json!({
        "title": pick("title"), "database": pick("db"), "sql": pick("sql"), "chart": pick("chart"),
        "refresh_seconds": pick("refresh_seconds"), "pos": choice.get("pos").cloned().unwrap_or(Value::Null),
    });
    let t = TileReq::from_json(&req)?;
    let dash = if let Some(id) = choice.get("dashboard").filter(|v| !v.is_null()) {
        editable(store, who, id_of(id)?).await?.id
    } else {
        let named = match text(choice, "new_dashboard") {
            "" => None,
            n => Some((n.to_owned(), true)),
        }
        .or_else(|| match text(proposal, "dashboard") {
            "" => None,
            n => Some((n.to_owned(), false)),
        });
        // Neither the user nor the assistant named one: the caller's own
        // default dashboard.
        let (name, fresh) = named.unwrap_or_else(|| (DEFAULT_DASHBOARD.to_owned(), false));
        let mut by_id = None;
        if !fresh
            && let Ok(id) = name.parse::<u64>()
            && let Some(d) = store.get(id).await?
            && can_edit(&d, who)
        {
            by_id = Some(d.id);
        }
        if !fresh && by_id.is_none() {
            by_id = store
                .list(who.user)
                .await?
                .into_iter()
                .find(|h| h.owner_id == who.user.0 && h.name.eq_ignore_ascii_case(&name))
                .map(|h| h.id);
        }
        if let Some(id) = by_id {
            id
        } else {
            let made = create(store, who, &json!({"name": name})).await?;
            id_of(&made["id"])?
        }
    };
    let d = editable(store, who, dash).await?;
    if d.tiles.len() >= MAX_TILES {
        return Err(invalid(format!(
            "a dashboard holds at most {MAX_TILES} tiles"
        )));
    }
    let tile = put_tile_checked(store, &d, None, &t).await?;
    Ok((dash, tile))
}
