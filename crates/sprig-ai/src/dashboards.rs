//! Dashboards: tiles of charts over read-only queries, for one user or
//! shared with the whole tenant, in the tenant's catalog records in the
//! storage engine ([`RecordsDashboards`], module `dashboards`).
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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::migrate::{Migration, Module, Phase};
use sprig_catalog::records::{next_ids, prefix_end, scan_all};
use sprig_catalog::{Pk, RecordStore, RecordTx, RowTable};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};

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

/// The module's migrations, for tenant catalogs on MariaDB made before
/// the dashboards moved to the catalog records (the contracts listing
/// still names the module); nothing creates these tables now.
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

fn too_many_tiles() -> SprigError {
    invalid(format!("a dashboard holds at most {MAX_TILES} tiles"))
}

fn no_tile(dash: u64, tile: u64) -> SprigError {
    invalid(format!("dashboard {dash} has no tile {tile}"))
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
    /// Adds (`tile` `None`) or replaces a tile; its id. Checked with the
    /// dashboard held, so concurrent changes can't get past them: a tile
    /// replaced must still be there, and a dashboard takes at most
    /// [`MAX_TILES`].
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
        match tile {
            Some(id) if !d.tiles.iter().any(|x| x.id == id) => return Err(no_tile(dash, id)),
            None if d.tiles.len() >= MAX_TILES => return Err(too_many_tiles()),
            _ => {}
        }
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

/// The dashboards' rows in a tenant's catalog records
/// (`m/dashboards/dash/<id>` and `m/dashboards/tile/<dashboard><tile>`),
/// their ids from the counters `dashboards.dash` and `dashboards.tile`.
/// Each call is one record transaction; a tile change locks its
/// dashboard's row, so two editors of one dashboard take turns.
pub const DASH_ROWS: RowTable = RowTable::new(MODULE, "dash");
/// The tiles' rows (see [`DASH_ROWS`]).
pub const TILE_ROWS: RowTable = RowTable::new(MODULE, "tile");
const DASH_COUNTER: &str = "dashboards.dash";
const TILE_COUNTER: &str = "dashboards.tile";

/// A dashboard's row as stored (its tiles are their own rows).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DashRec {
    id: u64,
    owner_id: u32,
    owner: String,
    shared: bool,
    name: String,
    filters: Vec<Filter>,
    layout: Value,
    created_ms: i64,
    updated_ms: i64,
}

/// In the tenant's catalog records (the storage engine).
pub struct RecordsDashboards {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for RecordsDashboards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordsDashboards")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

fn dash_pk(id: u64) -> Pk {
    Pk::new().u64(id)
}

fn tile_pk(dash: u64, tile: u64) -> Pk {
    Pk::new().u64(dash).u64(tile)
}

impl RecordsDashboards {
    /// `tenant`'s dashboards in `records`.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    async fn begin(&self) -> Result<Box<dyn RecordTx>> {
        self.records.begin(self.tenant).await
    }

    /// The dashboard's row, locked until the transaction ends.
    async fn locked(tx: &mut dyn RecordTx, id: u64) -> Result<Option<DashRec>> {
        DASH_ROWS.get_for_update(tx, &dash_pk(id)).await
    }

    async fn touch(tx: &mut dyn RecordTx, mut d: DashRec) -> Result<()> {
        d.updated_ms = now_ms();
        DASH_ROWS.put(tx, &dash_pk(d.id), &d).await
    }
}

#[async_trait]
impl DashStore for RecordsDashboards {
    async fn list(&self, user: UserId) -> Result<Vec<Header>> {
        let mut tx = self.begin().await?;
        let mut out = Vec::new();
        for d in DASH_ROWS.rows::<DashRec>(&mut *tx, &Pk::new()).await? {
            if d.owner_id != user.0 && !d.shared {
                continue;
            }
            let tiles = TILE_ROWS
                .scan::<Value>(&mut *tx, &dash_pk(d.id))
                .await?
                .len();
            out.push(Header {
                id: d.id,
                name: d.name,
                owner_id: d.owner_id,
                owner: d.owner,
                shared: d.shared,
                tiles: u32::try_from(tiles).unwrap_or(u32::MAX),
                updated_ms: d.updated_ms,
            });
        }
        tx.rollback().await?;
        out.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    async fn get(&self, id: u64) -> Result<Option<Dashboard>> {
        let mut tx = self.begin().await?;
        let Some(d) = DASH_ROWS.get::<DashRec>(&mut *tx, &dash_pk(id)).await? else {
            tx.rollback().await?;
            return Ok(None);
        };
        let mut tiles = TILE_ROWS.rows::<Tile>(&mut *tx, &dash_pk(id)).await?;
        tx.rollback().await?;
        tiles.sort_by_key(|t| (t.pos.y, t.pos.x, t.id));
        Ok(Some(Dashboard {
            id: d.id,
            name: d.name,
            owner_id: d.owner_id,
            owner: d.owner,
            shared: d.shared,
            filters: d.filters,
            layout: d.layout,
            updated_ms: d.updated_ms,
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
        let mut tx = self.begin().await?;
        let id = next_ids(&mut *tx, DASH_COUNTER, 1).await?;
        let now = now_ms();
        let d = DashRec {
            id,
            owner_id: owner.0,
            owner: owner_name.to_owned(),
            shared,
            name: name.to_owned(),
            filters: filters.to_vec(),
            layout: json!({"columns": GRID_COLUMNS}),
            created_ms: now,
            updated_ms: now,
        };
        DASH_ROWS.put(&mut *tx, &dash_pk(id), &d).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn update(&self, d: &Dashboard) -> Result<()> {
        let mut tx = self.begin().await?;
        if let Some(mut have) = Self::locked(&mut *tx, d.id).await? {
            have.name.clone_from(&d.name);
            have.shared = d.shared;
            have.filters.clone_from(&d.filters);
            have.layout.clone_from(&d.layout);
            Self::touch(&mut *tx, have).await?;
        }
        tx.commit().await
    }

    async fn delete(&self, id: u64) -> Result<()> {
        let mut tx = self.begin().await?;
        if Self::locked(&mut *tx, id).await?.is_some() {
            for (pk, _) in TILE_ROWS.scan::<Value>(&mut *tx, &dash_pk(id)).await? {
                TILE_ROWS.delete(&mut *tx, &pk).await?;
            }
            DASH_ROWS.delete(&mut *tx, &dash_pk(id)).await?;
        }
        tx.commit().await
    }

    async fn put_tile(&self, dash: u64, tile: Option<u64>, t: &TileReq, pos: Pos) -> Result<u64> {
        let mut tx = self.begin().await?;
        let d = Self::locked(&mut *tx, dash)
            .await?
            .ok_or_else(|| missing(dash))?;
        let id = if let Some(id) = tile {
            if tx
                .get_for_update(&TILE_ROWS.key(&tile_pk(dash, id)))
                .await?
                .is_none()
            {
                return Err(no_tile(dash, id));
            }
            id
        } else {
            // Counted by key, with the dashboard held.
            let lo = TILE_ROWS.key(&dash_pk(dash));
            let have = scan_all(&mut *tx, &lo, &prefix_end(&lo)).await?.len();
            if have >= MAX_TILES {
                return Err(too_many_tiles());
            }
            next_ids(&mut *tx, TILE_COUNTER, 1).await?
        };
        let row = Tile {
            id,
            title: t.title.clone(),
            database: t.database.clone(),
            sql: t.sql.clone(),
            chart: t.chart.clone(),
            refresh_seconds: t.refresh_seconds,
            pos,
        };
        TILE_ROWS.put(&mut *tx, &tile_pk(dash, id), &row).await?;
        Self::touch(&mut *tx, d).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn move_tiles(&self, dash: u64, moves: &[(u64, Pos)]) -> Result<()> {
        let mut tx = self.begin().await?;
        let Some(d) = Self::locked(&mut *tx, dash).await? else {
            return tx.commit().await;
        };
        for (id, p) in moves {
            let pk = tile_pk(dash, *id);
            if let Some(mut t) = TILE_ROWS.get::<Tile>(&mut *tx, &pk).await? {
                t.pos = *p;
                TILE_ROWS.put(&mut *tx, &pk, &t).await?;
            }
        }
        Self::touch(&mut *tx, d).await?;
        tx.commit().await
    }

    async fn delete_tile(&self, dash: u64, tile: u64) -> Result<bool> {
        let mut tx = self.begin().await?;
        let Some(d) = Self::locked(&mut *tx, dash).await? else {
            tx.rollback().await?;
            return Ok(false);
        };
        let pk = tile_pk(dash, tile);
        let had = TILE_ROWS.get::<Value>(&mut *tx, &pk).await?.is_some();
        if had {
            TILE_ROWS.delete(&mut *tx, &pk).await?;
            Self::touch(&mut *tx, d).await?;
        }
        tx.commit().await?;
        Ok(had)
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
        return Err(too_many_tiles());
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
        return Err(no_tile(id, tile));
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
        return Err(too_many_tiles());
    }
    let tile = put_tile_checked(store, &d, None, &t).await?;
    Ok((dash, tile))
}
