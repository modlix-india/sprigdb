//! Reads of system variables (`@@x`) and user variables (`@x`), and
//! `SET @x = ...`, answered in the gateway when every database of the
//! tenant is on the engine, so a session that never touches MariaDB's
//! tables never opens a MariaDB connection.
//!
//! | Statement | Done as |
//! |---|---|
//! | a statement reading `@@x` / `@@session.x` / `@@global.x` | each read becomes a literal: the persona's value, the session's own value, the value its handshake gave, or the server default below; a whole select item keeps the name the client wrote |
//! | a statement reading `@x` | the variable's value as a literal (`NULL` when unset) |
//! | `SET @a = e [, @b := e]` | each `e` is run as `SELECT e` on the engine, in order; the value is kept in the session |
//!
//! A variable the table below doesn't know, a user-variable assignment
//! inside a statement (`@x := ...`, `INTO @x`), a value in a character
//! set other than UTF-8 that isn't plain ASCII, or anything the engine
//! can't run still goes to MariaDB as before.

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::Collation;
use sprig_exec::ExprType;
use sprig_exec::types::Derivation;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};
use sprig_sql::lex::{self, LexOptions, Token, TokenKind};

use super::super::PassSession;
use super::super::tenant::Step;
use crate::backend::charset;
use crate::backend::state::VarValue;
use crate::engine::catalog;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// How a variable's value reads in a `SELECT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A switch: `ON`/`OFF` read as `1`/`0`.
    Bool,
    /// A number, as it is.
    Num,
    /// A string.
    Str,
}

/// Server defaults of the variables clients read at connect and in their
/// dialect checks: name, kind, MariaDB 11.8's value, MySQL 8.4's value
/// (`None`: the server has no such variable).
const DEFAULTS: &[(&str, Kind, Option<&str>, Option<&str>)] = &[
    ("auto_increment_increment", Kind::Num, Some("1"), Some("1")),
    ("auto_increment_offset", Kind::Num, Some("1"), Some("1")),
    ("autocommit", Kind::Bool, Some("ON"), Some("ON")),
    ("big_tables", Kind::Bool, Some("OFF"), Some("OFF")),
    ("binlog_format", Kind::Str, Some("MIXED"), Some("ROW")),
    (
        "character_set_database",
        Kind::Str,
        Some("utf8mb4"),
        Some("utf8mb4"),
    ),
    (
        "character_set_filesystem",
        Kind::Str,
        Some("binary"),
        Some("binary"),
    ),
    (
        "character_set_server",
        Kind::Str,
        Some("utf8mb4"),
        Some("utf8mb4"),
    ),
    (
        "character_set_system",
        Kind::Str,
        Some("utf8mb3"),
        Some("utf8mb3"),
    ),
    (
        "character_set_collations",
        Kind::Str,
        Some(charset::DEFAULT_CHARSET_COLLATIONS),
        None,
    ),
    (
        "collation_database",
        Kind::Str,
        Some("utf8mb4_uca1400_ai_ci"),
        Some("utf8mb4_0900_ai_ci"),
    ),
    (
        "collation_server",
        Kind::Str,
        Some("utf8mb4_uca1400_ai_ci"),
        Some("utf8mb4_0900_ai_ci"),
    ),
    (
        "completion_type",
        Kind::Str,
        Some("NO_CHAIN"),
        Some("NO_CHAIN"),
    ),
    ("concurrent_insert", Kind::Str, Some("AUTO"), Some("AUTO")),
    ("connect_timeout", Kind::Num, Some("10"), Some("10")),
    ("cte_max_recursion_depth", Kind::Num, None, Some("1000")),
    ("default_password_lifetime", Kind::Num, Some("0"), Some("0")),
    (
        "default_storage_engine",
        Kind::Str,
        Some("InnoDB"),
        Some("InnoDB"),
    ),
    (
        "default_tmp_storage_engine",
        Kind::Str,
        Some(""),
        Some("InnoDB"),
    ),
    ("default_week_format", Kind::Num, Some("0"), Some("0")),
    ("div_precision_increment", Kind::Num, Some("4"), Some("4")),
    ("event_scheduler", Kind::Str, Some("OFF"), Some("ON")),
    (
        "explicit_defaults_for_timestamp",
        Kind::Bool,
        Some("ON"),
        Some("ON"),
    ),
    ("foreign_key_checks", Kind::Bool, Some("ON"), Some("ON")),
    (
        "group_concat_max_len",
        Kind::Num,
        Some("1048576"),
        Some("1024"),
    ),
    ("have_ssl", Kind::Str, Some("YES"), None),
    ("init_connect", Kind::Str, Some(""), Some("")),
    ("innodb_autoinc_lock_mode", Kind::Num, Some("1"), Some("2")),
    (
        "innodb_default_row_format",
        Kind::Str,
        Some("dynamic"),
        Some("dynamic"),
    ),
    ("innodb_file_per_table", Kind::Bool, Some("ON"), Some("ON")),
    (
        "innodb_lock_wait_timeout",
        Kind::Num,
        Some("50"),
        Some("50"),
    ),
    ("innodb_page_size", Kind::Num, Some("16384"), Some("16384")),
    ("innodb_snapshot_isolation", Kind::Bool, Some("ON"), None),
    ("innodb_strict_mode", Kind::Bool, Some("ON"), Some("ON")),
    (
        "interactive_timeout",
        Kind::Num,
        Some("28800"),
        Some("28800"),
    ),
    ("license", Kind::Str, Some("GPL"), Some("GPL")),
    ("local_infile", Kind::Bool, Some("ON"), Some("OFF")),
    (
        "lock_wait_timeout",
        Kind::Num,
        Some("86400"),
        Some("31536000"),
    ),
    ("log_bin", Kind::Bool, Some("OFF"), Some("OFF")),
    (
        "log_bin_trust_function_creators",
        Kind::Bool,
        Some("OFF"),
        Some("OFF"),
    ),
    (
        "long_query_time",
        Kind::Num,
        Some("10.000000"),
        Some("10.000000"),
    ),
    (
        "lower_case_file_system",
        Kind::Bool,
        Some("OFF"),
        Some("OFF"),
    ),
    ("lower_case_table_names", Kind::Num, Some("0"), Some("0")),
    (
        "max_allowed_packet",
        Kind::Num,
        Some("16777216"),
        Some("67108864"),
    ),
    ("max_execution_time", Kind::Num, None, Some("0")),
    (
        "max_heap_table_size",
        Kind::Num,
        Some("16777216"),
        Some("16777216"),
    ),
    (
        "max_join_size",
        Kind::Num,
        Some("18446744073709551615"),
        Some("18446744073709551615"),
    ),
    ("max_recursive_iterations", Kind::Num, Some("1000"), None),
    ("max_sort_length", Kind::Num, Some("1024"), Some("1024")),
    ("max_sp_recursion_depth", Kind::Num, Some("0"), Some("0")),
    ("max_statement_time", Kind::Num, Some("0.000000"), None),
    ("net_buffer_length", Kind::Num, Some("16384"), Some("16384")),
    ("net_read_timeout", Kind::Num, Some("30"), Some("30")),
    ("net_retry_count", Kind::Num, Some("10"), Some("10")),
    ("net_write_timeout", Kind::Num, Some("60"), Some("60")),
    ("old_mode", Kind::Str, Some("UTF8_IS_UTF8MB3"), None),
    ("optimizer_search_depth", Kind::Num, Some("62"), Some("62")),
    ("performance_schema", Kind::Bool, Some("OFF"), Some("ON")),
    ("protocol_version", Kind::Num, Some("10"), Some("10")),
    ("query_cache_size", Kind::Num, Some("1048576"), None),
    ("query_cache_type", Kind::Str, Some("OFF"), None),
    ("read_only", Kind::Bool, Some("OFF"), Some("OFF")),
    (
        "require_secure_transport",
        Kind::Bool,
        Some("OFF"),
        Some("OFF"),
    ),
    ("session_track_gtids", Kind::Str, None, Some("OFF")),
    ("session_track_schema", Kind::Bool, Some("ON"), Some("ON")),
    (
        "session_track_state_change",
        Kind::Bool,
        Some("OFF"),
        Some("OFF"),
    ),
    (
        "session_track_system_variables",
        Kind::Str,
        Some(
            "autocommit,character_set_client,character_set_connection,\
             character_set_results,redirect_url,time_zone",
        ),
        Some(
            "time_zone,autocommit,character_set_client,character_set_results,\
             character_set_connection",
        ),
    ),
    (
        "session_track_transaction_info",
        Kind::Str,
        Some("OFF"),
        Some("OFF"),
    ),
    // No local socket to switch to (drivers such as mysql_async read it at
    // connect and try the path when it names one).
    ("socket", Kind::Str, Some(""), Some("")),
    (
        "sort_buffer_size",
        Kind::Num,
        Some("2097152"),
        Some("262144"),
    ),
    ("sql_auto_is_null", Kind::Bool, Some("OFF"), Some("OFF")),
    ("sql_big_selects", Kind::Bool, Some("ON"), Some("ON")),
    ("sql_buffer_result", Kind::Bool, Some("OFF"), Some("OFF")),
    ("sql_log_bin", Kind::Bool, Some("ON"), Some("ON")),
    (
        "sql_mode",
        Kind::Str,
        Some(
            "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
        ),
        Some(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
        ),
    ),
    ("sql_notes", Kind::Bool, Some("ON"), Some("ON")),
    ("sql_quote_show_create", Kind::Bool, Some("ON"), Some("ON")),
    ("sql_require_primary_key", Kind::Bool, None, Some("OFF")),
    ("sql_safe_updates", Kind::Bool, Some("OFF"), Some("OFF")),
    (
        "sql_select_limit",
        Kind::Num,
        Some("18446744073709551615"),
        Some("18446744073709551615"),
    ),
    ("sql_warnings", Kind::Bool, Some("OFF"), Some("OFF")),
    ("storage_engine", Kind::Str, Some("InnoDB"), None),
    ("system_time_zone", Kind::Str, Some("UTC"), Some("UTC")),
    ("time_zone", Kind::Str, Some("SYSTEM"), Some("SYSTEM")),
    (
        "tmp_table_size",
        Kind::Num,
        Some("16777216"),
        Some("16777216"),
    ),
    (
        "transaction_isolation",
        Kind::Str,
        Some("REPEATABLE-READ"),
        Some("REPEATABLE-READ"),
    ),
    (
        "transaction_read_only",
        Kind::Bool,
        Some("OFF"),
        Some("OFF"),
    ),
    ("tx_isolation", Kind::Str, Some("REPEATABLE-READ"), None),
    ("tx_read_only", Kind::Bool, Some("OFF"), None),
    ("unique_checks", Kind::Bool, Some("ON"), Some("ON")),
    (
        "updatable_views_with_limit",
        Kind::Str,
        Some("YES"),
        Some("YES"),
    ),
    (
        "version_compile_machine",
        Kind::Str,
        Some("aarch64"),
        Some("aarch64"),
    ),
    (
        "version_compile_os",
        Kind::Str,
        Some("debian-linux-gnu"),
        Some("Linux"),
    ),
    ("wait_timeout", Kind::Num, Some("28800"), Some("28800")),
    ("wsrep_on", Kind::Bool, Some("OFF"), None),
];

/// The character-set variables a session's handshake decides until it
/// sets them.
const HANDSHAKE_VARS: &[&str] = &[
    "character_set_client",
    "character_set_connection",
    "character_set_results",
    "collation_connection",
];

fn default_of(persona: Persona, name: &str) -> Option<(Kind, &'static str)> {
    let (_, kind, maria, mysql) = DEFAULTS.iter().find(|d| d.0 == name)?;
    let v = match persona {
        Persona::MariaDb => maria,
        Persona::MySql84 => mysql,
    };
    v.map(|v| (*kind, v))
}

/// Whole-number variables both servers report as signed.
const SIGNED_NUMS: &[&str] = &["innodb_autoinc_lock_mode"];

fn kind_of(name: &str) -> Kind {
    DEFAULTS
        .iter()
        .find(|d| d.0 == name)
        .map_or(Kind::Str, |d| d.1)
}

/// A value as the literal a `SELECT` of the variable reads as.
fn literal(kind: Kind, v: &str, nbe: bool) -> String {
    match kind {
        Kind::Bool if v.eq_ignore_ascii_case("ON") || v == "1" => "1".to_owned(),
        Kind::Bool if v.eq_ignore_ascii_case("OFF") || v == "0" => "0".to_owned(),
        Kind::Num if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit() || b == b'.') => {
            v.to_owned()
        }
        _ => sprig_sql::names::rewrite::sql_string(v, nbe),
    }
}

/// A system variable token's name (lower case, without `@@` and scope),
/// and whether it was `@@global.`.
fn sysvar_name(text: &[u8]) -> (String, bool) {
    let t = String::from_utf8_lossy(text).to_ascii_lowercase();
    let n = t.trim_start_matches('@');
    let global = n.starts_with("global.");
    let n = n
        .strip_prefix("session.")
        .or_else(|| n.strip_prefix("local."))
        .or_else(|| n.strip_prefix("global."))
        .unwrap_or(n)
        .trim_matches('`')
        .to_owned();
    (n, global)
}

/// A user variable token's name, as the session keeps it (lower case,
/// unquoted).
fn uservar_name(text: &[u8]) -> String {
    let t = text.strip_prefix(b"@").unwrap_or(text);
    let t = match t.first() {
        Some(q @ (b'`' | b'\'' | b'"')) if t.len() >= 2 && t.last() == Some(q) => {
            &t[1..t.len() - 1]
        }
        _ => t,
    };
    String::from_utf8_lossy(t).to_ascii_lowercase()
}

/// An identifier quoted with backticks.
fn quoted(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Whether the token at `sig[k]` is a whole select item without an alias.
fn whole_item(tokens: &[Token], sig: &[usize], sql: &[u8], k: usize) -> bool {
    let punct =
        |i: usize, c: u8| tokens[i].kind == TokenKind::Punct && sql[tokens[i].start as usize] == c;
    let word = |i: usize, w: &[&[u8]]| {
        tokens[i].kind == TokenKind::Word
            && w.iter()
                .any(|x| tokens[i].bytes(sql).eq_ignore_ascii_case(x))
    };
    let mut depth = 0i32;
    for &i in &sig[..k] {
        if punct(i, b'(') {
            depth += 1;
        } else if punct(i, b')') {
            depth -= 1;
        }
    }
    if depth != 0 || k == 0 {
        return false;
    }
    let prev = sig[k - 1];
    if !(punct(prev, b',') || word(prev, &[b"SELECT", b"DISTINCT", b"ALL"])) {
        return false;
    }
    match sig.get(k + 1) {
        None => true,
        Some(&n) => {
            punct(n, b',')
                || punct(n, b';')
                || word(
                    n,
                    &[
                        b"FROM",
                        b"UNION",
                        b"WHERE",
                        b"LIMIT",
                        b"ORDER",
                        b"GROUP",
                        b"HAVING",
                        b"WINDOW",
                        b"FOR",
                        b"EXCEPT",
                        b"INTERSECT",
                    ],
                )
        }
    }
}

/// Where a user variable is read.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Place {
    /// In a `SELECT`.
    pub select: bool,
    /// As an operand of arithmetic (`v * @d`).
    pub arithmetic: bool,
}

/// What a read of a variable becomes, or `None` when the gateway doesn't
/// know it.
pub(super) trait VarSource {
    /// `@@name` (`global`: `@@global.name`).
    fn sysvar(&self, name: &str, global: bool) -> Option<String>;
    /// `@name`, read at `at`.
    fn uservar(&self, name: &str, at: Place) -> String;
}

/// The value a `SET @a = e` computes: a decimal variable keeps its own
/// scale in it (`SET @e = @d * 2` holds `3.0` on both servers, where
/// `SELECT @d * 2` prints the variables' scale).
struct Assigned<'a>(&'a dyn VarSource);

impl VarSource for Assigned<'_> {
    fn sysvar(&self, name: &str, global: bool) -> Option<String> {
        self.0.sysvar(name, global)
    }
    fn uservar(&self, name: &str, at: Place) -> String {
        self.0.uservar(
            name,
            Place {
                arithmetic: false,
                ..at
            },
        )
    }
}

/// `sql` with every variable read replaced by a literal; `None` when it
/// reads none, assigns a user variable, or reads one the source doesn't
/// know.
#[allow(clippy::too_many_lines)] // one pass over the tokens: what each read becomes, then the text
pub(super) fn rewrite(sql: &[u8], opts: LexOptions, src: &dyn VarSource) -> Option<Vec<u8>> {
    if !sql.contains(&b'@') {
        return None;
    }
    let tokens = lex::lex(sql, &opts).ok()?;
    let sig: Vec<usize> = (0..tokens.len())
        .filter(|&i| !tokens[i].kind.is_trivia())
        .collect();
    let first = sig
        .iter()
        .find(|&&i| tokens[i].kind == TokenKind::Word)
        .map(|&i| tokens[i].bytes(sql))?;
    let reads = [
        &b"SELECT"[..],
        b"WITH",
        b"VALUES",
        b"INSERT",
        b"REPLACE",
        b"UPDATE",
        b"DELETE",
    ];
    if !reads.iter().any(|w| first.eq_ignore_ascii_case(w)) {
        return None;
    }
    let select = first.eq_ignore_ascii_case(b"SELECT");
    // A select item that reads a variable keeps its text as its name, as
    // on both servers (`v * @d`, not the value written in).
    let items = if select {
        super::sqlprep::unaliased_items(sql, opts)
    } else {
        Vec::new()
    };
    let arith = |n: Option<&usize>| {
        n.is_some_and(|&n| {
            let t = tokens[n];
            t.kind == TokenKind::Op && matches!(t.bytes(sql), b"+" | b"-" | b"*" | b"/" | b"%")
                || t.kind == TokenKind::Word
                    && (t.bytes(sql).eq_ignore_ascii_case(b"DIV")
                        || t.bytes(sql).eq_ignore_ascii_case(b"MOD"))
        })
    };
    let mut edits: Vec<(
        std::ops::Range<usize>,
        String,
        Option<std::ops::Range<usize>>,
    )> = Vec::new();
    for (k, &i) in sig.iter().enumerate() {
        let t = tokens[i];
        let next_assigns = sig.get(k + 1).is_some_and(|&n| {
            tokens[n].kind == TokenKind::Op && matches!(tokens[n].bytes(sql), b":=")
        });
        // `INTO @a, @b`: only variables and commas back to the `INTO`.
        let after_into = sig[..k]
            .iter()
            .rev()
            .find(|&&j| {
                !(tokens[j].kind == TokenKind::UserVar
                    || tokens[j].kind == TokenKind::Punct && sql[tokens[j].start as usize] == b',')
            })
            .is_some_and(|&j| {
                tokens[j].kind == TokenKind::Word
                    && tokens[j].bytes(sql).eq_ignore_ascii_case(b"INTO")
            });
        let lit = match t.kind {
            TokenKind::SysVar => {
                if next_assigns {
                    return None;
                }
                let (name, global) = sysvar_name(t.bytes(sql));
                let v = src.sysvar(&name, global)?;
                let kind = kind_of(&name);
                // In a query: typed as both servers type a variable (see
                // the expression engine's `SPRIG_SYSVAR`): text in
                // `utf8mb3`, switches as `BIGINT` of one digit, whole
                // numbers as `BIGINT` of 21 (unsigned but for a few).
                if select && (v.starts_with('\'') || v.starts_with("CONVERT(")) {
                    format!("SPRIG_SYSVAR({v})")
                } else if select && kind == Kind::Bool && (v == "1" || v == "0") {
                    format!("SPRIG_SYSVAR_BOOL({v})")
                } else if select
                    && kind == Kind::Num
                    && !v.is_empty()
                    && v.bytes().all(|b| b.is_ascii_digit())
                {
                    let sign = if SIGNED_NUMS.contains(&name.as_str()) {
                        "SIGNED"
                    } else {
                        "UNSIGNED"
                    };
                    format!("SPRIG_SYSVAR(CAST({v} AS {sign}))")
                } else {
                    v
                }
            }
            TokenKind::UserVar => {
                if next_assigns || after_into {
                    return None;
                }
                let at = Place {
                    select,
                    arithmetic: k > 0 && arith(sig.get(k - 1)) || arith(sig.get(k + 1)),
                };
                src.uservar(&uservar_name(t.bytes(sql)), at)
            }
            _ => continue,
        };
        let whole = select && whole_item(&tokens, &sig, sql, k);
        let (lit, item) = if whole {
            (
                format!(
                    "{lit} AS {}",
                    quoted(&String::from_utf8_lossy(t.bytes(sql)))
                ),
                None,
            )
        } else {
            let at = t.start as usize;
            (lit, items.iter().find(|r| r.contains(&at)).cloned())
        };
        edits.push((t.range(), lit, item));
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(sql.len() + 32);
    let mut at = 0;
    for (n, (r, lit, item)) in edits.iter().enumerate() {
        out.extend_from_slice(&sql[at..r.start]);
        out.extend_from_slice(lit.as_bytes());
        at = r.end;
        // After the item's last variable: its name.
        if let Some(item) = item
            && edits.get(n + 1).is_none_or(|e| e.2.as_ref() != Some(item))
        {
            out.extend_from_slice(&sql[at..item.end]);
            at = item.end;
            let name = String::from_utf8_lossy(&sql[item.clone()]).into_owned();
            out.extend_from_slice(format!(" AS {}", quoted(&name)).as_bytes());
        }
    }
    out.extend_from_slice(&sql[at..]);
    Some(out)
}

/// Whether `sql` reads or assigns a variable (`@@x`, `@x`), quoted names
/// and strings aside.
pub(super) fn reads_vars(sql: &[u8], opts: LexOptions) -> bool {
    if !sql.contains(&b'@') {
        return false;
    }
    match lex::lex(sql, &opts) {
        Ok(t) => t
            .iter()
            .any(|t| matches!(t.kind, TokenKind::SysVar | TokenKind::UserVar)),
        Err(_) => true,
    }
}

/// `SET @a = e [, @b := e ...]`: the assignments (name, expression text);
/// `None` for any other `SET`.
pub(super) fn user_sets(sql: &[u8], opts: LexOptions) -> Option<Vec<(String, String)>> {
    let tokens = lex::lex(sql, &opts).ok()?;
    let sig: Vec<usize> = (0..tokens.len())
        .filter(|&i| !tokens[i].kind.is_trivia())
        .collect();
    let mut sig = &sig[..];
    // A trailing `;`.
    if let Some(&l) = sig.last()
        && tokens[l].kind == TokenKind::Punct
        && sql[tokens[l].start as usize] == b';'
    {
        sig = &sig[..sig.len() - 1];
    }
    let (&first, mut rest) = sig.split_first()?;
    if !(tokens[first].kind == TokenKind::Word
        && tokens[first].bytes(sql).eq_ignore_ascii_case(b"SET"))
    {
        return None;
    }
    let mut out = Vec::new();
    loop {
        let (&v, r) = rest.split_first()?;
        let (&op, r) = r.split_first()?;
        if tokens[v].kind != TokenKind::UserVar
            || tokens[op].kind != TokenKind::Op
            || !matches!(tokens[op].bytes(sql), b"=" | b":=")
        {
            return None;
        }
        // The expression: up to a comma at depth 0.
        let mut depth = 0i32;
        let mut end = r.len();
        for (j, &i) in r.iter().enumerate() {
            if tokens[i].kind == TokenKind::Punct {
                match sql[tokens[i].start as usize] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    b',' if depth == 0 => {
                        end = j;
                        break;
                    }
                    _ => {}
                }
            }
        }
        if end == 0 {
            return None;
        }
        let (s, e) = (tokens[r[0]].start as usize, tokens[r[end - 1]].end as usize);
        out.push((
            uservar_name(tokens[v].bytes(sql)),
            String::from_utf8_lossy(&sql[s..e]).into_owned(),
        ));
        if end == r.len() {
            break;
        }
        rest = &r[end + 1..];
    }
    Some(out)
}

/// Every database of the tenant is on the engine.
pub(super) fn all_engine(cat: &TenantCatalog) -> bool {
    // A tenant with no database yet counts: every new database is an
    // engine database.
    let engine = catalog::of(cat);
    cat.databases
        .iter()
        .all(|d| engine.is_some_and(|e| e.dbs.contains(&d.db_id)))
}

/// The session's variables, read once per statement.
struct Snapshot {
    persona: Persona,
    nbe: bool,
    persona_vars: sprig_sql::compat::vars::PersonaVars,
    vars: std::collections::BTreeMap<String, String>,
    gateway_vars: std::collections::BTreeMap<String, String>,
    users: std::collections::BTreeMap<String, VarValue>,
    handshake: u16,
}

impl Snapshot {
    /// The value the handshake's collation gives a character-set variable.
    fn handshake_value(&self, name: &str) -> Option<String> {
        let (coll, primary) = charset::by_id(self.handshake)?;
        let cs = charset::charset_of(coll);
        if name == "collation_connection" {
            if self.persona == Persona::MariaDb && primary {
                let m = self
                    .vars
                    .get("character_set_collations")
                    .map_or(charset::DEFAULT_CHARSET_COLLATIONS, String::as_str);
                if let Some(c) = charset::mapped(m, cs) {
                    return Some(c.to_owned());
                }
            }
            return Some(coll.to_owned());
        }
        Some(cs.to_owned())
    }
}

impl VarSource for Snapshot {
    fn sysvar(&self, name: &str, global: bool) -> Option<String> {
        let kind = kind_of(name);
        if let Some(v) = self.persona_vars.value_at(name, global) {
            return Some(literal(kind, &v, self.nbe));
        }
        if !global {
            // The MySQL port's `SET cte_max_recursion_depth` is kept under
            // MariaDB's name for it.
            if self.persona == Persona::MySql84
                && name == "cte_max_recursion_depth"
                && let Some(v) = self.vars.get("max_recursive_iterations")
            {
                return Some(literal(kind, v, self.nbe));
            }
            if let Some(v) = self.gateway_vars.get(name).or_else(|| self.vars.get(name)) {
                if name == "character_set_results" && v == "NULL" {
                    return Some("NULL".to_owned());
                }
                // The backend names MySQL's utf8mb4 default by MariaDB's.
                if self.persona == Persona::MySql84
                    && name.starts_with("collation_")
                    && v.eq_ignore_ascii_case("utf8mb4_uca1400_ai_ci")
                {
                    return Some(literal(kind, "utf8mb4_0900_ai_ci", self.nbe));
                }
                return Some(literal(kind, v, self.nbe));
            }
            if HANDSHAKE_VARS.contains(&name) {
                return self
                    .handshake_value(name)
                    .map(|v| literal(Kind::Str, &v, self.nbe));
            }
        } else if HANDSHAKE_VARS.contains(&name) {
            // The server's.
            let v = if name == "collation_connection" {
                default_of(self.persona, "collation_server")?.1
            } else {
                "utf8mb4"
            };
            return Some(literal(Kind::Str, v, self.nbe));
        }
        let (kind, v) = default_of(self.persona, name)?;
        Some(literal(kind, v, self.nbe))
    }

    fn uservar(&self, name: &str, at: Place) -> String {
        let mysql = self.persona == Persona::MySql84;
        match self.users.get(name) {
            // The MySQL port: text in the session's own collation as a
            // string literal, which takes on the collation of what it meets
            // (the same as MySQL's for the port's tables, and it still
            // compares with tables created before they got MySQL's
            // collation).
            Some(VarValue::Text(b)) if mysql => plain_text(b, self.nbe),
            Some(VarValue::Collated(b, c))
                if mysql
                    && (c.eq_ignore_ascii_case("utf8mb4_0900_ai_ci")
                        || c.eq_ignore_ascii_case("utf8mb4_uca1400_ai_ci")) =>
            {
                plain_text(b, self.nbe)
            }
            // Text in another collation, in a query: the variable's
            // collation with a column's weight, so a column of another
            // collation is MySQL's 1267.
            Some(v @ VarValue::Collated(..)) if at.select => {
                format!("SPRIG_IMPLICIT({})", v.literal())
            }
            // A decimal in arithmetic has the servers' scale for decimal
            // variables (30 on MySQL, 38 on MariaDB): `v * @d` prints them.
            Some(VarValue::Decimal(d)) if at.arithmetic && !d.contains(['e', 'E']) => {
                format!("CAST({d} AS DECIMAL(65,{}))", if mysql { 30 } else { 38 })
            }
            v => v.unwrap_or(&VarValue::Null).literal(),
        }
    }
}

/// Text as a string literal; bytes that aren't UTF-8 as a converted hex
/// string.
fn plain_text(b: &[u8], nbe: bool) -> String {
    match std::str::from_utf8(b) {
        Ok(t) => sprig_sql::names::rewrite::sql_string(t, nbe),
        Err(_) => VarValue::Text(b.to_vec()).literal(),
    }
}

/// The session's user variables as one statement reads and assigns them
/// (`@v`, `@v := e` in a query the planner runs): seeded from the session's
/// values, the assigned ones written back when the statement ends.
#[derive(Debug, Default)]
pub(super) struct StatementVars {
    vars:
        std::sync::Mutex<std::collections::BTreeMap<String, (sprig_engine::row::Value, ExprType)>>,
    changed: std::sync::Mutex<std::collections::BTreeSet<String>>,
    warned: std::sync::atomic::AtomicBool,
}

impl sprig_exec::UserVars for StatementVars {
    fn get(&self, name: &str) -> Option<(sprig_engine::row::Value, ExprType)> {
        self.vars.lock().ok()?.get(name).cloned()
    }

    fn set(&self, name: &str, value: sprig_engine::row::Value, ty: ExprType) {
        if let Ok(mut v) = self.vars.lock() {
            v.insert(name.to_owned(), (value, ty));
        }
        if let Ok(mut c) = self.changed.lock() {
            c.insert(name.to_owned());
        }
    }

    fn first_assignment(&self) -> bool {
        !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed)
    }
}

/// The session's `utf8mb4` collation on each port.
fn utf8_collation(persona: Persona) -> Collation {
    if persona == Persona::MySql84 {
        Collation::Utf8mb4Mysql0900AiCi
    } else {
        Collation::Utf8mb4Uca1400AiCi
    }
}

/// A kept user variable as a value of the expression engine, with its
/// type; `None` for `NULL`.
fn var_value(v: &VarValue, persona: Persona) -> Option<(sprig_engine::row::Value, ExprType)> {
    use sprig_engine::row::Value;
    // Text on the MySQL port takes on the collation of what it meets (see
    // `Snapshot::uservar`).
    let deriv = if persona == Persona::MySql84 {
        Derivation::Coercible
    } else {
        Derivation::Implicit
    };
    let text = |b: &[u8], coll: Collation| {
        let n = u32::try_from(String::from_utf8_lossy(b).chars().count()).unwrap_or(u32::MAX);
        (
            Value::Str(Bytes::copy_from_slice(b)),
            ExprType::string(n, coll, deriv),
        )
    };
    Some(match v {
        VarValue::Null => return None,
        // An integer variable is a BIGINT.
        VarValue::Integer(s) => {
            let (v, mut ty) = match s.parse::<i64>() {
                Ok(i) => (Value::Int(i), ExprType::int(20, false)),
                Err(_) => (Value::UInt(s.parse().ok()?), ExprType::int(20, true)),
            };
            ty.width = 8;
            (v, ty)
        }
        VarValue::Decimal(s) if s.contains(['e', 'E']) => {
            (Value::Double(s.parse().ok()?), ExprType::double())
        }
        VarValue::Decimal(s) => {
            let d = sprig_engine::row::decimal::Decimal::parse(s)?;
            let scale = u32::from(d.scale());
            (Value::Decimal(d), ExprType::decimal(65, scale, false))
        }
        VarValue::Text(b) => text(b, utf8_collation(persona)),
        VarValue::Collated(b, c) => {
            let flavor = if persona == Persona::MySql84 {
                sprig_engine::keys::collation::Flavor::MySql
            } else {
                sprig_engine::keys::collation::Flavor::MariaDb
            };
            let coll = Collation::resolve(c, flavor).unwrap_or_else(|| utf8_collation(persona));
            let (v, mut ty) = text(b, coll);
            let l = c.to_ascii_lowercase();
            ty.mb3 = l.starts_with("utf8mb3_") || l.starts_with("utf8_");
            // Another collation than the session's own: the variable's, as
            // on MySQL (a column of a third collation is 1267).
            if coll != utf8_collation(persona) && coll != Collation::Utf8mb4Uca1400AiCi {
                ty.deriv = Derivation::Implicit;
            }
            (v, ty)
        }
        VarValue::Binary(b) => {
            let n = u32::try_from(b.len()).unwrap_or(u32::MAX);
            (
                Value::Bytes(Bytes::copy_from_slice(b)),
                ExprType::string(n, Collation::Binary, Derivation::Implicit),
            )
        }
    })
}

/// An assigned value as the session keeps user variables.
fn kept_value(v: &sprig_engine::row::Value, ty: &ExprType, persona: Persona) -> VarValue {
    use sprig_engine::row::Value;
    let exec = if persona == Persona::MySql84 {
        sprig_exec::Persona::MySql
    } else {
        sprig_exec::Persona::MariaDb
    };
    let printed = || sprig_exec::value_text(v, ty, exec).map(|b| b.to_vec());
    match v {
        Value::Null => VarValue::Null,
        Value::Int(i) => VarValue::Integer(i.to_string()),
        Value::UInt(u) => VarValue::Integer(u.to_string()),
        Value::Decimal(d) => VarValue::Decimal(d.to_string()),
        Value::Double(_) | Value::Float(_) => printed().map_or(VarValue::Null, |b| {
            VarValue::Decimal(String::from_utf8_lossy(&b).into_owned())
        }),
        Value::Bytes(b) => VarValue::Binary(b.to_vec()),
        // The session's own `utf8mb4` collation (on the MySQL port the engine
        // holds MySQL's default as MariaDB's equivalent): plain text.
        Value::Str(b)
            if !ty.mb3
                && (ty.coll == utf8_collation(persona)
                    || ty.coll == Collation::Utf8mb4Uca1400AiCi) =>
        {
            VarValue::Text(b.to_vec())
        }
        Value::Str(b) => VarValue::Collated(
            b.to_vec(),
            sprig_exec::types::collation_name(ty.coll, ty.mb3).into_owned(),
        ),
        _ => printed().map_or(VarValue::Null, VarValue::Text),
    }
}

/// Keeps one statement's first row.
struct Capture {
    wire: Wire,
    columns: Vec<ColumnDef>,
    row: Option<Vec<Option<Vec<u8>>>>,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait::async_trait]
impl ResultSink for Capture {
    fn wire(&self) -> Wire {
        self.wire
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.to_vec();
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        if let RowValues::Text(v) = values
            && self.row.is_none()
        {
            self.row = Some(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect());
        }
        Ok(())
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        if self.row.is_none() {
            self.row = Some(
                sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len())?
                    .into_iter()
                    .map(|c| c.map(<[u8]>::to_vec))
                    .collect(),
            );
        }
        Ok(())
    }

    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn ok(&mut self, _ok: OkPacket) -> Result<()> {
        self.done = true;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }

    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Ok(())
    }

    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::internal("LOAD DATA in a variable's value"))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.done
    }
}

/// A captured column's value as a user variable's, or `None` when the
/// text isn't UTF-8 or ASCII (its character set isn't the variable's).
fn captured(col: &ColumnDef, v: Option<&[u8]>) -> Option<VarValue> {
    if let Some(b) = v
        && !b.is_ascii()
        && col.charset != 63
        && !charset::by_id(col.charset).is_some_and(|(n, _)| n.starts_with("utf8"))
    {
        return None;
    }
    Some(VarValue::from_column(col.col_type, col.charset, v))
}

impl PassSession {
    fn var_snapshot(&mut self, cx: &ExecCtx<'_>) -> Snapshot {
        let persona = self.persona;
        let nbe = self.lex_opts().0.no_backslash_escapes;
        let mut persona_vars = self.persona_vars();
        persona_vars = persona_vars.with_tenant_globals(self.tenant_globals());
        let handshake = cx.proto.collation;
        // The engine keeps the session's lock wait itself.
        let lock_wait = self.eng.lock_wait;
        let bs = self.bs(cx);
        let v = bs.vars();
        let mut vars = v.vars.clone();
        if let Some(n) = lock_wait {
            vars.insert("innodb_lock_wait_timeout".to_owned(), n.to_string());
        }
        Snapshot {
            persona,
            nbe,
            persona_vars,
            vars,
            gateway_vars: v.gateway_vars.clone(),
            users: v.user_vars.clone(),
            handshake,
        }
    }

    /// User variable `name`'s value as text; `None` when it is `NULL` or
    /// unset.
    pub(super) fn user_var_text(&mut self, cx: &ExecCtx<'_>, name: &str) -> Option<Vec<u8>> {
        match self.bs(cx).vars().user_vars.get(name)? {
            VarValue::Null => None,
            VarValue::Integer(s) | VarValue::Decimal(s) => Some(s.clone().into_bytes()),
            VarValue::Text(b) | VarValue::Binary(b) | VarValue::Collated(b, _) => Some(b.clone()),
        }
    }

    /// A statement that assigns user variables inside a query (`@v :=
    /// e`): its handle on the session's variables, seeded with their
    /// values; `None` for any other statement.
    pub(super) fn statement_vars(
        &mut self,
        cx: &ExecCtx<'_>,
        sql: &[u8],
    ) -> Option<std::sync::Arc<StatementVars>> {
        if !sql.contains(&b'@') || !sql.windows(2).any(|w| w == b":=") {
            return None;
        }
        let persona = self.persona;
        let seeded = self
            .bs(cx)
            .vars()
            .user_vars
            .iter()
            .filter_map(|(k, v)| Some((k.clone(), var_value(v, persona)?)))
            .collect();
        Some(std::sync::Arc::new(StatementVars {
            vars: std::sync::Mutex::new(seeded),
            ..StatementVars::default()
        }))
    }

    /// The variables a statement assigned become the session's.
    pub(super) fn statement_vars_end(&mut self, cx: &ExecCtx<'_>, h: &StatementVars) {
        let persona = self.persona;
        let (Ok(changed), Ok(vars)) = (h.changed.lock(), h.vars.lock()) else {
            return;
        };
        let kept: Vec<(String, VarValue)> = changed
            .iter()
            .filter_map(|n| {
                let (v, ty) = vars.get(n)?;
                Some((n.clone(), kept_value(v, ty, persona)))
            })
            .collect();
        drop((changed, vars));
        let bs = self.bs(cx);
        for (n, v) in kept {
            bs.vars_mut().user_vars.insert(n, v);
        }
    }

    /// The statement with its variable reads replaced (see the module
    /// docs); `None` when it reads none or the gateway can't answer one.
    pub(super) fn engine_var_reads(
        &mut self,
        cx: &ExecCtx<'_>,
        cat: &TenantCatalog,
        sql: &[u8],
    ) -> Option<Bytes> {
        if !sql.contains(&b'@') || self.backend_in_txn() || !all_engine(cat) {
            return None;
        }
        let lex = self.lex_opts().0;
        let snap = self.var_snapshot(cx);
        rewrite(sql, lex, &snap).map(Bytes::from)
    }

    /// `SET @a = e, ...` run on the engine (see the module docs): `Some`
    /// once answered, `None` when MariaDB still answers it.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn engine_user_set(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &std::sync::Arc<TenantCatalog>,
        current: Option<DbId>,
        sql: &[u8],
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if self.backend_in_txn() || !all_engine(cat) {
            return Ok(None);
        }
        let lex = self.lex_opts().0;
        let Some(sets) = user_sets(sql, lex) else {
            return Ok(None);
        };
        let default_conn = self
            .bs(cx)
            .vars()
            .vars
            .get("collation_connection")
            .is_none_or(|c| {
                c.eq_ignore_ascii_case("utf8mb4_uca1400_ai_ci")
                    || c.eq_ignore_ascii_case("utf8mb4_0900_ai_ci")
            });
        for (name, expr) in &sets {
            // A value given a collation keeps it (`SET @v = 'x' COLLATE
            // utf8mb4_bin`), and so does text in a session whose
            // collation isn't the port's default (a MySQL client's
            // `utf8mb4_general_ci` login): its name is read with it.
            let collated = expr.to_ascii_uppercase().contains("COLLATE") || !default_conn;
            let select = if collated {
                format!("SELECT {expr}, COLLATION({expr})")
            } else {
                format!("SELECT {expr}")
            };
            let snap = self.var_snapshot(cx);
            let text = rewrite(select.as_bytes(), lex, &Assigned(&snap))
                .unwrap_or_else(|| select.into_bytes());
            let mut cap = Capture {
                wire: out.wire(),
                columns: Vec::new(),
                row: None,
                error: None,
                done: false,
            };
            let text = Bytes::from(text);
            let step = Box::pin(self.engine_text_in(
                cx,
                cat,
                current,
                &text,
                None,
                false,
                RowFormat::Text,
                &mut cap,
            ))
            .await?;
            if step.is_none() {
                // MariaDB answers the whole `SET`; what was kept so far
                // goes with it (the same values).
                return Ok(None);
            }
            if let Some(e) = cap.error {
                return self.fail(out, &e).await.map(Some);
            }
            let (Some(col), Some(row)) = (cap.columns.first(), cap.row.as_ref()) else {
                return Ok(None);
            };
            let Some(v) = captured(col, row.first().and_then(|c| c.as_deref())) else {
                return Ok(None);
            };
            let v = if collated {
                v.with_collation(row.get(1).and_then(|c| c.as_deref()))
            } else {
                v
            };
            self.bs(cx).vars_mut().user_vars.insert(name.clone(), v);
        }
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }
}

/// `DO e, ...`: the expressions' text; `None` for any other statement.
pub(super) fn do_exprs(sql: &[u8], opts: LexOptions) -> Option<String> {
    let tokens = lex::lex(sql, &opts).ok()?;
    let mut sig = tokens.iter().filter(|t| !t.kind.is_trivia());
    let first = sig.next()?;
    if !(first.kind == TokenKind::Word && first.bytes(sql).eq_ignore_ascii_case(b"DO")) {
        return None;
    }
    let mut rest: Vec<&Token> = sig.collect();
    if let Some(l) = rest.last()
        && l.kind == TokenKind::Punct
        && sql[l.start as usize] == b';'
    {
        rest.pop();
    }
    let (s, e) = (rest.first()?.start as usize, rest.last()?.end as usize);
    Some(String::from_utf8_lossy(&sql[s..e]).into_owned())
}

impl PassSession {
    /// `DO e, ...` run as `SELECT e, ...` on the engine, its rows dropped:
    /// `Some` once answered.
    pub(super) async fn engine_do(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &std::sync::Arc<TenantCatalog>,
        current: Option<DbId>,
        sql: &[u8],
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if self.backend_in_txn() || !all_engine(cat) {
            return Ok(None);
        }
        let lex = self.lex_opts().0;
        let Some(exprs) = do_exprs(sql, lex) else {
            return Ok(None);
        };
        let select = format!("SELECT {exprs}");
        let snap = self.var_snapshot(cx);
        let text = Bytes::from(
            rewrite(select.as_bytes(), lex, &snap).unwrap_or_else(|| select.into_bytes()),
        );
        let mut cap = Capture {
            wire: out.wire(),
            columns: Vec::new(),
            row: None,
            error: None,
            done: false,
        };
        let step = Box::pin(self.engine_text_in(
            cx,
            cat,
            current,
            &text,
            None,
            false,
            RowFormat::Text,
            &mut cap,
        ))
        .await?;
        if step.is_none() {
            return Ok(None);
        }
        if let Some(e) = cap.error {
            return self.fail(out, &e).await.map(Some);
        }
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// A named-lock statement (`GET_LOCK` and its kin, see
    /// [`super::locks`]) answered in the gateway: `Some` once answered.
    /// `sql` is the client's text (the name layer scopes lock names for
    /// MariaDB).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn engine_named_lock(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &std::sync::Arc<TenantCatalog>,
        current: Option<DbId>,
        sql: &[u8],
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !all_engine(cat) {
            return Ok(None);
        }
        let lex = self.lex_opts().0;
        let Some(l) = super::locks::parse(sql, lex) else {
            return Ok(None);
        };
        let (tenant, session) = (cat.tenant.0, cx.session.id.0);
        if self.eng.locks.is_none() {
            self.eng.locks = Some(super::locks::Owner { tenant, session });
        }
        let v = super::locks::run(tenant, session, &l.call).await;
        if l.is_do {
            let sv = self.engine_vars(cx);
            let mut status = self.engine_status(&sv);
            if more {
                status |= StatusFlags::MORE_RESULTS_EXISTS;
            }
            out.ok(OkPacket::new(status)).await?;
            return Ok(Some(Step {
                errored: false,
                status: Some(status),
            }));
        }
        let value = v.map_or_else(|| "NULL".to_owned(), |v| v.to_string());
        let text = Bytes::from(format!("SELECT {value} AS {}", quoted(&l.column)));
        self.eng.lock_answer = true;
        let r =
            Box::pin(self.engine_text_in(cx, cat, current, &text, None, more, format, out)).await;
        self.eng.lock_answer = false;
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed;
    impl VarSource for Fixed {
        fn sysvar(&self, name: &str, global: bool) -> Option<String> {
            match (name, global) {
                ("autocommit", _) => Some("1".into()),
                ("sql_mode", false) => Some("'STRICT'".into()),
                _ => None,
            }
        }
        fn uservar(&self, name: &str, _at: Place) -> String {
            if name == "x" {
                "5".into()
            } else {
                "NULL".into()
            }
        }
    }

    fn rw(s: &str) -> Option<String> {
        rewrite(s.as_bytes(), LexOptions::mariadb(), &Fixed).map(|v| String::from_utf8(v).unwrap())
    }

    #[test]
    fn reads_become_literals() {
        assert_eq!(
            rw("SELECT @@autocommit, @@session.sql_mode AS m"),
            Some(
                "SELECT SPRIG_SYSVAR_BOOL(1) AS `@@autocommit`, SPRIG_SYSVAR('STRICT') AS m".into()
            )
        );
        assert_eq!(
            rw("select @x + 1, @`x`"),
            Some("select 5 + 1 AS `@x + 1`, 5 AS `@``x```".into())
        );
        assert_eq!(
            rw("SELECT * FROM t WHERE id = @x"),
            Some("SELECT * FROM t WHERE id = 5".into())
        );
        assert_eq!(rw("SELECT @y"), Some("SELECT NULL AS `@y`".into()));
    }

    #[test]
    fn left_alone() {
        for s in [
            "SELECT 1",
            "SELECT @@unknown_thing",
            "SELECT @@global.sql_mode",
            "SELECT @x := 1",
            "SELECT 1 INTO @x",
            "SELECT 1, 2 INTO @x, @y",
            "SET @x = @@autocommit",
            "SHOW VARIABLES LIKE '@x'",
        ] {
            assert_eq!(rw(s), None, "{s}");
        }
    }

    #[test]
    fn user_set_forms() {
        let o = LexOptions::mariadb();
        assert_eq!(
            user_sets(b"SET @a = 1, @`B` := (1, 2) IN ((1, 2)), @c = 'x';", o),
            Some(vec![
                ("a".into(), "1".into()),
                ("b".into(), "(1, 2) IN ((1, 2))".into()),
                ("c".into(), "'x'".into())
            ])
        );
        assert_eq!(user_sets(b"SET @a = 1, autocommit = 0", o), None);
        assert_eq!(user_sets(b"SET autocommit = 0", o), None);
        assert_eq!(user_sets(b"SET @a", o), None);
    }

    #[test]
    fn do_forms() {
        let o = LexOptions::mariadb();
        assert_eq!(do_exprs(b"DO 1, SLEEP(0);", o), Some("1, SLEEP(0)".into()));
        assert_eq!(do_exprs(b"DO", o), None);
        assert_eq!(do_exprs(b"SELECT 1", o), None);
    }

    #[test]
    fn literals() {
        assert_eq!(literal(Kind::Bool, "ON", false), "1");
        assert_eq!(literal(Kind::Num, "28800", false), "28800");
        assert_eq!(literal(Kind::Str, "it's", true), "'it''s'");
        assert_eq!(default_of(Persona::MySql84, "tx_isolation"), None);
        assert_eq!(
            default_of(Persona::MariaDb, "lock_wait_timeout"),
            Some((Kind::Num, "86400"))
        );
    }
}
