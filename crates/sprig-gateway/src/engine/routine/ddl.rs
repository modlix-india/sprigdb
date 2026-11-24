//! `CREATE`, `ALTER` and `DROP PROCEDURE | FUNCTION` on engine databases:
//! the catalog's DDL path runs them (existence, `OR REPLACE`, `IF [NOT]
//! EXISTS`, the publish); this module produces the text the catalog stores.

use sprig_common::{Result, SprigError};

use super::super::define::{self, Created};
use super::parse::{self, Kind};
use super::{Context, show_text, stored};

/// What a routine statement records about its session.
#[derive(Debug, Clone, Default)]
pub(crate) struct Session {
    /// The client's text of the statement (names as written).
    pub entry: Option<String>,
    /// `sql_mode`.
    pub sql_mode: String,
    /// `character_set_client`.
    pub charset_client: String,
    /// `collation_connection`.
    pub collation_connection: String,
    /// Now, as `YYYY-MM-DD hh:mm:ss`.
    pub now: String,
}

/// The default `sql_mode` of each server.
pub(crate) fn default_sql_mode(mysql: bool) -> &'static str {
    if mysql {
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
    } else {
        "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
    }
}

/// A type as `SHOW CREATE FUNCTION` prints a return type: the column type
/// as `SHOW CREATE TABLE` prints it, with the character set (and on
/// MariaDB the collation) of a string type.
pub(crate) fn render_type(ty: &str, mysql: bool, db_cs: &(String, String)) -> Result<String> {
    let cx = define::Ctx {
        mysql,
        db_cs: db_cs.clone(),
    };
    let def = match define::create(&format!("CREATE TABLE `r` (`v` {ty})"), &cx)? {
        Created::Def(d) => d,
        Created::Like(..) => return Err(SprigError::internal("routine type read as LIKE")),
    };
    let text = define::render(&def);
    let line = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("`v` "))
        .unwrap_or(ty)
        .trim_end_matches(',')
        .to_owned();
    let mut t = line
        .split(" DEFAULT ")
        .next()
        .unwrap_or(&line)
        .trim_end_matches(" NULL")
        .to_owned();
    let lower = t.to_ascii_lowercase();
    let stringy = [
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
        "enum",
        "set",
    ]
    .iter()
    .any(|k| lower.starts_with(k) && lower[k.len()..].starts_with(['(', ' ']) || lower == *k);
    if stringy {
        if let Some(i) = t.find(" CHARACTER SET ") {
            t.replace_range(i..i + 15, " CHARSET ");
        } else if !t.contains(" CHARSET ") {
            let (cs, co) = db_cs;
            let (cs, co) = if cs.is_empty() {
                if mysql {
                    ("utf8mb4", "utf8mb4_0900_ai_ci")
                } else {
                    ("utf8mb4", "utf8mb4_uca1400_ai_ci")
                }
            } else {
                (cs.as_str(), co.as_str())
            };
            t.push_str(" CHARSET ");
            t.push_str(cs);
            if !mysql {
                t.push_str(" COLLATE ");
                t.push_str(co);
            }
        }
        if mysql && let Some(i) = t.find(" COLLATE ") {
            t.truncate(i);
        }
    }
    Ok(t)
}

/// The catalog text of a `CREATE PROCEDURE | FUNCTION` (`sql`: the
/// statement as the catalog runs it; the session's own text is preferred
/// when it is the same statement).
pub(crate) fn create(
    sql: &str,
    sess: &Session,
    db_name: &str,
    account: &(String, String),
    mysql: bool,
    db_cs: &(String, String),
) -> Result<String> {
    let text = sess
        .entry
        .as_deref()
        .filter(|e| parse::parse_create(e).is_ok())
        .unwrap_or(sql);
    let c = parse::parse_create(text)?;
    // MariaDB has no `GET STACKED DIAGNOSTICS`.
    if !mysql
        && let Ok(toks) = super::super::define::lex::lex(text)
        && let Some(t) = toks
            .windows(2)
            .find(|w| w[0].is("STACKED") && w[1].is("DIAGNOSTICS"))
    {
        return Err(parse::syntax(text, t[0].at));
    }
    super::code::compile(&c, db_name)?;
    let definer = match &c.definer {
        Some((u, h)) if !u.is_empty() || !h.is_empty() => (u.clone(), h.clone()),
        _ => account.clone(),
    };
    let returns = match (&c.kind, &c.returns) {
        (Kind::Function, Some(t)) => render_type(t, mysql, db_cs)?,
        _ => String::new(),
    };
    let show = show_text(&c, &definer, &returns);
    let cx = Context {
        sql_mode: if sess.sql_mode.is_empty() {
            default_sql_mode(mysql).to_owned()
        } else {
            sess.sql_mode.clone()
        },
        charset_client: or(&sess.charset_client, "utf8mb4"),
        collation_connection: or(
            &sess.collation_connection,
            if mysql {
                "utf8mb4_0900_ai_ci"
            } else {
                "utf8mb4_uca1400_ai_ci"
            },
        ),
        db_collation: or(
            &db_cs.1,
            if mysql {
                "utf8mb4_0900_ai_ci"
            } else {
                "utf8mb4_uca1400_ai_ci"
            },
        ),
        created: sess.now.clone(),
        modified: sess.now.clone(),
        definer,
        order: 0,
    };
    Ok(stored(&cx, &show))
}

fn or(s: &str, d: &str) -> String {
    if s.is_empty() {
        d.to_owned()
    } else {
        s.to_owned()
    }
}

/// `ALTER PROCEDURE | FUNCTION name characteristic ...` applied to the
/// routine's catalog text `old`.
pub(crate) fn alter(old: &str, sql: &str, sess: &Session) -> Result<String> {
    let r = super::load(
        sprig_common::ids::ObjectId(0),
        sprig_common::ids::DbId(0),
        "",
        "",
        old,
    );
    let (mut c, _) = r.def.map_err(|e| {
        super::not_on_engine(&format!("ALTER of a routine the engine can't read: {e}"))
    })?;
    let mut p = parse::P::new(sql)?;
    let ch = p.alter_chars()?;
    if let Some(v) = ch.comment {
        c.chars.comment = Some(v);
    }
    if let Some(v) = ch.access {
        c.chars.access = Some(v);
    }
    if let Some(v) = ch.invoker {
        c.chars.invoker = Some(v);
    }
    let returns = r
        .show
        .lines()
        .next()
        .and_then(|l| l.split_once(") RETURNS ").map(|(_, t)| t.to_owned()))
        .unwrap_or_default();
    let show = show_text(&c, &r.cx.definer, &returns);
    let mut cx = r.cx;
    cx.modified.clone_from(&sess.now);
    Ok(stored(&cx, &show))
}
