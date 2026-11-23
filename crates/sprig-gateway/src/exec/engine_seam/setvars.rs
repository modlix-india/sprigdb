//! `SET` of session variables when every database of the tenant is on the
//! engine: the gateway keeps the value, and MariaDB hears nothing.
//!
//! | Statement | Kept as |
//! |---|---|
//! | `SET [SESSION \| LOCAL] v = ON \| OFF \| TRUE \| FALSE \| 1 \| 0`, `SET @@[session.]v = ...` (a switch below) | `ON` / `OFF` |
//! | `SET [SESSION] transaction_isolation \| tx_isolation = 'READ-COMMITTED'` (and the other three) | both names, upper case |
//! | `SET [SESSION] transaction_read_only \| tx_read_only = ...` | both names, `ON` / `OFF` |
//! | `SET [SESSION] innodb_lock_wait_timeout = n` (1 to 1073741824), `= DEFAULT` | the engine's lock wait |
//! | `SET SESSION TRANSACTION ISOLATION LEVEL ... \| READ ONLY \| READ WRITE` | the two variables above |
//! | `SET NAMES cs [COLLATE c]` (a character set the engine knows) | `character_set_client`, `_connection`, `_results`, `collation_connection` (without `COLLATE`: the session's `character_set_collations` entry, else the default) |
//!
//! Switches: `autocommit`, `foreign_key_checks`, `unique_checks`,
//! `sql_notes`, `sql_warnings`, `sql_safe_updates`, `sql_auto_is_null`,
//! `sql_big_selects`, `sql_quote_show_create`, `big_tables`.
//!
//! Anything else still goes to MariaDB as before: other variables, an
//! expression or a quoted switch value, a value the servers would refuse
//! or adjust with a warning (their exact errors and warnings stay theirs),
//! `GLOBAL`, user variables, `SET NAMES DEFAULT`. So do the isolation and
//! read-only variables inside a transaction, and with `@@` and no scope
//! (MySQL then sets the next transaction only).
//!
//! The value goes into the session's variables
//! ([`crate::backend::BackendSession::vars_mut`]), where the engine path
//! reads it and where state sync would replay it if a statement still
//! reached MariaDB. The OK reports each variable in session tracking, as
//! the relay reported MariaDB's.

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::Result;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::{OkPacket, SessionChange};
use sprig_sql::lex::{self, LexOptions, Token, TokenKind};

use super::super::PassSession;
use super::super::tenant::Step;
use crate::engine::catalog;
use crate::pipeline::{ExecCtx, ResultSink};

/// The switches kept by value.
const SWITCHES: &[&str] = &[
    "autocommit",
    "foreign_key_checks",
    "unique_checks",
    "sql_notes",
    "sql_warnings",
    "sql_safe_updates",
    "sql_auto_is_null",
    "sql_big_selects",
    "sql_quote_show_create",
    "big_tables",
];

const ISOLATION: &[&str] = &["transaction_isolation", "tx_isolation"];
const READ_ONLY: &[&str] = &["transaction_read_only", "tx_read_only"];

/// `innodb_lock_wait_timeout`'s range on both servers.
const LOCK_WAIT_MAX: u64 = 1_073_741_824;

/// One assignment the gateway keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Assign {
    /// Variables kept by value (aliases share one), as MariaDB's session
    /// tracking reports it.
    Var {
        /// The names: the first is the one reported.
        names: Vec<&'static str>,
        /// The value.
        value: String,
    },
    /// `innodb_lock_wait_timeout`: seconds, or `None` for `DEFAULT`.
    LockWait(Option<u64>),
    /// `SET NAMES cs [COLLATE c]`: the character set as the server names
    /// it, and the collation (checked to belong to it) when one is given.
    Names {
        /// The character set.
        charset: &'static str,
        /// The collation written, if any.
        collation: Option<String>,
    },
}

impl Assign {
    /// It sets the isolation level or the access mode.
    fn transactional(&self) -> bool {
        matches!(self, Assign::Var { names, .. }
            if names.iter().any(|n| ISOLATION.contains(n) || READ_ONLY.contains(n)))
    }

    /// The `autocommit` value it sets, if any.
    fn autocommit(&self) -> Option<bool> {
        match self {
            Assign::Var { names, value } if names.first() == Some(&"autocommit") => {
                Some(value == "ON")
            }
            _ => None,
        }
    }
}

/// The significant tokens of `sql`, a trailing `;` dropped.
fn tokens(sql: &[u8], opts: LexOptions) -> Option<Vec<Token>> {
    let mut t: Vec<Token> = lex::lex(sql, &opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    if t.last().is_some_and(|l| l.bytes(sql) == b";") {
        t.pop();
    }
    Some(t)
}

fn word(t: &Token, sql: &[u8]) -> Option<String> {
    (t.kind == TokenKind::Word).then(|| String::from_utf8_lossy(t.bytes(sql)).to_ascii_lowercase())
}

/// A switch's value: `ON`/`OFF`.
fn switch(t: &Token, sql: &[u8]) -> Option<&'static str> {
    let text = String::from_utf8_lossy(t.bytes(sql)).to_ascii_uppercase();
    match (t.kind, text.as_str()) {
        (TokenKind::Word, "ON" | "TRUE") | (TokenKind::Number, "1") => Some("ON"),
        (TokenKind::Word, "OFF" | "FALSE") | (TokenKind::Number, "0") => Some("OFF"),
        _ => None,
    }
}

/// An isolation level written as the variable's value: one of the four
/// names with hyphens, any case.
fn isolation_value(t: &Token, sql: &[u8], opts: LexOptions) -> Option<String> {
    if t.kind != TokenKind::String {
        return None;
    }
    let v = lex::string_value(t.bytes(sql), opts.no_backslash_escapes)?;
    let v = String::from_utf8(v).ok()?.to_ascii_uppercase();
    matches!(
        v.as_str(),
        "READ-UNCOMMITTED" | "READ-COMMITTED" | "REPEATABLE-READ" | "SERIALIZABLE"
    )
    .then_some(v)
}

/// The assignments of `sql` when it is a `SET` the gateway keeps whole
/// (see the module docs); `None` otherwise.
pub(super) fn parse(sql: &[u8], opts: LexOptions) -> Option<Vec<Assign>> {
    let t = tokens(sql, opts)?;
    if word(t.first()?, sql)? != "set" {
        return None;
    }
    let second = word(t.get(1)?, sql);
    let third = t.get(2).and_then(|x| word(x, sql));
    if matches!(second.as_deref(), Some("session" | "local"))
        && third.as_deref() == Some("transaction")
    {
        return transaction(&t[3..], sql);
    }
    if second.as_deref() == Some("names") {
        return names(&t[2..], sql, opts).map(|a| vec![a]);
    }
    let mut out = Vec::new();
    let mut i = 1;
    loop {
        let (name, plain_sysvar) = target(&t, sql, &mut i)?;
        let op = t.get(i)?;
        if !(op.kind == TokenKind::Op && matches!(op.bytes(sql), b"=" | b":=")) {
            return None;
        }
        let value = t.get(i + 1)?;
        i += 2;
        let a = assignment(&name, value, sql, opts)?;
        if plain_sysvar && a.transactional() {
            return None;
        }
        out.push(a);
        match t.get(i) {
            None => return Some(out),
            Some(c) if c.kind == TokenKind::Punct && c.bytes(sql) == b"," => i += 1,
            Some(_) => return None,
        }
    }
}

/// The variable an assignment names, lower case, and whether it was
/// written `@@name` (no scope).
fn target(t: &[Token], sql: &[u8], i: &mut usize) -> Option<(String, bool)> {
    let first = t.get(*i)?;
    if first.kind == TokenKind::SysVar {
        *i += 1;
        let text = String::from_utf8_lossy(first.bytes(sql)).to_ascii_lowercase();
        let rest = text.strip_prefix("@@")?;
        return match rest.split_once('.') {
            Some(("session" | "local", n)) => Some((n.to_owned(), false)),
            Some(_) => None,
            None => Some((rest.to_owned(), true)),
        };
    }
    let w = word(first, sql)?;
    *i += 1;
    match w.as_str() {
        "session" | "local" => {
            let n = word(t.get(*i)?, sql)?;
            *i += 1;
            Some((n, false))
        }
        _ => Some((w, false)),
    }
}

fn assignment(name: &str, value: &Token, sql: &[u8], opts: LexOptions) -> Option<Assign> {
    if let Some(n) = SWITCHES.iter().find(|s| **s == name) {
        return Some(Assign::Var {
            names: vec![*n],
            value: switch(value, sql)?.to_owned(),
        });
    }
    if ISOLATION.contains(&name) {
        return Some(Assign::Var {
            names: if name == "tx_isolation" {
                vec!["tx_isolation", "transaction_isolation"]
            } else {
                ISOLATION.to_vec()
            },
            value: isolation_value(value, sql, opts)?,
        });
    }
    if READ_ONLY.contains(&name) {
        return Some(Assign::Var {
            names: if name == "tx_read_only" {
                vec!["tx_read_only", "transaction_read_only"]
            } else {
                READ_ONLY.to_vec()
            },
            value: switch(value, sql)?.to_owned(),
        });
    }
    if name == "innodb_lock_wait_timeout" {
        if word(value, sql).as_deref() == Some("default") {
            return Some(Assign::LockWait(None));
        }
        if value.kind != TokenKind::Number {
            return None;
        }
        let n: u64 = std::str::from_utf8(value.bytes(sql)).ok()?.parse().ok()?;
        return (1..=LOCK_WAIT_MAX)
            .contains(&n)
            .then_some(Assign::LockWait(Some(n)));
    }
    None
}

/// `SET NAMES` (the tokens after `NAMES`): a character set the engine
/// knows, written as a word or a string, and perhaps `COLLATE` with one of
/// its collations. `DEFAULT`, anything else and a collation of another
/// character set stay MariaDB's (its errors).
fn names(t: &[Token], sql: &[u8], opts: LexOptions) -> Option<Assign> {
    let text = |x: &Token| -> Option<String> {
        match x.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                Some(String::from_utf8_lossy(&lex::ident_value(x.bytes(sql))).into_owned())
            }
            TokenKind::String => {
                String::from_utf8(lex::string_value(x.bytes(sql), opts.no_backslash_escapes)?).ok()
            }
            _ => None,
        }
    };
    let cs = text(t.first()?)?;
    if cs.eq_ignore_ascii_case("default") {
        return None;
    }
    let charset = crate::engine::define::charset_name(&cs)?;
    let collation = match t.get(1..) {
        Some([]) | None => None,
        Some([k, c]) if word(k, sql).as_deref() == Some("collate") => {
            let (name, of) = crate::engine::define::collation(&text(c)?)?;
            if of != charset {
                return None;
            }
            Some(name)
        }
        _ => return None,
    };
    Some(Assign::Names { charset, collation })
}

/// The collation `SET NAMES cs` gives without `COLLATE`: the session's
/// `character_set_collations` entry for `cs` (MariaDB 11.8 reads it),
/// else the character set's default.
fn names_collation(charset: &str, mapping: Option<&str>) -> String {
    mapping
        .into_iter()
        .flat_map(|m| m.split(','))
        .filter_map(|p| p.split_once('='))
        .find(|(c, _)| c.trim().eq_ignore_ascii_case(charset))
        .map_or_else(
            || crate::engine::define::default_collation(charset).to_owned(),
            |(_, coll)| coll.trim().to_ascii_lowercase(),
        )
}

/// `SET SESSION TRANSACTION` characteristics (the tokens after
/// `TRANSACTION`).
fn transaction(t: &[Token], sql: &[u8]) -> Option<Vec<Assign>> {
    let words: Vec<String> = t
        .iter()
        .map(|x| {
            if x.kind == TokenKind::Punct && x.bytes(sql) == b"," {
                Some(",".to_owned())
            } else {
                word(x, sql)
            }
        })
        .collect::<Option<_>>()?;
    let mut out = Vec::new();
    for part in words.split(|w| w == ",") {
        let p: Vec<&str> = part.iter().map(String::as_str).collect();
        let a = match p.as_slice() {
            ["isolation", "level", rest @ ..] => {
                let v = match rest {
                    ["read", "uncommitted"] => "READ-UNCOMMITTED",
                    ["read", "committed"] => "READ-COMMITTED",
                    ["repeatable", "read"] => "REPEATABLE-READ",
                    ["serializable"] => "SERIALIZABLE",
                    _ => return None,
                };
                Assign::Var {
                    names: ISOLATION.to_vec(),
                    value: v.to_owned(),
                }
            }
            ["read", "only"] => Assign::Var {
                names: READ_ONLY.to_vec(),
                value: "ON".to_owned(),
            },
            ["read", "write"] => Assign::Var {
                names: READ_ONLY.to_vec(),
                value: "OFF".to_owned(),
            },
            _ => return None,
        };
        out.push(a);
    }
    (!out.is_empty()).then_some(out)
}

/// Every database of the tenant is on the engine.
fn all_engine(cat: &TenantCatalog) -> bool {
    catalog::of(cat).is_some_and(|e| cat.databases.iter().all(|d| e.dbs.contains(&d.db_id)))
}

impl PassSession {
    /// A `SET` the gateway keeps (see the module docs): `Some` once
    /// answered, `None` when MariaDB still answers it.
    pub(super) async fn engine_set(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &TenantCatalog,
        sql: &[u8],
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if self.backend_in_txn() || !all_engine(cat) {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        let Some(assigns) = parse(sql, lex) else {
            return Ok(None);
        };
        let open = self.eng.txn.is_open();
        if open && assigns.iter().any(Assign::transactional) {
            return Ok(None);
        }
        if let Some(on) = assigns.iter().filter_map(Assign::autocommit).next_back() {
            // Inside an XA branch turning autocommit on is refused by the
            // transaction-control path.
            if self.eng.txn.xa_phase().is_some() {
                return Ok(None);
            }
            // Turning autocommit on (from off) commits.
            if on
                && open
                && self.engine_vars(cx).autocommit_off
                && let Err(e) = self.engine_implicit_commit().await
            {
                return self.fail(out, &e).await.map(Some);
            }
        }
        let mut changes = Vec::new();
        {
            let bs = self.bs(cx);
            for a in &assigns {
                match a {
                    Assign::Var { names, value } => {
                        for n in names {
                            bs.vars_mut().vars.insert((*n).to_owned(), value.clone());
                        }
                        changes.push(SessionChange::SystemVariable {
                            name: Bytes::from_static(names[0].as_bytes()),
                            value: Bytes::from(value.clone()),
                        });
                    }
                    Assign::Names { charset, collation } => {
                        let coll = collation.clone().unwrap_or_else(|| {
                            names_collation(
                                charset,
                                bs.vars()
                                    .vars
                                    .get("character_set_collations")
                                    .map(String::as_str),
                            )
                        });
                        for (n, v) in [
                            ("character_set_client", (*charset).to_owned()),
                            ("character_set_connection", (*charset).to_owned()),
                            ("character_set_results", (*charset).to_owned()),
                            ("collation_connection", coll),
                        ] {
                            bs.vars_mut().vars.insert(n.to_owned(), v.clone());
                            changes.push(SessionChange::SystemVariable {
                                name: Bytes::from_static(n.as_bytes()),
                                value: Bytes::from(v),
                            });
                        }
                    }
                    Assign::LockWait(_) => {}
                }
            }
        }
        // From here on MySQL reports the results character set's
        // collation, not the handshake's.
        if assigns.iter().any(|a| matches!(a, Assign::Names { .. })) {
            self.eng.results_set = true;
        }
        for a in &assigns {
            if let Assign::LockWait(n) = a {
                self.eng.lock_wait = *n;
                changes.push(SessionChange::SystemVariable {
                    name: Bytes::from_static(b"innodb_lock_wait_timeout"),
                    value: Bytes::from(n.map_or_else(|| "50".to_owned(), |n| n.to_string())),
                });
            }
        }
        // The engine path reads the variables again.
        self.eng.stale();
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let mut ok = OkPacket::new(status);
        ok.set_session_changes(&changes);
        out.ok(ok).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(sql: &str) -> Option<Vec<Assign>> {
        parse(sql.as_bytes(), LexOptions::mariadb())
    }

    fn var(names: &[&'static str], value: &str) -> Assign {
        Assign::Var {
            names: names.to_vec(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn switches_and_scopes() {
        assert_eq!(
            p("SET autocommit=0"),
            Some(vec![var(&["autocommit"], "OFF")])
        );
        assert_eq!(
            p("set session foreign_key_checks = ON, @@session.unique_checks := false;"),
            Some(vec![
                var(&["foreign_key_checks"], "ON"),
                var(&["unique_checks"], "OFF")
            ])
        );
        assert_eq!(
            p("SET @@autocommit = 1"),
            Some(vec![var(&["autocommit"], "ON")])
        );
        assert_eq!(
            p("SET LOCAL sql_notes = TRUE"),
            Some(vec![var(&["sql_notes"], "ON")])
        );
    }

    #[test]
    fn isolation_and_read_only() {
        assert_eq!(
            p("SET SESSION transaction_isolation = 'read-committed'"),
            Some(vec![var(ISOLATION, "READ-COMMITTED")])
        );
        assert_eq!(
            p("SET tx_isolation='SERIALIZABLE'"),
            Some(vec![var(
                &["tx_isolation", "transaction_isolation"],
                "SERIALIZABLE"
            )])
        );
        assert_eq!(
            p("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"),
            Some(vec![
                var(ISOLATION, "REPEATABLE-READ"),
                var(READ_ONLY, "ON")
            ])
        );
        assert_eq!(
            p("SET transaction_read_only = OFF"),
            Some(vec![var(READ_ONLY, "OFF")])
        );
    }

    #[test]
    fn lock_wait() {
        assert_eq!(
            p("SET innodb_lock_wait_timeout=3"),
            Some(vec![Assign::LockWait(Some(3))])
        );
        assert_eq!(
            p("SET SESSION innodb_lock_wait_timeout = DEFAULT"),
            Some(vec![Assign::LockWait(None)])
        );
        assert_eq!(p("SET innodb_lock_wait_timeout=0"), None);
        assert_eq!(p("SET innodb_lock_wait_timeout=1073741825"), None);
    }

    #[test]
    fn set_names() {
        let n = |cs: &'static str, c: Option<&str>| {
            Some(vec![Assign::Names {
                charset: cs,
                collation: c.map(str::to_owned),
            }])
        };
        assert_eq!(p("SET NAMES utf8mb4"), n("utf8mb4", None));
        assert_eq!(p("SET NAMES 'utf8'"), n("utf8mb3", None));
        assert_eq!(
            p("SET NAMES latin1 COLLATE 'latin1_bin'"),
            n("latin1", Some("latin1_bin"))
        );
        assert_eq!(p("SET NAMES DEFAULT"), None);
        assert_eq!(p("SET NAMES latin1 COLLATE utf8mb4_bin"), None);
        assert_eq!(p("SET NAMES nope"), None);
        assert_eq!(
            names_collation(
                "utf8mb4",
                Some("utf8mb3=utf8mb3_general_ci,utf8mb4=utf8mb4_bin")
            ),
            "utf8mb4_bin"
        );
        assert_eq!(names_collation("latin1", None), "latin1_swedish_ci");
    }

    #[test]
    fn left_to_mariadb() {
        for s in [
            "SET GLOBAL autocommit = 0",
            "SET @@global.autocommit = 0",
            "SET @x = 1",
            "SET NAMES utf8mb4, autocommit = 0",
            "SET autocommit = 'ON'",
            "SET autocommit = 2",
            "SET autocommit = 1 + 0",
            "SET sql_mode = 'ANSI'",
            "SET transaction_isolation = 'READ COMMITTED'",
            "SET @@transaction_isolation = 'READ-COMMITTED'",
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET GLOBAL TRANSACTION READ ONLY",
            "SET autocommit = 0, sql_mode = ''",
            "SET autocommit",
            "SELECT 1",
        ] {
            assert_eq!(p(s), None, "{s}");
        }
    }
}
