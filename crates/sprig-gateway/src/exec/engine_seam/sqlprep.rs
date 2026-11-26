//! `PREPARE`, `EXECUTE` and `DEALLOCATE PREPARE` as SQL statements, on a
//! tenant whose databases are all the engine's (the session keeps its user
//! variables in the same case).
//!
//! The session keeps each prepared statement's text, by name (names are
//! case-insensitive). `PREPARE` checks the text as the engine would bind it
//! (an unknown table fails there, as on the servers) and counts its
//! placeholders. `EXECUTE ... USING` writes each argument in for its
//! placeholder, in parentheses (a placeholder that is a whole select item
//! keeps `?` as its column name), and runs the text as the client's
//! statement, through every layer a statement of the client's goes
//! through. MariaDB takes any expression as an argument and also
//! `EXECUTE IMMEDIATE`; MySQL takes user variables only.

use std::collections::HashMap;

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{Persona, StatusFlags};
use sprig_sql::lex::{self, LexOptions, Token, TokenKind};

use super::super::PassSession;
use super::super::tenant::Step;
use super::sessvars::all_engine;
use crate::exec::QueryAttrs;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// The session's prepared statements made by `PREPARE`, by lower-case
/// name.
#[derive(Debug, Default)]
pub(in crate::exec) struct SqlPrepared {
    texts: HashMap<String, Bytes>,
}

/// A statement of this family, as read from its text.
enum Form {
    /// `PREPARE name FROM source`.
    Prepare { name: String, source: Source },
    /// `EXECUTE name [USING a, ...]`.
    Execute { name: String, args: Vec<String> },
    /// `EXECUTE IMMEDIATE source [USING a, ...]` (MariaDB).
    Immediate { source: Source, args: Vec<String> },
    /// `{DEALLOCATE | DROP} PREPARE name`.
    Deallocate { name: String },
}

/// Where a statement's text comes from.
enum Source {
    /// A string literal's value.
    Text(String),
    /// A user variable (lower-case name).
    Var(String),
}

fn unknown(name: &str, verb: &str) -> SprigError {
    SprigError::standard(
        1243,
        *b"HY000",
        format!("Unknown prepared statement handler ({name}) given to {verb}"),
    )
}

fn bad_args() -> SprigError {
    SprigError::standard(1210, *b"HY000", "Incorrect arguments to EXECUTE")
}

pub(super) fn near(persona: Persona, sql: &[u8], at: usize) -> SprigError {
    let rest = String::from_utf8_lossy(sql.get(at..).unwrap_or_default());
    let rest = rest.trim_end().trim_end_matches(';').trim_end();
    let shown: String = rest.chars().take(80).collect();
    let server = if persona == Persona::MySql84 {
        "MySQL"
    } else {
        "MariaDB"
    };
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             {server} server version for the right syntax to use near '{shown}' at line 1"
        ),
    )
}

/// An identifier token's name (backticks removed).
pub(super) fn ident(t: &Token, sql: &[u8]) -> Option<String> {
    let b = t.bytes(sql);
    match t.kind {
        TokenKind::Word => Some(String::from_utf8_lossy(b).into_owned()),
        TokenKind::QuotedIdent if b.len() >= 2 => {
            Some(String::from_utf8_lossy(&b[1..b.len() - 1]).replace("``", "`"))
        }
        _ => None,
    }
}

/// A string literal's value (quotes doubled or backslash-escaped).
pub(super) fn string_value(tok: &Token, sql: &[u8], nbe: bool) -> Option<String> {
    if tok.kind != TokenKind::String {
        return None;
    }
    let quoted = tok.bytes(sql);
    let quote = *quoted.first()?;
    let inner = quoted.get(1..quoted.len().checked_sub(1)?)?;
    let mut out = Vec::with_capacity(inner.len());
    let mut pos = 0;
    while pos < inner.len() {
        let ch = inner[pos];
        if ch == quote && inner.get(pos + 1) == Some(&quote) {
            out.push(quote);
            pos += 2;
        } else if ch == b'\\' && !nbe && pos + 1 < inner.len() {
            out.push(match inner[pos + 1] {
                b'n' => b'\n',
                b't' => b'\t',
                b'r' => b'\r',
                b'0' => 0,
                b'Z' => 0x1a,
                b'b' => 0x08,
                other => other,
            });
            pos += 2;
        } else {
            out.push(ch);
            pos += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The user variable token's name, as the session keeps it.
fn var_name(t: &Token, sql: &[u8]) -> Option<String> {
    if t.kind != TokenKind::UserVar {
        return None;
    }
    let b = t.bytes(sql).strip_prefix(b"@")?;
    let b = match b.first() {
        Some(q @ (b'`' | b'\'' | b'"')) if b.len() >= 2 && b.last() == Some(q) => {
            &b[1..b.len() - 1]
        }
        _ => b,
    };
    Some(String::from_utf8_lossy(b).to_ascii_lowercase())
}

/// Reads one of the family's statements; `Ok(None)` for any other text.
fn read(sql: &[u8], opts: LexOptions, persona: Persona) -> Result<Option<Form>> {
    let Ok(tokens) = lex::lex(sql, &opts) else {
        return Ok(None);
    };
    let sig: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |k: usize, w: &[u8]| {
        sig.get(k)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    // A trailing `;` is allowed.
    let end = if sig
        .last()
        .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == b";")
    {
        sig.len() - 1
    } else {
        sig.len()
    };
    let at = |k: usize| sig.get(k).map_or(sql.len(), |t| t.start as usize);
    let source = |k: usize| -> Result<Source> {
        let t = sig.get(k).ok_or_else(|| near(persona, sql, at(k)))?;
        if let Some(v) = var_name(t, sql) {
            return Ok(Source::Var(v));
        }
        string_value(t, sql, opts.no_backslash_escapes)
            .map(Source::Text)
            .ok_or_else(|| near(persona, sql, at(k)))
    };
    // `USING a, b, ...` from `k` to the end: each argument's text.
    let args = |k: usize| -> Result<Vec<String>> {
        if k >= end {
            return Ok(Vec::new());
        }
        if !word(k, b"USING") {
            return Err(near(persona, sql, at(k)));
        }
        let mut out = Vec::new();
        let mut depth = 0i32;
        let mut from = k + 1;
        for j in k + 1..=end {
            let t = sig.get(j);
            let comma = t.is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == b",");
            if let Some(t) = t.filter(|t| t.kind == TokenKind::Punct) {
                match t.bytes(sql) {
                    b"(" => depth += 1,
                    b")" => depth -= 1,
                    _ => {}
                }
            }
            if j == end || (comma && depth == 0) {
                if from >= j {
                    return Err(near(persona, sql, at(j)));
                }
                // MySQL: user variables only.
                if persona == Persona::MySql84
                    && !(j == from + 1 && sig[from].kind == TokenKind::UserVar)
                {
                    return Err(near(persona, sql, at(from)));
                }
                let a = sig[from].start as usize;
                let b = sig[j - 1].end as usize;
                out.push(String::from_utf8_lossy(&sql[a..b]).into_owned());
                from = j + 1;
            }
        }
        Ok(out)
    };
    if word(0, b"PREPARE") {
        let Some(name) = sig.get(1).and_then(|t| ident(t, sql)) else {
            return Err(near(persona, sql, at(1)));
        };
        if !word(2, b"FROM") {
            return Err(near(persona, sql, at(2)));
        }
        let source = source(3)?;
        if end > 4 {
            return Err(near(persona, sql, at(4)));
        }
        return Ok(Some(Form::Prepare { name, source }));
    }
    if word(0, b"EXECUTE") {
        if word(1, b"IMMEDIATE") && persona != Persona::MySql84 {
            let source = source(2)?;
            let args = args(3)?;
            return Ok(Some(Form::Immediate { source, args }));
        }
        let Some(name) = sig.get(1).and_then(|t| ident(t, sql)) else {
            return Err(near(persona, sql, at(1)));
        };
        let args = args(2)?;
        return Ok(Some(Form::Execute { name, args }));
    }
    if (word(0, b"DEALLOCATE") || word(0, b"DROP")) && word(1, b"PREPARE") {
        let Some(name) = sig.get(2).and_then(|t| ident(t, sql)) else {
            return Err(near(persona, sql, at(2)));
        };
        if end > 3 {
            return Err(near(persona, sql, at(3)));
        }
        return Ok(Some(Form::Deallocate { name }));
    }
    Ok(None)
}

/// Whether `text` starts with a word that begins a statement.
fn statement_word(text: &[u8], opts: LexOptions) -> bool {
    const WORDS: &[&[u8]] = &[
        b"SELECT",
        b"WITH",
        b"VALUES",
        b"TABLE",
        b"INSERT",
        b"REPLACE",
        b"UPDATE",
        b"DELETE",
        b"CALL",
        b"DO",
        b"SET",
        b"SHOW",
        b"CREATE",
        b"ALTER",
        b"DROP",
        b"TRUNCATE",
        b"RENAME",
        b"BEGIN",
        b"START",
        b"COMMIT",
        b"ROLLBACK",
        b"SAVEPOINT",
        b"RELEASE",
        b"LOCK",
        b"UNLOCK",
        b"GRANT",
        b"REVOKE",
        b"ANALYZE",
        b"OPTIMIZE",
        b"CHECK",
        b"CHECKSUM",
        b"REPAIR",
        b"EXPLAIN",
        b"DESCRIBE",
        b"DESC",
        b"USE",
        b"XA",
        b"KILL",
        b"FLUSH",
        b"LOAD",
        b"HANDLER",
        b"SIGNAL",
        b"RESIGNAL",
        b"INSTALL",
        b"UNINSTALL",
        b"RESET",
        b"PURGE",
    ];
    let Ok(tokens) = lex::lex(text, &opts) else {
        return true;
    };
    tokens.iter().find(|t| !t.kind.is_trivia()).is_none_or(|t| {
        t.kind == TokenKind::Punct && t.bytes(text) == b"("
            || t.kind == TokenKind::Word
                && WORDS.iter().any(|w| t.bytes(text).eq_ignore_ascii_case(w))
    })
}

/// A placeholder of a prepared text: its byte range, and where it lies in
/// a select item without an alias, that item's text (the result column's
/// name on the servers).
struct Mark {
    at: std::ops::Range<usize>,
    item: Option<std::ops::Range<usize>>,
    /// A `LIMIT` or `OFFSET` count: written in as a bare number.
    count: bool,
}

/// The outermost select list's items that have no alias, as byte ranges:
/// the result columns both servers name by their text.
pub(super) fn unaliased_items(text: &[u8], opts: LexOptions) -> Vec<std::ops::Range<usize>> {
    let Ok(tokens) = lex::lex(text, &opts) else {
        return Vec::new();
    };
    let sig: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let is_word = |t: &Token, w: &[&[u8]]| {
        t.kind == TokenKind::Word && w.iter().any(|x| t.bytes(text).eq_ignore_ascii_case(x))
    };
    let punct = |t: &Token, c: &[u8]| t.kind == TokenKind::Punct && t.bytes(text) == c;
    // The outermost select list's items, by token index (a `SELECT` first).
    let mut items: Vec<(usize, usize)> = Vec::new();
    if sig.first().is_some_and(|t| is_word(t, &[b"SELECT"])) {
        let mut k = 1;
        while sig
            .get(k)
            .is_some_and(|t| is_word(t, &[b"DISTINCT", b"ALL", b"DISTINCTROW"]))
        {
            k += 1;
        }
        let mut depth = 0i32;
        let mut from = k;
        while k <= sig.len() {
            let t = sig.get(k);
            let ends = t.is_none_or(|t| {
                depth == 0
                    && (punct(t, b",")
                        || punct(t, b";")
                        || is_word(
                            t,
                            &[
                                b"FROM",
                                b"WHERE",
                                b"GROUP",
                                b"HAVING",
                                b"ORDER",
                                b"LIMIT",
                                b"UNION",
                                b"WINDOW",
                                b"INTO",
                                b"FOR",
                                b"EXCEPT",
                                b"INTERSECT",
                            ],
                        ))
            });
            if ends {
                if k > from {
                    items.push((from, k));
                }
                if t.is_none_or(|t| !punct(t, b",")) {
                    break;
                }
                from = k + 1;
            } else if let Some(t) = t {
                if punct(t, b"(") {
                    depth += 1;
                } else if punct(t, b")") {
                    depth -= 1;
                }
            }
            k += 1;
        }
    }
    // An item ending in an alias (`AS name`, or a name after an
    // expression) keeps it.
    let unaliased = |(a, b): (usize, usize)| {
        let n = b - a;
        !(n >= 2 && is_word(sig[b - 2], &[b"AS"])
            || n >= 2
                && matches!(sig[b - 1].kind, TokenKind::Word | TokenKind::QuotedIdent)
                && !matches!(sig[b - 2].kind, TokenKind::Op)
                && !punct(sig[b - 2], b".")
                && !punct(sig[b - 2], b"("))
    };
    items
        .into_iter()
        .filter(|r| unaliased(*r))
        .map(|(a, b)| sig[a].start as usize..sig[b - 1].end as usize)
        .collect()
}

/// The placeholders of `text`, in order.
fn placeholders(text: &[u8], opts: LexOptions) -> Vec<Mark> {
    let Ok(tokens) = lex::lex(text, &opts) else {
        return Vec::new();
    };
    let sig: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let is_word = |t: &Token, w: &[&[u8]]| {
        t.kind == TokenKind::Word && w.iter().any(|x| t.bytes(text).eq_ignore_ascii_case(x))
    };
    let punct = |t: &Token, c: &[u8]| t.kind == TokenKind::Punct && t.bytes(text) == c;
    let items = unaliased_items(text, opts);
    sig.iter()
        .enumerate()
        .filter(|(_, t)| t.kind == TokenKind::Param)
        .map(|(k, t)| Mark {
            count: k > 0
                && (is_word(sig[k - 1], &[b"LIMIT", b"OFFSET"])
                    || k > 2 && punct(sig[k - 1], b",") && is_word(sig[k - 3], &[b"LIMIT"])),
            at: t.range(),
            item: items
                .iter()
                .find(|r| r.contains(&(t.start as usize)))
                .cloned(),
        })
        .collect()
}

/// `text` with each placeholder replaced by its argument (in parentheses);
/// a select item holding one is named by its text.
fn written(text: &[u8], marks: &[Mark], args: &[String]) -> Bytes {
    let mut out = Vec::with_capacity(text.len() + 16 * args.len());
    let mut at = 0;
    for (i, (m, a)) in marks.iter().zip(args).enumerate() {
        out.extend_from_slice(&text[at..m.at.start]);
        if m.count {
            out.extend_from_slice(a.as_bytes());
        } else {
            out.push(b'(');
            out.extend_from_slice(a.as_bytes());
            out.push(b')');
        }
        at = m.at.end;
        // After the item's last placeholder: its name.
        if let Some(item) = &m.item
            && marks
                .get(i + 1)
                .is_none_or(|n| n.item.as_ref() != Some(item))
        {
            out.extend_from_slice(&text[at..item.end]);
            at = item.end;
            let name = String::from_utf8_lossy(&text[item.clone()]).replace('`', "``");
            out.extend_from_slice(format!(" AS `{name}`").as_bytes());
        }
    }
    out.extend_from_slice(&text[at..]);
    Bytes::from(out)
}

impl PassSession {
    /// The statement's text from its source: a string, or a variable's
    /// value as text.
    fn sql_prep_source(&mut self, cx: &ExecCtx<'_>, s: Source) -> Bytes {
        match s {
            Source::Text(t) => Bytes::from(t),
            // `NULL` reads as its text, which then fails to parse, as on the
            // servers.
            Source::Var(v) => Bytes::from(
                self.user_var_text(cx, &v)
                    .unwrap_or_else(|| b"NULL".to_vec()),
            ),
        }
    }

    /// Whether `text` can be prepared: not empty, not one of this family
    /// (1295), and bound by the engine without an error.
    fn sql_prep_check(
        &self,
        cat: &TenantCatalog,
        current: Option<DbId>,
        text: &[u8],
        opts: LexOptions,
    ) -> Result<()> {
        if text.trim_ascii().is_empty() {
            return Err(near(self.persona, text, 0));
        }
        if !matches!(read(text, opts, self.persona), Ok(None)) {
            return Err(SprigError::standard(
                1295,
                *b"HY000",
                "This command is not supported in the prepared statement protocol yet",
            ));
        }
        // A text no layer reads: the servers' syntax error, near its start.
        if let super::Bound::Elsewhere { .. } = self.engine_bind(cat, current, text)?
            && !statement_word(text, opts)
        {
            let at = text.len() - text.trim_ascii_start().len();
            return Err(near(self.persona, text, at));
        }
        Ok(())
    }

    /// The OK that answers `PREPARE` (`info` "Statement prepared") or
    /// `DEALLOCATE PREPARE`.
    async fn sql_prep_ok(
        &mut self,
        cx: &ExecCtx<'_>,
        more: bool,
        info: &'static [u8],
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let mut ok = OkPacket::new(status);
        ok.info = Bytes::from_static(info);
        out.ok(ok).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    /// MySQL binds a parameter as a constant of the connection: a text
    /// variable held in a collation of its own is written in as its text,
    /// not with the variable's weight.
    fn sql_prep_params(&mut self, cx: &ExecCtx<'_>, args: &mut [String]) {
        if self.persona != sprig_proto_mysql::Persona::MySql84 {
            return;
        }
        let nbe = self.lex_opts().0.no_backslash_escapes;
        for a in args.iter_mut() {
            if !a.starts_with('@') || a.starts_with("@@") {
                continue;
            }
            let name = a[1..].trim_matches('`').to_ascii_lowercase();
            if let Some(crate::backend::state::VarValue::Collated(b, _)) =
                self.bs(cx).vars().user_vars.get(&name)
                && let Ok(t) = std::str::from_utf8(b)
            {
                *a = sprig_sql::names::rewrite::sql_string(t, nbe);
            }
        }
    }

    /// A `LIMIT` count takes a number: a variable's value is written in.
    /// `false` when it isn't a whole number.
    fn sql_prep_counts(&mut self, cx: &ExecCtx<'_>, marks: &[Mark], args: &mut [String]) -> bool {
        for (m, a) in marks.iter().zip(args.iter_mut()) {
            if m.count && a.starts_with('@') && !a.starts_with("@@") {
                let name = a[1..].trim_matches('`').to_ascii_lowercase();
                let v = self.user_var_text(cx, &name).unwrap_or_default();
                *a = String::from_utf8_lossy(&v).into_owned();
                if a.is_empty() || !a.bytes().all(|b| b.is_ascii_digit()) {
                    return false;
                }
            }
        }
        true
    }

    /// `PREPARE`, `EXECUTE` and `DEALLOCATE PREPARE`: `Some` once
    /// answered; `None` when the statement isn't one of them, or the
    /// tenant has databases outside the engine (MariaDB answers them then).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn engine_sql_prepared(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &std::sync::Arc<TenantCatalog>,
        current: Option<DbId>,
        sql: &[u8],
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let first = sql.trim_ascii_start();
        let starts = |w: &[u8]| first.len() >= w.len() && first[..w.len()].eq_ignore_ascii_case(w);
        if !(starts(b"PREPARE") || starts(b"EXECUTE") || starts(b"DEALLOCATE") || starts(b"DROP")) {
            return Ok(None);
        }
        if self.backend_in_txn() || !all_engine(cat) {
            return Ok(None);
        }
        let opts = self.lex_opts().0;
        let form = match read(sql, opts, self.persona) {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(None),
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        let (text, args) = match form {
            Form::Prepare { name, source } => {
                let text = self.sql_prep_source(cx, source);
                // Checked as the engine binds it: an error is the
                // statement's, and the name is then forgotten.
                self.eng.sql_prep.texts.remove(&name.to_ascii_lowercase());
                if let Err(e) = self.sql_prep_check(cat, current, &text, opts) {
                    return self.fail(out, &e).await.map(Some);
                }
                self.eng
                    .sql_prep
                    .texts
                    .insert(name.to_ascii_lowercase(), text);
                return self
                    .sql_prep_ok(cx, more, b"Statement prepared", out)
                    .await
                    .map(Some);
            }
            Form::Deallocate { name } => {
                if self
                    .eng
                    .sql_prep
                    .texts
                    .remove(&name.to_ascii_lowercase())
                    .is_none()
                {
                    return self
                        .fail(out, &unknown(&name, "DEALLOCATE PREPARE"))
                        .await
                        .map(Some);
                }
                return self.sql_prep_ok(cx, more, b"", out).await.map(Some);
            }
            Form::Execute { name, args } => {
                let Some(text) = self.eng.sql_prep.texts.get(&name.to_ascii_lowercase()) else {
                    return self.fail(out, &unknown(&name, "EXECUTE")).await.map(Some);
                };
                (text.clone(), args)
            }
            Form::Immediate { source, args } => {
                let text = self.sql_prep_source(cx, source);
                if let Err(e) = self.sql_prep_check(cat, current, &text, opts) {
                    return self.fail(out, &e).await.map(Some);
                }
                (text, args)
            }
        };
        let marks = placeholders(&text, opts);
        if marks.len() != args.len() {
            return self.fail(out, &bad_args()).await.map(Some);
        }
        let mut args = args;
        if !self.sql_prep_counts(cx, &marks, &mut args) {
            return self.fail(out, &bad_args()).await.map(Some);
        }
        self.sql_prep_params(cx, &mut args);
        let run = written(&text, &marks, &args);
        let attrs = QueryAttrs::default();
        Box::pin(self.tenant_text(cx, &run, &attrs, more, format, out))
            .await
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> LexOptions {
        LexOptions::mysql84()
    }

    #[test]
    fn statements_of_the_family_are_read() {
        let p = Persona::MariaDb;
        let f = read(b"PREPARE s FROM 'SELECT ?'", opts(), p).unwrap();
        assert!(
            matches!(f, Some(Form::Prepare { ref name, source: Source::Text(ref t) })
            if name == "s" && t == "SELECT ?")
        );
        let f = read(b"PREPARE `S` FROM @q", opts(), p).unwrap();
        assert!(
            matches!(f, Some(Form::Prepare { ref name, source: Source::Var(ref v) })
            if name == "S" && v == "q")
        );
        let f = read(b"EXECUTE s USING @a, 1 + 2, f(3, 4)", opts(), p).unwrap();
        assert!(matches!(f, Some(Form::Execute { ref args, .. })
            if args == &["@a", "1 + 2", "f(3, 4)"]));
        assert!(matches!(
            read(b"DEALLOCATE PREPARE s;", opts(), p).unwrap(),
            Some(Form::Deallocate { .. })
        ));
        assert!(matches!(
            read(b"DROP PREPARE s", opts(), p).unwrap(),
            Some(Form::Deallocate { .. })
        ));
        assert!(read(b"SELECT 1", opts(), p).unwrap().is_none());
        // MySQL takes user variables only, and no EXECUTE IMMEDIATE.
        let m = Persona::MySql84;
        assert!(read(b"EXECUTE s USING 3", opts(), m).is_err());
        assert!(read(b"EXECUTE IMMEDIATE 'SELECT 1'", opts(), m).is_err());
        assert!(matches!(
            read(b"EXECUTE IMMEDIATE 'SELECT 1'", opts(), p).unwrap(),
            Some(Form::Immediate { .. })
        ));
    }

    #[test]
    fn arguments_are_written_in_and_items_keep_their_names() {
        let text = b"SELECT ? + 1, ? AS p, v FROM t WHERE id = ? LIMIT ?";
        let marks = placeholders(text, opts());
        assert_eq!(marks.len(), 4);
        assert!(marks[0].item.is_some() && marks[1].item.is_none() && marks[2].item.is_none());
        assert!(marks[3].count && !marks[2].count);
        let args: Vec<String> = ["@a", "'x'", "@b", "2"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(
            &written(text, &marks, &args)[..],
            &b"SELECT (@a) + 1 AS `? + 1`, ('x') AS p, v FROM t WHERE id = (@b) LIMIT 2"[..]
        );
        // A `?` inside a string isn't a placeholder.
        assert!(placeholders(b"SELECT '?' FROM t", opts()).is_empty());
    }
}
