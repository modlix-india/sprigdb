//! Named locks (`GET_LOCK`, `RELEASE_LOCK`, `IS_FREE_LOCK`,
//! `IS_USED_LOCK`, `RELEASE_ALL_LOCKS`) kept by the gateway when every
//! database of the tenant is on the engine: migration tools (Flyway,
//! Rails, Laravel) take one before they change the schema.
//!
//! The statement must be exactly `SELECT f(args) [[AS] alias]` or
//! `DO f(args)` with literal arguments; anything else still goes to
//! MariaDB. Locks are per tenant and per gateway process (one node), held
//! by the session until released or until it ends; a session may take the
//! same lock more than once (both servers count). The answer is the
//! function's value as a one-row result, named as the client wrote it.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use sprig_sql::lex::{self, LexOptions, TokenKind};
use tokio::sync::Notify;

/// One held lock.
#[derive(Debug, Clone, Copy)]
struct Held {
    session: u32,
    count: u64,
}

static LOCKS: LazyLock<Mutex<HashMap<(u64, String), Held>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static RELEASED: LazyLock<Notify> = LazyLock::new(Notify::new);

fn table() -> std::sync::MutexGuard<'static, HashMap<(u64, String), Held>> {
    LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The function a statement calls.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Call {
    /// `GET_LOCK(name, seconds)`; a negative wait is forever.
    Get(String, f64),
    /// `RELEASE_LOCK(name)`.
    Release(String),
    /// `IS_FREE_LOCK(name)`.
    IsFree(String),
    /// `IS_USED_LOCK(name)`.
    IsUsed(String),
    /// `RELEASE_ALL_LOCKS()`.
    ReleaseAll,
}

/// A parsed statement: the call, the column name, and whether it was `DO`
/// (no result set).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct LockStmt {
    pub(super) call: Call,
    pub(super) column: String,
    pub(super) is_do: bool,
}

/// The lock statement `sql` is, if it is one (see the module docs).
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one pass over the tokens
pub(super) fn parse(sql: &[u8], opts: LexOptions) -> Option<LockStmt> {
    let toks = lex::lex(sql, &opts).ok()?;
    let sig: Vec<_> = toks.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let mut sig = &sig[..];
    if let Some(l) = sig.last()
        && l.kind == TokenKind::Punct
        && sql[l.start as usize] == b';'
    {
        sig = &sig[..sig.len() - 1];
    }
    let word = |i: usize, w: &[u8]| {
        sig.get(i)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |i: usize, c: u8| {
        sig.get(i)
            .is_some_and(|t| t.kind == TokenKind::Punct && sql[t.start as usize] == c)
    };
    let is_do = word(0, b"DO");
    if !is_do && !word(0, b"SELECT") {
        return None;
    }
    let f = sig.get(1).filter(|t| t.kind == TokenKind::Word)?;
    let fname = String::from_utf8_lossy(f.bytes(sql)).to_ascii_uppercase();
    if !punct(2, b'(') {
        return None;
    }
    // The arguments: literals separated by commas.
    let mut args: Vec<lex::Token> = Vec::new();
    let mut i = 3;
    if !punct(i, b')') {
        loop {
            let t = *sig.get(i)?;
            // A signed number.
            let neg = t.kind == TokenKind::Op && t.bytes(sql) == b"-";
            let t = if neg { *sig.get(i + 1)? } else { t };
            if neg {
                if t.kind != TokenKind::Number {
                    return None;
                }
                i += 1;
            }
            if !matches!(t.kind, TokenKind::String | TokenKind::Number) {
                return None;
            }
            args.push(if neg {
                lex::Token {
                    kind: TokenKind::Number,
                    start: t.start - 1,
                    end: t.end,
                }
            } else {
                t
            });
            i += 1;
            if punct(i, b',') {
                i += 1;
                continue;
            }
            break;
        }
    }
    if !punct(i, b')') {
        return None;
    }
    let close = sig[i];
    let mut column =
        String::from_utf8_lossy(&sql[f.start as usize..close.end as usize]).into_owned();
    i += 1;
    // `[AS] alias` (SELECT only).
    if !is_do && i < sig.len() {
        if word(i, b"AS") {
            i += 1;
        }
        let a = sig.get(i)?;
        column = match a.kind {
            TokenKind::Word => String::from_utf8_lossy(a.bytes(sql)).into_owned(),
            TokenKind::QuotedIdent => {
                let b = a.bytes(sql);
                let q = b[0];
                String::from_utf8_lossy(&b[1..b.len() - 1]).replace(
                    &format!("{}{}", q as char, q as char),
                    &(q as char).to_string(),
                )
            }
            TokenKind::String => {
                String::from_utf8(lex::string_value(a.bytes(sql), opts.no_backslash_escapes)?)
                    .ok()?
            }
            _ => return None,
        };
        i += 1;
    }
    if i != sig.len() {
        return None;
    }
    let text = |t: &lex::Token| -> Option<String> {
        match t.kind {
            TokenKind::String => {
                String::from_utf8(lex::string_value(t.bytes(sql), opts.no_backslash_escapes)?).ok()
            }
            _ => None,
        }
    };
    let name = |t: &lex::Token| -> Option<String> {
        let n = text(t)?;
        // Both servers refuse an empty name and one over 64 characters
        // with their own errors: theirs to give.
        (!n.is_empty() && n.chars().count() <= 64).then_some(n)
    };
    let call = match (fname.as_str(), args.as_slice()) {
        ("GET_LOCK", [n, w]) if w.kind == TokenKind::Number => Call::Get(
            name(n)?,
            std::str::from_utf8(w.bytes(sql))
                .ok()?
                .parse::<f64>()
                .ok()?,
        ),
        ("RELEASE_LOCK", [n]) => Call::Release(name(n)?),
        ("IS_FREE_LOCK", [n]) => Call::IsFree(name(n)?),
        ("IS_USED_LOCK", [n]) => Call::IsUsed(name(n)?),
        ("RELEASE_ALL_LOCKS", []) => Call::ReleaseAll,
        _ => return None,
    };
    Some(LockStmt {
        call,
        column,
        is_do,
    })
}

/// The session's hold on its locks: dropping it (the session ends)
/// releases every lock it holds.
#[derive(Debug)]
pub(super) struct Owner {
    pub(super) tenant: u64,
    pub(super) session: u32,
}

impl Drop for Owner {
    fn drop(&mut self) {
        release_all(self.tenant, self.session);
    }
}

fn key(tenant: u64, name: &str) -> (u64, String) {
    // Both servers compare names without case.
    (tenant, name.to_lowercase())
}

fn try_take(tenant: u64, session: u32, name: &str) -> bool {
    let mut t = table();
    let e = t
        .entry(key(tenant, name))
        .or_insert(Held { session, count: 0 });
    if e.session != session {
        return false;
    }
    e.count += 1;
    true
}

fn release_all(tenant: u64, session: u32) -> u64 {
    let mut n = 0;
    table().retain(|(t, _), h| {
        if *t == tenant && h.session == session {
            n += h.count;
            false
        } else {
            true
        }
    });
    if n > 0 {
        RELEASED.notify_waiters();
    }
    n
}

/// Runs `call` for `session` of `tenant`: the function's value (`None`
/// is `NULL`).
pub(super) async fn run(tenant: u64, session: u32, call: &Call) -> Option<i64> {
    match call {
        Call::Get(name, wait) => {
            let deadline = (*wait >= 0.0).then(|| {
                tokio::time::Instant::now() + Duration::from_secs_f64(wait.min(31_536_000.0))
            });
            loop {
                let released = RELEASED.notified();
                if try_take(tenant, session, name) {
                    return Some(1);
                }
                match deadline {
                    Some(d) => {
                        if tokio::time::timeout_at(d, released).await.is_err() {
                            return Some(0);
                        }
                    }
                    None => released.await,
                }
            }
        }
        Call::Release(name) => {
            let mut t = table();
            let k = key(tenant, name);
            let h = t.get_mut(&k)?;
            if h.session != session {
                return Some(0);
            }
            h.count -= 1;
            if h.count == 0 {
                t.remove(&k);
                drop(t);
                RELEASED.notify_waiters();
            }
            Some(1)
        }
        Call::IsFree(name) => Some(i64::from(!table().contains_key(&key(tenant, name)))),
        Call::IsUsed(name) => table()
            .get(&key(tenant, name))
            .map(|h| i64::from(h.session)),
        Call::ReleaseAll => Some(i64::try_from(release_all(tenant, session)).unwrap_or(i64::MAX)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Option<LockStmt> {
        parse(s.as_bytes(), LexOptions::mariadb())
    }

    #[test]
    fn forms() {
        assert_eq!(
            p("SELECT GET_LOCK('flyway', 10)"),
            Some(LockStmt {
                call: Call::Get("flyway".into(), 10.0),
                column: "GET_LOCK('flyway', 10)".into(),
                is_do: false
            })
        );
        assert_eq!(
            p("select get_lock('a', -1) as `got`;").map(|l| (l.call, l.column)),
            Some((Call::Get("a".into(), -1.0), "got".into()))
        );
        assert_eq!(p("DO RELEASE_LOCK('a')").map(|l| l.is_do), Some(true));
        assert_eq!(
            p("SELECT RELEASE_ALL_LOCKS()").map(|l| l.call),
            Some(Call::ReleaseAll)
        );
        assert_eq!(
            p("SELECT IS_USED_LOCK(\"x\") u").map(|l| (l.call, l.column)),
            Some((Call::IsUsed("x".into()), "u".into()))
        );
        for s in [
            "SELECT GET_LOCK(@n, 10)",
            "SELECT GET_LOCK('a', 1), 2",
            "SELECT GET_LOCK('', 1)",
            "SELECT GET_LOCK('a')",
            "SELECT NOW()",
            "SELECT GET_LOCK('a', 1) FROM t",
        ] {
            assert_eq!(p(s), None, "{s}");
        }
    }

    #[tokio::test]
    async fn take_wait_release() {
        let t = 990_001;
        assert_eq!(run(t, 1, &Call::Get("L".into(), 0.0)).await, Some(1));
        assert_eq!(run(t, 1, &Call::Get("l".into(), 0.0)).await, Some(1));
        assert_eq!(run(t, 2, &Call::Get("l".into(), 0.05)).await, Some(0));
        assert_eq!(run(t, 2, &Call::IsUsed("l".into())).await, Some(1));
        assert_eq!(run(t, 2, &Call::Release("l".into())).await, Some(0));
        assert_eq!(run(t, 1, &Call::Release("l".into())).await, Some(1));
        let waiter = tokio::spawn(async move { run(t, 2, &Call::Get("l".into(), 5.0)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        {
            let _o = Owner {
                tenant: t,
                session: 1,
            };
        }
        assert_eq!(waiter.await.unwrap(), Some(1));
        assert_eq!(run(t, 1, &Call::IsFree("l".into())).await, Some(0));
        assert_eq!(run(t, 2, &Call::ReleaseAll).await, Some(1));
        assert_eq!(run(t, 1, &Call::Release("l".into())).await, None);
    }
}
