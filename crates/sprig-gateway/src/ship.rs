//! Reads a session on its tenant's leader sends to a copy.
//!
//! A session on the leader that asks for a copy for one read
//! (`/*+ READ_FROM(follower) */`, `SET sprig.read_from = follower`, the
//! tenant's `reads.read_from`, MongoDB's `secondary` read preferences) has
//! that read run on a node holding another replica of the tenant's unit, so
//! the leader does none of the read's engine work. The session keeps one
//! **shadow connection** to that copy: a connection of the same protocol,
//! opened over the peer listener as a hand-off ([`crate::forward`]) marked
//! as a reader session, which the copy serves like any reader session
//! ([`crate::reader`]). Each shipped read carries the session's token (its
//! own last commit), so it never reads older than the session's writes.
//!
//! - MySQL and MariaDB: the copy reads the client's own login packet and
//!   greeting (its capabilities, character set and database), and starts
//!   logged in as the session's principal (taken from the mutually
//!   authenticated peer, as a login hand-off's principal is); the session's
//!   database and changed variables follow it before a statement; the
//!   result's packets come back as the copy wrote them for the client's
//!   capabilities.
//! - MongoDB: the copy's connection starts logged in as the session's
//!   principal (taken from the mutually authenticated peer, as a login
//!   hand-off's principal is); a shipped command's reply comes back as is,
//!   and a cursor it opened stays there for `getMore`.
//!
//! Anything that fails before the read reaches the copy (no copy known,
//! the copy unreachable, a login the copy wants more for) runs the read on
//! the leader instead, which is never older than a copy; a session then
//! tries again only after [`RETRY_AFTER`].

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::handshake::{read_packet, write_packet};
use sprig_proto_mysql::packets::{EofPacket, OkPacket};
use sprig_proto_mysql::{Caps, ErrPacket};
use tokio::io::DuplexStream;

use crate::frontend::{Protocol, TlsInfo};
use crate::pipeline::{EndOfRows, ResultSink, RowFormat};
use crate::reader::ReadFrom;

/// How long a session waits before it tries a copy again after a failure.
pub const RETRY_AFTER: Duration = Duration::from_secs(5);

/// The in-process pipe's buffer.
const PIPE: usize = 256 * 1024;

/// The largest packet a shadow connection reads.
const MAX_PACKET: usize = 1 << 30;

/// The leading comment a shipped statement carries its token in: the copy
/// raises its reader session's token to the number and strips the comment.
pub const TOKEN_PREFIX: &[u8] = b"/*sprig_read_token=";

/// A shadow connection's ends: the pipe and the copy's Raft id.
pub(crate) struct Shadow {
    io: DuplexStream,
    pub(crate) node: u64,
}

impl std::fmt::Debug for Shadow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shadow")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// What opening a shadow connection needs.
pub(crate) struct Open {
    /// The listener the client connected to (the copy serves the shadow on
    /// its listener of the same name).
    pub(crate) listener: &'static str,
    /// The protocol.
    pub(crate) protocol: Protocol,
    /// The client's address.
    pub(crate) peer: SocketAddr,
    /// The client's TLS.
    pub(crate) tls: Option<TlsInfo>,
    /// The tenant.
    pub(crate) tenant: u64,
    /// The protocol's resume JSON, marked `"reader": true`.
    pub(crate) resume: String,
    /// The first bytes the copy reads.
    pub(crate) prelude: Bytes,
    /// Chooses among the copies.
    pub(crate) pick: u64,
}

fn unavailable(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ForwardFailed, msg)
}

/// Opens a shadow connection to a copy of the tenant's unit that doesn't
/// lead it.
///
/// # Errors
/// 39788 when no such copy is known.
pub(crate) fn open(o: Open) -> Result<Shadow> {
    let to = crate::forward::reader_target(o.tenant, o.pick)
        .ok_or_else(|| unavailable("no copy of the tenant's data is known"))?;
    let (mine, theirs) = tokio::io::duplex(PIPE);
    let h = crate::forward::Handoff {
        listener: o.listener,
        protocol: o.protocol,
        peer: o.peer,
        tls: o.tls,
        tenant: o.tenant,
        resume: o.resume,
        prelude: o.prelude,
        to,
    };
    tokio::spawn(async move {
        if let Err(e) = crate::forward::splice(theirs, h).await {
            tracing::debug!(to, error = %e, "shadow connection to a copy not opened");
        }
    });
    Ok(Shadow { io: mine, node: to })
}

/// Counts a shipped read's outcome (`sprig_unit_shipped_reads_total`).
pub(crate) fn count(protocol: Protocol, result: &'static str) {
    metrics::counter!("sprig_unit_shipped_reads_total", "protocol" => protocol.as_str(), "result" => result)
        .increment(1);
}

/// A session's shipping state: its shadow connection, when the next try
/// may happen after a failure, and its `SET sprig.read_from`.
#[derive(Debug, Default)]
pub struct SessionShip {
    /// What a MySQL session needs to open its shadow connection.
    pub(crate) mysql_login: Option<Arc<MysqlLogin>>,
    /// The MySQL session's shadow connection.
    pub(crate) mysql: Option<MysqlShadow>,
    /// No shadow connection before then.
    pub(crate) retry_at: Option<Instant>,
    /// `SET sprig.read_from` on this session.
    pub(crate) read_pref: Option<ReadFrom>,
    /// The tenant's `reads.read_from` and when it was read.
    pub(crate) tenant_pref: Option<(Instant, ReadFrom)>,
}

impl SessionShip {
    /// Whether a shadow connection may be tried now.
    pub(crate) fn may_try(&self) -> bool {
        self.retry_at.is_none_or(|t| Instant::now() >= t)
    }

    /// A failure: no shadow connection for [`RETRY_AFTER`].
    pub(crate) fn failed(&mut self) {
        self.mysql = None;
        self.retry_at = Some(Instant::now() + RETRY_AFTER);
    }

    /// Where the session reads by default: its `SET sprig.read_from`,
    /// else the tenant's (read at most once a second).
    pub(crate) fn default_from(&mut self, tenant: u64) -> ReadFrom {
        if let Some(p) = self.read_pref {
            return p;
        }
        match self.tenant_pref {
            Some((at, p)) if at.elapsed() < Duration::from_secs(1) => p,
            _ => {
                let p = crate::reader::knobs(tenant).read_from;
                self.tenant_pref = Some((Instant::now(), p));
                p
            }
        }
    }
}

/// A MySQL session's login, kept to open its shadow connection: the
/// resume of its greeting and its login packet.
#[derive(Debug, Clone)]
pub(crate) struct MysqlLogin {
    /// The listener the client connected to.
    pub(crate) listener: &'static str,
    /// MySQL or MariaDB.
    pub(crate) protocol: Protocol,
    /// The greeting's resume (`reader` set when used).
    pub(crate) resume: crate::mysql::handoff::MysqlResume,
    /// The login packet as the client sent it.
    pub(crate) packet: Bytes,
}

/// A MySQL shadow connection.
#[derive(Debug)]
pub(crate) struct MysqlShadow {
    s: Shadow,
    caps: Caps,
    /// The database the copy's session has.
    db: Option<String>,
    /// The variables sent to it.
    vars: BTreeMap<String, String>,
}

/// How a shipped statement went.
#[derive(Debug)]
pub(crate) enum Shipped {
    /// The copy answered; the answer went to the client.
    Done,
    /// The copy couldn't run it, and nothing went to the client: run it
    /// here.
    Fallback(SprigError),
}

impl MysqlShadow {
    /// Opens the session's shadow connection and logs in.
    ///
    /// # Errors
    /// The copy is unknown, unreachable, or refused the login.
    pub(crate) async fn open(
        login: &MysqlLogin,
        principal: sprig_auth::handoff::PrincipalWire,
        o: Open,
        caps: Caps,
        db: Option<String>,
    ) -> Result<Self> {
        let mut resume = login.resume.clone();
        resume.reader = true;
        resume.conn_id = 0;
        resume.shadow = Some(principal);
        let mut prelude = Vec::with_capacity(login.packet.len() + 4);
        let mut seq = resume.seq;
        write_packet(&mut prelude, &mut seq, &login.packet)
            .await
            .map_err(|e| unavailable(format!("shadow login: {e}")))?;
        let o = Open {
            resume: serde_json::to_string(&resume).unwrap_or_default(),
            prelude: prelude.into(),
            ..o
        };
        let mut s = open(o)?;
        let p = tokio::time::timeout(
            Duration::from_secs(5),
            read_packet(&mut s.io, &mut seq, MAX_PACKET),
        )
        .await
        .map_err(|_| unavailable("the copy didn't answer the login in 5 s"))?
        .map_err(|e| unavailable(format!("the copy ended the login: {e}")))?;
        if p.first() != Some(&0x00) {
            return Err(unavailable(match p.first() {
                Some(0xFF) => match ErrPacket::decode(&p) {
                    Ok(e) => format!("the copy refused the login: {}", e.message),
                    Err(_) => "the copy refused the login".to_owned(),
                },
                _ => "the copy answered the login with something else".to_owned(),
            }));
        }
        Ok(Self {
            s,
            caps,
            db,
            vars: BTreeMap::new(),
        })
    }

    /// A `COM_QUERY`'s head: with `QUERY_ATTRIBUTES`, no attributes (a
    /// count of 0 in one set).
    fn com_query(&self) -> Vec<u8> {
        if self.caps.contains(Caps::QUERY_ATTRIBUTES) {
            vec![0x03, 0x00, 0x01]
        } else {
            vec![0x03]
        }
    }

    /// One command whose answer must be OK.
    async fn command_ok(&mut self, payload: &[u8]) -> Result<()> {
        let mut seq = 0u8;
        write_packet(&mut self.s.io, &mut seq, payload)
            .await
            .map_err(|e| unavailable(format!("shadow write: {e}")))?;
        let p = read_packet(&mut self.s.io, &mut seq, MAX_PACKET)
            .await
            .map_err(|e| unavailable(format!("shadow read: {e}")))?;
        match p.first() {
            Some(0x00) => Ok(()),
            Some(0xFF) => Err(match ErrPacket::decode(&p) {
                Ok(e) => crate::exec::relay::backend_error(&e),
                Err(e) => unavailable(e.to_string()),
            }),
            _ => {
                // A result set where an OK was expected: drain it.
                Err(unavailable("unexpected answer on the shadow connection"))
            }
        }
    }

    /// Makes the copy's session match: its database and the variables the
    /// session set.
    async fn sync(&mut self, db: Option<&str>, vars: &BTreeMap<String, String>) -> Result<()> {
        if let Some(d) = db
            && self.db.as_deref() != Some(d)
        {
            let mut p = Vec::with_capacity(d.len() + 1);
            p.push(0x02); // COM_INIT_DB
            p.extend_from_slice(d.as_bytes());
            self.command_ok(&p).await?;
            self.db = Some(d.to_owned());
        }
        let changed: Vec<(&String, &String)> = vars
            .iter()
            .filter(|(k, v)| shipped_var(k) && self.vars.get(*k) != Some(*v))
            .collect();
        if !changed.is_empty() {
            let set = |vars: &[(&String, &String)]| {
                let mut sql = String::from("SET SESSION ");
                for (i, (k, v)) in vars.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(k);
                    sql.push_str(" = ");
                    sql.push_str(&literal(v));
                }
                sql
            };
            let all = set(&changed);
            let mut p = self.com_query();
            p.extend_from_slice(all.as_bytes());
            if self.command_ok(&p).await.is_err() {
                // One by one: a variable the copy's engine doesn't take (a
                // MariaDB-only one a driver sets at connect) is skipped, as
                // it is on the leader's engine path.
                for kv in &changed {
                    let one = set(std::slice::from_ref(kv));
                    let mut p = self.com_query();
                    p.extend_from_slice(one.as_bytes());
                    if let Err(e) = self.command_ok(&p).await {
                        tracing::debug!(statement = %one, error = %e, "shadow connection: variable not taken");
                    }
                }
            }
            for (k, v) in changed {
                self.vars.insert(k.clone(), v.clone());
            }
        }
        Ok(())
    }

    /// Runs `sql` on the copy with the session's token and database, and
    /// sends its answer to `out` with the session's `status`.
    ///
    /// # Errors
    /// Only the client's sink's: the copy's errors go to `out`, and
    /// failures before anything reached the client come back as
    /// [`Shipped::Fallback`].
    #[allow(clippy::too_many_lines)] // the copy's answer, packet by packet
    pub(crate) async fn run(
        &mut self,
        sql: &[u8],
        token: u64,
        db: Option<&str>,
        vars: &BTreeMap<String, String>,
        status: sprig_proto_mysql::StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Shipped> {
        if let Err(e) = self.sync(db, vars).await {
            return Ok(Shipped::Fallback(e));
        }
        let mut p = self.com_query();
        p.reserve(sql.len() + 40);
        p.extend_from_slice(TOKEN_PREFIX);
        p.extend_from_slice(token.to_string().as_bytes());
        p.extend_from_slice(b"*/ ");
        p.extend_from_slice(sql);
        let mut seq = 0u8;
        if let Err(e) = write_packet(&mut self.s.io, &mut seq, &p).await {
            return Ok(Shipped::Fallback(unavailable(format!("shadow write: {e}"))));
        }
        let first = match read_packet(&mut self.s.io, &mut seq, MAX_PACKET).await {
            Ok(p) => p,
            Err(e) => return Ok(Shipped::Fallback(unavailable(format!("shadow read: {e}")))),
        };
        match first.first() {
            Some(0x00) => {
                let ok = OkPacket::decode(&first, self.caps).ok();
                let mut reply = OkPacket::new(status);
                if let Some(ok) = ok {
                    reply.warnings = ok.warnings;
                    reply.affected_rows = ok.affected_rows;
                }
                out.ok(reply).await?;
                return Ok(Shipped::Done);
            }
            Some(0xFF) => {
                let e = match ErrPacket::decode(&first) {
                    Ok(e) => e,
                    Err(e) => return Ok(Shipped::Fallback(unavailable(e.to_string()))),
                };
                // The copy couldn't reach a leader, or stopped holding the
                // data: the leader (here) answers instead.
                if matches!(e.code, 39110 | 39301 | 39502 | 39788) {
                    return Ok(Shipped::Fallback(crate::exec::relay::backend_error(&e)));
                }
                out.error(&crate::exec::relay::backend_error(&e)).await?;
                return Ok(Shipped::Done);
            }
            Some(0xFB) | None => {
                return Ok(Shipped::Fallback(unavailable(
                    "unexpected answer on the shadow connection",
                )));
            }
            Some(_) => {}
        }
        let n = match sprig_proto_mysql::resultset::decode_column_count(&first) {
            Ok(n) => usize::try_from(n).unwrap_or(0),
            Err(e) => return Ok(Shipped::Fallback(unavailable(e.to_string()))),
        };
        let mut defs = Vec::with_capacity(n);
        for _ in 0..n {
            match read_packet(&mut self.s.io, &mut seq, MAX_PACKET).await {
                Ok(d) => defs.push(d),
                Err(e) => return Ok(Shipped::Fallback(unavailable(format!("shadow read: {e}")))),
            }
        }
        let deprecate_eof = self.caps.contains(Caps::DEPRECATE_EOF);
        if !deprecate_eof && let Err(e) = read_packet(&mut self.s.io, &mut seq, MAX_PACKET).await {
            return Ok(Shipped::Fallback(unavailable(format!("shadow read: {e}"))));
        }
        out.begin_rows_raw(&defs, RowFormat::Text).await?;
        loop {
            let row = match read_packet(&mut self.s.io, &mut seq, MAX_PACKET).await {
                Ok(r) => r,
                Err(e) => {
                    // Rows already went out: the client sees the loss.
                    out.error(&SprigError::new(
                        SprigCode::BackendLost,
                        format!("the copy serving this read went away: {e}"),
                    ))
                    .await?;
                    return Ok(Shipped::Done);
                }
            };
            match row.first() {
                Some(0xFE) if row.len() < 0xFF_FFFF => {
                    let warnings = if deprecate_eof {
                        OkPacket::decode(&row, self.caps).map_or(0, |o| o.warnings)
                    } else {
                        EofPacket::decode(&row, self.caps).map_or(0, |e| e.warnings)
                    };
                    out.end_rows(EndOfRows { status, warnings }).await?;
                    return Ok(Shipped::Done);
                }
                Some(0xFF) => {
                    let e = ErrPacket::decode(&row).map_or_else(
                        |e| unavailable(e.to_string()),
                        |e| crate::exec::relay::backend_error(&e),
                    );
                    out.error(&e).await?;
                    return Ok(Shipped::Done);
                }
                _ => {
                    if let Err(r) = out.try_raw_row(row) {
                        out.raw_row(r).await?;
                    }
                }
            }
        }
    }
}

/// A MongoDB connection's reads from copies: on the leader, its shadow
/// connection and the cursors its shipped reads left open there; on a
/// copy, the reader state of a shadow connection.
#[derive(Debug, Default)]
pub struct MongoFollow {
    /// This connection is a leader connection's shadow: its reads run under
    /// these rules ([`crate::reader`]).
    pub(crate) reader: Option<Arc<crate::reader::Reader>>,
    /// The shadow connection.
    pub(crate) shadow: Option<MongoShadow>,
    /// Cursors open on the shadow connection's copy.
    pub(crate) cursors: std::collections::HashSet<i64>,
    /// No shadow connection before then.
    pub(crate) retry_at: Option<Instant>,
}

impl MongoFollow {
    /// Whether a shadow connection may be tried now.
    pub(crate) fn may_try(&self) -> bool {
        self.retry_at.is_none_or(|t| Instant::now() >= t)
    }

    /// A failure: the shadow connection and its cursors are gone; none for
    /// [`RETRY_AFTER`].
    pub(crate) fn failed(&mut self) {
        self.shadow = None;
        self.cursors.clear();
        self.retry_at = Some(Instant::now() + RETRY_AFTER);
    }
}

/// A MongoDB shadow connection: commands go as `OP_MSG`s, uncompressed.
#[derive(Debug)]
pub(crate) struct MongoShadow {
    s: Shadow,
    next: i32,
}

impl MongoShadow {
    /// Opens it: the copy's connection starts logged in (the resume carries
    /// the principal), so nothing is exchanged before the first command.
    ///
    /// # Errors
    /// No copy is known.
    pub(crate) fn open(o: Open) -> Result<Self> {
        Ok(Self {
            s: open(o)?,
            next: 1,
        })
    }

    /// The copy's Raft id.
    pub(crate) fn node(&self) -> u64 {
        self.s.node
    }

    /// Runs one command on the copy: its reply body.
    ///
    /// # Errors
    /// The connection failed (the command may or may not have run).
    pub(crate) async fn command(
        &mut self,
        body: bson::RawDocumentBuf,
    ) -> Result<bson::RawDocumentBuf> {
        use sprig_proto_mongo::codec;
        use tokio::io::AsyncWriteExt;
        let f = codec::Frame::Msg(codec::OpMsg {
            flags: 0,
            body,
            sequences: Vec::new(),
        });
        let bytes = codec::encode(self.next, 0, &f, None)
            .map_err(|e| unavailable(format!("shadow command: {e}")))?;
        self.next = self.next.wrapping_add(1);
        self.s
            .io
            .write_all(&bytes)
            .await
            .map_err(|e| unavailable(format!("shadow write: {e}")))?;
        let msg = codec::read_message(&mut self.s.io)
            .await
            .map_err(|e| unavailable(format!("shadow read: {e}")))?
            .ok_or_else(|| unavailable("the copy closed the shadow connection"))?;
        match codec::decode(&msg).map_err(|e| unavailable(format!("shadow reply: {e}")))? {
            (_, codec::Frame::Msg(m), _) => Ok(m.body),
            _ => Err(unavailable("the copy answered with another opcode")),
        }
    }
}

/// Session variables a shadow connection takes: the ones that change what
/// a read returns. Transaction, lock and timeout variables stay the copy's
/// own; the character sets come with the replayed login packet (`SET NAMES`
/// arrives as the same variables, which the copy's login already set).
fn shipped_var(name: &str) -> bool {
    !matches!(
        name,
        "autocommit"
            | "transaction_read_only"
            | "tx_read_only"
            | "transaction_isolation"
            | "tx_isolation"
            | "innodb_lock_wait_timeout"
            | "lock_wait_timeout"
            | "sql_log_bin"
            | "foreign_key_checks"
            | "unique_checks"
            | "insert_id"
            | "last_insert_id"
            | "timestamp"
            | "pseudo_thread_id"
    ) && !name.starts_with("character_set_")
        && !name.starts_with("collation_")
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// A variable's value as a SQL literal: numbers as they are, anything else
/// quoted.
fn literal(v: &str) -> String {
    let numeric = !v.is_empty()
        && v.trim_start_matches('-')
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'.')
        && v.trim_start_matches('-')
            .bytes()
            .any(|b| b.is_ascii_digit());
    if numeric {
        return v.to_owned();
    }
    let mut s = String::with_capacity(v.len() + 2);
    s.push('\'');
    for c in v.chars() {
        match c {
            '\'' => s.push_str("''"),
            '\\' => s.push_str("\\\\"),
            c => s.push(c),
        }
    }
    s.push('\'');
    s
}

/// A shipped statement's token comment, if `sql` starts with one: the
/// token and the statement after it.
#[must_use]
pub fn take_token(sql: &[u8]) -> Option<(u64, &[u8])> {
    let rest = sql.strip_prefix(TOKEN_PREFIX)?;
    let end = rest.windows(2).position(|w| w == b"*/")?;
    let t = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
    let after = &rest[end + 2..];
    Some((t, after.strip_prefix(b" ").unwrap_or(after)))
}

/// Whether a statement may run on a copy instead of the session's node:
/// one `SELECT` (or `WITH`) that takes no locks, assigns nothing, and
/// reads nothing of the session's own state (user variables, the last
/// insert id, locks, sequences). A statement that only looks like one of
/// those runs on the leader, which is always allowed.
#[must_use]
pub fn shippable(sql: &[u8]) -> bool {
    const NOT: [&[u8]; 19] = [
        b" INTO ",
        b"\nINTO ",
        b"FOR UPDATE",
        b"FOR SHARE",
        b"LOCK IN SHARE MODE",
        b"LAST_INSERT_ID",
        b"FOUND_ROWS",
        b"SQL_CALC_FOUND_ROWS",
        b"ROW_COUNT",
        b"CONNECTION_ID",
        b"GET_LOCK",
        b"RELEASE_LOCK",
        b"RELEASE_ALL_LOCKS",
        b"IS_USED_LOCK",
        b"IS_FREE_LOCK",
        b"NEXTVAL",
        b"NEXT VALUE",
        b"SETVAL",
        b"LASTVAL",
    ];
    let s = skip_space_and_comments(sql);
    let head: Vec<u8> = s.iter().take(6).map(u8::to_ascii_uppercase).collect();
    if !(head.starts_with(b"SELECT") || head.starts_with(b"WITH")) {
        return false;
    }
    if s.contains(&b'@') {
        return false;
    }
    // A trailing `;` is fine; another statement after it is not.
    if let Some(i) = s.iter().position(|b| *b == b';')
        && s[i + 1..].iter().any(|b| !b.is_ascii_whitespace())
    {
        return false;
    }
    let upper = s.to_ascii_uppercase();
    !NOT.iter().any(|w| upper.windows(w.len()).any(|x| x == *w))
}

fn skip_space_and_comments(mut s: &[u8]) -> &[u8] {
    loop {
        while let Some((b, rest)) = s.split_first()
            && b.is_ascii_whitespace()
        {
            s = rest;
        }
        if let Some(rest) = s.strip_prefix(b"/*")
            && !rest.starts_with(b"!")
            && let Some(end) = rest.windows(2).position(|w| w == b"*/")
        {
            s = &rest[end + 2..];
            continue;
        }
        return s;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_reads_ship() {
        assert!(shippable(
            b"SELECT /*+ READ_FROM(follower) */ * FROM t WHERE id = 1"
        ));
        assert!(shippable(b"/* x */ select a from t;"));
        assert!(shippable(b"WITH c AS (SELECT 1) SELECT * FROM c"));
        assert!(!shippable(b"SELECT @a"));
        assert!(!shippable(b"SELECT a INTO @x FROM t"));
        assert!(!shippable(b"SELECT * FROM t FOR UPDATE"));
        assert!(!shippable(b"select last_insert_id()"));
        assert!(!shippable(b"SELECT 1; DELETE FROM t"));
        assert!(!shippable(b"UPDATE t SET a = 1"));
        assert!(!shippable(b"select nextval(s)"));
    }

    #[test]
    fn the_token_comment_round_trips() {
        let mut q = TOKEN_PREFIX.to_vec();
        q.extend_from_slice(b"42*/ SELECT 1");
        assert_eq!(take_token(&q), Some((42, &b"SELECT 1"[..])));
        assert_eq!(take_token(b"SELECT 1"), None);
    }

    #[test]
    fn variables_become_literals() {
        assert_eq!(literal("42"), "42");
        assert_eq!(literal("-1.5"), "-1.5");
        assert_eq!(literal("+00:00"), "'+00:00'");
        assert_eq!(
            literal("STRICT_TRANS_TABLES,NO_ZERO_DATE"),
            "'STRICT_TRANS_TABLES,NO_ZERO_DATE'"
        );
        assert_eq!(literal("it's"), "'it''s'");
        assert!(shipped_var("sql_mode"));
        assert!(!shipped_var("autocommit"));
        assert!(!shipped_var("character_set_results"));
        assert!(!shipped_var("a;b"));
    }
}
