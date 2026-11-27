//! Lua scripts and functions on a database stored on the engine: `EVAL`,
//! `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`, `SCRIPT` and
//! `FUNCTION`.
//!
//! A script runs in one engine transaction ([`EngineScript`]): its
//! `redis.call`s run in it, each key held from the first command that names
//! it, and it commits once, when the script ends, so other clients never
//! see half of it. A script that raises an error keeps the writes it made
//! before (Redis does the same); one stopped by `SCRIPT KILL`, or by its
//! memory budget, changes nothing. When two scripts deadlock, one of them
//! fails before anything of it was sent and runs again from the start.
//!
//! `redis.lua_time_limit_ms` is Redis's busy threshold here
//! (`busy-reply-threshold`, once called `lua-time-limit`): a script running
//! longer makes the tenant's other connections answer `BUSY`, and `SCRIPT
//! KILL` / `FUNCTION KILL` stop it. Unlike Redis, a script still running at
//! twice the threshold is stopped and rolled back (39850): a node serves
//! many tenants, and a script whose client went away has nobody to kill it.
//!
//! The script cache and the function libraries are kept on the engine
//! ([`LuaStore`]), so every gateway sees them; `FLUSHALL` doesn't remove
//! them.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_proto_redis::{Command, Frame};

use super::{Session, backoff};
use crate::ctx::upper;
use crate::engine::{EngineDatabase, EngineScript, LuaStore, ScriptFailed};
use crate::{lua, reply};

/// A script on the engine stops at this many times the busy threshold.
const HARD_LIMIT_TIMES: u32 = 2;

/// The wall-time budget of a script on the engine: twice the busy
/// threshold.
fn hard_limit(threshold: Duration) -> Duration {
    threshold.saturating_mul(HARD_LIMIT_TIMES)
}

/// The commands this module answers on an engine database.
pub(super) fn handles(name: &str) -> bool {
    runs_script(name) || matches!(name, "script" | "function")
}

/// The commands that run a script (and may be queued in `MULTI`).
pub(super) fn runs_script(name: &str) -> bool {
    matches!(
        name,
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro"
    )
}

/// Whether another connection may still send `a` (named `name`) while a
/// script (`function`: a function) runs past the busy threshold.
pub(super) fn allowed_when_busy(name: &str, a: &[Bytes], function: bool) -> bool {
    let sub = |s: &[u8]| a.get(1).is_some_and(|x| x.eq_ignore_ascii_case(s));
    match name {
        "auth" | "hello" | "quit" | "reset" | "multi" | "discard" | "watch" | "unwatch" => true,
        "script" => !function && sub(b"kill"),
        "function" => function && sub(b"kill") || sub(b"stats"),
        _ => false,
    }
}

/// Redis's `BUSY` reply while a script (or a function) runs long.
pub(super) fn busy_reply(function: bool) -> Frame {
    Frame::error(format!(
        "BUSY Redis is busy running a script. You can only call {} or SHUTDOWN NOSAVE.",
        if function {
            "FUNCTION KILL"
        } else {
            "SCRIPT KILL"
        }
    ))
}

/// Redis's arity of `SCRIPT`/`FUNCTION` subcommand `sub` (negative: at
/// least), or `None` for one it doesn't know.
fn sub_arity(cmd: &str, sub: &str) -> Option<i32> {
    Some(match (cmd, sub) {
        ("script", "load" | "debug") | ("function", "delete") => 3,
        ("script", "exists") | ("function", "load" | "restore") => -3,
        ("script" | "function", "flush") | ("function", "list") => -2,
        ("script" | "function", "kill" | "help") | ("function", "stats" | "dump") => 2,
        _ => return None,
    })
}

/// Redis's reply to subcommand `a[1]` of `cmd` with the wrong number of
/// arguments.
fn sub_arity_err(cmd: &str, a: &[Bytes]) -> Option<Frame> {
    let sub = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
    let n = sub_arity(cmd, &sub)?;
    let len = i32::try_from(a.len()).unwrap_or(i32::MAX);
    let ok = if n < 0 { len >= -n } else { len == n };
    (!ok).then(|| {
        Frame::err(&format!(
            "wrong number of arguments for '{cmd}|{sub}' command"
        ))
    })
}

/// A run's place in the running-scripts list, removed (and the run
/// stopped) however the run ends, even when its client goes away and the
/// command's future is dropped.
pub(super) struct RunGuard {
    pub(super) env: Arc<crate::env::Env>,
    pub(super) id: u64,
    pub(super) state: Arc<sprig_logic_js::budget::RunState>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        // A run that already ended ignores this.
        self.state.cancel();
        self.env.scripts.remove(self.id);
    }
}

/// A script ready to run.
struct Prepared {
    body: lua::Body,
    keys: Vec<Bytes>,
    args: Vec<Bytes>,
    read_only: bool,
    function: bool,
    /// `EVAL`'s digest and text, cached once it ran.
    cache: Option<(String, Bytes)>,
}

/// How a script's run ended.
enum Ran {
    /// It returned, or raised an error: commit.
    Reply(Frame),
    /// It was killed or ran out of memory: roll back.
    Abort(Frame),
    /// Its transaction failed: roll back, maybe run again.
    Failed(ScriptFailed),
}

/// One library of the tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Library {
    name: String,
    code: String,
    /// `(function, flags)`.
    functions: Vec<(String, Vec<String>)>,
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&u32::try_from(s.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn get_str(b: &mut &[u8]) -> Option<String> {
    let n = usize::try_from(u32::from_be_bytes(b.get(..4)?.try_into().ok()?)).ok()?;
    let s = b.get(4..4 + n)?;
    let out = String::from_utf8_lossy(s).into_owned();
    *b = &b[4 + n..];
    Some(out)
}

/// The stored form of the tenant's libraries (sorted by name).
fn encode_libs(libs: &[Library]) -> Bytes {
    let mut out = Vec::new();
    for l in libs {
        put_str(&mut out, &l.name);
        put_str(&mut out, &l.code);
        put_str(&mut out, &l.functions.len().to_string());
        for (f, flags) in &l.functions {
            put_str(&mut out, f);
            put_str(&mut out, &flags.join(","));
        }
    }
    out.into()
}

fn decode_libs(mut b: &[u8]) -> Vec<Library> {
    let mut out = Vec::new();
    while !b.is_empty() {
        let (Some(name), Some(code), Some(n)) = (get_str(&mut b), get_str(&mut b), get_str(&mut b))
        else {
            break;
        };
        let n: usize = n.parse().unwrap_or(0);
        let mut functions = Vec::with_capacity(n);
        for _ in 0..n {
            let (Some(f), Some(flags)) = (get_str(&mut b), get_str(&mut b)) else {
                break;
            };
            functions.push((
                f,
                flags
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ));
        }
        out.push(Library {
            name,
            code,
            functions,
        });
    }
    out
}

/// A library's body with its shebang line blanked (line numbers stay).
fn library_body(code: &str) -> String {
    format!("\n{}", code.split_once('\n').map_or("", |x| x.1))
}

// ---- FUNCTION DUMP / RESTORE payloads: Redis's own format ----

/// The RDB opcode of a function library.
const RDB_FUNCTION2: u8 = 245;
/// The RDB version written (Redis 7.4's).
const RDB_VERSION: u16 = 12;

/// CRC-64 (Jones polynomial, reflected, as Redis's payloads use).
fn crc64(data: &[u8]) -> u64 {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    let mut crc = 0u64;
    for &b in data {
        crc ^= u64::from(b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn rdb_len(out: &mut Vec<u8>, n: usize) {
    if n < 1 << 6 {
        out.push(u8::try_from(n).unwrap_or(0));
    } else if n < 1 << 14 {
        out.push(0x40 | u8::try_from(n >> 8).unwrap_or(0));
        out.push(u8::try_from(n & 0xff).unwrap_or(0));
    } else {
        out.push(0x80);
        out.extend_from_slice(&u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes());
    }
}

/// `FUNCTION DUMP`'s payload for `libs` (strings stored plainly).
fn dump_payload(libs: &[Library]) -> Vec<u8> {
    let mut out = Vec::new();
    for l in libs {
        out.push(RDB_FUNCTION2);
        rdb_len(&mut out, l.code.len());
        out.extend_from_slice(l.code.as_bytes());
    }
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// An RDB length: `(value, special encoding)`.
fn read_len(b: &mut &[u8]) -> Option<(u64, bool)> {
    let first = *b.first()?;
    *b = &b[1..];
    Some(match first >> 6 {
        0 => (u64::from(first & 0x3f), false),
        1 => {
            let second = *b.first()?;
            *b = &b[1..];
            ((u64::from(first & 0x3f) << 8) | u64::from(second), false)
        }
        2 if first == 0x80 => {
            let v = u32::from_be_bytes(b.get(..4)?.try_into().ok()?);
            *b = &b[4..];
            (u64::from(v), false)
        }
        2 if first == 0x81 => {
            let v = u64::from_be_bytes(b.get(..8)?.try_into().ok()?);
            *b = &b[8..];
            (v, false)
        }
        2 => return None,
        _ => (u64::from(first & 0x3f), true),
    })
}

/// LZF decompression (the RDB string encoding 3).
fn lzf_decompress(input: &[u8], out_len: usize) -> Option<Vec<u8>> {
    // A library's text is never near this; a crafted length is refused
    // before anything is allocated.
    if out_len > 512 << 20 {
        return None;
    }
    let mut out = Vec::with_capacity(out_len.min(input.len().saturating_mul(4)));
    let mut i = 0;
    while i < input.len() {
        let ctrl = usize::from(input[i]);
        i += 1;
        if ctrl < 32 {
            let n = ctrl + 1;
            out.extend_from_slice(input.get(i..i + n)?);
            i += n;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += usize::from(*input.get(i)?);
                i += 1;
            }
            let back = ((ctrl & 0x1f) << 8) + usize::from(*input.get(i)?) + 1;
            i += 1;
            let start = out.len().checked_sub(back)?;
            for k in 0..len + 2 {
                let c = *out.get(start + k)?;
                out.push(c);
            }
        }
        if out.len() > out_len {
            return None;
        }
    }
    (out.len() == out_len).then_some(out)
}

/// An RDB string.
fn read_rdb_string(b: &mut &[u8]) -> Option<Vec<u8>> {
    let (n, special) = read_len(b)?;
    if !special {
        let n = usize::try_from(n).ok()?;
        let s = b.get(..n)?.to_vec();
        *b = &b[n..];
        return Some(s);
    }
    match n {
        0 => {
            let v = i8::from_le_bytes([*b.first()?]);
            *b = &b[1..];
            Some(v.to_string().into_bytes())
        }
        1 => {
            let v = i16::from_le_bytes(b.get(..2)?.try_into().ok()?);
            *b = &b[2..];
            Some(v.to_string().into_bytes())
        }
        2 => {
            let v = i32::from_le_bytes(b.get(..4)?.try_into().ok()?);
            *b = &b[4..];
            Some(v.to_string().into_bytes())
        }
        3 => {
            let (clen, _) = read_len(b)?;
            let (ulen, _) = read_len(b)?;
            let clen = usize::try_from(clen).ok()?;
            let data = b.get(..clen)?;
            let out = lzf_decompress(data, usize::try_from(ulen).ok()?)?;
            *b = &b[clen..];
            Some(out)
        }
        _ => None,
    }
}

/// The library codes of a `FUNCTION DUMP` payload, or `None` when its
/// version or checksum is wrong (or it is malformed).
fn restore_payload(p: &[u8]) -> Option<Vec<String>> {
    if p.len() < 10 {
        return None;
    }
    let (body, footer) = p.split_at(p.len() - 8);
    let crc = u64::from_le_bytes(footer.try_into().ok()?);
    if crc != 0 && crc != crc64(body) {
        return None;
    }
    let (mut rest, ver) = body.split_at(body.len() - 2);
    if u16::from_le_bytes(ver.try_into().ok()?) > RDB_VERSION {
        return None;
    }
    let mut out = Vec::new();
    while let Some(&op) = rest.first() {
        if op != RDB_FUNCTION2 {
            return None;
        }
        rest = &rest[1..];
        out.push(String::from_utf8(read_rdb_string(&mut rest)?).ok()?);
    }
    Some(out)
}

impl Session {
    fn scope(&self) -> String {
        self.login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default()
    }

    /// The reply to `BUSY`-blocked command `a` (named `name`), when another
    /// connection's script runs past the busy threshold.
    pub(super) fn engine_busy(&self, name: &str, a: &[Bytes]) -> Option<Frame> {
        let function = self.busy_run()?;
        (!allowed_when_busy(name, a, function)).then(|| busy_reply(function))
    }

    /// Whether a script of the tenant runs past the busy threshold: then
    /// whether it is a function's.
    pub(super) fn busy_run(&self) -> Option<bool> {
        if !self.env.scripts.any() {
            return None;
        }
        let scope = self.login.as_ref().map_or("", |l| l.scope.as_str());
        let threshold = self.env.config().lua_time_limit;
        self.env.scripts.busy_over(scope, threshold)
    }

    /// Answers script command `a` (named `name`, see [`handles`]) on
    /// engine database `e`.
    pub(super) async fn lua_on_engine(
        &mut self,
        e: &Arc<dyn EngineDatabase>,
        name: &str,
        a: &[Bytes],
    ) -> Frame {
        match name {
            "script" => self.engine_script_cmd(e, a).await,
            "function" => self.engine_function_cmd(e, a).await,
            _ => match self.prepare_script(e, name, a, false).await {
                Ok(p) => self.run_alone(e, &p).await,
                Err(f) => f,
            },
        }
    }

    /// The tenant's libraries.
    async fn libraries(
        e: &Arc<dyn EngineDatabase>,
    ) -> Result<(Option<Bytes>, Vec<Library>), Frame> {
        let raw = e.lua_get(LuaStore::Functions, b"").await?;
        let libs = raw.as_deref().map(decode_libs).unwrap_or_default();
        Ok((raw, libs))
    }

    /// What `EVAL...`/`FCALL...` command `a` runs, or its reply.
    async fn prepare_script(
        &mut self,
        e: &Arc<dyn EngineDatabase>,
        name: &str,
        a: &[Bytes],
        in_exec: bool,
    ) -> Result<Prepared, Frame> {
        let (keys, args) = Self::numkeys(a, 2)?;
        if name.starts_with("fcall") {
            let fname = String::from_utf8_lossy(&a[1]).into_owned();
            let (_, libs) = Self::libraries(e).await?;
            let found = libs.iter().find_map(|l| {
                l.functions
                    .iter()
                    .find(|(f, _)| *f == fname)
                    .map(|(_, flags)| (l, flags))
            });
            let Some((lib, flags)) = found else {
                if in_exec {
                    // It would run outside the transaction, again on a rerun.
                    return Err(Frame::err(
                        "a logic function can't be called inside MULTI on this database yet",
                    ));
                }
                // Not a Lua function: a logic-layer function exposed to
                // Redis, if there is one.
                return Err(self.logic_fcall(name, &a[1], &keys, &args).await);
            };
            let no_writes = flags.iter().any(|f| f == "no-writes");
            if name == "fcall_ro" && !no_writes {
                return Err(Frame::err(
                    "Can not execute a script with write flag using *_ro command.",
                ));
            }
            return Ok(Prepared {
                body: lua::Body::Function {
                    source: library_body(&lib.code),
                    name: fname,
                },
                keys,
                args,
                read_only: no_writes || name == "fcall_ro",
                function: true,
                cache: None,
            });
        }
        let by_sha = name.starts_with("evalsha");
        let source = if by_sha {
            let sha = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
            match e.lua_get(LuaStore::Scripts, sha.as_bytes()).await? {
                Some(b) => b,
                None => {
                    return Err(Frame::error(
                        "NOSCRIPT No matching script. Please use EVAL.",
                    ));
                }
            }
        } else {
            a[1].clone()
        };
        let text = String::from_utf8_lossy(&source).into_owned();
        let sha = lua::sha1_hex(&source);
        let (flags, body) = lua::shebang(&text).map_err(Frame::error)?;
        Ok(Prepared {
            body: lua::Body::Script {
                source: body,
                name: sha.clone(),
            },
            keys,
            args,
            read_only: name.ends_with("_ro") || flags.no_writes,
            function: false,
            cache: (!by_sha).then_some((sha, source)),
        })
    }

    /// Caches `EVAL`'s script once it ran (not when it didn't compile).
    async fn cache_script(e: &Arc<dyn EngineDatabase>, p: &Prepared, reply: &Frame) {
        let Some((sha, source)) = &p.cache else {
            return;
        };
        if matches!(reply, Frame::Error(m) if m.starts_with(b"ERR Error compiling script")) {
            return;
        }
        if matches!(e.lua_get(LuaStore::Scripts, sha.as_bytes()).await, Ok(None))
            && let Err(f) = e
                .lua_swap(
                    LuaStore::Scripts,
                    sha.as_bytes(),
                    Some(None),
                    Some(source.clone()),
                )
                .await
        {
            tracing::debug!(reply = ?f, "redis script not cached");
        }
    }

    /// Sends a committed script's `PUBLISH`es.
    async fn publish_all(&mut self, pubs: Vec<Vec<Bytes>>) {
        for a in pubs {
            let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
            if let Some(spec) = crate::spec::lookup(&name)
                && let Err(err) = Box::pin(self.dispatch(&name, spec, &a, true)).await
            {
                tracing::debug!(error = %err, "a script's PUBLISH was not sent");
            }
        }
    }

    /// Runs `p` in its own transaction, again after a deadlock.
    async fn run_alone(&mut self, e: &Arc<dyn EngineDatabase>, p: &Prepared) -> Frame {
        let retries = self.env.config().write_retries;
        let mut attempt = 0;
        loop {
            let mut txn = match e.script(&[]).await {
                Ok(Some(t)) => t,
                Ok(None) => return Frame::err("the script's transaction could not start"),
                Err(f) => return f,
            };
            let mut pubs = Vec::new();
            let failed = match self.run_lua(e, txn.as_mut(), p, &mut pubs).await {
                Ran::Reply(f) => match txn.commit().await {
                    Ok(()) => {
                        self.publish_all(pubs).await;
                        Self::cache_script(e, p, &f).await;
                        return f;
                    }
                    Err(sf) => sf,
                },
                Ran::Abort(f) => {
                    // A script that doesn't compile is still cached by
                    // nothing; a killed one changed nothing.
                    return f;
                }
                Ran::Failed(sf) => {
                    drop(txn);
                    sf
                }
            };
            if failed.retry && attempt < retries {
                attempt += 1;
                backoff(attempt).await;
                continue;
            }
            return if failed.retry {
                reply::tryagain()
            } else {
                failed.reply
            };
        }
    }

    /// Runs `p`, serving its `redis.call`s in `txn`.
    async fn run_lua(
        &mut self,
        e: &Arc<dyn EngineDatabase>,
        txn: &mut dyn EngineScript,
        p: &Prepared,
        pubs: &mut Vec<Vec<Bytes>>,
    ) -> Ran {
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        let scope = self.scope();
        let mut started = lua::start_with(
            p.body.clone(),
            p.keys.clone(),
            p.args.clone(),
            self.resp3(),
            &cfg,
            hard_limit(cfg.lua_time_limit),
        );
        let guard = RunGuard {
            id: env.scripts.add(&scope, p.function, &started.state),
            env: Arc::clone(&env),
            state: Arc::clone(&started.state),
        };
        let mut failed: Option<ScriptFailed> = None;
        let done = loop {
            tokio::select! {
                call = started.calls.recv() => {
                    let Some(call) = call else {
                        break (&mut started.done).await;
                    };
                    let reply = if failed.is_some() {
                        Frame::err("the script's transaction failed and will rerun")
                    } else {
                        match self.engine_call(e, txn, &call, p.read_only, pubs).await {
                            Ok(f) => f,
                            Err(sf) => {
                                started.state.cancel();
                                let r = sf.reply.clone();
                                failed = Some(sf);
                                r
                            }
                        }
                    };
                    let _ = call.reply.send(reply);
                }
                done = &mut started.done => break done,
            }
        };
        drop(guard);
        if let Some(sf) = failed {
            return Ran::Failed(sf);
        }
        match done {
            Ok(lua::Outcome::Reply(f)) => Ran::Reply(f),
            Ok(lua::Outcome::Abort(f)) => Ran::Abort(f),
            Err(e) => Ran::Abort(Frame::err(&format!("the script stopped: {e}"))),
        }
    }

    /// One `redis.call` of a script on the engine.
    async fn engine_call(
        &mut self,
        e: &Arc<dyn EngineDatabase>,
        txn: &mut dyn EngineScript,
        call: &lua::Call,
        read_only: bool,
        pubs: &mut Vec<Vec<Bytes>>,
    ) -> Result<Frame, ScriptFailed> {
        let a = &call.args;
        let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
        let Some(spec) = crate::engine::lookup(&name).filter(|s| !s.has('x') || e.runs(&name))
        else {
            return Ok(Frame::err("Unknown Redis command called from script"));
        };
        if !spec.arity_ok(a.len()) {
            return Ok(Frame::err(
                "Wrong number of args calling Redis command from script",
            ));
        }
        if spec.has('s') || spec.has('p') && name != "publish" && name != "spublish" {
            return Ok(Frame::err("This Redis command is not allowed from script"));
        }
        // Redis counts PFCOUNT as a write (it may refresh the cached count).
        if read_only && (spec.has('w') || name == "pfcount") {
            return Ok(Frame::err(
                "Write commands are not allowed from read-only scripts.",
            ));
        }
        if e.runs(&name) || name == "select" {
            return txn.call(a, call.resp3).await;
        }
        Ok(match name.as_str() {
            "ping" => a
                .get(1)
                .map_or_else(|| Frame::simple("PONG"), |m| Frame::Bulk(m.clone())),
            "echo" => Frame::Bulk(a[1].clone()),
            "publish" | "spublish" => {
                // Sent once the script commits: never by an attempt that
                // is rolled back or run again.
                pubs.push(a.clone());
                Frame::Integer(0)
            }
            _ => crate::engine::not_on_engine(&name),
        })
    }

    /// `EXEC` of a queue holding scripts on engine database `e`: the whole
    /// queue in one script transaction.
    pub(super) async fn exec_engine_scripted(
        &mut self,
        e: &Arc<dyn EngineDatabase>,
        queue: &[Command],
        watched: &[crate::engine::Watched],
    ) -> Frame {
        let names: Vec<String> = queue.iter().map(Command::name_lower).collect();
        let retries = self.env.config().write_retries;
        let mut attempt = 0;
        loop {
            let mut txn = match e.script(watched).await {
                Ok(Some(t)) => t,
                Ok(None) => return Frame::null_array(),
                Err(f) => return f,
            };
            let mut out = Vec::with_capacity(queue.len());
            let mut db = None;
            let mut cur = self.db;
            let mut cached = Vec::new();
            let mut pubs = Vec::new();
            let mut failed = None;
            for (c, n) in queue.iter().zip(&names) {
                let r = if e.runs(n) || n == "select" {
                    match txn.call(&c.args, self.resp3()).await {
                        Ok(f) => {
                            if n == "select" && !f.is_error() {
                                db = sprig_proto_redis::num::parse_i64(&c.args[1])
                                    .and_then(|d| u32::try_from(d).ok());
                                cur = db.unwrap_or(cur);
                            }
                            f
                        }
                        Err(sf) => {
                            failed = Some(sf);
                            break;
                        }
                    }
                } else if runs_script(n) {
                    match self.prepare_script(e, n, &c.args, true).await {
                        Err(f) => f,
                        Ok(p) => match self.run_lua(e, txn.as_mut(), &p, &mut pubs).await {
                            Ran::Reply(f) => {
                                // A SELECT inside the script ends with it.
                                let back =
                                    [Bytes::from_static(b"select"), Bytes::from(cur.to_string())];
                                if let Err(sf) = txn.call(&back, false).await {
                                    failed = Some(sf);
                                    break;
                                }
                                cached.push((p, f.clone()));
                                f
                            }
                            // A killed script inside EXEC rolls the whole
                            // transaction back.
                            Ran::Abort(f) => {
                                failed = Some(ScriptFailed {
                                    reply: f,
                                    retry: false,
                                });
                                break;
                            }
                            Ran::Failed(sf) => {
                                failed = Some(sf);
                                break;
                            }
                        },
                    }
                } else {
                    match n.as_str() {
                        "ping" => c
                            .args
                            .get(1)
                            .map_or_else(|| Frame::simple("PONG"), |m| Frame::Bulk(m.clone())),
                        "echo" => Frame::Bulk(c.args[1].clone()),
                        _ => Frame::err(&format!(
                            "'{}' inside MULTI is not available on this database yet",
                            n.to_ascii_uppercase()
                        )),
                    }
                };
                out.push(r);
            }
            let failed = match failed {
                None => match txn.commit().await {
                    Ok(()) => {
                        self.publish_all(pubs).await;
                        for (p, f) in &cached {
                            Self::cache_script(e, p, f).await;
                        }
                        if let Some(d) = db {
                            self.db = d;
                            self.refresh_engine();
                        }
                        return Frame::Array(out);
                    }
                    Err(sf) => sf,
                },
                Some(sf) => {
                    drop(txn);
                    sf
                }
            };
            if failed.retry && attempt < retries {
                attempt += 1;
                backoff(attempt).await;
                continue;
            }
            return if failed.retry {
                reply::tryagain()
            } else {
                failed.reply
            };
        }
    }

    /// `SCRIPT ...` on an engine database.
    async fn engine_script_cmd(&mut self, e: &Arc<dyn EngineDatabase>, a: &[Bytes]) -> Frame {
        if let Some(f) = sub_arity_err("script", a) {
            return f;
        }
        match upper(&a[1]).as_str() {
            "LOAD" if a.len() == 3 => {
                let text = String::from_utf8_lossy(&a[2]).into_owned();
                let stripped = match lua::shebang(&text) {
                    Ok((_, b)) => b,
                    Err(m) => return Frame::error(m),
                };
                if let Err(f) = lua::check(&stripped) {
                    return f;
                }
                let sha = lua::sha1_hex(&a[2]);
                match e
                    .lua_swap(LuaStore::Scripts, sha.as_bytes(), None, Some(a[2].clone()))
                    .await
                {
                    Ok(_) => Frame::bulk(sha),
                    Err(f) => f,
                }
            }
            "EXISTS" if a.len() >= 3 => {
                let mut out = Vec::with_capacity(a.len() - 2);
                for s in &a[2..] {
                    let sha = String::from_utf8_lossy(s).to_ascii_lowercase();
                    match e.lua_get(LuaStore::Scripts, sha.as_bytes()).await {
                        Ok(r) => out.push(Frame::Integer(i64::from(r.is_some()))),
                        Err(f) => return f,
                    }
                }
                Frame::Array(out)
            }
            "FLUSH" if a.len() <= 3 => {
                if a.len() == 3 && !matches!(upper(&a[2]).as_str(), "SYNC" | "ASYNC") {
                    return Frame::err("SCRIPT FLUSH only support SYNC|ASYNC option");
                }
                match e.lua_clear(LuaStore::Scripts).await {
                    Ok(()) => Frame::ok(),
                    Err(f) => f,
                }
            }
            "KILL" if a.len() == 2 => {
                Self::engine_kill(Arc::clone(&self.env), self.scope(), false).await
            }
            "DEBUG" => reply::sprig(
                sprig_common::SprigCode::RedisLuaUnsupported,
                "SCRIPT DEBUG is not offered",
            ),
            "HELP" if a.len() == 2 => Frame::Array(
                [
                    "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "EXISTS <sha1> [<sha1> ...]",
                    "FLUSH [ASYNC|SYNC]",
                    "KILL",
                    "LOAD <script>",
                    "HELP",
                ]
                .into_iter()
                .map(Frame::simple)
                .collect(),
            ),
            _ => reply::unknown_sub("script", &a[1]),
        }
    }

    /// `SCRIPT KILL` / `FUNCTION KILL`: stops the tenant's script (or
    /// function) that runs past the busy threshold, whether or not it
    /// wrote: nothing of it is committed, so it is rolled back. A younger
    /// one is waited for, as Redis serves the kill only once it is busy.
    async fn engine_kill(env: Arc<crate::env::Env>, scope: String, function: bool) -> Frame {
        let threshold = env.config().lua_time_limit;
        loop {
            if env.scripts.kill(&scope, function, threshold) > 0 {
                return Frame::ok();
            }
            if !env.scripts.busy(&scope, function) {
                return Frame::error("NOTBUSY No scripts in execution right now.");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Loads `code` as a library: its name and functions.
    async fn load_lib(cfg: crate::env::Config, code: &Bytes) -> Result<Library, Frame> {
        let code = String::from_utf8_lossy(code).into_owned();
        match tokio::task::spawn_blocking(move || {
            lua::load_library(&code, &cfg).map(|i| Library {
                name: i.name,
                code,
                functions: i.functions,
            })
        })
        .await
        {
            Ok(r) => r,
            Err(e) => Err(Frame::err(&e.to_string())),
        }
    }

    /// Replaces the tenant's libraries with what `change` makes of them,
    /// again when another connection changed them meanwhile.
    async fn change_libs<T>(
        e: &Arc<dyn EngineDatabase>,
        mut change: impl FnMut(&mut Vec<Library>) -> Result<T, Frame>,
    ) -> Result<T, Frame> {
        for _ in 0..32 {
            let (raw, mut libs) = Self::libraries(e).await?;
            let out = change(&mut libs)?;
            libs.sort_by(|a, b| a.name.cmp(&b.name));
            let new = (!libs.is_empty()).then(|| encode_libs(&libs));
            if e.lua_swap(LuaStore::Functions, b"", Some(raw), new).await? {
                return Ok(out);
            }
        }
        Err(reply::tryagain())
    }

    /// Adds `lib` to `libs`: Redis's checks for a library or function name
    /// already taken.
    fn add_lib(libs: &mut Vec<Library>, lib: Library, replace: bool) -> Result<(), Frame> {
        let existing = libs.iter().position(|l| l.name == lib.name);
        if existing.is_some() && !replace {
            return Err(Frame::err(&format!(
                "Library '{}' already exists",
                lib.name
            )));
        }
        for (f, _) in &lib.functions {
            if libs
                .iter()
                .any(|l| l.name != lib.name && l.functions.iter().any(|(g, _)| g == f))
            {
                return Err(Frame::err(&format!("Function {f} already exists")));
            }
        }
        match existing {
            Some(i) => libs[i] = lib,
            None => libs.push(lib),
        }
        Ok(())
    }

    /// `FUNCTION ...` on an engine database.
    #[allow(clippy::too_many_lines)]
    async fn engine_function_cmd(&mut self, e: &Arc<dyn EngineDatabase>, a: &[Bytes]) -> Frame {
        if let Some(f) = sub_arity_err("function", a) {
            return f;
        }
        match upper(&a[1]).as_str() {
            "LOAD" if a.len() == 3 || a.len() == 4 => {
                let (replace, code) = if a.len() == 4 {
                    if upper(&a[2]) != "REPLACE" {
                        return Frame::err(&format!(
                            "Unknown option given: {}",
                            String::from_utf8_lossy(&a[2])
                        ));
                    }
                    (true, &a[3])
                } else {
                    (false, &a[2])
                };
                let lib = match Self::load_lib(self.env.config(), code).await {
                    Ok(l) => l,
                    Err(f) => return f,
                };
                let name = lib.name.clone();
                match Self::change_libs(e, |libs| Self::add_lib(libs, lib.clone(), replace)).await {
                    Ok(()) => Frame::bulk(name),
                    Err(f) => f,
                }
            }
            "DELETE" if a.len() == 3 => {
                let name = String::from_utf8_lossy(&a[2]).into_owned();
                let r = Self::change_libs(e, |libs| {
                    let before = libs.len();
                    libs.retain(|l| l.name != name);
                    if libs.len() == before {
                        Err(Frame::err("Library not found"))
                    } else {
                        Ok(())
                    }
                })
                .await;
                match r {
                    Ok(()) => Frame::ok(),
                    Err(f) => f,
                }
            }
            "FLUSH" if a.len() <= 3 => {
                if a.len() == 3 && !matches!(upper(&a[2]).as_str(), "SYNC" | "ASYNC") {
                    return Frame::err("FUNCTION FLUSH only supports SYNC|ASYNC option");
                }
                match e.lua_swap(LuaStore::Functions, b"", None, None).await {
                    Ok(_) => Frame::ok(),
                    Err(f) => f,
                }
            }
            "LIST" => {
                let mut pattern: Option<Bytes> = None;
                let mut withcode = false;
                let mut i = 2;
                while i < a.len() {
                    match upper(&a[i]).as_str() {
                        "WITHCODE" if !withcode => withcode = true,
                        "WITHCODE" => {
                            return Frame::err("Unknown argument withcode, WITHCODE already given");
                        }
                        "LIBRARYNAME" if pattern.is_some() => {
                            return Frame::err(
                                "Unknown argument libraryname, LIBRARYNAME already given",
                            );
                        }
                        "LIBRARYNAME" if i + 1 < a.len() => {
                            pattern = Some(a[i + 1].clone());
                            i += 1;
                        }
                        "LIBRARYNAME" => {
                            return Frame::err("library name argument was not given");
                        }
                        _ => {
                            return Frame::err(&format!(
                                "Unknown argument {}",
                                String::from_utf8_lossy(&a[i])
                            ));
                        }
                    }
                    i += 1;
                }
                let libs = match Self::libraries(e).await {
                    Ok((_, l)) => l,
                    Err(f) => return f,
                };
                let out = libs
                    .iter()
                    .filter(|l| {
                        pattern
                            .as_ref()
                            .is_none_or(|p| crate::glob::matches(p, l.name.as_bytes()))
                    })
                    .map(|l| {
                        let funcs = l
                            .functions
                            .iter()
                            .map(|(f, flags)| {
                                Frame::Map(vec![
                                    (Frame::bulk("name"), Frame::bulk(f.clone())),
                                    (Frame::bulk("description"), Frame::null()),
                                    (
                                        Frame::bulk("flags"),
                                        Frame::Set(
                                            flags.iter().map(|s| Frame::simple(s)).collect(),
                                        ),
                                    ),
                                ])
                            })
                            .collect();
                        let mut m = vec![
                            (Frame::bulk("library_name"), Frame::bulk(l.name.clone())),
                            (Frame::bulk("engine"), Frame::bulk("LUA")),
                            (Frame::bulk("functions"), Frame::Array(funcs)),
                        ];
                        if withcode {
                            m.push((Frame::bulk("library_code"), Frame::bulk(l.code.clone())));
                        }
                        Frame::Map(m)
                    })
                    .collect();
                Frame::Array(out)
            }
            "STATS" if a.len() == 2 => {
                let libs = match Self::libraries(e).await {
                    Ok((_, l)) => l,
                    Err(f) => return f,
                };
                let functions: usize = libs.iter().map(|l| l.functions.len()).sum();
                Frame::Map(vec![
                    (Frame::bulk("running_script"), Frame::null()),
                    (
                        Frame::bulk("engines"),
                        Frame::Map(vec![(
                            Frame::bulk("LUA"),
                            Frame::Map(vec![
                                (
                                    Frame::bulk("libraries_count"),
                                    Frame::Integer(i64::try_from(libs.len()).unwrap_or(0)),
                                ),
                                (
                                    Frame::bulk("functions_count"),
                                    Frame::Integer(i64::try_from(functions).unwrap_or(0)),
                                ),
                            ]),
                        )]),
                    ),
                ])
            }
            "KILL" if a.len() == 2 => {
                Self::engine_kill(Arc::clone(&self.env), self.scope(), true).await
            }
            "DUMP" if a.len() == 2 => match Self::libraries(e).await {
                Ok((_, libs)) => Frame::bulk(dump_payload(&libs)),
                Err(f) => f,
            },
            "RESTORE" if a.len() == 3 || a.len() == 4 => {
                let policy = if a.len() == 4 {
                    upper(&a[3])
                } else {
                    "APPEND".to_owned()
                };
                if !matches!(policy.as_str(), "APPEND" | "REPLACE" | "FLUSH") {
                    return Frame::err(
                        "Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.",
                    );
                }
                let Some(codes) = restore_payload(&a[2]) else {
                    return Frame::err("DUMP payload version or checksum are wrong");
                };
                let mut loaded = Vec::with_capacity(codes.len());
                for c in codes {
                    match Self::load_lib(self.env.config(), &Bytes::from(c)).await {
                        Ok(l) => loaded.push(l),
                        Err(f) => return f,
                    }
                }
                let r = Self::change_libs(e, |libs| {
                    let mut next = if policy == "FLUSH" {
                        Vec::new()
                    } else {
                        libs.clone()
                    };
                    for l in &loaded {
                        if policy == "APPEND" && next.iter().any(|x| x.name == l.name) {
                            return Err(Frame::err(&format!("Library {} already exists", l.name)));
                        }
                        Self::add_lib(&mut next, l.clone(), true)?;
                    }
                    *libs = next;
                    Ok(())
                })
                .await;
                match r {
                    Ok(()) => Frame::ok(),
                    Err(f) => f,
                }
            }
            "HELP" if a.len() == 2 => Frame::Array(
                [
                    "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "LOAD [REPLACE] <FUNCTION CODE>",
                    "DELETE <LIBRARY NAME>",
                    "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
                    "FLUSH [ASYNC|SYNC]",
                    "STATS",
                    "KILL",
                    "DUMP",
                    "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
                    "HELP",
                ]
                .into_iter()
                .map(Frame::simple)
                .collect(),
            ),
            _ => reply::unknown_sub("function", &a[1]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_matches_the_jones_check_value() {
        assert_eq!(crc64(b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn payloads_round_trip() {
        let libs = vec![Library {
            name: "a".into(),
            code: "#!lua name=a\nredis.register_function('f', function() return 1 end)".repeat(3),
            functions: Vec::new(),
        }];
        let p = dump_payload(&libs);
        assert_eq!(restore_payload(&p), Some(vec![libs[0].code.clone()]));
        let mut bad = p.clone();
        bad[3] ^= 1;
        assert_eq!(restore_payload(&bad), None);
    }

    #[test]
    fn lzf_back_references() {
        // "abcabcabc": three literals, then a back reference of 6.
        let c = [2, b'a', b'b', b'c', 0x80, 2];
        assert_eq!(lzf_decompress(&c, 9).as_deref(), Some(&b"abcabcabc"[..]));
    }

    #[test]
    fn libraries_round_trip() {
        let libs = vec![
            Library {
                name: "x".into(),
                code: "code".into(),
                functions: vec![
                    ("f".into(), vec!["no-writes".into()]),
                    ("g".into(), Vec::new()),
                ],
            },
            Library {
                name: "y".into(),
                code: String::new(),
                functions: Vec::new(),
            },
        ];
        assert_eq!(decode_libs(&encode_libs(&libs)), libs);
    }
}
