//! Redis databases stored on SprigDB's own storage engine instead of the
//! key-value tables.
//!
//! The gateway installs an [`EngineBackend`] on the [`crate::Env`]. After a
//! login and on every `SELECT`, the session asks it whether the tenant's
//! selected database lives on the engine: it does when the tenant's
//! logical database holding its Redis databases (`redis.logical_database`)
//! is placed on the engine ([`sprig_kv::SqlRunner::engine_placed`]), or
//! when the backend puts the whole tenant there. If it does, the session sends its
//! data commands to that [`EngineDatabase`] and answers only connection,
//! server and pub/sub commands itself; any other command is refused, since
//! the engine runs a subset of Redis for now. Databases the backend doesn't
//! claim keep the key-value tables, unchanged.
//!
//! A pipeline's commands reach [`EngineDatabase::run`] together: the engine
//! runs them in order as one transaction with one commit, and their replies
//! go out once that commit is durable.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_proto_redis::Frame;

/// Says which tenants' Redis databases live on the engine.
pub trait EngineBackend: Send + Sync + std::fmt::Debug {
    /// Database `db` of tenant `scope`, when it lives on the engine;
    /// `placed`: the tenant's Redis databases are placed on the engine.
    fn database(&self, scope: &str, db: u32, placed: bool) -> Option<Arc<dyn EngineDatabase>>;

    /// Tenant `scope`'s keyspace notification flags changed on this
    /// gateway (`CONFIG SET notify-keyspace-events`): the engine's writes
    /// record events from now on, or stop.
    fn set_keyspace_flags(&self, scope: &str, flags: u16) {
        let _ = (scope, flags);
    }
}

/// A key a transaction watches on the engine (`WATCH`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watched {
    /// The key's database.
    pub db: u32,
    /// The key.
    pub key: Bytes,
    /// What `EXEC` compares: it changes whenever the key is written.
    pub token: u64,
    /// When the key expires, as it was at `WATCH`: `EXEC` fails once it
    /// has passed.
    pub exp: Option<i64>,
}

/// Why a script's transaction failed: the script's reply, and whether
/// running it again may succeed (a deadlock, a lock wait that ran out).
/// Nothing of the transaction is kept.
#[derive(Debug, Clone)]
pub struct ScriptFailed {
    /// The reply when it isn't run again.
    pub reply: Frame,
    /// Running the script again may succeed.
    pub retry: bool,
}

/// A Lua script's transaction on the engine (`EVAL`, `FCALL`, or an `EXEC`
/// holding scripts): every command the script calls runs in it, each key
/// locked when a command first names it, and it commits once, when the
/// script ends. Dropped without [`EngineScript::commit`], it changes
/// nothing.
#[async_trait]
pub trait EngineScript: Send {
    /// Runs one command (one [`EngineDatabase::runs`], or `SELECT`, which
    /// changes the database of the commands after it).
    async fn call(&mut self, cmd: &[Bytes], resp3: bool) -> Result<Frame, ScriptFailed>;

    /// Commits; returns once the writes are durable.
    async fn commit(self: Box<Self>) -> Result<(), ScriptFailed>;
}

/// Where the tenant's Lua code is kept on the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LuaStore {
    /// The script cache, by SHA1 (`EVAL`, `SCRIPT LOAD`).
    Scripts,
    /// The function libraries: one entry, named `""`, holding them all.
    Functions,
}

/// One Redis database on the engine.
#[async_trait]
pub trait EngineDatabase: Send + Sync + std::fmt::Debug {
    /// Whether the engine runs command `name` (lower case).
    fn runs(&self, name: &str) -> bool;

    /// Runs `cmds` (each with its name first, every one passing
    /// [`EngineDatabase::runs`] and its arity check) in order as one
    /// batch: one transaction, one commit, a reply per command. The replies
    /// come back once everything they show is durable. A command refused
    /// with an error reply changes nothing, and the others still apply, as
    /// in a Redis pipeline.
    async fn run(&self, cmds: &[&[Bytes]], resp3: bool) -> Vec<Frame>;

    /// `WATCH` of `keys` in this database: what `EXEC` checks.
    async fn watch(&self, keys: &[Bytes]) -> Vec<Watched> {
        let _ = keys;
        Vec::new()
    }

    /// `EXEC`: `cmds` (each passing [`EngineDatabase::runs`]) as one
    /// transaction, after checking that no key of `watched` changed since
    /// its `WATCH`; `None` when one did (nothing ran). Blocking commands
    /// don't wait inside it.
    async fn exec(
        &self,
        cmds: &[&[Bytes]],
        watched: &[Watched],
        resp3: bool,
    ) -> Option<Vec<Frame>> {
        let _ = watched;
        Some(self.run(cmds, resp3).await)
    }

    /// Opens a script's transaction, after checking that no key of
    /// `watched` changed since its `WATCH` (`Ok(None)`: one did).
    ///
    /// # Errors
    /// The reply when no transaction can be opened.
    async fn script(&self, watched: &[Watched]) -> Result<Option<Box<dyn EngineScript>>, Frame> {
        let _ = watched;
        Err(not_on_engine("eval"))
    }

    /// The tenant's stored Lua entry `name` of `store`.
    ///
    /// # Errors
    /// The engine's, as a reply.
    async fn lua_get(&self, store: LuaStore, name: &[u8]) -> Result<Option<Bytes>, Frame> {
        let _ = (store, name);
        Err(not_on_engine("script"))
    }

    /// Replaces entry `name` of `store` with `new` (`None` deletes it) when
    /// it still holds `expect` (`None`: whatever it holds); whether it did.
    ///
    /// # Errors
    /// The engine's, as a reply.
    async fn lua_swap(
        &self,
        store: LuaStore,
        name: &[u8],
        expect: Option<Option<Bytes>>,
        new: Option<Bytes>,
    ) -> Result<bool, Frame> {
        let _ = (store, name, expect, new);
        Err(not_on_engine("script"))
    }

    /// Deletes every entry of `store`.
    ///
    /// # Errors
    /// The engine's, as a reply.
    async fn lua_clear(&self, store: LuaStore) -> Result<(), Frame> {
        let _ = store;
        Err(not_on_engine("script"))
    }
}

/// Commands a session on an engine database still answers itself: they
/// read or change only the connection, the server's facts or pub/sub.
#[must_use]
pub fn session_command(name: &str) -> bool {
    matches!(
        name,
        "ping"
            | "echo"
            | "quit"
            | "reset"
            | "select"
            | "hello"
            | "auth"
            | "client"
            | "info"
            | "config"
            | "command"
            | "cluster"
            | "acl"
            | "slowlog"
            | "latency"
            | "memory"
            | "lolwut"
            | "role"
            | "lastsave"
            | "readonly"
            | "readwrite"
            | "asking"
            | "multi"
            | "exec"
            | "discard"
            | "watch"
            | "unwatch"
            | "subscribe"
            | "psubscribe"
            | "ssubscribe"
            | "unsubscribe"
            | "punsubscribe"
            | "sunsubscribe"
            | "publish"
            | "spublish"
            | "pubsub"
            | "sprig.query"
    )
}

/// Commands only databases on the engine run, missing from the key-value
/// tables' command table (Redis 7.4's hash-field lifetimes).
pub const ENGINE_ONLY: &[crate::spec::Spec] = &[
    engine_only("hexpire", -6, "w"),
    engine_only("hpexpire", -6, "w"),
    engine_only("hexpireat", -6, "w"),
    engine_only("hpexpireat", -6, "w"),
    engine_only("httl", -5, "r"),
    engine_only("hpttl", -5, "r"),
    engine_only("hexpiretime", -5, "r"),
    engine_only("hpexpiretime", -5, "r"),
    engine_only("hpersist", -5, "w"),
];

const fn engine_only(name: &'static str, arity: i32, flags: &'static str) -> crate::spec::Spec {
    crate::spec::Spec {
        name,
        arity,
        flags,
        first: 1,
        last: 1,
        step: 1,
    }
}

/// An engine-only command's entry.
#[must_use]
pub fn engine_spec(name: &str) -> Option<&'static crate::spec::Spec> {
    ENGINE_ONLY.iter().find(|s| s.name == name)
}

/// A command's entry: the command table's, else an engine-only one.
#[must_use]
pub fn lookup(name: &str) -> Option<&'static crate::spec::Spec> {
    crate::spec::lookup(name).or_else(|| engine_spec(name))
}

/// The reply to a command the engine doesn't run yet.
#[must_use]
pub fn not_on_engine(name: &str) -> Frame {
    Frame::err(&format!(
        "'{}' is not available on this database yet: it is stored on the SprigDB engine, which doesn't run this command yet",
        name.to_ascii_uppercase()
    ))
}
