//! What a user may do on Redis databases stored on the engine: the session
//! asks the login's SQL session for rights on the tenant's logical
//! database before a command runs, on every way a command gets in (one
//! command, a pipeline's batch, `MULTI`/`EXEC`, a script's calls).
//!
//! The Redis front end runs in process on an in-memory engine; each user
//! logs in with a SQL session that holds a fixed set of rights.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::sync::Arc;

use async_trait::async_trait;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_kv::sql::DbRight;
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Command, Frame};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

/// A SQL session holding `rights` on the logical database `redis` and
/// nothing anywhere else.
struct Holds(&'static [DbRight]);

#[async_trait]
impl SqlRunner for Holds {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        panic!("an engine database sent SQL: {sql}");
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        assert!(sql == "ROLLBACK", "an engine database sent SQL: {sql}");
        Ok(0)
    }

    fn holds_on_database(&self, rights: &[DbRight], database: &str) -> Option<bool> {
        Some(database == "redis" && rights.iter().all(|r| self.0.contains(r)))
    }
}

const READ: &[DbRight] = &[DbRight::Select];
const WRITE: &[DbRight] = &[
    DbRight::Select,
    DbRight::Insert,
    DbRight::Update,
    DbRight::Delete,
];
const SCRIPT_READER: &[DbRight] = &[DbRight::Select, DbRight::Execute];
const ALL: &[DbRight] = &[
    DbRight::Select,
    DbRight::Insert,
    DbRight::Update,
    DbRight::Delete,
    DbRight::Drop,
    DbRight::Execute,
    DbRight::CreateRoutine,
    DbRight::AlterRoutine,
];

/// Users of one tenant, by name: what each was granted on `redis.*`.
struct Users;

#[async_trait]
impl Auth for Users {
    async fn login(&self, user: Option<&[u8]>, _pass: &[u8]) -> Result<Login, Frame> {
        let user = String::from_utf8_lossy(user.unwrap_or(b"")).into_owned();
        let rights = match user.as_str() {
            "owner" => ALL,
            "writer" => WRITE,
            "reader" => READ,
            "scripter" => SCRIPT_READER,
            // Granted SELECT on some table of another database only.
            "elsewhere" => &[],
            _ => return Err(sprig_redis::session::wrongpass()),
        };
        Ok(Login {
            sql: Box::new(Holds(rights)),
            scope: "4311".into(),
            user,
        })
    }
}

fn env() -> Arc<Env> {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let env = Arc::new(Env::new(Config::default(), "authz-test"));
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "redis-authz-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = EngineConfig::new(&dir);
    cfg.store = StoreKind::Mem;
    cfg.budget.engine_mb = 64;
    let engine = Engine::open(cfg).unwrap();
    let backend = Arc::new(Backend::new(
        Arc::new(EngineUnit::on_engine(engine)),
        Tenants::All,
        Config::default().databases,
    ));
    env.set_engine(backend as Arc<dyn sprig_redis::engine::EngineBackend>);
    env
}

fn cmd(parts: &[&str]) -> Command {
    Command::from_parts(parts.iter().map(|p| p.as_bytes()))
}

async fn session(env: &Arc<Env>, user: &str) -> Session {
    let mut s = Session::new(Arc::clone(env), Arc::new(Users), "test");
    let r = one(&mut s, &["AUTH", user, "pw"]).await;
    assert_eq!(r, "OK", "{user}");
    s
}

fn text(f: &Frame) -> String {
    match f {
        Frame::Error(e) => format!("-{}", String::from_utf8_lossy(e)),
        Frame::Simple(s) => String::from_utf8_lossy(s).into_owned(),
        Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        Frame::Integer(i) => i.to_string(),
        Frame::Array(a) => format!("[{}]", a.iter().map(text).collect::<Vec<_>>().join(", ")),
        other => format!("{other:?}"),
    }
}

async fn one(s: &mut Session, parts: &[&str]) -> String {
    let frames = s.handle(cmd(parts)).await;
    assert_eq!(frames.len(), 1, "{parts:?}: {frames:?}");
    text(&frames[0])
}

fn refused(reply: &str) -> bool {
    reply.starts_with("-NOPERM ")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_user_reaches_only_what_it_was_granted() {
    let env = env();
    let mut owner = session(&env, "owner").await;
    assert_eq!(one(&mut owner, &["SET", "k", "v"]).await, "OK");
    assert_eq!(one(&mut owner, &["RPUSH", "l", "a", "b"]).await, "2");

    // A user with no grant on the Redis databases (a grant on a table
    // elsewhere in the tenant): nothing, by any command.
    let mut u = session(&env, "elsewhere").await;
    for c in [
        &["GET", "k"][..],
        &["SET", "k", "stolen"],
        &["DEL", "k"],
        &["KEYS", "*"],
        &["SCAN", "0"],
        &["LRANGE", "l", "0", "-1"],
        &["FLUSHDB"],
        &["FLUSHALL"],
        &["EVAL", "return redis.call('GET', 'k')", "0"],
        &["SCRIPT", "LOAD", "return 1"],
        &["FUNCTION", "LIST"],
        &["WATCH", "k"],
    ] {
        let r = one(&mut u, c).await;
        assert!(refused(&r), "{c:?}: {r}");
        assert!(r.contains("User elsewhere"), "{r}");
    }
    // In another numbered database too.
    assert_eq!(one(&mut u, &["SELECT", "3"]).await, "OK");
    assert!(refused(&one(&mut u, &["SET", "k", "v"]).await));
    assert!(refused(&one(&mut u, &["GET", "k"]).await));
    // Connection commands still answer.
    assert_eq!(one(&mut u, &["PING"]).await, "PONG");

    // A reader reads and writes nothing.
    let mut r = session(&env, "reader").await;
    assert_eq!(one(&mut r, &["GET", "k"]).await, "v");
    assert_eq!(one(&mut r, &["LRANGE", "l", "0", "-1"]).await, "[a, b]");
    for c in [
        &["SET", "k", "x"][..],
        &["DEL", "k"],
        &["LPOP", "l"],
        &["EXPIRE", "k", "1"],
        &["GETDEL", "k"],
        &["RENAME", "k", "k2"],
        &["FLUSHDB"],
        &["EVAL", "return 1", "0"],
    ] {
        let reply = one(&mut r, c).await;
        assert!(refused(&reply), "{c:?}: {reply}");
    }

    // A writer without EXECUTE runs no scripts; without DROP no FLUSHALL.
    let mut w = session(&env, "writer").await;
    assert_eq!(one(&mut w, &["SET", "w", "1"]).await, "OK");
    assert!(refused(&one(&mut w, &["EVAL", "return 1", "0"]).await));
    assert!(refused(&one(&mut w, &["FLUSHALL"]).await));
    assert!(refused(
        &one(&mut w, &["FUNCTION", "LOAD", "#!lua name=x\nreturn 1"]).await
    ));

    // Nothing above changed the owner's data.
    assert_eq!(one(&mut owner, &["GET", "k"]).await, "v");
    assert_eq!(one(&mut owner, &["LLEN", "l"]).await, "2");
    assert_eq!(one(&mut owner, &["GET", "w"]).await, "1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelines_transactions_and_scripts_are_checked_too() {
    let env = env();
    let mut owner = session(&env, "owner").await;
    assert_eq!(one(&mut owner, &["SET", "k", "v"]).await, "OK");

    // A pipeline's batch: each command the user may not run answers
    // NOPERM, the others run, the replies keep their order.
    let mut r = session(&env, "reader").await;
    let batch = [
        cmd(&["GET", "k"]),
        cmd(&["SET", "k", "x"]),
        cmd(&["GET", "k"]),
        cmd(&["DEL", "k"]),
    ];
    assert_eq!(r.engine_len(&batch), 4, "the engine batches all four");
    let replies: Vec<String> = r
        .handle_engine(&batch)
        .await
        .iter()
        .map(|f| text(&f[0]))
        .collect();
    assert_eq!(replies[0], "v");
    assert!(refused(&replies[1]), "{replies:?}");
    assert_eq!(replies[2], "v");
    assert!(refused(&replies[3]), "{replies:?}");
    let mut none = session(&env, "elsewhere").await;
    let replies = none.handle_engine(&batch).await;
    assert!(replies.iter().all(|f| refused(&text(&f[0]))), "{replies:?}");

    // MULTI: a command the user may not run is refused when queued, and
    // EXEC then runs nothing.
    assert_eq!(one(&mut r, &["MULTI"]).await, "OK");
    assert_eq!(one(&mut r, &["GET", "k"]).await, "QUEUED");
    assert!(refused(&one(&mut r, &["SET", "k", "x"]).await));
    let exec = one(&mut r, &["EXEC"]).await;
    assert!(exec.starts_with("-EXECABORT"), "{exec}");

    // A script runs with its caller's rights: each call is checked.
    let mut s = session(&env, "scripter").await;
    assert_eq!(
        one(
            &mut s,
            &["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]
        )
        .await,
        "v"
    );
    let w = one(
        &mut s,
        &["EVAL", "return redis.call('SET', KEYS[1], 'x')", "1", "k"],
    )
    .await;
    assert!(w.contains("ACL failure in script"), "{w}");
    let w = one(
        &mut s,
        &["EVAL", "return redis.pcall('DEL', KEYS[1])", "1", "k"],
    )
    .await;
    assert!(w.contains("ACL failure in script"), "{w}");

    assert_eq!(one(&mut owner, &["GET", "k"]).await, "v");
}
