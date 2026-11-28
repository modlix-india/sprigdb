//! Differential tests of Lua scripts and functions on engine-backed Redis
//! databases against a reference Redis, through a real client (`redis`,
//! BSD-3-Clause).
//!
//! - `SPRIG_TEST_REDIS_REF`: a reference Redis, such as `127.0.0.1:61479`.
//! - `SPRIG_TEST_REDIS_ENGINE`: a SprigDB node's Redis listener whose
//!   tenant is on the engine, as a URL with a login. When unset, the tests
//!   start the Redis front end in process with every database on the
//!   engine (RocksDB, in the build's temporary folder).
//!
//! Both servers are flushed, and their scripts and functions removed,
//! before each test; never point these at a server holding data.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::Value;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Frame, Limits};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

const CORPUS: &str = include_str!("lua_corpus.txt");

/// No SQL: an engine database's session never sends any.
struct NoSql;

#[async_trait]
impl SqlRunner for NoSql {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        panic!("an engine database sent SQL: {sql}");
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        assert!(sql == "ROLLBACK", "an engine database sent SQL: {sql}");
        Ok(0)
    }
}

struct TestAuth;

#[async_trait]
impl Auth for TestAuth {
    async fn login(&self, _user: Option<&[u8]>, pass: &[u8]) -> Result<Login, Frame> {
        if pass != b"pw" {
            return Err(sprig_redis::session::wrongpass());
        }
        Ok(Login {
            sql: Box::new(NoSql),
            scope: "4243".into(),
            user: "app".into(),
        })
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn rand_tag() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// The engine-backed SprigDB: a node, or an in-process server.
async fn sprig_url() -> String {
    if let Some(u) = var("SPRIG_TEST_REDIS_ENGINE") {
        return u;
    }
    let env = Arc::new(Env::new(Config::default(), "lua-test"));
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "lua-diff-{}-{}",
        std::process::id(),
        rand_tag()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = EngineConfig::new(&dir);
    cfg.store = StoreKind::Rocks;
    cfg.budget.engine_mb = 64;
    let engine = Engine::open(cfg).unwrap();
    let backend = Arc::new(Backend::new(
        Arc::new(EngineUnit::on_engine(engine)),
        Tenants::All,
        Config::default().databases,
    ));
    env.set_engine(Arc::clone(&backend) as Arc<dyn sprig_redis::engine::EngineBackend>);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let auth: Arc<dyn Auth> = Arc::new(TestAuth);
    tokio::spawn(async move {
        let _sweeper = backend.spawn_sweeper(Duration::from_millis(100));
        loop {
            let Ok((sock, peer)) = listener.accept().await else {
                return;
            };
            let s = Session::new(Arc::clone(&env), Arc::clone(&auth), &peer.to_string());
            tokio::spawn(sprig_redis::conn::serve(
                sock,
                s,
                Limits::default(),
                std::future::pending(),
            ));
        }
    });
    format!("redis://app:pw@{addr}")
}

async fn conn(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

fn show(r: &redis::RedisResult<Value>) -> String {
    match r {
        Ok(v) => show_v(v),
        Err(e) => format!(
            "ERR[{}] {}",
            e.code().unwrap_or("?"),
            e.detail().unwrap_or("")
        ),
    }
}

fn show_v(v: &Value) -> String {
    match v {
        Value::Nil => "nil".into(),
        Value::Int(i) => format!(":{i}"),
        Value::BulkString(b) => format!("${}", String::from_utf8_lossy(b)),
        Value::SimpleString(s) => format!("+{s}"),
        Value::Okay => "+OK".into(),
        Value::Array(a) => format!("[{}]", a.iter().map(show_v).collect::<Vec<_>>().join(", ")),
        other => format!("{other:?}"),
    }
}

async fn run<S: AsRef<str>>(
    c: &mut redis::aio::MultiplexedConnection,
    cmd: &[S],
) -> redis::RedisResult<Value> {
    let mut q = redis::cmd(cmd[0].as_ref());
    for a in &cmd[1..] {
        q.arg(a.as_ref());
    }
    q.query_async::<Value>(c).await
}

/// `FUNCTION LIST`'s reply with each library's functions sorted: Redis
/// lists them in its hash table's order.
fn sort_functions(v: Value) -> Value {
    let Value::Array(libs) = v else { return v };
    Value::Array(
        libs.into_iter()
            .map(|l| match l {
                Value::Array(mut kv) => {
                    for i in (0..kv.len().saturating_sub(1)).step_by(2) {
                        if matches!(&kv[i], Value::BulkString(k) if k == b"functions")
                            && let Value::Array(fs) = &mut kv[i + 1]
                        {
                            fs.sort_by_key(show_v);
                        }
                    }
                    Value::Array(kv)
                }
                other => other,
            })
            .collect(),
    )
}

/// A reply as compared.
fn normalize(cmd: &[String], r: redis::RedisResult<Value>) -> String {
    let list = cmd.len() > 1
        && cmd[0].eq_ignore_ascii_case("function")
        && cmd[1].eq_ignore_ascii_case("list");
    show(&r.map(|v| if list { sort_functions(v) } else { v }))
}

/// The corpus's commands.
fn lines(body: &str) -> Vec<Vec<String>> {
    body.lines()
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|l| {
            l.split(" | ")
                .map(|s| {
                    if s == "''" {
                        String::new()
                    } else {
                        s.replace("\\n", "\n")
                    }
                })
                .collect()
        })
        .collect()
}

/// Empties a server: data, scripts and functions.
async fn reset(c: &mut redis::aio::MultiplexedConnection) {
    for cmd in [
        &["FLUSHALL"][..],
        &["SCRIPT", "FLUSH"],
        &["FUNCTION", "FLUSH"],
    ] {
        run(c, cmd).await.unwrap();
    }
}

async fn pair() -> Option<(
    redis::aio::MultiplexedConnection,
    redis::aio::MultiplexedConnection,
    String,
    String,
)> {
    let Some(refaddr) = var("SPRIG_TEST_REDIS_REF") else {
        eprintln!("SPRIG_TEST_REDIS_REF not set: skipped");
        return None;
    };
    let rurl = format!("redis://{refaddr}");
    let surl = sprig_url().await;
    let mut r = conn(&rurl).await;
    let mut s = conn(&surl).await;
    reset(&mut r).await;
    reset(&mut s).await;
    Some((s, r, surl, rurl))
}

#[ignore = "live: needs SPRIG_TEST_REDIS_ENGINE, SPRIG_TEST_REDIS_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_corpus_matches_redis() {
    let Some((mut s, mut r, _, _)) = pair().await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let cmds = lines(CORPUS);
    let mut bad = Vec::new();
    for (i, c) in cmds.iter().enumerate() {
        let a = normalize(c, run(&mut s, c).await);
        let b = normalize(c, run(&mut r, c).await);
        if a != b {
            bad.push(format!(
                "#{i} {}\n   sprig: {a}\n   redis: {b}",
                c.join(" | ").replace('\n', "\\n")
            ));
        }
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!(
        "lua corpus: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    reset(&mut r).await;
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// A script that runs past the busy threshold: the others get `BUSY`,
/// `SCRIPT KILL` stops it (it wrote nothing), and the server is free again.
#[ignore = "live: needs SPRIG_TEST_REDIS_ENGINE, SPRIG_TEST_REDIS_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_scripts_and_kill_match_redis() {
    let Some((_s, _r, surl, rurl)) = pair().await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut logs = Vec::new();
    for url in [&rurl, &surl] {
        let mut a = conn(url).await;
        let mut b = conn(url).await;
        let mut log = Vec::new();
        run(&mut b, &["SET", "busy", "before"]).await.unwrap();
        log.push(show(&run(&mut b, &["SCRIPT", "KILL"]).await));
        let spin = tokio::spawn(async move {
            show(
                &run(
                    &mut a,
                    &[
                        "EVAL",
                        "local x = redis.call('GET', KEYS[1]); while true do end",
                        "1",
                        "busy",
                    ],
                )
                .await,
            )
        });
        tokio::time::sleep(Duration::from_millis(5600)).await;
        log.push(show(&run(&mut b, &["GET", "busy"]).await));
        log.push(show(&run(&mut b, &["PING"]).await));
        log.push(show(&run(&mut b, &["FUNCTION", "KILL"]).await));
        log.push(show(&run(&mut b, &["SCRIPT", "KILL"]).await));
        log.push(spin.await.unwrap());
        log.push(show(&run(&mut b, &["GET", "busy"]).await));
        log.push(show(&run(&mut b, &["SCRIPT", "KILL"]).await));
        eprintln!("{url}: {log:?}");
        logs.push(log);
    }
    let mut r = conn(&rurl).await;
    reset(&mut r).await;
    assert_eq!(logs[0], logs[1]);
}

/// Scripts doing read-modify-write from many connections lose nothing, and
/// scripts taking undeclared keys in opposite orders all finish (a
/// deadlock runs one of them again).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_scripts_are_atomic() {
    let url = sprig_url().await;
    let mut c = conn(&url).await;
    reset(&mut c).await;
    let mut tasks = Vec::new();
    for t in 0..8 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = conn(&url).await;
            for i in 0..40 {
                // Read, then write later in Lua: atomic only if nobody
                // writes in between.
                run(
                    &mut c,
                    &[
                        "EVAL",
                        "local v = tonumber(redis.call('GET', KEYS[1]) or '0'); \
                         redis.call('SET', KEYS[1], v + 1); return v + 1",
                        "1",
                        "counter",
                    ],
                )
                .await
                .unwrap();
                // Undeclared keys in an order that depends on the task.
                let (x, y) = if (t + i) % 2 == 0 {
                    ("ua", "ub")
                } else {
                    ("ub", "ua")
                };
                run(
                    &mut c,
                    &[
                        "EVAL",
                        "redis.call('INCR', ARGV[1]); redis.call('INCR', ARGV[2]); return 1",
                        "0",
                        x,
                        y,
                    ],
                )
                .await
                .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    for k in ["counter", "ua", "ub"] {
        assert_eq!(
            show(&run(&mut c, &["GET", k]).await),
            "$320",
            "key {k} after 320 increments"
        );
    }
    reset(&mut c).await;
}

/// A library dumped by Redis restores on SprigDB and back, and `RESTORE`'s
/// policies match.
#[ignore = "live: needs SPRIG_TEST_REDIS_ENGINE, SPRIG_TEST_REDIS_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn function_dump_and_restore_cross_servers() {
    let Some((mut s, mut r, _, _)) = pair().await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let lib = "#!lua name=dumped\nredis.register_function('d1', function(keys, args) return 'd1:' .. args[1] end)\nredis.register_function{function_name='d2', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    let lib2 = "#!lua name=second\nredis.register_function('s1', function() return 's1' end)";
    run(&mut r, &["FUNCTION", "LOAD", lib]).await.unwrap();
    run(&mut r, &["FUNCTION", "LOAD", lib2]).await.unwrap();
    let Value::BulkString(from_redis) = run(&mut r, &["FUNCTION", "DUMP"]).await.unwrap() else {
        panic!("DUMP isn't a string");
    };
    let mut bad = Vec::new();
    let mut check = |what: &str, a: String, b: String| {
        if a != b {
            bad.push(format!("{what}\n   sprig: {a}\n   redis: {b}"));
        }
    };
    let restore = |p: Vec<u8>, policy: Option<&'static str>| {
        let mut q = redis::cmd("FUNCTION");
        q.arg("RESTORE").arg(p);
        if let Some(x) = policy {
            q.arg(x);
        }
        q
    };
    // Redis's payload on SprigDB.
    let a = show(
        &restore(from_redis.clone(), None)
            .query_async::<Value>(&mut s)
            .await,
    );
    check("restore redis payload", a, "+OK".into());
    for c in [
        ["FCALL", "d1", "0", "x"].map(String::from).to_vec(),
        ["FCALL_RO", "d2", "1", "nokey"].map(String::from).to_vec(),
        ["FCALL", "s1", "0"].map(String::from).to_vec(),
        ["FUNCTION", "LIST", "LIBRARYNAME", "dumped"]
            .map(String::from)
            .to_vec(),
    ] {
        let a = normalize(&c, run(&mut s, &c).await);
        let b = normalize(&c, run(&mut r, &c).await);
        check(&c.join(" "), a, b);
    }
    // Policies, on both.
    for (policy, c) in [
        (None, "append over existing"),
        (Some("APPEND"), "append over existing"),
        (Some("REPLACE"), "replace"),
        (Some("FLUSH"), "flush"),
        (Some("BOGUS"), "bogus"),
    ] {
        let a = show(
            &restore(from_redis.clone(), policy)
                .query_async::<Value>(&mut s)
                .await,
        );
        let b = show(
            &restore(from_redis.clone(), policy)
                .query_async::<Value>(&mut r)
                .await,
        );
        check(c, a, b);
    }
    let mut corrupt = from_redis.clone();
    let n = corrupt.len();
    corrupt[n - 1] ^= 0x55;
    let a = show(
        &restore(corrupt.clone(), None)
            .query_async::<Value>(&mut s)
            .await,
    );
    let b = show(&restore(corrupt, None).query_async::<Value>(&mut r).await);
    check("corrupt payload", a, b);
    // SprigDB's payload on Redis.
    let Value::BulkString(from_sprig) = run(&mut s, &["FUNCTION", "DUMP"]).await.unwrap() else {
        panic!("DUMP isn't a string");
    };
    run(&mut r, &["FUNCTION", "FLUSH"]).await.unwrap();
    let b = show(&restore(from_sprig, None).query_async::<Value>(&mut r).await);
    check("restore sprig payload on redis", "+OK".into(), b);
    for c in [
        vec!["FCALL", "d1", "0", "y"],
        vec!["FCALL", "s1", "0"],
        vec!["FUNCTION", "STATS"],
    ] {
        let a = show(&run(&mut s, &c).await);
        let b = show(&run(&mut r, &c).await);
        check(&c.join(" "), a, b);
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    reset(&mut r).await;
    reset(&mut s).await;
    assert!(bad.is_empty(), "{} mismatches", bad.len());
}

/// Reads from `sub` until `what` has arrived (at most 5 s).
async fn read_until(sub: &mut tokio::net::TcpStream, seen: &mut Vec<u8>, what: &[u8]) {
    use tokio::io::AsyncReadExt as _;
    let fut = async {
        while !seen.windows(what.len()).any(|w| w == what) {
            let mut buf = [0u8; 1024];
            let n = sub.read(&mut buf).await.unwrap();
            assert!(n > 0, "subscriber closed");
            seen.extend_from_slice(&buf[..n]);
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("nothing arrived");
}

/// A client that goes away while its script runs leaves nothing behind:
/// the script is stopped, rolled back, and nobody gets `BUSY` later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_leaving_mid_script_frees_the_tenant() {
    let url = sprig_url().await;
    let mut b = conn(&url).await;
    reset(&mut b).await;
    run(&mut b, &["SET", "left", "before"]).await.unwrap();
    {
        let mut a = conn(&url).await;
        let spin = tokio::spawn(async move {
            let _ = run(
                &mut a,
                &[
                    "EVAL",
                    "redis.call('SET', KEYS[1], 'during'); while true do end",
                    "1",
                    "left",
                ],
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        spin.abort();
    }
    tokio::time::sleep(Duration::from_millis(5600)).await;
    assert_eq!(show(&run(&mut b, &["GET", "left"]).await), "$before");
    assert_eq!(show(&run(&mut b, &["PING"]).await), "+PONG");
    assert_eq!(
        show(&run(&mut b, &["SCRIPT", "KILL"]).await),
        "ERR[NOTBUSY] No scripts in execution right now."
    );
}

/// A script's `PUBLISH` reaches subscribers once the script commits; a
/// crafted, deeply nested `cmsgpack` string is refused, not a crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn script_publishes_and_msgpack_limits() {
    use tokio::io::AsyncWriteExt as _;
    let url = sprig_url().await;
    let addr = url.rsplit('@').next().unwrap().to_owned();
    let mut sub = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sub.write_all(
        b"*3\r\n$4\r\nAUTH\r\n$3\r\napp\r\n$2\r\npw\r\n*2\r\n$9\r\nSUBSCRIBE\r\n$6\r\nevents\r\n",
    )
    .await
    .unwrap();
    let mut seen = Vec::new();
    read_until(&mut sub, &mut seen, b"subscribe").await;
    let mut c = conn(&url).await;
    let r = run(
        &mut c,
        &[
            "EVAL",
            "redis.call('PUBLISH', 'events', ARGV[1]); return 'sent'",
            "0",
            "hello",
        ],
    )
    .await;
    assert_eq!(show(&r), "$sent");
    read_until(&mut sub, &mut seen, b"hello").await;
    let r = run(
        &mut c,
        &[
            "EVAL",
            "return cmsgpack.unpack(string.rep('\\145', 100000) .. '\\192')",
            "0",
        ],
    )
    .await;
    assert!(show(&r).contains("Bad data format"), "{}", show(&r));
    assert_eq!(show(&run(&mut c, &["PING"]).await), "+PONG");
}
