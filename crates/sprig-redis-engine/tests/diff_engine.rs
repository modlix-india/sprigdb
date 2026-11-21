//! Differential tests of engine-backed Redis databases against a
//! reference Redis, through a real client (`redis`, BSD-3-Clause).
//!
//! - `SPRIG_TEST_REDIS_REF`: a reference Redis, such as `127.0.0.1:61479`.
//! - `SPRIG_TEST_REDIS_ENGINE`: a SprigDB node's Redis listener whose
//!   tenant is on the engine, as a URL with a login
//!   (`redis://acme.app:pw@127.0.0.1:61480`). When unset, the tests start
//!   the Redis front end in process with every database on the engine
//!   (RocksDB and its log with real syncs, in the build's temporary
//!   folder).
//!
//! The commands are those of the key-value differential corpus
//! (`sprig-redis/tests/diff_live.rs`) that the engine runs, plus random
//! commands and pipelines over a small keyspace. Both servers are flushed
//! before each test; never point these at a server holding data.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::match_same_arms
)]

use std::sync::Arc;

use async_trait::async_trait;
use redis::Value;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Frame, Limits};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

/// The key-value suite's file, whose corpus this test reuses.
const KV_SUITE: &str = include_str!("../../sprig-redis/tests/diff_live.rs");

/// Whether an engine database answers command `name`, itself or through
/// the session.
fn covered(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    sprig_redis_engine::exec::runs(&n)
        || matches!(
            n.as_str(),
            "ping" | "echo" | "select" | "multi" | "exec" | "discard" | "watch" | "unwatch"
        )
}

/// No SQL: an engine database's session never sends any.
struct NoSql;

#[async_trait]
impl SqlRunner for NoSql {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        panic!("an engine database sent SQL: {sql}");
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        // Closing a connection rolls back; nothing else may arrive.
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
            scope: "4242".into(),
            user: "app".into(),
        })
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

/// A tag that keeps each in-process server's directory its own.
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
    let env = Arc::new(Env::new(Config::default(), "e0-redis-test"));
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "e0-redis-diff-{}-{}",
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
        let _sweeper = backend.spawn_sweeper(std::time::Duration::from_millis(100));
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

/// Sorts an array reply (unordered collections).
fn sorted(v: Value) -> Value {
    match v {
        Value::Array(mut a) | Value::Set(mut a) => {
            a.sort_by_key(show_v);
            Value::Array(a)
        }
        Value::Map(mut m) => {
            m.sort_by_key(|(k, _)| show_v(k));
            Value::Map(m)
        }
        other => other,
    }
}

/// Sorts a flat `[k, v, k, v]` array by pairs.
fn sorted_pairs(v: Value) -> Value {
    match v {
        Value::Array(a) => {
            let mut pairs: Vec<(Value, Value)> = a
                .chunks(2)
                .map(|c| (c[0].clone(), c.get(1).cloned().unwrap_or(Value::Nil)))
                .collect();
            pairs.sort_by_key(|(k, _)| show_v(k));
            Value::Array(pairs.into_iter().flat_map(|(k, v)| [k, v]).collect())
        }
        other => sorted(other),
    }
}

/// A reply as compared: unordered collections sorted, a positive TTL
/// only as "positive", a lifetime in ms rounded to seconds, a future
/// expiry instant as "future".
fn normalize(cmd: &[String], r: redis::RedisResult<Value>) -> String {
    let name = cmd[0].to_ascii_lowercase();
    let withscores = cmd.iter().any(|a| a.eq_ignore_ascii_case("withscores"));
    let r = r.map(|v| match name.as_str() {
        "smembers" | "sunion" | "sinter" | "sdiff" | "keys" | "hkeys" | "hvals" | "zunion"
            if !withscores =>
        {
            sorted(v)
        }
        "hgetall" => sorted_pairs(v),
        "ttl" => match v {
            Value::Int(i) if i > 0 => Value::SimpleString("positive".into()),
            o => o,
        },
        // Per-field lifetimes: positive ones as "positive", future
        // instants as "future".
        "httl" | "hpttl" | "hexpiretime" | "hpexpiretime" => match v {
            Value::Array(a) => Value::Array(
                a.into_iter()
                    .map(|x| match x {
                        Value::Int(i) if i > 0 => Value::SimpleString("positive".into()),
                        o => o,
                    })
                    .collect(),
            ),
            o => o,
        },
        "pttl" => match v {
            Value::Int(i) if i > 0 => Value::Int((i + 500) / 1000),
            o => o,
        },
        "expiretime" | "pexpiretime" => match v {
            Value::Int(i) if i > 0 => Value::SimpleString("future".into()),
            o => o,
        },
        _ => v,
    });
    show(&r)
}

async fn run(
    c: &mut redis::aio::MultiplexedConnection,
    cmd: &[String],
) -> redis::RedisResult<Value> {
    let mut q = redis::cmd(&cmd[0]);
    for a in &cmd[1..] {
        q.arg(a.as_str());
    }
    q.query_async::<Value>(c).await
}

/// How long to wait past a short lifetime before the next command, so the
/// key has ended on both servers whatever the load on the machine.
const EXPIRY_MARGIN: std::time::Duration = std::time::Duration::from_millis(100);

/// The lifetime a command gives its key, when it can end during a test (a
/// `PX` of at most a second). The two servers run a command at different
/// moments, so a later command could find such a key gone on one and still
/// there on the other; the tests wait it out on both instead. Lifetimes in
/// seconds are long enough never to end during a test.
fn short_ttl(cmd: &[String]) -> Option<std::time::Duration> {
    let at = cmd.iter().position(|a| a == "PX")?;
    let ms: u64 = cmd.get(at + 1)?.parse().ok()?;
    (1..=1_000)
        .contains(&ms)
        .then(|| std::time::Duration::from_millis(ms))
}

async fn compare(
    sprig: &mut redis::aio::MultiplexedConnection,
    reference: &mut redis::aio::MultiplexedConnection,
    cmds: &[Vec<String>],
) -> Vec<String> {
    let mut bad = Vec::new();
    let mut recent: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    for (i, c) in cmds.iter().enumerate() {
        let a = normalize(c, run(sprig, c).await);
        let b = normalize(c, run(reference, c).await);
        if a != b && bad.is_empty() {
            for r in &recent {
                eprintln!("   before: {r}");
            }
        }
        recent.push_back(format!("#{i} {} -> {a}", c.join(" ")));
        if recent.len() > 25 {
            recent.pop_front();
        }
        if a != b {
            bad.push(format!(
                "#{i} {}\n   sprig: {a}\n   redis: {b}",
                c.join(" ")
            ));
        }
        if let Some(ttl) = short_ttl(c) {
            tokio::time::sleep(ttl + EXPIRY_MARGIN).await;
        }
    }
    bad
}

/// The key-value corpus's lines whose command the engine answers.
fn corpus() -> Vec<Vec<String>> {
    let start = KV_SUITE.find("const CORPUS: &str = r\"").unwrap();
    let body = &KV_SUITE[start..];
    let body = &body[body.find('\n').unwrap() + 1..];
    let body = &body[..body.find("\";").unwrap()];
    body.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            l.split(' ')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    if s == "''" {
                        String::new()
                    } else {
                        s.to_owned()
                    }
                })
                .collect::<Vec<_>>()
        })
        .filter(|c| covered(&c[0]))
        .collect()
}

/// Lines of a corpus: one command each, `''` for an empty argument.
fn lines(body: &str) -> Vec<Vec<String>> {
    body.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            l.split(' ')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    if s == "''" {
                        String::new()
                    } else {
                        s.to_owned()
                    }
                })
                .collect()
        })
        .collect()
}

async fn pair() -> Option<(
    redis::aio::MultiplexedConnection,
    redis::aio::MultiplexedConnection,
)> {
    let Some(refaddr) = var("SPRIG_TEST_REDIS_REF") else {
        eprintln!("SPRIG_TEST_REDIS_REF not set: skipped");
        return None;
    };
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&sprig_url().await).await;
    for c in [&mut r, &mut s] {
        redis::cmd("FLUSHALL").query_async::<()>(c).await.unwrap();
    }
    Some((s, r))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corpus_matches_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let cmds = corpus();
    assert!(cmds.len() >= 100, "corpus not found: {}", cmds.len());
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("corpus: {} commands, {} mismatches", cmds.len(), bad.len());
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// A small deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[(self.next() % xs.len() as u64) as usize]
    }
}

/// One random command of the engine's set over a few keys of each type.
/// Lifetimes are either far longer than a test run on a loaded machine
/// (1,000 seconds and up) or a short `PX` that the tests wait out
/// ([`short_ttl`]), so no key ends on one server and not on the other.
fn random_cmd(g: &mut Rng) -> Vec<String> {
    let keys = ["k0", "k1", "k2", "k3", "h0", "h1", "l0", "l1"];
    let vals = [
        "a",
        "1",
        "-3",
        "10",
        "x y",
        "",
        "9223372036854775807",
        "007",
    ];
    let k = g.pick(&keys).to_owned();
    match g.next() % 16 {
        0 => vec!["GET".into(), k],
        1 => vec!["SET".into(), k, g.pick(&vals).into()],
        2 => {
            let opt = g.pick(&["NX", "XX", "GET", "EX", "PX", "KEEPTTL", "XX GET", "NX EX"]);
            let mut c = vec!["SET".into(), k, g.pick(&vals).into()];
            for o in opt.split(' ') {
                c.push(o.into());
                if o == "EX" {
                    c.push(g.pick(&["1000", "100000", "0", "-5", "x"]).into());
                } else if o == "PX" {
                    c.push(g.pick(&["100", "100000000", "0", "-5", "x"]).into());
                }
            }
            c
        }
        3 | 4 => vec!["INCR".into(), k],
        5 => vec!["DEL".into(), k, g.pick(&keys).into()],
        6 => {
            let mut c = vec![
                "EXPIRE".into(),
                k,
                g.pick(&["1000", "2000", "-1", "0", "x"]).into(),
            ];
            if g.next().is_multiple_of(2) {
                c.push(g.pick(&["NX", "XX", "GT", "LT"]).into());
            }
            c
        }
        7 => vec!["TTL".into(), k],
        8 | 9 => vec![
            "HSET".into(),
            k,
            g.pick(&["f", "g", "h"]).into(),
            g.pick(&vals).into(),
            g.pick(&["g", "i"]).into(),
            g.pick(&vals).into(),
        ],
        10 => vec!["HGET".into(), k, g.pick(&["f", "g", "h", "i"]).into()],
        11 => vec!["MGET".into(), k, g.pick(&keys).into(), "nokey".into()],
        12 => vec![
            "MSET".into(),
            k,
            g.pick(&vals).into(),
            g.pick(&keys).into(),
            g.pick(&vals).into(),
        ],
        13 | 14 => vec![
            "LPUSH".into(),
            k,
            g.pick(&vals).into(),
            g.pick(&vals).into(),
        ],
        _ => vec!["SELECT".into(), g.pick(&["0", "1"]).into()],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_ops_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let mut g = Rng(20_261_118);
    let cmds: Vec<Vec<String>> = (0..3000).map(|_| random_cmd(&mut g)).collect();
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in bad.iter().take(20) {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("random: {} commands, {} mismatches", cmds.len(), bad.len());
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// Reads one whole reply's bytes.
async fn read_replies(s: &mut tokio::net::TcpStream, n: usize) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    loop {
        if count_replies(&buf) >= n {
            return buf;
        }
        let mut chunk = vec![0u8; 65536];
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), s.read(&mut chunk))
            .await
            .unwrap()
            .unwrap();
        assert!(got > 0, "connection closed");
        buf.extend_from_slice(&chunk[..got]);
    }
}

/// How many whole RESP2 replies `b` holds.
fn count_replies(b: &[u8]) -> usize {
    let mut at = 0;
    let mut n = 0;
    while let Some(next) = reply_end(b, at) {
        at = next;
        n += 1;
    }
    n
}

fn line_end(b: &[u8], at: usize) -> Option<usize> {
    b.get(at..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| at + p + 2)
}

fn reply_end(b: &[u8], at: usize) -> Option<usize> {
    let kind = *b.get(at)?;
    let le = line_end(b, at)?;
    let num = || -> Option<i64> { std::str::from_utf8(&b[at + 1..le - 2]).ok()?.parse().ok() };
    match kind {
        b'+' | b'-' | b':' => Some(le),
        b'$' => {
            let n = num()?;
            if n < 0 {
                return Some(le);
            }
            let end = le + n as usize + 2;
            (end <= b.len()).then_some(end)
        }
        b'*' => {
            let n = num()?;
            let mut p = le;
            for _ in 0..n.max(0) {
                p = reply_end(b, p)?;
            }
            Some(p)
        }
        _ => None,
    }
}

fn wire(args: &[String]) -> Vec<u8> {
    let mut v = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        v.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        v.extend_from_slice(a.as_bytes());
        v.extend_from_slice(b"\r\n");
    }
    v
}

async fn raw(url_or_addr: &str, auth: bool) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let addr = url_or_addr
        .rsplit('@')
        .next()
        .unwrap()
        .trim_start_matches("redis://");
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    if auth {
        let (user, pass) = url_or_addr
            .trim_start_matches("redis://")
            .split('@')
            .next()
            .unwrap()
            .split_once(':')
            .unwrap();
        s.write_all(&wire(&["AUTH".into(), user.into(), pass.into()]))
            .await
            .unwrap();
        let r = read_replies(&mut s, 1).await;
        assert_eq!(r, b"+OK\r\n");
    }
    s
}

/// Pipelines sent in one write each, replies compared byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelines_match_redis_bytes() {
    use tokio::io::AsyncWriteExt;
    let Some(refaddr) = var("SPRIG_TEST_REDIS_REF") else {
        eprintln!("SPRIG_TEST_REDIS_REF not set: skipped");
        return;
    };
    let url = sprig_url().await;
    let mut sprig = raw(&url, true).await;
    let mut reference = raw(&refaddr, false).await;
    let mut g = Rng(7);
    let mut bad = 0;
    for round in 0..60 {
        let mut cmds: Vec<Vec<String>> = Vec::new();
        if round == 0 {
            cmds.push(vec!["FLUSHALL".into()]);
        }
        let len = 1 + (g.next() % 40) as usize;
        // A short lifetime ends its pipeline, and is waited out before the
        // next one: SprigDB can take longer over a pipeline than Redis does.
        let mut short = None;
        for _ in 0..len {
            let c = random_cmd(&mut g);
            // TTL's exact value can differ by a rounding step between
            // servers; it is compared in the random test instead.
            if c[0] == "TTL" {
                continue;
            }
            short = short_ttl(&c);
            cmds.push(c);
            if short.is_some() {
                break;
            }
        }
        let mut bytes = Vec::new();
        for c in &cmds {
            bytes.extend(wire(c));
        }
        sprig.write_all(&bytes).await.unwrap();
        reference.write_all(&bytes).await.unwrap();
        let a = read_replies(&mut sprig, cmds.len()).await;
        let b = read_replies(&mut reference, cmds.len()).await;
        if a != b {
            bad += 1;
            eprintln!(
                "MISMATCH round {round}\n   sprig: {}\n   redis: {}",
                String::from_utf8_lossy(&a),
                String::from_utf8_lossy(&b)
            );
        }
        if let Some(ttl) = short {
            tokio::time::sleep(ttl + EXPIRY_MARGIN).await;
        }
    }
    assert_eq!(bad, 0);
}

/// Many clients pipelining on shared keys: every write lands once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_pipelines_lose_nothing() {
    let url = sprig_url().await;
    let mut c0 = conn(&url).await;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut c0)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for i in 0..16 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = conn(&url).await;
            for _ in 0..25 {
                let mut p = redis::pipe();
                for _ in 0..8 {
                    p.cmd("INCR").arg("shared");
                    p.cmd("LPUSH").arg("list").arg(i);
                    p.cmd("INCR").arg(format!("own{i}"));
                }
                let r: Vec<Value> = p.query_async(&mut c).await.unwrap();
                assert_eq!(r.len(), 24);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let shared: i64 = redis::cmd("GET")
        .arg("shared")
        .query_async(&mut c0)
        .await
        .unwrap();
    assert_eq!(shared, 16 * 25 * 8);
    let len: i64 = redis::cmd("LPUSH")
        .arg("list")
        .arg("x")
        .query_async(&mut c0)
        .await
        .unwrap();
    assert_eq!(len, 16 * 25 * 8 + 1);
    let own: i64 = redis::cmd("GET")
        .arg("own3")
        .query_async(&mut c0)
        .await
        .unwrap();
    assert_eq!(own, 200);
}

/// The engine's own corpus: every command it runs, with its options and
/// its errors. Lines whose result depends on timing or chance are left out
/// (they are in the random and model tests).
const ENGINE_CORPUS: &str = include_str!("engine_corpus.txt");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_corpus_matches_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let cmds = lines(ENGINE_CORPUS);
    for c in &cmds {
        assert!(
            covered(&c[0]) || c[0].starts_with("NOSUCH"),
            "the engine doesn't run {}",
            c[0]
        );
    }
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!(
        "engine corpus: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// A random keyspace or string command over a few keys of each type.
fn random_kv_cmd(g: &mut Rng) -> Vec<String> {
    let keys = ["s0", "s1", "s2", "n0", "n1", "h0", "l0", "b0"];
    let vals = ["a", "1", "-3", "10", "x y", "", "3.5", "007", "hello world"];
    let k = g.pick(&keys).to_owned();
    let k2 = g.pick(&keys).to_owned();
    let v = g.pick(&vals).to_owned();
    let n = |g: &mut Rng| g.pick(&["0", "1", "-1", "3", "-4", "100", "x"]).to_owned();
    let c: Vec<String> = match g.next() % 40 {
        0 => vec!["GET".into(), k],
        1 => vec!["SET".into(), k, v],
        2 => vec!["SETNX".into(), k, v],
        3 => vec![
            "SETEX".into(),
            k,
            g.pick(&["1000", "0", "-1", "x"]).into(),
            v,
        ],
        4 => vec!["GETSET".into(), k, v],
        5 => vec!["GETDEL".into(), k],
        6 => {
            let mut c = vec!["GETEX".into(), k];
            match g.next() % 4 {
                0 => {}
                1 => c.push("PERSIST".into()),
                2 => c.extend(["EX".into(), g.pick(&["1000", "0", "x"]).into()]),
                _ => c.extend(["PXAT".into(), "99999999999999".into()]),
            }
            c
        }
        7 => vec!["MSETNX".into(), k, v.clone(), k2, v],
        8 => vec!["APPEND".into(), k, v],
        9 => vec!["STRLEN".into(), k],
        10 => vec!["INCRBY".into(), k, n(g)],
        11 => vec!["DECRBY".into(), k, n(g)],
        12 => vec!["DECR".into(), k],
        13 => vec![
            "INCRBYFLOAT".into(),
            k,
            g.pick(&["1.5", "-0.25", "1e3", "x", "0"]).into(),
        ],
        14 => vec!["GETRANGE".into(), k, n(g), n(g)],
        15 => vec![
            "SETRANGE".into(),
            k,
            g.pick(&["0", "2", "7", "-1"]).into(),
            v,
        ],
        16 => vec![
            "SETBIT".into(),
            k,
            g.pick(&["0", "7", "13", "40", "-1"]).into(),
            g.pick(&["0", "1", "2"]).into(),
        ],
        17 => vec!["GETBIT".into(), k, g.pick(&["0", "7", "13", "99"]).into()],
        18 => {
            let mut c = vec!["BITCOUNT".into(), k];
            if g.next().is_multiple_of(2) {
                c.extend([n(g), n(g)]);
                if g.next().is_multiple_of(2) {
                    c.push(g.pick(&["BIT", "BYTE", "BAD"]).into());
                }
            }
            c
        }
        19 => {
            let mut c = vec!["BITPOS".into(), k, g.pick(&["0", "1", "2"]).into()];
            for _ in 0..(g.next() % 3) {
                c.push(n(g));
            }
            c
        }
        20 => vec![
            "BITOP".into(),
            g.pick(&["AND", "OR", "XOR", "NOT"]).into(),
            k,
            k2,
            g.pick(&keys).into(),
        ],
        21 => vec![
            "BITFIELD".into(),
            k,
            "INCRBY".into(),
            g.pick(&["u8", "i5", "u1", "i64"]).into(),
            g.pick(&["0", "#1", "3"]).into(),
            g.pick(&["1", "100", "-200"]).into(),
            "GET".into(),
            "u4".into(),
            "0".into(),
        ],
        22 => vec!["LCS".into(), k, k2],
        23 => vec!["EXISTS".into(), k.clone(), k2, k],
        24 => vec!["TYPE".into(), k],
        25 => vec!["RENAME".into(), k, k2],
        26 => vec!["RENAMENX".into(), k, k2],
        27 => {
            let mut c = vec!["COPY".into(), k, k2];
            if g.next().is_multiple_of(2) {
                c.push("REPLACE".into());
            }
            if g.next().is_multiple_of(3) {
                c.extend(["DB".into(), g.pick(&["0", "1"]).into()]);
            }
            c
        }
        28 => vec!["MOVE".into(), k, g.pick(&["0", "1", "x", "99"]).into()],
        29 => vec!["PERSIST".into(), k],
        30 => {
            let mut c = vec![
                g.pick(&["EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"])
                    .into(),
                k,
                g.pick(&["100000", "99999999999", "0", "-1", "x"]).into(),
            ];
            if g.next().is_multiple_of(2) {
                c.push(g.pick(&["NX", "XX", "GT", "LT"]).into());
            }
            c
        }
        31 => vec!["TTL".into(), k],
        32 => vec!["EXPIRETIME".into(), k],
        33 => vec!["PERSIST".into(), k],
        34 => vec![
            "KEYS".into(),
            g.pick(&["*", "s*", "?0", "[sn]1", "h*"]).into(),
        ],
        35 => vec!["DEL".into(), k, k2],
        36 => vec!["HSET".into(), k, "f".into(), v],
        37 => vec!["LPUSH".into(), k, v],
        38 => vec!["TOUCH".into(), k, k2],
        _ => vec!["SELECT".into(), g.pick(&["0", "1"]).into()],
    };
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_keyspace_and_strings_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let mut g = Rng(20_261_121);
    let cmds: Vec<Vec<String>> = (0..4000).map(|_| random_kv_cmd(&mut g)).collect();
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in bad.iter().take(20) {
        eprintln!("MISMATCH {b}");
    }
    eprintln!(
        "random keyspace/strings: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// Every key, by a full `SCAN` iteration with these options.
async fn scan_all(c: &mut redis::aio::MultiplexedConnection, opts: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_owned();
    let mut out = std::collections::BTreeSet::new();
    let mut rounds = 0;
    loop {
        let mut q = redis::cmd("SCAN");
        q.arg(&cursor);
        for o in opts {
            q.arg(*o);
        }
        let (next, keys): (String, Vec<String>) = q.query_async(c).await.unwrap();
        out.extend(keys);
        cursor = next;
        rounds += 1;
        assert!(rounds < 10_000, "SCAN never ended");
        if cursor == "0" {
            break;
        }
    }
    out.into_iter().collect()
}

/// A full `SCAN` (with `MATCH`, `COUNT` and `TYPE`) returns the same keys
/// as Redis; `RANDOMKEY` and `DBSIZE` agree with them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_iterations_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    for c in [&mut s, &mut r] {
        let mut p = redis::pipe();
        for i in 0..700 {
            match i % 4 {
                0 => p.cmd("SET").arg(format!("str:{i}")).arg(i),
                1 => p.cmd("HSET").arg(format!("hash:{i}")).arg("f").arg(i),
                2 => p.cmd("LPUSH").arg(format!("list:{i}")).arg(i),
                _ => p.cmd("SET").arg(format!("{{tag}}:{i}")).arg(i),
            };
        }
        p.query_async::<()>(c).await.unwrap();
    }
    for opts in [
        &[][..],
        &["COUNT", "7"],
        &["COUNT", "1000"],
        &["MATCH", "str:*"],
        &["MATCH", "*1?", "COUNT", "3"],
        &["TYPE", "hash"],
        &["TYPE", "list", "MATCH", "list:1*"],
        &["TYPE", "nosuchtype"],
    ] {
        let a = scan_all(&mut s, opts).await;
        let b = scan_all(&mut r, opts).await;
        assert_eq!(a, b, "SCAN {opts:?}");
    }
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut s).await.unwrap();
    assert_eq!(size, 700);
    let all = scan_all(&mut s, &[]).await;
    for _ in 0..20 {
        let k: String = redis::cmd("RANDOMKEY").query_async(&mut s).await.unwrap();
        assert!(all.binary_search(&k).is_ok(), "RANDOMKEY gave {k}");
    }
    redis::cmd("SELECT")
        .arg(3)
        .query_async::<()>(&mut s)
        .await
        .unwrap();
    let none: Option<String> = redis::cmd("RANDOMKEY").query_async(&mut s).await.unwrap();
    assert_eq!(none, None);
}

/// A random collection command. Keys ending in `b` start with 150 entries
/// (split on SprigDB); the others stay small. Commands whose result is
/// random, or ordered by a hash table in Redis, are left out.
fn random_coll_cmd(g: &mut Rng) -> Vec<String> {
    let hk = ["h1", "h2", "hb", "s1", "l1"];
    let lk = ["l1", "l2", "lb", "h1", "z1"];
    let sk = ["s1", "s2", "sb", "i1", "z1", "l1"];
    let zk = ["z1", "z2", "zb", "s1", "h1"];
    let f = |g: &mut Rng| {
        g.pick(&["a", "b", "c", "d", "e", "f7", "f77", "m5", "3", "-2", "10"])
            .to_owned()
    };
    let v = |g: &mut Rng| g.pick(&["1", "-3", "x", "", "2.5", "10"]).to_owned();
    let n = |g: &mut Rng| {
        g.pick(&["0", "1", "-1", "2", "-3", "5", "100", "x"])
            .to_owned()
    };
    let sc = |g: &mut Rng| {
        g.pick(&["0", "1", "-1", "2.5", "+inf", "-inf", "7", "(2", "(1", "x"])
            .to_owned()
    };
    let lx = |g: &mut Rng| {
        g.pick(&["-", "+", "[a", "(b", "[m5", "(f77", "c"])
            .to_owned()
    };
    let s = |x: &str| x.to_owned();
    match g.next() % 60 {
        0 | 1 => vec![s("HSET"), g.pick(&hk).into(), f(g), v(g), f(g), v(g)],
        2 => vec![s("HGET"), g.pick(&hk).into(), f(g)],
        3 => vec![s("HDEL"), g.pick(&hk).into(), f(g), f(g)],
        4 => vec![s("HINCRBY"), g.pick(&hk).into(), f(g), n(g)],
        5 => vec![
            s("HINCRBYFLOAT"),
            g.pick(&hk).into(),
            f(g),
            g.pick(&["1.5", "-2", "x"]).into(),
        ],
        6 => vec![s("HGETALL"), g.pick(&hk).into()],
        7 => vec![s(g.pick(&["HLEN", "HKEYS", "HVALS"])), g.pick(&hk).into()],
        8 => vec![s("HMGET"), g.pick(&hk).into(), f(g), f(g)],
        9 => vec![s("HSETNX"), g.pick(&hk).into(), f(g), v(g)],
        10 => vec![s(g.pick(&["HEXISTS", "HSTRLEN"])), g.pick(&hk).into(), f(g)],
        11 | 12 => vec![
            s(g.pick(&["LPUSH", "RPUSH", "LPUSHX", "RPUSHX"])),
            g.pick(&lk).into(),
            f(g),
            f(g),
        ],
        13 => {
            let mut c = vec![s(g.pick(&["LPOP", "RPOP"])), g.pick(&lk).into()];
            if g.next().is_multiple_of(2) {
                c.push(g.pick(&["0", "1", "3", "-1"]).into());
            }
            c
        }
        14 => vec![s("LRANGE"), g.pick(&lk).into(), n(g), n(g)],
        15 => vec![s("LINDEX"), g.pick(&lk).into(), n(g)],
        16 => vec![s("LSET"), g.pick(&lk).into(), n(g), f(g)],
        17 => vec![
            s("LINSERT"),
            g.pick(&lk).into(),
            s(g.pick(&["BEFORE", "AFTER"])),
            f(g),
            f(g),
        ],
        18 => vec![s("LREM"), g.pick(&lk).into(), n(g), f(g)],
        19 => vec![
            s("LTRIM"),
            g.pick(&lk).into(),
            n(g),
            g.pick(&["-1", "-2", "200", "3"]).into(),
        ],
        20 => {
            let mut c = vec![s("LPOS"), g.pick(&lk).into(), f(g)];
            if g.next().is_multiple_of(2) {
                c.extend([s("RANK"), s(g.pick(&["1", "-1", "2"]))]);
            }
            if g.next().is_multiple_of(2) {
                c.extend([s("COUNT"), s(g.pick(&["0", "2"]))]);
            }
            c
        }
        21 => vec![
            s("LMOVE"),
            g.pick(&lk).into(),
            g.pick(&lk).into(),
            s(g.pick(&["LEFT", "RIGHT"])),
            s(g.pick(&["LEFT", "RIGHT"])),
        ],
        22 => vec![
            s("LMPOP"),
            s("2"),
            g.pick(&lk).into(),
            g.pick(&lk).into(),
            s(g.pick(&["LEFT", "RIGHT"])),
            s("COUNT"),
            s(g.pick(&["1", "4"])),
        ],
        23 => vec![s("LLEN"), g.pick(&lk).into()],
        24 | 25 => vec![s("SADD"), g.pick(&sk).into(), f(g), f(g)],
        26 => vec![s("SREM"), g.pick(&sk).into(), f(g), f(g)],
        27 => vec![s("SMEMBERS"), g.pick(&sk).into()],
        28 => vec![
            s(g.pick(&["SISMEMBER", "SMISMEMBER"])),
            g.pick(&sk).into(),
            f(g),
        ],
        29 => vec![s("SCARD"), g.pick(&sk).into()],
        30 => vec![s("SMOVE"), g.pick(&sk).into(), g.pick(&sk).into(), f(g)],
        31 => vec![
            s(g.pick(&["SINTER", "SUNION", "SDIFF"])),
            g.pick(&sk).into(),
            g.pick(&sk).into(),
            g.pick(&sk).into(),
        ],
        32 => vec![
            s(g.pick(&["SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"])),
            g.pick(&["s2", "sd", "z2"]).into(),
            g.pick(&sk).into(),
            g.pick(&sk).into(),
        ],
        33 => vec![
            s("SINTERCARD"),
            s("2"),
            g.pick(&sk).into(),
            g.pick(&sk).into(),
            s("LIMIT"),
            s(g.pick(&["0", "2"])),
        ],
        34 | 35 => {
            let mut c = vec![s("ZADD"), g.pick(&zk).into()];
            for o in ["NX", "XX", "GT", "LT", "CH"] {
                if g.next().is_multiple_of(6) {
                    c.push(o.into());
                }
            }
            c.extend([sc(g).trim_start_matches('(').to_owned(), f(g)]);
            if g.next().is_multiple_of(2) {
                c.extend([g.pick(&["3", "4"]).into(), f(g)]);
            }
            c
        }
        36 => vec![
            s("ZINCRBY"),
            g.pick(&zk).into(),
            g.pick(&["1", "-2.5", "x"]).into(),
            f(g),
        ],
        37 => vec![s("ZREM"), g.pick(&zk).into(), f(g), f(g)],
        38 => vec![
            s(g.pick(&["ZSCORE", "ZRANK", "ZREVRANK"])),
            g.pick(&zk).into(),
            f(g),
        ],
        39 => vec![s("ZRANK"), g.pick(&zk).into(), f(g), s("WITHSCORE")],
        40 => vec![s("ZMSCORE"), g.pick(&zk).into(), f(g), f(g)],
        41 => vec![s("ZCARD"), g.pick(&zk).into()],
        42 => vec![s("ZCOUNT"), g.pick(&zk).into(), sc(g), sc(g)],
        43 => {
            let mut c = vec![s("ZRANGE"), g.pick(&zk).into(), n(g), n(g)];
            if g.next().is_multiple_of(2) {
                c.push(s("REV"));
            }
            if g.next().is_multiple_of(2) {
                c.push(s("WITHSCORES"));
            }
            c
        }
        44 => {
            let mut c = vec![s("ZRANGE"), g.pick(&zk).into(), sc(g), sc(g), s("BYSCORE")];
            if g.next().is_multiple_of(2) {
                c.push(s("REV"));
            }
            if g.next().is_multiple_of(2) {
                c.extend([
                    s("LIMIT"),
                    s(g.pick(&["0", "1", "3"])),
                    s(g.pick(&["2", "-1"])),
                ]);
            }
            c.push(s("WITHSCORES"));
            c
        }
        45 => vec![
            s(g.pick(&["ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZLEXCOUNT"])),
            g.pick(&["zl", "z1"]).into(),
            lx(g),
            lx(g),
        ],
        46 => vec![s("ZADD"), s("zl"), s("0"), f(g), s("0"), f(g)],
        47 => vec![
            s(g.pick(&["ZPOPMIN", "ZPOPMAX"])),
            g.pick(&zk).into(),
            s(g.pick(&["1", "2"])),
        ],
        48 => vec![s("ZREMRANGEBYRANK"), g.pick(&zk).into(), n(g), n(g)],
        49 => vec![s("ZREMRANGEBYSCORE"), g.pick(&zk).into(), sc(g), sc(g)],
        50 => vec![
            s(g.pick(&["ZUNION", "ZINTER", "ZDIFF"])),
            s("2"),
            g.pick(&zk).into(),
            g.pick(&zk).into(),
            s("WITHSCORES"),
        ],
        51 => vec![
            s(g.pick(&["ZUNIONSTORE", "ZINTERSTORE"])),
            g.pick(&["z2", "zd"]).into(),
            s("2"),
            g.pick(&zk).into(),
            g.pick(&zk).into(),
            s("WEIGHTS"),
            s("2"),
            s("-1"),
            s("AGGREGATE"),
            s(g.pick(&["SUM", "MIN", "MAX"])),
        ],
        52 => vec![
            s("ZRANGESTORE"),
            g.pick(&["z2", "zd"]).into(),
            g.pick(&zk).into(),
            n(g),
            n(g),
        ],
        53 => vec![
            s("ZMPOP"),
            s("2"),
            g.pick(&zk).into(),
            g.pick(&zk).into(),
            s(g.pick(&["MIN", "MAX"])),
            s("COUNT"),
            s("2"),
        ],
        54 => vec![
            s("ZINTERCARD"),
            s("2"),
            g.pick(&zk).into(),
            g.pick(&zk).into(),
        ],
        55 => vec![
            s("DEL"),
            g.pick(&["h1", "l1", "s1", "z1", "h2", "s2"]).into(),
        ],
        56 => vec![
            s("TYPE"),
            g.pick(&["h1", "l1", "s1", "z1", "hb", "lb", "sb", "zb"])
                .into(),
        ],
        57 => vec![
            s("COPY"),
            g.pick(&["hb", "lb", "sb", "zb", "z1"]).into(),
            g.pick(&["c1", "c2"]).into(),
            s("REPLACE"),
        ],
        58 => vec![
            s(g.pick(&["SCARD", "ZCARD", "HLEN", "LLEN"])),
            g.pick(&["c1", "c2"]).into(),
        ],
        _ => vec![
            s("RENAME"),
            g.pick(&["c1", "c2"]).into(),
            g.pick(&["c2", "c1", "h2"]).into(),
        ],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_collections_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    for c in [&mut s, &mut r] {
        let mut p = redis::pipe();
        let mut h = redis::cmd("HSET");
        let mut l = redis::cmd("RPUSH");
        let mut st = redis::cmd("SADD");
        let mut z = redis::cmd("ZADD");
        h.arg("hb");
        l.arg("lb");
        st.arg("sb");
        z.arg("zb");
        for i in 0..150 {
            h.arg(format!("f{i}")).arg(i);
            l.arg(format!("f{}", i % 20));
            st.arg(format!("f{i}"));
            z.arg(i % 11).arg(format!("f{i}"));
        }
        p.add_command(h)
            .add_command(l)
            .add_command(st)
            .add_command(z);
        p.cmd("SADD").arg("i1").arg(1).arg(2).arg(3);
        p.query_async::<()>(c).await.unwrap();
    }
    let mut g = Rng(20_261_122);
    let cmds: Vec<Vec<String>> = (0..5000).map(|_| random_coll_cmd(&mut g)).collect();
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in bad.iter().take(20) {
        eprintln!("MISMATCH {b}");
    }
    eprintln!(
        "random collections: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// Runs `cmds` on `c` and returns the replies as compared.
async fn replies(c: &mut redis::aio::MultiplexedConnection, cmds: &[&str]) -> Vec<String> {
    let mut out = Vec::new();
    for line in cmds {
        let cmd: Vec<String> = line.split(' ').map(str::to_owned).collect();
        out.push(normalize(&cmd, run(c, &cmd).await));
    }
    out
}

/// `WATCH`, then another client's write (or not), then `MULTI`/`EXEC`, on
/// both servers: the same outcomes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_and_exec_match_redis() {
    let Some((_, _)) = pair().await else {
        return;
    };
    let url = sprig_url().await;
    let refaddr = var("SPRIG_TEST_REDIS_REF").unwrap();
    let refurl = format!("redis://{refaddr}");
    // (watcher's commands before, the other client's, watcher's after).
    let cases: &[(&[&str], &[&str], &[&str])] = &[
        (
            &["SET w1 1", "WATCH w1"],
            &["SET w1 2"],
            &["MULTI", "INCR w1", "EXEC", "GET w1"],
        ),
        (
            &["SET w1 1", "WATCH w1"],
            &["GET w1"],
            &["MULTI", "INCR w1", "EXEC", "GET w1"],
        ),
        (
            &["WATCH w2"],
            &["SET w2 x"],
            &["MULTI", "SET w2 y", "EXEC", "GET w2"],
        ),
        (
            &["SET w3 1", "WATCH w3 other"],
            &["DEL other"],
            &["MULTI", "SET w3 2", "EXEC"],
        ),
        (
            &["SET w3 1", "WATCH w3"],
            &["SET w3 1"],
            &["MULTI", "SET w3 2", "EXEC"],
        ),
        (
            &["SET w4 1", "WATCH w4"],
            &["SET w4 2"],
            &["UNWATCH", "MULTI", "SET w4 3", "EXEC", "GET w4"],
        ),
        (
            &["SET w5 1", "WATCH w5"],
            &["SET w5 2"],
            &["MULTI", "DISCARD", "MULTI", "SET w5 3", "EXEC"],
        ),
        (
            &["RPUSH w6 a", "WATCH w6"],
            &["LPUSH w6 b"],
            &["MULTI", "LPOP w6", "EXEC", "LRANGE w6 0 -1"],
        ),
        (
            &["HSET w7 f 1", "WATCH w7"],
            &["HINCRBY w7 f 1"],
            &["MULTI", "HGET w7 f", "EXEC"],
        ),
        (
            &["SET w8 1", "WATCH w8"],
            &["FLUSHDB"],
            &["MULTI", "SET w8 2", "EXEC"],
        ),
        (
            &["SET w9 1", "WATCH w9"],
            &["EXPIRE w9 100"],
            &["MULTI", "GET w9", "EXEC"],
        ),
        (
            &["SET w10 1", "WATCH w10"],
            &["SELECT 1", "SET w10 2"],
            &["MULTI", "GET w10", "EXEC"],
        ),
        (
            &["SELECT 1", "SET w11 1", "WATCH w11", "SELECT 0"],
            &["SELECT 1", "SET w11 2"],
            &["MULTI", "SET x 1", "EXEC"],
        ),
        (
            &["SET w12 v PX 150", "WATCH w12"],
            &[],
            &["SLEEP", "MULTI", "SET z 1", "EXEC"],
        ),
        (
            &["SET w13 1", "WATCH w13"],
            &["RENAME w13 w14"],
            &["MULTI", "SET z 1", "EXEC"],
        ),
        (
            &["SET w15 1", "WATCH w16"],
            &["RENAME w15 w16"],
            &["MULTI", "SET z 1", "EXEC"],
        ),
    ];
    let mut bad = 0;
    for (i, (before, other, after)) in cases.iter().enumerate() {
        let mut outs = Vec::new();
        for u in [&url, &refurl] {
            let mut w = conn(u).await;
            let mut o = conn(u).await;
            redis::cmd("FLUSHALL")
                .query_async::<()>(&mut w)
                .await
                .unwrap();
            let mut got = replies(&mut w, before).await;
            got.extend(replies(&mut o, other).await);
            for line in *after {
                if *line == "SLEEP" {
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                } else {
                    got.extend(replies(&mut w, &[line]).await);
                }
            }
            outs.push(got);
        }
        if outs[0] != outs[1] {
            bad += 1;
            eprintln!(
                "MISMATCH case {i}\n   sprig: {:?}\n   redis: {:?}",
                outs[0], outs[1]
            );
        }
    }
    assert_eq!(bad, 0);
}

/// A blocking command, then (after a pause) another client's commands: the
/// blocked reply, on both servers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_commands_match_redis() {
    let Some((_, _)) = pair().await else {
        return;
    };
    let url = sprig_url().await;
    let refaddr = var("SPRIG_TEST_REDIS_REF").unwrap();
    let refurl = format!("redis://{refaddr}");
    let cases: &[(&str, &[&str])] = &[
        ("BLPOP b1 b2 5", &["RPUSH b2 x y"]),
        ("BRPOP b1 5", &["RPUSH b1 x y"]),
        ("BLPOP b1 0", &["SET other 1", "LPUSH b1 z"]),
        ("BLMOVE b1 b3 RIGHT LEFT 5", &["RPUSH b1 p q"]),
        ("BRPOPLPUSH b1 b3 5", &["RPUSH b1 p q"]),
        ("BLMPOP 5 2 b1 b2 LEFT COUNT 3", &["RPUSH b2 1 2 3 4"]),
        ("BZPOPMIN z1 z2 5", &["ZADD z2 3 c 1 a"]),
        ("BZPOPMAX z1 5", &["ZADD z1 3 c 1 a"]),
        ("BZMPOP 5 1 z1 MAX COUNT 2", &["ZADD z1 3 c 1 a 2 b"]),
        ("BLPOP b1 5", &["MULTI", "RPUSH b1 m", "EXEC"]),
        ("BLPOP b1 0.3", &[]),
        ("BZPOPMIN z1 0.3", &[]),
        ("BLMOVE b1 b3 LEFT LEFT 0.3", &[]),
        (
            "BLPOP b1 5",
            &["SET b1 notalist", "DEL b1", "RPUSH b1 late"],
        ),
    ];
    let mut bad = 0;
    for (i, (blocking, other)) in cases.iter().enumerate() {
        let mut outs = Vec::new();
        for u in [&url, &refurl] {
            let mut b = conn(u).await;
            let mut o = conn(u).await;
            redis::cmd("FLUSHALL")
                .query_async::<()>(&mut o)
                .await
                .unwrap();
            let cmd: Vec<String> = blocking.split(' ').map(str::to_owned).collect();
            let waiter = tokio::spawn(async move {
                let r = run(&mut b, &cmd).await;
                normalize(&cmd, r)
            });
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let mut got = replies(&mut o, other).await;
            got.push(waiter.await.unwrap());
            got.extend(
                replies(
                    &mut o,
                    &["LRANGE b1 0 -1", "LRANGE b3 0 -1", "ZRANGE z1 0 -1"],
                )
                .await,
            );
            outs.push(got);
        }
        if outs[0] != outs[1] {
            bad += 1;
            eprintln!(
                "MISMATCH case {i}\n   sprig: {:?}\n   redis: {:?}",
                outs[0], outs[1]
            );
        }
    }
    assert_eq!(bad, 0);
}

/// Optimistic increments (`WATCH`, `GET`, `MULTI`, `SET`, `EXEC`, retried
/// on nil) from many clients on one key: none is lost, and transactions
/// that read their own writes see them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn watched_counter_loses_nothing() {
    let url = sprig_url().await;
    let mut c0 = conn(&url).await;
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut c0)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut c = conn(&url).await;
            let mut retries = 0;
            for _ in 0..20 {
                loop {
                    redis::cmd("WATCH")
                        .arg("ctr")
                        .query_async::<()>(&mut c)
                        .await
                        .unwrap();
                    let v: Option<i64> = redis::cmd("GET")
                        .arg("ctr")
                        .query_async(&mut c)
                        .await
                        .unwrap();
                    let r: Option<Vec<Value>> = redis::pipe()
                        .atomic()
                        .cmd("SET")
                        .arg("ctr")
                        .arg(v.unwrap_or(0) + 1)
                        .cmd("INCR")
                        .arg("ops")
                        .query_async(&mut c)
                        .await
                        .unwrap();
                    if r.is_some() {
                        break;
                    }
                    retries += 1;
                }
            }
            retries
        }));
    }
    let mut retries = 0;
    for t in tasks {
        retries += t.await.unwrap();
    }
    let ctr: i64 = redis::cmd("GET")
        .arg("ctr")
        .query_async(&mut c0)
        .await
        .unwrap();
    let ops: i64 = redis::cmd("GET")
        .arg("ops")
        .query_async(&mut c0)
        .await
        .unwrap();
    eprintln!("watched counter: 320 increments, {retries} retries");
    assert_eq!(ctr, 320);
    assert_eq!(ops, 320);
}

/// Random `PFADD`, `PFCOUNT` and `PFMERGE` over a few sketches, from a pool
/// of 50,000 elements: the same answers as Redis (same hash, registers and
/// estimator), through the sparse and dense forms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_hyperloglog_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let mut g = Rng(20_261_123);
    let keys = ["p0", "p1", "p2", "p3"];
    let mut cmds: Vec<Vec<String>> = Vec::new();
    for _ in 0..600 {
        let k = g.pick(&keys).to_owned();
        match g.next() % 6 {
            0..=2 => {
                let mut c = vec!["PFADD".to_owned(), k];
                let n = 1 + g.next() % 200;
                let base = g.next() % 50_000;
                for i in 0..n {
                    c.push(format!("e{}", (base + i * 7) % 50_000));
                }
                cmds.push(c);
            }
            3 => cmds.push(vec!["PFCOUNT".into(), k]),
            4 => cmds.push(vec![
                "PFCOUNT".into(),
                k,
                g.pick(&keys).into(),
                g.pick(&keys).into(),
            ]),
            _ => cmds.push(vec!["PFMERGE".into(), k, g.pick(&keys).into()]),
        }
    }
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in bad.iter().take(10) {
        eprintln!("MISMATCH {}", &b[..b.len().min(300)]);
    }
    eprintln!(
        "random hyperloglog: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    assert!(bad.is_empty());
}

/// Hash fields with short lifetimes really expire, on both servers: reads,
/// lengths, scans and the key's own existence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_fields_expire_like_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let big: Vec<String> = (0..140).map(|i| format!("f{i} v{i}")).collect();
    let steps: &[&[&str]] = &[
        &[
            "HSET a x 1 y 2 z 3",
            "HPEXPIRE a 150 FIELDS 2 x y",
            "HSET b only 1",
            "HPEXPIRE b 150 FIELDS 1 only",
        ],
        &[
            "HGETALL a",
            "HLEN a",
            "EXISTS b",
            "HTTL a FIELDS 3 x y z",
            "HGET a x",
            "TYPE b",
            "DBSIZE",
            "HSCAN a 0",
        ],
    ];
    let mut outs = Vec::new();
    for c in [&mut s, &mut r] {
        let mut got = replies(c, steps[0]).await;
        let line = format!("HSET big {}", big.join(" "));
        got.extend(
            replies(
                c,
                &[
                    line.as_str(),
                    "HPEXPIRE big 150 FIELDS 3 f1 f2 f3",
                    "HLEN big",
                ],
            )
            .await,
        );
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        got.extend(replies(c, steps[1]).await);
        got.extend(replies(c, &["HLEN big", "HGET big f2", "HEXISTS big f4", "KEYS *"]).await);
        outs.push(got);
    }
    assert_eq!(outs[0], outs[1]);
}

/// Long strings changed in place (SprigDB stores them in chunks): random
/// `SETRANGE`, `SETBIT`, `APPEND`, `BITFIELD`, `GETRANGE`, `GETBIT`,
/// `STRLEN`, `BITCOUNT`, `BITPOS` and `GET` agree with Redis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_long_strings_match_redis() {
    let Some((mut s, mut r)) = pair().await else {
        return;
    };
    let mut g = Rng(20_261_124);
    let keys = ["b0", "b1"];
    let mut cmds: Vec<Vec<String>> = Vec::new();
    for _ in 0..1_500 {
        let k = g.pick(&keys).to_owned();
        let n = g.next() % 700_000;
        let c = match g.next() % 12 {
            0 | 1 => vec![
                "SETRANGE".into(),
                k,
                n.to_string(),
                format!("v{}", g.next() % 1000),
            ],
            2 | 3 => vec![
                "SETBIT".into(),
                k,
                (n * 8 + g.next() % 8).to_string(),
                (g.next() % 2).to_string(),
            ],
            4 => vec!["APPEND".into(), k, "z".repeat((g.next() % 3000) as usize)],
            5 => vec![
                "BITFIELD".into(),
                k,
                "INCRBY".into(),
                "u12".into(),
                (n * 3).to_string(),
                "77".into(),
                "GET".into(),
                "i32".into(),
                (n * 2).to_string(),
            ],
            6 | 7 => vec![
                "GETRANGE".into(),
                k,
                n.to_string(),
                (n + g.next() % 20_000).to_string(),
            ],
            8 => vec!["GETBIT".into(), k, (n * 8).to_string()],
            9 => vec!["STRLEN".into(), k],
            10 => vec![
                "BITCOUNT".into(),
                k,
                (n / 10).to_string(),
                (n / 10 + g.next() % 50_000).to_string(),
            ],
            _ => vec![
                "BITPOS".into(),
                k,
                (g.next() % 2).to_string(),
                (n / 10).to_string(),
            ],
        };
        cmds.push(c);
    }
    cmds.push(vec!["GET".into(), "b0".into()]);
    cmds.push(vec!["GET".into(), "b1".into()]);
    cmds.push(vec!["BITCOUNT".into(), "b0".into()]);
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in bad.iter().take(10) {
        eprintln!("MISMATCH {}", &b[..b.len().min(300)]);
    }
    eprintln!(
        "random long strings: {} commands, {} mismatches",
        cmds.len(),
        bad.len()
    );
    assert!(bad.is_empty());
}
