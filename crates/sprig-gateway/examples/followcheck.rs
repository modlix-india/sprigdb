//! "Never older than the token" checks for reads from copies over
//! MongoDB, Redis and Bolt, against a running deployment.
//!
//! ```text
//! followcheck <mongo|redis|bolt> <secs> <writers> <writer host:port> <reader host:port>[,...]
//! ```
//!
//! Environment: `FC_TENANT`, `FC_USER`, `FC_PASS` (the login is
//! `<tenant>.<user>`). Each writer bumps its own keys (so every key's
//! value only grows) on a connection the deployment hands to the tenant's
//! leader, takes its token, and passes key, value and token to a reader
//! connection, which must read that value or a newer one. Every reader
//! connection's values must also never go back.
//!
//! - `mongo`: writes in a causally consistent session; the token is its
//!   operation time. Readers use `readPreference=secondary` in a causally
//!   consistent session advanced to the token, so the leader ships the
//!   read to a copy with `afterClusterTime`.
//! - `redis`: the token is `SPRIG.TOKEN` after the write. Readers send
//!   `READONLY` before `AUTH` (reader connections, served on a copy) and
//!   `SPRIG.TOKEN <token>` before each `GET`.
//! - `bolt`: the token is the commit's bookmark (graph `FC_GRAPH_DB`,
//!   default `nfd.neo4j`). Readers connect to the
//!   reader addresses as a routing driver (`HELLO` with a routing context)
//!   and read in `mode: "r"` transactions with the bookmark.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mongodb::bson::{Document, doc};
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::client::BoltClient;
use sprig_proto_bolt::message::{Request, Response};
use tokio::net::TcpStream;

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

fn login() -> (String, String) {
    (
        format!("{}.{}", env("FC_TENANT", "nfa"), env("FC_USER", "app")),
        env("FC_PASS", "nf-app-1"),
    )
}

#[derive(Default)]
struct Tally {
    checked: AtomicU64,
    older: AtomicU64,
    back: AtomicU64,
    errors: std::sync::Mutex<HashMap<String, u64>>,
}

impl Tally {
    fn error(&self, what: impl Into<String>) {
        *self.errors.lock().unwrap().entry(what.into()).or_default() += 1;
    }

    fn check(&self, who: &str, key: u64, got: i64, wrote: i64, seen: &mut HashMap<u64, i64>) {
        self.checked.fetch_add(1, Ordering::Relaxed);
        if got < wrote {
            self.older.fetch_add(1, Ordering::Relaxed);
            eprintln!("OLDER: {who} key {key} read {got} after write {wrote}");
        }
        let last = seen.entry(key).or_insert(got);
        if got < *last {
            self.back.fetch_add(1, Ordering::Relaxed);
            eprintln!("BACK: {who} key {key} read {got} after {last}");
        }
        *last = (*last).max(got);
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// A writer's next key and value: its own keys only (`w + writers * j`).
fn next_key(rng: &mut Rng, w: usize, writers: usize, vals: &mut HashMap<u64, i64>) -> (u64, i64) {
    let k = w as u64 + writers as u64 * (rng.next() % (1000 / writers as u64).max(1));
    let v = vals.get(&k).copied().unwrap_or(0) + 1;
    (k, v)
}

async fn mongo(secs: u64, writers: usize, wa: String, ras: Vec<String>, t: Arc<Tally>) {
    use mongodb::Client;
    let (user, pass) = login();
    let uri = |a: &str, pref: &str| {
        format!(
            "mongodb://{user}:{pass}@{a}/?authSource=admin&directConnection=true&retryWrites=false&readPreference={pref}"
        )
    };
    let deadline = Instant::now() + Duration::from_secs(secs);
    let wc = Client::with_uri_str(uri(&wa, "primary")).await.unwrap();
    let mut readers = Vec::new();
    for a in &ras {
        readers.push(Client::with_uri_str(uri(a, "secondary")).await.unwrap());
    }
    // `FC_MONGO_SQL=1`: the SQL table `nfd.kv` (`k`, `v`; readload's)
    // as the collection, keyed by `k`.
    let sql = env("FC_MONGO_SQL", "0") == "1";
    let (dbn, key) = if sql { ("nfd", "k") } else { ("fc", "_id") };
    if !sql {
        let _ = wc.database(dbn).collection::<Document>("kv").drop().await;
    }
    let mut hs = Vec::new();
    for w in 0..writers {
        let (wc, readers, ras, t) = (wc.clone(), readers.clone(), ras.clone(), Arc::clone(&t));
        hs.push(tokio::spawn(async move {
            let mut rng = Rng(0x5EED_0000 + w as u64 * 7 + 1);
            let mut vals = HashMap::new();
            let mut seen: Vec<HashMap<u64, i64>> = readers.iter().map(|_| HashMap::new()).collect();
            let wcoll = wc.database(dbn).collection::<Document>("kv");
            let mut ws = wc.start_session().causal_consistency(true).await.unwrap();
            let mut rs = Vec::new();
            for r in &readers {
                rs.push(r.start_session().causal_consistency(true).await.unwrap());
            }
            while Instant::now() < deadline {
                let (k, v) = next_key(&mut rng, w, writers, &mut vals);
                let r = wcoll
                    .update_one(doc! {key: k as i64}, doc! {"$set": {"v": v}})
                    .upsert(!sql)
                    .session(&mut ws)
                    .await;
                if let Err(e) = r {
                    t.error(format!("write {}", e.kind));
                    continue;
                }
                vals.insert(k, v);
                let Some(token) = ws.operation_time() else {
                    t.error("no operationTime");
                    continue;
                };
                let i = (rng.next() % readers.len() as u64) as usize;
                rs[i].advance_operation_time(token);
                let rcoll = readers[i].database(dbn).collection::<Document>("kv");
                match rcoll
                    .find_one(doc! {key: k as i64})
                    .session(&mut rs[i])
                    .await
                {
                    Ok(Some(d)) => {
                        let got = d.get_i64("v").or_else(|_| d.get_i32("v").map(i64::from));
                        match got {
                            Ok(g) => t.check(&ras[i], k, g, v, &mut seen[i]),
                            Err(_) => t.error("read without v"),
                        }
                    }
                    Ok(None) => t.error("read missing"),
                    Err(e) => t.error(format!("read {}", e.kind)),
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

async fn redis_conn(
    addr: &str,
    reader: bool,
) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
    let (user, pass) = login();
    let c = redis::Client::open(format!("redis://{addr}/"))?;
    let mut con = c.get_multiplexed_async_connection().await?;
    if reader {
        redis::cmd("READONLY").query_async::<()>(&mut con).await?;
    }
    redis::cmd("AUTH")
        .arg(user)
        .arg(pass)
        .query_async::<()>(&mut con)
        .await?;
    Ok(con)
}

async fn redis_check(secs: u64, writers: usize, wa: String, ras: Vec<String>, t: Arc<Tally>) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut hs = Vec::new();
    for w in 0..writers {
        let (wa, ras, t) = (wa.clone(), ras.clone(), Arc::clone(&t));
        hs.push(tokio::spawn(async move {
            let mut rng = Rng(0xFEED_0000 + w as u64 * 11 + 3);
            let mut vals = HashMap::new();
            let mut seen: Vec<HashMap<u64, i64>> = ras.iter().map(|_| HashMap::new()).collect();
            let mut wc = redis_conn(&wa, false).await.unwrap();
            let mut rcs = Vec::new();
            for a in &ras {
                rcs.push(redis_conn(a, true).await.unwrap());
            }
            while Instant::now() < deadline {
                let (k, v) = next_key(&mut rng, w, writers, &mut vals);
                let key = format!("fc:{k}");
                if let Err(e) = redis::cmd("SET")
                    .arg(&key)
                    .arg(v)
                    .query_async::<()>(&mut wc)
                    .await
                {
                    t.error(format!("write {}", e.code().unwrap_or("io")));
                    // A connection closed by a leadership move: reconnect.
                    if e.code().is_none() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        if let Ok(c) = redis_conn(&wa, false).await {
                            wc = c;
                        }
                    }
                    continue;
                }
                vals.insert(k, v);
                let token: String = match redis::cmd("SPRIG.TOKEN").query_async(&mut wc).await {
                    Ok(x) => x,
                    Err(e) => {
                        t.error(format!("token {}", e.code().unwrap_or("io")));
                        continue;
                    }
                };
                let i = (rng.next() % ras.len() as u64) as usize;
                let rc = &mut rcs[i];
                if let Err(e) = redis::cmd("SPRIG.TOKEN")
                    .arg(&token)
                    .query_async::<String>(rc)
                    .await
                {
                    t.error(format!("set token {}", e.code().unwrap_or("io")));
                    continue;
                }
                match redis::cmd("GET")
                    .arg(&key)
                    .query_async::<Option<i64>>(rc)
                    .await
                {
                    Ok(Some(g)) => t.check(&ras[i], k, g, v, &mut seen[i]),
                    Ok(None) => t.error("read missing"),
                    Err(e) => {
                        t.error(format!("read {}", e.code().unwrap_or("io")));
                        if e.code().is_none()
                            && let Ok(c) = redis_conn(&ras[i], true).await
                        {
                            rcs[i] = c;
                        }
                    }
                }
            }
            // A write on a reader connection is refused.
            let refused = redis::cmd("SET")
                .arg("fc:x")
                .arg(1)
                .query_async::<()>(&mut rcs[0])
                .await;
            if !matches!(&refused, Err(e) if e.code() == Some("READONLY")) {
                t.error(format!("reader write not refused: {refused:?}"));
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

async fn bolt_conn(addr: &str, routing: bool) -> Result<BoltClient<TcpStream>, String> {
    let (user, pass) = login();
    let io = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let mut c = BoltClient::handshake(io, None)
        .await
        .map_err(|e| e.to_string())?;
    if !routing {
        c.login(&user, &pass).await.map_err(|e| e.to_string())?;
        return Ok(c);
    }
    let hello = PackValue::map([
        ("user_agent", PackValue::str("followcheck/1")),
        (
            "routing",
            PackValue::map([("address", PackValue::str(addr))]),
        ),
    ]);
    c.send(&Request::Hello(hello))
        .await
        .map_err(|e| e.to_string())?;
    match c.recv().await.map_err(|e| e.to_string())? {
        Response::Success(_) => {}
        other => return Err(format!("HELLO: {other:?}")),
    }
    let auth = PackValue::map([
        ("scheme", PackValue::str("basic")),
        ("principal", PackValue::str(&user)),
        ("credentials", PackValue::str(&pass)),
    ]);
    c.send(&Request::Logon(auth))
        .await
        .map_err(|e| e.to_string())?;
    match c.recv().await.map_err(|e| e.to_string())? {
        Response::Success(_) => Ok(c),
        other => Err(format!("LOGON: {other:?}")),
    }
}

async fn bolt(secs: u64, writers: usize, wa: String, ras: Vec<String>, t: Arc<Tally>) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut hs = Vec::new();
    for w in 0..writers {
        let (wa, ras, t) = (wa.clone(), ras.clone(), Arc::clone(&t));
        hs.push(tokio::spawn(async move {
            let mut rng = Rng(0xB017_0000 + w as u64 * 13 + 5);
            // `<logical database>.<graph>` (`FC_GRAPH_DB`).
            let db = env("FC_GRAPH_DB", "nfd.neo4j");
            let mut vals = HashMap::new();
            let mut seen: Vec<HashMap<u64, i64>> = ras.iter().map(|_| HashMap::new()).collect();
            let mut wc = bolt_conn(&wa, false).await.unwrap();
            let mut rcs = Vec::new();
            for a in &ras {
                rcs.push(bolt_conn(a, true).await.unwrap());
            }
            while Instant::now() < deadline {
                let (k, v) = next_key(&mut rng, w, writers, &mut vals);
                let wrote: Result<PackValue, String> = async {
                    wc.begin(vec![("db", PackValue::str(&db))])
                        .await
                        .map_err(|e| e.code.clone())?;
                    wc.query(
                        "MERGE (n:K {id: $k}) SET n.v = $v",
                        vec![("k", PackValue::Int(k as i64)), ("v", PackValue::Int(v))],
                        vec![],
                    )
                    .await
                    .map_err(|e| e.code.clone())?;
                    wc.commit().await.map_err(|e| e.code.clone())
                }
                .await;
                let bookmark = match wrote {
                    Ok(m) => m
                        .get("bookmark")
                        .and_then(PackValue::as_str)
                        .map(str::to_owned),
                    Err(e) => {
                        t.error(format!("write {e}"));
                        let _ = wc.reset().await;
                        continue;
                    }
                };
                vals.insert(k, v);
                let Some(bookmark) = bookmark else {
                    t.error("no bookmark");
                    continue;
                };
                let i = (rng.next() % ras.len() as u64) as usize;
                let rc = &mut rcs[i];
                let read: Result<Option<i64>, String> = async {
                    rc.begin(vec![
                        ("db", PackValue::str(&db)),
                        ("mode", PackValue::str("r")),
                        (
                            "bookmarks",
                            PackValue::List(vec![PackValue::str(&bookmark)]),
                        ),
                    ])
                    .await
                    .map_err(|e| e.code.clone())?;
                    let rows = rc
                        .query(
                            "MATCH (n:K {id: $k}) RETURN n.v",
                            vec![("k", PackValue::Int(k as i64))],
                            vec![],
                        )
                        .await
                        .map_err(|e| e.code.clone())?;
                    rc.commit().await.map_err(|e| e.code.clone())?;
                    Ok(rows.records.first().and_then(|r| match r.first() {
                        Some(PackValue::Int(x)) => Some(*x),
                        _ => None,
                    }))
                }
                .await;
                match read {
                    Ok(Some(g)) => t.check(&ras[i], k, g, v, &mut seen[i]),
                    Ok(None) => t.error("read missing"),
                    Err(e) => {
                        t.error(format!("read {e}"));
                        let _ = rc.reset().await;
                    }
                }
            }
            // A write transaction on a copy's routing connection is
            // refused with NotALeader (the readers are copies here).
            match rcs[0].begin(vec![("db", PackValue::str(&db))]).await {
                Err(e) if e.code == "Neo.ClientError.Cluster.NotALeader" => {}
                other => t.error(format!("reader write begin not refused: {other:?}")),
            }
            let _ = rcs[0].reset().await;
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() < 5 {
        eprintln!("usage: followcheck <mongo|redis|bolt> <secs> <writers> <writer> <reader>[,...]");
        std::process::exit(2);
    }
    let secs: u64 = a[1].parse().unwrap();
    let writers: usize = a[2].parse().unwrap();
    let wa = a[3].clone();
    let ras: Vec<String> = a[4].split(',').map(str::to_owned).collect();
    let t = Arc::new(Tally::default());
    let started = Instant::now();
    match a[0].as_str() {
        "mongo" => mongo(secs, writers, wa, ras.clone(), Arc::clone(&t)).await,
        "redis" => redis_check(secs, writers, wa, ras.clone(), Arc::clone(&t)).await,
        "bolt" => bolt(secs, writers, wa, ras.clone(), Arc::clone(&t)).await,
        other => {
            eprintln!("unknown protocol {other}");
            std::process::exit(2);
        }
    }
    println!(
        "{}: {writers} writers, readers {:?}, {:.0} s: {} checked reads, {} older than the token, {} went back, errors {:?}",
        a[0],
        ras,
        started.elapsed().as_secs_f64(),
        t.checked.load(Ordering::Relaxed),
        t.older.load(Ordering::Relaxed),
        t.back.load(Ordering::Relaxed),
        t.errors.lock().unwrap()
    );
}
