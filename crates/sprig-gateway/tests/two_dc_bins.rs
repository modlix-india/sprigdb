//! Two data centres with the real binaries: `sprigdb controller` and
//! `sprigdb gateway` in `fra1` (tagged `jurisdiction=DE`) and `ams1`
//! (`jurisdiction=NL`), each with its own private `mariadb:11.8` cluster,
//! over one private etcd (single-etcd mode), set up with `sprigctl` as an
//! operator would. Every tenant has one home DC; the other DC's gateway
//! relays.
//!
//! ```sh
//! cargo build -p sprigdb -p sprigctl
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-gateway --test two_dc_bins -- --nocapture
//! ```
//!
//! `XDC_TWO_TIER=1` runs it in two etcd tiers (a global etcd on 33379 and
//! one per DC on 33376 and 33377; other DCs' tiers are read through the
//! controllers' relay); run it there with `XDC_ONLY=residency,streams,sinks`
//! (job standbys are single-etcd only). `XDC_ONLY` picks parts.
//!
//! Ports 33306-33392 on 127.0.0.1; tenant ids from 33,306,000,000 (the
//! MySQL port x 1,000,000, `tenant.id_min`/`id_max`). Nothing touches the
//! shared stack. Checked, each part recording its checks so a failure in one
//! doesn't hide the others:
//!
//! 1. **Residency.** Tenant `xdc-eu` lives in `fra1`; with
//!    `jurisdiction = "DE"` applied, every login at `ams1` is handed to
//!    `fra1` (MySQL password, MySQL token, HTTP Basic, HTTP bearer token, API
//!    key, `MONGODB-OIDC`, MongoDB SCRAM, Redis `AUTH`) and the statements
//!    after it run against `fra1`'s cluster; with `deny_remote_access` each
//!    one is refused with 39302 while `fra1` still accepts.
//!    `SELECT SPRIG_CLAIM('org')` in a plain query gives the token's claim
//!    over MySQL and HTTP.
//! 2. **Change streams.** A change stream of a `fra1` tenant opened at
//!    `ams1` (NDJSON and SSE) is relayed from a `fra1` gateway: it gets the
//!    tenant's changes, outlives the peer call timeout, and resumes from its
//!    last token. A MongoDB change stream at `ams1` renders the items of a
//!    `fra1` gateway's tail (typed, resumable), and a Kafka fetch at `ams1`
//!    reads the tenant's stream and wakes on `fra1`'s next record.
//! 3. **Inherited sinks and members.** Declarer `xdc-p` (in `fra1`) has a
//!    `FOR EACH TENANT` stream, connection and sink; its member `xdc-m` is
//!    created directly in `ams1`. The member's instance appears on its first
//!    publish; a second sink created by the declarer reaches it; `DROP SINK`
//!    at `fra1` removes the member's sink and connection in `ams1` at once.
//!    `SHOW INHERITED DEFINITIONS` at `fra1` counts the member's instance, an
//!    override `FOR TENANT` is applied to it, `DROP TRIGGER` stops its
//!    trigger instance, and a broadcast reaches it once. A queue defined at
//!    `fra1` after the member made its AMQP vhost's instances in `ams1`
//!    reaches that vhost without another use.
//! 4. **A move into the standby's DC.** A tenant with its own
//!    `failover.standby_dcs = 1` gets a job standby in `ams1`; `sprigctl
//!    tenant move --to ams1` drops the standby first and moves it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    missing_docs
)]

use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use futures::FutureExt as _;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, KeyPair};
use serde_json::{Value, json};
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Tier, Txn};
use sprig_testkit::faults::compose::OwnContainer;
use sprig_testkit::skip;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const H: &str = "127.0.0.1";
const ROOT_PW: &str = "sprig-xdc-root";
const APP_PW: &str = "sprig-xdc-app";
const DEPLOYMENT: &str = "sprigdb-xdc";
/// The MySQL port of `fra1`'s gateway x 1,000,000 (below 2^38).
const TENANT_BASE: u64 = 33_306_000_000;
const ETCD_PORT: u16 = 33379;

/// `XDC_TWO_TIER=1`: each DC gets its own etcd (33376 for `fra1`, 33377
/// for `ams1`) beside the global one on 33379, and other DCs' tiers are
/// read through the controllers' relay.
fn two_tier() -> bool {
    std::env::var("XDC_TWO_TIER").is_ok_and(|v| v == "1")
}

/// The etcd of `dc`'s tier.
fn dc_seed(dc: &str) -> String {
    if two_tier() {
        format!(
            "http://127.0.0.1:{}",
            if dc == "fra1" { 33376 } else { 33377 }
        )
    } else {
        format!("http://127.0.0.1:{ETCD_PORT}")
    }
}

/// `[meta] global_seeds`: empty in single-etcd mode.
fn global_seeds() -> String {
    if two_tier() {
        format!("\"http://127.0.0.1:{ETCD_PORT}\"")
    } else {
        String::new()
    }
}
const ISS: &str = "https://idp.xdc.test";
const AUD: &str = "xdc-app";
/// The test's webhook receiver for sinks.
const HOOK_PORT: u16 = 33390;

#[derive(Clone, Copy)]
struct Ports {
    mysql: u16,
    mariadb: u16,
    http: u16,
    https: u16,
    peer: u16,
    gw_tel: u16,
    admin: u16,
    internal: u16,
    ctl_tel: u16,
    mongo: u16,
    redis: u16,
    wk_tel: u16,
    kafka: u16,
    db: u16,
}

fn ports(dc: &str) -> Ports {
    let b: u16 = if dc == "fra1" { 33306 } else { 33336 };
    Ports {
        mysql: b,
        mariadb: b + 1,
        http: b + 12,
        https: b + 13,
        peer: b + 14,
        gw_tel: b + 15,
        admin: b + 16,
        internal: b + 17,
        ctl_tel: b + 18,
        mongo: b + 19,
        redis: b + 20,
        wk_tel: b + 21,
        kafka: b + 22,
        db: if dc == "fra1" { 33391 } else { 33392 },
    }
}

const DCS: [&str; 2] = ["fra1", "ams1"];

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            eprintln!("PASS {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn bins() -> Option<(PathBuf, PathBuf)> {
    let target = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")),
        PathBuf::from,
    );
    let d = target.join("debug");
    let (s, c) = (d.join("sprigdb"), d.join("sprigctl"));
    (s.exists() && c.exists()).then_some((s, c))
}

struct Procs {
    bin: PathBuf,
    work: PathBuf,
    kids: Vec<Child>,
}

impl Procs {
    fn start(&mut self, name: &str, role: &str, toml: &str) {
        let cfg = self.work.join(format!("{name}.toml"));
        std::fs::write(&cfg, toml).expect("config");
        let log = std::fs::File::create(self.work.join(format!("{name}.log"))).expect("log");
        let child = Command::new(&self.bin)
            .args(["--config", cfg.to_str().expect("path"), role])
            .env(
                "RUST_LOG",
                std::env::var("XDC_RUST_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdout(Stdio::from(log.try_clone().expect("log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn sprigdb");
        self.kids.push(child);
    }
}

impl Drop for Procs {
    fn drop(&mut self) {
        for c in &mut self.kids {
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
        }
        let t0 = Instant::now();
        for c in &mut self.kids {
            while t0.elapsed() < Duration::from_secs(10) {
                if c.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// One HTTP/1.1 request on a plain connection: (status, body).
async fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut h = String::new();
    for (k, v) in headers {
        h.push_str(k);
        h.push_str(": ");
        h.push_str(v);
        h.push_str("\r\n");
    }
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {H}:{port}\r\n{h}Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut out = Vec::new();
    let r = async {
        let mut s = TcpStream::connect((H, port)).await?;
        s.write_all(req.as_bytes()).await?;
        s.read_to_end(&mut out).await
    };
    if tokio::time::timeout(Duration::from_secs(15), r)
        .await
        .is_err()
    {
        return (0, "timeout".into());
    }
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, b)| b.to_owned());
    (status, body)
}

async fn ready(port: u16, within: Duration) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < within {
        if http(port, "GET", "/readyz", &[], "").await.0 == 200 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    false
}

async fn metric(port: u16, line_prefix: &str) -> f64 {
    http(port, "GET", "/metrics", &[], "")
        .await
        .1
        .lines()
        .filter(|l| l.starts_with(line_prefix))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

fn ctl(sprigctl: &Path, dc: &str, args: &[&str]) -> Result<Value, String> {
    let out = Command::new(sprigctl)
        .args([
            "--endpoint",
            &format!("http://{H}:{}", ports(dc).admin),
            "-o",
            "json",
        ])
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

async fn ctl_retry(sprigctl: &Path, dc: &str, args: &[&str], within: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        match ctl(sprigctl, dc, args) {
            Ok(v) => return v,
            Err(e) if t0.elapsed() < within => {
                eprintln!("retrying: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn key(path: &str) -> sprig_meta::Key {
    if let Some(rel) = path.strip_prefix("/sprig/g/") {
        return Tier::Global.key(rel).expect("key");
    }
    let rest = path.strip_prefix("/sprig/dc/").expect("a tier key");
    let (dc, rel) = rest.split_once('/').expect("dc/rel");
    Tier::Dc(sprig_common::ids::DcId::new(dc).expect("dc"))
        .key(rel)
        .expect("key")
}

async fn put(store: &dyn MetaStore, path: &str, value: Value) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key(path),
                value: serde_json::to_vec(&value).expect("json"),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .expect("put");
}

async fn get(store: &dyn MetaStore, path: &str) -> Option<Value> {
    let kv = store
        .get(&key(path), Consistency::Linearizable)
        .await
        .ok()??;
    serde_json::from_slice(&kv.value).ok()
}

/// Keys under `prefix` (a tier path ending in `/`).
async fn keys_under(store: &dyn MetaStore, prefix: &str) -> Vec<String> {
    store
        .list(&key(prefix), ListOpts::default())
        .await
        .map(|p| p.kvs.iter().map(|kv| kv.key.as_str().to_owned()).collect())
        .unwrap_or_default()
}

async fn setting(store: &dyn MetaStore, path: &str, v: Value) {
    put(
        store,
        path,
        json!({"v": 1, "value": v, "set_by": "op:xdc", "reason": "two_dc_bins"}),
    )
    .await;
}

fn tls() -> SslOpts {
    SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true)
}

async fn my(dc: &str, user: &str, pw: &str) -> Result<Conn, u16> {
    let o = OptsBuilder::default()
        .ip_or_hostname(H)
        .tcp_port(ports(dc).mysql)
        .user(Some(user))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(tls());
    match tokio::time::timeout(Duration::from_secs(15), Conn::new(o)).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(mysql_async::Error::Server(e))) => Err(e.code),
        Ok(Err(e)) => {
            eprintln!("login {user} at {dc}: {e}");
            Err(0)
        }
        Err(_) => {
            eprintln!("login {user} at {dc}: timeout");
            Err(0)
        }
    }
}

/// Runs `sqls` as `user` at `dc`; panics on an error (setup only). A
/// write right after start-up may meet 39501 until the gateway's first
/// DC permit arrives (renewed every second): retried for 30 s.
async fn run(dc: &str, user: &str, sqls: &[&str]) {
    let mut c = my(dc, user, APP_PW)
        .await
        .unwrap_or_else(|e| panic!("login {user} at {dc}: {e}"));
    for s in sqls {
        let t0 = Instant::now();
        loop {
            match c.query_drop(*s).await {
                Ok(()) => break,
                Err(mysql_async::Error::Server(e))
                    if e.code == 39501 && t0.elapsed() < Duration::from_secs(30) =>
                {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => panic!("{s} as {user} at {dc}: {e}"),
            }
        }
    }
    let _ = c.disconnect().await;
}

/// Column `col` of each row of `sql`, as text; the error's code.
async fn rows_col(dc: &str, user: &str, sql: &str, col: usize) -> Result<Vec<String>, u16> {
    let mut c = my(dc, user, APP_PW).await?;
    let r: Result<Vec<mysql_async::Row>, _> = c.query(sql).await;
    let _ = c.disconnect().await;
    match r {
        Ok(rs) => Ok(rs
            .into_iter()
            .map(|mut row| {
                row.take::<Option<String>, _>(col)
                    .flatten()
                    .unwrap_or_default()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err(e.code),
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// The first column of each row of `sql`.
async fn rows(dc: &str, user: &str, sql: &str) -> Result<Vec<String>, u16> {
    rows_col(dc, user, sql, 0).await
}

/// Every row of `sql`, each column as text; the error's code.
async fn table(dc: &str, user: &str, sql: &str) -> Result<Vec<Vec<String>>, u16> {
    let mut c = my(dc, user, APP_PW).await?;
    let r: Result<Vec<mysql_async::Row>, _> = c.query(sql).await;
    let _ = c.disconnect().await;
    match r {
        Ok(rs) => Ok(rs
            .into_iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| {
                        row.get_opt::<Option<String>, _>(i)
                            .and_then(Result::ok)
                            .flatten()
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err(e.code),
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// The notes `sql` left (`SHOW WARNINGS` after it), as `user` at `dc`.
async fn info(dc: &str, user: &str, sql: &str) -> Result<String, String> {
    let mut c = my(dc, user, APP_PW).await.map_err(|e| e.to_string())?;
    let r = c.query_drop(sql).await.map_err(|e| e.to_string());
    let notes: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap_or_default();
    let _ = c.disconnect().await;
    r.map(|()| {
        notes
            .into_iter()
            .map(|(_, _, m)| m)
            .collect::<Vec<_>>()
            .join("; ")
    })
}

fn now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

struct Key {
    kid: String,
    kp: EcdsaKeyPair,
}

impl Key {
    fn new(kid: &str) -> Self {
        let rng = SystemRandom::new();
        let a = &signature::ECDSA_P256_SHA256_FIXED_SIGNING;
        let der = EcdsaKeyPair::generate_pkcs8(a, &rng).unwrap();
        Self {
            kid: kid.into(),
            kp: EcdsaKeyPair::from_pkcs8(a, der.as_ref(), &rng).unwrap(),
        }
    }

    fn jwk(&self) -> Value {
        let p = self.kp.public_key().as_ref();
        json!({"kty": "EC", "crv": "P-256", "kid": self.kid, "use": "sig",
               "x": URL_SAFE_NO_PAD.encode(&p[1..33]), "y": URL_SAFE_NO_PAD.encode(&p[33..65])})
    }

    fn sign(&self, claims: &Value) -> String {
        let h = URL_SAFE_NO_PAD
            .encode(json!({"alg": "ES256", "kid": self.kid, "typ": "JWT"}).to_string());
        let p = URL_SAFE_NO_PAD.encode(claims.to_string());
        let msg = format!("{h}.{p}");
        let sig = self.kp.sign(&SystemRandom::new(), msg.as_bytes()).unwrap();
        format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
    }
}

fn claims(user: &str, org: &str) -> Value {
    let n = now();
    json!({"iss": ISS, "aud": AUD, "sub": format!("s-{user}"), "preferred_username": user,
           "org": org, "iat": n, "exp": n + 600})
}

/// The key set, served on 127.0.0.1 by the test.
async fn serve_jwks(key: &Key) -> String {
    let l = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = format!("http://{}/jwks", l.local_addr().unwrap());
    let body = json!({"keys": [key.jwk()]}).to_string();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    url
}

/// A webhook receiver that answers 200 to everything and counts requests.
async fn serve_hook() -> Arc<Mutex<usize>> {
    let hits = Arc::new(Mutex::new(0));
    let l = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, HOOK_PORT))
        .await
        .unwrap();
    let h = Arc::clone(&hits);
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let h = Arc::clone(&h);
            tokio::spawn(async move {
                let mut chunk = [0u8; 4096];
                loop {
                    match tokio::time::timeout(Duration::from_millis(500), s.read(&mut chunk)).await
                    {
                        Ok(Ok(n)) if n > 0 => {
                            if chunk[..n].windows(4).any(|w| w == b"HTTP") {
                                *h.lock().unwrap() += 1;
                                let _ = s
                                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                                    .await;
                            }
                        }
                        _ => return,
                    }
                }
            });
        }
    });
    hits
}

fn basic(user: &str, pw: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{pw}")))
}

async fn http_sql(
    dc: &str,
    auth: &str,
    tenant: Option<&str>,
    db: &str,
    sql: &str,
) -> (u16, String) {
    let mut hs = vec![("Authorization", auth)];
    if let Some(t) = tenant {
        hs.push(("x-sprig-tenant", t));
    }
    http(
        ports(dc).http,
        "POST",
        &format!("/v1/databases/{db}/sql:query"),
        &hs,
        &json!({"sql": sql}).to_string(),
    )
    .await
}

async fn mongo_ping(dc: &str, cred: mongodb::options::Credential) -> Result<(), String> {
    use mongodb::options::ClientOptions;
    let mut o = ClientOptions::parse(format!(
        "mongodb://{H}:{}/?directConnection=true&serverSelectionTimeoutMS=8000",
        ports(dc).mongo
    ))
    .await
    .map_err(|e| e.to_string())?;
    o.credential = Some(cred);
    let c = mongodb::Client::with_options(o).map_err(|e| e.to_string())?;
    let r = c
        .database("eu")
        .run_command(mongodb::bson::doc! {"ping": 1})
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    c.shutdown().await;
    r
}

async fn mongo_oidc(dc: &str, user: &str, token: String) -> Result<(), String> {
    use mongodb::options::{AuthMechanism, Credential, oidc};
    let cb = oidc::Callback::human(move |_ctx| {
        let token = token.clone();
        async move {
            Ok(oidc::IdpServerResponse::builder()
                .access_token(token)
                .build())
        }
        .boxed()
    });
    let mut cred = Credential::builder()
        .mechanism(AuthMechanism::MongoDbOidc)
        .username(user.to_owned())
        .build();
    cred.oidc_callback = cb;
    mongo_ping(dc, cred).await
}

async fn mongo_scram(dc: &str, user: &str, pw: &str) -> Result<(), String> {
    use mongodb::options::{AuthMechanism, Credential};
    let cred = Credential::builder()
        .mechanism(AuthMechanism::ScramSha256)
        .username(user.to_owned())
        .password(pw.to_owned())
        .source("admin".to_owned())
        .build();
    mongo_ping(dc, cred).await
}

/// A MongoDB client of `user` (SCRAM) at `dc`.
async fn mongo_client(dc: &str, user: &str) -> mongodb::Client {
    use mongodb::options::{AuthMechanism, ClientOptions, Credential};
    let mut o = ClientOptions::parse(format!(
        "mongodb://{H}:{}/?directConnection=true&serverSelectionTimeoutMS=8000",
        ports(dc).mongo
    ))
    .await
    .expect("mongo options");
    o.credential = Some(
        Credential::builder()
            .mechanism(AuthMechanism::ScramSha256)
            .username(user.to_owned())
            .password(APP_PW.to_owned())
            .source("admin".to_owned())
            .build(),
    );
    mongodb::Client::with_options(o).expect("mongo client")
}

/// The next change event of `s`, within `within`.
async fn next_change(
    s: &mut mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >,
    within: Duration,
) -> Result<mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>, String> {
    let t0 = Instant::now();
    while t0.elapsed() < within {
        match tokio::time::timeout(Duration::from_secs(2), s.next_if_any()).await {
            Ok(Ok(Some(ev))) => return Ok(ev),
            Ok(Ok(None)) | Err(_) => {}
            Ok(Err(e)) => return Err(e.to_string()),
        }
    }
    Err("no event".into())
}

type KafkaIo = tokio_rustls::client::TlsStream<TcpStream>;

/// A Kafka client of `user` at `dc` (TLS, SASL PLAIN).
async fn kafka(dc: &str, user: &str) -> sprig_proto_kafka::client::Client<KafkaIo> {
    use rustls::pki_types::pem::PemObject as _;
    let tcp = TcpStream::connect((H, ports(dc).kafka))
        .await
        .expect("kafka tcp");
    let mut roots = rustls::RootCertStore::empty();
    let ca = std::fs::read(sprig_testkit::compose::compose_dir().join("gateway-certs/ca.pem"))
        .expect("ca");
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(&ca) {
        roots.add(c.expect("ca pem")).expect("ca");
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("name");
    let s = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await
        .expect("kafka tls");
    let mut c = sprig_proto_kafka::client::Client::new(s, "two-dc");
    c.sasl_plain(user, APP_PW).await.expect("kafka login");
    c
}

/// One Fetch of `topic` partition 0 from `offset`, waiting up to
/// `wait_ms`: the records' values and the high watermark, or the error.
async fn kafka_fetch(
    c: &mut sprig_proto_kafka::client::Client<KafkaIo>,
    topic: &str,
    offset: i64,
    wait_ms: i32,
) -> Result<(Vec<String>, i64), String> {
    use sprig_proto_kafka::messages::{FetchRequest, FetchResponse, fetch_request};
    use sprig_proto_kafka::records::RecordBatch;
    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: wait_ms,
        min_bytes: 1,
        max_bytes: 1 << 20,
        isolation_level: 1,
        session_epoch: -1,
        topics: vec![fetch_request::FetchTopic {
            topic: topic.into(),
            partitions: vec![fetch_request::FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let r: FetchResponse = c.call(12, &req).await.map_err(|e| e.to_string())?;
    let p = r
        .responses
        .first()
        .and_then(|t| t.partitions.first())
        .ok_or("no partition")?;
    if p.error_code != 0 {
        return Err(format!("error {}", p.error_code));
    }
    let values = RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 26, true)
        .map_err(|e| e.to_string())?
        .into_iter()
        .flat_map(|b| b.records)
        .map(|r| String::from_utf8_lossy(&r.value.unwrap_or_default()).into_owned())
        .collect();
    Ok((values, p.high_watermark))
}

async fn redis_auth(dc: &str, user: &str, pass: &str) -> Result<String, String> {
    let c = redis::Client::open(format!("redis://{H}:{}/", ports(dc).redis)).unwrap();
    let mut con = tokio::time::timeout(
        Duration::from_secs(10),
        c.get_multiplexed_async_connection(),
    )
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
    redis::cmd("AUTH")
        .arg(user)
        .arg(pass)
        .query_async::<String>(&mut con)
        .await
        .map_err(|e| e.to_string())
}

fn controller_toml(dc: &str, work: &Path) -> String {
    let p = ports(dc);
    let seed = dc_seed(dc);
    let global = global_seeds();
    format!(
        r#"[node]
dc = "{dc}"
id = "ctl-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = [{global}]
deployment_id = "{DEPLOYMENT}"
[controller]
bootstrap = {boot}
global_candidate = {boot}
admin_addr = "127.0.0.1:{admin}"
internal_addr = "127.0.0.1:{internal}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
[audit]
spool_dir = "{w}/audit-{dc}"
sink_dir = "{w}/audit-sink-{dc}"
"#,
        boot = dc == "fra1",
        admin = p.admin,
        internal = p.internal,
        tel = p.ctl_tel,
        w = work.display()
    )
}

/// A worker: the mover's jobs of a DC move run on the target DC's.
fn worker_toml(dc: &str) -> String {
    let seed = dc_seed(dc);
    let global = global_seeds();
    format!(
        r#"[node]
dc = "{dc}"
id = "worker-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = [{global}]
deployment_id = "{DEPLOYMENT}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#,
        tel = ports(dc).wk_tel
    )
}

fn gateway_toml(dc: &str, backends: &Path) -> String {
    let p = ports(dc);
    let seed = dc_seed(dc);
    let global = global_seeds();
    format!(
        r#"[node]
dc = "{dc}"
id = "gw-{dc}"
[meta]
dc_seeds = ["{seed}"]
global_seeds = [{global}]
deployment_id = "{DEPLOYMENT}"
[gateway]
login_mode = "catalog"
backends_file = "{b}"
advertise_addr = "127.0.0.1"
[gateway.listen_override]
mysql = "127.0.0.1:{my}"
mariadb = "127.0.0.1:{ma}"
http = "127.0.0.1:{ht}"
https = "127.0.0.1:{hs}"
peer = "127.0.0.1:{pe}"
mongo = "127.0.0.1:{mo}"
redis = "127.0.0.1:{re}"
kafka = "127.0.0.1:{ka}"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:{tel}"
"#,
        b = backends.display(),
        my = p.mysql,
        ma = p.mariadb,
        ht = p.http,
        hs = p.https,
        pe = p.peer,
        mo = p.mongo,
        re = p.redis,
        ka = p.kafka,
        tel = p.gw_tel
    )
}

/// A private MariaDB for `dc`'s cluster.
fn mariadb(dc: &str, server_id: u32) -> OwnContainer {
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        sprig_testkit::compose::compose_dir()
            .join("mariadb/binlog.cnf")
            .display()
    );
    let publish = format!("127.0.0.1:{}:3306", ports(dc).db);
    let root_env = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let sid = format!("--server-id={server_id}");
    OwnContainer::run(
        &format!("xdc-{dc}db"),
        &[
            "--memory",
            "640m",
            "-p",
            &publish,
            "-e",
            &root_env,
            "-v",
            &binlog,
            "mariadb:11.8",
            &sid,
            "--innodb-buffer-pool-size=96M",
            "--max-connections=300",
        ],
    )
    .expect("mariadb")
}

async fn db_up(dc: &str) {
    let url = format!("mysql://root:{ROOT_PW}@{H}:{}", ports(dc).db);
    let t0 = Instant::now();
    loop {
        let o: mysql_async::Opts =
            OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
                .prefer_socket(false)
                .into();
        if let Ok(Ok(mut c)) =
            tokio::time::timeout(Duration::from_secs(3), mysql_async::Conn::new(o)).await
        {
            let one: Option<u8> = c.query_first("SELECT 1").await.ok().flatten();
            let _ = c.disconnect().await;
            if one == Some(1) {
                return;
            }
        }
        assert!(
            t0.elapsed() < Duration::from_secs(150),
            "{dc} mariadb never up"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The deployment: containers, processes, the two clusters' ids.
struct Deployment {
    store: Arc<dyn MetaStore>,
    /// Each DC's tier (the global store's etcd in single-etcd mode).
    dc_stores: [Arc<dyn MetaStore>; 2],
    sprigctl: PathBuf,
    work: PathBuf,
    clusters: [String; 2],
    // Dropped last: processes first, then the containers.
    _procs: Procs,
    _containers: Vec<OwnContainer>,
}

impl Deployment {
    fn cluster(&self, dc: &str) -> &str {
        &self.clusters[usize::from(dc != "fra1")]
    }

    fn dc_store(&self, dc: &str) -> &dyn MetaStore {
        &*self.dc_stores[usize::from(dc != "fra1")]
    }

    /// Creates `name` on `dc`'s cluster (under `parent`), with user `app`;
    /// checks its id is inside the range.
    async fn tenant(&self, dc: &str, name: &str, parent: Option<&str>) -> u64 {
        let mut args = vec!["tenant", "create", name, "--cluster", self.cluster(dc)];
        if let Some(p) = parent {
            args.extend(["--under", p]);
        }
        let v = ctl_retry(&self.sprigctl, dc, &args, Duration::from_secs(60)).await;
        let id = v["tenant"]["tenant_id"].as_u64().expect("tenant id");
        assert!(
            (TENANT_BASE..TENANT_BASE + 100_000).contains(&id),
            "tenant {name}: id {id} outside the range"
        );
        ctl_retry(
            &self.sprigctl,
            dc,
            &[
                "tenant",
                "user",
                "add",
                name,
                "app",
                "--password",
                APP_PW,
                "--if-not-exists",
            ],
            Duration::from_secs(30),
        )
        .await;
        let t0 = Instant::now();
        let user = format!("{name}.app");
        while my(dc, &user, APP_PW).await.is_err() {
            assert!(
                t0.elapsed() < Duration::from_secs(60),
                "{user} never logs in at {dc}"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let dir = get(&*self.store, &format!("/sprig/g/tenants/{id}"))
            .await
            .expect("entry");
        assert_eq!(dir["home_dc"], dc, "{name}'s home: {dir}");
        eprintln!("tenant {name} ({id}) homed in {dc}");
        id
    }

    fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.work.join(format!("{name}.log"))).unwrap_or_default()
    }
}

async fn deploy() -> Option<Deployment> {
    let (sprigdb, sprigctl) = bins()?;
    let work = std::env::temp_dir().join(format!("xdc-two-dc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).expect("work dir");
    let mut containers = Vec::new();

    // ---- one private etcd, advertising its own port ----
    let etcd_pub = format!("127.0.0.1:{ETCD_PORT}:2379");
    let advertise = format!("--advertise-client-urls=http://127.0.0.1:{ETCD_PORT}");
    containers.push(
        OwnContainer::run(
            "xdc-etcd",
            &[
                "--memory",
                "256m",
                "-p",
                &etcd_pub,
                "quay.io/coreos/etcd:v3.6.5",
                "etcd",
                "--listen-client-urls=http://0.0.0.0:2379",
                &advertise,
                "--auto-compaction-mode=periodic",
                "--auto-compaction-retention=1h",
            ],
        )
        .expect("etcd"),
    );
    if two_tier() {
        for (dc, port) in [("fra1", 33376), ("ams1", 33377)] {
            let publish = format!("127.0.0.1:{port}:2379");
            let adv = format!("--advertise-client-urls=http://127.0.0.1:{port}");
            containers.push(
                OwnContainer::run(
                    &format!("xdc-etcd-{dc}"),
                    &[
                        "--memory",
                        "256m",
                        "-p",
                        &publish,
                        "quay.io/coreos/etcd:v3.6.5",
                        "etcd",
                        "--listen-client-urls=http://0.0.0.0:2379",
                        &adv,
                        "--auto-compaction-mode=periodic",
                        "--auto-compaction-retention=1h",
                    ],
                )
                .expect("dc etcd"),
            );
        }
    }
    containers.push(mariadb("fra1", 331));
    containers.push(mariadb("ams1", 332));
    let connect = |tier: Tier, seed: String| async move {
        let mut cfg = EtcdConfig::new(vec![seed]);
        cfg.request_timeout = Duration::from_secs(2);
        let t0 = Instant::now();
        loop {
            match EtcdStore::connect(tier.clone(), &cfg).await {
                Ok(s) => break Arc::new(s) as Arc<dyn MetaStore>,
                Err(_) if t0.elapsed() < Duration::from_secs(60) => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Err(e) => panic!("etcd: {e}"),
            }
        }
    };
    let store = connect(Tier::Global, format!("http://127.0.0.1:{ETCD_PORT}")).await;
    let dc_stores: [Arc<dyn MetaStore>; 2] = if two_tier() {
        let dc = |d: &str| Tier::Dc(sprig_common::ids::DcId::new(d).expect("dc"));
        [
            connect(dc("fra1"), dc_seed("fra1")).await,
            connect(dc("ams1"), dc_seed("ams1")).await,
        ]
    } else {
        [Arc::clone(&store), Arc::clone(&store)]
    };
    db_up("fra1").await;
    db_up("ams1").await;

    // ---- settings an operator's bootstrap writes ----
    let certs = sprig_testkit::compose::compose_dir().join("gateway-certs");
    for d in DCS {
        for (k, v) in [
            (
                "tls.bundles.default.cert_path",
                json!(certs.join("server.pem").display().to_string()),
            ),
            (
                "tls.bundles.default.key_path",
                json!(certs.join("server-key.pem").display().to_string()),
            ),
            ("listener.mysql.tls", json!("optional")),
            ("listener.mariadb.tls", json!("optional")),
            ("listener.http.tls", json!("off")),
            ("listener.https.tls", json!("required")),
            ("listener.mongo.enabled", json!(true)),
            ("listener.mongo.tls", json!("off")),
            ("listener.redis.enabled", json!(true)),
            ("listener.redis.tls", json!("off")),
            ("listener.kafka.enabled", json!(true)),
            ("listener.kafka.tls", json!("optional")),
            // No standby copies in the other DC: a standby's job locks the
            // tenant, and the member's move would wait for it.
            ("failover.standby_dcs", json!(0)),
        ] {
            setting(
                &*dc_stores[usize::from(d != "fra1")],
                &format!("/sprig/dc/{d}/settings/{k}"),
                v,
            )
            .await;
        }
    }
    setting(
        &*store,
        "/sprig/g/settings/tenant.id_min",
        json!(TENANT_BASE),
    )
    .await;
    setting(
        &*store,
        "/sprig/g/settings/tenant.id_max",
        json!(TENANT_BASE + 99_999),
    )
    .await;
    let creds = work.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .expect("creds");
    let backends = work.join("backends.toml");
    std::fs::write(
        &backends,
        "[per_user]\nsecret = \"file:backends.toml#backend_secret\"\ntls = \"skip_verify\"\n\
         [backend_secret]\npassword = \"xdc-per-user-secret\"\n",
    )
    .expect("backends");

    let mut procs = Procs {
        bin: sprigdb,
        work: work.clone(),
        kids: Vec::new(),
    };
    procs.start("ctl-fra1", "controller", &controller_toml("fra1", &work));
    assert!(
        ready(ports("fra1").ctl_tel, Duration::from_secs(120)).await,
        "ctl-fra1 ready"
    );
    let mut fra1 = get(&*store, "/sprig/g/dcs/fra1").await.expect("dcs/fra1");
    fra1["tags"] = json!({"jurisdiction": "DE"});
    put(&*store, "/sprig/g/dcs/fra1", fra1).await;
    ctl_retry(
        &sprigctl,
        "fra1",
        &["dc", "register", "ams1", "--tags", "jurisdiction=NL"],
        Duration::from_secs(30),
    )
    .await;
    procs.start("ctl-ams1", "controller", &controller_toml("ams1", &work));
    assert!(
        ready(ports("ams1").ctl_tel, Duration::from_secs(120)).await,
        "ctl-ams1 ready"
    );
    let cred = format!("file:{}", creds.display());
    let mut clusters: [String; 2] = Default::default();
    for (i, dc) in DCS.iter().enumerate() {
        let node = format!("127.0.0.1:{}", ports(dc).db);
        let name = format!("{dc}-db");
        let v = ctl_retry(
            &sprigctl,
            dc,
            &[
                "cluster",
                "register",
                &name,
                "--topology",
                "single",
                "--node",
                &node,
                "--credentials",
                &cred,
            ],
            Duration::from_secs(60),
        )
        .await;
        v["cluster"]["cluster_id"]
            .as_str()
            .expect("cluster id")
            .clone_into(&mut clusters[i]);
    }
    for dc in DCS {
        procs.start(&format!("gw-{dc}"), "gateway", &gateway_toml(dc, &backends));
        procs.start(&format!("worker-{dc}"), "worker", &worker_toml(dc));
    }
    for dc in DCS {
        assert!(
            ready(ports(dc).gw_tel, Duration::from_secs(120)).await,
            "gw-{dc} ready"
        );
        assert!(
            ready(ports(dc).wk_tel, Duration::from_secs(120)).await,
            "worker-{dc} ready"
        );
    }
    eprintln!("clusters {clusters:?}");
    Some(Deployment {
        store,
        dc_stores,
        sprigctl,
        work,
        clusters,
        _procs: procs,
        _containers: containers,
    })
}

/// Part 1: logins and statements of a tenant pinned to `fra1` arriving at
/// `ams1`.
async fn residency(d: &Deployment, c: &mut Checks) {
    const T: &str = "xdc-eu";
    let id = d.tenant("fra1", T, None).await;
    let app = format!("{T}.app");
    let key = Key::new("k1");
    let jwks = serve_jwks(&key).await;
    run(
        "fra1",
        &app,
        &[
            "CREATE DATABASE eu",
            "CREATE TABLE eu.notes (id INT PRIMARY KEY, body VARCHAR(40))",
            "INSERT INTO eu.notes VALUES (1, 'from fra1')",
            "CREATE USER tok IDENTIFIED BY 'Tok-pass-1'",
            "GRANT SELECT ON eu.* TO tok",
            // A row policy on the token's claim: tok sees its org's rows.
            "CREATE TABLE eu.docs (id INT PRIMARY KEY, org VARCHAR(20))",
            "INSERT INTO eu.docs VALUES (1, 'o-7'), (2, 'o-7'), (3, 'o-8')",
            "CREATE POLICY byorg ON eu.docs USING (org = SPRIG_CLAIM('org'))",
            "ALTER TABLE eu.docs ENABLE ROW LEVEL SECURITY",
        ],
    )
    .await;
    let issuer = json!({
        "issuer": ISS, "jwks_url": jwks, "audiences": [AUD], "algorithms": ["ES256"],
        "client_id": "xdc-client", "max_lifetime_s": 3600,
        "mappings": [{"priority": 1, "kind": "user", "claim": "preferred_username",
                      "match_kind": "any", "target": "{preferred_username}"}]
    });
    let admin = ports("fra1").admin;
    let (st, b) = http(
        admin,
        "POST",
        &format!("/admin/v1/tenants/{T}/issuers"),
        &[],
        &issuer.to_string(),
    )
    .await;
    c.ok("residency setup: issuer created", st == 200, || {
        format!("{st} {b}")
    });
    let (st, b) = http(
        admin,
        "POST",
        &format!("/admin/v1/tenants/{T}/users:setTokenLogin"),
        &[],
        &json!({"user": "tok", "enabled": true}).to_string(),
    )
    .await;
    c.ok("residency setup: tok takes tokens", st == 200, || {
        format!("{st} {b}")
    });
    let (st, b) = http(
        ports("fra1").http,
        "POST",
        "/v1/api-keys",
        &[("Authorization", &basic(&app, APP_PW))],
        &json!({"name": "xdc"}).to_string(),
    )
    .await;
    let api_key = serde_json::from_str::<Value>(&b).unwrap_or_default()["key"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    c.ok(
        "residency setup: API key created at fra1",
        st == 200 && api_key.starts_with("sprig_"),
        || format!("{st} {b}"),
    );

    // Before the policy, ams1 decides its own logins.
    c.ok(
        "before the policy: ams1 accepts a MySQL login",
        my("ams1", &app, APP_PW).await.is_ok(),
        String::new,
    );

    // The policy.
    let policy = r#"jurisdiction = "DE""#;
    let applied = ctl(
        &d.sprigctl,
        "ams1",
        &["tenant", "residency", T, "--policy", policy, "--apply"],
    )
    .expect("apply");
    let dir = get(&*d.store, &format!("/sprig/g/tenants/{id}"))
        .await
        .expect("entry");
    c.ok(
        "policy applied: allowed_dcs = [fra1]",
        applied["applied"] == true && dir["residency"]["allowed_dcs"] == json!(["fra1"]),
        || format!("{applied} {dir}"),
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let ams1_mark = d.log("gw-ams1").len();
    let decided = r#"sprig_auth_handoffs_total{outcome="decided"}"#;
    let before = metric(ports("ams1").gw_tel, decided).await;
    let t = Instant::now();

    // MySQL password: handed off, then statements against fra1's cluster.
    match my("ams1", &app, APP_PW).await {
        Ok(mut s) => {
            let ms = t.elapsed().as_millis();
            let w = s
                .query_drop("INSERT INTO eu.notes VALUES (2, 'via ams1')")
                .await;
            let n: Option<u64> = s
                .query_first("SELECT COUNT(*) FROM eu.notes")
                .await
                .unwrap_or(None);
            let _ = s.disconnect().await;
            c.ok(
                &format!("ams1: MySQL login handed to fra1 ({ms} ms), INSERT and SELECT run"),
                w.is_ok() && n == Some(2),
                || format!("{w:?} {n:?}"),
            );
        }
        Err(e) => c.ok("ams1: MySQL login handed off", false, || e.to_string()),
    }
    let seen = rows("fra1", &app, "SELECT body FROM eu.notes WHERE id = 2").await;
    c.ok(
        "fra1 reads the row written through ams1",
        seen == Ok(vec!["via ams1".to_owned()]),
        || format!("{seen:?}"),
    );
    c.ok(
        "ams1: a wrong MySQL password gets fra1's 1045",
        my("ams1", &app, "wrong").await.err() == Some(1045),
        String::new,
    );

    // MySQL token: the JWT as the password, its claims kept (the row
    // policy on SPRIG_CLAIM('org') shows org o-7's 2 rows of 3).
    let tok = key.sign(&claims("tok", "o-7"));
    match my("ams1", &format!("{T}.tok"), &tok).await {
        Ok(mut s) => {
            let ids: Result<Vec<i32>, _> = s.query("SELECT id FROM eu.docs ORDER BY id").await;
            // The session functions in a statement of its own, bound as in
            // policies.
            let own: Result<Vec<(Option<String>, Option<String>)>, _> = s
                .query("SELECT SPRIG_CLAIM('org'), SPRIG_CLAIM('missing')")
                .await;
            let filtered: Result<Vec<i32>, _> = s
                .query("SELECT id FROM eu.notes WHERE SPRIG_CLAIM('org') = 'o-7' AND id = 1")
                .await;
            let _ = s.disconnect().await;
            c.ok(
                "ams1: MySQL token login handed off, its claim filters rows",
                ids.as_ref().is_ok_and(|v| v == &vec![1, 2]),
                || format!("{ids:?}"),
            );
            c.ok(
                "ams1: SELECT SPRIG_CLAIM('org') in a plain query gives the claim over MySQL",
                own.as_ref()
                    .is_ok_and(|v| v == &vec![(Some("o-7".to_owned()), None)])
                    && filtered.as_ref().is_ok_and(|v| v == &vec![1]),
                || format!("{own:?} {filtered:?}"),
            );
        }
        Err(e) => c.ok("ams1: MySQL token login", false, || e.to_string()),
    }

    // HTTP Basic, bearer token and API key.
    let (st, b) = http_sql(
        "ams1",
        &basic(&app, APP_PW),
        None,
        "eu",
        "SELECT body FROM notes WHERE id = 1",
    )
    .await;
    c.ok(
        "ams1: HTTP Basic handed off, statement runs",
        st == 200 && b.contains("from fra1"),
        || format!("{st} {b}"),
    );
    let (st, b) = http_sql(
        "ams1",
        &format!("Bearer {tok}"),
        Some(T),
        "eu",
        "SELECT GROUP_CONCAT(id ORDER BY id) AS ids FROM docs",
    )
    .await;
    c.ok(
        "ams1: HTTP bearer token handed off, its claim filters rows (1,2)",
        st == 200 && b.contains("\"1,2\""),
        || format!("{st} {b}"),
    );
    let (st, b) = http_sql(
        "ams1",
        &format!("Bearer {tok}"),
        Some(T),
        "eu",
        "SELECT SPRIG_CLAIM('org') AS org",
    )
    .await;
    c.ok(
        "ams1: SELECT SPRIG_CLAIM('org') in a plain query gives the claim over HTTP",
        st == 200 && b.contains("\"o-7\""),
        || format!("{st} {b}"),
    );
    let (st, b) = http_sql(
        "ams1",
        &format!("Bearer {api_key}"),
        None,
        "eu",
        "SELECT body FROM notes WHERE id = 2",
    )
    .await;
    c.ok(
        "ams1: API key handed off, statement runs",
        st == 200 && b.contains("via ams1"),
        || format!("{st} {b}"),
    );

    // MongoDB.
    let r = mongo_oidc("ams1", &format!("{T}.tok"), tok.clone()).await;
    c.ok("ams1: MONGODB-OIDC handed off", r.is_ok(), || {
        format!("{r:?}")
    });
    // SCRAM can't be handed off: refused, naming the DC that decides.
    let r = mongo_scram("ams1", &app, APP_PW).await;
    c.ok(
        "ams1: MongoDB SCRAM-SHA-256 refused with 39301 naming fra1",
        r.as_ref()
            .is_err_and(|e| e.contains("ER_SPRIG_AUTH_UNAVAILABLE") && e.contains("fra1")),
        || format!("{r:?}"),
    );
    // Redis: the password crosses, like HTTP Basic.
    let r = redis_auth("ams1", &app, APP_PW).await;
    c.ok("ams1: Redis AUTH handed off", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = redis_auth("ams1", &app, "wrong").await;
    c.ok(
        "ams1: a wrong Redis password is refused",
        r.is_err(),
        || format!("{r:?}"),
    );

    let after = metric(ports("ams1").gw_tel, decided).await;
    c.ok(
        "ams1 counts its decided hand-offs (9 or more)",
        after >= before + 9.0,
        || format!("{before} -> {after}"),
    );
    let fra1_log = d.log("gw-fra1");
    let ams1_log = d
        .log("gw-ams1")
        .get(ams1_mark..)
        .unwrap_or_default()
        .to_owned();
    for (mech, what) in [("jwt", "token logins"), ("api_key", "the API key")] {
        c.ok(
            &format!("fra1 audits the hand-off of {what}"),
            fra1_log
                .lines()
                .any(|l| l.contains("auth.handoff") && l.contains(mech)),
            String::new,
        );
    }
    c.ok(
        "ams1 decides none of the tenant's logins after the policy (no auth.login_succeeded there)",
        !ams1_log
            .lines()
            .any(|l| l.contains("auth.login_succeeded") && l.contains(T)),
        || {
            ams1_log
                .lines()
                .filter(|l| l.contains("auth.login_succeeded") && l.contains(T))
                .take(3)
                .collect::<Vec<_>>()
                .join("\n")
        },
    );

    // deny_remote_access: every path refused at ams1 with 39302.
    ctl(
        &d.sprigctl,
        "fra1",
        &[
            "tenant",
            "residency",
            T,
            "--deny-remote-access",
            "true",
            "--apply",
        ],
    )
    .expect("deny");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    c.ok(
        "deny: ams1 refuses the MySQL login with 39302",
        my("ams1", &app, APP_PW).await.err() == Some(39302),
        String::new,
    );
    c.ok(
        "deny: ams1 refuses the MySQL token login with 39302",
        my("ams1", &format!("{T}.tok"), &tok).await.err() == Some(39302),
        String::new,
    );
    for (what, auth, tenant) in [
        ("HTTP Basic", basic(&app, APP_PW), None),
        ("HTTP bearer token", format!("Bearer {tok}"), Some(T)),
        ("API key", format!("Bearer {api_key}"), None),
    ] {
        let (st, b) = http_sql("ams1", &auth, tenant, "eu", "SELECT 1").await;
        c.ok(
            &format!("deny: ams1 refuses the {what} with 39302"),
            st >= 400 && b.contains("39302"),
            || format!("{st} {b}"),
        );
    }
    let r = mongo_oidc("ams1", &format!("{T}.tok"), tok.clone()).await;
    c.ok("deny: ams1 refuses MONGODB-OIDC", r.is_err(), || {
        format!("{r:?}")
    });
    let r = mongo_scram("ams1", &app, APP_PW).await;
    c.ok(
        "deny: ams1 refuses MongoDB SCRAM with 39302",
        r.as_ref()
            .is_err_and(|e| e.contains("ER_SPRIG_REMOTE_ACCESS_DENIED")),
        || format!("{r:?}"),
    );
    let r = redis_auth("ams1", &app, APP_PW).await;
    c.ok("deny: ams1 refuses Redis AUTH", r.is_err(), || {
        format!("{r:?}")
    });
    c.ok(
        "deny: fra1 still accepts MySQL",
        my("fra1", &app, APP_PW).await.is_ok(),
        String::new,
    );
    let (st, b) = http_sql("fra1", &format!("Bearer {api_key}"), None, "eu", "SELECT 1").await;
    c.ok("deny: fra1 still accepts the API key", st == 200, || {
        format!("{st} {b}")
    });
    let r = mongo_scram("fra1", &app, APP_PW).await;
    c.ok("deny: fra1 still accepts MongoDB SCRAM", r.is_ok(), || {
        format!("{r:?}")
    });
}

/// Reads NDJSON or SSE lines of a change stream opened at `port`.
struct Stream {
    lines: tokio::io::Lines<tokio::io::BufReader<TcpStream>>,
    status: u16,
}

async fn open_stream(port: u16, auth: &str, db: &str, extra: &[(&str, &str)]) -> Stream {
    let mut h = String::new();
    for (k, v) in extra {
        let _ = write!(h, "{k}: {v}\r\n");
    }
    let req = format!(
        "GET /v1/databases/{db}/changes HTTP/1.1\r\nHost: {H}:{port}\r\nAuthorization: {auth}\r\n{h}\r\n"
    );
    let mut s = TcpStream::connect((H, port)).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    let mut lines = tokio::io::BufReader::new(s).lines();
    let first = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
        .await
        .ok()
        .and_then(Result::ok)
        .flatten()
        .unwrap_or_default();
    let status = first
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // The rest of the header.
    while let Ok(Ok(Some(l))) =
        tokio::time::timeout(Duration::from_secs(5), lines.next_line()).await
    {
        if l.is_empty() {
            break;
        }
    }
    Stream { lines, status }
}

impl Stream {
    /// Lines that look like events or errors (chunk sizes dropped), until
    /// `want` returns true or `within` passes.
    async fn until(&mut self, within: Duration, want: impl Fn(&[String]) -> bool) -> Vec<String> {
        let mut got = Vec::new();
        let t0 = Instant::now();
        while t0.elapsed() < within && !want(&got) {
            let left = within.saturating_sub(t0.elapsed());
            match tokio::time::timeout(left, self.lines.next_line()).await {
                Ok(Ok(Some(l))) => {
                    if l.contains('{') || l.starts_with("id:") || l.starts_with("event:") {
                        got.push(l);
                    }
                }
                _ => break,
            }
        }
        got
    }
}

fn token_of(line: &str) -> Option<String> {
    let j: Value = serde_json::from_str(line.trim_start_matches("data: ")).ok()?;
    j["resume_token"].as_str().map(str::to_owned)
}

/// Part 2: a change stream of a `fra1` tenant opened at `ams1`.
async fn change_streams(d: &Deployment, c: &mut Checks) {
    const T: &str = "xdc-cs";
    d.tenant("fra1", T, None).await;
    let app = format!("{T}.app");
    run(
        "fra1",
        &app,
        &[
            "CREATE DATABASE cs",
            "CREATE TABLE cs.items (id INT PRIMARY KEY, v VARCHAR(20))",
        ],
    )
    .await;
    let auth = basic(&app, APP_PW);
    let fwd = "sprig_api_change_streams_forwarded_total";
    let fwd_before = metric(ports("ams1").gw_tel, fwd).await;
    let mut s = open_stream(ports("ams1").http, &auth, "cs", &[]).await;
    c.ok(
        "ams1: the change stream opens (200)",
        s.status == 200,
        || format!("{}", s.status),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    run(
        "fra1",
        &app,
        &["INSERT INTO cs.items VALUES (1, 'one'), (2, 'two')"],
    )
    .await;
    let got = s
        .until(Duration::from_secs(20), |g| {
            g.iter().filter(|l| l.contains("\"insert\"")).count() >= 2
        })
        .await;
    let inserts = got.iter().filter(|l| l.contains("\"insert\"")).count();
    c.ok(
        "ams1: the stream relays both inserts made at fra1",
        inserts == 2,
        || format!("{got:?}"),
    );
    let last = got.iter().rev().find_map(|l| token_of(l));
    // Longer than the peer call timeout (5 s): heartbeats, then an event.
    tokio::time::sleep(Duration::from_secs(8)).await;
    run(
        "fra1",
        &app,
        &["UPDATE cs.items SET v = 'uno' WHERE id = 1"],
    )
    .await;
    let got = s
        .until(Duration::from_secs(20), |g| {
            g.iter().any(|l| l.contains("\"update\""))
        })
        .await;
    c.ok(
        "ams1: the relayed stream outlives the peer call timeout (an update 8 s later)",
        got.iter().any(|l| l.contains("\"update\"")),
        || format!("{got:?}"),
    );
    drop(s);
    let fwd_after = metric(ports("ams1").gw_tel, fwd).await;
    c.ok(
        "ams1 counts the forwarded stream",
        fwd_after > fwd_before,
        || format!("{fwd_before} -> {fwd_after}"),
    );
    // Resume as SSE from the token after the inserts: the update only.
    if let Some(tok) = last {
        let mut s = open_stream(
            ports("ams1").http,
            &auth,
            "cs",
            &[("Accept", "text/event-stream"), ("Last-Event-ID", &tok)],
        )
        .await;
        let got = s
            .until(Duration::from_secs(20), |g| {
                g.iter().any(|l| l.contains("\"update\""))
            })
            .await;
        c.ok(
            "ams1: an SSE stream resumes from the last token (the update, no insert again)",
            got.iter().any(|l| l.contains("\"update\""))
                && !got.iter().any(|l| l.contains("\"insert\"")),
            || format!("{got:?}"),
        );
    } else {
        c.ok(
            "ams1: a resume token came with the events",
            false,
            String::new,
        );
    }
    // A wrong password is refused by the home DC's gateway.
    let s = open_stream(ports("ams1").http, &basic(&app, "wrong"), "cs", &[]).await;
    c.ok("ams1: a wrong password gets 401", s.status == 401, || {
        format!("{}", s.status)
    });
}

/// Part 2, continued: a MongoDB change stream and Kafka fetches of a
/// `fra1` tenant at `ams1`. The stream's items come from a `fra1` gateway's
/// reader (`TailFeed`) and are rendered at `ams1`; a long-polling fetch at
/// `ams1` wakes on `fra1`'s watermarks.
async fn mongo_and_kafka(c: &mut Checks) {
    use mongodb::bson::{Document, doc};
    use mongodb::change_stream::event::OperationType;
    const T: &str = "xdc-cs";
    let app = format!("{T}.app");
    let relayed = "sprig_mongo_change_streams_relayed_total";
    let before = metric(ports("ams1").gw_tel, relayed).await;
    let home = mongo_client("fra1", &app).await;
    let away = mongo_client("ams1", &app).await;
    let at_home = home.database("mcs").collection::<Document>("c");
    let r = at_home.insert_one(doc! {"_id": 0, "a": "first"}).await;
    c.ok("fra1: a MongoDB insert", r.is_ok(), || format!("{r:?}"));
    let coll = away.database("mcs").collection::<Document>("c");
    match coll.watch().await {
        Ok(mut s) => {
            let _ = tokio::time::timeout(Duration::from_secs(2), s.next_if_any()).await;
            let _ = at_home
                .insert_one(doc! {"_id": 1, "a": "x", "n": 7_i64})
                .await;
            let ev = next_change(&mut s, Duration::from_secs(20)).await;
            c.ok(
                "ams1: a MongoDB change stream of a fra1 tenant gets fra1's insert, typed",
                ev.as_ref().is_ok_and(|e| {
                    e.operation_type == OperationType::Insert
                        && e.full_document
                            .as_ref()
                            .is_some_and(|d| d.get_i64("n") == Ok(7) && d.get_str("a") == Ok("x"))
                }),
                || format!("{ev:?}"),
            );
            let token = s.resume_token();
            drop(s);
            let _ = at_home
                .update_one(doc! {"_id": 1}, doc! {"$set": {"a": "y"}})
                .await;
            let resumed = match token {
                Some(t) => match coll.watch().resume_after(t).await {
                    Ok(mut s2) => next_change(&mut s2, Duration::from_secs(20)).await,
                    Err(e) => Err(e.to_string()),
                },
                None => Err("no resume token".into()),
            };
            c.ok(
                "ams1: the stream resumes from its token with the update only",
                resumed
                    .as_ref()
                    .is_ok_and(|e| e.operation_type == OperationType::Update),
                || format!("{resumed:?}"),
            );
        }
        Err(e) => c.ok("ams1: a MongoDB change stream opens", false, || {
            e.to_string()
        }),
    }
    home.shutdown().await;
    away.shutdown().await;
    c.ok(
        "ams1 counted relayed change feeds",
        metric(ports("ams1").gw_tel, relayed).await >= before + 2.0,
        String::new,
    );

    // Kafka: a stream of the tenant fetched at ams1.
    run("fra1", &app, &["CREATE STREAM cs.kev PARTITIONS 1"]).await;
    run(
        "fra1",
        &app,
        &["INSERT INTO cs.kev (key, value) VALUES ('k', 'one')"],
    )
    .await;
    let mut k = kafka("ams1", &app).await;
    let t0 = Instant::now();
    let mut first = kafka_fetch(&mut k, "cs.kev", 0, 1000).await;
    while t0.elapsed() < Duration::from_secs(20)
        && !first
            .as_ref()
            .is_ok_and(|(v, _)| v.iter().any(|x| x == "one"))
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
        first = kafka_fetch(&mut k, "cs.kev", 0, 1000).await;
    }
    c.ok(
        "ams1: a Kafka fetch of a fra1 tenant's stream returns its record",
        first
            .as_ref()
            .is_ok_and(|(v, _)| v.iter().any(|x| x == "one")),
        || format!("{first:?}"),
    );
    // At the end: fetches (each waiting up to kafka.fetch_max_wait_ms)
    // get fra1's next record.
    let hw = first.as_ref().map_or(1, |(_, hw)| *hw);
    run(
        "fra1",
        &app,
        &["INSERT INTO cs.kev (key, value) VALUES ('k', 'two')"],
    )
    .await;
    let published = Instant::now();
    let mut polled = kafka_fetch(&mut k, "cs.kev", hw, 5_000).await;
    while published.elapsed() < Duration::from_secs(15)
        && !polled
            .as_ref()
            .is_ok_and(|(v, _)| v.iter().any(|x| x == "two"))
    {
        polled = kafka_fetch(&mut k, "cs.kev", hw, 5_000).await;
    }
    eprintln!(
        "kafka at ams1 got fra1's next record {} ms after its insert",
        published.elapsed().as_millis()
    );
    c.ok(
        "ams1: a waiting Kafka fetch gets fra1's next record",
        polled
            .as_ref()
            .is_ok_and(|(v, _)| v.iter().any(|x| x == "two")),
        || format!("{polled:?}"),
    );
}

/// The member's sinks and connections of database `sw`.
async fn member_sinks(user: &str) -> (Result<Vec<String>, u16>, Result<Vec<String>, u16>) {
    (
        rows_col("ams1", user, "SHOW SINKS FROM sw", 1).await,
        rows_col("ams1", user, "SHOW CONNECTIONS FROM sw", 1).await,
    )
}

/// Part 3: a declarer in `fra1`, its member in `ams1`.
async fn inherited_sinks(d: &Deployment, c: &mut Checks) {
    const P: &str = "xdc-p";
    const M: &str = "xdc-m";
    let _hits = serve_hook().await;
    let pid = d.tenant("fra1", P, None).await;
    // A member homed in another DC than its declarer, created there
    // directly: the ams1 controller reads its ancestor's catalog through
    // fra1's map (bootstrap copies the ancestry, then the tenant is
    // active).
    let t0 = Instant::now();
    let mid = d.tenant("ams1", M, Some(P)).await;
    c.ok(
        "a child is created directly in ams1 under a parent homed in fra1",
        true,
        String::new,
    );
    eprintln!(
        "member {M} created in ams1 under {P} in {} ms",
        t0.elapsed().as_millis()
    );
    let allow = format!("[\"127.0.0.1:{HOOK_PORT}\"]");
    for (dc, t) in [("fra1", P), ("ams1", M)] {
        let scope = format!("tenant:{t}");
        ctl_retry(
            &d.sprigctl,
            dc,
            &[
                "settings",
                "set",
                "sink.destination_allowlist",
                &allow,
                "--scope",
                &scope,
                "--reason",
                "two_dc_bins",
            ],
            Duration::from_secs(30),
        )
        .await;
    }
    let (pa, ma) = (format!("{P}.app"), format!("{M}.app"));
    let hook = format!(
        "CREATE CONNECTION sw.hook FOR EACH TENANT TYPE WEBHOOK WITH \
         (url = 'http://127.0.0.1:{HOOK_PORT}', format = 'ndjson', signature = 'none')"
    );
    run(
        "fra1",
        &pa,
        &[
            "GRANT SPRIG_SINK_ADMIN ON *.* TO app",
            "CREATE DATABASE sw",
            "CREATE STREAM sw.ev FOR EACH TENANT PARTITIONS 1",
            &hook,
            "CREATE SINK sw.out FOR EACH TENANT FROM STREAM ev TO WEBHOOK CONNECTION hook",
        ],
    )
    .await;
    // The member's instance, on its first publish at its home.
    run(
        "ams1",
        &ma,
        &["INSERT INTO sw.ev (key, value) VALUES ('k', 'member')"],
    )
    .await;
    let t0 = Instant::now();
    let mut have = member_sinks(&ma).await;
    while t0.elapsed() < Duration::from_secs(20)
        && !have.0.as_ref().is_ok_and(|s| s.iter().any(|n| n == "out"))
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
        have = member_sinks(&ma).await;
    }
    c.ok(
        "member in ams1 has sink out and connection hook",
        have.0.as_ref().is_ok_and(|s| s.iter().any(|n| n == "out"))
            && have.1.as_ref().is_ok_and(|s| s.iter().any(|n| n == "hook")),
        || format!("{have:?}"),
    );
    let ams1_index = keys_under(d.dc_store("ams1"), &format!("/sprig/dc/ams1/sinks/{mid}/")).await;
    c.ok(
        "the member's sink is indexed in ams1's tier",
        !ams1_index.is_empty(),
        || format!("{ams1_index:?}"),
    );

    // A second sink of the declarer reaches the member in ams1.
    run(
        "fra1",
        &pa,
        &["CREATE SINK sw.out2 FOR EACH TENANT FROM STREAM ev TO WEBHOOK CONNECTION hook"],
    )
    .await;
    let t0 = Instant::now();
    let mut have = member_sinks(&ma).await;
    while t0.elapsed() < Duration::from_secs(10)
        && !have.0.as_ref().is_ok_and(|s| s.iter().any(|n| n == "out2"))
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
        have = member_sinks(&ma).await;
    }
    c.ok(
        "CREATE SINK at fra1 reaches the member in ams1 within 10 s",
        have.0.as_ref().is_ok_and(|s| s.iter().any(|n| n == "out2")),
        || format!("{have:?}"),
    );

    // DROP SINK at fra1: the member's sink goes at once; its connection
    // stays while out2 uses it.
    run("fra1", &pa, &["DROP SINK sw.out"]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let have = member_sinks(&ma).await;
    c.ok(
        "DROP SINK at fra1 removes the member's sink in ams1 within 2 s",
        have.0.as_ref().is_ok_and(|s| !s.iter().any(|n| n == "out")),
        || format!("{have:?}"),
    );
    run("fra1", &pa, &["DROP SINK sw.out2"]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let have = member_sinks(&ma).await;
    c.ok(
        "the last DROP SINK removes the member's sink and connection in ams1 within 2 s",
        have.0.as_ref().is_ok_and(Vec::is_empty) && have.1.as_ref().is_ok_and(Vec::is_empty),
        || format!("{have:?}"),
    );
    let ams1_index = keys_under(d.dc_store("ams1"), &format!("/sprig/dc/ams1/sinks/{mid}/")).await;
    c.ok(
        "the member's sink index in ams1 is gone",
        ams1_index.is_empty(),
        || format!("{ams1_index:?}"),
    );
    members_in_ams1(d, c, &pa, &ma, M).await;
    let fra1_log = d.log("gw-fra1");
    c.ok(
        "fra1 asked ams1 for its members (drop_sink and rollout)",
        fra1_log.lines().any(|l| {
            l.contains("FOR EACH TENANT change applied in another DC") && l.contains("drop_sink")
        }) && fra1_log.lines().any(|l| {
            l.contains("FOR EACH TENANT change applied in another DC") && l.contains("rollout")
        }),
        String::new,
    );
    eprintln!("declarer {pid}, member {mid}");
}

/// Part 4: a tenant with a job standby in `ams1` (its own
/// `failover.standby_dcs = 1`; the DCs' default is 0 here) moves to
/// `ams1`: the move drops the standby first instead of failing with 39131.
async fn standby_move(d: &Deployment, c: &mut Checks) {
    const T: &str = "xdc-sb";
    let id = d.tenant("fra1", T, None).await;
    let app = format!("{T}.app");
    run(
        "fra1",
        &app,
        &[
            "CREATE DATABASE sb",
            "CREATE TABLE sb.t (id INT PRIMARY KEY, v VARCHAR(20))",
            "INSERT INTO sb.t VALUES (1, 'before the move')",
        ],
    )
    .await;
    let scope = format!("tenant:{T}");
    ctl_retry(
        &d.sprigctl,
        "fra1",
        &[
            "settings",
            "set",
            "failover.standby_dcs",
            "1",
            "--scope",
            &scope,
            "--reason",
            "two_dc_bins",
        ],
        Duration::from_secs(30),
    )
    .await;
    let t0 = Instant::now();
    let mut entry = Value::Null;
    while t0.elapsed() < Duration::from_secs(120) {
        entry = get(&*d.store, &format!("/sprig/g/tenants/{id}"))
            .await
            .unwrap_or_default();
        if entry["standby_jobs"]["ams1"]["job"].is_string() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let job = entry["standby_jobs"]["ams1"]["job"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    c.ok(
        "the tenant gets a job standby in ams1",
        !job.is_empty(),
        || format!("{entry}"),
    );
    eprintln!(
        "standby job {job} in ams1 after {} ms",
        t0.elapsed().as_millis()
    );
    // Let it stream a little: its job holds the tenant's lock in ams1.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t0 = Instant::now();
    let r = ctl(
        &d.sprigctl,
        "fra1",
        &[
            "tenant",
            "move",
            T,
            "--to",
            "ams1",
            "--cluster",
            d.cluster("ams1"),
        ],
    );
    c.ok(
        "tenant move to the standby's DC starts (no 39131)",
        r.is_ok(),
        || format!("{r:?}"),
    );
    let mut moved = false;
    while r.is_ok() && t0.elapsed() < Duration::from_secs(240) {
        let e = get(&*d.store, &format!("/sprig/g/tenants/{id}"))
            .await
            .unwrap_or_default();
        if e["home_dc"] == "ams1" && e["state"] == "active" {
            moved = true;
            entry = e;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    c.ok("the tenant moves to ams1", moved, || format!("{entry}"));
    eprintln!("moved to ams1 in {} ms", t0.elapsed().as_millis());
    let was = entry["last_move"]["job"]
        .as_str()
        .or_else(|| entry["last_move"]["migration"]["job"].as_str())
        .unwrap_or_default();
    c.ok(
        "the move's job isn't the dropped standby's",
        moved && !was.is_empty() && was != job,
        || format!("{entry}"),
    );
    let t0 = Instant::now();
    let mut seen = rows("ams1", &app, "SELECT v FROM sb.t WHERE id = 1").await;
    while t0.elapsed() < Duration::from_secs(60) && seen != Ok(vec!["before the move".to_owned()]) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        seen = rows("ams1", &app, "SELECT v FROM sb.t WHERE id = 1").await;
    }
    c.ok(
        "the row written before the move reads back at ams1",
        seen == Ok(vec!["before the move".to_owned()]),
        || format!("{seen:?}"),
    );
}

/// Part 3, continued: `FOR EACH TENANT` changes beyond sinks reach the
/// member homed in `ams1` through `InheritedMembers`: `SHOW INHERITED`
/// counts its instance, an override `FOR TENANT` it applies there, `DROP
/// TRIGGER` stops its trigger instance, and a broadcast is delivered by a
/// gateway of `ams1`.
async fn members_in_ams1(d: &Deployment, c: &mut Checks, pa: &str, ma: &str, m: &str) {
    // The declarer's own count includes the member's stream instance in
    // ams1.
    let defs = table("fra1", pa, "SHOW INHERITED DEFINITIONS").await;
    c.ok(
        "SHOW INHERITED DEFINITIONS at fra1 counts the member's instance in ams1",
        defs.as_ref().is_ok_and(|v| {
            v.iter()
                .any(|r| r.get(2).is_some_and(|n| n == "ev") && r.get(6).is_some_and(|n| n == "1"))
        }),
        || format!("{defs:?}"),
    );
    // An override FOR TENANT the member applies in ams1.
    let r = info(
        "fra1",
        pa,
        &format!(
            "ALTER INHERITED STREAM sw.ev FOR TENANT `{m}` SPRIG SET (stream.retention_hours = 72)"
        ),
    )
    .await;
    c.ok(
        "ALTER ... FOR TENANT at fra1 is applied to the member's instance in ams1",
        r.as_ref()
            .is_ok_and(|m| m.contains("applied to 1 instance")),
        || format!("{r:?}"),
    );
    // A FOR EACH TENANT trigger: its member instance in ams1 is made by
    // the rollout or the member's next publish, and DROP TRIGGER at fra1
    // stops it there.
    run(
        "fra1",
        pa,
        &["CREATE TRIGGER sw.tg ON STREAM sw.ev FOR EACH TENANT LANGUAGE javascript AS $$\nexport default function (ctx) { return; }\n$$"],
    )
    .await;
    let t0 = Instant::now();
    let mut has = false;
    while !has && t0.elapsed() < Duration::from_secs(30) {
        // A publish while the rollout deploys the trigger leaves the
        // deploy to the rollout; it doesn't fail.
        run(
            "ams1",
            ma,
            &["INSERT INTO sw.ev (key, value) VALUES ('k', 'for the trigger')"],
        )
        .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let inst = table("ams1", ma, "SHOW INHERITED INSTANCES").await;
        has = inst.as_ref().is_ok_and(|v| {
            v.iter().any(|r| {
                r.get(2).is_some_and(|n| n == "tg") && r.get(5).is_some_and(|s| s == "active")
            })
        });
    }
    c.ok(
        "the member in ams1 has an instance of the FOR EACH TENANT trigger",
        has,
        String::new,
    );
    let r = info("fra1", pa, "DROP TRIGGER sw.tg").await;
    c.ok(
        "DROP TRIGGER at fra1 stops the member's trigger instance in ams1",
        r.as_ref()
            .is_ok_and(|m| m.contains("1 instance(s) removed")),
        || format!("{r:?}"),
    );
    // A broadcast reaches the member through a gateway of ams1.
    run(
        "fra1",
        pa,
        &["INSERT INTO sw.ev FOR ALL TENANTS (key, value) VALUES ('bk', 'to all')"],
    )
    .await;
    let q = "SELECT COUNT(*) FROM sw.ev WHERE CAST(value AS CHAR) = 'to all'";
    let t0 = Instant::now();
    let mut got = rows("ams1", ma, q).await;
    while t0.elapsed() < Duration::from_secs(30) && got != Ok(vec!["1".to_owned()]) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        got = rows("ams1", ma, q).await;
    }
    c.ok(
        "a broadcast at fra1 reaches the member in ams1 exactly once",
        got == Ok(vec!["1".to_owned()]),
        || format!("{got:?}"),
    );
    // An AMQP vhost: the member in ams1 makes its instances with a
    // publish of its own; a queue the declarer defines afterwards reaches
    // it through a gateway of ams1, before the member uses the vhost again.
    run(
        "fra1",
        pa,
        &[
            "CREATE DATABASE swq",
            "CREATE EXCHANGE swq.jobs TYPE topic FOR EACH TENANT",
            "CREATE QUEUE swq.first_q FOR EACH TENANT",
        ],
    )
    .await;
    run(
        "ams1",
        ma,
        &["CALL sprig.mq_publish('jobs', 'a.b', 'first', '{}', 0, 'swq')"],
    )
    .await;
    let active = |name: &'static str| async move {
        let t0 = Instant::now();
        loop {
            let inst = table("ams1", ma, "SHOW INHERITED INSTANCES").await;
            let has = inst.as_ref().is_ok_and(|v| {
                v.iter().any(|r| {
                    r.get(2).is_some_and(|n| n == name) && r.get(5).is_some_and(|s| s == "active")
                })
            });
            if has || t0.elapsed() > Duration::from_secs(30) {
                return (has, format!("{inst:?}"));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let (has, why) = active("first_q").await;
    c.ok(
        "the member's publish at ams1 made its vhost's instances",
        has,
        || why,
    );
    run("fra1", pa, &["CREATE QUEUE swq.later_q FOR EACH TENANT"]).await;
    let (has, why) = active("later_q").await;
    c.ok(
        "a queue defined at fra1 later is applied to the member's vhost in ams1",
        has,
        || why,
    );
    let log = d.log("gw-fra1");
    for op in [
        "count",
        "override",
        "drop_trigger",
        "broadcast",
        "mq_rollout",
    ] {
        c.ok(
            &format!("fra1 asked ams1 to apply {op}"),
            log.lines().any(|l| {
                l.contains("FOR EACH TENANT change applied in another DC") && l.contains(op)
            }),
            String::new,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bins_two_data_centres() {
    if skip::var(skip::DOCKER).is_none() {
        skip::report_missing(skip::DOCKER, "two_dc_bins");
        return;
    }
    let Some(d) = deploy().await else {
        eprintln!("two_dc_bins: build sprigdb and sprigctl first");
        return;
    };
    let mut c = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let only = std::env::var("XDC_ONLY").unwrap_or_default();
    if only.is_empty() || only.contains("residency") {
        residency(&d, &mut c).await;
    }
    if only.is_empty() || only.contains("streams") {
        change_streams(&d, &mut c).await;
        mongo_and_kafka(&mut c).await;
    }
    if only.is_empty() || only.contains("sinks") {
        inherited_sinks(&d, &mut c).await;
    }
    if only.is_empty() || only.contains("standby") {
        standby_move(&d, &mut c).await;
    }
    eprintln!("two_dc_bins: {} checks, {} failed", c.n, c.failed.len());
    let work = d.work.clone();
    drop(d);
    if c.failed.is_empty() {
        let _ = std::fs::remove_dir_all(&work);
    } else {
        eprintln!("logs kept in {}", work.display());
    }
    assert!(c.failed.is_empty(), "{:#?}", c.failed);
}
