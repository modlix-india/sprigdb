//! Live checks of logins with an issuer's JWT (chapter 14's external
//! tokens), against a running private deployment, with a key set the test
//! serves itself on 127.0.0.1 (no identity provider, nothing downloaded):
//!
//! - an issuer and its claim mapping through the Admin API, and an
//!   account's `token` method;
//! - a valid token logs in over MySQL, MariaDB, HTTP, Redis and MongoDB
//!   (`MONGODB-OIDC`'s human flow); expired, wrong-audience, wrong-issuer
//!   and bad-signature tokens are refused (1045, 401, `WRONGPASS`), each
//!   with its reason in the gateway's audit log;
//! - a row policy on `SPRIG_CLAIM('org')` filters rows by the token's claim;
//! - a just-in-time account over HTTP (`auth.user_created_jit`);
//! - token roles (chapter 29): a role mapping on the `groups` claim gives
//!   the session role `ajgate`, the only way `tok` reads table `gated`;
//!   without the claim, or with a password, 1142; a mapped role the tenant
//!   doesn't define is ignored with `auth.token_roles_ignored`, never
//!   created; `SET ROLE NONE`, `DEFAULT` and the role by name; MySQL,
//!   MariaDB, HTTP and `COM_CHANGE_USER`;
//! - key rotation: a token signed with a new key is accepted after the
//!   gateway refetches the key set (at most once a minute).
//!
//! ```sh
//! source target/agent-authjwt/env.sh   # AJ_MYSQL, AJ_MARIADB, AJ_HTTP, AJ_REDIS, AJ_MONGO, AJ_ADMIN, AJ_LOG
//! cargo test -p sprig-gateway --test authjwt_live -- --nocapture --test-threads 1
//! ```
//!
//! Tenant `acme` with `app` (password `sprig-dev-app`), as gateway-dev.sh
//! makes it. Skipped without `AJ_MYSQL`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    missing_docs
)]

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, KeyPair};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const H: &str = "127.0.0.1";
const PW: &str = "sprig-dev-app";
const ISS: &str = "https://idp.authjwt.test";
const AUD: &str = "authjwt-app";
const DB: &str = "ajdb";

fn var(v: &str) -> Option<String> {
    std::env::var(v).ok().filter(|s| !s.is_empty())
}

fn port(v: &str) -> u16 {
    var(v).unwrap_or_else(|| panic!("{v}")).parse().unwrap()
}

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

fn now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

fn claims(user: &str, org: &str) -> Value {
    let n = now();
    json!({"iss": ISS, "aud": AUD, "sub": format!("s-{user}"), "preferred_username": user,
           "org": org, "iat": n, "exp": n + 600})
}

/// [`claims`] with a `groups` claim.
fn claims_in(user: &str, groups: &[&str]) -> Value {
    let mut c = claims(user, "o-1");
    c["groups"] = json!(groups);
    c
}

/// The rows of `gated` the session reads, or the error's code.
async fn gated(s: &mut Conn) -> Result<usize, u16> {
    s.query::<i32, _>(format!("SELECT id FROM {DB}.gated"))
        .await
        .map(|v| v.len())
        .map_err(|e| code(&e))
}

/// The key set, served on 127.0.0.1 by the test.
struct Idp {
    url: String,
    jwks: Arc<Mutex<String>>,
    hits: Arc<Mutex<usize>>,
}

impl Idp {
    async fn start(keys: &[&Key]) -> Self {
        let l = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let url = format!("http://{}/jwks", l.local_addr().unwrap());
        let jwks = Arc::new(Mutex::new(String::new()));
        let hits = Arc::new(Mutex::new(0));
        let (j, h) = (Arc::clone(&jwks), Arc::clone(&hits));
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let (j, h) = (Arc::clone(&j), Arc::clone(&h));
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    *h.lock().unwrap() += 1;
                    let body = j.lock().unwrap().clone();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        let idp = Self { url, jwks, hits };
        idp.set(keys);
        idp
    }

    fn set(&self, keys: &[&Key]) {
        *self.jwks.lock().unwrap() =
            json!({"keys": keys.iter().map(|k| k.jwk()).collect::<Vec<_>>()}).to_string();
    }

    fn hits(&self) -> usize {
        *self.hits.lock().unwrap()
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

async fn http_sql(token: &str, tenant: Option<&str>, sql: &str) -> (u16, String) {
    let auth = format!("Bearer {token}");
    let mut hs = vec![("Authorization", auth.as_str())];
    if let Some(t) = tenant {
        hs.push(("x-sprig-tenant", t));
    }
    http(
        port("AJ_HTTP"),
        "POST",
        &format!("/v1/databases/{DB}/sql:query"),
        &hs,
        &json!({"sql": sql}).to_string(),
    )
    .await
}

async fn my(port: u16, user: &str, pass: &str) -> mysql_async::Result<Conn> {
    let o = OptsBuilder::default()
        .ip_or_hostname(H)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
        .prefer_socket(false);
    Conn::new(o).await
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        _ => 0,
    }
}

fn log_has(needle: &[&str]) -> bool {
    let Some(p) = var("AJ_LOG") else {
        return true;
    };
    std::fs::read_to_string(p)
        .unwrap_or_default()
        .lines()
        .any(|l| needle.iter().all(|n| l.contains(n)))
}

async fn redis_auth(user: &str, pass: &str) -> Result<String, String> {
    let c = redis::Client::open(format!("redis://{H}:{}/", port("AJ_REDIS"))).unwrap();
    let mut con = c
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| e.to_string())?;
    redis::cmd("AUTH")
        .arg(user)
        .arg(pass)
        .query_async::<String>(&mut con)
        .await
        .map_err(|e| e.to_string())
}

async fn mongo_oidc(user: &str, token: String) -> Result<(), String> {
    use mongodb::options::{AuthMechanism, ClientOptions, Credential, oidc};
    let mut o = ClientOptions::parse(format!(
        "mongodb://{H}:{}/?directConnection=true&serverSelectionTimeoutMS=5000",
        port("AJ_MONGO")
    ))
    .await
    .map_err(|e| e.to_string())?;
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
    o.credential = Some(cred);
    let c = mongodb::Client::with_options(o).map_err(|e| e.to_string())?;
    c.database("ajdb")
        .run_command(mongodb::bson::doc! {"ping": 1})
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

use futures::FutureExt as _;

#[tokio::test]
async fn jwt_logins_live() {
    if var("AJ_MYSQL").is_none() {
        eprintln!("AJ_MYSQL not set: skipped");
        return;
    }
    let (mysql, mariadb, admin) = (port("AJ_MYSQL"), port("AJ_MARIADB"), port("AJ_ADMIN"));
    let mut c = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let (k1, k2, rogue) = (Key::new("k1"), Key::new("k2"), Key::new("k1"));
    let idp = Idp::start(&[&k1]).await;

    // Setup as acme.app: a table, a row policy on the token's org claim,
    // and the account `tok` that logs in with tokens.
    let mut app = my(mysql, "acme.app", PW).await.unwrap();
    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.docs (id INT PRIMARY KEY, org VARCHAR(20), body VARCHAR(50))"),
        format!("INSERT INTO {DB}.docs VALUES (1, 'o-1', 'a'), (2, 'o-1', 'b'), (3, 'o-2', 'c')"),
        "DROP USER IF EXISTS tok".into(),
        "CREATE USER tok IDENTIFIED BY 'Tok-pass-1'".into(),
        // tok reads docs; gated only through the token role ajgate.
        format!("GRANT SELECT ON {DB}.docs TO tok"),
        format!("CREATE TABLE {DB}.gated (id INT PRIMARY KEY)"),
        format!("INSERT INTO {DB}.gated VALUES (1), (2)"),
        "DROP ROLE IF EXISTS ajgate".into(),
        "DROP ROLE IF EXISTS ajghost".into(),
        "CREATE ROLE ajgate".into(),
        format!("GRANT SELECT ON {DB}.gated TO ajgate"),
        format!("CREATE POLICY byorg ON {DB}.docs USING (org = SPRIG_CLAIM('org'))"),
        format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
    ] {
        app.query_drop(&s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let issuer = json!({
        "issuer": ISS, "jwks_url": idp.url, "audiences": [AUD], "algorithms": ["ES256"],
        "jit_users": true, "max_lifetime_s": 3600,
        "mappings": [{"priority": 1, "kind": "user", "claim": "preferred_username",
                      "match_kind": "any", "target": "{preferred_username}"},
                     {"priority": 2, "kind": "role", "claim": "groups",
                      "match_kind": "equals", "match_value": "gate", "target": "ajgate"},
                     {"priority": 3, "kind": "role", "claim": "groups",
                      "match_kind": "equals", "match_value": "ghost", "target": "ajghost"}]
    });
    let (st, b) = http(
        admin,
        "POST",
        "/admin/v1/tenants/acme/issuers",
        &[],
        &issuer.to_string(),
    )
    .await;
    c.ok("admin: issuer created", st == 200, || format!("{st} {b}"));
    let (st, b) = http(
        admin,
        "POST",
        "/admin/v1/tenants/acme/users:setTokenLogin",
        &[],
        &json!({"user": "tok", "enabled": true}).to_string(),
    )
    .await;
    c.ok("admin: tok takes tokens", st == 200, || format!("{st} {b}"));
    let (st, b) = http(admin, "GET", "/admin/v1/tenants/acme/issuers", &[], "").await;
    c.ok("admin: issuer listed", st == 200 && b.contains(ISS), || {
        format!("{st} {b}")
    });
    tokio::time::sleep(Duration::from_secs(2)).await;

    // MySQL: the token as the password; the policy reads the token's org.
    let t1 = k1.sign(&claims("tok", "o-1"));
    match my(mysql, "acme.tok", &t1).await {
        Ok(mut s) => {
            let n: Option<u64> = s
                .query_first(format!("SELECT COUNT(*) FROM {DB}.docs"))
                .await
                .unwrap();
            c.ok(
                "mysql: token login, policy shows org o-1's 2 rows",
                n == Some(2),
                || format!("{n:?}"),
            );
        }
        Err(e) => c.ok("mysql: token login", false, || e.to_string()),
    }
    let t2 = k1.sign(&claims("tok", "o-2"));
    match my(mysql, "acme.tok", &t2).await {
        Ok(mut s) => {
            let ids: Vec<i32> = s.query(format!("SELECT id FROM {DB}.docs")).await.unwrap();
            c.ok(
                "mysql: org o-2's token sees row 3 only",
                ids == vec![3],
                || format!("{ids:?}"),
            );
        }
        Err(e) => c.ok("mysql: second token", false, || e.to_string()),
    }
    match my(mariadb, "acme.tok", &t1).await {
        Ok(mut s) => {
            let n: Option<u64> = s
                .query_first(format!("SELECT COUNT(*) FROM {DB}.docs"))
                .await
                .unwrap();
            c.ok("mariadb: token login, 2 rows", n == Some(2), || {
                format!("{n:?}")
            });
        }
        Err(e) => c.ok("mariadb: token login", false, || e.to_string()),
    }
    // The password still works, and sees no rows (no claims).
    match my(mysql, "acme.tok", "Tok-pass-1").await {
        Ok(mut s) => {
            let n: Option<u64> = s
                .query_first(format!("SELECT COUNT(*) FROM {DB}.docs"))
                .await
                .unwrap();
            c.ok(
                "mysql: password login has no claims, 0 rows",
                n == Some(0),
                || format!("{n:?}"),
            );
        }
        Err(e) => c.ok("mysql: password login", false, || e.to_string()),
    }

    // The refusals, each with its reason in the audit log.
    let n = now();
    let mut expired = claims("tok", "o-1");
    expired["iat"] = json!(n - 4000);
    expired["exp"] = json!(n - 3600);
    let mut aud = claims("tok", "o-1");
    aud["aud"] = json!("someone-else");
    let mut iss = claims("tok", "o-1");
    iss["iss"] = json!("https://evil.test");
    let bad: Vec<(&str, String, &str)> = vec![
        ("expired", k1.sign(&expired), "token: expired"),
        ("wrong audience", k1.sign(&aud), "token: wrong audience"),
        ("wrong issuer", k1.sign(&iss), "token: unknown issuer"),
        (
            "bad signature",
            rogue.sign(&claims("tok", "o-1")),
            "token: bad signature",
        ),
        (
            "another account's token",
            k1.sign(&claims("app", "o-1")),
            "token: no claim mapping names this account",
        ),
    ];
    for (what, tok, detail) in &bad {
        let r = my(mysql, "acme.tok", tok).await;
        c.ok(
            &format!("mysql: {what} refused with 1045"),
            r.as_ref().err().map(code) == Some(1045),
            || format!("{:?}", r.as_ref().map(|_| ())),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        c.ok(
            &format!("audit: {what} names its reason"),
            log_has(&["auth.login_failed", "\"jwt\"", detail]),
            || (*detail).to_owned(),
        );
    }
    c.ok(
        "audit: a token login succeeded",
        log_has(&["auth.login_succeeded", "\"jwt\"", ISS]),
        String::new,
    );

    // HTTP bearer tokens.
    let (st, b) = http_sql(
        &t1,
        Some("acme"),
        &format!("SELECT COUNT(*) AS n FROM {DB}.docs"),
    )
    .await;
    c.ok(
        "http: bearer token, 2 rows",
        st == 200 && b.contains('2'),
        || format!("{st} {b}"),
    );
    let (st, b) = http_sql(&t2, Some("acme"), &format!("SELECT id FROM {DB}.docs")).await;
    c.ok(
        "http: org o-2's token sees row 3 only",
        st == 200 && b.contains('3') && !b.contains("\"1\""),
        || format!("{st} {b}"),
    );
    for (what, tok, _) in &bad {
        let (st, b) = http_sql(tok, Some("acme"), "SELECT 1").await;
        c.ok(
            &format!("http: {what} refused with 401"),
            st == 401 && b.contains("1045"),
            || format!("{st} {b}"),
        );
    }
    let (st, b) = http_sql(&t1, None, "SELECT 1").await;
    c.ok(
        "http: no tenant named: 401 with 39786",
        st == 401 && b.contains("39786"),
        || format!("{st} {b}"),
    );
    // Just in time: an unknown user mapped by the token is created.
    let (st, b) = http_sql(
        &k1.sign(&claims("ajnew", "o-9")),
        Some("acme"),
        "SELECT CURRENT_USER() AS u",
    )
    .await;
    c.ok(
        "http: just-in-time account logs in",
        st == 200 && b.contains("ajnew"),
        || format!("{st} {b}"),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.ok(
        "audit: auth.user_created_jit",
        log_has(&["auth.user_created_jit", "ajnew"]),
        String::new,
    );

    // Redis AUTH.
    let r = redis_auth("acme.tok", &t1).await;
    c.ok("redis: AUTH with a token", r.as_deref() == Ok("OK"), || {
        format!("{r:?}")
    });
    let r = redis_auth("acme.tok", &bad[0].1).await;
    c.ok(
        "redis: expired token WRONGPASS",
        r.as_ref().is_err_and(|e| e.contains("WRONGPASS")),
        || format!("{r:?}"),
    );

    // MongoDB: MONGODB-OIDC's human flow (IdpInfo, then the token).
    let r = mongo_oidc("acme.tok", t1.clone()).await;
    c.ok("mongo: MONGODB-OIDC login", r.is_ok(), || format!("{r:?}"));
    let r = mongo_oidc("acme.tok", bad[1].1.clone()).await;
    c.ok("mongo: wrong audience refused", r.is_err(), || {
        format!("{r:?}")
    });

    // Token roles (chapter 29): the claim gives ajgate for the session.
    let with = k1.sign(&claims_in("tok", &["gate"]));
    let without = k1.sign(&claims_in("tok", &["staff"]));
    let ghost = k1.sign(&claims_in("tok", &["gate", "ghost"]));
    for (proto, p) in [("mysql", mysql), ("mariadb", mariadb)] {
        match my(p, "acme.tok", &with).await {
            Ok(mut s) => {
                let r = gated(&mut s).await;
                c.ok(
                    &format!("{proto}: the mapped role reads the gated table"),
                    r == Ok(2),
                    || format!("{r:?}"),
                );
                let role: Option<String> = s.query_first("SELECT CURRENT_ROLE()").await.unwrap();
                c.ok(
                    &format!("{proto}: CURRENT_ROLE() names ajgate"),
                    role.as_deref().is_some_and(|r| r.contains("ajgate")),
                    || format!("{role:?}"),
                );
                if proto == "mysql" {
                    s.query_drop("SET ROLE NONE").await.unwrap();
                    let r = gated(&mut s).await;
                    c.ok(
                        "mysql: SET ROLE NONE drops it: 1142",
                        r == Err(1142),
                        || format!("{r:?}"),
                    );
                    s.query_drop("SET ROLE DEFAULT").await.unwrap();
                    let r = gated(&mut s).await;
                    c.ok("mysql: SET ROLE DEFAULT includes it", r == Ok(2), || {
                        format!("{r:?}")
                    });
                    s.query_drop("SET ROLE NONE").await.unwrap();
                    let r = s.query_drop("SET ROLE ajgate").await;
                    let g = gated(&mut s).await;
                    c.ok("mysql: SET ROLE names it", r.is_ok() && g == Ok(2), || {
                        format!("{r:?} {g:?}")
                    });
                }
            }
            Err(e) => c.ok(&format!("{proto}: token with the role"), false, || {
                e.to_string()
            }),
        }
        match my(p, "acme.tok", &without).await {
            Ok(mut s) => {
                let r = gated(&mut s).await;
                c.ok(
                    &format!("{proto}: without the claim: 1142"),
                    r == Err(1142),
                    || format!("{r:?}"),
                );
                let r = s.query_drop("SET ROLE ajgate").await;
                c.ok(
                    &format!("{proto}: without the claim SET ROLE ajgate is refused"),
                    r.is_err(),
                    String::new,
                );
            }
            Err(e) => c.ok(&format!("{proto}: token without the role"), false, || {
                e.to_string()
            }),
        }
    }
    match my(mysql, "acme.tok", "Tok-pass-1").await {
        Ok(mut s) => {
            let r = gated(&mut s).await;
            c.ok("mysql: password login: 1142", r == Err(1142), || {
                format!("{r:?}")
            });
            // COM_CHANGE_USER to a token with the claim, then without.
            let r = s
                .change_user(
                    mysql_async::ChangeUserOpts::default()
                        .with_user(Some("acme.tok".into()))
                        .with_pass(Some(with.clone())),
                )
                .await;
            let g = gated(&mut s).await;
            c.ok(
                "change_user: to a token with the claim reads gated",
                r.is_ok() && g == Ok(2),
                || format!("{r:?} {g:?}"),
            );
            let r = s
                .change_user(
                    mysql_async::ChangeUserOpts::default()
                        .with_user(Some("acme.tok".into()))
                        .with_pass(Some(without.clone())),
                )
                .await;
            let g = gated(&mut s).await;
            c.ok(
                "change_user: to a token without it: 1142",
                r.is_ok() && g == Err(1142),
                || format!("{r:?} {g:?}"),
            );
        }
        Err(e) => c.ok("mysql: password login for change_user", false, || {
            e.to_string()
        }),
    }
    let (st, b) = http_sql(&with, Some("acme"), &format!("SELECT id FROM {DB}.gated")).await;
    c.ok("http: the mapped role reads gated", st == 200, || {
        format!("{st} {b}")
    });
    let (st, b) = http_sql(
        &without,
        Some("acme"),
        &format!("SELECT id FROM {DB}.gated"),
    )
    .await;
    c.ok(
        "http: without the claim: 1142",
        st != 200 && b.contains("1142"),
        || format!("{st} {b}"),
    );
    // A mapped role the tenant doesn't define: ignored, audited, not made.
    match my(mysql, "acme.tok", &ghost).await {
        Ok(mut s) => {
            let r = gated(&mut s).await;
            c.ok(
                "undefined role: the login works, ajgate still applies",
                r == Ok(2),
                || format!("{r:?}"),
            );
            let role: Option<String> = s.query_first("SELECT CURRENT_ROLE()").await.unwrap();
            c.ok(
                "undefined role: not active",
                role.as_deref().is_some_and(|r| !r.contains("ajghost")),
                || format!("{role:?}"),
            );
        }
        Err(e) => c.ok("undefined role: login", false, || e.to_string()),
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.ok(
        "audit: auth.token_roles_ignored names ajghost",
        log_has(&["auth.token_roles_ignored", "ajghost"]),
        String::new,
    );
    c.ok(
        "audit: the login names its token role",
        log_has(&["auth.login_succeeded", "token roles ajgate"]),
        String::new,
    );
    let r = app.query_drop("CREATE ROLE ajghost").await;
    c.ok(
        "undefined role: never created (CREATE ROLE succeeds)",
        r.is_ok(),
        || format!("{r:?}"),
    );
    let _ = app.query_drop("DROP ROLE IF EXISTS ajghost").await;

    // Key rotation: the issuer adds k2. A k2 token is accepted once the
    // gateway refetches (an unknown kid refetches at most once a minute).
    let before = idp.hits();
    idp.set(&[&k1, &k2]);
    eprintln!("waiting 61 s for the refetch limit");
    tokio::time::sleep(Duration::from_secs(61)).await;
    let r = my(mysql, "acme.tok", &k2.sign(&claims("tok", "o-1"))).await;
    c.ok("rotation: a new key's token logs in", r.is_ok(), || {
        format!("{:?}", r.as_ref().err())
    });
    c.ok("rotation: one refetch", idp.hits() == before + 1, || {
        format!("{} -> {}", before, idp.hits())
    });
    // The rogue key reuses kid k1: still a bad signature.
    let r = my(mysql, "acme.tok", &rogue.sign(&claims("tok", "o-1"))).await;
    c.ok(
        "rotation: a rogue key with a known kid refused",
        r.is_err(),
        String::new,
    );

    // Cleanup: what this test made.
    let (st, _) = http(admin, "POST", "/admin/v1/tenants/acme/issuers", &[], &{
        let mut i = issuer.clone();
        i["active"] = json!(false);
        i.to_string()
    })
    .await;
    c.ok("admin: issuer disabled", st == 200, String::new);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let r = my(mysql, "acme.tok", &k1.sign(&claims("tok", "o-1"))).await;
    c.ok(
        "disabled issuer: refused",
        r.as_ref().err().map(code) == Some(1045),
        || format!("{:?}", r.as_ref().err()),
    );
    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS tok".into(),
        "DROP USER IF EXISTS ajnew".into(),
        "DROP ROLE IF EXISTS ajgate".into(),
        "DROP ROLE IF EXISTS ajghost".into(),
    ] {
        let _ = app.query_drop(&s).await;
    }
    eprintln!("jwt_logins_live: {} checks, {} failed", c.n, c.failed.len());
    assert!(c.failed.is_empty(), "{:#?}", c.failed);
}
