//! A Studio cursor opened with an issuer's bearer token on gateway A and
//! fetched through gateway B: B forwards the call to A over the peer
//! service, naming the tenant (the token's tenant came from the tenant
//! header, which the forwarded call doesn't carry), so A finds the cursor
//! and serves the next page. Another account's token gets 39934 through B,
//! as if the cursor didn't exist; a close through B ends it on A.
//!
//! ```sh
//! export SPRIG_TEST_CF_SQL=127.0.0.1:44306        # gateway A's MySQL port
//! export SPRIG_TEST_CF_HTTP_A=127.0.0.1:44080     # gateway A's HTTP listener
//! export SPRIG_TEST_CF_HTTP_B=127.0.0.1:44090     # gateway B's
//! export SPRIG_TEST_CF_ADMIN=127.0.0.1:44480      # the Admin API
//! export SPRIG_TEST_CF_TENANT=cf SPRIG_TEST_CF_USER=admin SPRIG_TEST_CF_PASSWORD=...
//! # optional: gateway B's telemetry, for sprig_studio_cursors_forwarded_total
//! export SPRIG_TEST_CF_TELEMETRY_B=127.0.0.1:44191
//! cargo test -p sprig-gateway --test studio_cursor_forward_live -- --nocapture
//! ```
//!
//! The test serves its own key set on 127.0.0.1 (no identity provider) and
//! registers it as an issuer of the tenant.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mysql_async::prelude::Queryable;
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, KeyPair};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DB: &str = "cf_live";
const AUD: &str = "cf-app";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            eprintln!("ok   {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn var(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
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
    kp: EcdsaKeyPair,
}

impl Key {
    fn new() -> Self {
        let rng = SystemRandom::new();
        let alg = &signature::ECDSA_P256_SHA256_FIXED_SIGNING;
        let der = EcdsaKeyPair::generate_pkcs8(alg, &rng).unwrap();
        Self {
            kp: EcdsaKeyPair::from_pkcs8(alg, der.as_ref(), &rng).unwrap(),
        }
    }

    fn jwks(&self) -> String {
        let point = self.kp.public_key().as_ref();
        json!({"keys": [{"kty": "EC", "crv": "P-256", "kid": "cf1", "use": "sig",
               "x": URL_SAFE_NO_PAD.encode(&point[1..33]), "y": URL_SAFE_NO_PAD.encode(&point[33..65])}]})
        .to_string()
    }

    fn sign(&self, iss: &str, user: &str) -> String {
        let at = now();
        let claims = json!({"iss": iss, "aud": AUD, "sub": format!("s-{user}"),
                            "preferred_username": user, "iat": at, "exp": at + 600});
        let header =
            URL_SAFE_NO_PAD.encode(json!({"alg": "ES256", "kid": "cf1", "typ": "JWT"}).to_string());
        let msg = format!("{header}.{}", URL_SAFE_NO_PAD.encode(claims.to_string()));
        let sig = self.kp.sign(&SystemRandom::new(), msg.as_bytes()).unwrap();
        format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
    }
}

/// Serves `body` as the key set on 127.0.0.1; its URL.
async fn serve_jwks(body: String) -> String {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(got) => buf.extend_from_slice(&chunk[..got]),
                    }
                }
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

/// One HTTP/1.1 request: (status, body).
async fn http(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut lines = String::new();
    for (name, value) in headers {
        let _ = write!(lines, "{name}: {value}\r\n");
    }
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{lines}Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut out = Vec::new();
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(req.as_bytes()).await?;
        stream.read_to_end(&mut out).await
    };
    if tokio::time::timeout(Duration::from_secs(20), exchange)
        .await
        .is_err()
    {
        return (0, "timeout".into());
    }
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, rest)| rest.to_owned());
    (status, body)
}

/// A cursor call with a bearer token and the tenant header.
async fn call(addr: &str, path: &str, token: &str, body: &Value) -> (u16, Value, String) {
    let tenant = var("SPRIG_TEST_CF_TENANT", "cf");
    let auth = format!("Bearer {token}");
    let (st, raw) = http(
        addr,
        "POST",
        path,
        &[("Authorization", &auth), ("x-sprig-tenant", &tenant)],
        &body.to_string(),
    )
    .await;
    let reply = serde_json::from_str(&raw).unwrap_or(Value::Null);
    (st, reply, raw)
}

/// The first column of a page's rows, as numbers.
fn ids(page: &Value) -> Vec<i64> {
    page["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let cell = row.as_array().map_or(row, |cells| &cells[0]);
                    cell.as_i64()
                        .or_else(|| cell.as_str().and_then(|text| text.parse().ok()))
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn forwarded_total(telemetry: Option<&str>) -> Option<f64> {
    let (_, text) = http(telemetry?, "GET", "/metrics", &[], "").await;
    Some(
        text.lines()
            .filter(|line| line.starts_with("sprig_studio_cursors_forwarded_total"))
            .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
            .sum(),
    )
}

#[ignore = "live: needs SPRIG_TEST_CF_SQL; tools/live/README.md"]
#[tokio::test]
async fn studio_cursor_forwarded_with_a_bearer_token() {
    let Ok(sql) = std::env::var("SPRIG_TEST_CF_SQL") else {
        eprintln!("skipped: set SPRIG_TEST_CF_SQL");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let (gw_a, gw_b) = (
        var("SPRIG_TEST_CF_HTTP_A", "127.0.0.1:44080"),
        var("SPRIG_TEST_CF_HTTP_B", "127.0.0.1:44090"),
    );
    let admin = var("SPRIG_TEST_CF_ADMIN", "127.0.0.1:44480");
    let tenant = var("SPRIG_TEST_CF_TENANT", "cf");
    let telemetry_b = std::env::var("SPRIG_TEST_CF_TELEMETRY_B").ok();
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };

    // The table, and two accounts that log in with tokens.
    let (host, port) = sql.rsplit_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!(
            "{tenant}.{}",
            var("SPRIG_TEST_CF_USER", "admin")
        )))
        .pass(Some(var("SPRIG_TEST_CF_PASSWORD", "sprig-dev-app")))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    let values: Vec<String> = (1..=50).map(|i| format!("({i}, 'v{i}')")).collect();
    for stmt in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.t (id INT PRIMARY KEY, v VARCHAR(20))"),
        format!("INSERT INTO {DB}.t VALUES {}", values.join(", ")),
        "DROP USER IF EXISTS cftok".into(),
        "DROP USER IF EXISTS cfother".into(),
        "CREATE USER cftok IDENTIFIED BY 'Cf-pass-1'".into(),
        "CREATE USER cfother IDENTIFIED BY 'Cf-pass-2'".into(),
        format!("GRANT SELECT ON {DB}.t TO cftok"),
        format!("GRANT SELECT ON {DB}.t TO cfother"),
    ] {
        conn.query_drop(&stmt)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    let key = Key::new();
    let jwks = serve_jwks(key.jwks()).await;
    // An issuer of its own per run, so the test runs again as it is.
    let iss = format!("https://idp.cf.test/{}", now());
    let issuer = json!({
        "issuer": iss, "jwks_url": jwks, "audiences": [AUD], "algorithms": ["ES256"],
        "jit_users": false, "max_lifetime_s": 3600,
        "mappings": [{"priority": 1, "kind": "user", "claim": "preferred_username",
                      "match_kind": "any", "target": "{preferred_username}"}]
    });
    let (st, body) = http(
        &admin,
        "POST",
        &format!("/admin/v1/tenants/{tenant}/issuers"),
        &[],
        &issuer.to_string(),
    )
    .await;
    ck.ok("the issuer is created", st == 200, || {
        format!("{st} {body}")
    });
    for user in ["cftok", "cfother"] {
        let (st, body) = http(
            &admin,
            "POST",
            &format!("/admin/v1/tenants/{tenant}/users:setTokenLogin"),
            &[],
            &json!({"user": user, "enabled": true}).to_string(),
        )
        .await;
        ck.ok(&format!("{user} takes tokens"), st == 200, || {
            format!("{st} {body}")
        });
    }
    let tok = key.sign(&iss, "cftok");
    let other = key.sign(&iss, "cfother");

    // Open on A (the token's tenant from the header); wait until both
    // gateways know the issuer.
    let open_body = json!({"sql": "SELECT id FROM t ORDER BY id", "page_rows": 10});
    let open_path = format!("/v1/databases/{DB}/cursors");
    let mut opened = (0, Value::Null, String::new());
    for _ in 0..60 {
        opened = call(&gw_a, &open_path, &tok, &open_body).await;
        let probe = call(
            &gw_b,
            &format!("/v1/databases/{DB}/sql:query"),
            &tok,
            &json!({"sql": "SELECT 1"}),
        )
        .await;
        if opened.0 == 200 && probe.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (st, page, raw) = opened;
    let name = page["name"].as_str().unwrap_or("").to_owned();
    ck.ok(
        "a: the cursor opens with a bearer token, first page 1..10",
        st == 200 && !name.is_empty() && ids(&page) == (1..=10).collect::<Vec<_>>(),
        || format!("{st} {raw}"),
    );
    let handle = name.strip_prefix("cursors/").unwrap_or(&name).to_owned();
    let fetch = format!("/v1/cursors/{handle}:fetch");

    // Fetched through B: forwarded to A, the next page.
    let before = forwarded_total(telemetry_b.as_deref()).await;
    let (st, page, raw) = call(&gw_b, &fetch, &tok, &json!({"rows": 10})).await;
    ck.ok(
        "b: the fetch is forwarded to a and gives 11..20",
        st == 200 && ids(&page) == (11..=20).collect::<Vec<_>>() && page["position"] == "20",
        || format!("{st} {raw}"),
    );
    let after = forwarded_total(telemetry_b.as_deref()).await;
    if let (Some(was), Some(now_seen)) = (before, after) {
        ck.ok(
            "b counts the forwarded call (sprig_studio_cursors_forwarded_total)",
            now_seen >= was + 1.0,
            || format!("{was} → {now_seen}"),
        );
    }
    // And on A itself: the page after.
    let (st, page, raw) = call(&gw_a, &fetch, &tok, &json!({"rows": 10})).await;
    ck.ok(
        "a: the next fetch on a gives 21..30",
        st == 200 && ids(&page) == (21..=30).collect::<Vec<_>>(),
        || format!("{st} {raw}"),
    );
    // Another account's token: as if the cursor didn't exist.
    let (st, _, raw) = call(&gw_b, &fetch, &other, &json!({"rows": 10})).await;
    ck.ok(
        "b: another account's token gets 39934",
        st != 200 && raw.contains("39934"),
        || format!("{st} {raw}"),
    );
    // Closed through B; then gone on A.
    let (st, page, raw) = call(
        &gw_b,
        &format!("/v1/cursors/{handle}:close"),
        &tok,
        &json!({}),
    )
    .await;
    ck.ok(
        "b: the close is forwarded and closes it",
        st == 200 && page["closed"] == json!(true),
        || format!("{st} {raw}"),
    );
    let (st, _, raw) = call(&gw_a, &fetch, &tok, &json!({"rows": 10})).await;
    ck.ok(
        "a: the closed cursor is gone",
        st != 200 && (raw.contains("39934") || raw.contains("39935")),
        || format!("{st} {raw}"),
    );

    let _ = conn
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let _ = conn.query_drop("DROP USER IF EXISTS cftok").await;
    let _ = conn.query_drop("DROP USER IF EXISTS cfother").await;
    eprintln!("{} checks, {} failed", ck.n, ck.failed.len());
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
