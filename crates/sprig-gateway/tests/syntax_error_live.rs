//! A statement the gateway can't parse answers the server's syntax error
//! (1064, MariaDB's or MySQL's wording) on a tenant without MariaDB, on the
//! MySQL wire, REST `sql:query` and REST `sql:execute`; a statement that
//! parses keeps its answer.
//!
//! ```sh
//! export SPRIG_TEST_SYNTAX=127.0.0.1:<mysql port>,127.0.0.1:<http port>,<user>,<password>
//! cargo test -p sprig-gateway --test syntax_error_live -- --ignored --nocapture
//! ```
//!
//! The user belongs to a tenant placed without a MariaDB cluster (an
//! engine-only tenant). The MySQL login is over TLS when offered.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    missing_docs
)]

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

const VAR: &str = "SPRIG_TEST_SYNTAX";

struct Env {
    mysql: String,
    http: String,
    user: String,
    password: String,
}

fn env() -> Env {
    let v = std::env::var(VAR)
        .unwrap_or_else(|_| panic!("live test run without {VAR}: see tools/live/README.md"));
    let p: Vec<&str> = v.split(',').collect();
    assert_eq!(p.len(), 4, "{VAR}: mysql,http,user,password");
    Env {
        mysql: p[0].to_owned(),
        http: p[1].to_owned(),
        user: p[2].to_owned(),
        password: p[3].to_owned(),
    }
}

const NEAR: &str =
    "You have an error in your SQL syntax; check the manual that corresponds to your";

async fn rest(e: &Env, path: &str, sql: &str) -> (u16, serde_json::Value) {
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", e.user, e.password))
    );
    let body = serde_json::json!({ "sql": sql }).to_string();
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", &e.http)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&b).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_SYNTAX; tools/live/README.md"]
async fn an_unparsable_statement_is_a_syntax_error_without_mariadb() {
    let e = env();
    // The MySQL wire.
    let (host, port) = e.mysql.rsplit_once(':').unwrap();
    let o = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(e.user.clone()))
        .pass(Some(e.password.clone()))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
        .prefer_socket(false);
    let mut c = Conn::new(o).await.expect("login");
    let err = c.query_drop("SELEC 1").await.expect_err("SELEC 1 fails");
    let mysql_async::Error::Server(s) = &err else {
        panic!("not a server error: {err}");
    };
    assert_eq!(s.code, 1064, "{s:?}");
    assert_eq!(s.state, "42000", "{s:?}");
    assert!(s.message.starts_with(NEAR), "{}", s.message);
    assert!(
        s.message.contains("near 'SELEC 1' at line 1"),
        "{}",
        s.message
    );
    // A statement that parses still runs.
    let one: Option<i64> = c.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    // REST, both calls.
    for path in ["/v1/databases/-/sql:query", "/v1/databases/-/sql:execute"] {
        let (status, v) = rest(&e, path, "SELEC 1").await;
        assert!(status >= 400, "{path}: {status} {v}");
        assert_eq!(v["code"], 1064, "{path}: {v}");
        let msg = v["message"].as_str().unwrap_or_default();
        assert!(msg.contains(NEAR), "{path}: {v}");
        assert!(msg.contains("near 'SELEC 1' at line 1"), "{path}: {v}");
        let (status, v) = rest(&e, path, "SELECT 1").await;
        assert!(status < 400, "{path}: {status} {v}");
    }
}
