//! A statement's trace and its SLI on a running gateway: a statement slower than `telemetry.trace_slow_ms`
//! (500 ms by default) is kept by tail sampling, and `/debug/traces` on the
//! telemetry port shows its `gateway.request` root (with a `backend.query`
//! child naming the cluster when the statement was head-sampled);
//! `sprig_gateway_statements_total` counted it.
//!
//! ```sh
//! export SPRIG_TEST_OPS_TRACE=127.0.0.1:23307,http://127.0.0.1:23190,ops-src.admin,ops-pw-1
//! cargo test -p sprig-gateway --test trace_live -- --nocapture
//! ```
//!
//! The listener is the gateway's MySQL or MariaDB listener (TLS, any
//! certificate accepted); the URL its telemetry port; then a login and its
//! password. Without the variable the test reports itself skipped.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const ENV: &str = "SPRIG_TEST_OPS_TRACE";

async fn get(url: &str) -> String {
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let resp = client.get(url.parse().unwrap()).await.unwrap();
    String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

fn statements(metrics: &str) -> f64 {
    metrics
        .lines()
        .filter(|l| l.starts_with("sprig_gateway_statements_total{"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .fold(0.0, |a, b| a + b)
}

#[ignore = "live: needs SPRIG_TEST_OPS_TRACE; tools/live/README.md"]
#[tokio::test]
async fn a_slow_statement_is_traced_from_gateway_to_backend() {
    let Some(v) = skip::var(ENV) else {
        skip::report_missing(ENV, "a_slow_statement_is_traced_from_gateway_to_backend");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let parts: Vec<&str> = v.split(',').collect();
    let [listener, telemetry, user, password] = parts[..] else {
        panic!("{ENV}: expected listener,telemetry-url,user,password");
    };
    let (host, port) = listener.rsplit_once(':').unwrap();
    let before = statements(&get(&format!("{telemetry}/metrics")).await);
    let mut c = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(user))
            .pass(Some(password))
            .prefer_socket(false)
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
    )
    .await
    .unwrap();
    let slept: Option<i64> = c.query_first("SELECT SLEEP(0.6)").await.unwrap();
    assert_eq!(slept, Some(0));
    c.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let traces: serde_json::Value =
        serde_json::from_str(&get(&format!("{telemetry}/debug/traces")).await).unwrap();
    let t = traces
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|t| {
            t["kept_because"] == "slow"
                && t["spans"]
                    .as_array()
                    .unwrap()
                    .last()
                    .is_some_and(|r| r["duration_nanos"].as_u64().unwrap() >= 600_000_000)
        })
        .expect("a slow trace");
    let spans = t["spans"].as_array().unwrap();
    let root = spans.last().unwrap();
    assert_eq!(root["name"], "gateway.request");
    // A head-sampled statement shows its `backend.query` child; one that
    // wasn't ran without spans and is kept as its root alone.
    if let Some(child) = spans.iter().find(|s| s["name"] == "backend.query") {
        assert_eq!(child["parent_id"], root["span_id"]);
        let cluster = child["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a[0] == "cluster")
            .map(|a| a[1].clone())
            .unwrap();
        println!(
            "trace {}: gateway.request {} ms > backend.query on {cluster} {} ms",
            t["trace_id"],
            root["duration_nanos"].as_u64().unwrap() / 1_000_000,
            child["duration_nanos"].as_u64().unwrap() / 1_000_000
        );
    } else {
        assert_eq!(spans.len(), 1, "an unsampled root is kept alone");
        println!(
            "trace {}: gateway.request {} ms (not head-sampled: root only)",
            t["trace_id"],
            root["duration_nanos"].as_u64().unwrap() / 1_000_000
        );
    }
    let after = statements(&get(&format!("{telemetry}/metrics")).await);
    assert!(after > before, "statements counted: {before} -> {after}");
}
