//! The assistants on an engine-only tenant: the data and DBA assistants'
//! tools on engine tables, `ai.share_data`, stored conversations, `ASK`
//! and the MCP server, against a gateway whose model is a stub
//! (OpenAI-compatible, on loopback; no request leaves the machine).
//!
//! The stub logs every request body it gets, one JSON line each, so the
//! tests can check what reached the model. It answers `STUB:TOOL <name>
//! <json>` with that tool call, `STUB:PROPOSE <db> <sql>` with a
//! `propose_sql`, a tool result with `TOOL RESULT: <the result>`, and
//! anything else in words.
//!
//! Needs, besides the gateway: a provider record `stub` pointing at the
//! stub, `ai.model.data` and `ai.model.dba` = `stub/stub-1`, and a tenant
//! user who is the tenant's administrator.
//!
//! ```sh
//! export SPRIG_TEST_AI_MYSQL=127.0.0.1:38306 SPRIG_TEST_AI_HTTP=127.0.0.1:38080
//! export SPRIG_TEST_AI_USER=aic1.admin SPRIG_TEST_AI_PASSWORD=...
//! export SPRIG_TEST_AI_STUB_LOG=/path/to/requests.jsonl
//! cargo test -p sprig-gateway --test ai_assistants_live -- --ignored --test-threads 1
//! ```
//!
//! The tests create and drop the database `aic_live`, and leave
//! `ai.enabled` and `ai.mcp.enabled` on and `ai.share_data` off.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::format_push_string,
    missing_docs
)]

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const IGNORED: &str = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD and SPRIG_TEST_AI_STUB_LOG; tools/live/README.md";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{IGNORED} ({name} is missing)"))
}

/// A secret the tests plant in a row: it must reach the model only with
/// `ai.share_data` on.
const SECRET: &str = "carol.secret@example.com";

async fn conn() -> Conn {
    let addr = env("SPRIG_TEST_AI_MYSQL");
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(env("SPRIG_TEST_AI_USER")))
            .pass(Some(env("SPRIG_TEST_AI_PASSWORD")))
            .prefer_socket(false),
    )
    .await
    .unwrap()
}

/// One HTTP/1.1 request; the status and the whole body.
async fn http(
    method: &str,
    path: &str,
    body: Option<&Value>,
    auth: Option<(&str, &str)>,
) -> (u16, String) {
    let (st, b, _) = http_with(method, path, body, auth, &[]).await;
    (st, b)
}

async fn http_with(
    method: &str,
    path: &str,
    body: Option<&Value>,
    auth: Option<(&str, &str)>,
    headers: &[(&str, &str)],
) -> (u16, String, String) {
    let addr = env("SPRIG_TEST_AI_HTTP");
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let b = body.map(Value::to_string).unwrap_or_default();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n",
        b.len()
    );
    if body.is_some() {
        req.push_str("content-type: application/json\r\n");
    }
    if let Some((u, p)) = auth {
        let t = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
        req.push_str(&format!("authorization: Basic {t}\r\n"));
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(&b);
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out).into_owned();
    let (head, rest) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let chunked = head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    (
        status,
        if chunked {
            dechunk(rest)
        } else {
            rest.to_owned()
        },
        head.to_ascii_lowercase(),
    )
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = s.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        if n == 0 || rest.len() < n {
            break;
        }
        out.push_str(&rest[..n]);
        s = rest[n..].strip_prefix("\r\n").unwrap_or(&rest[n..]);
    }
    out
}

fn me() -> (String, String) {
    (env("SPRIG_TEST_AI_USER"), env("SPRIG_TEST_AI_PASSWORD"))
}

/// What the stub got since `from` lines: the request bodies.
fn stub_requests(from: usize) -> Vec<Value> {
    std::fs::read_to_string(env("SPRIG_TEST_AI_STUB_LOG"))
        .unwrap_or_default()
        .lines()
        .skip(from)
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .map(|v| v["body"].clone())
        .collect()
}

fn stub_mark() -> usize {
    std::fs::read_to_string(env("SPRIG_TEST_AI_STUB_LOG"))
        .unwrap_or_default()
        .lines()
        .count()
}

/// The stream's JSON lines.
fn events(body: &str) -> Vec<Value> {
    body.lines()
        .map(|l| l.strip_prefix("data: ").unwrap_or(l))
        .filter_map(|l| serde_json::from_str::<Value>(l.trim()).ok())
        .collect()
}

async fn setting(c: &mut Conn, kv: &str) {
    c.query_drop(format!("ALTER TENANT SPRIG SET ({kv})"))
        .await
        .unwrap();
}

/// Waits until settings reached the gateway: `ok` passes.
async fn eventually<F: Fn(&str) -> bool>(path: &str, body: &Value, ok: F) -> String {
    let (u, p) = me();
    let mut last = String::new();
    for _ in 0..50 {
        let (_, b) = http("POST", path, Some(body), Some((&u, &p))).await;
        if ok(&b) {
            return b;
        }
        last = b;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("never passed; last answer: {last}");
}

async fn setup(c: &mut Conn) {
    setting(c, "ai.enabled = TRUE").await;
    setting(c, "ai.share_data = FALSE").await;
    c.query_drop("DROP DATABASE IF EXISTS aic_live")
        .await
        .unwrap();
    c.query_drop("CREATE DATABASE aic_live").await.unwrap();
    c.query_drop("USE aic_live").await.unwrap();
    c.query_drop(
        "CREATE TABLE customers (id INT PRIMARY KEY, email VARCHAR(100) NOT NULL, name VARCHAR(50))",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, total DECIMAL(10,2), \
         KEY by_customer (customer_id), FOREIGN KEY (customer_id) REFERENCES customers (id))",
    )
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO customers VALUES (1, '{SECRET}', 'Carol'), (2, 'dan@example.com', 'Dan')"
    ))
    .await
    .unwrap();
    c.query_drop("INSERT INTO orders VALUES (10, 1, 150.00), (11, 2, 0.00), (12, 1, 20.00)")
        .await
        .unwrap();
}

fn tool_msg(name: &str, args: &Value) -> String {
    format!("STUB:TOOL {name} {args}")
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD and SPRIG_TEST_AI_STUB_LOG; tools/live/README.md"]
async fn the_data_assistant_queries_engine_tables_and_rows_leave_only_when_shared() {
    let mut c = conn().await;
    setup(&mut c).await;
    let q = json!({"db": "aic_live", "sql": "SELECT id, email FROM customers ORDER BY id"});
    let chat =
        json!({"role": "data", "database": "aic_live", "message": tool_msg("run_query", &q)});
    // Sharing off: the user sees the rows, the model only their shape.
    let mark = stub_mark();
    let body = eventually("/v1/ai/chat", &chat, |b| b.contains("TOOL RESULT")).await;
    assert!(
        body.contains(SECRET),
        "the user's result has the rows: {body}"
    );
    let sent = stub_requests(mark);
    assert!(!sent.is_empty(), "the model was called");
    let all = serde_json::to_string(&sent).unwrap();
    assert!(all.contains("customers"), "schema goes out: {all}");
    assert!(
        !all.contains(SECRET),
        "a row reached the model with ai.share_data off: {all}"
    );
    let conv = events(&body)
        .iter()
        .find_map(|e| {
            e.get("conversation")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            events(&body).iter().find_map(|e| {
                e.get("conversation")
                    .and_then(Value::as_u64)
                    .map(|n| n.to_string())
            })
        })
        .expect("a conversation id in the stream");
    // The conversation is stored in the tenant's records and reopens.
    let (u, p) = me();
    let (st, list) = http("GET", "/v1/ai/conversations", None, Some((&u, &p))).await;
    assert_eq!(st, 200, "{list}");
    assert!(
        list.contains(&format!("\"{conv}\"")) || list.contains(&format!(":{conv},")),
        "{list}"
    );
    let (st, shown) = http(
        "GET",
        &format!("/v1/ai/conversations/{conv}"),
        None,
        Some((&u, &p)),
    )
    .await;
    assert_eq!(st, 200, "{shown}");
    assert!(shown.contains("run_query"), "{shown}");
    assert!(
        !shown.contains(SECRET),
        "stored tool results are shape-only: {shown}"
    );
    // Sharing on: the rows go to the model too.
    setting(&mut c, "ai.share_data = TRUE").await;
    let mark = stub_mark();
    eventually("/v1/ai/chat", &chat, |b| {
        b.contains("TOOL RESULT") && b.contains(SECRET)
    })
    .await;
    let all = serde_json::to_string(&stub_requests(mark)).unwrap();
    assert!(all.contains(SECRET), "sharing on sends the rows: {all}");
    setting(&mut c, "ai.share_data = FALSE").await;
    // A write through run_query is refused, and nothing changes.
    let w = json!({"db": "aic_live", "sql": "DELETE FROM orders"});
    let (_, b) = http(
        "POST",
        "/v1/ai/chat",
        Some(
            &json!({"role": "data", "database": "aic_live", "message": tool_msg("run_query", &w)}),
        ),
        Some((&u, &p)),
    )
    .await;
    assert!(b.contains("TOOL RESULT"), "{b}");
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM aic_live.orders")
        .await
        .unwrap();
    assert_eq!(n, Some(3));
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD and SPRIG_TEST_AI_STUB_LOG; tools/live/README.md"]
async fn the_dba_assistants_read_tools_see_engine_catalogs_and_statistics() {
    let mut c = conn().await;
    setup(&mut c).await;
    let (u, p) = me();
    let ask = |tool: &str, args: Value| json!({"role": "dba", "database": "aic_live", "message": tool_msg(tool, &args)});
    // Each tool's result, as the model got it.
    let mut results = Vec::new();
    for (tool, args, want) in [
        ("catalog", json!({}), "aic_live"),
        ("catalog", json!({"db": "aic_live"}), "orders"),
        (
            "describe",
            json!({"db": "aic_live", "table": "orders"}),
            "customer_id",
        ),
        (
            "relationships",
            json!({"db": "aic_live", "table": "orders"}),
            "customers",
        ),
        (
            "explain",
            json!({"db": "aic_live", "sql": "SELECT * FROM orders WHERE customer_id = 1"}),
            "orders",
        ),
        (
            "column_stats",
            json!({"db": "aic_live", "table": "orders", "cols": ["total"]}),
            "total",
        ),
        (
            "indexes",
            json!({"db": "aic_live", "table": "orders"}),
            "by_customer",
        ),
        (
            "check_ddl",
            json!({"sql": "CREATE TABLE t2 (id INT PRIMARY KEY)"}),
            "passed",
        ),
        (
            "sensitive_columns",
            json!({"db": "aic_live", "table": "customers"}),
            "email",
        ),
        ("workload", json!({"db": "aic_live", "hours": 24}), ""),
        (
            "settings",
            json!({"key": "ai.", "db": "aic_live"}),
            "ai.share_data",
        ),
        ("approvals", json!({}), ""),
        ("auto_indexes", json!({"db": "aic_live"}), ""),
    ] {
        let (st, b) = http(
            "POST",
            "/v1/ai/chat",
            Some(&ask(tool, args.clone())),
            Some((&u, &p)),
        )
        .await;
        assert_eq!(st, 200, "{tool}: {b}");
        let line = b
            .lines()
            .find(|l| l.contains("TOOL RESULT"))
            .unwrap_or_else(|| panic!("{tool} {args}: no tool result in {b}"))
            .to_owned();
        results.push((tool, line.clone()));
        assert!(line.contains(want), "{tool} {args}: {want} not in {line}");
        assert!(!line.contains("\"error\""), "{tool} {args} failed: {line}");
    }
    for (t, l) in &results {
        eprintln!("{t}: {}", &l[..l.len().min(300)]);
    }
    assert!(
        !format!("{results:?}").contains(SECRET),
        "no values leave with sharing off"
    );
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD and SPRIG_TEST_AI_STUB_LOG; tools/live/README.md"]
async fn ask_proposes_on_an_engine_tenant_and_runs_nothing() {
    let mut c = conn().await;
    setup(&mut c).await;
    let rows: Vec<Row> = c
        .query("ASK 'STUB:PROPOSE aic_live DELETE FROM orders WHERE total = 0' ROLE dba IN DATABASE aic_live")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let q: Option<String> = rows[0].get(0).unwrap();
    assert_eq!(q.as_deref(), Some("DELETE FROM orders WHERE total = 0"));
    let conv: u64 = rows[0].get(5).unwrap();
    assert!(conv > 0);
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM aic_live.orders")
        .await
        .unwrap();
    assert_eq!(n, Some(3), "ASK ran nothing");
    // In words.
    let rows: Vec<Row> = c.query("ASK 'hello'").await.unwrap();
    let q: Option<String> = rows[0].get(0).unwrap();
    assert_eq!(q, None);
    let words: Option<String> = rows[0].get(2).unwrap();
    assert_eq!(words.as_deref(), Some("stub answer"));
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD and SPRIG_TEST_AI_STUB_LOG; tools/live/README.md"]
async fn the_mcp_server_lists_and_runs_its_tools_as_the_signed_in_user() {
    let mut c = conn().await;
    setup(&mut c).await;
    setting(&mut c, "ai.mcp.enabled = TRUE").await;
    let (u, p) = me();
    let rpc = |id: u64, method: &str, params: Value| json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    let accept = [("accept", "application/json, text/event-stream")];
    let tools_list = rpc(1, "tools/list", json!({}));
    // No login, a wrong password: refused before anything runs.
    let (st, b, _) = http_with(
        "POST",
        "/v1/mcp?role=data",
        Some(&tools_list),
        None,
        &accept,
    )
    .await;
    assert!(st == 401 || st == 403, "{st} {b}");
    let (st, b, _) = http_with(
        "POST",
        "/v1/mcp?role=data",
        Some(&tools_list),
        Some((&u, "not-the-password")),
        &accept,
    )
    .await;
    assert!(st == 401 || st == 403, "{st} {b}");
    // The handshake (a session, as Claude Code's revision does), then the tools.
    let init = rpc(
        1,
        "initialize",
        json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "aic-live", "version": "1"}}),
    );
    let mut got = (0, String::new(), String::new());
    for _ in 0..50 {
        got = http_with(
            "POST",
            "/v1/mcp?role=data",
            Some(&init),
            Some((&u, &p)),
            &accept,
        )
        .await;
        if got.0 == 200 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert_eq!(got.0, 200, "{}", got.1);
    let sid = got
        .2
        .lines()
        .find_map(|l| l.strip_prefix("mcp-session-id:"))
        .map(|v| v.trim().to_owned())
        .expect("a session id");
    let hs = [
        ("accept", "application/json, text/event-stream"),
        ("mcp-protocol-version", "2025-06-18"),
        ("mcp-session-id", sid.as_str()),
    ];
    let call = |id: u64, name: &str, args: Value| {
        rpc(id, "tools/call", json!({"name": name, "arguments": args}))
    };
    let (st, list, _) = http_with(
        "POST",
        "/v1/mcp",
        Some(&rpc(2, "tools/list", json!({}))),
        Some((&u, &p)),
        &hs,
    )
    .await;
    assert_eq!(st, 200, "{list}");
    for t in ["catalog", "describe", "run_query"] {
        assert!(list.contains(&format!("\"{t}\"")), "{t} not listed: {list}");
    }
    assert!(
        !list.contains("\"propose_sql\""),
        "proposals are off by default: {list}"
    );
    let (st, r, _) = http_with(
        "POST",
        "/v1/mcp",
        Some(&call(
            3,
            "describe",
            json!({"db": "aic_live", "table": "customers"}),
        )),
        Some((&u, &p)),
        &hs,
    )
    .await;
    assert_eq!(st, 200, "{r}");
    assert!(r.contains("email"), "{r}");
    let (st, r, _) = http_with(
        "POST",
        "/v1/mcp",
        Some(&call(
            4,
            "run_query",
            json!({"db": "aic_live", "sql": "SELECT email FROM customers"}),
        )),
        Some((&u, &p)),
        &hs,
    )
    .await;
    assert_eq!(st, 200, "{r}");
    assert!(
        !r.contains(SECRET),
        "rows are withheld with sharing off: {r}"
    );
    let (st, r, _) = http_with(
        "POST",
        "/v1/mcp",
        Some(&call(
            5,
            "run_query",
            json!({"db": "aic_live", "sql": "DELETE FROM orders"}),
        )),
        Some((&u, &p)),
        &hs,
    )
    .await;
    assert_eq!(st, 200, "{r}");
    assert!(
        r.contains("isError") || r.contains("only one SELECT"),
        "{r}"
    );
    // Another user's session id is refused.
    let (st, _, _) = http_with(
        "POST",
        "/v1/mcp",
        Some(&rpc(6, "tools/list", json!({}))),
        Some((&u, "not-the-password")),
        &hs,
    )
    .await;
    assert!(st == 401 || st == 403);
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM aic_live.orders")
        .await
        .unwrap();
    assert_eq!(n, Some(3));
    // Audited as a conversation in the tenant's records.
    let (_, convs) = http("GET", "/v1/ai/conversations", None, Some((&u, &p))).await;
    assert!(convs.contains("MCP: aic-live"), "{convs}");
    c.query_drop("DROP DATABASE aic_live").await.unwrap();
}

/// Two operator logins (`SPRIG_TEST_AI_OPERATORS=login:password,login:password`,
/// both in `studio.operator_logins`); the test user is not one.
fn operators() -> Vec<(String, String)> {
    env("SPRIG_TEST_AI_OPERATORS")
        .split(',')
        .map(|p| {
            let (l, w) = p.split_once(':').expect("login:password");
            (l.trim().to_owned(), w.trim().to_owned())
        })
        .collect()
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AI_MYSQL, SPRIG_TEST_AI_HTTP, SPRIG_TEST_AI_USER, SPRIG_TEST_AI_PASSWORD, SPRIG_TEST_AI_STUB_LOG and SPRIG_TEST_AI_OPERATORS; tools/live/README.md"]
async fn admin_conversations_are_their_operators_only() {
    let ops = operators();
    let (op1, op2) = (&ops[0], &ops[1]);
    let (u, p) = me();
    // An operator's administration turn, stored in the system records.
    let chat = json!({"role": "admin", "message": "why is the disk filling up?"});
    let mut body = String::new();
    for _ in 0..50 {
        let (st, b) = http("POST", "/v1/ai/chat", Some(&chat), Some((&op1.0, &op1.1))).await;
        body = b;
        if st == 200 && body.contains("stub answer") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(body.contains("stub answer"), "{body}");
    assert!(!body.contains("unstored"), "the turn was stored: {body}");
    let id = events(&body)
        .iter()
        .find_map(|e| {
            e.get("conversation")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .expect("a conversation id");
    assert!(id.starts_with('a'), "{id}");
    // Its operator lists and opens it.
    let (st, l) = http("GET", "/v1/ai/conversations", None, Some((&op1.0, &op1.1))).await;
    assert_eq!(st, 200, "{l}");
    assert!(l.contains(&format!("\"{id}\"")), "{l}");
    let (st, s) = http(
        "GET",
        &format!("/v1/ai/conversations/{id}"),
        None,
        Some((&op1.0, &op1.1)),
    )
    .await;
    assert_eq!(st, 200, "{s}");
    assert!(s.contains("disk filling up"), "{s}");
    // Another operator: not listed, not opened, not continued, not rated.
    let (st, l) = http("GET", "/v1/ai/conversations", None, Some((&op2.0, &op2.1))).await;
    assert_eq!(st, 200, "{l}");
    assert!(!l.contains(&format!("\"{id}\"")), "{l}");
    let (st, s) = http(
        "GET",
        &format!("/v1/ai/conversations/{id}"),
        None,
        Some((&op2.0, &op2.1)),
    )
    .await;
    assert_ne!(st, 200, "another operator opened it: {s}");
    assert!(!s.contains("disk filling up"), "{s}");
    let (_, s) = http(
        "POST",
        "/v1/ai/chat",
        Some(&json!({"role": "admin", "conversation": id, "message": "and now?"})),
        Some((&op2.0, &op2.1)),
    )
    .await;
    assert!(!s.contains("disk filling up"), "{s}");
    // A tenant user who isn't an operator: no admin turns, nothing to open.
    let (st, s) = http("POST", "/v1/ai/chat", Some(&chat), Some((&u, &p))).await;
    assert!(st != 200 || !s.contains("stub answer"), "{st} {s}");
    let (st, s) = http(
        "GET",
        &format!("/v1/ai/conversations/{id}"),
        None,
        Some((&u, &p)),
    )
    .await;
    assert_ne!(st, 200, "a tenant user opened it: {s}");
    assert!(!s.contains("disk filling up"), "{s}");
    let (_, l) = http("GET", "/v1/ai/conversations", None, Some((&u, &p))).await;
    assert!(!l.contains(&format!("\"{id}\"")), "{l}");
    // No login at all.
    let (st, _) = http("GET", &format!("/v1/ai/conversations/{id}"), None, None).await;
    assert!(st == 401 || st == 403, "{st}");
}
