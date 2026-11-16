//! The AI functions through a live gateway: SQL over the MySQL protocol
//! and the MongoDB operators, against a stub OpenAI-compatible provider
//! that this test serves itself (nothing leaves the machine), and the
//! bundled embedding model.
//!
//! Needs a gateway with this build and the model in `docs.model_dir`, a
//! tenant user who is the tenant's administrator, a provider record named
//! `stub` of kind `open_ai_chat` with base URL `http://<SPRIG_TEST_AIFN_STUB>/v1`
//! and no key, `ai.functions.model = stub/fn-model`, and a MongoDB
//! listener:
//!
//! ```sh
//! export SPRIG_TEST_AIFN_GATEWAY=127.0.0.1:29306   # MySQL persona
//! export SPRIG_TEST_AIFN_MONGO=127.0.0.1:29017
//! export SPRIG_TEST_AIFN_STUB=127.0.0.1:29999      # served by this test
//! export SPRIG_TEST_AIFN_TENANT=acme SPRIG_TEST_AIFN_USER=app SPRIG_TEST_AIFN_PASSWORD=...
//! cargo test -p sprig-gateway --test ai_fn_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `aifn_live`, dropped afterwards; leaves the
//! tenant's `ai.functions.*`, `ai.share_data` and budget values as they
//! were before (it resets them).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::sync::{Arc, Mutex};

use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts};
use serde_json::{Value, json};
use sprig_testkit::skip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GATEWAY: &str = "SPRIG_TEST_AIFN_GATEWAY";
const DB: &str = "aifn_live";

type Seen = Arc<Mutex<Vec<String>>>;

fn var(k: &str, d: &str) -> String {
    skip::var(k).unwrap_or_else(|| d.to_owned())
}

/// One SSE chunk of an OpenAI-compatible stream.
fn chunk(delta: &Value, finish: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({"id": "c1", "object": "chat.completion.chunk", "model": "fn-model",
               "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})
    )
}

/// The stub's answer to one request body.
fn answer(body: &Value) -> String {
    let tool = body["tools"][0]["function"]["name"].as_str().unwrap_or("");
    let user = body["messages"]
        .as_array()
        .and_then(|m| m.last())
        .and_then(|m| m["content"].as_str())
        .unwrap_or("")
        .to_owned();
    let mut s = String::new();
    match tool {
        "classify" => {
            let answers: Vec<&str> = user
                .split("<input id=")
                .skip(1)
                .map(|p| if p.contains("crash") { "bug" } else { "idea" })
                .collect();
            let args = json!({"answers": answers}).to_string();
            s.push_str(&chunk(
                &json!({"role": "assistant", "tool_calls": [{"index": 0, "id": "t1", "type": "function",
                    "function": {"name": "classify", "arguments": args}}]}),
                None,
            ));
            s.push_str(&chunk(&json!({}), Some("tool_calls")));
        }
        "record" => {
            let city = if user.contains("Pune") {
                "Pune"
            } else {
                "Delhi"
            };
            let args = json!({"value": {"city": city}}).to_string();
            s.push_str(&chunk(
                &json!({"role": "assistant", "tool_calls": [{"index": 0, "id": "t1", "type": "function",
                    "function": {"name": "record", "arguments": args}}]}),
                None,
            ));
            s.push_str(&chunk(&json!({}), Some("tool_calls")));
        }
        _ => {
            let words = user.split_whitespace().count();
            s.push_str(&chunk(
                &json!({"role": "assistant", "content": format!("{words} words")}),
                None,
            ));
            s.push_str(&chunk(&json!({}), Some("stop")));
        }
    }
    s.push_str(&format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"id": "c1", "object": "chat.completion.chunk", "model": "fn-model", "choices": [],
               "usage": {"prompt_tokens": 50, "completion_tokens": 5, "total_tokens": 55}})
    ));
    s
}

/// Serves the stub on `addr`; every request body is kept in `seen`.
async fn stub(addr: &str, seen: Seen) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 8192];
                // Headers, then Content-Length bytes of body.
                let (head_end, len) = loop {
                    let n = sock.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..p]).to_ascii_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (p + 4, len);
                    }
                };
                while buf.len() < head_end + len {
                    let n = sock.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let body = String::from_utf8_lossy(&buf[head_end..head_end + len]).into_owned();
                let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                seen.lock().unwrap().push(body);
                let sse = answer(&v);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{sse}",
                    sse.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
}

async fn conn(addr: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!(
                "{}.{}",
                var("SPRIG_TEST_AIFN_TENANT", "acme"),
                var("SPRIG_TEST_AIFN_USER", "app")
            )))
            .pass(Some(var("SPRIG_TEST_AIFN_PASSWORD", "sprig-dev-app")))
            .prefer_socket(false)
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
    )
    .await
    .unwrap()
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        e => panic!("not a server error: {e}"),
    }
}

fn mcode(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

async fn set(c: &mut Conn, kv: &str) {
    c.query_drop(format!("ALTER TENANT SPRIG SET ({kv})"))
        .await
        .unwrap();
}

/// Waits until the tenant's settings change reached the gateway (they are
/// read from its catalog cache).
async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
}

fn vec_of(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

#[tokio::test]
async fn ai_functions_end_to_end() {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "ai_functions_end_to_end");
        return;
    };
    let stub_addr = var("SPRIG_TEST_AIFN_STUB", "127.0.0.1:29999");
    let seen: Seen = Arc::default();
    stub(&stub_addr, Arc::clone(&seen)).await;
    let sent = |s: &Seen| s.lock().unwrap().len();

    let mut c = conn(&addr).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    c.query_drop("CREATE TABLE tickets (id INT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();
    // A tag of this run in every input sent, so answers cached by an
    // earlier run don't hide the calls.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    c.query_drop(format!(
        "INSERT INTO tickets VALUES (1, 'the app crashes on start {run}'), (2, 'add a dark mode {run}'), \
         (3, NULL), (4, 'the app crashes on start {run}'), (5, 'ship it to Pune please {run}')"
    ))
    .await
    .unwrap();

    // Off: 39991, before anything.
    set(
        &mut c,
        "ai.functions.enabled = FALSE, ai.share_data = FALSE",
    )
    .await;
    settle().await;
    let e = c
        .query_drop("SELECT AI_SUMMARIZE('hello')")
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39991);

    // On, sharing off: the bundled model works, providers are refused.
    set(&mut c, "ai.functions.enabled = TRUE").await;
    settle().await;
    let e = c
        .query_drop("SELECT id, AI_CLASSIFY(body, 'bug', 'idea') FROM tickets")
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39995);
    assert_eq!(sent(&seen), 0, "no value left with sharing off");

    c.query_drop("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, e VECTOR(384) NOT NULL)")
        .await
        .unwrap();
    for (id, name) in [
        (1, "red running shoes"),
        (2, "blue denim jacket"),
        (3, "kitchen knife set"),
    ] {
        c.query_drop(format!(
            "INSERT INTO products VALUES ({id}, '{name}', AI_EMBED('{name}'))"
        ))
        .await
        .unwrap();
    }
    let best: Option<i64> = c
        .query_first(
            "SELECT id FROM products ORDER BY VEC_DISTANCE_COSINE(e, AI_EMBED('crimson sneakers')) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(best, Some(1));
    let rows: Vec<Row> = c
        .query("SELECT id, AI_EMBED(name) AS v FROM products ORDER BY id")
        .await
        .unwrap();
    let v: Vec<u8> = rows[0].get(1).unwrap();
    assert_eq!(v.len(), 384 * 4);
    let n: f32 = vec_of(&v).iter().map(|x| x * x).sum();
    assert!((n - 1.0).abs() < 1e-3, "unit length: {n}");
    assert_eq!(rows[0].columns_ref()[1].name_str(), "v");
    // Equal to what the INSERT stored.
    let stored: Vec<u8> = c
        .query_first("SELECT e FROM products WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, v);
    assert_eq!(sent(&seen), 0, "the bundled model sends nothing");

    // Sharing on: provider answers, batched, then cached.
    set(&mut c, "ai.share_data = TRUE").await;
    settle().await;
    let rows: Vec<Row> = c
        .query("SELECT id, AI_CLASSIFY(body, 'bug', 'idea') AS kind FROM tickets ORDER BY id")
        .await
        .unwrap();
    let kinds: Vec<Option<String>> = rows.iter().map(|r| r.get(1).unwrap()).collect();
    assert_eq!(
        kinds,
        [
            Some("bug".into()),
            Some("idea".into()),
            None,
            Some("bug".into()),
            Some("idea".into())
        ]
    );
    assert_eq!(rows[0].columns_ref()[1].name_str(), "kind");
    assert_eq!(sent(&seen), 1, "three distinct inputs in one call");
    assert!(seen.lock().unwrap()[0].contains("dark mode"));
    let again: Vec<Row> = c
        .query("SELECT id, AI_CLASSIFY(body, 'bug', 'idea') AS kind FROM tickets ORDER BY id")
        .await
        .unwrap();
    assert_eq!(again.len(), 5);
    assert_eq!(sent(&seen), 1, "the second run is answered from the cache");

    // Extract and summarize; JSON on 3306.
    let rows: Vec<Row> = c
        .query(
            r#"SELECT AI_EXTRACT(body, '{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}') FROM tickets WHERE id = 5"#,
        )
        .await
        .unwrap();
    let j: String = rows[0].get(0).unwrap();
    // As MySQL 8.4 prints a JSON value.
    assert_eq!(j, r#"{"city": "Pune"}"#);
    let s: Option<String> = c
        .query_first("SELECT AI_SUMMARIZE(body, 10) FROM tickets WHERE id = 2")
        .await
        .unwrap();
    assert!(s.unwrap().ends_with("words"));

    // Where a call over rows can't stand; prepared statements.
    let e = c
        .query_drop("SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug'")
        .await
        .unwrap_err();
    assert_eq!(code(&e), 1235);
    let e = c
        .exec_drop("SELECT AI_SUMMARIZE(?)", ("x",))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39051);
    let e = c
        .query_drop("SELECT AI_CLASSIFY(body, 'bug') FROM tickets")
        .await
        .unwrap_err();
    assert_eq!(code(&e), 1582);

    // Budgets: spent → 39992; on_budget = null → NULL and a warning.
    set(&mut c, "ai.budget.user_tokens_day = 1").await;
    settle().await;
    let before = sent(&seen);
    let e = c
        .query_drop(format!("SELECT AI_COMPLETE('write a new line here {run}')"))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 39992);
    set(&mut c, "ai.functions.on_budget = 'null'").await;
    settle().await;
    let rows: Vec<Row> = c
        .query("SELECT id, AI_SUMMARIZE(body, 7) FROM tickets ORDER BY id")
        .await
        .unwrap();
    assert!(
        rows.iter()
            .all(|r| r.get::<Option<String>, _>(1).unwrap().is_none())
    );
    assert_eq!(c.get_warnings(), 1);
    assert_eq!(sent(&seen), before, "nothing sent past the budget");
    set(
        &mut c,
        "ai.budget.user_tokens_day = 200000, ai.functions.on_budget = 'error'",
    )
    .await;
    settle().await;

    // MongoDB.
    if let Some(m) = skip::var("SPRIG_TEST_AIFN_MONGO") {
        let uri = format!(
            "mongodb://{}.{}:{}@{m}/?authSource=admin&directConnection=true",
            var("SPRIG_TEST_AIFN_TENANT", "acme"),
            var("SPRIG_TEST_AIFN_USER", "app"),
            var("SPRIG_TEST_AIFN_PASSWORD", "sprig-dev-app")
        );
        let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
        let coll = client.database(DB).collection::<Document>("notes");
        coll.insert_many([
            doc! {"_id": 1, "text": format!("it crashes on save {run}")},
            doc! {"_id": 2, "text": format!("please add export {run}")},
            doc! {"_id": 3},
        ])
        .await
        .unwrap();
        let before = sent(&seen);
        let mut cur = coll
            .aggregate([
                doc! {"$sort": {"_id": 1}},
                doc! {"$set": {
                    "kind": {"$aiClassify": {"input": "$text", "labels": ["bug", "idea"]}},
                    "loud": {"$toUpper": {"$aiClassify": {"input": "$text", "labels": ["bug", "idea"]}}},
                    "e": {"$aiEmbed": "$text"},
                }},
                doc! {"$project": {"kind": 1, "loud": 1, "dims": {"$size": {"$ifNull": ["$e", []]}}}},
            ])
            .await
            .unwrap();
        let mut docs = Vec::new();
        while cur.advance().await.unwrap() {
            docs.push(cur.deserialize_current().unwrap());
        }
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].get_str("kind").unwrap(), "bug");
        assert_eq!(docs[0].get_str("loud").unwrap(), "BUG");
        assert_eq!(docs[1].get_str("kind").unwrap(), "idea");
        assert_eq!(docs[2].get("kind"), Some(&Bson::Null));
        assert_eq!(docs[0].get_i32("dims").unwrap(), 384);
        assert!(!docs[0].contains_key("__sprig_ai_0"));
        assert_eq!(
            sent(&seen),
            before + 1,
            "one call; the second operator hit the cache"
        );
        // Sharing off for the database: the provider operators are refused.
        c.query_drop(format!(
            "ALTER DATABASE {DB} SPRIG SET (ai.share_data = FALSE)"
        ))
        .await
        .unwrap();
        settle().await;
        let e = coll
            .aggregate([doc! {"$set": {"s": {"$aiSummarize": "$text"}}}])
            .await
            .unwrap_err();
        assert_eq!(mcode(&e), 39995);
        // The bundled model still works.
        let mut cur = coll
            .aggregate([
                doc! {"$match": {"_id": 1}},
                doc! {"$project": {"e": {"$aiEmbed": "$text"}}},
            ])
            .await
            .unwrap();
        assert!(cur.advance().await.unwrap());
        let d = cur.deserialize_current().unwrap();
        assert_eq!(d.get_array("e").unwrap().len(), 384);
        let e = coll
            .aggregate([doc! {"$set": {"s": {"$aiSummarize": 3}}}])
            .await
            .unwrap_err();
        assert_eq!(mcode(&e), 14);
        client.database(DB).drop().await.unwrap();
    }

    set(
        &mut c,
        "ai.functions.enabled = FALSE, ai.share_data = FALSE",
    )
    .await;
    c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
}
