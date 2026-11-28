//! AI functions over row values in `WHERE`, inside expressions, in
//! `ORDER BY` and in `UPDATE ... SET`, through a live gateway, against a
//! stub OpenAI-compatible provider this test serves itself (nothing leaves
//! the machine) and the bundled embedding model.
//!
//! Needs what `ai_fn_live` needs: a gateway with this build and the model
//! in `docs.model_dir`, a tenant user who is the tenant's administrator, a
//! provider record named `stub` of kind `openai` with base URL
//! `http://<SPRIG_TEST_AIFNB_STUB>/v1` and no key, and
//! `ai.functions.model = stub/fn-model`:
//!
//! ```sh
//! export SPRIG_TEST_AIFNB_GATEWAY=127.0.0.1:29306   # MySQL persona
//! export SPRIG_TEST_AIFNB_STUB=127.0.0.1:29999      # served by this test
//! export SPRIG_TEST_AIFNB_TENANT=acme SPRIG_TEST_AIFNB_USER=app SPRIG_TEST_AIFNB_PASSWORD=...
//! # optional: MongoDB's $match, and a sharded table (the command groups,
//! # then splits, the pinned table: `<cmd> group`, `<cmd> split`)
//! export SPRIG_TEST_AIFNB_MONGO=127.0.0.1:29017
//! export SPRIG_TEST_AIFNB_SHARD_CMD='.../deploy.sh shard'
//! # optional: an operator's command setting a tenant value (`<cmd> <key> <value>`)
//! export SPRIG_TEST_AIFNB_OPERATOR_SET='sprigctl settings set --scope tenant:acme --reason test'
//! cargo test -p sprig-gateway --test ai_fn_bound_live -- --nocapture
//! ```
//!
//! Works in the database `aifnb_live`, dropped afterwards; leaves the
//! tenant's `ai.functions.*`, `ai.share_data` and budget values reset.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongodb::bson::{Document, doc};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts};
use serde_json::{Value, json};
use sprig_testkit::skip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GATEWAY: &str = "SPRIG_TEST_AIFNB_GATEWAY";
const DB: &str = "aifnb_live";

type Seen = Arc<Mutex<Vec<String>>>;

fn var(k: &str, d: &str) -> String {
    skip::var(k).unwrap_or_else(|| d.to_owned())
}

fn chunk(delta: &Value, finish: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({"id": "c1", "object": "chat.completion.chunk", "model": "fn-model",
               "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})
    )
}

/// The stub's answer: `classify` labels an input with "crash" in it as its
/// first label, others as the second; any other prompt is answered with
/// "<n> words".
fn answer(body: &Value) -> String {
    let tool = body["tools"][0]["function"]["name"].as_str().unwrap_or("");
    let user = body["messages"]
        .as_array()
        .and_then(|m| m.last())
        .and_then(|m| m["content"].as_str())
        .unwrap_or("")
        .to_owned();
    let mut s = String::new();
    if tool == "classify" {
        // The labels, from the system prompt: "... these labels: [..]. ...".
        let labels: Vec<String> = body["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m["content"].as_str())
            .find_map(|t| {
                let from = t.find("these labels: ")? + "these labels: ".len();
                let to = from + t[from..].find(']')? + 1;
                serde_json::from_str(&t[from..to]).ok()
            })
            .unwrap_or_else(|| vec!["bug".into(), "idea".into()]);
        let answers: Vec<&str> = user
            .split("<input id=")
            .skip(1)
            .map(|p| {
                if p.contains("crash") {
                    labels[0].as_str()
                } else {
                    labels[1].as_str()
                }
            })
            .collect();
        let args = json!({"answers": answers}).to_string();
        s.push_str(&chunk(
            &json!({"role": "assistant", "tool_calls": [{"index": 0, "id": "t1", "type": "function",
                "function": {"name": "classify", "arguments": args}}]}),
            None,
        ));
        s.push_str(&chunk(&json!({}), Some("tool_calls")));
    } else {
        let words = user.split_whitespace().count();
        s.push_str(&chunk(
            &json!({"role": "assistant", "content": format!("{words} words")}),
            None,
        ));
        s.push_str(&chunk(&json!({}), Some("stop")));
    }
    let usage = json!({"id": "c1", "object": "chat.completion.chunk", "model": "fn-model", "choices": [],
           "usage": {"prompt_tokens": 50, "completion_tokens": 5, "total_tokens": 55}});
    s.push_str("data: ");
    s.push_str(&usage.to_string());
    s.push_str("\n\ndata: [DONE]\n\n");
    s
}

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

async fn login(addr: &str, user: &str, password: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!(
                "{}.{user}",
                var("SPRIG_TEST_AIFNB_TENANT", "acme")
            )))
            .pass(Some(password))
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
        _ => 0,
    }
}

/// Checks counted and reported, so one run shows every failure.
#[derive(Default)]
struct Checks {
    passed: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, good: bool, detail: impl FnOnce() -> String) {
        if good {
            self.passed += 1;
            eprintln!("  ok   {what}");
        } else {
            let d = detail();
            eprintln!("  FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

async fn must(c: &mut Conn, sql: &str) {
    if let Err(e) = c.query_drop(sql).await {
        panic!("{sql}: {e}");
    }
}

async fn ids(c: &mut Conn, sql: &str) -> Result<Vec<i64>, u16> {
    c.query::<i64, _>(sql).await.map_err(|e| code(&e))
}

async fn texts(c: &mut Conn, sql: &str) -> Result<Vec<Option<String>>, u16> {
    c.query::<Option<String>, _>(sql)
        .await
        .map_err(|e| code(&e))
}

async fn fails(c: &mut Conn, sql: &str) -> u16 {
    match c.query_drop(sql).await {
        Ok(()) => 0,
        Err(e) => code(&e),
    }
}

async fn in_trx(c: &mut Conn) -> i64 {
    c.query_first::<i64, _>("SELECT @@in_transaction")
        .await
        .unwrap()
        .unwrap_or(-1)
}

async fn set(c: &mut Conn, kv: &str) {
    must(c, &format!("ALTER TENANT SPRIG SET ({kv})")).await;
    // Read from the gateway's catalog cache.
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

/// Inputs in a classify request.
fn inputs_in(body: &str) -> usize {
    body.matches("<input id=").count()
}

fn shell(cmd: &str) -> bool {
    std::process::Command::new("bash")
        .arg("-c")
        .arg(cmd)
        .status()
        .is_ok_and(|s| s.success())
}

#[ignore = "live: needs SPRIG_TEST_AIFNB_GATEWAY, SPRIG_TEST_AIFNB_TENANT, SPRIG_TEST_AIFNB_STUB, ...; tools/live/README.md"]
#[tokio::test]
async fn ai_functions_bound_end_to_end() {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "ai_functions_bound_end_to_end");
        return;
    };
    let stub_addr = var("SPRIG_TEST_AIFNB_STUB", "127.0.0.1:29999");
    let seen: Seen = Arc::default();
    stub(&stub_addr, Arc::clone(&seen)).await;
    let sent = |s: &Seen| s.lock().unwrap().len();
    let last = |s: &Seen| s.lock().unwrap().last().cloned().unwrap_or_default();
    let user = var("SPRIG_TEST_AIFNB_USER", "app");
    let pass = var("SPRIG_TEST_AIFNB_PASSWORD", "sprig-dev-app");
    let mut ck = Checks::default();

    let mut c = login(&addr, &user, &pass).await;
    let mut c2 = login(&addr, &user, &pass).await;
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    must(&mut c2, &format!("USE {DB}")).await;
    must(
        &mut c,
        "CREATE TABLE tickets (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, body TEXT, \
         gist TEXT, kind VARCHAR(40))",
    )
    .await;
    // A tag of this run in every input, so an earlier run's cache hides nothing.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let rows = format!(
        "(1, 'alice', 'the app crashes on start {run}', NULL, NULL), \
         (2, 'bob', 'add a dark mode {run}', NULL, NULL), \
         (3, 'alice', NULL, NULL, NULL), \
         (4, 'bob', 'the app crashes on start {run}', NULL, NULL), \
         (5, 'alice', 'ship it to Pune please {run}', NULL, NULL), \
         (6, 'bob', 'crash when saving {run}', NULL, NULL)"
    );
    must(&mut c, &format!("INSERT INTO tickets VALUES {rows}")).await;
    set(
        &mut c,
        "ai.functions.enabled = TRUE, ai.share_data = TRUE, ai.functions.on_budget = 'error', \
         ai.budget.user_tokens_day = 200000",
    )
    .await;

    // ---- WHERE ----------------------------------------------------------------------
    let n0 = sent(&seen);
    let got = ids(
        &mut c,
        "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug' ORDER BY id",
    )
    .await;
    ck.ok(
        "WHERE AI_CLASSIFY(...) = 'bug'",
        got == Ok(vec![1, 4, 6]),
        || format!("{got:?}"),
    );
    ck.ok(
        "its four distinct inputs went in one call",
        sent(&seen) == n0 + 1 && inputs_in(&last(&seen)) == 4,
        || {
            format!(
                "{} calls, last with {} inputs",
                sent(&seen) - n0,
                inputs_in(&last(&seen))
            )
        },
    );
    let n0 = sent(&seen);
    let again = ids(
        &mut c,
        "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug' ORDER BY id",
    )
    .await;
    ck.ok(
        "the second run comes from the cache",
        again == Ok(vec![1, 4, 6]) && sent(&seen) == n0,
        || format!("{again:?}, {} calls", sent(&seen) - n0),
    );
    let n0 = sent(&seen);
    let got = ids(
        &mut c,
        "SELECT id FROM tickets WHERE id <= 2 AND AI_CLASSIFY(body, 'bug', 'idea', 'other') = 'bug'",
    )
    .await;
    ck.ok(
        "conditions AND-ed beside the call narrow the inputs read",
        got == Ok(vec![1]) && sent(&seen) == n0 + 1 && inputs_in(&last(&seen)) == 2,
        || format!("{got:?}, last call with {} inputs", inputs_in(&last(&seen))),
    );

    // ---- the SELECT list and ORDER BY ------------------------------------------------
    let n0 = sent(&seen);
    let r: Result<Vec<Row>, _> = c
        .query(
            "SELECT id, UPPER(AI_CLASSIFY(body, 'bug', 'idea')) FROM tickets ORDER BY id LIMIT 2",
        )
        .await;
    let ok = r.as_ref().is_ok_and(|rows| {
        let v: Vec<Option<String>> = rows.iter().map(|r| r.get(1).unwrap()).collect();
        v == [Some("BUG".to_owned()), Some("IDEA".to_owned())]
            && rows[0].columns_ref()[1].name_str() == "UPPER(AI_CLASSIFY(body, 'bug', 'idea'))"
    });
    ck.ok(
        "a call inside an expression of the list, named as written",
        ok && sent(&seen) == n0,
        || format!("{r:?}"),
    );
    let n0 = sent(&seen);
    let got = ids(
        &mut c,
        "SELECT CHAR_LENGTH(AI_SUMMARIZE(body, 5)) > 0 FROM tickets ORDER BY id LIMIT 1",
    )
    .await;
    ck.ok(
        "LIMIT holds: only the returned row's input is summarized",
        got == Ok(vec![1]) && sent(&seen) == n0 + 1,
        || format!("{got:?}, {} calls", sent(&seen) - n0),
    );
    let got = ids(
        &mut c,
        "SELECT id FROM tickets ORDER BY AI_CLASSIFY(body, 'bug', 'idea'), id",
    )
    .await;
    ck.ok("ORDER BY a call", got == Ok(vec![3, 1, 4, 6, 2, 5]), || {
        format!("{got:?}")
    });
    let got: Result<Vec<(i64, Option<String>)>, _> = c
        .query(
            "SELECT id, AI_CLASSIFY(body, 'bug', 'idea') AS k FROM tickets ORDER BY k DESC, id LIMIT 3",
        )
        .await;
    ck.ok(
        "ORDER BY a call's alias with LIMIT",
        got.as_ref().is_ok_and(|v| {
            v.iter().map(|x| x.0).collect::<Vec<_>>() == [2, 5, 1]
                && v[0].1.as_deref() == Some("idea")
        }),
        || format!("{got:?}"),
    );
    let got = texts(
        &mut c,
        "SELECT DISTINCT AI_CLASSIFY(body, 'bug', 'idea') AS k FROM tickets ORDER BY k",
    )
    .await;
    ck.ok(
        "SELECT DISTINCT over answers",
        got == Ok(vec![None, Some("bug".into()), Some("idea".into())]),
        || format!("{got:?}"),
    );
    let r: Result<Vec<(String, i64)>, _> = c
        .query(
            "SELECT owner, SUM(AI_CLASSIFY(body, 'bug', 'idea') = 'bug') AS bugs \
             FROM tickets GROUP BY owner ORDER BY owner",
        )
        .await;
    ck.ok(
        "inside an aggregate, over every row",
        r.as_ref()
            .is_ok_and(|v| v == &[("alice".to_owned(), 1), ("bob".to_owned(), 2)]),
        || format!("{r:?}"),
    );
    let r: Result<Vec<(i64, String)>, _> = c
        .query(
            "SELECT id, AI_CLASSIFY(body, 'bug', 'idea') AS k FROM tickets \
             WHERE AI_CLASSIFY(body, 'bug', 'idea') <> 'bug' ORDER BY id",
        )
        .await;
    ck.ok(
        "the same call in the list and WHERE",
        r.as_ref()
            .is_ok_and(|v| v == &[(2, "idea".to_owned()), (5, "idea".to_owned())]),
        || format!("{r:?}"),
    );

    // ---- UPDATE ... SET ----------------------------------------------------------------
    let n0 = sent(&seen);
    let r = c
        .query_drop(
            "UPDATE tickets SET gist = AI_SUMMARIZE(body, 10), \
             kind = COALESCE(AI_CLASSIFY(body, 'bug', 'idea'), 'none') WHERE id <= 4",
        )
        .await;
    let affected = c.affected_rows();
    ck.ok(
        "UPDATE with two calls, one inside COALESCE",
        r.is_ok() && affected == 4,
        || format!("{r:?}, {affected} rows"),
    );
    ck.ok(
        "its two distinct summaries were asked for, the labels came from the cache",
        sent(&seen) == n0 + 2,
        || format!("{} calls", sent(&seen) - n0),
    );
    ck.ok(
        "the UPDATE committed its own transaction",
        in_trx(&mut c).await == 0,
        String::new,
    );
    let r: Vec<(i64, Option<String>, Option<String>)> = c2
        .query("SELECT id, gist, kind FROM tickets ORDER BY id")
        .await
        .unwrap();
    let kinds: Vec<Option<&str>> = r.iter().map(|x| x.2.as_deref()).collect();
    let gists: Vec<bool> = r
        .iter()
        .map(|x| x.1.as_deref().is_some_and(|g| g.ends_with("words")))
        .collect();
    ck.ok(
        "another session sees the answers in rows 1 to 4 only",
        kinds
            == [
                Some("bug"),
                Some("idea"),
                Some("none"),
                Some("bug"),
                None,
                None,
            ]
            && gists == [true, true, false, true, false, false],
        || format!("{r:?}"),
    );
    must(
        &mut c,
        "UPDATE tickets SET gist = CONCAT('[', AI_CLASSIFY(body, 'bug', 'idea'), '] ', LEFT(body, 10)) \
         WHERE id = 2",
    )
    .await;
    let g: Option<String> = c
        .query_first("SELECT gist FROM tickets WHERE id = 2")
        .await
        .unwrap()
        .flatten();
    ck.ok(
        "a call inside CONCAT in SET",
        g.as_deref() == Some("[idea] add a dark"),
        || format!("{g:?}"),
    );
    // In the caller's transaction: rolled back with it.
    must(&mut c, "START TRANSACTION").await;
    must(
        &mut c,
        "UPDATE tickets SET kind = AI_CLASSIFY(body, 'bug', 'idea') WHERE id = 5",
    )
    .await;
    let inside: Option<String> = c
        .query_first("SELECT kind FROM tickets WHERE id = 5")
        .await
        .unwrap()
        .flatten();
    let open = in_trx(&mut c).await;
    must(&mut c, "ROLLBACK").await;
    let after: Option<String> = c
        .query_first("SELECT kind FROM tickets WHERE id = 5")
        .await
        .unwrap()
        .flatten();
    ck.ok(
        "an UPDATE in the caller's transaction stays in it",
        inside.as_deref() == Some("idea") && open == 1 && after.is_none(),
        || format!("{inside:?} {open} {after:?}"),
    );
    // The inputs are read FOR UPDATE: a row another transaction holds
    // makes the UPDATE wait (1205 here), before anything is sent.
    must(&mut c2, "START TRANSACTION").await;
    must(&mut c2, "SELECT id FROM tickets WHERE id = 6 FOR UPDATE").await;
    must(&mut c, "SET SESSION innodb_lock_wait_timeout = 1").await;
    let n0 = sent(&seen);
    let e = fails(
        &mut c,
        "UPDATE tickets SET kind = AI_CLASSIFY(body, 'bug', 'idea', 'held') WHERE id = 6",
    )
    .await;
    ck.ok(
        "a locked row: the read of the inputs waits for it, nothing is sent",
        e == 1205 && sent(&seen) == n0,
        || format!("{e}, {} calls", sent(&seen) - n0),
    );
    ck.ok(
        "and the UPDATE's own transaction is rolled back",
        in_trx(&mut c).await == 0,
        String::new,
    );
    must(&mut c2, "ROLLBACK").await;
    must(
        &mut c,
        "UPDATE tickets SET kind = AI_CLASSIFY(body, 'bug', 'idea', 'held') WHERE id = 6",
    )
    .await;
    let k: Option<String> = c
        .query_first("SELECT kind FROM tickets WHERE id = 6")
        .await
        .unwrap()
        .flatten();
    ck.ok("once released it runs", k.as_deref() == Some("bug"), || {
        format!("{k:?}")
    });
    must(&mut c, "SET SESSION innodb_lock_wait_timeout = 50").await;
    must(&mut c, "UPDATE tickets SET kind = NULL, gist = NULL").await;

    // ---- still refused ---------------------------------------------------------------
    for (sql, want) in [
        (
            "DELETE FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug'",
            1235,
        ),
        (
            "INSERT INTO tickets (id, owner, kind) SELECT id + 100, owner, AI_CLASSIFY(body, 'bug', 'idea') FROM tickets",
            1235,
        ),
        (
            "UPDATE tickets SET kind = 'x' WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug'",
            1235,
        ),
        (
            "SELECT AI_CLASSIFY(body, 'bug', 'idea') AS k, COUNT(*) FROM tickets GROUP BY k",
            1235,
        ),
        (
            "SELECT owner FROM tickets GROUP BY owner HAVING MAX(AI_CLASSIFY(body, 'bug', 'idea')) = 'bug'",
            1235,
        ),
        (
            "SELECT id FROM tickets WHERE id IN (SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug')",
            1235,
        ),
        (
            "SELECT AI_SUMMARIZE(body) FROM tickets UNION SELECT 'x'",
            1235,
        ),
        (
            "UPDATE tickets SET body = 'x', gist = AI_SUMMARIZE(body)",
            1235,
        ),
    ] {
        let e = fails(&mut c, sql).await;
        ck.ok(&format!("refused: {sql}"), e == want, || format!("{e}"));
    }
    let e = c
        .exec_drop(
            "UPDATE tickets SET gist = AI_SUMMARIZE(body) WHERE id = ?",
            (1,),
        )
        .await
        .map_or_else(|e| code(&e), |()| 0);
    ck.ok("refused in a prepared UPDATE", e == 39051, || {
        format!("{e}")
    });

    // ---- egress, the bundled model, max_inputs, budgets -------------------------------
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (ai.share_data = FALSE)"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let n0 = sent(&seen);
    let e = fails(
        &mut c,
        "UPDATE tickets SET gist = AI_SUMMARIZE(body, 4) WHERE id = 5",
    )
    .await;
    let g: Option<String> = c
        .query_first("SELECT gist FROM tickets WHERE id = 5")
        .await
        .unwrap()
        .flatten();
    ck.ok(
        "sharing off: the UPDATE fails with 39995, sends nothing, changes nothing",
        e == 39995 && sent(&seen) == n0 && g.is_none() && in_trx(&mut c).await == 0,
        || format!("{e} {g:?}"),
    );
    let e = fails(
        &mut c,
        "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug'",
    )
    .await;
    ck.ok("and so does WHERE", e == 39995 && sent(&seen) == n0, || {
        format!("{e}")
    });
    let got = ids(
        &mut c,
        "SELECT id FROM tickets WHERE body IS NOT NULL \
         ORDER BY VEC_DISTANCE_COSINE(AI_EMBED(body), AI_EMBED('the program crashes when it starts')), id LIMIT 2",
    )
    .await;
    ck.ok(
        "the bundled model in ORDER BY, with sharing off",
        got == Ok(vec![1, 4]) && sent(&seen) == n0,
        || format!("{got:?}"),
    );
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (ai.share_data = TRUE)"),
    )
    .await;
    // ai.functions.max_inputs is an operator's: set through the command.
    if let Some(op) = skip::var("SPRIG_TEST_AIFNB_OPERATOR_SET") {
        let set_ok = shell(&format!("{op} ai.functions.max_inputs 2"));
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let n0 = sent(&seen);
        let e = fails(
            &mut c,
            "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea', 'many') = 'bug'",
        )
        .await;
        ck.ok(
            "ai.functions.max_inputs holds (39992, nothing sent)",
            set_ok && e == 39992 && sent(&seen) == n0,
            || format!("{set_ok} {e}"),
        );
        shell(&format!("{op} ai.functions.max_inputs 10000"));
    } else {
        eprintln!("     (no SPRIG_TEST_AIFNB_OPERATOR_SET: max_inputs isn't checked)");
    }
    set(
        &mut c,
        "ai.budget.user_tokens_day = 1, ai.functions.on_budget = 'null'",
    )
    .await;
    let r = c
        .query_drop("UPDATE tickets SET gist = AI_SUMMARIZE(body, 3) WHERE id = 5")
        .await;
    let w: Vec<(String, u16, String)> = c.query("SHOW WARNINGS").await.unwrap_or_default();
    let g: Option<String> = c
        .query_first("SELECT gist FROM tickets WHERE id = 5")
        .await
        .unwrap()
        .flatten();
    ck.ok(
        "budget spent under on_budget = null: NULL and warning 39992",
        r.is_ok() && g.is_none() && w.iter().any(|x| x.1 == 39992),
        || format!("{r:?} {g:?} {w:?}"),
    );
    set(
        &mut c,
        "ai.budget.user_tokens_day = 200000, ai.functions.on_budget = 'error'",
    )
    .await;

    // ---- row-level security --------------------------------------------------------------
    must(
        &mut c,
        "CREATE USER 'aifnbreader'@'%' IDENTIFIED BY 'Aifnb-reader-Passw0rd'",
    )
    .await;
    must(
        &mut c,
        &format!("GRANT SELECT, UPDATE ON {DB}.* TO 'aifnbreader'@'%'"),
    )
    .await;
    must(
        &mut c,
        "CREATE POLICY alice_only ON tickets TO aifnbreader USING (owner = 'alice')",
    )
    .await;
    must(&mut c, "ALTER TABLE tickets ENABLE ROW LEVEL SECURITY").await;
    let mut r = login(&addr, "aifnbreader", "Aifnb-reader-Passw0rd").await;
    must(&mut r, &format!("USE {DB}")).await;
    let n0 = sent(&seen);
    let got = ids(
        &mut r,
        "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea', 'rls') = 'bug' ORDER BY id",
    )
    .await;
    let body = last(&seen);
    ck.ok(
        "under a policy the reader's WHERE reads only rows it sees",
        got == Ok(vec![1])
            && sent(&seen) == n0 + 1
            && inputs_in(&body) == 2
            && !body.contains("dark mode")
            && !body.contains("saving"),
        || format!("{got:?}, {} inputs", inputs_in(&body)),
    );
    let n0 = sent(&seen);
    let rr = r
        .query_drop(
            "UPDATE tickets SET kind = COALESCE(AI_CLASSIFY(body, 'bug', 'idea', 'rls2'), 'none')",
        )
        .await;
    let affected = r.affected_rows();
    let body = last(&seen);
    let k: Vec<(i64, Option<String>)> = c
        .query("SELECT id, kind FROM tickets ORDER BY id")
        .await
        .unwrap();
    ck.ok(
        "and its UPDATE changes and sends only those rows",
        rr.is_ok()
            && affected == 3
            && sent(&seen) == n0 + 1
            && !body.contains("dark mode")
            && k.iter().all(|(id, kind)| (id % 2 == 1) == kind.is_some()),
        || format!("{rr:?} {affected} {k:?}"),
    );
    let _ = r.disconnect().await;
    must(&mut c, "ALTER TABLE tickets DISABLE ROW LEVEL SECURITY").await;
    must(&mut c, "DROP POLICY alice_only ON tickets").await;
    must(&mut c, "DROP USER 'aifnbreader'@'%'").await;

    // ---- a sharded table ----------------------------------------------------------------
    if let Some(cmd) = skip::var("SPRIG_TEST_AIFNB_SHARD_CMD") {
        must(&mut c, "CREATE TABLE sh LIKE tickets").await;
        must(&mut c, &format!("INSERT INTO sh VALUES {rows}")).await;
        must(&mut c, "SPRIG PIN sh SHARD KEY (id)").await;
        let grouped = shell(&format!("{cmd} group"));
        let split = grouped && shell(&format!("{cmd} split"));
        ck.ok("the table is grouped and split in two", split, || {
            format!("group {grouped}")
        });
        let got = ids(
            &mut c,
            "SELECT id FROM sh WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug' ORDER BY id",
        )
        .await;
        ck.ok("sharded: WHERE", got == Ok(vec![1, 4, 6]), || {
            format!("{got:?}")
        });
        let r = c
            .query_drop(
                "UPDATE sh SET kind = COALESCE(AI_CLASSIFY(body, 'bug', 'idea'), 'none'), \
                 gist = AI_SUMMARIZE(body, 10)",
            )
            .await;
        let affected = c.affected_rows();
        let k: Vec<(i64, Option<String>)> = c
            .query("SELECT id, kind FROM sh ORDER BY id")
            .await
            .unwrap_or_default();
        let want: Vec<(i64, Option<String>)> = (1..=6)
            .zip(["bug", "idea", "none", "bug", "idea", "bug"])
            .map(|(i, k)| (i, Some(k.to_owned())))
            .collect();
        ck.ok(
            "sharded: UPDATE ... SET over every shard",
            r.is_ok() && affected == 6 && k == want && in_trx(&mut c).await == 0,
            || format!("{r:?} {affected} {k:?}"),
        );
        let got = ids(
            &mut c,
            "SELECT id FROM sh ORDER BY AI_CLASSIFY(body, 'bug', 'idea'), id LIMIT 4",
        )
        .await;
        ck.ok(
            "sharded: ORDER BY a call with LIMIT",
            got == Ok(vec![3, 1, 4, 6]),
            || format!("{got:?}"),
        );
    } else {
        eprintln!("     (no SPRIG_TEST_AIFNB_SHARD_CMD: the sharded table isn't checked)");
    }

    // ---- MongoDB's $match ---------------------------------------------------------------
    if let Some(m) = skip::var("SPRIG_TEST_AIFNB_MONGO") {
        let uri = format!(
            "mongodb://{}.{user}:{pass}@{m}/?authSource=admin&directConnection=true",
            var("SPRIG_TEST_AIFNB_TENANT", "acme"),
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
        let r = coll
            .aggregate([
                doc! {"$match": {"$expr": {"$eq": [
                    {"$aiClassify": {"input": "$text", "labels": ["bug", "idea"]}}, "bug"
                ]}}},
                doc! {"$project": {"_id": 1}},
            ])
            .await;
        let mut got = Vec::new();
        if let Ok(mut cur) = r {
            while cur.advance().await.unwrap_or(false) {
                if let Ok(d) = cur.deserialize_current() {
                    got.push(d.get_i32("_id").unwrap_or(-1));
                }
            }
        }
        ck.ok(
            "MongoDB: $match with $expr over $aiClassify",
            got == [1],
            || format!("{got:?}"),
        );
        let _ = client.database(DB).drop().await;
    } else {
        eprintln!("     (no SPRIG_TEST_AIFNB_MONGO: $match isn't checked)");
    }

    set(
        &mut c,
        "ai.functions.enabled = FALSE, ai.share_data = FALSE",
    )
    .await;
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    let _ = c2.disconnect().await;
    let _ = c.disconnect().await;
    eprintln!(
        "  {} of {} checks passed; {} provider calls",
        ck.passed,
        ck.passed + ck.failed.len(),
        sent(&seen)
    );
    assert!(ck.failed.is_empty(), "failed: {:#?}", ck.failed);
}
