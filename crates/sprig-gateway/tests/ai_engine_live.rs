//! AI functions and embedded columns on engine tables, through a live
//! gateway whose tenant has no MariaDB at all: SQL over both personas
//! (the MySQL port and the MariaDB port), against a stub OpenAI-compatible
//! provider this test serves itself (nothing leaves the machine) and the
//! bundled embedding model.
//!
//! Needs a gateway with this build and the model in `docs.model_dir`, an
//! engine-only tenant, a user who is its administrator, a provider record
//! named `stub` of kind `openai` with base URL
//! `http://<SPRIG_TEST_AIE_STUB>/v1` and no key, and `ai.functions.model =
//! stub/fn-model` (global):
//!
//! ```sh
//! export SPRIG_TEST_AIE_GATEWAY=127.0.0.1:23406   # MySQL persona
//! export SPRIG_TEST_AIE_MARIADB=127.0.0.1:23407   # MariaDB persona
//! export SPRIG_TEST_AIE_STUB=127.0.0.1:23499      # served by this test
//! export SPRIG_TEST_AIE_TENANT=t1 SPRIG_TEST_AIE_USER=app SPRIG_TEST_AIE_PASSWORD=...
//! cargo test -p sprig-gateway --test ai_engine_live -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `ai_engine_restart_part_1` and `_part_2` are the restart check: part 1
//! leaves a table of rows whose embedded column is still being filled; the
//! caller kills the gateway (`kill -9`) and starts it again; part 2 checks
//! that every row is filled with its current source's vector, and that
//! changes after the restart are followed.
//!
//! Works in the databases `aie_live`, `aie_live2` and `aie_restart`,
//! dropped at the start of each run; leaves the tenant's AI values reset.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_testkit::skip;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GATEWAY: &str = "SPRIG_TEST_AIE_GATEWAY";

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

async fn login(addr: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!(
                "{}.{}",
                var("SPRIG_TEST_AIE_TENANT", "acme"),
                var("SPRIG_TEST_AIE_USER", "app")
            )))
            .pass(Some(var("SPRIG_TEST_AIE_PASSWORD", "sprig-dev-app")))
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

    fn done(self, name: &str) {
        eprintln!(
            "{name}: {} passed, {} failed",
            self.passed,
            self.failed.len()
        );
        assert!(self.failed.is_empty(), "{:#?}", self.failed);
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

async fn one_i64(c: &mut Conn, sql: &str) -> Result<i64, u16> {
    c.query_first::<Option<i64>, _>(sql)
        .await
        .map(|v| v.flatten().unwrap_or(-1))
        .map_err(|e| code(&e))
}

async fn fails(c: &mut Conn, sql: &str) -> u16 {
    match c.query_drop(sql).await {
        Ok(()) => 0,
        Err(e) => code(&e),
    }
}

async fn set(c: &mut Conn, kv: &str) {
    must(c, &format!("ALTER TENANT SPRIG SET ({kv})")).await;
    // Read from the gateway's catalog cache.
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

fn run_tag() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Rows of `t` whose embedded column `emb` doesn't hold the bundled
/// model's vector of `src` (an SQL expression over the row): wrong, or not
/// filled yet (NULL for a source that isn't NULL or blank).
fn wrong_rows_sql(t: &str, src: &str) -> String {
    format!(
        "SELECT COUNT(*) FROM {t} WHERE CASE WHEN {src} IS NULL OR TRIM({src}) = '' THEN emb IS NOT NULL \
         ELSE emb IS NULL OR COALESCE(VEC_DISTANCE_COSINE(emb, AI_EMBED({src})), 1) > 0.0001 END"
    )
}

/// Waits up to `secs` for no wrong rows; the count left.
async fn settled(c: &mut Conn, t: &str, src: &str, secs: u64) -> i64 {
    let until = Instant::now() + Duration::from_secs(secs);
    loop {
        let n = one_i64(c, &wrong_rows_sql(t, src)).await.unwrap_or(-2);
        if n == 0 || Instant::now() >= until {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The AI functions on one persona's port.
async fn functions_on(addr: &str, db: &str, seen: &Seen, ck: &mut Checks, tag: &str) {
    let sent = || seen.lock().unwrap().len();
    let mut c = login(addr).await;
    must(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    must(
        &mut c,
        "CREATE TABLE tickets (id INT PRIMARY KEY, body TEXT, kind VARCHAR(40), gist TEXT)",
    )
    .await;
    let run = run_tag();
    must(
        &mut c,
        &format!(
            "INSERT INTO tickets VALUES (1, 'the app crashes on start {run}', NULL, NULL), \
             (2, 'add a dark mode {run}', NULL, NULL), (3, NULL, NULL, NULL), \
             (4, 'the app crashes on start {run}', NULL, NULL), (5, 'ship it to Pune please {run}', NULL, NULL), \
             (6, 'crash when saving {run}', NULL, NULL)"
        ),
    )
    .await;
    set(
        &mut c,
        "ai.functions.enabled = TRUE, ai.share_data = TRUE, ai.functions.on_budget = 'error', \
         ai.budget.user_tokens_day = 200000",
    )
    .await;

    // SELECT: a whole item, then from the cache.
    let n0 = sent();
    let got = texts(
        &mut c,
        "SELECT AI_CLASSIFY(body, 'bug', 'idea') FROM tickets ORDER BY id",
    )
    .await;
    let want: Vec<Option<String>> = ["bug", "idea", "", "bug", "idea", "bug"]
        .iter()
        .map(|s| (!s.is_empty()).then(|| (*s).to_owned()))
        .collect();
    ck.ok(
        &format!("{tag}: SELECT AI_CLASSIFY over rows"),
        got.as_ref() == Ok(&want) && sent() == n0 + 1,
        || format!("{got:?}, {} calls", sent() - n0),
    );
    let n1 = sent();
    let again = texts(
        &mut c,
        "SELECT AI_CLASSIFY(body, 'bug', 'idea') FROM tickets ORDER BY id",
    )
    .await;
    ck.ok(
        &format!("{tag}: the same SELECT again comes from the cache"),
        again.as_ref() == Ok(&want) && sent() == n1,
        || format!("{again:?}, {} calls", sent() - n1),
    );

    // WHERE, and inside an expression.
    let got = ids(
        &mut c,
        "SELECT id FROM tickets WHERE AI_CLASSIFY(body, 'bug', 'idea') = 'bug' ORDER BY id",
    )
    .await;
    ck.ok(
        &format!("{tag}: WHERE AI_CLASSIFY(...) = 'bug'"),
        got == Ok(vec![1, 4, 6]),
        || format!("{got:?}"),
    );
    let got = texts(
        &mut c,
        "SELECT UPPER(AI_CLASSIFY(body, 'bug', 'idea')) FROM tickets WHERE id IN (1, 2) ORDER BY id",
    )
    .await;
    ck.ok(
        &format!("{tag}: a call inside an expression"),
        got == Ok(vec![Some("BUG".into()), Some("IDEA".into())]),
        || format!("{got:?}"),
    );

    // UPDATE ... SET.
    let r = c
        .query_drop("UPDATE tickets SET kind = AI_CLASSIFY(body, 'bug', 'idea') WHERE id <= 4")
        .await;
    let got = texts(&mut c, "SELECT kind FROM tickets ORDER BY id").await;
    ck.ok(
        &format!("{tag}: UPDATE ... SET kind = AI_CLASSIFY(body, ...)"),
        r.is_ok()
            && got
                == Ok(vec![
                    Some("bug".into()),
                    Some("idea".into()),
                    None,
                    Some("bug".into()),
                    None,
                    None,
                ]),
        || format!("{r:?} {got:?}"),
    );
    let n2 = sent();
    let r = c
        .query_drop("UPDATE tickets SET gist = AI_SUMMARIZE(body, 5) WHERE id = 5")
        .await;
    let got = texts(&mut c, "SELECT gist FROM tickets WHERE id = 5").await;
    ck.ok(
        &format!("{tag}: UPDATE ... SET gist = AI_SUMMARIZE(body)"),
        r.is_ok()
            && matches!(got.as_deref(), Ok([Some(g)]) if g.ends_with(" words"))
            && sent() == n2 + 1,
        || format!("{r:?} {got:?} {} calls", sent() - n2),
    );
    // In the caller's transaction, rolled back with it.
    must(&mut c, "START TRANSACTION").await;
    let r = c
        .query_drop("UPDATE tickets SET kind = AI_CLASSIFY(body, 'bug', 'idea') WHERE id = 6")
        .await;
    must(&mut c, "ROLLBACK").await;
    let got = texts(&mut c, "SELECT kind FROM tickets WHERE id = 6").await;
    ck.ok(
        &format!("{tag}: an UPDATE in the caller's transaction rolls back with it"),
        r.is_ok() && got == Ok(vec![None]),
        || format!("{r:?} {got:?}"),
    );

    // The bundled model, and vector distance.
    let d: Result<Vec<Option<f64>>, u16> = c
        .query::<Option<f64>, _>(
            "SELECT VEC_DISTANCE_COSINE(AI_EMBED('red shoes'), AI_EMBED('red shoes'))",
        )
        .await
        .map_err(|e| code(&e));
    ck.ok(
        &format!("{tag}: AI_EMBED and VEC_DISTANCE_COSINE"),
        matches!(d.as_deref(), Ok([Some(x)]) if x.abs() < 1e-6),
        || format!("{d:?}"),
    );
    let t = texts(&mut c, "SELECT VEC_TOTEXT(VEC_FROMTEXT('[1, 0.5, -2]'))").await;
    ck.ok(
        &format!("{tag}: VEC_FROMTEXT and VEC_TOTEXT"),
        t == Ok(vec![Some("[1,0.5,-2]".into())]),
        || format!("{t:?}"),
    );

    // Sharing off for the database: 39995 with nothing sent; the bundled
    // model still works.
    must(
        &mut c,
        &format!("ALTER DATABASE {db} SPRIG SET (ai.share_data = FALSE)"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let n3 = sent();
    let e = fails(
        &mut c,
        &format!("SELECT AI_SUMMARIZE(body) FROM tickets WHERE id = 2 AND body LIKE '%{run}%'"),
    )
    .await;
    ck.ok(
        &format!("{tag}: ai.share_data off: 39995, nothing sent"),
        e == 39995 && sent() == n3,
        || format!("{e} {}", sent() - n3),
    );
    let local = one_i64(
        &mut c,
        "SELECT COUNT(*) FROM tickets WHERE AI_EMBED(body) IS NOT NULL",
    )
    .await;
    ck.ok(
        &format!("{tag}: the bundled model with sharing off"),
        local == Ok(5),
        || format!("{local:?}"),
    );
    must(
        &mut c,
        &format!("ALTER DATABASE {db} SPRIG SET (ai.share_data = TRUE)"),
    )
    .await;

    // Budgets: spent is 39992, or NULL and warning 39992 under null.
    set(&mut c, "ai.budget.user_tokens_day = 1").await;
    let n4 = sent();
    let e = fails(
        &mut c,
        "SELECT AI_SUMMARIZE(CONCAT(body, ' b1')) FROM tickets WHERE id = 2",
    )
    .await;
    ck.ok(
        &format!("{tag}: budget spent: 39992, nothing sent"),
        e == 39992 && sent() == n4,
        || format!("{e} {}", sent() - n4),
    );
    set(&mut c, "ai.functions.on_budget = 'null'").await;
    let r = c
        .query_drop("UPDATE tickets SET gist = AI_SUMMARIZE(CONCAT(body, ' b2')) WHERE id = 2")
        .await;
    let w: Vec<(String, u16, String)> = c.query("SHOW WARNINGS").await.unwrap_or_default();
    let g = texts(&mut c, "SELECT gist FROM tickets WHERE id = 2").await;
    ck.ok(
        &format!("{tag}: budget spent, on_budget = null: NULL and warning 39992"),
        r.is_ok() && g == Ok(vec![None]) && w.iter().any(|x| x.1 == 39992),
        || format!("{r:?} {g:?} {w:?}"),
    );
    set(
        &mut c,
        "ai.budget.user_tokens_day = 200000, ai.functions.on_budget = 'error'",
    )
    .await;

    // Off: 39991.
    set(&mut c, "ai.functions.enabled = FALSE").await;
    let e = fails(&mut c, "SELECT AI_EMBED('x')").await;
    ck.ok(
        &format!("{tag}: ai.functions.enabled off: 39991"),
        e == 39991,
        || format!("{e}"),
    );
    set(&mut c, "ai.functions.enabled = TRUE").await;
}

/// Embedded columns on one port.
async fn embedded_on(addr: &str, db: &str, ck: &mut Checks, tag: &str) {
    let mut c = login(addr).await;
    must(&mut c, &format!("USE {db}")).await;
    let src = "CONCAT(title, ' ', body)";
    // CREATE TABLE with the clause, then rows.
    let r = c
        .query_drop(format!(
            "CREATE TABLE docs (id INT PRIMARY KEY, title VARCHAR(100), body TEXT, \
             emb VECTOR(384) GENERATED BY EMBEDDING({src}) USING MODEL 'local:bge-small')"
        ))
        .await;
    ck.ok(
        &format!("{tag}: CREATE TABLE with an embedded column"),
        r.is_ok(),
        || format!("{r:?}"),
    );
    let mut vals = Vec::new();
    for i in 1..=30 {
        vals.push(format!(
            "({i}, 'note {i}', 'about topic {} and the number {i}')",
            ["shoes", "weather", "databases"][i % 3]
        ));
    }
    must(
        &mut c,
        &format!(
            "INSERT INTO docs (id, title, body) VALUES {}",
            vals.join(", ")
        ),
    )
    .await;
    let left = settled(&mut c, "docs", src, 60).await;
    ck.ok(
        &format!("{tag}: 30 inserted rows filled with their vectors"),
        left == 0,
        || format!("{left} rows wrong"),
    );
    // SELECT * leaves the hidden columns out.
    let cols = c
        .query_iter("SELECT * FROM docs WHERE id = 1")
        .await
        .map(|r| {
            r.columns_ref()
                .iter()
                .map(|c| c.name_str().into_owned())
                .collect::<Vec<_>>()
        });
    ck.ok(
        &format!("{tag}: SELECT * shows id, title, body, emb"),
        cols.as_ref()
            .is_ok_and(|v| v == &["id", "title", "body", "emb"]),
        || format!("{cols:?}"),
    );
    drop(cols);
    // A search by meaning.
    let best = ids(
        &mut c,
        "SELECT id FROM docs ORDER BY VEC_DISTANCE_COSINE(emb, AI_EMBED('note 7 about topic weather and the number 7')) LIMIT 1",
    )
    .await;
    ck.ok(
        &format!("{tag}: ORDER BY VEC_DISTANCE_COSINE finds the row"),
        best == Ok(vec![7]),
        || format!("{best:?}"),
    );
    // UPDATE, NULL source, DELETE, INSERT after.
    must(
        &mut c,
        "UPDATE docs SET body = 'now about databases only' WHERE id IN (2, 3)",
    )
    .await;
    must(&mut c, "UPDATE docs SET body = NULL WHERE id = 4").await;
    must(&mut c, "DELETE FROM docs WHERE id IN (5, 6)").await;
    must(
        &mut c,
        "INSERT INTO docs (id, title, body) VALUES (31, 'note 31', 'a new row after the backfill')",
    )
    .await;
    let left = settled(&mut c, "docs", src, 30).await;
    let n = one_i64(&mut c, "SELECT COUNT(*) FROM docs").await;
    let null4 = one_i64(
        &mut c,
        "SELECT COUNT(*) FROM docs WHERE id = 4 AND emb IS NULL",
    )
    .await;
    ck.ok(
        &format!("{tag}: UPDATE, a NULL source, DELETE and INSERT followed"),
        left == 0 && n == Ok(29) && null4 == Ok(1),
        || format!("{left} wrong, {n:?} rows, {null4:?}"),
    );
    // ALTER TABLE ... ADD over existing rows: a backfill.
    must(&mut c, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)").await;
    let vals: Vec<String> = (1..=60)
        .map(|i| format!("({i}, 'existing note number {i}')"))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO notes VALUES {}", vals.join(", ")),
    )
    .await;
    let r = c
        .query_drop(
            "ALTER TABLE notes ADD COLUMN emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small'",
        )
        .await;
    let left = settled(&mut c, "notes", "body", 60).await;
    ck.ok(
        &format!("{tag}: ALTER TABLE ... ADD backfills 60 rows"),
        r.is_ok() && left == 0,
        || format!("{r:?} {left} wrong"),
    );
    // Refusals.
    let e = fails(
        &mut c,
        "ALTER TABLE notes MODIFY COLUMN emb VECTOR(384) GENERATED BY EMBEDDING(UPPER(body)) USING MODEL 'local:bge-small'",
    )
    .await;
    ck.ok(
        &format!("{tag}: a new generation is refused (39997)"),
        e == 39997,
        || format!("{e}"),
    );
    let e = fails(
        &mut c,
        "ALTER TABLE notes CHANGE COLUMN emb emb2 VARBINARY(1536)",
    )
    .await;
    ck.ok(
        &format!("{tag}: CHANGE of an embedded column is refused (39997)"),
        e == 39997,
        || format!("{e}"),
    );
    let e = fails(
        &mut c,
        "CREATE TABLE nokey (body TEXT, emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    ck.ok(
        &format!("{tag}: no primary key is refused (39997)"),
        e == 39997,
        || format!("{e}"),
    );
    let e = fails(
        &mut c,
        "ALTER TABLE notes ADD COLUMN e2 VECTOR(384) GENERATED BY EMBEDDING(CONCAT(body, RAND())) USING MODEL 'local:bge-small'",
    )
    .await;
    ck.ok(
        &format!("{tag}: a non-deterministic source is refused"),
        e != 0,
        || format!("{e}"),
    );
    // DROP COLUMN takes the hidden columns.
    must(&mut c, "ALTER TABLE notes DROP COLUMN emb").await;
    let left = one_i64(
        &mut c,
        &format!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 'notes'"
        ),
    )
    .await;
    ck.ok(
        &format!("{tag}: DROP COLUMN takes the hidden columns"),
        left == Ok(2),
        || format!("{left:?}"),
    );
}

/// The runner's writes fire no trigger and leave `ON UPDATE
/// CURRENT_TIMESTAMP` alone; a failing row doesn't stop the column; a
/// user's write to the vector is replaced; `CREATE TABLE ... LIKE` copies
/// are filled; a declarer limited by row-level security is refused.
async fn hardening_on(addr: &str, db: &str, ck: &mut Checks, tag: &str) {
    let mut c = login(addr).await;
    must(&mut c, &format!("USE {db}")).await;
    let epoch = "2000-01-01 00:00:00";
    must(
        &mut c,
        &format!(
            "CREATE TABLE trg (id INT PRIMARY KEY, body TEXT, \
             at DATETIME NOT NULL DEFAULT '{epoch}' ON UPDATE CURRENT_TIMESTAMP, \
             emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small', \
             CONSTRAINT not3 CHECK (emb IS NULL OR id <> 3))"
        ),
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE trg_log (n INT AUTO_INCREMENT PRIMARY KEY, id INT)",
    )
    .await;
    must(
        &mut c,
        "CREATE TRIGGER trg_au AFTER UPDATE ON trg FOR EACH ROW INSERT INTO trg_log (id) VALUES (NEW.id)",
    )
    .await;
    let vals: Vec<String> = (1..=6)
        .map(|i| format!("({i}, 'trigger test row number {i}')"))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO trg (id, body) VALUES {}", vals.join(", ")),
    )
    .await;
    // Row 3 can never take a vector (the CHECK): the others fill anyway.
    let until = Instant::now() + Duration::from_secs(60);
    let mut filled = -1;
    while Instant::now() < until {
        filled = one_i64(
            &mut c,
            "SELECT COUNT(*) FROM trg WHERE id <> 3 AND emb IS NOT NULL AND VEC_DISTANCE_COSINE(emb, AI_EMBED(body)) <= 0.0001",
        )
        .await
        .unwrap_or(-2);
        if filled == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let three = one_i64(
        &mut c,
        "SELECT COUNT(*) FROM trg WHERE id = 3 AND emb IS NULL",
    )
    .await;
    ck.ok(
        &format!("{tag}: a row that can't be written is passed over, the rest are filled"),
        filled == 5 && three == Ok(1),
        || format!("{filled} filled, {three:?}"),
    );
    let fired = one_i64(&mut c, "SELECT COUNT(*) FROM trg_log").await;
    let bumped = one_i64(
        &mut c,
        &format!("SELECT COUNT(*) FROM trg WHERE at <> '{epoch}'"),
    )
    .await;
    ck.ok(
        &format!("{tag}: the runner's writes fire no trigger and leave ON UPDATE columns"),
        fired == Ok(0) && bumped == Ok(0),
        || format!("trigger rows {fired:?}, bumped {bumped:?}"),
    );
    // A client's UPDATE of that table fires the trigger and bumps `at`; the
    // runner's write that follows (the new source's vector) does neither.
    must(
        &mut c,
        "UPDATE trg SET body = 'changed by the user' WHERE id = 1",
    )
    .await;
    let fired = one_i64(&mut c, "SELECT COUNT(*) FROM trg_log").await;
    let until = Instant::now() + Duration::from_secs(30);
    let mut refilled = false;
    while Instant::now() < until {
        refilled = one_i64(
            &mut c,
            "SELECT COUNT(*) FROM trg WHERE id = 1 AND VEC_DISTANCE_COSINE(emb, AI_EMBED(body)) <= 0.0001",
        )
        .await
            == Ok(1);
        if refilled {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let fired_after = one_i64(&mut c, "SELECT COUNT(*) FROM trg_log").await;
    let bumped = one_i64(
        &mut c,
        &format!("SELECT COUNT(*) FROM trg WHERE id = 1 AND at <> '{epoch}'"),
    )
    .await;
    ck.ok(
        &format!("{tag}: a client's UPDATE fires the trigger, the runner's refill doesn't"),
        fired == Ok(1) && refilled && fired_after == Ok(1) && bumped == Ok(1),
        || format!("{fired:?} {refilled} {fired_after:?} {bumped:?}"),
    );
    // CREATE TABLE ... LIKE: the copy has its own records and is filled.
    let r = c.query_drop("CREATE TABLE trg_copy LIKE trg").await;
    must(
        &mut c,
        "INSERT INTO trg_copy (id, body) VALUES (1, 'a copy row'), (2, 'another copy row')",
    )
    .await;
    let left = settled(&mut c, "trg_copy", "body", 30).await;
    ck.ok(
        &format!("{tag}: CREATE TABLE ... LIKE copies are filled"),
        r.is_ok() && left == 0,
        || format!("{r:?} {left} wrong"),
    );
    // A user's write to the vector is replaced by the model's.
    must(&mut c, "UPDATE trg_copy SET emb = X'0000803F' WHERE id = 2").await;
    let until = Instant::now() + Duration::from_secs(30);
    let mut ok = false;
    while Instant::now() < until {
        ok = one_i64(
            &mut c,
            "SELECT COUNT(*) FROM trg_copy WHERE id = 2 AND LENGTH(emb) = 1536 AND VEC_DISTANCE_COSINE(emb, AI_EMBED(body)) <= 0.0001",
        )
        .await
            == Ok(1);
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    ck.ok(
        &format!("{tag}: a vector written by hand is embedded again"),
        ok,
        String::new,
    );
    // (The copy's CHECK reads the column: it goes first.)
    must(&mut c, "ALTER TABLE trg_copy DROP CONSTRAINT not3").await;
    let r = c.query_drop("ALTER TABLE trg_copy DROP COLUMN emb").await;
    let n = one_i64(
        &mut c,
        &format!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 'trg_copy'"
        ),
    )
    .await;
    ck.ok(
        &format!("{tag}: DROP COLUMN on the copy takes its hidden columns"),
        r.is_ok() && n == Ok(3),
        || format!("{r:?} {n:?}"),
    );
    // Re-running a declaration against a table that is there leaves it and
    // its data alone: IF NOT EXISTS does nothing, without it 1050.
    must(
        &mut c,
        "CREATE TABLE hid (id INT PRIMARY KEY, body TEXT, \
         emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    must(
        &mut c,
        "INSERT INTO hid (id, body) VALUES (1, 'a row before the re-run'), (2, 'another one')",
    )
    .await;
    let left = settled(&mut c, "hid", "body", 30).await;
    let again = c
        .query_drop(
            "CREATE TABLE IF NOT EXISTS hid (id INT PRIMARY KEY, body TEXT, \
             emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
        )
        .await;
    let like_again = c
        .query_drop("CREATE TABLE IF NOT EXISTS hid LIKE trg")
        .await;
    let plain = fails(
        &mut c,
        "CREATE TABLE hid (id INT PRIMARY KEY, body TEXT, \
         emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    let plain_like = fails(&mut c, "CREATE TABLE hid LIKE trg").await;
    let n = one_i64(&mut c, "SELECT COUNT(*) FROM hid").await;
    must(
        &mut c,
        "UPDATE hid SET body = 'changed after the re-runs' WHERE id = 1",
    )
    .await;
    let left2 = settled(&mut c, "hid", "body", 30).await;
    ck.ok(
        &format!(
            "{tag}: re-running CREATE (IF NOT EXISTS, LIKE, or 1050) leaves the table and its data"
        ),
        left == 0
            && again.is_ok()
            && like_again.is_ok()
            && plain == 1050
            && plain_like == 1050
            && n == Ok(2)
            && left2 == 0,
        || format!("{again:?} {like_again:?} {plain} {plain_like} {n:?} {left} {left2}"),
    );
    // A user column of the embedded column's name stays the user's.
    must(
        &mut c,
        "CREATE TABLE usercol (id INT PRIMARY KEY, body TEXT, emb VARBINARY(16))",
    )
    .await;
    must(&mut c, "INSERT INTO usercol VALUES (1, 'x', X'01')").await;
    let r = c
        .query_drop(
            "CREATE TABLE IF NOT EXISTS usercol (id INT PRIMARY KEY, body TEXT, \
             emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
        )
        .await;
    let r2 = c
        .query_drop("CREATE TABLE IF NOT EXISTS usercol LIKE trg")
        .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let kept = one_i64(&mut c, "SELECT COUNT(*) FROM usercol WHERE emb = X'01'").await;
    ck.ok(
        &format!("{tag}: IF NOT EXISTS on a table with a user column of that name changes nothing"),
        r.is_ok() && r2.is_ok() && kept == Ok(1),
        || format!("{r:?} {r2:?} {kept:?}"),
    );
    // DROP COLUMN after the re-runs: one column, its hidden ones, gone.
    let r = c.query_drop("ALTER TABLE hid DROP COLUMN emb").await;
    let cols = one_i64(
        &mut c,
        &format!(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 'hid'"
        ),
    )
    .await;
    must(
        &mut c,
        "UPDATE hid SET body = 'after the drop' WHERE id = 2",
    )
    .await;
    ck.ok(
        &format!("{tag}: DROP COLUMN after the re-runs takes the column whole"),
        r.is_ok() && cols == Ok(2),
        || format!("{r:?} {cols:?}"),
    );
    // A client can't write the hidden columns itself (no planted hash).
    must(
        &mut c,
        "CREATE TABLE plant (id INT PRIMARY KEY, body TEXT, \
         emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    must(
        &mut c,
        "INSERT INTO plant (id, body) VALUES (1, 'planted?')",
    )
    .await;
    let e1 = fails(
        &mut c,
        "UPDATE plant SET _sprig_emb_emb_hash = X'00112233445566778899AABBCCDDEEFF' WHERE id = 1",
    )
    .await;
    let e2 = fails(
        &mut c,
        "INSERT INTO plant (id, body, _sprig_emb_emb_hash) VALUES (2, 'x', X'00112233445566778899AABBCCDDEEFF')",
    )
    .await;
    let e3 = fails(
        &mut c,
        "UPDATE plant SET _sprig_emb_emb_src = 'x' WHERE id = 1",
    )
    .await;
    let e4 = fails(
        &mut c,
        "UPDATE plant SET emb = X'0000803F', _sprig_emb_emb_hash = NULL WHERE id = 1",
    )
    .await;
    ck.ok(
        &format!("{tag}: client DML can't name the hidden hash or source columns"),
        e1 != 0 && e2 != 0 && e3 != 0 && e4 != 0,
        || format!("{e1} {e2} {e3} {e4}"),
    );
    // Row-level security: a user it limits can't declare one.
    must(
        &mut c,
        "CREATE TABLE sec (id INT PRIMARY KEY, owner VARCHAR(20), body TEXT)",
    )
    .await;
    must(&mut c, "DROP USER IF EXISTS 'aiereader'@'%'").await;
    must(
        &mut c,
        "CREATE USER 'aiereader'@'%' IDENTIFIED BY 'Aie-reader-Passw0rd'",
    )
    .await;
    must(
        &mut c,
        &format!("GRANT SELECT, UPDATE, ALTER, CREATE ON {db}.* TO 'aiereader'@'%'"),
    )
    .await;
    must(
        &mut c,
        "CREATE POLICY own_rows ON sec TO aiereader USING (owner = 'aiereader')",
    )
    .await;
    must(&mut c, "ALTER TABLE sec ENABLE ROW LEVEL SECURITY").await;
    // Forced: the reader's ALTER right doesn't exempt it (as a manager of
    // an unforced table would be).
    must(&mut c, "ALTER TABLE sec FORCE ROW LEVEL SECURITY").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let mut r = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!(
                "{}.aiereader",
                var("SPRIG_TEST_AIE_TENANT", "acme")
            )))
            .pass(Some("Aie-reader-Passw0rd"))
            .prefer_socket(false)
            .ssl_opts(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
    )
    .await
    .unwrap();
    must(&mut r, &format!("USE {db}")).await;
    let e = fails(
        &mut r,
        "ALTER TABLE sec ADD COLUMN emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small'",
    )
    .await;
    ck.ok(
        &format!("{tag}: a declarer limited by row-level security is refused (39997)"),
        e == 39997,
        || format!("{e}"),
    );
    let _ = r.disconnect().await;
    must(&mut c, "ALTER TABLE sec DISABLE ROW LEVEL SECURITY").await;
    must(&mut c, "DROP POLICY own_rows ON sec").await;
    must(&mut c, "DROP USER 'aiereader'@'%'").await;
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AIE_GATEWAY, SPRIG_TEST_AIE_TENANT, SPRIG_TEST_AIE_USER, SPRIG_TEST_AIE_PASSWORD, SPRIG_TEST_AIE_STUB, ...; tools/live/README.md"]
async fn ai_engine_end_to_end() {
    let addr = skip::var(GATEWAY).expect("SPRIG_TEST_AIE_GATEWAY");
    let maria = var("SPRIG_TEST_AIE_MARIADB", &addr);
    let stub_addr = var("SPRIG_TEST_AIE_STUB", "127.0.0.1:23499");
    let seen: Seen = Arc::default();
    stub(&stub_addr, Arc::clone(&seen)).await;
    let mut ck = Checks::default();
    functions_on(&addr, "aie_live", &seen, &mut ck, "3306").await;
    functions_on(&maria, "aie_live2", &seen, &mut ck, "3307").await;
    embedded_on(&addr, "aie_live", &mut ck, "3306").await;
    embedded_on(&maria, "aie_live2", &mut ck, "3307").await;
    hardening_on(&addr, "aie_live", &mut ck, "3306").await;
    // ASK on the SQL path (ai-core's): reported, not a failure of this test.
    let mut c = login(&addr).await;
    must(&mut c, "USE aie_live").await;
    let r: Result<Vec<mysql_async::Row>, _> = c.query("ASK 'how many tickets are there?'").await;
    eprintln!(
        "  note ASK on an engine tenant: {:?}",
        r.as_ref().map(Vec::len).map_err(code)
    );
    eprintln!("  provider calls: {}", seen.lock().unwrap().len());
    ck.done("ai_engine_end_to_end");
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AIE_GATEWAY, SPRIG_TEST_AIE_TENANT, SPRIG_TEST_AIE_USER, SPRIG_TEST_AIE_PASSWORD, SPRIG_TEST_AIE_STUB, ...; tools/live/README.md"]
async fn ai_engine_restart_part_1() {
    let addr = skip::var(GATEWAY).expect("SPRIG_TEST_AIE_GATEWAY");
    let mut c = login(&addr).await;
    set(&mut c, "ai.functions.enabled = TRUE").await;
    must(&mut c, "DROP DATABASE IF EXISTS aie_restart").await;
    must(&mut c, "CREATE DATABASE aie_restart").await;
    must(&mut c, "USE aie_restart").await;
    must(
        &mut c,
        "CREATE TABLE r (id INT PRIMARY KEY, body TEXT, \
         emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    for b in 0..8 {
        let vals: Vec<String> = (1..=100)
            .map(|i| {
                let id = b * 100 + i;
                format!("({id}, 'row {id} before the restart')")
            })
            .collect();
        must(
            &mut c,
            &format!("INSERT INTO r (id, body) VALUES {}", vals.join(", ")),
        )
        .await;
    }
    let filled = one_i64(&mut c, "SELECT COUNT(*) FROM r WHERE emb IS NOT NULL").await;
    eprintln!("ai_engine_restart_part_1: 800 rows, {filled:?} filled at the kill");
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_AIE_GATEWAY, SPRIG_TEST_AIE_TENANT, SPRIG_TEST_AIE_USER, SPRIG_TEST_AIE_PASSWORD, SPRIG_TEST_AIE_STUB, ...; tools/live/README.md"]
async fn ai_engine_restart_part_2() {
    let addr = skip::var(GATEWAY).expect("SPRIG_TEST_AIE_GATEWAY");
    let mut ck = Checks::default();
    let mut c = login(&addr).await;
    must(&mut c, "USE aie_restart").await;
    let left = settled(&mut c, "r", "body", 180).await;
    ck.ok(
        "after the restart: all 800 rows filled with their vectors",
        left == 0,
        || format!("{left} wrong"),
    );
    must(
        &mut c,
        "UPDATE r SET body = CONCAT(body, ' and after it') WHERE id <= 20",
    )
    .await;
    must(&mut c, "DELETE FROM r WHERE id > 790").await;
    must(
        &mut c,
        "INSERT INTO r (id, body) VALUES (1001, 'a row after the restart')",
    )
    .await;
    let left = settled(&mut c, "r", "body", 60).await;
    let n = one_i64(&mut c, "SELECT COUNT(*) FROM r").await;
    ck.ok(
        "after the restart: changes are followed",
        left == 0 && n == Ok(791),
        || format!("{left} wrong, {n:?} rows"),
    );
    ck.done("ai_engine_restart_part_2");
}
