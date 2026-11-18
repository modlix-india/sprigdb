//! The AI writers of a tenant on Sprig 2PC's relay path, on a deployment
//! with relay agents and the account rule on: an embedded column's vector
//! `UPDATE`s and a retrieval index's locking work (the reconcile's guard
//! and its chunk writes, the clean-up of deleted rows, the sweep after a
//! `TRUNCATE` and the routing hash catch-up after the table is sharded)
//! must reach the nodes through a relay, never straight from the worker.
//! Then a relay writer's agent is killed (`kill -9`) while rows change,
//! and once it is back every vector must be its row's current text's and
//! every row's chunks one whole version of the row: nothing half-applied,
//! nothing left behind.
//!
//! Where a statement came from is read from MariaDB's general log (to a
//! table) on every node: the relays' hosts are the hosts of the tenant's
//! per-user accounts other than `'%'` (the account rule), so a write from
//! any other host went around the relay.
//!
//! ```sh
//! export SPRIG_TEST_AIR_SQL=127.0.0.1:49506            # the gateway
//! export SPRIG_TEST_AIR_TENANT=acme SPRIG_TEST_AIR_TENANT_ID=<id>  # user app
//! export SPRIG_TEST_AIR_NODES=mysql://root:...@127.0.0.1:49511,...  # every node, as root
//! export SPRIG_EMBED_MODEL_DIR=target/models/bge-small-en-v1.5
//! # optional: shards the table after `SPRIG PIN` (the routing hash catch-up)
//! export SPRIG_TEST_AIR_SHARD_CMD='sprigctl ... advisor run/apply'
//! # optional: kill -9 the relay writer's agent, and start it again
//! export SPRIG_TEST_AIR_KILL='.../deploy.sh kill9 agent-w1'
//! export SPRIG_TEST_AIR_START='.../deploy.sh start agent-w1'
//! export SPRIG_TEST_AIR_ROUNDS=3     # kills, each followed by the checks (default 1)
//! cargo test -p sprig-gateway --test ai_relay_live -- --nocapture
//! ```
//!
//! Works in the database `ai_relay_live`, dropped afterwards (kept when a
//! check failed, with the stale rows printed). It turns
//! the general log on and off on the nodes it is given: use a private
//! deployment.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::many_single_char_names,
    clippy::format_push_string
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "ai_relay_live";

struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl std::fmt::Display) {
        if cond {
            eprintln!("ok   {what}: {detail}");
        } else {
            eprintln!("FAIL {what}: {detail}");
            self.failed.push(format!("{what}: {detail}"));
        }
    }
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.trim().is_empty())
}

fn gateway_opts(addr: &str, tenant: &str) -> mysql_async::Opts {
    let (host, port) = addr.split_once(':').unwrap();
    mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some(
            var("SPRIG_TEST_AIR_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into()),
        ))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true))
        .into()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

/// Retries `s` on a fresh connection while the relay recovers (39030
/// and the like); whether it went through.
async fn try_write(opts: &mysql_async::Opts, s: &str) -> bool {
    let Ok(mut c) = mysql_async::Conn::new(opts.clone()).await else {
        return false;
    };
    let r = c.query_drop(format!("USE {DB}")).await.is_ok() && c.query_drop(s).await.is_ok();
    let _ = c.disconnect().await;
    r
}

fn parse_vec(s: &str) -> Vec<f32> {
    s.trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .filter(|x| !x.trim().is_empty())
        .map(|x| x.trim().parse().unwrap())
        .collect()
}

fn cos(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

const WORDS: [&str; 24] = [
    "harbour", "lantern", "granite", "orchard", "meadow", "copper", "falcon", "river", "summit",
    "willow", "quarry", "beacon", "thistle", "glacier", "saffron", "compass", "ember", "canyon",
    "juniper", "marble", "tundra", "cobalt", "fjord", "prairie",
];

/// A text of `words` words, different for each `(seed, version)`.
fn text(seed: u64, version: u64, words: usize) -> String {
    let mut x = seed.wrapping_mul(6_364_136_223_846_793_005) ^ version.wrapping_add(1);
    (0..words)
        .map(|_| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            WORDS[((x >> 33) % WORDS.len() as u64) as usize]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A note's body: 10 to 130 words, so some rows have several chunks.
fn note(id: u64, version: u64) -> String {
    text(
        id + 10_000,
        version,
        10 + ((id * 37 + version * 11) % 121) as usize,
    )
}

/// Rows of `air_docs` whose vector isn't the model's embedding of their
/// `body`, as (id, cosine; -1 for a missing vector).
async fn wrong_vectors(
    c: &mut mysql_async::Conn,
    model: &sprig_embed::Embedder,
) -> Vec<(i64, f32)> {
    let rows: Vec<(i64, String, Option<String>)> = c
        .query(format!(
            "SELECT id, body, VEC_ToText(emb) FROM {DB}.air_docs ORDER BY id"
        ))
        .await
        .unwrap();
    let mut wrong = Vec::new();
    let mut have = Vec::new();
    for (id, body, v) in rows {
        match v {
            Some(v) => have.push((id, body, parse_vec(&v))),
            None => wrong.push((id, -1.0)),
        }
    }
    for chunk in have.chunks(64) {
        let refs: Vec<&str> = chunk.iter().map(|(_, s, _)| s.as_str()).collect();
        let want = model.embed_passages(&refs).unwrap();
        for ((id, _, got), w) in chunk.iter().zip(want) {
            let c = cos(got, &w);
            if c < 0.9999 {
                wrong.push((*id, c));
            }
        }
    }
    wrong
}

/// The chunk tables of retrieval index `id`, as (node, schema), over
/// every node (a sharded table's shards may sit on several clusters; on
/// Galera every node of a cluster holds the same, so the first node of
/// each cluster that has the schema is read).
async fn chunk_tables(nodes: &mut [mysql_async::Conn], id: u64) -> Vec<(usize, String)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (i, n) in nodes.iter_mut().enumerate() {
        let schemas: Vec<String> = n
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ?",
                (format!("__sprig_rx_{id}"),),
            )
            .await
            .unwrap();
        for s in schemas {
            if seen.insert(s.clone()) {
                out.push((i, s));
            }
        }
    }
    out
}

/// What is wrong with the chunks of index `id` against `air_notes` in the
/// same shard schemas: rows with text but no chunks, chunks of rows gone,
/// rows whose chunks aren't one whole current version (a chunk's
/// `src_hash` not the row's now, or chunk numbers with gaps), and chunks
/// whose routing hash isn't their row's. Empty when all is well.
async fn chunk_faults(nodes: &mut [mysql_async::Conn], id: u64) -> BTreeMap<&'static str, u64> {
    let mut f: BTreeMap<&'static str, u64> = BTreeMap::new();
    let tables = chunk_tables(nodes, id).await;
    if tables.is_empty() {
        f.insert("no chunk table", 1);
    }
    for (i, s) in tables {
        let n = &mut nodes[i];
        let has_src: u64 = n
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'air_notes'",
                (s.as_str(),),
            )
            .await
            .unwrap()
            .unwrap_or(0);
        if has_src == 0 {
            // A chunk table left in a schema the table moved out of is
            // dropped a minute later; it is not the index's answer.
            continue;
        }
        let has_rk: u64 = n
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'air_notes' AND COLUMN_NAME = '_sprig_rk'",
                (s.as_str(),),
            )
            .await
            .unwrap()
            .unwrap_or(0);
        let x = format!("`{s}`.`__sprig_rx_{id}`");
        let m = format!("`{s}`.`air_notes`");
        let q = [
            (
                "rows without chunks",
                format!(
                    "SELECT COUNT(*) FROM {m} m WHERE TRIM(m.body) <> '' AND NOT EXISTS \
                     (SELECT 1 FROM {x} x WHERE x.k_id = m.id)"
                ),
            ),
            (
                "chunks of rows gone",
                format!(
                    "SELECT COUNT(*) FROM {x} x WHERE NOT EXISTS (SELECT 1 FROM {m} m WHERE m.id = x.k_id)"
                ),
            ),
            (
                "chunks not of the row's current text",
                format!(
                    "SELECT COUNT(*) FROM {x} x JOIN {m} m ON m.id = x.k_id \
                     WHERE x.src_hash <> UNHEX(MD5(JSON_ARRAY(m.body, NULL)))"
                ),
            ),
            (
                "rows whose chunk numbers have gaps",
                format!(
                    "SELECT COUNT(*) FROM (SELECT k_id FROM {x} GROUP BY k_id \
                     HAVING COUNT(*) <> MAX(chunk_no) + 1) g"
                ),
            ),
        ];
        for (what, sql) in q {
            let k: u64 = n.query_first(sql).await.unwrap().unwrap_or(0);
            if k > 0 {
                *f.entry(what).or_default() += k;
            }
        }
        if has_rk > 0 {
            let k: u64 = n
                .query_first(format!(
                    "SELECT COUNT(*) FROM {x} x JOIN {m} m ON m.id = x.k_id \
                     WHERE NOT (x._sprig_rk <=> m._sprig_rk)"
                ))
                .await
                .unwrap()
                .unwrap_or(0);
            if k > 0 {
                *f.entry("chunks whose routing hash isn't their row's")
                    .or_default() += k;
            }
        }
    }
    f
}

/// The index's id in the tenant's catalog.
async fn index_id(nodes: &mut [mysql_async::Conn], tid: &str, name: &str) -> Option<u64> {
    for n in nodes.iter_mut() {
        let id: Option<u64> = n
            .exec_first(
                format!(
                    "SELECT index_id FROM `t{tid}_sprig`.ai_retrieval_indexes WHERE name = ? \
                     ORDER BY index_id DESC"
                ),
                (name,),
            )
            .await
            .ok()
            .flatten();
        if id.is_some() {
            return id;
        }
    }
    None
}

async fn index_state(c: &mut mysql_async::Conn, name: &str) -> String {
    let rows: Vec<mysql_async::Row> = c
        .query(format!("SHOW RETRIEVAL INDEXES FROM {DB}"))
        .await
        .unwrap_or_default();
    rows.into_iter()
        .find(|r| r.get::<String, _>(0).as_deref() == Some(name))
        .and_then(|r| r.get::<String, _>(4))
        .unwrap_or_default()
}

/// Waits until both the vectors and the chunks are right; the time it
/// took, and what was still wrong at the end.
async fn wait_caught_up(
    c: &mut mysql_async::Conn,
    nodes: &mut [mysql_async::Conn],
    model: &sprig_embed::Embedder,
    id: u64,
    limit: Duration,
) -> (Option<Duration>, String) {
    let t = Instant::now();
    loop {
        let wrong = wrong_vectors(c, model).await;
        let faults = chunk_faults(nodes, id).await;
        if wrong.is_empty() && faults.is_empty() {
            return (Some(t.elapsed()), String::new());
        }
        if t.elapsed() > limit {
            let w: Vec<_> = wrong.iter().take(5).collect();
            return (
                None,
                format!("{} wrong vectors {w:?}; chunks {faults:?}", wrong.len()),
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// The hosts of the tenant's per-user accounts other than `'%'` on a
/// node: its relays' hosts under the account rule.
async fn relay_hosts(n: &mut mysql_async::Conn, tid: &str) -> BTreeSet<String> {
    n.query::<String, _>(format!(
        "SELECT DISTINCT Host FROM mysql.global_priv WHERE User LIKE 't{tid}\\_u%' AND Host <> '%'"
    ))
    .await
    .unwrap()
    .into_iter()
    .collect()
}

async fn general_log(nodes: &mut [mysql_async::Conn], on: bool) {
    for n in nodes.iter_mut() {
        if on {
            must(n, "SET GLOBAL log_output = 'TABLE'").await;
            must(n, "TRUNCATE mysql.general_log").await;
            must(n, "SET GLOBAL general_log = 1").await;
        } else {
            must(n, "SET GLOBAL general_log = 0").await;
        }
    }
}

/// The AI writers' statement kinds this test looks for.
fn kind(stmt: &str) -> Option<&'static str> {
    let s = stmt.trim_start();
    let up = |p: &str| s.get(..p.len()).is_some_and(|x| x.eq_ignore_ascii_case(p));
    if s.contains("air_docs") && up("UPDATE") && s.contains("_sprig_emb_") {
        return Some("embedded column: vector UPDATE");
    }
    if !s.contains("__sprig_rx_") && s.contains("air_notes") && s.contains("LOCK IN SHARE MODE") {
        return Some("retrieval: reconcile guard");
    }
    if s.contains("__sprig_rx_") {
        if up("UPDATE") && s.contains("_sprig_rk") && s.contains("air_notes") {
            return Some("retrieval: routing hash catch-up");
        }
        if up("DELETE") && s.contains("WHERE NOT EXISTS") {
            return Some("retrieval: sweep");
        }
        if up("DELETE") && s.contains("AND NOT EXISTS") {
            return Some("retrieval: clean-up of deleted rows");
        }
        if up("DELETE") || up("INSERT") {
            return Some("retrieval: chunk writes");
        }
        if up("UPDATE") && s.contains("_sprig_rk") {
            return Some("retrieval: chunk routing hash under the guard");
        }
    }
    None
}

/// Per kind, the statements from a relay's host and from anywhere else
/// (the general log of every node, root sessions only: the tenant's own
/// sessions use its accounts).
async fn writers(
    nodes: &mut [mysql_async::Conn],
    tid: &str,
) -> (BTreeMap<&'static str, (u64, u64)>, Vec<String>) {
    let mut out: BTreeMap<&'static str, (u64, u64)> = BTreeMap::new();
    let mut direct = Vec::new();
    for n in nodes.iter_mut() {
        let relays = relay_hosts(n, tid).await;
        let rows: Vec<(String, Vec<u8>)> = n
            .query(
                "SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(user_host, '[', -1), ']', 1), argument \
                 FROM mysql.general_log WHERE command_type IN ('Query', 'Execute') \
                 AND user_host LIKE 'root%' \
                 AND (argument LIKE '%air\\_docs%' OR argument LIKE '%air\\_notes%' \
                      OR argument LIKE '%\\_\\_sprig\\_rx\\_%')",
            )
            .await
            .unwrap();
        for (host, arg) in rows {
            let s = String::from_utf8_lossy(&arg);
            let Some(k) = kind(&s) else { continue };
            let e = out.entry(k).or_default();
            if relays.contains(&host) {
                e.0 += 1;
            } else {
                e.1 += 1;
                if direct.len() < 10 {
                    direct.push(format!(
                        "{host}: {}",
                        s.chars().take(200).collect::<String>()
                    ));
                }
            }
        }
        must(n, "TRUNCATE mysql.general_log").await;
        must(n, "SET GLOBAL log_output = 'FILE'").await;
    }
    (out, direct)
}

fn sh(cmd: &str) {
    let st = std::process::Command::new("sh")
        .args(["-c", cmd])
        .status()
        .unwrap();
    eprintln!("     $ {cmd}: {st}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ai_writers_go_through_the_relay_and_survive_a_relay_kill() {
    let (Some(addr), Some(tenant), Some(tid), Some(nodes_v)) = (
        var("SPRIG_TEST_AIR_SQL"),
        var("SPRIG_TEST_AIR_TENANT"),
        var("SPRIG_TEST_AIR_TENANT_ID"),
        var("SPRIG_TEST_AIR_NODES"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_AIR_SQL, SPRIG_TEST_AIR_TENANT, SPRIG_TEST_AIR_TENANT_ID and SPRIG_TEST_AIR_NODES"
        );
        return;
    };
    let dir = var("SPRIG_EMBED_MODEL_DIR").expect("SPRIG_EMBED_MODEL_DIR");
    let model = sprig_embed::Embedder::load(std::path::Path::new(&dir)).unwrap();
    let opts = gateway_opts(&addr, &tenant);
    let mut c = mysql_async::Conn::new(opts.clone()).await.unwrap();
    let mut nodes = Vec::new();
    for u in nodes_v.split(',') {
        nodes.push(mysql_async::Conn::from_url(u.trim()).await.unwrap());
    }
    let mut ck = Checks { failed: Vec::new() };

    // ---- setup: an embedded column filled by a backfill, a retrieval index
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    must(
        &mut c,
        "CREATE TABLE air_docs (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await;
    let v: Vec<String> = (1..=100u64)
        .map(|i| format!("({i}, '{}')", text(i, 0, 12)))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO air_docs VALUES {}", v.join(", ")),
    )
    .await;
    must(
        &mut c,
        "ALTER TABLE air_docs ADD COLUMN emb VECTOR(384) \
         GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small'",
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE air_notes (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await;
    let v: Vec<String> = (1..=120u64)
        .map(|i| format!("({i}, '{}')", note(i, 0)))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO air_notes VALUES {}", v.join(", ")),
    )
    .await;
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX air_nx ON air_notes (body) CHUNK BY TOKENS 60 \
         EMBED WITH MODEL 'local:bge-small'",
    )
    .await;
    let id = index_id(&mut nodes, &tid, "air_nx")
        .await
        .expect("index id");
    let t = Instant::now();
    while index_state(&mut c, "air_nx").await != "ready" && t.elapsed() < Duration::from_secs(300) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (done, why) =
        wait_caught_up(&mut c, &mut nodes, &model, id, Duration::from_secs(300)).await;
    ck.ok(
        "setup: the backfill and the index fill are complete and right",
        done.is_some(),
        format!("{:.1} s {why}", t.elapsed().as_secs_f64()),
    );

    // ---- 1. every AI writer's locking work arrives from a relay's host
    general_log(&mut nodes, true).await;
    for i in 1..=40u64 {
        must(
            &mut c,
            &format!(
                "UPDATE air_docs SET body = '{}' WHERE id = {i}",
                text(i, 1, 12)
            ),
        )
        .await;
    }
    for i in 1..=30u64 {
        must(
            &mut c,
            &format!(
                "UPDATE air_notes SET body = '{}' WHERE id = {i}",
                note(i, 1)
            ),
        )
        .await;
    }
    must(&mut c, "DELETE FROM air_notes WHERE id > 110").await;
    let v: Vec<String> = (121..=130u64)
        .map(|i| format!("({i}, '{}')", note(i, 0)))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO air_notes VALUES {}", v.join(", ")),
    )
    .await;
    let (done, why) =
        wait_caught_up(&mut c, &mut nodes, &model, id, Duration::from_secs(180)).await;
    ck.ok(
        "updates, deletes and inserts are applied",
        done.is_some(),
        format!("{done:?} {why}"),
    );
    // The routing hash catch-up: chunks written before the table was
    // sharded take their row's hash once it is.
    let sharded = if let Some(cmd) = var("SPRIG_TEST_AIR_SHARD_CMD") {
        must(&mut c, "SPRIG PIN air_notes SHARD KEY (id)").await;
        sh(&cmd);
        let (done, why) =
            wait_caught_up(&mut c, &mut nodes, &model, id, Duration::from_secs(240)).await;
        ck.ok(
            "after the table is sharded every chunk has its row's routing hash",
            done.is_some(),
            format!("{done:?} {why}"),
        );
        true
    } else {
        eprintln!("     (no SPRIG_TEST_AIR_SHARD_CMD: the routing hash catch-up isn't run)");
        false
    };
    // The sweep: a TRUNCATE empties every shard's chunks.
    must(&mut c, "TRUNCATE TABLE air_notes").await;
    let v: Vec<String> = (1..=60u64)
        .map(|i| format!("({i}, '{}')", note(i, 2)))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO air_notes VALUES {}", v.join(", ")),
    )
    .await;
    let (done, why) =
        wait_caught_up(&mut c, &mut nodes, &model, id, Duration::from_secs(180)).await;
    ck.ok(
        "TRUNCATE and new rows are applied",
        done.is_some(),
        format!("{done:?} {why}"),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    general_log(&mut nodes, false).await;
    let (by_kind, direct) = writers(&mut nodes, &tid).await;
    for (k, (relayed, straight)) in &by_kind {
        eprintln!("     {k}: {relayed} through a relay's host, {straight} from elsewhere");
    }
    let mut want = vec![
        "embedded column: vector UPDATE",
        "retrieval: reconcile guard",
        "retrieval: chunk writes",
        "retrieval: clean-up of deleted rows",
        "retrieval: sweep",
    ];
    if sharded {
        want.push("retrieval: routing hash catch-up");
    }
    for k in want {
        let (relayed, _) = by_kind.get(k).copied().unwrap_or_default();
        ck.ok(
            &format!("{k} seen through a relay"),
            relayed > 0,
            format!("{relayed} statements"),
        );
    }
    ck.ok(
        "no AI writer's locking statement went around the relays",
        direct.is_empty(),
        format!("{direct:#?}"),
    );

    // ---- 2. kill -9 of the relay writer's agent while rows change
    if let (Some(kill), Some(start)) = (var("SPRIG_TEST_AIR_KILL"), var("SPRIG_TEST_AIR_START")) {
        let rounds: u64 = var("SPRIG_TEST_AIR_ROUNDS")
            .and_then(|r| r.parse().ok())
            .unwrap_or(1);
        for round in 1..=rounds {
            let stop = Arc::new(AtomicBool::new(false));
            let (ok, failed) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
            let mut load = Vec::new();
            for w in 0..2u64 {
                let (stop, ok, failed, opts) = (
                    Arc::clone(&stop),
                    Arc::clone(&ok),
                    Arc::clone(&failed),
                    opts.clone(),
                );
                load.push(tokio::spawn(async move {
                    let mut version = round * 1_000_000 + 10 + w * 100_000;
                    while !stop.load(Ordering::Relaxed) {
                        version += 1;
                        let i = 1 + (version * 7919) % 100;
                        let j = 1 + (version * 104_729) % 60;
                        let s = if w == 0 {
                            format!(
                                "UPDATE air_docs SET body = '{}' WHERE id = {i}",
                                text(i, version, 12)
                            )
                        } else if version % 10 == 0 {
                            format!(
                                "INSERT INTO air_notes VALUES ({}, '{}')",
                                1000 + version,
                                note(j, version)
                            )
                        } else if version % 10 == 5 {
                            format!("DELETE FROM air_notes WHERE id = {}", 1000 + version - 5)
                        } else {
                            format!(
                                "UPDATE air_notes SET body = '{}' WHERE id = {j}",
                                note(j, version)
                            )
                        };
                        if try_write(&opts, &s).await {
                            ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            failed.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(300)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                }));
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
            sh(&kill);
            tokio::time::sleep(Duration::from_secs(6)).await;
            sh(&start);
            tokio::time::sleep(Duration::from_secs(30)).await;
            stop.store(true, Ordering::Relaxed);
            for l in load {
                l.await.unwrap();
            }
            let ended = Instant::now();
            eprintln!(
                "     round {round}: {} writes went through, {} failed (the relay recovering)",
                ok.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed)
            );
            c = mysql_async::Conn::new(opts.clone()).await.unwrap();
            let (done, why) =
                wait_caught_up(&mut c, &mut nodes, &model, id, Duration::from_secs(240)).await;
            ck.ok(
                &format!(
                    "round {round}: after the kill every vector is its row's and every row's chunks one whole current version"
                ),
                done.is_some(),
                format!(
                    "caught up {:.1} s after the load ended {why}",
                    ended.elapsed().as_secs_f64()
                ),
            );
            if done.is_none() {
                eprintln!("{}", stale_rows(&mut c, &model).await);
                break;
            }
            let st = index_state(&mut c, "air_nx").await;
            ck.ok(
                &format!("round {round}: the index is ready"),
                st == "ready",
                &st,
            );
        }
    } else {
        eprintln!("     (no SPRIG_TEST_AIR_KILL/START: the relay kill isn't run)");
    }
    // Kept for a look when something failed.
    if ck.failed.is_empty() {
        must(&mut c, &format!("DROP DATABASE {DB}")).await;
    } else {
        eprintln!("     the database {DB} is kept for a look");
    }
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

/// The rows whose vector is wrong: their text, their stored source hash
/// against their text's, as lines.
async fn stale_rows(c: &mut mysql_async::Conn, model: &sprig_embed::Embedder) -> String {
    let wrong = wrong_vectors(c, model).await;
    let _ = c.query_drop("SET @sprig_show_internal = 1").await;
    let mut out = String::new();
    for (id, cos) in wrong {
        let r: Option<(String, Option<String>, String)> = c
            .query_first(format!(
                "SELECT body, HEX(_sprig_emb_emb_hash), HEX(UNHEX(MD5(body))) FROM {DB}.air_docs WHERE id = {id}"
            ))
            .await
            .unwrap_or_default();
        out.push_str(&format!("     stale row {id} (cosine {cos}): {r:?}\n"));
    }
    let _ = c.query_drop("SET @sprig_show_internal = 0").await;
    out
}
