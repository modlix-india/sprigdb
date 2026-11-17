//! Retrieval indexes end to end: `CREATE RETRIEVAL INDEX` through a
//! gateway, the worker's `ai_retrieval` job filling the chunk tables and
//! following inserts, updates and deletes, `RETRIEVE()` and the REST
//! route, `SHOW CREATE` round trips, filters, row-level security hiding
//! hits from a restricted user, a restricted user's ranks unchanged by
//! rows hidden from it, the warning (39827, SQL and REST) when a
//! restricted user's search skips its keyword list, recall@5 on a small
//! corpus, an indexed table moved into its table group, split into two
//! shards and merged back (the chunks moved with their rows, nothing
//! embedded again) and its fused hits against the same table unsharded,
//! its shard moved to another cluster (the chunks with it) and re-keyed
//! (filled again), a kill -9 of the worker in the middle of a fill, and
//! `DROP`.
//!
//! ```sh
//! export SPRIG_TEST_RX_SQL=127.0.0.1:42306          # a gateway's MySQL port
//! export SPRIG_TEST_RX_HTTP=127.0.0.1:42080         # its HTTP listener
//! export SPRIG_TEST_RX_TENANT=acme SPRIG_TEST_RX_USER=admin SPRIG_TEST_RX_PASSWORD=...
//! # the backend, to look at the chunk tables (root)
//! export SPRIG_TEST_RX_BACKEND=mysql://root:...@127.0.0.1:42301
//! # optional: shards rx_live.many (the test makes the pin): run with
//! # `group` it applies the advisor's design (the table moves into its
//! # table group), with `split` it splits the group's shard into two
//! # (e.g. sprigctl advisor run/apply, sprigctl shard split), with `merge`
//! # it merges the two back; each waits
//! # for its jobs to end
//! export SPRIG_TEST_RX_SHARD_CMD='.../rx.sh shard'
//! # optional, with the above: a second cluster's backend (root). Then
//! # `move` moves the group's one shard to that cluster and `rekey`
//! # re-keys the group by `product`, each waiting for its job to end
//! export SPRIG_TEST_RX_BACKEND2=mysql://root:...@127.0.0.1:42321
//! # optional: kills the worker (kill -9) and starts it again
//! export SPRIG_TEST_RX_KILL='.../rx.sh kill-worker'
//! cargo test -p sprig-gateway --test retrieval_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "rx_live";

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

async fn login(addr: &str, user: &str, password: &str) -> mysql_async::Conn {
    let tenant = var("SPRIG_TEST_RX_TENANT", "acme");
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(password))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn run(c: &mut mysql_async::Conn, s: &str) -> Result<(), String> {
    c.query_drop(s).await.map_err(|e| e.to_string())
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = run(c, s).await {
        panic!("{s}: {e}");
    }
}

fn q(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// `RETRIEVE()`'s hits as (`id`, `chunk_no`, `bm25_rank`, `vector_rank`).
async fn retrieve(
    c: &mut mysql_async::Conn,
    index: &str,
    query: &str,
    limit: u32,
    filter: Option<&str>,
) -> Result<Vec<(i64, u32, Option<u32>, Option<u32>)>, String> {
    let f = filter.map_or_else(String::new, |f| format!(", {}", q(f)));
    c.query(format!(
        "SELECT id, chunk_no, bm25_rank, vector_rank FROM RETRIEVE({index}, {}, {limit}{f}) \
         ORDER BY `rank`",
        q(query)
    ))
    .await
    .map_err(|e| e.to_string())
}

/// Every question's top five of `index` as `c` sees them.
async fn all_hits(
    c: &mut mysql_async::Conn,
    index: &str,
) -> Vec<Vec<(i64, u32, Option<u32>, Option<u32>)>> {
    let mut out = Vec::new();
    for (question, _) in QUESTIONS {
        out.push(retrieve(c, index, question, 5, None).await.unwrap());
    }
    out
}

/// Every question's whole keyword list of `index` as `c` sees it:
/// (`id`, `chunk_no`) by `bm25_rank` (from the first 100 hits).
async fn keyword_lists(c: &mut mysql_async::Conn, index: &str) -> Vec<Vec<(u32, i64, u32)>> {
    let mut out = Vec::new();
    for (question, _) in QUESTIONS {
        let mut v: Vec<(u32, i64, u32)> = retrieve(c, index, question, 100, None)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|h| h.2.map(|r| (r, h.0, h.1)))
            .collect();
        v.sort_unstable();
        out.push(v);
    }
    out
}

/// The chunks of index `id` that don't sit with their row (`table`, key
/// `id`) in the same shard schema, those whose `_sprig_rk` isn't their
/// row's, and the chunk tables in a schema without `table`.
async fn misplaced(b: &mut mysql_async::Conn, id: u64, table: &str) -> (u64, u64, u64) {
    let (mut away, mut wrong_rk, mut orphans) = (0, 0, 0);
    for (s, _) in chunk_tables(b, id).await {
        let here: Option<u64> = b
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ?",
                (s.as_str(), table),
            )
            .await
            .unwrap();
        if here.unwrap_or(0) == 0 {
            orphans += 1;
            continue;
        }
        let has_rk: Option<u64> = b
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ? AND COLUMN_NAME = '_sprig_rk'",
                (s.as_str(), table),
            )
            .await
            .unwrap();
        away += b
            .query_first::<u64, _>(format!(
                "SELECT COUNT(*) FROM `{s}`.`__sprig_rx_{id}` x WHERE NOT EXISTS \
                 (SELECT 1 FROM `{s}`.`{table}` m WHERE m.id = x.k_id)"
            ))
            .await
            .unwrap()
            .unwrap_or(99);
        if has_rk.unwrap_or(0) > 0 {
            wrong_rk += b
                .query_first::<u64, _>(format!(
                    "SELECT COUNT(*) FROM `{s}`.`__sprig_rx_{id}` x JOIN `{s}`.`{table}` m \
                     ON m.id = x.k_id WHERE NOT (x._sprig_rk <=> m._sprig_rk)"
                ))
                .await
                .unwrap()
                .unwrap_or(99);
        }
    }
    (away, wrong_rk, orphans)
}

/// Per question, alice's ten best chunks (ids under 1000) of `index` by
/// MariaDB's full-text relevance over its whole chunk table (backend).
async fn relevance(b: &mut Option<mysql_async::Conn>, index: &str) -> Vec<Vec<(i64, u32)>> {
    let Some(bk) = b.as_mut() else {
        return Vec::new();
    };
    let Some(id) = index_id(bk, index).await else {
        return Vec::new();
    };
    let Some((s, _)) = chunk_tables(bk, id).await.into_iter().next() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (question, _) in QUESTIONS {
        let m = format!("MATCH(text, title) AGAINST ({})", q(question));
        out.push(
            bk.query::<(i64, u32), _>(format!(
                "SELECT k_id, chunk_no FROM `{s}`.`__sprig_rx_{id}` WHERE k_id < 1000 AND {m} \
                 ORDER BY {m} DESC, k_id, chunk_no LIMIT 10"
            ))
            .await
            .unwrap_or_default(),
        );
    }
    out
}

async fn ids(c: &mut mysql_async::Conn, index: &str, query: &str, limit: u32) -> Vec<i64> {
    retrieve(c, index, query, limit, None)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|h| h.0)
        .collect()
}

async fn state(c: &mut mysql_async::Conn, index: &str) -> (String, u64) {
    let rows: Vec<mysql_async::Row> = c
        .query(format!("SHOW RETRIEVAL INDEXES FROM {DB}"))
        .await
        .unwrap();
    for r in rows {
        let name: String = r.get(0).unwrap();
        if name == index {
            let st: String = r.get(4).unwrap();
            let n: u64 = r.get(5).unwrap();
            return (st, n);
        }
    }
    ("missing".into(), 0)
}

async fn wait_ready(c: &mut mysql_async::Conn, index: &str, limit: Duration) -> Option<Duration> {
    let t = Instant::now();
    while t.elapsed() < limit {
        if state(c, index).await.0 == "ready" {
            return Some(t.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// Waits until `f` holds, polling every 200 ms.
async fn wait_for<F, Fut>(limit: Duration, mut f: F) -> Option<Duration>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let t = Instant::now();
    while t.elapsed() < limit {
        if f().await {
            return Some(t.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

/// The test corpus: (id, product, title, body), and the questions with the
/// article that answers each.
const CORPUS: &[(i64, &str, &str, &str)] = &[
    (
        1,
        "app",
        "Resetting your password",
        "If you forgot your password, open the sign-in page and choose Forgot password. We email you a link that is valid for one hour; follow it and pick a new password of at least twelve characters.",
    ),
    (
        2,
        "app",
        "Two-factor authentication",
        "Turn on two-factor authentication under Security. Scan the QR code with an authenticator app such as Google Authenticator, then enter the six-digit code to confirm. Keep the recovery codes somewhere safe.",
    ),
    (
        3,
        "billing",
        "Refunds for cancelled orders",
        "When you cancel an order before it ships, the full amount is refunded to the original card within five to seven business days. Orders that already shipped must be returned first.",
    ),
    (
        4,
        "billing",
        "Changing your payment card",
        "To change the card we charge, go to Billing, then Payment methods, add the new card and make it the default. The old card can be removed once no invoice is pending.",
    ),
    (
        5,
        "billing",
        "Downloading invoices",
        "Every invoice is a PDF under Billing, Invoices. Choose a month and press Download. Invoices include your company's tax number when you entered one in the billing profile.",
    ),
    (
        6,
        "shipping",
        "Tracking a parcel",
        "Each shipment gets a tracking number by email when it leaves the warehouse. Open the carrier's website and enter the number to see where the parcel is and when it will arrive.",
    ),
    (
        7,
        "shipping",
        "International shipping and customs",
        "We ship to forty countries. Parcels outside the European Union may be charged import duties and customs fees by the destination country, paid by the recipient on delivery.",
    ),
    (
        8,
        "shipping",
        "Damaged items on arrival",
        "If an item arrives broken or damaged, take photos of the item and the box within forty-eight hours and send them to support. We replace the item or refund it, whichever you prefer.",
    ),
    (
        9,
        "app",
        "Exporting your data",
        "You can export all your notes and files as a ZIP archive from Settings, Privacy, Export data. The archive is prepared in the background and a download link arrives by email.",
    ),
    (
        10,
        "app",
        "Deleting your account",
        "Deleting your account removes your profile, notes and files permanently after a thirty-day grace period. Go to Settings, Account, Delete account and confirm with your password.",
    ),
    (
        11,
        "app",
        "Offline mode on mobile",
        "The mobile app keeps your recent notes on the phone, so you can read and edit them without a connection. Changes sync automatically when the phone is online again.",
    ),
    (
        12,
        "app",
        "Sharing a notebook with a team",
        "Invite colleagues to a notebook with their email address. Editors can change pages, viewers can only read them. The notebook owner can remove people at any time.",
    ),
    (
        13,
        "billing",
        "Annual versus monthly plans",
        "The annual plan costs the same as ten months of the monthly plan. You can switch from monthly to annual at any time; the unused part of the month is credited.",
    ),
    (
        14,
        "billing",
        "Student discount",
        "Students with a university email address get fifty percent off the premium plan. Verify your student status once a year to keep the discount.",
    ),
    (
        15,
        "shipping",
        "Changing the delivery address",
        "You can change the delivery address of an order until it is packed. Open the order, choose Edit address and save; after packing the address can no longer be changed.",
    ),
    (
        16,
        "shipping",
        "Express delivery",
        "Express delivery brings parcels the next working day when you order before two in the afternoon. It is available for most postcodes in the country for a small extra fee.",
    ),
    (
        17,
        "app",
        "Dark mode",
        "Switch the app to a dark theme under Settings, Appearance. You can follow the system setting so the theme changes automatically at night.",
    ),
    (
        18,
        "app",
        "Keyboard shortcuts",
        "Press question mark anywhere in the web app to see every keyboard shortcut. Control K opens the command palette to jump to any notebook or page.",
    ),
    (
        19,
        "billing",
        "Failed payments",
        "When a payment fails, we retry the card three times over a week and email you each time. If every attempt fails, the account moves to the free plan until you update the card.",
    ),
    (
        20,
        "shipping",
        "Returning an item",
        "Items can be returned within thirty days in their original packaging. Print the prepaid return label from your order page and drop the parcel at any post office.",
    ),
];

const QUESTIONS: &[(&str, i64)] = &[
    ("I forgot my password, how do I get back in", 1),
    ("set up an authenticator app for login codes", 2),
    ("money back after cancelling an order", 3),
    ("update the credit card you bill", 4),
    ("where can I get a PDF of my invoice", 5),
    ("where is my package right now", 6),
    ("do I have to pay import duties", 7),
    ("my order arrived broken", 8),
    ("download everything I stored as an archive", 9),
    ("close my account permanently", 10),
    ("can I use the phone app without internet", 11),
    ("let coworkers edit my notebook", 12),
    ("is yearly billing cheaper than monthly", 13),
    ("discount for university students", 14),
    ("ship my order to a different address", 15),
    ("next day delivery", 16),
    ("night theme for the app", 17),
    ("list of hotkeys", 18),
    ("my card was declined, what happens", 19),
    ("send back something I bought", 20),
];

/// A long text of `n` paragraphs about topic `i`.
fn long_text(i: usize, n: usize) -> String {
    let topics = [
        "gardening tomatoes in raised beds",
        "repairing a bicycle chain",
        "baking sourdough bread at home",
        "learning the basics of chess openings",
    ];
    (0..n)
        .map(|p| {
            format!(
                "Paragraph {p} of note {i} is about {}. It explains one step in some detail, \
                 with a few words that make each paragraph a little different: item {i}-{p}.",
                topics[i % topics.len()]
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

async fn backend() -> Option<mysql_async::Conn> {
    backend_of("SPRIG_TEST_RX_BACKEND").await
}

async fn backend_of(var: &str) -> Option<mysql_async::Conn> {
    let url = std::env::var(var).ok()?;
    let opts = mysql_async::Opts::from_url(&url).ok()?;
    mysql_async::Conn::new(opts).await.ok()
}

/// The chunk tables of index `id` on both backends: (cluster 1 or 2,
/// schema, rows).
async fn chunk_tables_both(
    b1: &mut mysql_async::Conn,
    b2: &mut mysql_async::Conn,
    id: u64,
) -> Vec<(u8, String, u64)> {
    let mut out: Vec<(u8, String, u64)> = chunk_tables(b1, id)
        .await
        .into_iter()
        .map(|(s, n)| (1, s, n))
        .collect();
    out.extend(
        chunk_tables(b2, id)
            .await
            .into_iter()
            .map(|(s, n)| (2, s, n)),
    );
    out
}

/// The index's states seen over `secs` seconds, and its rows indexed then.
async fn states_for(
    c: &mut mysql_async::Conn,
    index: &str,
    secs: u64,
) -> (BTreeSet<String>, u64) {
    let mut states = BTreeSet::new();
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(secs) {
        states.insert(state(c, index).await.0);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (states, state(c, index).await.1)
}

/// Runs `cmd` while watching the index's state every 100 ms: whether it
/// passed, and the states seen.
async fn shell_watching(
    c: &mut mysql_async::Conn,
    cmd: &str,
    index: &str,
) -> (bool, BTreeSet<String>) {
    let cmd = cmd.to_owned();
    let mut job = tokio::task::spawn_blocking(move || shell(&cmd));
    let mut states = BTreeSet::new();
    loop {
        states.insert(state(c, index).await.0);
        tokio::select! {
            r = &mut job => return (r.unwrap_or(false), states),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

/// How many questions have the same hits (id, chunk) in their top five,
/// and how many the same hits with every rank.
fn same_hits(
    a: &[Vec<(i64, u32, Option<u32>, Option<u32>)>],
    b: &[Vec<(i64, u32, Option<u32>, Option<u32>)>],
) -> (usize, usize) {
    let sets = a
        .iter()
        .zip(b)
        .filter(|(x, y)| {
            let x: BTreeSet<(i64, u32)> = x.iter().map(|h| (h.0, h.1)).collect();
            let y: BTreeSet<(i64, u32)> = y.iter().map(|h| (h.0, h.1)).collect();
            x == y
        })
        .count();
    let exact = a.iter().zip(b).filter(|(x, y)| x == y).count();
    (sets, exact)
}

/// `RETRIEVE()`'s (`id`, `bm25_rank`) and the statement's warning count
/// and warnings (`SHOW WARNINGS`: code, message).
async fn retrieve_warned(
    c: &mut mysql_async::Conn,
    sql: &str,
) -> (Vec<(i64, Option<u32>)>, u16, Vec<(u16, String)>) {
    let hits: Vec<(i64, Option<u32>)> = c.query(sql).await.unwrap();
    let count = c.get_warnings();
    let shown: Vec<(String, u16, String)> = c.query("SHOW WARNINGS").await.unwrap();
    (
        hits,
        count,
        shown.into_iter().map(|(_, code, m)| (code, m)).collect(),
    )
}

/// The chunk tables of index `id` on the backend: (schema, rows).
async fn chunk_tables(b: &mut mysql_async::Conn, id: u64) -> Vec<(String, u64)> {
    let schemas: Vec<String> = b
        .exec(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ?",
            (format!("__sprig_rx_{id}"),),
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    for s in schemas {
        let n: u64 = b
            .query_first(format!("SELECT COUNT(*) FROM `{s}`.`__sprig_rx_{id}`"))
            .await
            .unwrap()
            .unwrap_or(0);
        out.push((s, n));
    }
    out
}

/// The id of index `name` in the tenant catalog (from the backend).
async fn index_id(b: &mut mysql_async::Conn, name: &str) -> Option<u64> {
    let schemas: Vec<String> = b
        .query(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES \
             WHERE TABLE_NAME = 'ai_retrieval_indexes'",
        )
        .await
        .unwrap();
    for s in schemas {
        let id: Option<u64> = b
            .exec_first(
                format!("SELECT index_id FROM `{s}`.ai_retrieval_indexes WHERE name = ? ORDER BY index_id DESC"),
                (name,),
            )
            .await
            .unwrap();
        if id.is_some() {
            return id;
        }
    }
    None
}

fn shell(cmd: &str) -> bool {
    std::process::Command::new("bash")
        .args(["-c", cmd])
        .status()
        .is_ok_and(|s| s.success())
}

fn rest(
    http: &str,
    user: &str,
    password: &str,
    path: &str,
    body: &serde_json::Value,
) -> (u16, serde_json::Value) {
    use base64::Engine as _;
    let tenant = var("SPRIG_TEST_RX_TENANT", "acme");
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.{user}:{password}"));
    let out = std::process::Command::new("curl")
        .args([
            "-sS",
            "--max-time",
            "30",
            "-w",
            "\n%{http_code}",
            "-H",
            &format!("authorization: Basic {auth}"),
            "-H",
            "content-type: application/json",
            "-d",
            &body.to_string(),
            &format!("http://{http}{path}"),
        ])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (json, code) = text.rsplit_once('\n').unwrap_or(("", "0"));
    (
        code.trim().parse().unwrap_or(0),
        serde_json::from_str(json).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retrieval_indexes_end_to_end() {
    let Ok(addr) = std::env::var("SPRIG_TEST_RX_SQL") else {
        eprintln!("skipped: set SPRIG_TEST_RX_SQL");
        return;
    };
    let user = var("SPRIG_TEST_RX_USER", "admin");
    let password = var("SPRIG_TEST_RX_PASSWORD", "sprig-dev-app");
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut c = login(&addr, &user, &password).await;
    let mut b = backend().await;
    let _ = run(&mut c, "DROP USER IF EXISTS 'rxreader'@'%'").await;
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    must(
        &mut c,
        "CREATE TABLE articles (id INT PRIMARY KEY, product VARCHAR(20), title VARCHAR(200), \
         body TEXT, owner VARCHAR(20) NOT NULL DEFAULT 'alice')",
    )
    .await;
    let values: Vec<String> = CORPUS
        .iter()
        .map(|(id, p, t, body)| {
            format!(
                "({id}, {}, {}, {}, {})",
                q(p),
                q(t),
                q(body),
                q(if id % 2 == 0 { "bob" } else { "alice" })
            )
        })
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO articles VALUES {}", values.join(", ")),
    )
    .await;

    // ---- DDL ----------------------------------------------------------------
    let t = Instant::now();
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX kb ON articles (body) TITLE title FILTERS (product)",
    )
    .await;
    eprintln!("CREATE RETRIEVAL INDEX took {} ms", t.elapsed().as_millis());
    let e = run(&mut c, "CREATE RETRIEVAL INDEX kb ON articles (body)").await;
    ck.ok(
        "a second index of the same name is 1061",
        e.as_ref().is_err_and(|e| e.contains("1061")),
        || format!("{e:?}"),
    );
    ck.ok(
        "IF NOT EXISTS passes",
        run(
            &mut c,
            "CREATE RETRIEVAL INDEX IF NOT EXISTS kb ON articles (body)",
        )
        .await
        .is_ok(),
        String::new,
    );
    for (bad, code) in [
        ("CREATE RETRIEVAL INDEX x ON articles (nope)", "1054"),
        ("CREATE RETRIEVAL INDEX x ON articles (id)", "39997"),
        (
            "CREATE RETRIEVAL INDEX x ON articles (body) FILTERS (body)",
            "39997",
        ),
        (
            "CREATE RETRIEVAL INDEX x ON articles (body) EMBED WITH MODEL 'nope'",
            "39997",
        ),
        ("CREATE RETRIEVAL INDEX x ON nope (body)", "1146"),
        (
            "CREATE RETRIEVAL INDEX x ON articles (body) CHUNK BY TOKENS 9000",
            "1064",
        ),
    ] {
        let e = run(&mut c, bad).await;
        ck.ok(
            &format!("refused: {bad}"),
            e.as_ref().is_err_and(|e| e.contains(code)),
            || format!("{e:?}"),
        );
    }
    if let Ok(alias) = std::env::var("SPRIG_TEST_RX_PROVIDER_ALIAS") {
        let e = run(
            &mut c,
            &format!(
                "CREATE RETRIEVAL INDEX x ON articles (body) EMBED WITH MODEL {}",
                q(&alias)
            ),
        )
        .await;
        ck.ok(
            "a provider's model is 39997 (not built yet)",
            e.as_ref()
                .is_err_and(|e| e.contains("39997") && e.contains("provider")),
            || format!("{e:?}"),
        );
    }
    let (_, shown): (String, String) = c
        .query_first("SHOW CREATE RETRIEVAL INDEX kb")
        .await
        .unwrap()
        .unwrap();
    ck.ok(
        "SHOW CREATE RETRIEVAL INDEX prints every clause",
        shown
            == "CREATE RETRIEVAL INDEX `kb` ON `rx_live`.`articles` (`body`) TITLE `title` FILTERS \
                (`product`) CHUNK BY TOKENS 400 EMBED WITH MODEL 'local:bge-small'",
        || shown.clone(),
    );
    let filled = wait_ready(&mut c, "kb", Duration::from_secs(180)).await;
    let now = state(&mut c, "kb").await;
    ck.ok("the index fills and is ready", filled.is_some(), || {
        format!("{now:?}")
    });
    if let Some(d) = filled {
        eprintln!(
            "     fill of {} rows: {:.1} s",
            CORPUS.len(),
            d.as_secs_f64()
        );
    }
    let (st, rows) = state(&mut c, "kb").await;
    ck.ok(
        "SHOW RETRIEVAL INDEXES counts the rows",
        st == "ready" && rows == CORPUS.len() as u64,
        || format!("{st} {rows}"),
    );
    // The round trip: the shown statement makes the same index.
    must(&mut c, "DROP RETRIEVAL INDEX kb").await;
    must(&mut c, &shown).await;
    let (_, again): (String, String) = c
        .query_first("SHOW CREATE RETRIEVAL INDEX kb")
        .await
        .unwrap()
        .unwrap();
    ck.ok("SHOW CREATE round trip", again == shown, || again.clone());
    ck.ok(
        "ready again",
        wait_ready(&mut c, "kb", Duration::from_secs(180))
            .await
            .is_some(),
        String::new,
    );

    // ---- recall@5 ------------------------------------------------------------
    let t = Instant::now();
    let mut found = 0;
    let mut top1 = 0;
    for (question, want) in QUESTIONS {
        let got = ids(&mut c, "kb", question, 5).await;
        if got.contains(want) {
            found += 1;
        } else {
            eprintln!("     miss: {question:?} wants {want}, got {got:?}");
        }
        if got.first() == Some(want) {
            top1 += 1;
        }
    }
    let per = t.elapsed().as_secs_f64() * 1000.0 / QUESTIONS.len() as f64;
    let recall = f64::from(found) / QUESTIONS.len() as f64;
    eprintln!(
        "     recall@5 {recall:.2} ({found}/{}), top-1 {top1}, {per:.0} ms per search",
        QUESTIONS.len()
    );
    ck.ok("recall@5 ≥ 0.9 on the test corpus", recall >= 0.9, || {
        format!("{recall}")
    });
    let hits = retrieve(&mut c, "kb", "two-factor login codes", 3, None)
        .await
        .unwrap();
    ck.ok(
        "hits carry both ranks",
        hits.first().is_some_and(|h| h.2.is_some() && h.3.is_some()),
        || format!("{hits:?}"),
    );

    // ---- filters ----------------------------------------------------------------
    let rows: Vec<(i64, String)> = c
        .query(
            "SELECT r.id, a.product FROM RETRIEVE(kb, 'how do I get my money back', 5, \
             'product = ''shipping''') AS r JOIN articles a ON a.id = r.id",
        )
        .await
        .unwrap();
    ck.ok(
        "a filter keeps only its rows",
        !rows.is_empty() && rows.iter().all(|(_, p)| p == "shipping"),
        || format!("{rows:?}"),
    );
    let e = retrieve(&mut c, "kb", "x", 3, Some("owner = 'bob'")).await;
    ck.ok(
        "a filter on another column is 1054",
        e.as_ref().is_err_and(|e| e.contains("1054")),
        || format!("{e:?}"),
    );
    let e = retrieve(&mut c, "kb", "x", 3, Some("product = SLEEP(1)")).await;
    ck.ok("a function in a filter is refused", e.is_err(), || {
        format!("{e:?}")
    });
    let e = c
        .exec_drop("SELECT * FROM RETRIEVE(kb, 'x', 3)", ())
        .await
        .map_err(|e| e.to_string());
    ck.ok(
        "RETRIEVE in a prepared statement is 39051",
        e.as_ref().is_err_and(|e| e.contains("39051")),
        || format!("{e:?}"),
    );
    let e = run(&mut c, "SELECT * FROM RETRIEVE(nope, 'x', 3)").await;
    ck.ok(
        "an unknown index is 1146",
        e.as_ref().is_err_and(|e| e.contains("1146")),
        || format!("{e:?}"),
    );
    let e = run(&mut c, "SELECT * FROM RETRIEVE(kb, 'x', 0)").await;
    ck.ok(
        "a limit of 0 is 1210",
        e.as_ref().is_err_and(|e| e.contains("1210")),
        || format!("{e:?}"),
    );

    // ---- sync: insert, update, delete -----------------------------------------
    must(
        &mut c,
        "INSERT INTO articles (id, product, title, body) VALUES (100, 'app', 'Calendar sync', \
         'Connect your Google or Outlook calendar under Integrations to see meetings next to your notes.')",
    )
    .await;
    let d = wait_for(Duration::from_secs(60), || {
        let addr = addr.clone();
        let (u, p) = (user.clone(), password.clone());
        async move {
            let mut c = login(&addr, &u, &p).await;
            let _ = c.query_drop(format!("USE {DB}")).await;
            let got = ids(&mut c, "kb", "link my outlook calendar", 1).await;
            let _ = c.disconnect().await;
            got == [100]
        }
    })
    .await;
    ck.ok("an inserted row is found", d.is_some(), String::new);
    if let Some(d) = d {
        eprintln!("     insert to hit: {} ms", d.as_millis());
    }
    must(
        &mut c,
        "UPDATE articles SET title = 'Sourdough', body = 'Bake bread with a sourdough starter: feed it flour and water every day.' WHERE id = 100",
    )
    .await;
    let d = wait_for(Duration::from_secs(60), || {
        let addr = addr.clone();
        let (u, p) = (user.clone(), password.clone());
        async move {
            let mut c = login(&addr, &u, &p).await;
            let _ = c.query_drop(format!("USE {DB}")).await;
            let new = ids(&mut c, "kb", "sourdough starter bread", 1).await;
            let old = ids(&mut c, "kb", "link my outlook calendar", 1).await;
            let _ = c.disconnect().await;
            new == [100] && old != [100]
        }
    })
    .await;
    let new = ids(&mut c, "kb", "sourdough starter bread", 3).await;
    let old = ids(&mut c, "kb", "link my outlook calendar", 3).await;
    ck.ok(
        "an updated row is found by its new text, not its old",
        d.is_some(),
        || format!("new text: {new:?}, old text: {old:?}"),
    );
    let kb_id = match &mut b {
        Some(b) => index_id(b, "kb").await,
        None => None,
    };
    if let (Some(b), Some(id)) = (&mut b, kb_id) {
        let before: u64 = chunk_tables(b, id).await.iter().map(|x| x.1).sum();
        must(&mut c, "DELETE FROM articles WHERE id = 100").await;
        let gone = ids(&mut c, "kb", "sourdough starter bread", 3).await;
        ck.ok(
            "a deleted row is never a hit, even before its chunks go",
            !gone.contains(&100),
            || format!("{gone:?}"),
        );
        let mut after = before;
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(30) {
            after = chunk_tables(b, id).await.iter().map(|x| x.1).sum();
            if after < before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        ck.ok("a deleted row's chunks are deleted", after < before, || {
            format!("{before} -> {after}")
        });
        // An update of another column changes nothing; of the title, the chunks.
        must(&mut c, "UPDATE articles SET owner = owner WHERE id = 1").await;
    } else {
        must(&mut c, "DELETE FROM articles WHERE id = 100").await;
        eprintln!("     (no SPRIG_TEST_RX_BACKEND: chunk tables not checked)");
    }

    // ---- REST ---------------------------------------------------------------------
    if let Ok(http) = std::env::var("SPRIG_TEST_RX_HTTP") {
        let (code, v) = rest(
            &http,
            &user,
            &password,
            &format!("/v1/databases/{DB}/retrieval/kb:search"),
            &serde_json::json!({"query": "where is my package right now", "limit": 3}),
        );
        let first = v["hits"][0]["key"]["id"].as_i64();
        ck.ok(
            "REST search answers with keys and ranks",
            code == 200
                && first == Some(6)
                && v["hits"][0]["vector_rank"].is_number()
                && v["hits"][0]["text"].is_string(),
            || format!("{code} {v}"),
        );
        ck.ok(
            "REST: at most the limit",
            v["hits"].as_array().is_some_and(|h| h.len() <= 3),
            || format!("{v}"),
        );
        let (code, v) = rest(
            &http,
            &user,
            &password,
            &format!("/v1/databases/{DB}/retrieval/kb:search"),
            &serde_json::json!({"query": "money back", "limit": 5, "filter": "product = 'billing'"}),
        );
        ck.ok(
            "REST filter",
            code == 200 && v["hits"].as_array().is_some_and(|h| !h.is_empty()),
            || format!("{code} {v}"),
        );
        let (code, _) = rest(
            &http,
            &user,
            &password,
            &format!("/v1/databases/{DB}/retrieval/nope:search"),
            &serde_json::json!({"query": "x"}),
        );
        ck.ok("REST: an unknown index fails", code >= 400, || {
            format!("{code}")
        });
        let (code, _) = rest(
            &http,
            &user,
            &password,
            &format!("/v1/databases/{DB}/retrieval/kb:search"),
            &serde_json::json!({}),
        );
        ck.ok("REST: a query is required (400)", code == 400, || {
            format!("{code}")
        });
    } else {
        eprintln!("     (no SPRIG_TEST_RX_HTTP: REST not checked)");
    }

    // ---- row-level security --------------------------------------------------------
    must(
        &mut c,
        "CREATE USER 'rxreader'@'%' IDENTIFIED BY 'Rx-reader-Passw0rd'",
    )
    .await;
    must(&mut c, &format!("GRANT SELECT ON {DB}.* TO 'rxreader'@'%'")).await;
    must(
        &mut c,
        "CREATE POLICY alice_only ON articles FOR SELECT TO rxreader USING (owner = 'alice')",
    )
    .await;
    must(&mut c, "ALTER TABLE articles ENABLE ROW LEVEL SECURITY").await;
    let mut r = login(&addr, "rxreader", "Rx-reader-Passw0rd").await;
    must(&mut r, &format!("USE {DB}")).await;
    // Article 6 (parcel tracking) is bob's: the admin finds it, the reader doesn't.
    let admin_hits = ids(&mut c, "kb", "where is my package right now", 5).await;
    let reader_hits = ids(&mut r, "kb", "where is my package right now", 5).await;
    ck.ok(
        "the admin sees bob's article",
        admin_hits.first() == Some(&6),
        || format!("{admin_hits:?}"),
    );
    ck.ok(
        "row-level security hides bob's rows from the reader",
        !reader_hits.is_empty() && reader_hits.iter().all(|id| id % 2 == 1),
        || format!("{reader_hits:?}"),
    );
    ck.ok(
        "the reader still gets a full page of hits",
        reader_hits.len() == 5,
        || format!("{reader_hits:?}"),
    );
    if let Ok(http) = std::env::var("SPRIG_TEST_RX_HTTP") {
        let (code, v) = rest(
            &http,
            "rxreader",
            "Rx-reader-Passw0rd",
            &format!("/v1/databases/{DB}/retrieval/kb:search"),
            &serde_json::json!({"query": "where is my package right now", "limit": 5}),
        );
        let odd = v["hits"].as_array().is_some_and(|h| {
            !h.is_empty()
                && h.iter()
                    .all(|x| x["key"]["id"].as_i64().is_some_and(|i| i % 2 == 1))
        });
        ck.ok(
            "REST applies row-level security too",
            code == 200 && odd,
            || format!("{code} {v}"),
        );
    }
    must(&mut c, "REVOKE SELECT ON rx_live.* FROM 'rxreader'@'%'").await;
    let e = retrieve(&mut r, "kb", "parcel", 3, None).await;
    ck.ok(
        "without SELECT on the table, RETRIEVE is refused",
        e.as_ref().is_err_and(|e| e.contains("1142")),
        || format!("{e:?}"),
    );
    let _ = r.disconnect().await;
    must(&mut c, "ALTER TABLE articles DISABLE ROW LEVEL SECURITY").await;
    must(&mut c, "DROP USER 'rxreader'@'%'").await;

    // ---- a restricted user's ranks don't move with rows hidden from it ---------------
    // alice's 20 articles, then 80 of bob's written to crowd the same words
    // and the same neighbourhood: under a policy, alice's reader must get
    // exactly the same hits and ranks before and after bob's rows exist.
    must(
        &mut c,
        "CREATE TABLE shared (id INT PRIMARY KEY, title VARCHAR(200), body TEXT, \
         owner VARCHAR(20) NOT NULL)",
    )
    .await;
    let alice: Vec<String> = CORPUS
        .iter()
        .map(|(id, _, t, body)| format!("({id}, {}, {}, 'alice')", q(t), q(body)))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO shared VALUES {}", alice.join(", ")),
    )
    .await;
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX kbs ON shared (body) TITLE title",
    )
    .await;
    ck.ok(
        "the shared table's index fills",
        wait_ready(&mut c, "kbs", Duration::from_secs(240))
            .await
            .is_some(),
        String::new,
    );
    for (u, who) in [("rxalice", "alice"), ("rxbob", "bob")] {
        let _ = run(&mut c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
        must(
            &mut c,
            &format!("CREATE USER '{u}'@'%' IDENTIFIED BY 'Rx-shared-Passw0rd'"),
        )
        .await;
        must(&mut c, &format!("GRANT SELECT ON {DB}.shared TO '{u}'@'%'")).await;
        must(
            &mut c,
            &format!(
                "CREATE POLICY {who}_rows ON shared FOR SELECT TO {u} USING (owner = '{who}')"
            ),
        )
        .await;
    }
    must(&mut c, "ALTER TABLE shared ENABLE ROW LEVEL SECURITY").await;
    let mut ra = login(&addr, "rxalice", "Rx-shared-Passw0rd").await;
    must(&mut ra, &format!("USE {DB}")).await;
    let mut rb = login(&addr, "rxbob", "Rx-shared-Passw0rd").await;
    must(&mut rb, &format!("USE {DB}")).await;
    let before = all_hits(&mut ra, "kbs").await;
    let kw_before = keyword_lists(&mut ra, "kbs").await;
    let relevance_before = relevance(&mut b, "kbs").await;
    let bob: Vec<String> = (0..80)
        .map(|i| {
            let (_, _, t, body) = CORPUS[i % CORPUS.len()];
            let (_, _, _, other) = CORPUS[(i * 7 + 3) % CORPUS.len()];
            let words = QUESTIONS[i % QUESTIONS.len()].0;
            format!(
                "({}, {}, {}, 'bob')",
                1000 + i,
                q(&format!("{t} ({i})")),
                q(&format!("{words}. {body} {words}. {other}"))
            )
        })
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO shared VALUES {}", bob.join(", ")),
    )
    .await;
    // Every one of bob's rows indexed, and bob's reader finding them.
    let t = Instant::now();
    let (mut bob_seen, mut indexed) = (0usize, 0u64);
    while t.elapsed() < Duration::from_secs(240) {
        indexed = state(&mut c, "kbs").await.1;
        bob_seen = 0;
        for (question, _) in &QUESTIONS[..5] {
            bob_seen += ids(&mut rb, "kbs", question, 5)
                .await
                .iter()
                .filter(|id| **id >= 1000)
                .count();
        }
        if indexed >= 100 && bob_seen >= 25 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.ok(
        "bob's rows are indexed and bob's reader finds them",
        indexed >= 100 && bob_seen >= 25,
        || format!("rows indexed {indexed}, bob's hits {bob_seen}"),
    );
    let bob_hits = all_hits(&mut rb, "kbs").await;
    ck.ok(
        "bob's reader never sees alice's rows",
        bob_hits.iter().flatten().all(|h| h.0 >= 1000),
        || format!("{bob_hits:?}"),
    );
    let after = all_hits(&mut ra, "kbs").await;
    let kw_after = keyword_lists(&mut ra, "kbs").await;
    let kw_same = kw_before
        .iter()
        .zip(&kw_after)
        .filter(|(x, y)| x == y)
        .count();
    let same = before.iter().zip(&after).filter(|(x, y)| x == y).count();
    eprintln!(
        "     alice's reader after bob's 80 rows: the same keyword list for {kw_same}/{n} questions \
         ({} ranked chunks), the same top five with all ranks for {same}/{n} (the rest differ by the \
         approximate vector index's answers)",
        kw_after.iter().map(Vec::len).sum::<usize>(),
        n = QUESTIONS.len()
    );
    ck.ok(
        "a restricted user's keyword ranks don't change with rows hidden from it",
        kw_same == QUESTIONS.len() && after.iter().flatten().all(|h| h.0 < 1000),
        || format!("before {kw_before:?}\nafter {kw_after:?}"),
    );
    ck.ok(
        "a restricted user's hits stay nearly the same (only the vector index's approximation moves)",
        same * 10 >= QUESTIONS.len() * 8,
        || format!("before {before:?}\nafter {after:?}"),
    );
    ck.ok(
        "a restricted user still gets five hits per question",
        after.iter().all(|h| h.len() == 5),
        || format!("{after:?}"),
    );
    // For contrast: MariaDB's relevance over the whole chunk table, which
    // an unrestricted user's keyword list follows, orders alice's own
    // chunks differently once bob's rows are there.
    let relevance_after = relevance(&mut b, "kbs").await;
    if !relevance_before.is_empty() {
        let moved = relevance_before
            .iter()
            .zip(&relevance_after)
            .filter(|(x, y)| x != y)
            .count();
        eprintln!(
            "     for contrast: MariaDB's relevance order of alice's chunks moved for {moved}/{} questions",
            QUESTIONS.len()
        );
    }
    let _ = ra.disconnect().await;
    let _ = rb.disconnect().await;
    must(&mut c, "DROP RETRIEVAL INDEX kbs").await;
    must(&mut c, "DROP TABLE shared").await;
    must(&mut c, "DROP USER 'rxalice'@'%'").await;
    must(&mut c, "DROP USER 'rxbob'@'%'").await;

    // ---- a restricted user past 1,000 keyword matches: a warning ------------------
    // 1,050 rows share the words "brass lantern"; four also say "harbour".
    must(
        &mut c,
        "CREATE TABLE crowd (id INT PRIMARY KEY, body TEXT, owner VARCHAR(20) NOT NULL)",
    )
    .await;
    let sides = ["north", "south", "east", "west"];
    for part in (0..1050usize).collect::<Vec<_>>().chunks(350) {
        let v: Vec<String> = part
            .iter()
            .map(|i| {
                let extra = if *i < 4 {
                    " The harbour master keeps the spare key."
                } else {
                    ""
                };
                format!(
                    "({i}, {}, 'alice')",
                    q(&format!(
                        "Note {i}: the brass lantern by the {} door needs new oil.{extra}",
                        sides[i % 4]
                    ))
                )
            })
            .collect();
        must(&mut c, &format!("INSERT INTO crowd VALUES {}", v.join(", "))).await;
    }
    must(&mut c, "CREATE RETRIEVAL INDEX kbc ON crowd (body)").await;
    let filled = wait_ready(&mut c, "kbc", Duration::from_secs(400)).await;
    ck.ok("the crowded table's index fills", filled.is_some(), || {
        format!("{:?}", filled)
    });
    if let Some(d) = filled {
        eprintln!("     fill of 1050 rows: {:.1} s", d.as_secs_f64());
    }
    let _ = run(&mut c, "DROP USER IF EXISTS 'rxcrowd'@'%'").await;
    must(
        &mut c,
        "CREATE USER 'rxcrowd'@'%' IDENTIFIED BY 'Rx-crowd-Passw0rd'",
    )
    .await;
    must(&mut c, &format!("GRANT SELECT ON {DB}.crowd TO 'rxcrowd'@'%'")).await;
    must(
        &mut c,
        "CREATE POLICY crowd_alice ON crowd FOR SELECT TO rxcrowd USING (owner = 'alice')",
    )
    .await;
    must(&mut c, "ALTER TABLE crowd ENABLE ROW LEVEL SECURITY").await;
    let mut rc = login(&addr, "rxcrowd", "Rx-crowd-Passw0rd").await;
    must(&mut rc, &format!("USE {DB}")).await;
    let lantern = "SELECT id, bm25_rank FROM RETRIEVE(kbc, 'brass lantern', 5) ORDER BY `rank`";
    let (hits, count, shown) = retrieve_warned(&mut rc, lantern).await;
    eprintln!("     restricted, 1050 matches: {count} warning(s) {shown:?}, hits {hits:?}");
    ck.ok(
        "a restricted user past 1,000 matches gets warning 39827, counted in the result",
        count == 1
            && shown.len() == 1
            && shown[0].0 == 39827
            && shown[0]
                .1
                .starts_with("RETRIEVE(rx_live.kbc): no keyword list: its shard has more than 1000"),
        || format!("{count} {shown:?}"),
    );
    ck.ok(
        "its hits come from the vector list alone",
        hits.len() == 5 && hits.iter().all(|h| h.1.is_none()),
        || format!("{hits:?}"),
    );
    let (_, count, shown) = retrieve_warned(
        &mut rc,
        "SELECT a.id, a.bm25_rank FROM RETRIEVE(kbc, 'brass lantern', 3) AS a \
         JOIN RETRIEVE(kbc, 'brass lantern', 3) AS b ON a.id = b.id",
    )
    .await;
    ck.ok(
        "two calls over the same index: one warning",
        count == 1 && shown.len() == 1,
        || format!("{count} {shown:?}"),
    );
    let (hits, count, shown) = retrieve_warned(
        &mut rc,
        "SELECT id, bm25_rank FROM RETRIEVE(kbc, 'harbour', 5) ORDER BY `rank`",
    )
    .await;
    ck.ok(
        "a restricted user with few matches: no warning, a keyword list",
        count == 0 && shown.is_empty() && hits.iter().any(|h| h.1.is_some()),
        || format!("{count} {shown:?} {hits:?}"),
    );
    let (hits, count, shown) = retrieve_warned(&mut c, lantern).await;
    ck.ok(
        "an unrestricted user: no warning, a keyword list",
        count == 0 && shown.is_empty() && hits.iter().any(|h| h.1.is_some()),
        || format!("{count} {shown:?} {hits:?}"),
    );
    if let Ok(http) = std::env::var("SPRIG_TEST_RX_HTTP") {
        let path = format!("/v1/databases/{DB}/retrieval/kbc:search");
        let body = serde_json::json!({"query": "brass lantern", "limit": 5});
        let (code, v) = rest(&http, "rxcrowd", "Rx-crowd-Passw0rd", &path, &body);
        ck.ok(
            "REST: the restricted user's answer carries warning 39827",
            code == 200
                && v["warnings"].as_array().is_some_and(|w| w.len() == 1)
                && v["warnings"][0]["code"] == 39827
                && v["warnings"][0]["level"] == "Warning"
                && v["hits"].as_array().is_some_and(|h| h.len() == 5),
            || format!("{code} {v}"),
        );
        let (code, v) = rest(&http, &user, &password, &path, &body);
        ck.ok(
            "REST: an unrestricted user's warnings are empty",
            code == 200 && v["warnings"].as_array().is_some_and(Vec::is_empty),
            || format!("{code} {v}"),
        );
    }
    let _ = rc.disconnect().await;
    must(&mut c, "DROP RETRIEVAL INDEX kbc").await;
    must(&mut c, "DROP TABLE crowd").await;
    must(&mut c, "DROP USER 'rxcrowd'@'%'").await;

    // ---- sharded fusion against the unsharded table -------------------------------
    if let Ok(cmd) = std::env::var("SPRIG_TEST_RX_SHARD_CMD") {
        // The index first, then the table is grouped and split under it.
        must(&mut c, "CREATE TABLE many LIKE articles").await;
        must(&mut c, "INSERT INTO many SELECT * FROM articles").await;
        must(
            &mut c,
            "CREATE RETRIEVAL INDEX kb2 ON many (body) TITLE title FILTERS (product)",
        )
        .await;
        ck.ok(
            "the index over the table to shard fills",
            wait_ready(&mut c, "kb2", Duration::from_secs(240))
                .await
                .is_some(),
            String::new,
        );
        must(&mut c, "SPRIG PIN many SHARD KEY (id)").await;
        let rows_before_group = state(&mut c, "kb2").await.1;
        let group = format!("{cmd} group");
        ck.ok("the table moved into its group", shell(&group), || {
            group.clone()
        });
        let kb2 = match &mut b {
            Some(b) => index_id(b, "kb2").await,
            None => None,
        };
        // Moved into the group's schema with its chunks (renamed with the
        // table): nothing embedded again; the chunks written before the
        // rows had a routing hash get it, and no chunk table stays behind.
        if let (Some(bk), Some(id)) = (b.as_mut(), kb2) {
            let mut left = (99, 99, 99);
            let mut grouped = 0;
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(180) {
                left = misplaced(bk, id, "many").await;
                // Once no chunk table is left behind, every one is in the group.
                grouped = if left.2 == 0 {
                    chunk_tables(bk, id).await.iter().map(|x| x.1).sum::<u64>()
                } else {
                    0
                };
                if left == (0, 0, 0) && grouped >= 20 && state(&mut c, "kb2").await.0 == "ready" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            eprintln!(
                "     grouped: chunk tables {:?} after {} ms",
                chunk_tables(bk, id).await,
                t.elapsed().as_millis()
            );
            ck.ok(
                "after the move into the group the index is filled there and ready",
                grouped >= 20 && state(&mut c, "kb2").await.0 == "ready",
                || format!("{grouped} chunks in the group"),
            );
            ck.ok(
                "every chunk carries its row's routing hash before the split",
                left.0 == 0 && left.1 == 0,
                || format!("away, wrong rk: {left:?}"),
            );
            ck.ok(
                "the chunk table left behind in the old schema is dropped",
                left.2 == 0,
                || format!("{left:?}"),
            );
        } else {
            ck.ok(
                "after the move into the group the index is ready again",
                wait_ready(&mut c, "kb2", Duration::from_secs(240))
                    .await
                    .is_some(),
                String::new,
            );
        }
        let rows_before = state(&mut c, "kb2").await.1;
        eprintln!("     group: rows indexed {rows_before_group} → {rows_before}");
        ck.ok(
            "the move into the group embeds nothing again (rows indexed unchanged)",
            rows_before == rows_before_group,
            || format!("{rows_before_group} → {rows_before}"),
        );
        let before = all_hits(&mut c, "kb2").await;
        let split = format!("{cmd} split");
        ck.ok("the shard split ran", shell(&split), || split.clone());
        // At once after the cutover: the chunks are in the new shards, with
        // their rows, and searches find them.
        let right_after = all_hits(&mut c, "kb2").await;
        let found = right_after.iter().filter(|h| h.len() == 5).count();
        let vec_same = before
            .iter()
            .zip(&right_after)
            .filter(|(x, y)| {
                let va: HashMap<(i64, u32), Option<u32>> =
                    x.iter().map(|h| ((h.0, h.1), h.3)).collect();
                y.iter()
                    .all(|h| va.get(&(h.0, h.1)).is_none_or(|v| *v == h.3))
            })
            .count();
        eprintln!(
            "     right after the split: {found}/{} questions with five hits, {vec_same} with the same vector ranks",
            QUESTIONS.len()
        );
        ck.ok(
            "right after the split searches find every shard's rows",
            found == QUESTIONS.len() && vec_same == QUESTIONS.len(),
            || format!("before {before:?}\nafter {right_after:?}"),
        );
        let mut states = BTreeSet::new();
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(25) {
            states.insert(state(&mut c, "kb2").await.0);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let rows_after = state(&mut c, "kb2").await.1;
        eprintln!(
            "     after the split: states seen {states:?}, rows indexed {rows_before} → {rows_after}"
        );
        ck.ok(
            "the split leaves the index ready (never rebuilding)",
            states.len() == 1 && states.contains("ready"),
            || format!("{states:?}"),
        );
        ck.ok(
            "the split embeds nothing again",
            rows_after == rows_before,
            || format!("{rows_before} → {rows_after}"),
        );
        let shards = match (&mut b, kb2) {
            (Some(b), Some(id)) => chunk_tables(b, id).await,
            _ => Vec::new(),
        };
        eprintln!("     kb2 chunk tables: {shards:?}");
        ck.ok(
            "the sharded index has a chunk table per shard (2 or more)",
            shards.len() >= 2 || b.is_none(),
            || format!("{shards:?}"),
        );
        ck.ok(
            "the chunks moved: 20 rows' chunks over the new shards, none lost",
            b.is_none() || shards.iter().map(|x| x.1).sum::<u64>() >= 20,
            || format!("{shards:?}"),
        );
        if let (Some(bk), Some(id)) = (b.as_mut(), kb2) {
            let m = misplaced(bk, id, "many").await;
            ck.ok(
                "every chunk is in its row's shard, with its routing hash",
                m.0 == 0 && m.1 == 0,
                || format!("away, wrong rk: {m:?}"),
            );
        }
        let (mut same_top, mut overlap, mut vec_same) = (0usize, 0f64, 0usize);
        for (question, _) in QUESTIONS {
            let one = retrieve(&mut c, "kb", question, 5, None).await.unwrap();
            let many = retrieve(&mut c, "kb2", question, 5, None).await.unwrap();
            if one.first().map(|h| h.0) == many.first().map(|h| h.0) {
                same_top += 1;
            }
            let a: BTreeSet<(i64, u32)> = one.iter().map(|h| (h.0, h.1)).collect();
            let m: BTreeSet<(i64, u32)> = many.iter().map(|h| (h.0, h.1)).collect();
            overlap += a.intersection(&m).count() as f64 / a.len().max(1) as f64;
            // Vector ranks don't depend on how rows are spread.
            let va: HashMap<(i64, u32), Option<u32>> =
                one.iter().map(|h| ((h.0, h.1), h.3)).collect();
            if many
                .iter()
                .all(|h| va.get(&(h.0, h.1)).is_none_or(|v| *v == h.3))
            {
                vec_same += 1;
            }
        }
        let n = QUESTIONS.len();
        let overlap = overlap / n as f64;
        eprintln!(
            "     sharded vs one shard: same top-1 {same_top}/{n}, top-5 overlap {overlap:.2}, same vector ranks {vec_same}/{n}"
        );
        ck.ok(
            "sharded fusion: the same best hit for ≥ 90% of questions",
            same_top * 10 >= n * 9,
            || format!("{same_top}/{n}"),
        );
        ck.ok(
            "sharded fusion: top-5 overlap ≥ 0.8",
            overlap >= 0.8,
            || format!("{overlap}"),
        );
        ck.ok(
            "sharded fusion: vector ranks equal the single shard's",
            vec_same == n,
            || format!("{vec_same}/{n}"),
        );
        // And back: the two shards merged into one, the chunks with them.
        let rows_before = state(&mut c, "kb2").await.1;
        let before = all_hits(&mut c, "kb2").await;
        let merge = format!("{cmd} merge");
        ck.ok("the shards were merged", shell(&merge), || merge.clone());
        let right_after = all_hits(&mut c, "kb2").await;
        let found = right_after.iter().filter(|h| h.len() == 5).count();
        let mut states = BTreeSet::new();
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(20) {
            states.insert(state(&mut c, "kb2").await.0);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let rows_after = state(&mut c, "kb2").await.1;
        let shards = match (&mut b, kb2) {
            (Some(b), Some(id)) => chunk_tables(b, id).await,
            _ => Vec::new(),
        };
        let m = match (&mut b, kb2) {
            (Some(b), Some(id)) => misplaced(b, id, "many").await,
            _ => (0, 0, 0),
        };
        eprintln!(
            "     after the merge: {found}/{} questions with five hits, states {states:?}, rows indexed {rows_before} → {rows_after}, chunk tables {shards:?}",
            QUESTIONS.len()
        );
        ck.ok(
            "after the merge searches find every row at once",
            found == QUESTIONS.len(),
            || format!("before {before:?}\nafter {right_after:?}"),
        );
        ck.ok(
            "the merge leaves the index ready and embeds nothing again",
            states.len() == 1 && states.contains("ready") && rows_after == rows_before,
            || format!("{states:?} {rows_before} → {rows_after}"),
        );
        ck.ok(
            "after the merge the chunks are in the one shard, with their rows",
            (b.is_none() || shards.iter().map(|x| x.1).sum::<u64>() >= 20) && m.0 == 0 && m.1 == 0,
            || format!("{shards:?} {m:?}"),
        );
        let mut b2 = backend_of("SPRIG_TEST_RX_BACKEND2").await;
        if let (Some(b1), Some(b2), Some(id)) = (b.as_mut(), b2.as_mut(), kb2) {
            // ---- the shard moved to the other cluster: the chunks go with it ----
            let source = chunk_tables_both(b1, b2, id).await;
            let chunks_before: u64 = source.iter().map(|x| x.2).sum();
            let rows_before = state(&mut c, "kb2").await.1;
            let before = all_hits(&mut c, "kb2").await;
            let mv = format!("{cmd} move");
            let t = Instant::now();
            let (moved_ok, mut states) = shell_watching(&mut c, &mv, "kb2").await;
            ck.ok("the shard moved to the other cluster", moved_ok, || {
                mv.clone()
            });
            eprintln!("     move: {} ms", t.elapsed().as_millis());
            let right_after = all_hits(&mut c, "kb2").await;
            let (later, rows_after) = states_for(&mut c, "kb2", 20).await;
            states.extend(later);
            let tables = chunk_tables_both(b1, b2, id).await;
            let moved: Vec<&(u8, String, u64)> = tables
                .iter()
                .filter(|x| !source.iter().any(|s| s.0 == x.0 && s.1 == x.1))
                .collect();
            let m = misplaced(
                if moved.first().is_some_and(|x| x.0 == 1) {
                    &mut *b1
                } else {
                    &mut *b2
                },
                id,
                "many",
            )
            .await;
            eprintln!(
                "     after the move: chunk tables {source:?} → {tables:?}, states {states:?}, rows indexed {rows_before} → {rows_after}"
            );
            ck.ok(
                "the chunk table arrives on the other cluster with its rows",
                moved.len() == 1
                    && moved[0].0 != source.first().map_or(0, |s| s.0)
                    && moved[0].2 == chunks_before
                    && m.0 == 0
                    && m.1 == 0,
                || format!("{source:?} → {tables:?}, away and wrong rk {m:?}"),
            );
            ck.ok(
                "the move leaves the index ready",
                states.len() == 1 && states.contains("ready"),
                || format!("{states:?}"),
            );
            ck.ok(
                "the move embeds nothing again (rows indexed unchanged)",
                rows_after == rows_before,
                || format!("{rows_before} → {rows_after}"),
            );
            let (sets, exact) = same_hits(&before, &right_after);
            eprintln!(
                "     move: the same top five for {sets}/{n}, with every rank for {exact}/{n}",
                n = QUESTIONS.len()
            );
            ck.ok(
                "searches give the same hits before and after the move",
                sets == QUESTIONS.len() && exact * 10 >= QUESTIONS.len() * 9,
                || format!("before {before:?}\nafter {right_after:?}"),
            );
            // The mover's cleanup drops the source shard, chunk table too.
            let mut left = source.len();
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(300) {
                let now = chunk_tables_both(b1, b2, id).await;
                left = now
                    .iter()
                    .filter(|x| source.iter().any(|s| s.0 == x.0 && s.1 == x.1))
                    .count();
                if left == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            eprintln!(
                "     source chunk tables left: {left} after {} s",
                t.elapsed().as_secs()
            );
            ck.ok(
                "no chunk table is left in the source after the cleanup",
                left == 0,
                || format!("{left} left"),
            );

            // ---- re-key: the chunks stay behind, the new shards are filled again ----
            let old = chunk_tables_both(b1, b2, id).await;
            let rows_before = state(&mut c, "kb2").await.1;
            let before = all_hits(&mut c, "kb2").await;
            let rk = format!("{cmd} rekey");
            let t = Instant::now();
            let (rekeyed, mut states) = shell_watching(&mut c, &rk, "kb2").await;
            eprintln!("     re-key: {} ms", t.elapsed().as_millis());
            ck.ok("the group was re-keyed", rekeyed, || rk.clone());
            // The new shards get no chunks from the mover, so the worker
            // fills them again: every row embedded once more.
            let at_cutover = chunk_tables_both(b1, b2, id).await;
            let t = Instant::now();
            let mut ready_in = None;
            while t.elapsed() < Duration::from_secs(300) {
                let s = state(&mut c, "kb2").await.0;
                states.insert(s.clone());
                if s == "ready" && states.contains("rebuilding") {
                    ready_in = Some(t.elapsed());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            let rows_after = state(&mut c, "kb2").await.1;
            eprintln!(
                "     re-key: chunk tables {old:?} → at the cutover {at_cutover:?}, states {states:?}, ready again in {:?}, rows indexed {rows_before} → {rows_after}",
                ready_in.map(|d| d.as_millis())
            );
            // Chunks the mover had copied would be checked, not embedded
            // (as in the move above): rows indexed would stay the same.
            ck.ok(
                "the re-key leaves the chunk tables out of its copy (every row embedded once again)",
                rows_after == rows_before + 20,
                || format!("{rows_before} → {rows_after}"),
            );
            ck.ok(
                "the index goes rebuilding, then ready",
                ready_in.is_some(),
                || format!("{states:?}"),
            );
            let after = all_hits(&mut c, "kb2").await;
            let (sets, exact) = same_hits(&before, &after);
            let five = after.iter().filter(|h| h.len() == 5).count();
            let same_top = before
                .iter()
                .zip(&after)
                .filter(|(x, y)| x.first().map(|h| h.0) == y.first().map(|h| h.0))
                .count();
            eprintln!(
                "     re-key: five hits for {five}/{n}, the same best hit for {same_top}/{n}, the same top five for {sets}/{n}, with every rank for {exact}/{n}",
                n = QUESTIONS.len()
            );
            ck.ok(
                "searches match before and after the re-key",
                five == QUESTIONS.len() && same_top * 10 >= QUESTIONS.len() * 9,
                || format!("before {before:?}\nafter {after:?}"),
            );
            // The old shards' chunk tables are dropped; the new ones hold
            // every row's chunks next to the row.
            let mut left = old.len();
            let mut now = Vec::new();
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(300) {
                now = chunk_tables_both(b1, b2, id).await;
                left = now
                    .iter()
                    .filter(|x| old.iter().any(|s| s.0 == x.0 && s.1 == x.1))
                    .count();
                if left == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let (m1, m2) = (
                misplaced(b1, id, "many").await,
                misplaced(b2, id, "many").await,
            );
            eprintln!(
                "     re-key: old chunk tables left {left} after {} s; now {now:?}; away/wrong rk/orphans {m1:?} {m2:?}",
                t.elapsed().as_secs()
            );
            ck.ok("the old shards' chunk tables are dropped", left == 0, || {
                format!("{left} left: {now:?}")
            });
            ck.ok(
                "the new shards hold every row's chunks, next to the row",
                now.len() >= 2
                    && now.iter().map(|x| x.2).sum::<u64>() == chunks_before
                    && m1 == (0, 0, 0)
                    && m2 == (0, 0, 0),
                || format!("{now:?} {m1:?} {m2:?}"),
            );
        } else {
            eprintln!(
                "     (no SPRIG_TEST_RX_BACKEND2: the move to another cluster and the re-key not checked)"
            );
        }
    } else {
        eprintln!("     (no SPRIG_TEST_RX_SHARD_CMD: sharded fusion not checked)");
    }

    // ---- kill -9 of the worker in the middle of a fill ---------------------------
    if let (Ok(kill), Some(bk)) = (std::env::var("SPRIG_TEST_RX_KILL"), b.as_mut()) {
        must(
            &mut c,
            "CREATE TABLE notes (id INT PRIMARY KEY, body MEDIUMTEXT)",
        )
        .await;
        let n = 400usize;
        for part in (0..n).collect::<Vec<_>>().chunks(100) {
            let v: Vec<String> = part
                .iter()
                .map(|i| format!("({i}, {})", q(&long_text(*i, 6))))
                .collect();
            must(
                &mut c,
                &format!("INSERT INTO notes VALUES {}", v.join(", ")),
            )
            .await;
        }
        must(
            &mut c,
            "CREATE RETRIEVAL INDEX notes_ix ON notes (body) CHUNK BY TOKENS 64",
        )
        .await;
        let id = index_id(bk, "notes_ix").await.unwrap();
        // Kill once some rows are indexed.
        let t = Instant::now();
        let mut at = 0;
        while t.elapsed() < Duration::from_secs(120) {
            let chunks: u64 = chunk_tables(bk, id).await.iter().map(|x| x.1).sum();
            at = chunks;
            if chunks > 50 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ck.ok(
            "the worker was killed and started again",
            shell(&kill),
            || kill.clone(),
        );
        // Changes while it is down.
        must(&mut c, "UPDATE notes SET body = CONCAT(body, ' Extra words about a rainy afternoon.') WHERE id < 10").await;
        must(&mut c, "DELETE FROM notes WHERE id BETWEEN 390 AND 399").await;
        let ready = wait_ready(&mut c, "notes_ix", Duration::from_secs(600)).await;
        ck.ok(
            "after the kill the fill finishes",
            ready.is_some(),
            String::new,
        );
        let tables = chunk_tables(bk, id).await;
        let schema = tables.first().map(|x| x.0.clone()).unwrap_or_default();
        // Every row has chunks of its current text, and no others.
        let missing: u64 = bk
            .query_first(format!(
                "SELECT COUNT(*) FROM `{schema}`.notes n WHERE NOT EXISTS (SELECT 1 FROM `{schema}`.`__sprig_rx_{id}` x \
                 WHERE x.k_id = n.id AND x.chunk_no = 0 AND x.src_hash = UNHEX(MD5(JSON_ARRAY(n.body, NULL))))"
            ))
            .await
            .unwrap()
            .unwrap_or(99);
        let orphans: u64 = bk
            .query_first(format!(
                "SELECT COUNT(*) FROM `{schema}`.`__sprig_rx_{id}` x WHERE NOT EXISTS \
                 (SELECT 1 FROM `{schema}`.notes n WHERE n.id = x.k_id)"
            ))
            .await
            .unwrap()
            .unwrap_or(99);
        let stale: u64 = bk
            .query_first(format!(
                "SELECT COUNT(*) FROM `{schema}`.`__sprig_rx_{id}` x JOIN `{schema}`.notes n ON n.id = x.k_id \
                 WHERE x.src_hash <> UNHEX(MD5(JSON_ARRAY(n.body, NULL)))"
            ))
            .await
            .unwrap()
            .unwrap_or(99);
        let per_row: Vec<(i64, u64, u64)> = bk
            .query(format!(
                "SELECT k_id, COUNT(*), MAX(chunk_no) + 1 FROM `{schema}`.`__sprig_rx_{id}` GROUP BY k_id \
                 HAVING COUNT(*) <> MAX(chunk_no) + 1"
            ))
            .await
            .unwrap();
        eprintln!(
            "     killed with {at} chunks written; after: missing {missing}, orphans {orphans}, stale {stale}, gaps {}",
            per_row.len()
        );
        ck.ok(
            "kill -9: every row indexed with its current text",
            missing == 0 && stale == 0,
            || format!("missing {missing}, stale {stale}"),
        );
        ck.ok(
            "kill -9: no chunks of deleted rows, no gaps",
            orphans == 0 && per_row.is_empty(),
            || format!("orphans {orphans}, {per_row:?}"),
        );
    } else {
        eprintln!("     (no SPRIG_TEST_RX_KILL or backend: kill -9 not checked)");
    }

    // ---- DROP ---------------------------------------------------------------------
    if let (Some(bk), Some(_)) = (b.as_mut(), kb_id) {
        let id = index_id(bk, "kb").await.unwrap();
        must(&mut c, "DROP RETRIEVAL INDEX kb").await;
        let e = run(&mut c, "SELECT * FROM RETRIEVE(kb, 'x', 3)").await;
        ck.ok(
            "a dropped index can't be searched",
            e.as_ref().is_err_and(|e| e.contains("1146")),
            || format!("{e:?}"),
        );
        let t = Instant::now();
        let mut left = 1;
        while t.elapsed() < Duration::from_secs(30) {
            left = chunk_tables(bk, id).await.len();
            if left == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ck.ok(
            "DROP RETRIEVAL INDEX removes the chunk tables",
            left == 0,
            || format!("{left} left"),
        );
    }
    ck.ok(
        "DROP IF EXISTS of a missing index passes",
        run(&mut c, "DROP RETRIEVAL INDEX IF EXISTS kb")
            .await
            .is_ok(),
        String::new,
    );
    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    let _ = c.disconnect().await;
    eprintln!("{} checks, {} failed", ck.n, ck.failed.len());
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
