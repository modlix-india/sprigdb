//! Retrieval indexes and the docs index on an engine-only node: no
//! MariaDB, no worker. `CREATE RETRIEVAL INDEX` on an engine database, the
//! in-node job filling the chunk table and following inserts, updates and
//! deletes from the change log, `RETRIEVE()` (MATCH on the engine's
//! full-text index plus an exact vector scan), filters, `SHOW` and `DROP`,
//! row-level security (a restricted user never sees hidden rows, gets full
//! pages, and its keyword ranking doesn't move when hidden rows are added),
//! the chunk table out of a client's reach, recall@5 on a small help-centre
//! corpus; and the docs index served from the release's pack in memory
//! (REST search and pages, `sprig_docs` read-only).
//!
//! ```sh
//! export SPRIG_TEST_RXE_SQL=127.0.0.1:46107      # a gateway's MariaDB port
//! export SPRIG_TEST_RXE_HTTP=127.0.0.1:46101     # its HTTP listener
//! export SPRIG_TEST_RXE_TENANT=rxe SPRIG_TEST_RXE_USER=app SPRIG_TEST_RXE_PASSWORD=...
//! export SPRIG_TEST_RXE_GW_LOG=.../gw1.log          # the gateway's JSON log
//! cargo test -p sprig-gateway --test retrieval_engine_live -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "rxe_live";

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

fn need(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("set {k} (see the file's header)"))
}

fn tenant() -> String {
    need("SPRIG_TEST_RXE_TENANT")
}

async fn login(addr: &str, user: &str, password: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(password))
        .prefer_socket(false);
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

/// One hit: (`id`, `chunk_no`, `bm25_rank`, `vector_rank`).
type Hit = (i64, u32, Option<u32>, Option<u32>);

async fn retrieve(
    c: &mut mysql_async::Conn,
    index: &str,
    query: &str,
    limit: u32,
    filter: Option<&str>,
) -> Result<Vec<Hit>, String> {
    let f = filter.map_or_else(String::new, |f| format!(", {}", q(f)));
    c.query(format!(
        "SELECT id, chunk_no, bm25_rank, vector_rank FROM RETRIEVE({index}, {}, {limit}{f}) \
         ORDER BY `rank`",
        q(query)
    ))
    .await
    .map_err(|e| e.to_string())
}

async fn ids(c: &mut mysql_async::Conn, index: &str, query: &str, limit: u32) -> Vec<i64> {
    retrieve(c, index, query, limit, None)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|h| h.0)
        .collect()
}

/// Every question's keyword list (`bm25_rank`, `id`, `chunk_no`) from the
/// first 100 hits.
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

/// Waits until index `index`'s state is `st` (any, for `None`) with `rows`
/// rows indexed.
async fn wait_state(
    addr: &str,
    user: &str,
    password: &str,
    index: &'static str,
    st: Option<&'static str>,
    rows: Option<u64>,
    limit: Duration,
) -> Option<Duration> {
    wait_for(limit, || {
        let (at, who, pw) = (addr.to_owned(), user.to_owned(), password.to_owned());
        async move {
            let mut c = login(&at, &who, &pw).await;
            let s = state(&mut c, index).await;
            let _ = c.disconnect().await;
            st.is_none_or(|x| s.0 == x) && rows.is_none_or(|n| s.1 == n)
        }
    })
    .await
}

/// Waits until the top hit of `query` is `want` (and `not` doesn't find
/// `want`); `want` `None`: until id 100 isn't a hit.
async fn wait_hit(
    addr: &str,
    user: &str,
    password: &str,
    query: &'static str,
    want: Option<i64>,
    not: Option<&'static str>,
) -> Option<Duration> {
    wait_for(Duration::from_secs(60), || {
        let (at, who, pw) = (addr.to_owned(), user.to_owned(), password.to_owned());
        async move {
            let mut c = login(&at, &who, &pw).await;
            let _ = c.query_drop(format!("USE {DB}")).await;
            let got = ids(&mut c, "kb", query, 1).await;
            let other = match not {
                Some(n) => ids(&mut c, "kb", n, 1).await,
                None => Vec::new(),
            };
            let _ = c.disconnect().await;
            match want {
                Some(w) => got == [w] && !other.contains(&w),
                None => !got.contains(&100),
            }
        }
    })
    .await
}

/// Waits until the gateway's log has a line holding every one of `parts`.
async fn log_has(parts: &[&str], limit: Duration) -> bool {
    let path = need("SPRIG_TEST_RXE_GW_LOG");
    wait_for(limit, || {
        let path = path.clone();
        async move {
            std::fs::read_to_string(&path)
                .unwrap_or_default()
                .lines()
                .any(|l| parts.iter().all(|p| l.contains(p)))
        }
    })
    .await
    .is_some()
}

/// `curl` with the tenant user's basic credentials: (status, JSON body).
fn rest(
    method: &str,
    path: &str,
    user: &str,
    password: &str,
    body: Option<&serde_json::Value>,
) -> (u16, serde_json::Value) {
    use base64::Engine as _;
    let http = need("SPRIG_TEST_RXE_HTTP");
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{password}", tenant()));
    let mut args: Vec<String> = [
        "-sS",
        "--max-time",
        "30",
        "-X",
        method,
        "-w",
        "\n%{http_code}",
        "-H",
        "content-type: application/json",
    ]
    .map(str::to_owned)
    .to_vec();
    args.push("-H".into());
    args.push(format!("authorization: Basic {auth}"));
    if let Some(b) = body {
        args.push("-d".into());
        args.push(b.to_string());
    }
    args.push(format!("http://{http}{path}"));
    let out = std::process::Command::new("curl")
        .args(&args)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (json, code) = text.rsplit_once('\n').unwrap_or(("", "0"));
    (
        code.trim().parse().unwrap_or(0),
        serde_json::from_str(json).unwrap_or(serde_json::Value::Null),
    )
}

/// The test corpus: (id, product, title, body), and the questions with the
/// article that answers each (the same as the MariaDB-era live test).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live: needs SPRIG_TEST_RXE_SQL, SPRIG_TEST_RXE_HTTP, SPRIG_TEST_RXE_TENANT, SPRIG_TEST_RXE_USER, SPRIG_TEST_RXE_PASSWORD, SPRIG_TEST_RXE_GW_LOG; tools/live/README.md"]
async fn retrieval_and_docs_on_an_engine_node() {
    let addr = need("SPRIG_TEST_RXE_SQL");
    let user = need("SPRIG_TEST_RXE_USER");
    let password = need("SPRIG_TEST_RXE_PASSWORD");
    let (at, who, pw) = (addr.as_str(), user.as_str(), password.as_str());
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut c = login(at, who, pw).await;
    let _ = run(&mut c, "DROP USER IF EXISTS 'rxereader'@'%'").await;
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
        .map(|(id, pr, t, body)| {
            format!(
                "({id}, {}, {}, {}, {})",
                q(pr),
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

    // ---- DDL --------------------------------------------------------------
    let started = Instant::now();
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX kb ON articles (body) TITLE title FILTERS (product)",
    )
    .await;
    eprintln!(
        "     CREATE RETRIEVAL INDEX took {} ms",
        started.elapsed().as_millis()
    );
    let err = run(&mut c, "CREATE RETRIEVAL INDEX kb ON articles (body)").await;
    ck.ok(
        "a second index of the same name is 1061",
        err.as_ref().is_err_and(|e| e.contains("1061")),
        || format!("{err:?}"),
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
        ("CREATE RETRIEVAL INDEX x ON nope (body)", "1146"),
    ] {
        let err = run(&mut c, bad).await;
        ck.ok(
            &format!("refused: {bad}"),
            err.as_ref().is_err_and(|e| e.contains(code)),
            || format!("{err:?}"),
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
            == "CREATE RETRIEVAL INDEX `kb` ON `rxe_live`.`articles` (`body`) TITLE `title` FILTERS \
                (`product`) CHUNK BY TOKENS 400 EMBED WITH MODEL 'local:bge-small'",
        || shown.clone(),
    );
    let corpus = CORPUS.len() as u64;
    let filled = wait_state(
        at,
        who,
        pw,
        "kb",
        Some("ready"),
        Some(corpus),
        Duration::from_secs(180),
    )
    .await;
    let now = state(&mut c, "kb").await;
    ck.ok(
        "the in-node job fills the index: ready, every row",
        filled.is_some(),
        || format!("{now:?}"),
    );
    if let Some(took) = filled {
        eprintln!("     fill of {corpus} rows: {:.1} s", took.as_secs_f64());
    }
    let tables: Vec<String> = c.query("SHOW TABLES").await.unwrap();
    ck.ok(
        "SHOW TABLES doesn't list the chunk table",
        tables == ["articles"],
        || format!("{tables:?}"),
    );
    for n in 1..4 {
        let err = run(&mut c, &format!("SELECT COUNT(*) FROM `__sprig_rx_{n}`")).await;
        ck.ok(
            &format!("a client can't read chunk table {n}"),
            err.is_err(),
            || format!("{err:?}"),
        );
        let err = run(&mut c, &format!("DELETE FROM `__sprig_rx_{n}`")).await;
        ck.ok(
            &format!("a client can't write chunk table {n}"),
            err.is_err(),
            || format!("{err:?}"),
        );
    }

    // ---- recall@5 -----------------------------------------------------------
    let started = Instant::now();
    let (mut found, mut top1) = (0, 0);
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
    let per = started.elapsed().as_secs_f64() * 1000.0 / QUESTIONS.len() as f64;
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
        "hits carry both ranks (keyword and vector)",
        hits.first().is_some_and(|h| h.2.is_some() && h.3.is_some()),
        || format!("{hits:?}"),
    );

    // ---- filters ------------------------------------------------------------
    let rows: Vec<(i64, String)> = c
        .query(
            "SELECT r.id, a.product FROM RETRIEVE(kb, 'how do I get my money back', 5, \
             'product = ''shipping''') AS r JOIN articles a ON a.id = r.id",
        )
        .await
        .unwrap();
    ck.ok(
        "a filter keeps only its rows",
        !rows.is_empty() && rows.iter().all(|(_, pr)| pr == "shipping"),
        || format!("{rows:?}"),
    );
    let err = retrieve(&mut c, "kb", "x", 3, Some("owner = 'bob'")).await;
    ck.ok(
        "a filter on another column is 1054",
        err.as_ref().is_err_and(|e| e.contains("1054")),
        || format!("{err:?}"),
    );
    let err = run(&mut c, "SELECT * FROM RETRIEVE(nope, 'x', 3)").await;
    ck.ok(
        "an unknown index is 1146",
        err.as_ref().is_err_and(|e| e.contains("1146")),
        || format!("{err:?}"),
    );

    // ---- sync: insert, update, delete -------------------------------------
    must(
        &mut c,
        "INSERT INTO articles (id, product, title, body) VALUES (100, 'app', 'Calendar sync', \
         'Connect your Google or Outlook calendar under Integrations to see meetings next to your notes.')",
    )
    .await;
    let took = wait_hit(at, who, pw, "link my outlook calendar", Some(100), None).await;
    ck.ok("an inserted row is found", took.is_some(), String::new);
    if let Some(took) = took {
        eprintln!("     insert to hit: {} ms", took.as_millis());
    }
    must(
        &mut c,
        "UPDATE articles SET title = 'Sourdough', body = 'Bake bread with a sourdough starter: \
         feed it flour and water every day.' WHERE id = 100",
    )
    .await;
    let took = wait_hit(
        at,
        who,
        pw,
        "sourdough starter bread",
        Some(100),
        Some("link my outlook calendar"),
    )
    .await;
    ck.ok(
        "an updated row is found by its new text, not its old",
        took.is_some(),
        String::new,
    );
    if let Some(took) = took {
        eprintln!("     update to hit: {} ms", took.as_millis());
    }
    must(&mut c, "DELETE FROM articles WHERE id = 100").await;
    let gone = ids(&mut c, "kb", "sourdough starter bread", 3).await;
    ck.ok(
        "a deleted row is never a hit, even before its chunks go",
        !gone.contains(&100),
        || format!("{gone:?}"),
    );
    let took = wait_state(
        at,
        who,
        pw,
        "kb",
        None,
        Some(corpus),
        Duration::from_secs(60),
    )
    .await;
    ck.ok("a deleted row's chunks go", took.is_some(), || {
        format!("{:?}", "rows indexed didn't come back to the corpus")
    });

    // ---- row-level security ------------------------------------------------
    must(
        &mut c,
        "CREATE USER 'rxereader'@'%' IDENTIFIED BY 'Rx-reader-Passw0rd'",
    )
    .await;
    must(
        &mut c,
        &format!("GRANT SELECT ON {DB}.* TO 'rxereader'@'%'"),
    )
    .await;
    must(
        &mut c,
        "CREATE POLICY alice_only ON articles FOR SELECT TO rxereader USING (owner = 'alice')",
    )
    .await;
    must(&mut c, "ALTER TABLE articles ENABLE ROW LEVEL SECURITY").await;
    let mut reader = login(at, "rxereader", "Rx-reader-Passw0rd").await;
    must(&mut reader, &format!("USE {DB}")).await;
    let mut leaked = Vec::new();
    let mut full_pages = 0;
    for (question, _) in QUESTIONS {
        let got = ids(&mut reader, "kb", question, 5).await;
        if got.len() == 5 {
            full_pages += 1;
        }
        leaked.extend(got.into_iter().filter(|id| id % 2 == 0));
    }
    ck.ok(
        "the restricted reader never sees bob's rows",
        leaked.is_empty(),
        || format!("{leaked:?}"),
    );
    ck.ok(
        "the restricted reader still gets full pages",
        full_pages >= QUESTIONS.len() - 2,
        || format!("{full_pages} of {}", QUESTIONS.len()),
    );
    let before = keyword_lists(&mut reader, "kb").await;
    // Bob's rows sharing the questions' words: hidden from the reader.
    let more: Vec<String> = QUESTIONS
        .iter()
        .enumerate()
        .flat_map(|(i, (question, _))| {
            (0..2).map(move |k| {
                format!(
                    "({}, 'app', {}, {}, 'bob')",
                    1000 + i * 2 + k,
                    q(&format!("Note {i}.{k}")),
                    q(&format!("{question} {question} and more about {question}"))
                )
            })
        })
        .collect();
    must(
        &mut c,
        &format!(
            "INSERT INTO articles (id, product, title, body, owner) VALUES {}",
            more.join(", ")
        ),
    )
    .await;
    let want = corpus + more.len() as u64;
    let synced = wait_state(
        at,
        who,
        pw,
        "kb",
        None,
        Some(want),
        Duration::from_secs(120),
    )
    .await;
    ck.ok("bob's new rows are indexed", synced.is_some(), String::new);
    let after = keyword_lists(&mut reader, "kb").await;
    let same = before.iter().zip(&after).filter(|(x, y)| x == y).count();
    ck.ok(
        "the reader's keyword ranking ignores rows it can't see",
        same == QUESTIONS.len(),
        || format!("{same} of {} keyword lists unchanged", QUESTIONS.len()),
    );
    let mut leaked = Vec::new();
    for (question, _) in QUESTIONS {
        leaked.extend(
            ids(&mut reader, "kb", question, 10)
                .await
                .into_iter()
                .filter(|id| id % 2 == 0),
        );
    }
    ck.ok(
        "still nothing of bob's for the reader",
        leaked.is_empty(),
        || format!("{leaked:?}"),
    );
    let _ = reader.disconnect().await;

    // ---- the docs index, from memory ----------------------------------------
    let started = Instant::now();
    let (code, body) = rest(
        "POST",
        "/v1/docs:search",
        who,
        pw,
        Some(&serde_json::json!({"query": "what does error 39420 mean", "limit": 5})),
    );
    eprintln!(
        "     docs search over REST: {} ms",
        started.elapsed().as_millis()
    );
    let hits = body["hits"].as_array().cloned().unwrap_or_default();
    ck.ok(
        "docs search answers from the local pack (hybrid)",
        code == 200 && !hits.is_empty() && body["mode"] == "hybrid",
        || format!("{code} {body}"),
    );
    ck.ok(
        "an error code finds chunks that name it",
        hits.iter()
            .any(|h| h["text"].as_str().is_some_and(|x| x.contains("39420"))),
        || format!("{hits:?}"),
    );
    let (code, body) = rest("GET", "/v1/docs/pages/reference/errors.md", who, pw, None);
    ck.ok(
        "a docs page is rebuilt from the local pack",
        code == 200 && body["markdown"].as_str().is_some_and(|m| m.len() > 1000),
        || format!("{code}"),
    );
    let started = Instant::now();
    let pages: Vec<(String, String)> = c
        .query(
            "SELECT page, heading_path FROM sprig_docs.chunks \
             WHERE MATCH(text) AGAINST ('suspend tenant') LIMIT 3",
        )
        .await
        .unwrap_or_default();
    eprintln!(
        "     sprig_docs MATCH over SQL: {} ms",
        started.elapsed().as_millis()
    );
    ck.ok(
        "SQL MATCH over sprig_docs.chunks finds pages",
        pages.len() == 3,
        || format!("{pages:?}"),
    );
    let has: Option<i64> = c
        .query_first(
            "SELECT COUNT(*) > 0 AS has_operator_pages FROM sprig_docs.chunks \
             WHERE FIND_IN_SET('operator', audience)",
        )
        .await
        .unwrap_or(None);
    ck.ok("sprig_docs filters by audience", has == Some(1), || {
        format!("{has:?}")
    });
    let cols: Vec<mysql_async::Row> = c
        .query("DESCRIBE sprig_docs.chunks")
        .await
        .unwrap_or_default();
    ck.ok("DESCRIBE sprig_docs.chunks", cols.len() == 11, || {
        format!("{} columns", cols.len())
    });
    let started = Instant::now();
    let err = run(
        &mut c,
        "SELECT COUNT(*) FROM sprig_docs.chunks a, sprig_docs.chunks b",
    )
    .await;
    ck.ok(
        "a self-join of sprig_docs past the read cap fails cleanly (1041)",
        err.as_ref().is_err_and(|e| e.contains("1041")),
        || format!("{err:?}"),
    );
    eprintln!(
        "     sprig_docs self-join refused after {} ms",
        started.elapsed().as_millis()
    );
    let err = run(&mut c, "DELETE FROM sprig_docs.chunks").await;
    ck.ok(
        "sprig_docs stays read-only (1044)",
        err.as_ref().is_err_and(|e| e.contains("1044")),
        || format!("{err:?}"),
    );

    // ---- a DECIMAL key ------------------------------------------------------
    must(
        &mut c,
        "CREATE TABLE prices (id DECIMAL(10,2) PRIMARY KEY, body TEXT)",
    )
    .await;
    must(
        &mut c,
        "INSERT INTO prices VALUES (1.50, 'A dark roast coffee with notes of chocolate.')",
    )
    .await;
    must(&mut c, "CREATE RETRIEVAL INDEX pk ON prices (body)").await;
    let took = wait_state(
        at,
        who,
        pw,
        "pk",
        Some("ready"),
        Some(1),
        Duration::from_secs(60),
    )
    .await;
    ck.ok(
        "a DECIMAL-keyed table is indexed",
        took.is_some(),
        String::new,
    );
    must(
        &mut c,
        "UPDATE prices SET body = 'A green tea with jasmine flowers.' WHERE id = 1.50",
    )
    .await;
    let found = wait_for(Duration::from_secs(60), || {
        let (a2, u2, p2) = (addr.clone(), user.clone(), password.clone());
        async move {
            let mut c2 = login(&a2, &u2, &p2).await;
            let _ = c2.query_drop(format!("USE {DB}")).await;
            let got: Vec<String> = c2
                .query("SELECT CAST(id AS CHAR) FROM RETRIEVE(pk, 'jasmine green tea', 1)")
                .await
                .unwrap_or_default();
            let (st, rows) = state(&mut c2, "pk").await;
            let _ = c2.disconnect().await;
            got == ["1.50"] && st == "ready" && rows == 1
        }
    })
    .await;
    ck.ok(
        "a DECIMAL-keyed row keeps its chunks after an UPDATE",
        found.is_some(),
        || format!("{:?}", "the updated row isn't found, or its chunks went"),
    );
    must(&mut c, "DROP RETRIEVAL INDEX pk").await;

    // ---- DROP -----------------------------------------------------------------
    must(&mut c, "DROP RETRIEVAL INDEX kb").await;
    ck.ok(
        "DROP RETRIEVAL INDEX drops its chunk table",
        log_has(
            &["retrieval.chunks.dropped", "\"index\":\"kb\""],
            Duration::from_secs(10),
        )
        .await,
        String::new,
    );
    let err = run(&mut c, "SELECT * FROM RETRIEVE(kb, 'x', 3)").await;
    ck.ok(
        "a dropped index is gone at once (1146)",
        err.as_ref().is_err_and(|e| e.contains("1146")),
        || format!("{err:?}"),
    );
    ck.ok(
        "SHOW RETRIEVAL INDEXES no longer lists it",
        state(&mut c, "kb").await.0 == "missing",
        String::new,
    );
    // A dropped source table takes its index with it.
    must(&mut c, "CREATE RETRIEVAL INDEX kb2 ON articles (body)").await;
    must(&mut c, "DROP TABLE articles").await;
    let took = wait_state(
        at,
        who,
        pw,
        "kb2",
        Some("missing"),
        None,
        Duration::from_secs(30),
    )
    .await;
    ck.ok(
        "dropping the table drops its index",
        took.is_some(),
        String::new,
    );
    ck.ok(
        "dropping the table drops the index's chunk table",
        log_has(
            &["retrieval.chunks.dropped", "\"index\":\"kb2\""],
            Duration::from_secs(10),
        )
        .await,
        String::new,
    );
    // A dropped database takes its indexes' records with it.
    must(&mut c, "CREATE DATABASE rxe_live_gone").await;
    must(
        &mut c,
        "CREATE TABLE rxe_live_gone.t (id INT PRIMARY KEY, body TEXT)",
    )
    .await;
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX kg ON rxe_live_gone.t (body)",
    )
    .await;
    must(&mut c, "DROP DATABASE rxe_live_gone").await;
    ck.ok(
        "dropping the database forgets its indexes",
        log_has(
            &["retrieval indexes of a dropped database forgotten"],
            Duration::from_secs(30),
        )
        .await,
        String::new,
    );

    let _ = run(&mut c, "DROP USER IF EXISTS 'rxereader'@'%'").await;
    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    eprintln!("{} checks, {} failed", ck.n, ck.failed.len());
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
