//! Worker-run code's tenant subtree above `rls.inline_subtree_max`. An
//! after-commit trigger runs in a worker with its definer's row-level
//! security context, whose subtree sets are its tenant's subtree. Above
//! `rls.inline_subtree_max` the worker used to leave them empty (the
//! subtree table's view picks the tenant from the session account, and
//! worker-run code runs as `sprig_ctl`), so a policy with
//! `SPRIG_TENANT_IN_SUBTREE` refused every write of that code. Now the
//! worker inlines the subtree in the context at any size (up to 100,000
//! tenants).
//!
//! The tenant gets a child tenant and `rls.inline_subtree_max = 1`, so its
//! subtree (itself and the child) is above the limit. Its trigger on
//! `events` writes each event into `docs`, forced, whose policy keeps rows
//! of the tenant's subtree: the tenant's own row and the child's are
//! written, a row of tenant 1 isn't. The administrator's own session (the
//! subtree read from the subtree table) sees the same two rows.
//!
//! ```sh
//! export SPRIG_TEST_RWS_SQL=127.0.0.1:48306
//! export SPRIG_TEST_RWS_TENANT=sf13t SPRIG_TEST_RWS_TENANT_ID=48306000001
//! export SPRIG_TEST_RWS_CTL="/path/to/sprigctl --endpoint http://127.0.0.1:48480"
//! cargo test -p sprig-gateway --test rls_worker_subtree_live -- --nocapture
//! ```
//!
//! It creates the child tenant `<tenant>-kid` and deletes it at the end,
//! and sets the tenant's `rls.inline_subtree_max` back to its default.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::Duration;

use mysql_async::prelude::Queryable;

const DB: &str = "rws_suite";

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

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RWS_TENANT").unwrap_or_else(|_| "sf13t".into())
}

async fn sql(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", tenant())))
        .pass(Some(
            std::env::var("SPRIG_TEST_RWS_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
        ))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

fn ctl(args: &[&str]) -> Result<serde_json::Value, String> {
    let cmd = std::env::var("SPRIG_TEST_RWS_CTL").map_err(|_| "SPRIG_TEST_RWS_CTL".to_owned())?;
    let mut parts = cmd.split_whitespace();
    let bin = parts.next().ok_or("no sprigctl")?;
    let out = std::process::Command::new(bin)
        .args(parts)
        .args(["-o", "json"])
        .args(args)
        .env("SPRIGCTL_ACTOR", "rls_worker_subtree_live")
        .output()
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
}

/// The first `tenant_id` anywhere in `v`.
fn tenant_id(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::Object(m) => m
            .get("tenant_id")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| m.values().find_map(tenant_id)),
        serde_json::Value::Array(a) => a.iter().find_map(tenant_id),
        _ => None,
    }
}

fn setting(v: &str) -> Vec<String> {
    vec![
        "settings".into(),
        "set".into(),
        "rls.inline_subtree_max".into(),
        v.into(),
        "--scope".into(),
        format!("tenant:{}", tenant()),
        "--reason".into(),
        "rls_worker_subtree_live".into(),
        "--yes".into(),
    ]
}

async fn read(c: &mut mysql_async::Conn) -> Vec<String> {
    c.query::<String, _>(format!("SELECT note FROM {DB}.docs ORDER BY id"))
        .await
        .unwrap_or_default()
}

#[ignore = "live: needs SPRIG_TEST_RWS_SQL, SPRIG_TEST_RWS_TENANT_ID, SPRIG_TEST_RWS_CTL; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_run_code_reads_a_subtree_above_the_inline_limit() {
    let (Ok(sql_addr), Ok(tid), Ok(_)) = (
        std::env::var("SPRIG_TEST_RWS_SQL"),
        std::env::var("SPRIG_TEST_RWS_TENANT_ID"),
        std::env::var("SPRIG_TEST_RWS_CTL"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RWS_SQL, SPRIG_TEST_RWS_TENANT_ID and SPRIG_TEST_RWS_CTL"
        );
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let tid: u64 = tid.parse().unwrap();
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let kid_name = format!("{}-kid", tenant());
    let made = ctl(&["tenant", "create", &kid_name, "--under", &tenant()]);
    let kid = made.as_ref().ok().and_then(tenant_id);
    ck.ok("a child tenant", kid.is_some(), || format!("{made:?}"));
    let kid = kid.unwrap_or(0);
    let args = setting("1");
    let set = ctl(&args.iter().map(String::as_str).collect::<Vec<_>>());
    ck.ok(
        "the tenant's rls.inline_subtree_max is 1 (its subtree has 2 tenants)",
        set.is_ok(),
        || format!("{set:?}"),
    );
    // Settings reach the gateway and the worker within a few seconds.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let mut admin = sql(&sql_addr).await;
    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.events (id INT PRIMARY KEY, tid BIGINT, body VARCHAR(64))"),
        format!("CREATE TABLE {DB}.docs (id INT PRIMARY KEY, tid BIGINT, note VARCHAR(64))"),
        format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE {DB}.docs FORCE ROW LEVEL SECURITY"),
        format!(
            "CREATE POLICY tree ON {DB}.docs FOR ALL TO PUBLIC USING (SPRIG_TENANT_IN_SUBTREE(tid))"
        ),
    ] {
        must(&mut admin, &s).await;
    }
    must(&mut admin, &format!(
        r#"CREATE TRIGGER {DB}.a_tree AFTER COMMIT ON {DB}.events FOR INSERT LANGUAGE kirun AS '{{"namespace":"App","name":"Tree","parameters":{{"id":{{"parameterName":"id","schema":{{"type":["INTEGER"]}}}},"tid":{{"parameterName":"tid","schema":{{"type":["LONG"]}}}},"body":{{"parameterName":"body","schema":{{"type":["STRING"]}}}}}},"steps":{{"ins":{{"statementName":"ins","namespace":"Sprig.Db","name":"Exec","parameterMap":{{"sql":{{"s":{{"key":"s","type":"VALUE","value":"INSERT INTO docs (id, tid, note) VALUES (?, ?, ?)"}}}},"args":{{"a":{{"key":"a","type":"EXPRESSION","order":1,"expression":"Arguments.id"}},"b":{{"key":"b","type":"EXPRESSION","order":2,"expression":"Arguments.tid"}},"c":{{"key":"c","type":"EXPRESSION","order":3,"expression":"Arguments.body"}}}}}}}}}}}}'"#
    )).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    must(
        &mut admin,
        &format!(
            "INSERT INTO {DB}.events VALUES (1, {tid}, 'own'), (2, {kid}, 'child'), \
             (3, 1, 'outside')"
        ),
    )
    .await;
    let mut notes = Vec::new();
    for _ in 0..40 {
        notes = read(&mut admin).await;
        if notes.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Give the third event the same time to (not) arrive.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let notes2 = read(&mut admin).await;
    ck.ok(
        "worker-run code writes its tenant's own row above the inline limit",
        notes2.iter().any(|x| x == "own"),
        || format!("{notes:?} {notes2:?}"),
    );
    ck.ok(
        "worker-run code writes a child tenant's row above the inline limit",
        notes2.iter().any(|x| x == "child"),
        || format!("{notes2:?}"),
    );
    ck.ok(
        "LEAK: worker-run code writes a row outside its tenant's subtree",
        !notes2.iter().any(|x| x == "outside"),
        || format!("{notes2:?}"),
    );
    ck.ok(
        "the administrator's own session reads the same rows through the subtree table",
        notes2 == ["own", "child"],
        || format!("{notes2:?}"),
    );

    let _ = admin.query_drop(format!("DROP TRIGGER {DB}.a_tree")).await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let args = setting("1000");
    let _ = ctl(&args.iter().map(String::as_str).collect::<Vec<_>>());
    if kid != 0 {
        let r = ctl(&["tenant", "delete", &kid_name, "--yes"]);
        if r.is_err() {
            eprintln!("child tenant {kid_name} not deleted: {r:?}");
        }
    }
    println!(
        "worker-run code above rls.inline_subtree_max: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
