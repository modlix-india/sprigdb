//! Only bootstrap creates a tenant's catalog schema, with its owner mark;
//! the gateway's migrations never do.
//!
//! - A new tenant's catalog carries its mark (`sprig tenant <id> ...`)
//!   when the tenant is listed, and statements that make the gateway
//!   migrate modules into it (users and grants, a stream, a row policy)
//!   work and leave the mark as it was.
//! - A catalog dropped under a live session (as a deletion's cleanup does
//!   while sessions still run) isn't made again: a statement whose first
//!   step migrates a module gets an error, and for 15 s no schema of that
//!   name appears. Before, the migrator made the schema without a mark,
//!   and bootstrap later refused it as another tenant's (39072).
//!
//! ```sh
//! SPRIG_TEST_CATSCHEMA=<mysql port>,<admin port> \
//! SPRIG_TEST_CATSCHEMA_SPRIGCTL=<path to sprigctl> \
//! SPRIG_TEST_CATSCHEMA_NODE=<MySQL port of the cluster holding the catalogs> \
//! SPRIG_TEST_CATSCHEMA_CLUSTER=<that cluster's id> \
//! SPRIG_TEST_CATSCHEMA_PREFIX=<unique tenant name prefix> \
//! SPRIG_TEST_ROOT_PASSWORD=<the node's root password> \
//!   cargo test -p sprig-gateway --test catalog_schema_live -- --nocapture --test-threads 1
//! ```
//!
//! Each test deletes the tenants it created, by name.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::process::Command;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;

const PW: &str = "Cs4-schema-pw1";

struct Env {
    mysql: u16,
    admin: u16,
    sprigctl: String,
    node: u16,
    cluster: String,
    prefix: String,
}

fn env(test: &str) -> Option<Env> {
    let get = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    let e = (|| {
        let p: Vec<u16> = get("SPRIG_TEST_CATSCHEMA")?
            .split(',')
            .filter_map(|x| x.trim().parse().ok())
            .collect();
        let [mysql, admin] = p.as_slice() else {
            return None;
        };
        Some(Env {
            mysql: *mysql,
            admin: *admin,
            sprigctl: get("SPRIG_TEST_CATSCHEMA_SPRIGCTL")?,
            // No default: a private deployment's run never reads the
            // shared dev node.
            node: get("SPRIG_TEST_CATSCHEMA_NODE")?.parse().ok()?,
            cluster: get("SPRIG_TEST_CATSCHEMA_CLUSTER")?,
            prefix: get("SPRIG_TEST_CATSCHEMA_PREFIX").unwrap_or_else(|| "cschema".to_owned()),
        })
    })();
    if e.is_none() {
        eprintln!(
            "skipped {test}: set SPRIG_TEST_CATSCHEMA, SPRIG_TEST_CATSCHEMA_SPRIGCTL, \
             SPRIG_TEST_CATSCHEMA_NODE and SPRIG_TEST_CATSCHEMA_CLUSTER"
        );
    }
    e
}

fn ctl(e: &Env, args: &[&str]) -> Result<Value, String> {
    let out = Command::new(&e.sprigctl)
        .args([
            "--endpoint",
            &format!("http://127.0.0.1:{}", e.admin),
            "-o",
            "json",
        ])
        .args(args)
        .env("SPRIGCTL_ACTOR", "catalog_schema_live")
        .output()
        .map_err(|x| x.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// Creates tenant `name` on the test's cluster with user `app`; its id.
fn create(e: &Env, name: &str) -> u64 {
    ctl(e, &["tenant", "create", name, "--cluster", &e.cluster]).expect("tenant create");
    ctl(
        e,
        &[
            "tenant",
            "user",
            "add",
            name,
            "app",
            "--password",
            PW,
            "--if-not-exists",
        ],
    )
    .expect("user add");
    let id = ctl(e, &["tenant", "show", name])
        .ok()
        .and_then(|v| v["tenant_id"].as_u64())
        .unwrap_or(0);
    eprintln!("tenant {name}: id {id}");
    assert!(id > 0, "tenant {name} has an id");
    id
}

/// Deletes tenant `name` and waits (up to 2 minutes) until it is gone.
async fn delete(e: &Env, name: &str) {
    if let Err(x) = ctl(e, &["tenant", "delete", name, "--yes"]) {
        eprintln!("delete {name}: {x}");
        return;
    }
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(120) {
        if ctl(e, &["tenant", "show", name]).is_err() {
            eprintln!("tenant {name} deleted in {:?}", t0.elapsed());
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("tenant {name} still listed after 120 s");
}

async fn login(e: &Env, tenant: &str) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(e.mysql)
        .user(Some(format!("{tenant}.app")))
        .pass(Some(PW))
        .ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        );
    let t0 = Instant::now();
    loop {
        match Conn::new(opts.clone()).await {
            Ok(c) => return c,
            Err(x) if t0.elapsed() < Duration::from_secs(60) => {
                eprintln!("{tenant}.app: {x}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(x) => panic!("{tenant}.app: {x}"),
        }
    }
}

async fn ok(c: &mut Conn, q: &str) {
    if let Err(x) = c.query_drop(q).await {
        panic!("{q}: {x}");
    }
}

async fn root(e: &Env) -> Conn {
    let pw =
        std::env::var("SPRIG_TEST_ROOT_PASSWORD").unwrap_or_else(|_| "sprig-dev-root".to_owned());
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(e.node)
            .user(Some("root"))
            .pass(Some(pw)),
    )
    .await
    .expect("root login on the node")
}

/// The catalog schema's comment (its owner mark); `None` when it doesn't
/// exist.
async fn mark(r: &mut Conn, id: u64) -> Option<String> {
    let row: Option<Option<String>> = r
        .exec_first(
            "SELECT SCHEMA_COMMENT FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            (format!("t{id}_sprig"),),
        )
        .await
        .expect("schemata");
    row.map(Option::unwrap_or_default)
}

#[ignore = "live: needs SPRIG_TEST_CATSCHEMA, SPRIG_TEST_CATSCHEMA_SPRIGCTL, SPRIG_TEST_CATSCHEMA_NODE, ...; tools/live/README.md"]
#[tokio::test]
async fn a_new_tenants_catalog_is_marked_before_the_gateway_migrates_it() {
    let Some(e) = env("a_new_tenants_catalog_is_marked_before_the_gateway_migrates_it") else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let name = format!("{}-new", e.prefix);
    let id = create(&e, &name);
    let mut r = root(&e).await;
    let before = mark(&mut r, id).await;
    let want = format!("sprig tenant {id} ");
    assert!(
        before.as_deref().is_some_and(|m| m.starts_with(&want)),
        "bootstrap made and marked the catalog before the tenant was listed: {before:?}"
    );

    let mut c = login(&e, &name).await;
    let result = async {
        for q in [
            "CREATE DATABASE cs",
            "CREATE TABLE cs.docs (id INT PRIMARY KEY, owner VARCHAR(64), amount INT)",
            "INSERT INTO cs.docs VALUES (1, 'app', 10)",
            "CREATE USER reader IDENTIFIED BY 'Cs4-reader-pw1'",
            "GRANT SELECT ON cs.* TO reader",
            "CREATE STREAM cs.events PARTITIONS 1",
            "INSERT INTO cs.events (key, value) VALUES ('k', 'v')",
            "CREATE POLICY own ON cs.docs USING (owner = CURRENT_USER())",
        ] {
            ok(&mut c, q).await;
        }
        let n: Option<u64> = c
            .query_first("SELECT COUNT(*) FROM cs.docs")
            .await
            .expect("read");
        assert_eq!(n, Some(1));
        let after = mark(&mut r, id).await;
        assert_eq!(
            after, before,
            "the gateway's migrations left the mark as it was"
        );
        let tables: Vec<String> = r
            .exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME IN ('streams', 'row_security') ORDER BY TABLE_NAME",
                (format!("t{id}_sprig"),),
            )
            .await
            .expect("tables");
        eprintln!("modules the statements migrated: {tables:?}");
        assert!(
            tables.iter().any(|t| t == "streams") && tables.iter().any(|t| t == "row_security"),
            "the streams and row-security modules were migrated: {tables:?}"
        );
    };
    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(result)).await;
    let _ = c.disconnect().await;
    let _ = r.disconnect().await;
    delete(&e, &name).await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

#[ignore = "live: needs SPRIG_TEST_CATSCHEMA, SPRIG_TEST_CATSCHEMA_SPRIGCTL, SPRIG_TEST_CATSCHEMA_NODE, ...; tools/live/README.md"]
#[tokio::test]
async fn a_dropped_catalog_isnt_made_again_by_the_gateway() {
    let Some(e) = env("a_dropped_catalog_isnt_made_again_by_the_gateway") else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let name = format!("{}-gone", e.prefix);
    let id = create(&e, &name);
    let schema = format!("t{id}_sprig");
    let mut c = login(&e, &name).await;
    let mut r = root(&e).await;
    let result = async {
        for q in [
            "CREATE DATABASE cg",
            "CREATE TABLE cg.docs (id INT PRIMARY KEY, owner VARCHAR(64))",
            "INSERT INTO cg.docs VALUES (1, 'app')",
        ] {
            ok(&mut c, q).await;
        }
        assert!(mark(&mut r, id).await.is_some());
        // What a deletion's cleanup does while the tenant's sessions still
        // run.
        r.query_drop(format!("DROP DATABASE `{schema}`"))
            .await
            .expect("drop the catalog");
        // The row-security module isn't in the catalog yet: the policy's
        // first step migrates it.
        let got = c
            .query_drop("CREATE POLICY own ON cg.docs USING (owner = CURRENT_USER())")
            .await;
        eprintln!("CREATE POLICY on a dropped catalog: {got:?}");
        assert!(
            got.is_err(),
            "a statement needing the dropped catalog fails"
        );
        // Other statements, and the gateway's own loops, for 15 s.
        let t0 = Instant::now();
        let mut seen = None;
        while t0.elapsed() < Duration::from_secs(15) {
            let _ = c.query_drop("SELECT * FROM cg.docs").await;
            let _ = c.query_drop("CREATE STREAM cg.s PARTITIONS 1").await;
            if let Some(m) = mark(&mut r, id).await {
                seen = Some((t0.elapsed(), m));
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(
            seen, None,
            "{schema} was made again (time, mark) after the drop"
        );
    };
    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(result)).await;
    let _ = c.disconnect().await;
    // An unmarked schema left by a failing run would keep the deletion's
    // cleanup from dropping it: drop it by its exact name first.
    let _ = r
        .query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await;
    let _ = r.disconnect().await;
    delete(&e, &name).await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}
