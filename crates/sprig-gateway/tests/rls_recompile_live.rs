//! Row-level security policies stored by older builds, live: a policy
//! made from a MongoDB filter keeps the SQL it was created with in the
//! catalog, and the views, triggers and change topic view deployed from it.
//! Policies created before the boolean fix stored SQL that compares
//! `JSON_VALUE(...)` with `'true'`, which MariaDB 11.8 reads as `1`, so
//! `{deleted: {$ne: true}}` showed deleted documents. A gateway compiles the
//! stored filter again, rewrites the row and redeploys what MariaDB holds.
//!
//! The test makes such a policy the way an older build left it: a policy
//! with the old translator's SQL for `{deleted: {$ne: true}}`, deployed by
//! the gateway, whose row is then given the MongoDB form directly in the
//! catalog (root on the tenant's cluster, which must be a private one).
//! It also strips the format label from the triggers of a second table, as
//! a deployment of an older build. Then it restarts the deployment and
//! checks every path: MongoDB `find` and `countDocuments`, SQL `COUNT(*)`
//! (the rewrite), a view over the collection (MariaDB's policy view), the
//! change topic read through the rewrite and through its filter view, the
//! catalog row, and the triggers. A second restart changes nothing.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:42306
//! export SPRIG_TEST_RLS_MONGO=127.0.0.1:42737
//! export SPRIG_TEST_RLS_TENANT=r12x          # a tenant on the private cluster
//! export SPRIG_TEST_RLS_TENANT_ID=42306000002
//! export SPRIG_TEST_RLSR_ROOT=127.0.0.1:42301 # that cluster's MariaDB, as root
//! export SPRIG_TEST_RLSR_RESTART=target/agent-rls12/gw-restart.sh  # run with "all"
//! cargo test -p sprig-gateway --test rls_recompile_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;
use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;

const DB: &str = "rlsr_suite";
const ALICE: &str = "rlsr_alice";
const PW: &str = "Rlsr-suite-pw-1";

/// What the translator before the boolean fix stored for
/// `{deleted: {$ne: true}}`: present, a plain scalar, and not `'true'`.
const OLD_SQL: &str = "((JSON_TYPE(JSON_EXTRACT(doc, '$.\"deleted\"')) IN ('STRING', 'INTEGER', \
     'DOUBLE', 'BOOLEAN') AND (_types IS NULL OR JSON_VALUE(_types, '$.\"deleted\"') IS NULL) AND \
     NOT (JSON_TYPE(JSON_EXTRACT(doc, '$.\"deleted\"')) = 'BOOLEAN' AND JSON_VALUE(doc, \
     '$.\"deleted\"') = 'true')))";

/// The filter, as `sprigCreatePolicy` stores it.
const MONGO: &str = r#"{"deleted":{"$ne":true}}"#;

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if !cond {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn login(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn root(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let pw = std::env::var("SPRIG_TEST_RLS_ROOT_PW").unwrap_or_else(|_| "sprig-dev-root".into());
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some("root"))
        .pass(Some(pw))
        .prefer_socket(false);
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn mongo(addr: &str, user: &str, pw: &str) -> mongodb::Client {
    let uri = format!(
        "mongodb://{}.{user}:{pw}@{addr}/?authSource=admin&directConnection=true",
        tenant()
    );
    mongodb::Client::with_uri_str(&uri).await.unwrap()
}

async fn ids(c: &mut mysql_async::Conn, q: &str) -> Result<BTreeSet<i64>, String> {
    c.query::<Option<i64>, _>(q)
        .await
        .map(|v| v.into_iter().flatten().collect())
        .map_err(|e| e.to_string())
}

/// The `_id`s of the documents the inserts' change records carry.
fn record_ids(from: &str) -> String {
    format!(
        "SELECT CAST(JSON_VALUE(JSON_VALUE(CAST(value AS CHAR), '$.after.doc'), '$._id') AS SIGNED) \
         FROM {from} WHERE JSON_VALUE(CAST(value AS CHAR), '$.op') = 'c'"
    )
}

/// What alice sees on each path, as (path, ids).
async fn paths(
    sql_addr: &str,
    mongo_addr: &str,
    admin: &mut mysql_async::Conn,
) -> Vec<(&'static str, Result<BTreeSet<i64>, String>)> {
    let mut out = Vec::new();
    let m = mongo(mongo_addr, ALICE, PW).await;
    let coll = m.database(DB).collection::<Document>("notes");
    let found: Result<Vec<Document>, String> = match coll.find(doc! {}).await {
        Ok(c) => c.try_collect().await.map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    out.push((
        "MongoDB find",
        found.map(|d| {
            d.iter()
                .filter_map(|x| match x.get("_id") {
                    Some(Bson::Int32(i)) => Some(i64::from(*i)),
                    Some(Bson::Int64(i)) => Some(*i),
                    _ => None,
                })
                .collect()
        }),
    ));
    let n = coll.count_documents(doc! {}).await;
    out.push((
        "MongoDB countDocuments",
        n.map(|n| (0..i64::try_from(n).unwrap()).collect())
            .map_err(|e| e.to_string()),
    ));
    let mut a = login(sql_addr, ALICE, PW).await;
    let n: Result<Option<i64>, _> = a
        .query_first(format!("SELECT COUNT(*) FROM {DB}.notes"))
        .await;
    out.push((
        "SQL COUNT(*)",
        n.map(|n| (0..n.unwrap_or(-1)).collect())
            .map_err(|e| e.to_string()),
    ));
    out.push((
        "SQL rows",
        ids(
            &mut a,
            &format!("SELECT CAST(JSON_VALUE(doc, '$._id') AS SIGNED) FROM {DB}.notes"),
        )
        .await,
    ));
    out.push((
        "a view over the collection (policy view)",
        ids(&mut a, &format!("SELECT i FROM {DB}.v")).await,
    ));
    // The change topic, once its records are in.
    for _ in 0..60 {
        let raw: Option<i64> = admin
            .query_first(format!("SELECT COUNT(*) FROM {DB}.`notes.changes`"))
            .await
            .ok()
            .flatten();
        if raw.unwrap_or(0) >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    out.push((
        "change topic through the rewrite",
        ids(&mut a, &record_ids(&format!("{DB}.`notes.changes`"))).await,
    ));
    out.push((
        "change topic through its filter view",
        ids(&mut a, &record_ids(&format!("{DB}.vch"))).await,
    ));
    out
}

fn restart() {
    let script = std::env::var("SPRIG_TEST_RLSR_RESTART").unwrap();
    let st = std::process::Command::new(script)
        .arg("all")
        .status()
        .unwrap();
    assert!(st.success(), "restart failed: {st}");
}

/// The catalog row of policy `p` and its table's check versions.
async fn catalog_row(r: &mut mysql_async::Conn, tid: u64) -> (String, Option<String>, u64, u64) {
    let (expr, mongo, oid): (String, Option<String>, u64) = r
        .query_first(format!(
            "SELECT using_expr, CAST(using_mongo AS CHAR), object_id FROM t{tid}_sprig.row_policies \
             WHERE name = 'p' ORDER BY policy_id DESC LIMIT 1"
        ))
        .await
        .unwrap()
        .unwrap();
    let (cv, ch): (u64, u64) = r
        .query_first(format!(
            "SELECT checks_version, changed_version FROM t{tid}_sprig.row_security \
             WHERE object_id = {oid}"
        ))
        .await
        .unwrap()
        .unwrap();
    (expr, mongo, cv, ch)
}

/// The policy trigger bodies of table `table` in the tenant's schemas.
async fn trigger_bodies(r: &mut mysql_async::Conn, tid: u64, table: &str) -> Vec<String> {
    r.query(format!(
        "SELECT ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA LIKE \
         't{tid}\\_d%' AND EVENT_OBJECT_TABLE = '{table}' AND TRIGGER_NAME LIKE '\\_\\_sprig\\_rls\\_b%'"
    ))
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policies_stored_by_older_builds_are_compiled_again() {
    let (Ok(sql_addr), Ok(mongo_addr), Ok(root_addr), Ok(tid)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_MONGO"),
        std::env::var("SPRIG_TEST_RLSR_ROOT"),
        std::env::var("SPRIG_TEST_RLS_TENANT_ID"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_MONGO, SPRIG_TEST_RLSR_ROOT, \
             SPRIG_TEST_RLS_TENANT_ID and SPRIG_TEST_RLSR_RESTART"
        );
        return;
    };
    let tid: u64 = tid.parse().unwrap();
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = login(&sql_addr, "app", "sprig-dev-app").await;
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("DROP USER IF EXISTS '{ALICE}'@'%'"),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE USER '{ALICE}'@'%' IDENTIFIED BY '{PW}'"),
        format!("GRANT SELECT ON {DB}.* TO '{ALICE}'@'%'"),
    ] {
        let r = admin.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || format!("{r:?}"));
    }
    let am = mongo(&mongo_addr, "app", "sprig-dev-app").await;
    let adb = am.database(DB);
    adb.create_collection("notes").await.unwrap();
    for q in [
        format!("ALTER TABLE {DB}.notes SPRIG SET (stream.change_topic = on)"),
        // As an older build left it: the old SQL, deployed.
        format!("CREATE POLICY p ON {DB}.notes USING ({OLD_SQL})"),
        format!("ALTER TABLE {DB}.notes ENABLE ROW LEVEL SECURITY"),
        format!(
            "CREATE SQL SECURITY INVOKER VIEW {DB}.v AS \
             SELECT CAST(JSON_VALUE(doc, '$._id') AS SIGNED) AS i FROM {DB}.notes"
        ),
        format!("CREATE VIEW {DB}.vch AS SELECT * FROM {DB}.`notes.changes`"),
        format!("GRANT SELECT ON {DB}.vch TO '{ALICE}'@'%'"),
        // A table whose triggers will look like an older build's.
        format!("CREATE TABLE {DB}.plain (id INT PRIMARY KEY, owner VARCHAR(64))"),
        format!("CREATE POLICY own ON {DB}.plain USING (owner = CURRENT_USER())"),
        format!("ALTER TABLE {DB}.plain ENABLE ROW LEVEL SECURITY"),
    ] {
        let r = admin.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || format!("{r:?}"));
    }
    // The six documents of the boolean tests; MongoDB's
    // `{deleted: {$ne: true}}` matches 2, 3, 4 and 5.
    adb.collection::<Document>("notes")
        .insert_many(vec![
            doc! {"_id": 1, "deleted": true},
            doc! {"_id": 2, "deleted": false},
            doc! {"_id": 3},
            doc! {"_id": 4, "deleted": null},
            doc! {"_id": 5, "deleted": "yes"},
            doc! {"_id": 6, "deleted": [true]},
        ])
        .await
        .unwrap();
    let want: BTreeSet<i64> = [2, 3, 4, 5].into_iter().collect();
    let old: BTreeSet<i64> = [1, 2, 5].into_iter().collect();

    // Before: the old SQL shows deleted document 1 on every SQL path.
    for (path, got) in paths(&sql_addr, &mongo_addr, &mut admin).await {
        let leak = match path {
            "MongoDB countDocuments" | "SQL COUNT(*)" => {
                got.as_ref().is_ok_and(|g| g.len() == old.len())
            }
            _ => got.as_ref() == Ok(&old),
        };
        ck.ok(
            &format!("before: {path} shows what the old SQL shows (deleted document 1)"),
            leak,
            || format!("{got:?}, the old SQL shows {old:?}"),
        );
    }

    // The row as an older build stored it: the MongoDB form beside the old
    // SQL. And triggers of `plain` without the format label.
    let mut r = root(&root_addr).await;
    r.query_drop(format!(
        "UPDATE t{tid}_sprig.row_policies SET using_mongo = '{MONGO}' WHERE name = 'p'"
    ))
    .await
    .unwrap();
    let rows: Vec<(String, String)> = r
        .query(format!(
            "SELECT TRIGGER_SCHEMA, TRIGGER_NAME FROM information_schema.TRIGGERS WHERE \
             TRIGGER_SCHEMA LIKE 't{tid}\\_d%' AND EVENT_OBJECT_TABLE = 'plain' AND \
             TRIGGER_NAME LIKE '\\_\\_sprig\\_rls\\_b%'"
        ))
        .await
        .unwrap();
    ck.ok("plain has three policy triggers", rows.len() == 3, || {
        format!("{rows:?}")
    });
    for (schema, name) in &rows {
        let row: mysql_async::Row = r
            .query_first(format!("SHOW CREATE TRIGGER `{schema}`.`{name}`"))
            .await
            .unwrap()
            .unwrap();
        let text: String = row.get(2).unwrap();
        let unlabelled = text.replace("__sprig_rls_f2: ", "");
        r.query_drop(format!("USE `{schema}`")).await.unwrap();
        r.query_drop(format!("DROP TRIGGER `{name}`"))
            .await
            .unwrap();
        r.query_drop(&unlabelled).await.unwrap();
    }
    let bodies = trigger_bodies(&mut r, tid, "plain").await;
    ck.ok(
        "plain's triggers look like an older build's",
        bodies.len() == 3 && bodies.iter().all(|b| !b.contains("__sprig_rls_f2:")),
        || format!("{bodies:?}"),
    );
    drop(admin);

    // A new build starts.
    restart();
    let mut admin = login(&sql_addr, "app", "sprig-dev-app").await;
    for (path, got) in paths(&sql_addr, &mongo_addr, &mut admin).await {
        let right = match path {
            "MongoDB countDocuments" | "SQL COUNT(*)" => {
                got.as_ref().is_ok_and(|g| g.len() == want.len())
            }
            _ => got.as_ref() == Ok(&want),
        };
        ck.ok(
            &format!("after: {path} shows what MongoDB's filter matches"),
            right,
            || format!("{got:?}, want {want:?}"),
        );
    }
    let (expr, m, cv, ch) = catalog_row(&mut r, tid).await;
    ck.ok(
        "the row holds the SQL the filter compiles to now",
        expr.contains("IN ('1', 'true')") && !expr.contains("= 'true'"),
        || expr.clone(),
    );
    ck.ok("the row keeps its MongoDB form", m.is_some(), || {
        format!("{m:?}")
    });
    ck.ok(
        "the table's checks are deployed for the rewritten row",
        cv >= ch,
        || format!("checks_version {cv}, changed_version {ch}"),
    );
    let views: Vec<String> = r
        .query(format!(
            "SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA LIKE \
             't{tid}\\_d%' AND TABLE_NAME LIKE '\\_\\_sprig\\_rls\\_notes%'"
        ))
        .await
        .unwrap();
    ck.ok(
        "the policy view no longer compares with 'true' alone",
        !views.is_empty() && views.iter().all(|v| !v.contains("= 'true'")),
        || format!("{views:?}"),
    );
    let bodies = trigger_bodies(&mut r, tid, "plain").await;
    ck.ok(
        "an older build's triggers are redeployed in this build's format",
        bodies.len() == 3 && bodies.iter().all(|b| b.contains("__sprig_rls_f2:")),
        || format!("{bodies:?}"),
    );
    let bodies = trigger_bodies(&mut r, tid, "notes").await;
    ck.ok(
        "the collection's triggers check the new SQL",
        bodies.len() == 3
            && bodies
                .iter()
                .all(|b| b.contains("__sprig_rls_f2:") && !b.contains("= 'true'")),
        || format!("{bodies:?}"),
    );
    drop(admin);

    // Once is enough: a second start rewrites nothing.
    restart();
    let mut admin = login(&sql_addr, "app", "sprig-dev-app").await;
    let n: Result<Option<i64>, _> = admin
        .query_first(format!("SELECT COUNT(*) FROM {DB}.notes"))
        .await;
    ck.ok("after a second start: a statement", n.is_ok(), || {
        format!("{n:?}")
    });
    let again = catalog_row(&mut r, tid).await;
    ck.ok(
        "a second start changes neither the row nor its version",
        again == (expr.clone(), m.clone(), cv, ch),
        || format!("{again:?} vs {:?}", (&expr, &m, cv, ch)),
    );

    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("DROP USER IF EXISTS '{ALICE}'@'%'"),
    ] {
        let _ = admin.query_drop(&q).await;
    }
    eprintln!(
        "policies stored by older builds: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
