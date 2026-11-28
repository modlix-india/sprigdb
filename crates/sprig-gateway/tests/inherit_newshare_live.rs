//! A child tenant and the databases its parent shares, through a gateway.
//!
//! 1. A parent shares a new database; before the child runs
//!    any statement, a client connects to the child with that database
//!    as its default (the handshake's database, the same path as
//!    `COM_INIT_DB`), and another prepares a statement naming it
//!    (`COM_STMT_PREPARE`). Before, both got 1049 "Unknown database" until
//!    the child ran a text statement such as `SHOW DATABASES`: only
//!    `COM_QUERY` brought the child up to its parent.
//! 2. The parent ends the share and drops the database. The
//!    child keeps its database of that name with its own rows
//!    (`inherit.parent_delete = keep`): it reads and writes them, the
//!    parent's records are gone from it, and it can drop the database.
//! 3. When the parent drops a
//!    share, the child's overrides of the parent's records become the
//!    child's own rows, visible as written (before, they were hidden), and
//!    the share view `__sprig_share_<id>` leaves the child's schema at once.
//!    With `SPRIG_TEST_NEWSHARE_GRANDCHILD` (a child of the child), the
//!    grandchild's overrides of records that came from the parent become
//!    its own too; with `SPRIG_TEST_NEWSHARE_BACKEND` (`host:port` of the
//!    cluster, root password in `SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD`, and
//!    `SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS`, a `LIKE` pattern of the
//!    deployment's physical schemas such as `t59306%`, none of whose other
//!    databases may share anything while it runs) the test also checks the
//!    view is gone.
//!
//! ```sh
//! SPRIG_TEST_NEWSHARE_GATEWAY=127.0.0.1:58306 \
//! SPRIG_TEST_NEWSHARE_PARENT=acme SPRIG_TEST_NEWSHARE_CHILD=bf11kid \
//!   cargo test -p sprig-gateway --test inherit_newshare_live -- --nocapture --test-threads 1
//! ```
//!
//! The child is a child tenant of the parent on the same cluster; both
//! have the user `app` (password `sprig-dev-app`). Each run uses its own
//! database names and drops them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

struct Env {
    addr: String,
    parent: String,
    child: String,
}

fn env() -> Option<Env> {
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        addr: get("SPRIG_TEST_NEWSHARE_GATEWAY")?,
        parent: get("SPRIG_TEST_NEWSHARE_PARENT")?,
        child: get("SPRIG_TEST_NEWSHARE_CHILD")?,
    })
}

fn opts(addr: &str, tenant: &str, db: Option<&str>) -> OptsBuilder {
    let (host, port) = addr.rsplit_once(':').unwrap();
    OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .db_name(db)
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    Conn::new(opts(addr, tenant, None)).await.expect("login")
}

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Vec<Vec<Option<String>>>, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn ok(c: &mut Conn, q: &str) -> Vec<Vec<Option<String>>> {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 1_000_000_000
}

/// The parent makes database `db` with an inheritable `t1` and shares it
/// to all its children.
async fn share(p: &mut Conn, db: &str) {
    share_with(p, db, "").await;
}

/// [`share`], with `opts` after the share's target (`WITH RESHARE`).
async fn share_with(p: &mut Conn, db: &str, opts: &str) {
    ok(p, &format!("CREATE DATABASE {db}")).await;
    ok(
        p,
        &format!("CREATE TABLE {db}.t1 (id INT PRIMARY KEY, v VARCHAR(40))"),
    )
    .await;
    ok(p, &format!("USE {db}")).await;
    ok(p, "ALTER TABLE t1 SPRIG INHERITABLE KEY (id)").await;
    ok(
        p,
        "INSERT INTO t1 VALUES (1, 'parent one'), (2, 'parent two')",
    )
    .await;
    ok(
        p,
        &format!("CREATE SHARE {db}_s ON t1 TO ALL CHILDREN {opts}"),
    )
    .await;
}

#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_new_share_is_usable_before_the_childs_first_statement() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = format!("bf11ns{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;

    // The handshake's database (as COM_INIT_DB): the child's first
    // command on this gateway since the share.
    let r = Conn::new(opts(&e.addr, &e.child, Some(&db))).await;
    let mut c = match r {
        Ok(c) => c,
        Err(mysql_async::Error::Server(s)) => {
            panic!("connecting with database {db}: {} {}", s.code, s.message)
        }
        Err(err) => panic!("connecting with database {db}: {err}"),
    };
    let rows = ok(&mut c, "SELECT DATABASE(), COUNT(*) FROM t1").await;
    assert_eq!(rows[0][0].as_deref(), Some(db.as_str()));
    assert_eq!(rows[0][1].as_deref(), Some("2"), "the parent's records");
    drop(c);

    // COM_STMT_PREPARE first: a second new database. A prepared statement
    // on an inheritable table is refused (39xxx), but never
    // with 1049 for a database the child has.
    let db2 = format!("{db}b");
    share(&mut p, &db2).await;
    let mut c = conn(&e.addr, &e.child).await;
    match c
        .prep(format!("SELECT COUNT(*) FROM {db2}.t1 WHERE id > ?"))
        .await
    {
        Ok(_) => println!("prepared on {db2}.t1"),
        Err(mysql_async::Error::Server(s)) => {
            assert_ne!(s.code, 1049, "{db2}: {}", s.message);
            println!(
                "prepare on {db2}.t1 refused (inheritable table): {} {}",
                s.code, s.message
            );
        }
        Err(err) => panic!("prepare: {err}"),
    }
    let dbs: Vec<String> = ok(&mut c, "SHOW DATABASES")
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert!(dbs.contains(&db) && dbs.contains(&db2), "{dbs:?}");
    drop(c);

    for d in [&db, &db2] {
        ok(&mut p, &format!("USE {d}")).await;
        ok(&mut p, &format!("DROP SHARE {d}_s")).await;
        ok(&mut p, &format!("DROP DATABASE {d}")).await;
    }
    let mut c = conn(&e.addr, &e.child).await;
    for d in [&db, &db2] {
        ok(&mut c, &format!("DROP DATABASE IF EXISTS {d}")).await;
    }
}

#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_child_keeps_its_rows_when_the_parent_drops_a_shared_database() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = format!("bf11nk{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    ok(&mut c, "INSERT INTO t1 VALUES (10, 'child own')").await;
    let rows = ok(&mut c, "SELECT id FROM t1 ORDER BY id").await;
    assert_eq!(rows.len(), 3, "two inherited and one own: {rows:?}");

    // The parent ends the share (its DROP DATABASE is refused while the
    // table is shared, 39895) and drops the database.
    assert_eq!(
        sql(&mut p, &format!("DROP DATABASE {db}"))
            .await
            .err()
            .map(|e| e.0),
        Some(39895)
    );
    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;

    // The child keeps its database and its own row; the parent's records
    // are gone from it.
    let rows = ok(&mut c, &format!("SELECT id, v FROM {db}.t1 ORDER BY id")).await;
    assert_eq!(
        rows,
        vec![vec![Some("10".to_owned()), Some("child own".to_owned())]]
    );
    ok(&mut c, &format!("INSERT INTO {db}.t1 VALUES (11, 'after')")).await;
    ok(
        &mut c,
        &format!("UPDATE {db}.t1 SET v = 'changed' WHERE id = 10"),
    )
    .await;
    let rows = ok(&mut c, &format!("SELECT COUNT(*) FROM {db}.t1")).await;
    assert_eq!(rows[0][0].as_deref(), Some("2"));
    // A new connection with it as the default database works too.
    let mut c2 = Conn::new(opts(&e.addr, &e.child, Some(&db)))
        .await
        .expect("the child's own database");
    assert_eq!(
        ok(&mut c2, "SELECT COUNT(*) FROM t1").await[0][0].as_deref(),
        Some("2")
    );
    drop(c2);
    // And the child can drop it.
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
    let dbs: Vec<String> = ok(&mut c, "SHOW DATABASES")
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert!(!dbs.contains(&db), "{dbs:?}");
}

/// `SELECT id, v FROM <db>.t1 ORDER BY id` as `(id, v)` text pairs.
async fn rows_of(c: &mut Conn, db: &str) -> Vec<(String, String)> {
    ok(c, &format!("SELECT id, v FROM {db}.t1 ORDER BY id"))
        .await
        .into_iter()
        .map(|r| (r[0].clone().unwrap(), r[1].clone().unwrap_or_default()))
        .collect()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect()
}

/// The share views in the physical schemas matching
/// `SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS` on the backend, when
/// `SPRIG_TEST_NEWSHARE_BACKEND` names it (physical schema names don't
/// carry the logical database's).
async fn share_views() -> Option<Vec<String>> {
    let addr = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND").ok()?;
    let pw = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD").ok()?;
    let like = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS").ok()?;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let mut b = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some("root"))
            .pass(Some(pw)),
    )
    .await
    .expect("backend login");
    let v: Vec<(String, String)> = b
        .query(format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_NAME \
             LIKE '\\_\\_sprig\\_share\\_%' AND TABLE_SCHEMA LIKE '{}'",
            like.replace('\'', "")
        ))
        .await
        .unwrap();
    drop(b);
    Some(v.into_iter().map(|(s, t)| format!("{s}.{t}")).collect())
}

#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_dropped_share_leaves_the_childs_overrides_as_its_own_rows() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let gk = std::env::var("SPRIG_TEST_NEWSHARE_GRANDCHILD")
        .ok()
        .filter(|s| !s.is_empty());
    let db = format!("bf12ds{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share_with(&mut p, &db, if gk.is_some() { "WITH RESHARE" } else { "" }).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    // The child overrides the parent's record 1 and adds record 10.
    ok(&mut c, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    ok(&mut c, "INSERT INTO t1 VALUES (10, 'kid own')").await;
    assert_eq!(
        rows_of(&mut c, &db).await,
        pairs(&[("1", "kid ovr"), ("2", "parent two"), ("10", "kid own")])
    );
    let mut g = None;
    if let Some(gk) = &gk {
        ok(
            &mut c,
            &format!("CREATE SHARE {db}_k ON t1 TO ALL CHILDREN"),
        )
        .await;
        let mut gc = conn(&e.addr, gk).await;
        ok(&mut gc, &format!("USE {db}")).await;
        // The grandchild overrides the parent's record 2 (through the
        // child) and the child's override of record 1.
        ok(&mut gc, "UPDATE t1 SET v = 'gk ovr 2' WHERE id = 2").await;
        ok(&mut gc, "UPDATE t1 SET v = 'gk ovr 1' WHERE id = 1").await;
        assert_eq!(
            rows_of(&mut gc, &db).await,
            pairs(&[("1", "gk ovr 1"), ("2", "gk ovr 2"), ("10", "kid own")])
        );
        g = Some(gc);
    }
    if let Some(v) = share_views().await {
        // The child's of the parent's share, and the grandchild's of the
        // child's.
        assert_eq!(v.len(), 1 + usize::from(gk.is_some()), "{v:?}");
        println!("share views before the drop: {v:?}");
    }

    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;

    // The child's override stays, as written; the parent's record 2 goes.
    let after = rows_of(&mut c, &db).await;
    println!("the child after the drop: {after:?}");
    assert_eq!(after, pairs(&[("1", "kid ovr"), ("10", "kid own")]));
    if let Some(gc) = g.as_mut() {
        let after = rows_of(gc, &db).await;
        println!("the grandchild after the drop: {after:?}");
        assert_eq!(
            after,
            pairs(&[("1", "gk ovr 1"), ("2", "gk ovr 2"), ("10", "kid own")])
        );
    }
    if let Some(v) = share_views().await {
        println!("share views after the drop: {v:?}");
        // The child's own share to the grandchild keeps its view; the
        // dropped share's view is gone.
        assert_eq!(v.len(), usize::from(gk.is_some()), "{v:?}");
    }
    // The row is the child's own: it updates and deletes as one.
    ok(&mut c, "UPDATE t1 SET v = 'kid changed' WHERE id = 1").await;
    assert_eq!(
        rows_of(&mut c, &db).await,
        pairs(&[("1", "kid changed"), ("10", "kid own")])
    );

    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    if let Some(mut gc) = g {
        // The child's share first: a tenant can't drop a database it
        // inherits (39897).
        ok(&mut c, &format!("DROP SHARE {db}_k")).await;
        ok(&mut gc, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
}

/// The materialized copies (`__sprig_ih_<object>`, not the state table)
/// in the physical schemas matching `SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS`,
/// as [`share_views`] reads the views.
async fn copy_tables() -> Option<Vec<String>> {
    let addr = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND").ok()?;
    let pw = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD").ok()?;
    let like = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS").ok()?;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let mut b = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some("root"))
            .pass(Some(pw)),
    )
    .await
    .expect("backend login");
    let v: Vec<(String, String)> = b
        .query(format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME \
             LIKE '\\_\\_sprig\\_ih\\_%' AND TABLE_NAME <> '__sprig_ih_state' AND TABLE_SCHEMA \
             LIKE '{}'",
            like.replace('\'', "")
        ))
        .await
        .unwrap();
    drop(b);
    Some(v.into_iter().map(|(s, t)| format!("{s}.{t}")).collect())
}

/// Waits (20 s at most) until the child's `t1` holds `want`.
async fn rows_become(c: &mut Conn, db: &str, want: &[(&str, &str)]) -> Vec<(String, String)> {
    let want = pairs(want);
    let mut last = Vec::new();
    for _ in 0..80 {
        if let Ok(r) = sql(c, &format!("SELECT id, v FROM {db}.t1 ORDER BY id")).await {
            last = r
                .into_iter()
                .map(|r| (r[0].clone().unwrap(), r[1].clone().unwrap_or_default()))
                .collect();
            if last == want {
                return last;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    last
}

/// On a materialized share: when the
/// parent drops the share, the child's hides go with it (a share made
/// again shows the record the child had hidden under the old one), and
/// the copy the child read the parent's rows from leaves its schema at
/// once (with `SPRIG_TEST_NEWSHARE_BACKEND*`, as for
/// `a_dropped_share_leaves_the_childs_overrides_as_its_own_rows`).
/// Before, the hide rows stayed (record 2 stayed hidden under the new
/// share) and the copy table stayed, emptied by the worker.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_dropped_share_takes_the_childs_hides_and_copy_with_it() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = format!("bf13dh{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    let mut c = conn(&e.addr, &e.child).await;
    // The share's mode is its space's `inherit.merge` (as groups_live sets
    // it).
    for (x, t) in [(&mut p, &e.parent), (&mut c, &e.child)] {
        ok(x, &format!("USE SPACE `{t}`")).await;
        ok(x, "SPRIG SET inherit.merge = 'materialized'").await;
    }
    // Copies other databases of the deployment hold (none share while
    // this runs, as for the share views).
    let others: Option<std::collections::BTreeSet<String>> =
        copy_tables().await.map(|v| v.into_iter().collect());
    share(&mut p, &db).await;
    ok(&mut c, &format!("USE {db}")).await;
    let seen = rows_become(&mut c, &db, &[("1", "parent one"), ("2", "parent two")]).await;
    assert_eq!(seen, pairs(&[("1", "parent one"), ("2", "parent two")]));
    // The share's copy (the worker seeds it).
    let mine = |v: Vec<String>, others: &std::collections::BTreeSet<String>| -> Vec<String> {
        v.into_iter().filter(|t| !others.contains(t)).collect()
    };
    if let Some(o) = &others {
        let mut copy = Vec::new();
        for _ in 0..80 {
            copy = mine(copy_tables().await.unwrap(), o);
            if !copy.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        println!("the share's copy: {copy:?}");
        assert!(
            !copy.is_empty(),
            "no copy table: the share isn't materialized"
        );
    }
    // The child hides the parent's record 2.
    ok(&mut c, "DELETE FROM t1 WHERE id = 2").await;
    assert_eq!(rows_of(&mut c, &db).await, pairs(&[("1", "parent one")]));

    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    assert_eq!(rows_of(&mut c, &db).await, pairs(&[]));
    if let Some(o) = &others {
        // Gone at once, or within the worker's next refresh (2 s) when it
        // was seeding at the moment of the drop.
        let mut left = Vec::new();
        for _ in 0..40 {
            left = mine(copy_tables().await.unwrap(), o);
            if left.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        println!("the share's copy 10 s after the drop at most: {left:?}");
        assert!(left.is_empty(), "the copy stayed: {left:?}");
    }

    // A new share of the table: the old share's hide is gone with it.
    ok(
        &mut p,
        &format!("CREATE SHARE {db}_s ON t1 TO ALL CHILDREN"),
    )
    .await;
    let seen = rows_become(&mut c, &db, &[("1", "parent one"), ("2", "parent two")]).await;
    println!("the child under the new share: {seen:?}");
    assert_eq!(
        seen,
        pairs(&[("1", "parent one"), ("2", "parent two")]),
        "record 2 stayed hidden from the dropped share"
    );

    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    ok(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    for (x, t) in [(&mut p, &e.parent), (&mut c, &e.child)] {
        ok(x, &format!("USE SPACE `{t}`")).await;
        ok(x, "SPRIG RESET inherit.merge").await;
    }
}

/// Runs `SPRIG_TEST_NEWSHARE_RESTART` (a script that kills the gateway
/// with `kill -9` and starts it again, waiting until it is ready).
fn restart_gateway() -> bool {
    let Ok(script) = std::env::var("SPRIG_TEST_NEWSHARE_RESTART") else {
        return false;
    };
    let out = std::process::Command::new(script)
        .arg("kill9")
        .output()
        .expect("restart script");
    println!("{}", String::from_utf8_lossy(&out.stdout).trim());
    out.status.success()
}

/// A gateway that stops between `DROP SHARE`'s commit and its cleanup in
/// the children no longer leaves the child's overrides hidden for good:
/// the share stays as a `dropping` row, and the next statement of the
/// child (or the parent) that reaches the inheritance layer runs the
/// cleanup. The cleanup is held up with a row lock on the child's
/// override (an open transaction of the child's), the gateway is killed
/// while it waits, and started again. Needs `SPRIG_TEST_NEWSHARE_RESTART`.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_drop_cut_off_by_a_gateway_crash_is_finished_by_the_next_statement() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    if std::env::var("SPRIG_TEST_NEWSHARE_RESTART").is_err() {
        eprintln!("SPRIG_TEST_NEWSHARE_RESTART not set; skipped");
        return;
    }
    let db = format!("rls11dc{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    let seen = rows_become(&mut c, &db, &[("1", "parent one"), ("2", "parent two")]).await;
    assert_eq!(seen, pairs(&[("1", "parent one"), ("2", "parent two")]));
    // The child overrides record 1 and hides record 2.
    ok(&mut c, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    ok(&mut c, "DELETE FROM t1 WHERE id = 2").await;
    assert_eq!(rows_of(&mut c, &db).await, pairs(&[("1", "kid ovr")]));
    // Another session of the child holds its override's row lock, so the
    // drop's cleanup waits on it.
    let mut held = conn(&e.addr, &e.child).await;
    ok(&mut held, &format!("USE {db}")).await;
    ok(&mut held, "BEGIN").await;
    ok(&mut held, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    ok(&mut p, &format!("USE {db}")).await;
    let addr = e.addr.clone();
    let parent = e.parent.clone();
    let name = format!("{db}_s");
    let drop = tokio::spawn(async move {
        let mut p = conn(&addr, &parent).await;
        // The gateway is killed under it: the call fails.
        p.query_drop(format!("DROP SHARE {name}")).await
    });
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(
        !drop.is_finished(),
        "the drop didn't wait for the child's lock"
    );
    // The gateway stops after the drop committed, before the cleanup ran.
    assert!(restart_gateway(), "the gateway didn't restart");
    let _ = drop.await;
    drop_conns(vec![p, c, held]).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    // The child's next statement finishes the drop: its override is its own
    // row, as written, and its hide is gone with the share.
    let after = rows_of(&mut c, &db).await;
    println!("the child after the restart: {after:?}");
    assert_eq!(after, pairs(&[("1", "kid ovr")]));
    let mut p = conn(&e.addr, &e.parent).await;
    ok(&mut p, &format!("USE {db}")).await;
    let shares = ok(&mut p, "SHOW SHARES").await;
    assert!(
        shares.is_empty(),
        "the dropped share is still listed: {shares:?}"
    );
    // A new share shows the record the child had hidden under the old one.
    ok(
        &mut p,
        &format!("CREATE SHARE {db}_s ON t1 TO ALL CHILDREN"),
    )
    .await;
    let seen = rows_become(&mut c, &db, &[("1", "kid ovr"), ("2", "parent two")]).await;
    println!("the child under a new share: {seen:?}");
    assert_eq!(seen, pairs(&[("1", "kid ovr"), ("2", "parent two")]));
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    ok(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

/// A root connection to the backend `SPRIG_TEST_NEWSHARE_BACKEND` names,
/// with the schema pattern, when set.
async fn backend() -> Option<(Conn, String)> {
    let addr = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND").ok()?;
    let pw = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD").ok()?;
    let like = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_SCHEMAS").ok()?;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let b = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some("root"))
            .pass(Some(pw)),
    )
    .await
    .expect("backend login");
    Some((b, like.replace('\'', "")))
}

/// The states of the granted share `name` in the catalogs matching the
/// backend's schema pattern, read on the backend (no gateway statement).
async fn share_states(b: &mut Conn, like: &str, name: &str) -> Vec<String> {
    let schemas: Vec<String> = b
        .query(format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = 'shares' \
             AND TABLE_SCHEMA LIKE '{like}'"
        ))
        .await
        .unwrap();
    let mut out = Vec::new();
    for sc in schemas {
        let v: Vec<String> = b
            .exec(
                format!("SELECT state FROM `{sc}`.shares WHERE name = ? AND role = 'granted'"),
                (name,),
            )
            .await
            .unwrap();
        out.extend(v);
    }
    out
}

/// The same crash as above, but nobody runs a statement afterwards: the
/// tree's copy worker finishes the drop by itself, a minute after it
/// first sees the share `dropping`, and removes the share's row. Checked
/// on the backend only until the row is gone, so no gateway statement can
/// have done it. Needs `SPRIG_TEST_NEWSHARE_RESTART`, the backend variables
/// and a worker.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_drop_cut_off_by_a_gateway_crash_is_finished_by_the_copy_worker() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    if std::env::var("SPRIG_TEST_NEWSHARE_RESTART").is_err() {
        eprintln!("SPRIG_TEST_NEWSHARE_RESTART not set; skipped");
        return;
    }
    let Some((mut b, like)) = backend().await else {
        eprintln!("SPRIG_TEST_NEWSHARE_BACKEND* not set; skipped");
        return;
    };
    let db = format!("wkdc{}", suffix());
    let name = format!("{db}_s");
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    let seen = rows_become(&mut c, &db, &[("1", "parent one"), ("2", "parent two")]).await;
    assert_eq!(seen, pairs(&[("1", "parent one"), ("2", "parent two")]));
    ok(&mut c, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    ok(&mut c, "DELETE FROM t1 WHERE id = 2").await;
    assert_eq!(rows_of(&mut c, &db).await, pairs(&[("1", "kid ovr")]));
    assert_eq!(share_states(&mut b, &like, &name).await, ["active"]);
    // The child holds its override's row lock, so the drop's cleanup waits;
    // the gateway is killed while it does.
    let mut held = conn(&e.addr, &e.child).await;
    ok(&mut held, &format!("USE {db}")).await;
    ok(&mut held, "BEGIN").await;
    ok(&mut held, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    let addr = e.addr.clone();
    let parent = e.parent.clone();
    let (db2, name2) = (db.clone(), name.clone());
    let drop = tokio::spawn(async move {
        let mut p = conn(&addr, &parent).await;
        p.query_drop(format!("USE {db2}")).await.unwrap();
        p.query_drop(format!("DROP SHARE {name2}")).await
    });
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(
        !drop.is_finished(),
        "the drop didn't wait for the child's lock"
    );
    assert!(restart_gateway(), "the gateway didn't restart");
    let _ = drop.await;
    drop_conns(vec![p, c, held]).await;
    // The drop committed and its cleanup didn't run.
    assert_eq!(share_states(&mut b, &like, &name).await, ["dropping"]);
    let started = std::time::Instant::now();
    let mut left = Vec::new();
    while started.elapsed() < std::time::Duration::from_secs(150) {
        left = share_states(&mut b, &like, &name).await;
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    println!(
        "the share's row went after {:.1} s with no statement: {left:?}",
        started.elapsed().as_secs_f64()
    );
    assert!(left.is_empty(), "still there: {left:?}");
    std::mem::drop(b);
    // The child's override is its own row, and the hide went with the share.
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    assert_eq!(rows_of(&mut c, &db).await, pairs(&[("1", "kid ovr")]));
    let mut p = conn(&e.addr, &e.parent).await;
    ok(&mut p, &format!("USE {db}")).await;
    ok(
        &mut p,
        &format!("CREATE SHARE {name} ON t1 TO ALL CHILDREN"),
    )
    .await;
    let seen = rows_become(&mut c, &db, &[("1", "kid ovr"), ("2", "parent two")]).await;
    assert_eq!(seen, pairs(&[("1", "kid ovr"), ("2", "parent two")]));
    ok(&mut p, &format!("DROP SHARE {name}")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    ok(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
}

async fn drop_conns(v: Vec<Conn>) {
    for c in v {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), c.disconnect()).await;
    }
}

/// When a parent changes a materialized share's filter, its child's copy
/// is seeded again; the copy made from the child's view below it (a
/// grandchild's) follows for the records the filter now hides or shows
/// (before, only changes of the source's rows reached it). Needs
/// `SPRIG_TEST_NEWSHARE_GRANDCHILD` and a worker.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_childs_reseed_reaches_the_grandchilds_copy() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let Some(gk) = std::env::var("SPRIG_TEST_NEWSHARE_GRANDCHILD")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_NEWSHARE_GRANDCHILD not set; skipped");
        return;
    };
    let db = format!("rls11rs{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    let mut c = conn(&e.addr, &e.child).await;
    let mut g = conn(&e.addr, &gk).await;
    for (x, t) in [(&mut p, &e.parent), (&mut c, &e.child), (&mut g, &gk)] {
        ok(x, &format!("USE SPACE `{t}`")).await;
        ok(x, "SPRIG SET inherit.merge = 'materialized'").await;
    }
    share_with(&mut p, &db, "WITH RESHARE").await;
    ok(
        &mut p,
        "INSERT INTO t1 VALUES (3, 'parent three'), (4, 'parent four')",
    )
    .await;
    ok(&mut c, &format!("USE {db}")).await;
    let all = [
        ("1", "parent one"),
        ("2", "parent two"),
        ("3", "parent three"),
        ("4", "parent four"),
    ];
    assert_eq!(rows_become(&mut c, &db, &all).await, pairs(&all));
    ok(
        &mut c,
        &format!("CREATE SHARE {db}_k ON t1 TO ALL CHILDREN"),
    )
    .await;
    ok(&mut g, &format!("USE {db}")).await;
    assert_eq!(rows_become(&mut g, &db, &all).await, pairs(&all));
    // Let both copies serve before the filter changes.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    ok(&mut p, &format!("ALTER SHARE {db}_s WHERE id <= 2")).await;
    let two = [("1", "parent one"), ("2", "parent two")];
    let child = rows_become(&mut c, &db, &two).await;
    println!("the child after the filter change: {child:?}");
    assert_eq!(child, pairs(&two));
    let grand = rows_become(&mut g, &db, &two).await;
    println!("the grandchild after the filter change: {grand:?}");
    assert_eq!(
        grand,
        pairs(&two),
        "the grandchild's copy kept the hidden records"
    );
    // And back: the records the filter shows again reach it too.
    ok(&mut p, &format!("ALTER SHARE {db}_s WHERE id <= 3")).await;
    let three = [
        ("1", "parent one"),
        ("2", "parent two"),
        ("3", "parent three"),
    ];
    let grand = rows_become(&mut g, &db, &three).await;
    println!("the grandchild after widening: {grand:?}");
    assert_eq!(grand, pairs(&three));
    ok(&mut c, &format!("DROP SHARE {db}_k")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    ok(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    ok(&mut g, &format!("DROP DATABASE IF EXISTS {db}")).await;
    for (x, t) in [(&mut p, &e.parent), (&mut c, &e.child), (&mut g, &gk)] {
        ok(x, &format!("USE SPACE `{t}`")).await;
        ok(x, "SPRIG RESET inherit.merge").await;
    }
}

/// A parent's `FORCE ROW LEVEL SECURITY` binds the parent's own managers,
/// but not the account its share views run as (the tenant's share
/// definer, which no user owns and no policy names): its children still
/// read every record the share gives them, read through and materialized,
/// also records the parent writes while forced. Before the share definer,
/// the views ran as the share's creator, whom `FORCE` bound, and every
/// child's read failed with 1142.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn a_parents_forced_policies_dont_bind_its_share_views() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    for merge in ["read_through", "materialized"] {
        let db = format!("sf20fp{}{}", &merge[..1], suffix());
        let mut p = conn(&e.addr, &e.parent).await;
        ok(&mut p, &format!("SPRIG SET inherit.merge = '{merge}'")).await;
        share(&mut p, &db).await;
        ok(&mut p, "SPRIG RESET inherit.merge").await;
        let both = [("1", "parent one"), ("2", "parent two")];
        let mut c = conn(&e.addr, &e.child).await;
        ok(&mut c, &format!("USE {db}")).await;
        assert_eq!(
            rows_become(&mut c, &db, &both).await,
            pairs(&both),
            "{merge}"
        );

        // A policy no row passes for reads, one that lets the parent
        // write, and FORCE, so the parent's own `app` (a manager) is bound.
        ok(
            &mut p,
            "CREATE POLICY sf20_none ON t1 FOR SELECT USING (id < 0)",
        )
        .await;
        ok(
            &mut p,
            "CREATE POLICY sf20_add ON t1 FOR INSERT WITH CHECK (TRUE)",
        )
        .await;
        ok(&mut p, "ALTER TABLE t1 ENABLE ROW LEVEL SECURITY").await;
        ok(&mut p, "ALTER TABLE t1 FORCE ROW LEVEL SECURITY").await;
        let seen = ok(&mut p, "SELECT COUNT(*) FROM t1").await;
        assert_eq!(
            seen[0][0].as_deref(),
            Some("0"),
            "{merge}: the parent is bound"
        );
        ok(&mut p, "INSERT INTO t1 VALUES (3, 'parent three')").await;
        let seen = ok(&mut p, "SELECT COUNT(*) FROM t1").await;
        assert_eq!(seen[0][0].as_deref(), Some("0"), "{merge}: still bound");

        // The child, on its open connection and on a new one, reads all
        // three records.
        let three = [
            ("1", "parent one"),
            ("2", "parent two"),
            ("3", "parent three"),
        ];
        let got = rows_become(&mut c, &db, &three).await;
        println!("{merge}: the child under the parent's FORCE: {got:?}");
        assert_eq!(got, pairs(&three), "{merge}");
        let mut c2 = conn(&e.addr, &e.child).await;
        assert_eq!(rows_of(&mut c2, &db).await, pairs(&three), "{merge}");
        drop_conns(vec![c2]).await;

        ok(&mut p, "ALTER TABLE t1 NO FORCE ROW LEVEL SECURITY").await;
        ok(&mut p, "ALTER TABLE t1 DISABLE ROW LEVEL SECURITY").await;
        ok(&mut p, "DROP POLICY sf20_none ON t1").await;
        ok(&mut p, "DROP POLICY sf20_add ON t1").await;
        ok(&mut p, &format!("DROP SHARE {db}_s")).await;
        ok(&mut p, &format!("DROP DATABASE {db}")).await;
        ok(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
        drop_conns(vec![p, c]).await;
    }
}

/// One REST call (`sql:execute`, or `path` for another data call) as the
/// tenant's `app`, with the `Sprig-Space` header when `space` names one:
/// `(HTTP status, body)`.
async fn rest_call(
    http: &str,
    tenant: &str,
    user: &str,
    pw: &str,
    path: &str,
    space: Option<&str>,
    body: &serde_json::Value,
) -> (u16, serde_json::Value) {
    use base64::Engine as _;
    use http_body_util::BodyExt as _;
    let tcp = tokio::net::TcpStream::connect(http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.{user}:{pw}"));
    let mut b = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", http)
        .header("authorization", format!("Basic {auth}"))
        .header("content-type", "application/json");
    if let Some(s) = space {
        b = b.header("sprig-space", s);
    }
    let resp = tx
        .send_request(
            b.body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

/// The `(id, v)` rows of a REST `sql:execute` reply.
fn rest_rows(v: &serde_json::Value) -> Vec<(String, String)> {
    let r = sprig_api::results::execute_from_json(v).unwrap_or_else(|e| panic!("{e}: {v}"));
    let text = |c: &prost_types::Value| {
        sprig_api::results::cell_bytes(c, false)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default()
    };
    r.results
        .iter()
        .flat_map(|res| res.rows.iter())
        .map(|row| (text(&row.values[0]), text(&row.values[1])))
        .collect()
}

/// The `Sprig-Space` header (gRPC metadata `sprig-space`) runs a call's
/// session in the space it names, as `USE SPACE` does: reads see that
/// space's view and writes land there; a transaction keeps the space its
/// `Begin` named; an unknown space is 39880 and one above the user's home
/// space 39884. Needs `SPRIG_TEST_NEWSHARE_HTTP` (the gateway's HTTP
/// listener, plain HTTP).
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn the_space_header_picks_the_calls_space() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let Some(http) = std::env::var("SPRIG_TEST_NEWSHARE_HTTP")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_NEWSHARE_HTTP not set; skipped");
        return;
    };
    let pw = "sprig-dev-app";
    let n = suffix();
    let db = format!("sf20sh{n}");
    let space = format!("sf20s{n}");
    let user = format!("sf20u{n}");
    let mut p = conn(&e.addr, &e.parent).await;
    ok(&mut p, &format!("USE SPACE `{}`", e.parent)).await;
    ok(&mut p, &format!("CREATE SPACE `{space}`")).await;
    ok(&mut p, &format!("CREATE DATABASE {db}")).await;
    ok(
        &mut p,
        &format!("CREATE TABLE {db}.t1 (id INT PRIMARY KEY, v VARCHAR(40))"),
    )
    .await;
    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, "ALTER TABLE t1 SPRIG INHERITABLE KEY (id)").await;
    ok(
        &mut p,
        "INSERT INTO t1 VALUES (1, 'root one'), (2, 'root two')",
    )
    .await;
    ok(
        &mut p,
        &format!("CREATE SHARE {db}_sp ON t1 TO SPACE `{space}`"),
    )
    .await;
    ok(&mut p, &format!("USE SPACE `{space}`")).await;
    ok(&mut p, "UPDATE t1 SET v = 'space one' WHERE id = 1").await;
    ok(&mut p, &format!("USE SPACE `{}`", e.parent)).await;
    ok(
        &mut p,
        &format!("CREATE USER '{user}'@'%' IDENTIFIED BY '{pw}'"),
    )
    .await;
    ok(&mut p, &format!("GRANT SELECT ON {db}.* TO '{user}'@'%'")).await;
    ok(
        &mut p,
        &format!("ALTER SPACE `{space}` SET HOME FOR '{user}'@'%'"),
    )
    .await;

    let exec = format!("/v1/databases/{db}/sql:execute");
    let read = serde_json::json!({ "sql": "SELECT id, v FROM t1 ORDER BY id" });
    let root = pairs(&[("1", "root one"), ("2", "root two")]);
    let in_space = pairs(&[("1", "space one"), ("2", "root two")]);

    // REST: no header reads the home (root) space, the header the space.
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &exec, None, &read).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(rest_rows(&v), root);
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &exec, Some(&space), &read).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(rest_rows(&v), in_space, "the header's space");

    // A write with the header lands in the space, not in the root.
    let upd = serde_json::json!({ "sql": "UPDATE t1 SET v = 'rest two' WHERE id = 2" });
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &exec, Some(&space), &upd).await;
    assert_eq!(s, 200, "{v}");
    let (_, v) = rest_call(&http, &e.parent, "app", pw, &exec, Some(&space), &read).await;
    assert_eq!(
        rest_rows(&v),
        pairs(&[("1", "space one"), ("2", "rest two")])
    );
    assert_eq!(rows_of(&mut p, &db).await, root, "the root is unchanged");

    // A transaction keeps the space its Begin named.
    let (s, t) = rest_call(
        &http,
        &e.parent,
        "app",
        pw,
        &format!("/v1/databases/{db}/transactions"),
        Some(&space),
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(s, 200, "{t}");
    let handle = t["name"].as_str().unwrap().to_owned();
    let in_txn = serde_json::json!({
        "sql": "SELECT id, v FROM t1 ORDER BY id",
        "transaction": handle,
    });
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &exec, None, &in_txn).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(
        rest_rows(&v),
        pairs(&[("1", "space one"), ("2", "rest two")]),
        "a call in the transaction reads Begin's space"
    );
    let (s, _) = rest_call(
        &http,
        &e.parent,
        "app",
        pw,
        &format!("/v1/{handle}:rollback"),
        None,
        &serde_json::json!({}),
    )
    .await;
    assert_eq!(s, 200);

    // Refusals: an unknown space, and one above the user's home space.
    let (s, v) = rest_call(
        &http,
        &e.parent,
        "app",
        pw,
        &exec,
        Some("sf20_nosuch"),
        &read,
    )
    .await;
    assert_eq!((s >= 400, v["code"].as_u64()), (true, Some(39880)), "{v}");
    let (s, v) = rest_call(&http, &e.parent, &user, pw, &exec, None, &read).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(
        rest_rows(&v),
        pairs(&[("1", "space one"), ("2", "rest two")]),
        "home space"
    );
    let (s, v) = rest_call(&http, &e.parent, &user, pw, &exec, Some(&e.parent), &read).await;
    assert_eq!((s >= 400, v["code"].as_u64()), (true, Some(39884)), "{v}");

    // gRPC: the metadata `sprig-space`.
    {
        use base64::Engine as _;
        use sprig_api::proto::v1::QueryRequest;
        use sprig_api::proto::v1::sql_client::SqlClient;
        let ch = tonic::transport::Endpoint::from_shared(format!("http://{http}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = SqlClient::new(ch);
        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}.app:{pw}", e.parent))
        );
        for (sp, want) in [
            (None, root.clone()),
            (
                Some(space.as_str()),
                pairs(&[("1", "space one"), ("2", "rest two")]),
            ),
        ] {
            let mut req = tonic::Request::new(QueryRequest {
                database: db.clone(),
                sql: "SELECT id, v FROM t1 ORDER BY id".into(),
                ..QueryRequest::default()
            });
            req.metadata_mut()
                .insert("authorization", auth.parse().unwrap());
            if let Some(sp) = sp {
                req.metadata_mut()
                    .insert("sprig-space", sp.parse().unwrap());
            }
            let r = client.execute(req).await.unwrap().into_inner();
            let text = |c: &prost_types::Value| {
                sprig_api::results::cell_bytes(c, false)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };
            let got: Vec<(String, String)> = r
                .results
                .iter()
                .flat_map(|res| res.rows.iter())
                .map(|row| (text(&row.values[0]), text(&row.values[1])))
                .collect();
            assert_eq!(got, want, "gRPC with space {sp:?}");
        }
        let mut req = tonic::Request::new(QueryRequest {
            database: db.clone(),
            sql: "SELECT id FROM t1".into(),
            ..QueryRequest::default()
        });
        req.metadata_mut()
            .insert("authorization", auth.parse().unwrap());
        req.metadata_mut()
            .insert("sprig-space", "sf20_nosuch".parse().unwrap());
        let err = client.execute(req).await.unwrap_err();
        println!("gRPC with an unknown space: {err:?}");
        assert!(err.message().contains("doesn't exist"), "{err:?}");
    }

    ok(&mut p, &format!("DROP USER '{user}'@'%'")).await;
    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_sp")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    ok(&mut p, &format!("DROP SPACE `{space}` CASCADE")).await;
    drop_conns(vec![p]).await;
}

/// Analytics models can't read inheritable tables: the Analytics API reads
/// through internal statements the inheritance layer doesn't rewrite, so it
/// summed every space's layer rows (an override and the record it
/// overrides, hide markers, other spaces' records). A model over one is
/// refused (39752) when it is made, and at its next query when a table
/// becomes inheritable later. Needs `SPRIG_TEST_NEWSHARE_HTTP`.
#[ignore = "live: needs SPRIG_TEST_NEWSHARE_GATEWAY, SPRIG_TEST_NEWSHARE_PARENT, SPRIG_TEST_NEWSHARE_BACKEND, ...; tools/live/README.md"]
#[tokio::test]
async fn analytics_models_refuse_inheritable_tables() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let Some(http) = std::env::var("SPRIG_TEST_NEWSHARE_HTTP")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_NEWSHARE_HTTP not set; skipped");
        return;
    };
    let pw = "sprig-dev-app";
    let db = format!("sf20an{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    ok(&mut p, &format!("CREATE DATABASE {db}")).await;
    ok(&mut p, &format!("USE {db}")).await;
    ok(
        &mut p,
        "CREATE TABLE store (store_id INT PRIMARY KEY, country VARCHAR(10), region VARCHAR(10))",
    )
    .await;
    ok(&mut p, "INSERT INTO store VALUES (1, 'IN', 'S')").await;
    for t in ["sale", "sale2"] {
        ok(
            &mut p,
            &format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY, store_id INT, sold_at DATETIME, amount INT)"
            ),
        )
        .await;
        ok(
            &mut p,
            &format!(
                "INSERT INTO {t} VALUES (1, 1, '2026-01-01', 10), (2, 1, '2026-01-02', 20), \
                 (3, 1, '2026-01-03', 30)"
            ),
        )
        .await;
    }
    ok(&mut p, "ALTER TABLE sale SPRIG INHERITABLE KEY (id)").await;
    let def = |name: &str, table: &str| {
        serde_json::json!({"definition": {"model": name, "time_zone": "UTC",
            "dimensions": [{"name": "store", "table": "store", "key": "store_id",
                            "levels": ["country", "region"]},
                           {"name": "date", "builtin": "time"}],
            "facts": [{"name": name, "table": table, "key": ["id"],
                "links": [{"dimension": "store", "column": "store_id"},
                          {"dimension": "date", "column": "sold_at"}],
                "measures": [{"name": "revenue", "expr": "amount", "agg": "sum"}]}]}})
    };
    let models = format!("/v1/databases/{db}/models");
    let (s, v) = rest_call(
        &http,
        &e.parent,
        "app",
        pw,
        &models,
        None,
        &def("m1", "sale"),
    )
    .await;
    println!("a model over an inheritable table: {s} {v}");
    assert_eq!((s >= 400, v["code"].as_u64()), (true, Some(39752)), "{v}");

    // A model over a plain table answers; once the table is inheritable,
    // its next query is refused.
    let (s, v) = rest_call(
        &http,
        &e.parent,
        "app",
        pw,
        &models,
        None,
        &def("m2", "sale2"),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let q = serde_json::json!({"measures": ["revenue"],
        "dimensions": [{"dimension": "store", "level": "country"}]});
    let query = format!("{models}/m2:query");
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &query, None, &q).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v["rows"][0]["values"][0], "60", "{v}");
    ok(&mut p, "ALTER TABLE sale2 SPRIG INHERITABLE KEY (id)").await;
    let (s, v) = rest_call(&http, &e.parent, "app", pw, &query, None, &q).await;
    println!("the model's table made inheritable: {s} {v}");
    assert_eq!((s >= 400, v["code"].as_u64()), (true, Some(39752)), "{v}");

    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    drop_conns(vec![p]).await;
}
