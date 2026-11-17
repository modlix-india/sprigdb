//! Row-level security and statements prepared before a policy changed,
//! live, through both personas.
//!
//! A user prepares statements (`COM_STMT_PREPARE`, and SQL `PREPARE`)
//! while table `t` has no policy and runs them once. The administrator then
//! changes the table's row security under the open session, and the same
//! statements run again:
//!
//! - **policy created and row security enabled:** the protocol statements
//!   are prepared again under the policy, as a fresh prepare would be: a
//!   point read of another owner's row finds nothing, the user's own row is
//!   read, a count counts the user's rows, and an update of a hidden row
//!   changes nothing. A SQL `PREPARE` naming a protected table is refused
//!   (39366), so `EXECUTE` of one made before the policy is refused too,
//!   whether its text was a literal or a variable's; so is a fresh
//!   `PREPARE ... FROM @var` naming it. In older builds, under
//!   `rls.backend_enforcement = triggers` or `off` such an `EXECUTE`
//!   counted the hidden rows (MariaDB held the statement), and under
//!   `views` MariaDB refused it (1142);
//! - **row security disabled:** prepared again without the policy, every
//!   row reads again;
//! - **enabled again, then the policy dropped:** no policy shows no row;
//!   disabled again, every row.
//!
//! Each persona runs it under each `rls.backend_enforcement` value
//! (`views`, `triggers`, `off`).
//!
//! ```sh
//! export SPRIG_TEST_RLS=127.0.0.1:57306,127.0.0.1:57307   # both personas
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_prepared_live -- --nocapture
//! ```
//!
//! It uses database `rlsprep_db` and users `rlsprep_a` and `rlsprep_b`, and
//! drops them at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts, Statement};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_RLS";
const DB: &str = "rlsprep_db";
const PW: &str = "Rlsprep-pw-1";
const USERS: [&str; 2] = ["rlsprep_a", "rlsprep_b"];

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn conn(addr: &str, user: &str, pw: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .db_name(db)
        .prefer_socket(false)
        .stmt_cache_size(0)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("login {user} at {addr}: {e}"))
}

/// A server error's code; 0 for any other error.
fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        _ => 0,
    }
}

type Answer = Result<Option<i64>, u16>;

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

/// The user's open session and what it prepared before the change.
struct Prepared {
    c: Conn,
    /// `SELECT id FROM t WHERE id = ?`
    point: Statement,
    /// `SELECT COUNT(*) FROM t`
    count: Statement,
    /// `UPDATE t SET n = n + 1 WHERE id = ?`
    bump: Statement,
}

impl Prepared {
    async fn point(&mut self, id: i64) -> Answer {
        self.c
            .exec_first::<i64, _, _>(&self.point, (id,))
            .await
            .map_err(|e| code(&e))
    }

    async fn count(&mut self) -> Answer {
        self.c
            .exec_first::<i64, _, _>(&self.count, ())
            .await
            .map_err(|e| code(&e))
    }

    async fn bump(&mut self, id: i64) -> Result<u64, u16> {
        self.c
            .exec_drop(&self.bump, (id,))
            .await
            .map_err(|e| code(&e))?;
        Ok(self.c.affected_rows())
    }

    /// `EXECUTE s` (or `v`, prepared from a variable) of the SQL-level
    /// statement prepared before.
    async fn sql_count(&mut self) -> Answer {
        self.sql_exec("s").await
    }

    async fn sql_exec(&mut self, name: &str) -> Answer {
        self.c
            .query_first::<i64, _>(format!("EXECUTE {name}"))
            .await
            .map_err(|e| code(&e))
    }

    /// Counts until `want` holds or ten seconds pass (a change reaches the
    /// gateway through the catalog's watch); the last answer.
    async fn count_until(&mut self, want: impl Fn(&Answer) -> bool) -> Answer {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let r = self.count().await;
            if want(&r) || Instant::now() > deadline {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn cleanup(admin: &mut Conn) {
    for u in USERS {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
}

async fn setup(admin: &mut Conn) {
    cleanup(admin).await;
    let rows: Vec<String> = (1..=10i64)
        .map(|i| format!("({i}, '{}@%', {i})", USERS[usize::from(i % 2 == 1)]))
        .collect();
    for s in [
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.t (id INT PRIMARY KEY, owner VARCHAR(64) NOT NULL, n INT)"),
        format!("INSERT INTO {DB}.t VALUES {}", rows.join(", ")),
    ] {
        admin
            .query_drop(&s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    for u in USERS {
        for s in [
            format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"),
            format!("GRANT SELECT, INSERT, UPDATE, DELETE ON {DB}.* TO '{u}'@'%'"),
        ] {
            admin
                .query_drop(&s)
                .await
                .unwrap_or_else(|e| panic!("{s}: {e}"));
        }
    }
}

/// `rlsprep_a`'s session, its statements prepared and run once before any
/// policy. `rlsprep_a` owns the even ids.
async fn prepare(ck: &mut Checks, addr: &str, tag: &str) -> Prepared {
    let mut c = conn(addr, USERS[0], PW, Some(DB)).await;
    let point = c.prep("SELECT id FROM t WHERE id = ?").await.unwrap();
    let count = c.prep("SELECT COUNT(*) FROM t").await.unwrap();
    let bump = c.prep("UPDATE t SET n = n + 1 WHERE id = ?").await.unwrap();
    c.query_drop("PREPARE s FROM 'SELECT COUNT(*) FROM t'")
        .await
        .unwrap();
    c.query_drop("SET @q = 'SELECT COUNT(*) FROM t'")
        .await
        .unwrap();
    // The variable reads back on the pinned connection, whatever an
    // earlier session left on it.
    let q: Option<String> = c.query_first("SELECT @q").await.unwrap();
    ck.ok(
        &format!("{tag} before: @q set"),
        q.as_deref() == Some("SELECT COUNT(*) FROM t"),
        || format!("{q:?}"),
    );
    c.query_drop("PREPARE v FROM @q").await.unwrap();
    let mut p = Prepared {
        c,
        point,
        count,
        bump,
    };
    for _ in 0..3 {
        let r = p.point(1).await;
        ck.ok(
            &format!("{tag} before: point read"),
            r == Ok(Some(1)),
            || format!("{r:?}"),
        );
        let r = p.count().await;
        ck.ok(&format!("{tag} before: count"), r == Ok(Some(10)), || {
            format!("{r:?}")
        });
        let r = p.sql_count().await;
        ck.ok(&format!("{tag} before: EXECUTE"), r == Ok(Some(10)), || {
            format!("{r:?}")
        });
        let r = p.sql_exec("v").await;
        ck.ok(
            &format!("{tag} before: EXECUTE (from @var)"),
            r == Ok(Some(10)),
            || format!("{r:?}"),
        );
    }
    p
}

/// The user's view under the `own` policy: ids 2, 4, ... 10.
async fn filtered(ck: &mut Checks, p: &mut Prepared, tag: &str) {
    let r = p.count_until(|r| *r == Ok(Some(5))).await;
    ck.ok(&format!("{tag}: count filtered"), r == Ok(Some(5)), || {
        format!("{r:?}")
    });
    for _ in 0..3 {
        let r = p.point(1).await;
        ck.ok(
            &format!("{tag}: other owner's row hidden"),
            r == Ok(None),
            || format!("{r:?}"),
        );
        let r = p.point(2).await;
        ck.ok(&format!("{tag}: own row read"), r == Ok(Some(2)), || {
            format!("{r:?}")
        });
        let r = p.count().await;
        ck.ok(
            &format!("{tag}: count stays filtered"),
            r == Ok(Some(5)),
            || format!("{r:?}"),
        );
    }
    let r = p.bump(1).await;
    ck.ok(
        &format!("{tag}: hidden row not updated"),
        r == Ok(0),
        || format!("{r:?}"),
    );
    let r = p.bump(2).await;
    ck.ok(&format!("{tag}: own row updated"), r == Ok(1), || {
        format!("{r:?}")
    });
    // A SQL PREPARE naming a protected table is refused: EXECUTE of one
    // made before is refused too, from a literal or a variable.
    for name in ["s", "v"] {
        let r = p.sql_exec(name).await;
        ck.ok(
            &format!("{tag}: EXECUTE {name} of an earlier PREPARE refused"),
            r == Err(39366),
            || format!("{r:?}"),
        );
    }
    // A fresh one from a variable is refused as a literal is.
    let e = p.c.query_drop("PREPARE w FROM @q").await;
    ck.ok(
        &format!("{tag}: fresh PREPARE from @var refused"),
        e.as_ref().is_err_and(|e| code(e) == 39366),
        || format!("{e:?}"),
    );
    let e = p.c.query_drop("EXECUTE IMMEDIATE @q").await;
    ck.ok(
        &format!("{tag}: EXECUTE IMMEDIATE @var refused"),
        e.as_ref().is_err_and(|e| code(e) == 39366),
        || format!("{e:?}"),
    );
}

async fn unfiltered(ck: &mut Checks, p: &mut Prepared, tag: &str) {
    let r = p.count_until(|r| *r == Ok(Some(10))).await;
    ck.ok(
        &format!("{tag}: count unfiltered"),
        r == Ok(Some(10)),
        || format!("{r:?}"),
    );
    for _ in 0..3 {
        let r = p.point(1).await;
        ck.ok(
            &format!("{tag}: other owner's row read"),
            r == Ok(Some(1)),
            || format!("{r:?}"),
        );
        let r = p.count().await;
        ck.ok(
            &format!("{tag}: count stays unfiltered"),
            r == Ok(Some(10)),
            || format!("{r:?}"),
        );
    }
    let r = p.bump(1).await;
    ck.ok(&format!("{tag}: any row updated"), r == Ok(1), || {
        format!("{r:?}")
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statements_prepared_before_a_policy_change() {
    let Some(addrs) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "statements_prepared_before_a_policy_change");
        return;
    };
    let addrs = skip::urls(&addrs);
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = conn(&addrs[0], "app", "sprig-dev-app", None).await;
    for (addr, enforcement) in addrs
        .iter()
        .flat_map(|a| ["views", "triggers", "off"].map(|e| (a, e)))
    {
        let tag = format!("[{addr} {enforcement}]");
        setup(&mut admin).await;
        admin
            .query_drop(format!(
                "ALTER DATABASE {DB} SPRIG SET (rls.backend_enforcement = '{enforcement}')"
            ))
            .await
            .unwrap();
        let mut p = prepare(&mut ck, addr, &tag).await;

        // A policy created and row security enabled under the session.
        for s in [
            format!(
                "CREATE POLICY own ON {DB}.t USING (owner = CURRENT_USER()) \
                 WITH CHECK (owner = CURRENT_USER())"
            ),
            format!("ALTER TABLE {DB}.t ENABLE ROW LEVEL SECURITY"),
        ] {
            admin
                .query_drop(&s)
                .await
                .unwrap_or_else(|e| panic!("{s}: {e}"));
        }
        filtered(&mut ck, &mut p, &format!("{tag} enabled")).await;
        // A fresh SQL PREPARE is refused now.
        let e =
            p.c.query_drop("PREPARE s2 FROM 'SELECT COUNT(*) FROM t'")
                .await;
        ck.ok(
            &format!("{tag} enabled: fresh SQL PREPARE refused"),
            e.as_ref().is_err_and(|e| code(e) == 39366),
            || format!("{e:?}"),
        );

        // The mirror: row security disabled.
        admin
            .query_drop(format!("ALTER TABLE {DB}.t DISABLE ROW LEVEL SECURITY"))
            .await
            .unwrap();
        unfiltered(&mut ck, &mut p, &format!("{tag} disabled")).await;
        for name in ["s", "v"] {
            let r = p.sql_exec(name).await;
            ck.ok(
                &format!("{tag} disabled: EXECUTE {name}"),
                r == Ok(Some(10)),
                || format!("{r:?}"),
            );
        }

        // Enabled again, then the policy dropped: no row shows.
        admin
            .query_drop(format!("ALTER TABLE {DB}.t ENABLE ROW LEVEL SECURITY"))
            .await
            .unwrap();
        filtered(&mut ck, &mut p, &format!("{tag} enabled again")).await;
        admin
            .query_drop(format!("DROP POLICY own ON {DB}.t"))
            .await
            .unwrap();
        let r = p.count_until(|r| *r == Ok(Some(0))).await;
        ck.ok(
            &format!("{tag} policy dropped: no row counted"),
            r == Ok(Some(0)),
            || format!("{r:?}"),
        );
        let r = p.point(2).await;
        ck.ok(
            &format!("{tag} policy dropped: own row hidden"),
            r == Ok(None),
            || format!("{r:?}"),
        );
        admin
            .query_drop(format!("ALTER TABLE {DB}.t DISABLE ROW LEVEL SECURITY"))
            .await
            .unwrap();
        unfiltered(&mut ck, &mut p, &format!("{tag} disabled again")).await;

        drop(p);
    }
    cleanup(&mut admin).await;
    eprintln!(
        "statements prepared before a policy change: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
