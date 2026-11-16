#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{DriverError, Opts, OptsBuilder, PoolConstraints, PoolOpts};

use super::*;

/// A server that accepts connections and never greets them: a connection
/// to it never completes, so it keeps the pool's one slot taken.
async fn silent_server() -> (u16, tokio::task::JoinHandle<()>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let h = tokio::spawn(async move {
        let mut kept = Vec::new();
        while let Ok((s, _)) = l.accept().await {
            kept.push(s);
        }
    });
    (port, h)
}

fn one_conn(opts: OptsBuilder) -> Pool {
    Pool::new(
        opts.pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, 1).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

fn silent_pool(port: u16) -> Pool {
    one_conn(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("nobody")),
    )
}

#[tokio::test]
async fn a_wait_past_the_limit_fails_naming_the_pool_and_cluster() {
    let (port, server) = silent_server().await;
    let pool = silent_pool(port);
    let after = Duration::from_millis(300);
    limit(&pool, "sprig_ctl", "c7", 1, after);
    // A clone carries the limit.
    assert_eq!(limit_of(&pool.clone()), Some(after));
    // The first takes the one slot (its connection never completes), the
    // second waits for it; both give up at the limit.
    let t0 = Instant::now();
    let (a, b) = tokio::join!(pool.get_conn_bounded(), pool.get_conn_bounded());
    let took = t0.elapsed();
    assert!(took >= after, "gave up early: {took:?}");
    assert!(took < Duration::from_secs(3), "gave up late: {took:?}");
    for r in [a, b] {
        let e = r.expect_err("no server answers: both waits must fail");
        let t = PoolWaitTimeout::of(&e).unwrap_or_else(|| panic!("not a wait timeout: {e}"));
        assert_eq!((t.pool, t.cluster.as_str(), t.max), ("sprig_ctl", "c7", 1));
        assert!(t.waited >= after);
        let text = e.to_string();
        assert!(
            text.contains("no connection from the sprig_ctl pool of cluster c7 within")
                && text.contains("of 1 in use"),
            "{text}"
        );
    }
    let e = pool
        .start_transaction_bounded(TxOpts::default())
        .await
        .expect_err("a transaction waits the same way");
    assert!(PoolWaitTimeout::of(&e).is_some(), "{e}");
    server.abort();
}

#[tokio::test]
async fn a_pool_without_a_limit_waits_as_the_drivers_does() {
    let (port, server) = silent_server().await;
    let pool = silent_pool(port);
    assert_eq!(limit_of(&pool), None);
    let r = tokio::time::timeout(Duration::from_millis(500), pool.get_conn_bounded()).await;
    assert!(r.is_err(), "an unlimited wait must still be waiting");
    let e = mysql_async::Error::Driver(DriverError::PoolDisconnected);
    assert!(PoolWaitTimeout::of(&e).is_none());
    server.abort();
}

/// Against a real server (`SPRIG_TEST_MARIADB_SINGLE`): with the one
/// connection held, the next wait fails at the limit; once it is back, the
/// pool works again.
#[tokio::test]
async fn a_held_connection_makes_the_next_wait_fail_then_frees() {
    let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let pool = one_conn(OptsBuilder::from_opts(Opts::from_url(url.trim()).unwrap()));
    let after = Duration::from_millis(400);
    limit(&pool, "catalog", "c1", 1, after);
    let mut held = pool.get_conn_bounded().await.unwrap();
    let one: Option<u8> = held.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    let t0 = Instant::now();
    let e = pool
        .get_conn_bounded()
        .await
        .expect_err("the one connection is held");
    let took = t0.elapsed();
    let t = PoolWaitTimeout::of(&e).unwrap_or_else(|| panic!("not a wait timeout: {e}"));
    assert_eq!((t.in_use, t.max), (1, 1), "{t}");
    assert!(took >= after && took < Duration::from_secs(3), "{took:?}");
    drop(held);
    let mut c = pool.get_conn_bounded().await.unwrap();
    let one: Option<u8> = c.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    drop(c);
    pool.disconnect().await.unwrap();
}
