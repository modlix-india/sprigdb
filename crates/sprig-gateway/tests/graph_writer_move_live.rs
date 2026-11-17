//! Live: Cypher writes that need new ids go on after the graph's cluster
//! changes writer node, without a gateway restart. The gateway keeps each
//! graph's id counters over the cluster's `sprig_ctl` pool; when the writer
//! moved, that pool was replaced and disconnected, and every later `CREATE`
//! failed with "graph storage unavailable" until the gateway restarted.
//! Against a private deployment with a Galera cluster of several nodes.
//!
//! ```text
//! SPRIG_GWM_BOLT=127.0.0.1:44687      # the gateway's Bolt listener (TLS optional)
//! SPRIG_GWM_USER=<tenant>.app          # a user that may write the graph
//! SPRIG_GWM_PASSWORD=...
//! SPRIG_GWM_GRAPH=bank.g               # an existing graph (Bolt's db) on that cluster
//! SPRIG_GWM_MOVE='<command>'           # makes the cluster's writer change (stops its node)
//! SPRIG_GWM_RESTORE='<command>'        # optional: starts the node again
//! cargo test -p sprig-gateway --test graph_writer_move_live -- --ignored --nocapture
//! ```
//!
//! Checks: a `CREATE` before the move; after the move, once a write that
//! needs no new id goes through again (the new writer serves), a `CREATE`
//! succeeds within 20 s.

use std::time::{Duration, Instant};

use sprig_proto_bolt::client::BoltClient;
use sprig_proto_bolt::packstream::PackValue;
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn connect(addr: &str, user: &str, pass: &str) -> Result<BoltClient<TcpStream>, String> {
    let s = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let mut c = BoltClient::handshake(s, None)
        .await
        .map_err(|e| format!("{e:?}"))?;
    c.login(user, pass).await.map_err(|e| format!("{e:?}"))?;
    Ok(c)
}

/// One auto-commit statement on a fresh connection.
async fn run(addr: &str, user: &str, pass: &str, graph: &str, q: &str) -> Result<(), String> {
    let mut c = connect(addr, user, pass).await?;
    let r = tokio::time::timeout(
        Duration::from_secs(30),
        c.query(
            q,
            Vec::new(),
            vec![("db", PackValue::String(graph.to_owned()))],
        ),
    )
    .await
    .map_err(|_| "timeout".to_owned())?
    .map(drop)
    .map_err(|e| format!("{} {}", e.code, e.message));
    let _ = c.goodbye().await;
    r
}

fn shell(cmd: &str) {
    let st = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .expect("run the command");
    assert!(st.success(), "{cmd} failed: {st}");
}

#[tokio::test]
#[ignore = "needs a private deployment (SPRIG_GWM_*)"]
async fn creates_go_on_after_the_writer_moves() {
    let (Some(addr), Some(user), Some(pass), Some(graph), Some(mv)) = (
        env("SPRIG_GWM_BOLT"),
        env("SPRIG_GWM_USER"),
        env("SPRIG_GWM_PASSWORD"),
        env("SPRIG_GWM_GRAPH"),
        env("SPRIG_GWM_MOVE"),
    ) else {
        eprintln!("graph_writer_move_live: SPRIG_GWM_* not set; skipped");
        return;
    };
    let tag = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    run(
        &addr,
        &user,
        &pass,
        &graph,
        &format!("CREATE (:Gwm {{tag: {tag}, k: 0}})"),
    )
    .await
    .expect("a CREATE before the move");
    println!("before the move: CREATE ok");
    shell(&mv);
    let started = Instant::now();
    // The new writer serves: a write that takes no new id goes through.
    let mut last = String::new();
    loop {
        match run(
            &addr,
            &user,
            &pass,
            &graph,
            &format!("MATCH (w:Gwm {{tag: {tag}}}) SET w.seen = 1"),
        )
        .await
        {
            Ok(()) => break,
            Err(e) => last = e,
        }
        assert!(
            started.elapsed() < Duration::from_secs(240),
            "no write went through within 240 s of the move: {last}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "writes go through again {:.1} s after the move",
        started.elapsed().as_secs_f64()
    );
    let at = Instant::now();
    let mut created = false;
    while at.elapsed() < Duration::from_secs(20) {
        match run(
            &addr,
            &user,
            &pass,
            &graph,
            &format!("CREATE (:Gwm {{tag: {tag}, k: 1}})"),
        )
        .await
        {
            Ok(()) => {
                created = true;
                break;
            }
            Err(e) => {
                last = e;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if let Some(r) = env("SPRIG_GWM_RESTORE") {
        shell(&r);
    }
    assert!(
        created,
        "CREATE still failing 20 s after writes resumed: {last}"
    );
    println!(
        "after the move: CREATE ok {:.1} s later",
        at.elapsed().as_secs_f64()
    );
    let _ = run(
        &addr,
        &user,
        &pass,
        &graph,
        &format!("MATCH (w:Gwm {{tag: {tag}}}) DELETE w"),
    )
    .await;
}
