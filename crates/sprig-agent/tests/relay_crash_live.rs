//! Sprig 2PC through the agents' relays, on a deployment: transfers between
//! two Galera clusters through a gateway, with `kill -9` of the gateway, the
//! controller, a relay agent or a MariaDB node at a chosen step of the
//! coordinator, then recovery and the checks: no transfer lost, none
//! applied twice, none half-applied, every in-doubt branch resolved.
//!
//! The gateway runs with `SPRIGDB_TEST_TXN_PAUSE=<step>:1:<dir>`: every
//! distributed transaction stops at `<step>` and writes a file in `<dir>`.
//! For each round the test takes the first such file, kills the round's
//! target, lets the transaction go on (unless the gateway was the target),
//! restarts the target, and waits until transfers succeed again. Other
//! pauses are let go at once.
//!
//! Each transfer `T` takes 1 from an account on one cluster and gives it
//! to an account on the other, and appends `,T` to both rows' `note`. At
//! the end:
//!
//! - every transfer the client saw commit is in both rows exactly once;
//! - every other transfer is in both rows or in neither, at most once each
//!   (a client told "unknown" may have committed);
//! - the balances still sum to the start;
//! - no `__sprig_redo` statement row and no `pending` decision is left.
//!
//! The target `client` kills the client instead: the test's own workers
//! stop, a child process (this test binary, `client_victim`) runs
//! transfers until one pauses at the step, and the test `kill -9`s it.
//! The gateway sees its client gone with the transaction stopped there.
//! The child's transfers must be in both rows or in neither.
//!
//! `client_kills` (`R3W_CLIENT_KILLS=<n>`) kills such a child `n` times at
//! random moments of a 32-session load, without pauses, then checks the
//! same: every transfer in both rows or in neither, the balances, and no
//! branch left in doubt.
//!
//! ```sh
//! export R3W_SCRIPT=target/agent-r3wire/deploy/r3w.sh      # start|kill9 <proc>
//! export R3W_GATEWAY=mysql://r3wire-t.app:sprig-dev-app@127.0.0.1:27306/bank
//! export R3W_BACKENDS=mysql://root:sprig-dev-root@127.0.0.1:27311,mysql://root:sprig-dev-root@127.0.0.1:27321
//! export R3W_SCHEMAS=t27306000000_d1_g2_s2,t27306000000_d1_g2_s4   # the shards, in that order
//! export R3W_PAUSE_DIR=target/agent-r3wire/pauses
//! export R3W_STEP=prepared                 # the step the gateway pauses at
//! export R3W_TARGETS=gateway,controller,agent-g2a,agent-g1a,node:r3wire-g2a
//! export R3W_REPEAT=2
//! cargo test -p sprig-agent --test relay_crash_live -- --nocapture --test-threads 1
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::many_single_char_names,
    clippy::type_complexity,
    clippy::unused_async,
    clippy::cast_possible_truncation,
    clippy::similar_names,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool};
use tokio::sync::Mutex;

/// What the client saw of a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    Committed,
    Failed,
    Unknown,
}

struct Env {
    script: String,
    gateway: String,
    backends: Vec<String>,
    schemas: Vec<String>,
    pause_dir: PathBuf,
    step: String,
    targets: Vec<String>,
    repeat: usize,
    sessions: usize,
}

fn env() -> Option<Env> {
    let v = |k: &str| std::env::var(k).ok();
    Some(Env {
        script: v("R3W_SCRIPT")?,
        gateway: v("R3W_GATEWAY")?,
        backends: v("R3W_BACKENDS")?.split(',').map(str::to_owned).collect(),
        schemas: v("R3W_SCHEMAS")?.split(',').map(str::to_owned).collect(),
        pause_dir: PathBuf::from(v("R3W_PAUSE_DIR")?),
        step: v("R3W_STEP").unwrap_or_else(|| "prepared".into()),
        targets: v("R3W_TARGETS")?.split(',').map(str::to_owned).collect(),
        repeat: v("R3W_REPEAT").and_then(|r| r.parse().ok()).unwrap_or(1),
        sessions: v("R3W_SESSIONS").and_then(|r| r.parse().ok()).unwrap_or(4),
    })
}

fn pool(url: &str, max: usize) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, max).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    )
}

fn sh(script: &str, args: &[&str]) {
    let out = std::process::Command::new(script)
        .args(args)
        .output()
        .expect("run the deployment script");
    println!(
        "  $ {script} {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stdout).trim()
    );
}

fn docker(args: &[&str]) {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .expect("docker");
    println!("  $ docker {}: {}", args.join(" "), out.status);
}

/// Error numbers after which the transfer certainly didn't commit.
fn surely_rolled_back(code: u16) -> bool {
    matches!(
        code,
        1205 | 1213
            | 1047
            | 1927
            | 39030
            | 39031
            | 39100
            | 39440
            | 39443
            | 39446
            | 39448
            | 39449
            | 39501
    )
}

/// A connection to a backend, retried for a minute (a killed node may be
/// rejoining its cluster).
async fn backend(url: &str) -> mysql_async::Conn {
    let started = Instant::now();
    loop {
        match mysql_async::Conn::new(Opts::from_url(url).unwrap()).await {
            Ok(mut c) => {
                if c.query_drop("SELECT 1").await.is_ok() {
                    return c;
                }
            }
            Err(e) if started.elapsed() > Duration::from_secs(60) => panic!("{url}: {e}"),
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn ids_of(url: &str, schema: &str) -> Vec<i64> {
    let mut c = mysql_async::Conn::new(Opts::from_url(url).unwrap())
        .await
        .unwrap();
    let r: Vec<i64> = c
        .query(format!(
            "SELECT id FROM `{schema}`.acct ORDER BY id LIMIT 200"
        ))
        .await
        .unwrap();
    c.disconnect().await.unwrap();
    r
}

/// One transfer; `Seen` by the client.
async fn transfer(p: &Pool, t: u64, from: i64, to: i64) -> Seen {
    let Ok(mut c) = p.get_conn().await else {
        return Seen::Failed;
    };
    let tag = format!(",{t}");
    let body = async {
        c.query_drop("BEGIN").await?;
        c.exec_drop(
            "UPDATE acct SET bal = bal - 1, note = CONCAT(note, ?) WHERE id = ?",
            (&tag, from),
        )
        .await?;
        c.exec_drop(
            "UPDATE acct SET bal = bal + 1, note = CONCAT(note, ?) WHERE id = ?",
            (&tag, to),
        )
        .await?;
        Ok::<(), mysql_async::Error>(())
    }
    .await;
    if body.is_err() {
        let _ = c.query_drop("ROLLBACK").await;
        return Seen::Failed;
    }
    match c.query_drop("COMMIT").await {
        Ok(()) => Seen::Committed,
        Err(mysql_async::Error::Server(e)) if surely_rolled_back(e.code) => Seen::Failed,
        Err(_) => Seen::Unknown,
    }
}

fn pauses(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .map(|d| {
            d.filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("pause-"))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn release_all(dir: &Path) {
    for p in pauses(dir) {
        let _ = std::fs::remove_file(p);
    }
}

async fn kill(e: &Env, target: &str) {
    if let Some(c) = target.strip_prefix("node:") {
        docker(&["kill", "-s", "KILL", c]);
    } else {
        sh(&e.script, &["kill9", target]);
    }
}

async fn restart(e: &Env, target: &str) {
    if let Some(c) = target.strip_prefix("node:") {
        docker(&["start", c]);
    } else {
        sh(&e.script, &["start", target]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_survive_kill_9_at_each_step() {
    let Some(e) = env() else {
        eprintln!("relay_crash_live: R3W_* not set; skipped");
        return;
    };
    let e = Arc::new(e);
    std::fs::create_dir_all(&e.pause_dir).unwrap();
    release_all(&e.pause_dir);
    let a_ids = ids_of(&e.backends[0], &e.schemas[0]).await;
    let b_ids = ids_of(&e.backends[1], &e.schemas[1]).await;
    assert!(a_ids.len() >= 20 && b_ids.len() >= 20);
    let gw = pool(&e.gateway, e.sessions + 2);
    let sum0: i64 = {
        let mut c = gw.get_conn().await.unwrap();
        c.query_first("SELECT SUM(bal) FROM acct")
            .await
            .unwrap()
            .unwrap()
    };
    // Transfer ids continue from the time, so runs never reuse one.
    let next = Arc::new(AtomicU64::new(
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            % 100_000)
            * 10_000,
    ));
    let seen: Arc<Mutex<BTreeMap<u64, (Seen, i64, i64)>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let ok_since = Arc::new(Mutex::new(Instant::now()));
    let stop = Arc::new(AtomicBool::new(false));
    // While a child client runs (target `client`), the workers wait.
    let hold = Arc::new(AtomicBool::new(false));
    let busy = Arc::new(AtomicU64::new(0));
    let mut victims = 0usize;
    let mut workers = Vec::new();
    for w in 0..e.sessions {
        let (gw, next, seen, stop, ok_since, hold, busy) = (
            gw.clone(),
            Arc::clone(&next),
            Arc::clone(&seen),
            Arc::clone(&stop),
            Arc::clone(&ok_since),
            Arc::clone(&hold),
            Arc::clone(&busy),
        );
        let (a, b) = (a_ids.clone(), b_ids.clone());
        workers.push(tokio::spawn(async move {
            let mut x: u64 = 0x9e37_79b9_7f4a_7c15 ^ (w as u64 + 1);
            while !stop.load(Ordering::SeqCst) {
                if hold.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                busy.fetch_add(1, Ordering::SeqCst);
                if hold.load(Ordering::SeqCst) {
                    busy.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let t = next.fetch_add(1, Ordering::SeqCst);
                let (from, to) = (a[(x % 20) as usize], b[((x >> 8) % 20) as usize]);
                let (from, to) = if x & 1 == 0 { (from, to) } else { (to, from) };
                let s = transfer(&gw, t, from, to).await;
                if s == Seen::Committed {
                    *ok_since.lock().await = Instant::now();
                } else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                seen.lock().await.insert(t, (s, from, to));
                busy.fetch_sub(1, Ordering::SeqCst);
            }
        }));
    }
    let mut rounds = Vec::new();
    for rep in 0..e.repeat {
        for target in &e.targets {
            if target == "client" {
                let back = kill_a_client(&e, &hold, &busy, &ok_since, victims).await;
                victims += 1;
                println!("round {rep}/client: transfers commit again after {back:?}");
                assert!(
                    back.is_some(),
                    "no transfer committed within 240 s after the client kill"
                );
                rounds.push((rep, target.clone(), back));
                continue;
            }
            // Wait for the next paused transaction (others pass at once).
            release_all(&e.pause_dir);
            let started = Instant::now();
            let paused = loop {
                if let Some(p) = pauses(&e.pause_dir).into_iter().next() {
                    break Some(p);
                }
                if started.elapsed() > Duration::from_secs(120) {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let Some(paused) = paused else {
                panic!("no transaction paused at {} within 120 s", e.step);
            };
            println!(
                "round {rep}/{target}: {} paused at {}",
                paused.display(),
                e.step
            );
            kill(&e, target).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            // The paused transaction goes on (if its gateway lives), meeting
            // the dead target.
            let _ = std::fs::remove_file(&paused);
            let releaser = {
                let dir = e.pause_dir.clone();
                let stop = Arc::new(AtomicBool::new(false));
                let s2 = Arc::clone(&stop);
                (
                    tokio::spawn(async move {
                        while !s2.load(Ordering::SeqCst) {
                            release_all(&dir);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }),
                    stop,
                )
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            restart(&e, target).await;
            // Back when a transfer commits again.
            let killed_at = Instant::now();
            *ok_since.lock().await = killed_at.checked_sub(Duration::from_secs(1)).unwrap();
            let back = loop {
                if *ok_since.lock().await > killed_at {
                    break Some(killed_at.elapsed());
                }
                if killed_at.elapsed() > Duration::from_secs(240) {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            println!("round {rep}/{target}: transfers commit again after {back:?}");
            assert!(
                back.is_some(),
                "no transfer committed within 240 s after {target}"
            );
            // Let it run a little before the next kill.
            tokio::time::sleep(Duration::from_secs(3)).await;
            releaser.1.store(true, Ordering::SeqCst);
            let _ = releaser.0.await;
            rounds.push((rep, target.clone(), back));
        }
    }
    stop.store(true, Ordering::SeqCst);
    // The workers stop after their transfer; let paused ones go.
    let rel = {
        let dir = e.pause_dir.clone();
        tokio::spawn(async move {
            for _ in 0..3000 {
                release_all(&dir);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    for w in workers {
        let _ = w.await;
    }
    rel.abort();

    // In-doubt transactions resolved: no redo statement row, no pending
    // decision, within the resolver's minimum age and then some.
    let started = Instant::now();
    let mut left = (u64::MAX, u64::MAX);
    while started.elapsed() < Duration::from_secs(180) {
        left = (0, 0);
        for (url, schema) in e.backends.iter().zip(&e.schemas) {
            let mut c = backend(url).await;
            let redo: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM `{schema}`.__sprig_redo WHERE seq < 9223372036854775808"
                ))
                .await
                .unwrap_or(Some(0));
            let pending: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM `{schema}`.__sprig_dtx WHERE outcome = 'pending'"
                ))
                .await
                .unwrap_or(Some(0));
            left.0 += redo.unwrap_or(0);
            left.1 += pending.unwrap_or(0);
            c.disconnect().await.unwrap();
        }
        if left == (0, 0) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "in doubt after the run: {} redo statement rows, {} pending decisions ({:?} to resolve)",
        left.0,
        left.1,
        started.elapsed()
    );

    // The rows, read from the backends (every committed write is there).
    let mut notes: BTreeMap<i64, String> = BTreeMap::new();
    let mut sum = 0i64;
    for (url, schema) in e.backends.iter().zip(&e.schemas) {
        let mut c = backend(url).await;
        let rows: Vec<(i64, i64, String)> = c
            .query(format!("SELECT id, bal, note FROM `{schema}`.acct"))
            .await
            .unwrap();
        for (id, bal, note) in rows {
            sum += bal;
            notes.insert(id, note);
        }
        c.disconnect().await.unwrap();
    }
    let count = |id: i64, t: u64| -> usize {
        notes
            .get(&id)
            .map_or(0, |n| n.split(',').filter(|x| *x == t.to_string()).count())
    };
    let seen = seen.lock().await;
    let (mut committed, mut failed, mut unknown) = (0, 0, 0);
    let (mut lost, mut doubled, mut partial, mut failed_applied, mut unknown_applied) =
        (Vec::new(), Vec::new(), Vec::new(), 0, 0);
    for (&t, &(s, from, to)) in seen.iter() {
        let (a, b) = (count(from, t), count(to, t));
        if a > 1 || b > 1 {
            doubled.push(t);
        }
        if a != b {
            partial.push(t);
        }
        match s {
            Seen::Committed => {
                committed += 1;
                if a == 0 || b == 0 {
                    lost.push(t);
                }
            }
            Seen::Failed => {
                failed += 1;
                if a > 0 {
                    failed_applied += 1;
                }
            }
            Seen::Unknown => {
                unknown += 1;
                if a > 0 {
                    unknown_applied += 1;
                }
            }
        }
    }
    // The killed clients' transfers: in both rows or in neither.
    let v = victim_tags(&notes);
    if victims > 0 {
        println!(
            "killed clients: {victims}, their transfers applied {}, half applied {}, doubled {}",
            v.applied,
            v.partial.len(),
            v.doubled.len()
        );
    }
    // Tags in rows that no transfer of this run made.
    let mine: BTreeSet<String> = seen.keys().map(u64::to_string).collect();
    println!(
        "transfers: {committed} committed, {failed} failed, {unknown} unknown ({unknown_applied} of \
         them applied), {failed_applied} reported failed but applied; lost {}, doubled {}, partial {}; \
         sum {sum} (start {sum0}); rounds {}",
        lost.len(),
        doubled.len(),
        partial.len(),
        rounds.len()
    );
    let _ = mine;
    assert!(lost.is_empty(), "lost: {lost:?}");
    assert!(doubled.is_empty(), "applied twice: {doubled:?}");
    assert!(partial.is_empty(), "half applied: {partial:?}");
    assert!(
        v.partial.is_empty(),
        "a killed client's transfer half applied: {:?}",
        v.partial
    );
    assert!(
        v.doubled.is_empty(),
        "a killed client's transfer applied twice: {:?}",
        v.doubled
    );
    assert_eq!(failed_applied, 0, "a transfer reported failed was applied");
    assert_eq!(sum, sum0, "balances");
    assert_eq!(left, (0, 0), "in-doubt branches left");
}

/// Transfer ids of child clients start here; the workers' stay far below.
const VICTIM_BASE: u64 = 10_000_000_000_000;

/// The child clients' transfers as the rows show them.
struct VictimTags {
    applied: usize,
    partial: Vec<u64>,
    doubled: Vec<u64>,
}

/// Counts each child client's transfer id in the notes: a transfer is in
/// one account on each cluster, so each id must appear twice or not at all.
fn victim_tags(notes: &BTreeMap<i64, String>) -> VictimTags {
    let mut count: BTreeMap<u64, usize> = BTreeMap::new();
    for n in notes.values() {
        for t in n.split(',').filter_map(|x| x.parse::<u64>().ok()) {
            if t >= VICTIM_BASE {
                *count.entry(t).or_default() += 1;
            }
        }
    }
    VictimTags {
        applied: count.values().filter(|&&c| c == 2).count(),
        partial: count
            .iter()
            .filter(|(_, c)| **c == 1)
            .map(|(t, _)| *t)
            .collect(),
        doubled: count
            .iter()
            .filter(|(_, c)| **c > 2)
            .map(|(t, _)| *t)
            .collect(),
    }
}

/// Starts a child client (this binary's `client_victim`) running
/// `sessions` transfer loops from ids `base`; `spread` gives each session
/// its own rows (the cost bench's pattern) instead of random ones.
fn spawn_victim(base: u64, sessions: usize, spread: bool) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "client_victim",
            "--exact",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("R3W_VICTIM", base.to_string())
        .env("R3W_VICTIM_SESSIONS", sessions.to_string())
        .env("R3W_VICTIM_SPREAD", if spread { "1" } else { "0" })
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("start the child client")
}

/// One `client` round: the workers wait, a child client runs until one of
/// its transactions pauses at the gateway's step, and it is killed there.
/// Returns how long until the workers' transfers committed again.
async fn kill_a_client(
    e: &Env,
    hold: &AtomicBool,
    busy: &AtomicU64,
    ok_since: &Mutex<Instant>,
    n: usize,
) -> Option<Duration> {
    hold.store(true, Ordering::SeqCst);
    let started = Instant::now();
    while busy.load(Ordering::SeqCst) > 0 && started.elapsed() < Duration::from_secs(60) {
        release_all(&e.pause_dir);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    release_all(&e.pause_dir);
    let mut child = spawn_victim(VICTIM_BASE + n as u64 * 100_000_000, 4, false);
    let started = Instant::now();
    let paused = loop {
        if let Some(p) = pauses(&e.pause_dir).into_iter().next() {
            break Some(p);
        }
        if started.elapsed() > Duration::from_secs(120) {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let _ = child.kill();
    let _ = child.wait();
    let Some(paused) = paused else {
        panic!(
            "no transaction of the child client paused at {} within 120 s",
            e.step
        );
    };
    println!(
        "round client {n}: {} paused at {}; client killed",
        paused.display(),
        e.step
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The gateway goes on with the stopped transactions.
    for _ in 0..50 {
        release_all(&e.pause_dir);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    hold.store(false, Ordering::SeqCst);
    let at = Instant::now();
    *ok_since.lock().await = at.checked_sub(Duration::from_secs(1)).unwrap();
    let releaser = {
        let dir = e.pause_dir.clone();
        tokio::spawn(async move {
            for _ in 0..150 {
                release_all(&dir);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    let back = loop {
        if *ok_since.lock().await > at {
            break Some(at.elapsed());
        }
        if at.elapsed() > Duration::from_secs(240) {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let _ = releaser.await;
    back
}

/// The child client: transfers from `R3W_VICTIM` on until it is killed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_victim() {
    let (Ok(base), Some(e)) = (std::env::var("R3W_VICTIM"), env()) else {
        eprintln!(
            "relay_crash_live: R3W_VICTIM not set; skipped (a child client of the kill tests)"
        );
        return;
    };
    let base: u64 = base.parse().unwrap();
    let sessions: usize = std::env::var("R3W_VICTIM_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let spread = std::env::var("R3W_VICTIM_SPREAD").is_ok_and(|s| s == "1");
    let a = ids_of(&e.backends[0], &e.schemas[0]).await;
    let b = ids_of(&e.backends[1], &e.schemas[1]).await;
    let gw = pool(&e.gateway, sessions + 2);
    let mut hs = Vec::new();
    for w in 0..sessions {
        let (gw, a, b) = (gw.clone(), a.clone(), b.clone());
        hs.push(tokio::spawn(async move {
            let mut x: u64 = 0x9e37_79b9_7f4a_7c15 ^ (w as u64 + 7);
            for i in 0u64.. {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let (from, to) = if spread {
                    let k = (w * 4 + (i % 4) as usize) % a.len().min(b.len());
                    (a[k], b[k])
                } else {
                    let (f, t) = (a[(x % 20) as usize], b[((x >> 8) % 20) as usize]);
                    if x & 1 == 0 { (f, t) } else { (t, f) }
                };
                let t = base + w as u64 * 1_000_000 + i;
                if transfer(&gw, t, from, to).await != Seen::Committed {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }));
    }
    for h in hs {
        let _ = h.await;
    }
}

/// `R3W_CLIENT_KILLS=<n>`: a child client of `R3W_KILL_SESSIONS` (32)
/// sessions transfers between the clusters, each session on its own rows,
/// and is killed with `kill -9` after a random 1–4 s, `n` times. Then,
/// once no branch is in doubt, every child transfer must be in both rows
/// or in neither, and the balances must sum to the start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_kills() {
    let (Ok(n), Some(e)) = (std::env::var("R3W_CLIENT_KILLS"), env()) else {
        eprintln!("relay_crash_live: R3W_CLIENT_KILLS not set; skipped");
        return;
    };
    let kills: usize = n.parse().unwrap();
    let sessions: usize = std::env::var("R3W_KILL_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let in_doubt = || async {
        let mut left = (0u64, 0u64);
        for (url, schema) in e.backends.iter().zip(&e.schemas) {
            let mut c = backend(url).await;
            let redo: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM `{schema}`.__sprig_redo WHERE seq < 9223372036854775808"
                ))
                .await
                .unwrap_or(Some(0));
            let pending: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM `{schema}`.__sprig_dtx WHERE outcome = 'pending'"
                ))
                .await
                .unwrap_or(Some(0));
            left.0 += redo.unwrap_or(0);
            left.1 += pending.unwrap_or(0);
            c.disconnect().await.unwrap();
        }
        left
    };
    let settle = || async {
        let started = Instant::now();
        loop {
            let left = in_doubt().await;
            if left == (0, 0) || started.elapsed() > Duration::from_secs(180) {
                return (left, started.elapsed());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    };
    let (left, _) = settle().await;
    assert_eq!(left, (0, 0), "branches in doubt before the run");
    // A fresh start: the notes of earlier runs go.
    let mut sum0 = 0i64;
    for (url, schema) in e.backends.iter().zip(&e.schemas) {
        let mut c = backend(url).await;
        c.query_drop(format!(
            "UPDATE `{schema}`.acct SET note = '' WHERE note <> ''"
        ))
        .await
        .unwrap();
        let s: Option<i64> = c
            .query_first(format!("SELECT SUM(bal) FROM `{schema}`.acct"))
            .await
            .unwrap();
        sum0 += s.unwrap_or(0);
        c.disconnect().await.unwrap();
    }
    let run = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        % 1000)
        * 1_000_000_000;
    let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ run;
    for k in 0..kills {
        while time_wait(&[]).0 > 1500 {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let mut child = spawn_victim(VICTIM_BASE + run + k as u64 * 100_000_000, sessions, true);
        let ms = 1000 + x % 3000;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        let _ = child.kill();
        let _ = child.wait();
        println!("kill {k}: child client killed after {ms} ms");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (left, took) = settle().await;
    let mut notes: BTreeMap<i64, String> = BTreeMap::new();
    let mut sum = 0i64;
    for (url, schema) in e.backends.iter().zip(&e.schemas) {
        let mut c = backend(url).await;
        let rows: Vec<(i64, i64, String)> = c
            .query(format!("SELECT id, bal, note FROM `{schema}`.acct"))
            .await
            .unwrap();
        for (id, bal, note) in rows {
            sum += bal;
            notes.insert(id, note);
        }
        c.disconnect().await.unwrap();
    }
    let v = victim_tags(&notes);
    println!(
        "CLIENT-KILLS kills={kills} sessions={sessions} applied={} half_applied={} doubled={} \
         in_doubt_redo={} in_doubt_pending={} recovered_in={took:?} sum={sum} start={sum0} lost={}",
        v.applied,
        v.partial.len(),
        v.doubled.len(),
        left.0,
        left.1,
        sum0 - sum
    );
    assert!(v.partial.is_empty(), "half applied: {:?}", v.partial);
    assert!(v.doubled.is_empty(), "applied twice: {:?}", v.doubled);
    assert_eq!(left, (0, 0), "in doubt after recovery");
    assert_eq!(sum, sum0, "balances");
}

/// What a cost run measured.
struct Bench {
    done: u64,
    failed: u64,
    /// Failures by error, for the report.
    errors: BTreeMap<String, u64>,
    secs: f64,
    lat_ms: Vec<f64>,
}

impl Bench {
    fn pct(&self, p: f64) -> f64 {
        if self.lat_ms.is_empty() {
            return 0.0;
        }
        let i = ((self.lat_ms.len() as f64 - 1.0) * p).round() as usize;
        self.lat_ms[i]
    }
}

/// `kind`: `cross` (one row on each cluster), `local` (two rows of the
/// first cluster) or `autocommit` (one single-row write, no transaction).
async fn bench(gw: &Pool, a: &[i64], b: &[i64], kind: &str, sessions: usize, secs: u64) -> Bench {
    let until = Instant::now() + Duration::from_secs(secs);
    let mut hs = Vec::new();
    for w in 0..sessions {
        let (gw, a, b, kind) = (gw.clone(), a.to_vec(), b.to_vec(), kind.to_owned());
        hs.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut failed = 0u64;
            let mut errors: BTreeMap<String, u64> = BTreeMap::new();
            let mut c = gw.get_conn().await.unwrap();
            let mut i = w;
            while Instant::now() < until {
                // Each session keeps to its own rows: the cost of the
                // protocol, not of lock waits.
                let x = a[(w * 4 + i % 4) % a.len()];
                let y = if kind == "local" {
                    a[(w * 4 + i % 4 + 2) % a.len()]
                } else {
                    b[(w * 4 + i % 4) % b.len()]
                };
                i += 1;
                let t = Instant::now();
                let r = if kind == "autocommit" {
                    c.exec_drop("UPDATE acct SET bal = bal + 0 WHERE id = ?", (x,))
                        .await
                } else {
                    async {
                        c.query_drop("BEGIN").await?;
                        c.exec_drop("UPDATE acct SET bal = bal - 1 WHERE id = ?", (x,))
                            .await?;
                        c.exec_drop("UPDATE acct SET bal = bal + 1 WHERE id = ?", (y,))
                            .await?;
                        c.query_drop("COMMIT").await
                    }
                    .await
                };
                if r.is_ok() {
                    lat.push(t.elapsed().as_secs_f64() * 1000.0);
                } else if let Err(e) = r {
                    failed += 1;
                    let what = match &e {
                        mysql_async::Error::Server(s) => format!("{} {}", s.code, s.message),
                        e => e.to_string(),
                    };
                    *errors.entry(what.chars().take(120).collect()).or_default() += 1;
                    let _ = c.query_drop("ROLLBACK").await;
                }
            }
            (lat, failed, errors)
        }));
    }
    let mut out = Bench {
        done: 0,
        failed: 0,
        errors: BTreeMap::new(),
        secs: secs as f64,
        lat_ms: Vec::new(),
    };
    for h in hs {
        let (l, f, errs) = h.await.unwrap();
        out.done += l.len() as u64;
        out.failed += f;
        for (k, n) in errs {
            *out.errors.entry(k).or_default() += n;
        }
        out.lat_ms.extend(l);
    }
    out.lat_ms.sort_by(f64::total_cmp);
    out
}

/// `TIME_WAIT` sockets now: all of them, and those with an end on one of
/// `ports` (the deployment's MariaDB and relay ports).
fn time_wait(ports: &[u16]) -> (usize, usize) {
    let Ok(out) = std::process::Command::new("netstat")
        .args(["-an", "-p", "tcp"])
        .output()
    else {
        return (0, 0);
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut all = 0;
    let mut ours = 0;
    for l in text.lines().filter(|l| l.contains("TIME_WAIT")) {
        all += 1;
        let mine = l.split_whitespace().skip(3).take(2).any(|a| {
            a.rsplit_once('.')
                .and_then(|(_, p)| p.parse::<u16>().ok())
                .is_some_and(|p| ports.contains(&p))
        });
        if mine {
            ours += 1;
        }
    }
    (all, ours)
}

/// The sum of `Connections` (connection attempts since start) over the
/// nodes, on kept connections.
async fn connections(nodes: &mut [mysql_async::Conn]) -> u64 {
    let mut n = 0;
    for c in nodes {
        let r: Option<(String, u64)> = c
            .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
            .await
            .unwrap();
        n += r.map_or(0, |(_, v)| v);
    }
    n
}

fn list<T: std::str::FromStr>(var: &str, default: &str) -> Vec<T> {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

/// Cost of the configured path: `R3W_BENCH=<label>` with the tenant's
/// `txn.sprig_2pc_path` (and policy) set beforehand; prints one line per
/// kind and session count. `R3W_BENCH_KINDS` and `R3W_BENCH_SESSIONS`
/// choose the runs (default: every kind at 1, 8 and 32 sessions);
/// `R3W_COUNT_NODES` (every MariaDB node's URL) adds the backend
/// connections opened per transaction, and `R3W_TW_PORTS` the peak of
/// `TIME_WAIT` sockets on those ports, sampled every half second. A run
/// stops early when the laptop's `TIME_WAIT` sockets pass 8,000.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cost_of_the_path() {
    let (Ok(label), Some(e)) = (std::env::var("R3W_BENCH"), env()) else {
        eprintln!("relay_crash_live: R3W_BENCH not set; skipped");
        return;
    };
    let secs: u64 = std::env::var("R3W_BENCH_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let kinds: Vec<String> = list("R3W_BENCH_KINDS", "cross,local,autocommit");
    let counts: Vec<usize> = list("R3W_BENCH_SESSIONS", "1,8,32");
    let ports: Vec<u16> = list("R3W_TW_PORTS", "");
    let mut nodes = Vec::new();
    for u in list::<String>("R3W_COUNT_NODES", "") {
        nodes.push(backend(&u).await);
    }
    let a = ids_of(&e.backends[0], &e.schemas[0]).await;
    let b = ids_of(&e.backends[1], &e.schemas[1]).await;
    let gw = pool(&e.gateway, 34);
    for kind in &kinds {
        for &sessions in &counts {
            // Warm the pools.
            let _ = bench(&gw, &a, &b, kind, sessions, 2).await;
            let stop = Arc::new(AtomicBool::new(false));
            let peak = Arc::new(Mutex::new((0usize, 0usize)));
            let sampler = {
                let (stop, peak, ports) = (Arc::clone(&stop), Arc::clone(&peak), ports.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::SeqCst) {
                        let p = ports.clone();
                        let (all, ours) = tokio::task::spawn_blocking(move || time_wait(&p))
                            .await
                            .unwrap();
                        {
                            let mut g = peak.lock().await;
                            g.0 = g.0.max(all);
                            g.1 = g.1.max(ours);
                        }
                        assert!(all <= 8000, "TIME_WAIT at {all}: stopping the bench");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                })
            };
            let sum = || async {
                let mut c = gw.get_conn().await.unwrap();
                let s: Option<i64> = c.query_first("SELECT SUM(bal) FROM acct").await.unwrap();
                s.unwrap_or(0)
            };
            let sum0 = sum().await;
            let c0 = connections(&mut nodes).await;
            let r = bench(&gw, &a, &b, kind, sessions, secs).await;
            let opened = connections(&mut nodes).await.saturating_sub(c0);
            let sum1 = sum().await;
            stop.store(true, Ordering::SeqCst);
            let sampled = sampler.await;
            let (tw_all, tw_ours) = *peak.lock().await;
            println!(
                "BENCH {label} {kind} sessions={sessions} tps={:.1} p50={:.2}ms p95={:.2}ms p99={:.2}ms ok={} failed={} conns={opened} conns_per_txn={:.3} tw_peak={tw_ours} tw_all_peak={tw_all}",
                r.done as f64 / r.secs,
                r.pct(0.5),
                r.pct(0.95),
                r.pct(0.99),
                r.done,
                r.failed,
                opened as f64 / r.done.max(1) as f64,
            );
            if sum1 != sum0 {
                println!("BALANCE {label} {kind} sessions={sessions}: sum {sum0} -> {sum1}");
            }
            for (e, n) in &r.errors {
                println!("ERRORS {label} {kind} sessions={sessions}: {n} x {e}");
            }
            assert!(sampled.is_ok(), "TIME_WAIT passed 8,000: bench stopped");
        }
    }
}
