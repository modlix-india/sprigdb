//! `kill -9` with XA branches and open transactions on the real engine.
//!
//! A child process runs four kinds of transaction on eight threads and
//! prints each step once it is durable; the parent kills it with SIGKILL
//! at a random moment, opens the engine again in-process and checks:
//!
//! | The child's transaction | After the kill and the reopen |
//! |---|---|
//! | prepared, the prepare acknowledged, nothing more | listed by `XA RECOVER`, its rows invisible, its rows' locks held; then the parent commits or rolls it back and both rows follow together |
//! | prepared, then committed, the commit acknowledged | both rows there, not listed |
//! | prepared, then rolled back, the rollback acknowledged | no row, not listed |
//! | prepared and being finished when the kill came | prepared, or finished as asked: never half |
//! | open, never prepared | no row, no lock, not listed |
//!
//! Rounds: `SPRIG_XA_KILL9_ROUNDS` (default 4). The child is this same
//! test binary, run with `SPRIG_XA_KILL9_CHILD=<dir>`.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_engine::keys::{KeyPrefix, kind, memcmp};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{CommitPath, LockRequest, ReadMode, Txn, TxnOptions, Wait};
use sprig_engine::{Durability, Engine, EngineConfig};

const THREADS: u32 = 8;

fn cfg(dir: &Path) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.budget.engine_mb = 128;
    c.log.sync = SyncMode::Full;
    c.checkpoint_interval = Duration::from_millis(300);
    c.checkpoint_entries = 2_000;
    c
}

fn opts() -> TxnOptions {
    TxnOptions {
        durability: Some(Durability::Local),
        ..TxnOptions::default()
    }
}

/// Two rows per transaction: `(round, thread, i, side)`.
fn key(round: u32, thread: u32, i: u32, side: u8) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant: 9,
        shard: 0,
        object: u32::from(side),
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    for v in [round, thread, i] {
        memcmp::put_uint(&mut k, u64::from(v), 4);
    }
    k
}

fn xid(round: u32, thread: u32, i: u32) -> Vec<u8> {
    format!("r{round}-t{thread}-i{i}").into_bytes()
}

fn path(e: &Engine) -> Arc<dyn CommitPath> {
    Arc::new(e.clone())
}

/// What transaction `i` does: 0 prepare only, 1 prepare and commit,
/// 2 prepare and roll back, 3 stay open.
fn shape(i: u32) -> u32 {
    i % 4
}

#[test]
fn xa_kill9_child() {
    let Ok(dir) = std::env::var("SPRIG_XA_KILL9_CHILD") else {
        return;
    };
    let round: u32 = std::env::var("SPRIG_XA_KILL9_ROUND")
        .unwrap()
        .parse()
        .unwrap();
    let e = Engine::open(cfg(Path::new(&dir))).unwrap();
    let out = Arc::new(std::sync::Mutex::new(std::io::stdout()));
    let hs: Vec<_> = (0..THREADS)
        .map(|t| {
            let (e, out) = (e.clone(), Arc::clone(&out));
            std::thread::spawn(move || {
                let say = |what: &str, i: u32| {
                    let mut o = out.lock().unwrap();
                    writeln!(o, "{what} {t} {i}").unwrap();
                    o.flush().unwrap();
                };
                // Transactions left open on purpose, alive until the kill.
                let mut open = Vec::new();
                let p = path(&e);
                for i in 0.. {
                    let mut tx = e.begin(opts()).unwrap();
                    tx.put(key(round, t, i, 1), format!("a{i}").into_bytes())
                        .unwrap();
                    tx.put(key(round, t, i, 2), vec![b'b'; 200]).unwrap();
                    if shape(i) == 3 {
                        say("OPEN", i);
                        open.push(tx);
                        // A few are enough; the rest of the time goes to
                        // the prepared kinds.
                        if open.len() > 64 {
                            open.remove(0).rollback();
                        }
                        continue;
                    }
                    let x = xid(round, t, i);
                    tx.xa_prepare(&x).unwrap().wait().unwrap();
                    say("PREP", i);
                    match shape(i) {
                        1 => {
                            Txn::xa_commit(&p, &x).unwrap().wait().unwrap();
                            say("COMMIT", i);
                        }
                        2 => {
                            Txn::xa_rollback(&p, &x).unwrap().wait().unwrap();
                            say("ROLLBACK", i);
                        }
                        _ => {}
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
}

fn get(e: &Engine, k: &[u8]) -> bool {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(k, ReadMode::Snapshot)
        .unwrap()
        .expect_ready()
        .unwrap()
        .is_some()
}

/// Whether another transaction can lock `k` right now.
fn lockable(e: &Engine, k: &[u8]) -> bool {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    let key = bytes::Bytes::copy_from_slice(k);
    t.lock_blocking(&key, LockRequest::write().with_wait(Wait::NoWait))
        .is_ok()
}

#[test]
fn xa_kill9_loop() {
    if std::env::var("SPRIG_XA_KILL9_CHILD").is_ok() {
        return;
    }
    let rounds: u32 = std::env::var("SPRIG_XA_KILL9_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("sprig-engine-xa-kill9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().unwrap();
    let mut seed = u64::from(std::process::id()) | 1;
    let (mut n_prepared, mut n_committed, mut n_rolled, mut n_open, mut n_flight) =
        (0usize, 0usize, 0usize, 0usize, 0usize);
    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args([
                "xa_kill9_child",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("SPRIG_XA_KILL9_CHILD", &dir)
            .env("SPRIG_XA_KILL9_ROUND", round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            // (thread, i) -> the last step acknowledged.
            let mut last: HashMap<(u32, u32), String> = HashMap::new();
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let mut p = line.split_whitespace();
                let (Some(what), Some(t), Some(i)) = (p.next(), p.next(), p.next()) else {
                    continue;
                };
                if !matches!(what, "OPEN" | "PREP" | "COMMIT" | "ROLLBACK") {
                    continue;
                }
                let (Ok(t), Ok(i)) = (t.parse(), i.parse()) else {
                    continue;
                };
                last.insert((t, i), what.to_owned());
            }
            last
        });
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let wait = Duration::from_millis(500 + seed % 1200);
        let start = Instant::now();
        while start.elapsed() < wait {
            std::thread::sleep(Duration::from_millis(20));
        }
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();
        let last = reader.join().unwrap();
        assert!(
            last.values().any(|w| w == "PREP"),
            "round {round}: the child acknowledged no prepare"
        );

        let e = Engine::open(cfg(&dir)).unwrap();
        let p = path(&e);
        let listed: HashSet<Vec<u8>> = Txn::xa_recover(&p).into_iter().collect();
        let both = |t: u32, i: u32| {
            let (a, b) = (get(&e, &key(round, t, i, 1)), get(&e, &key(round, t, i, 2)));
            assert_eq!(a, b, "round {round}: half a transaction ({t}, {i})");
            a
        };
        // Every listed branch is one this round's child could have
        // prepared (earlier rounds' were all finished below).
        let mine: HashSet<Vec<u8>> = (0..THREADS)
            .flat_map(|t| {
                let max = last
                    .keys()
                    .filter(|k| k.0 == t)
                    .map(|k| k.1)
                    .max()
                    .unwrap_or(0);
                (0..=max + 2).map(move |i| xid(round, t, i))
            })
            .collect();
        for x in &listed {
            assert!(
                mine.contains(x),
                "round {round}: an unknown branch is listed: {:?}",
                String::from_utf8_lossy(x)
            );
        }
        let mut to_finish = Vec::new();
        for (&(t, i), what) in &last {
            let x = xid(round, t, i);
            let is_listed = listed.contains(&x);
            match (what.as_str(), shape(i)) {
                ("OPEN", _) => {
                    n_open += 1;
                    assert!(
                        !both(t, i),
                        "round {round}: an unprepared transaction's rows survived ({t}, {i})"
                    );
                    assert!(!is_listed);
                    assert!(
                        lockable(&e, &key(round, t, i, 1)) && lockable(&e, &key(round, t, i, 2)),
                        "round {round}: an unprepared transaction's lock survived ({t}, {i})"
                    );
                }
                ("PREP", 0) => {
                    n_prepared += 1;
                    assert!(
                        is_listed,
                        "round {round}: an acknowledged prepare is lost ({t}, {i})"
                    );
                    assert!(!both(t, i), "round {round}: a prepared row is visible");
                    assert!(
                        !lockable(&e, &key(round, t, i, 1)) && !lockable(&e, &key(round, t, i, 2)),
                        "round {round}: a prepared branch lost its locks ({t}, {i})"
                    );
                    to_finish.push((t, i));
                }
                ("PREP", s) => {
                    // The kill came while it was being committed (1) or
                    // rolled back (2): still prepared, or finished as asked.
                    n_flight += 1;
                    if is_listed {
                        assert!(!both(t, i));
                        assert!(!lockable(&e, &key(round, t, i, 1)));
                        to_finish.push((t, i));
                    } else {
                        assert_eq!(
                            both(t, i),
                            s == 1,
                            "round {round}: ({t}, {i}) ended the wrong way"
                        );
                        assert!(lockable(&e, &key(round, t, i, 1)));
                    }
                }
                ("COMMIT", _) => {
                    n_committed += 1;
                    assert!(
                        both(t, i),
                        "round {round}: an acknowledged XA COMMIT is lost ({t}, {i})"
                    );
                    assert!(!is_listed);
                    assert!(lockable(&e, &key(round, t, i, 1)));
                }
                ("ROLLBACK", _) => {
                    n_rolled += 1;
                    assert!(
                        !both(t, i),
                        "round {round}: an acknowledged XA ROLLBACK left rows ({t}, {i})"
                    );
                    assert!(!is_listed);
                    assert!(lockable(&e, &key(round, t, i, 1)));
                }
                _ => {}
            }
        }
        // The prepared branches are finished from here, every other one
        // committed: both rows follow, the locks go.
        let finished: HashSet<Vec<u8>> = to_finish.iter().map(|&(t, i)| xid(round, t, i)).collect();
        for (n, &(t, i)) in to_finish.iter().enumerate() {
            let x = xid(round, t, i);
            let commit = n % 2 == 0;
            if commit {
                Txn::xa_commit(&p, &x).unwrap().wait().unwrap();
            } else {
                Txn::xa_rollback(&p, &x).unwrap().wait().unwrap();
            }
            assert_eq!(both(t, i), commit);
            assert!(lockable(&e, &key(round, t, i, 1)) && lockable(&e, &key(round, t, i, 2)));
        }
        // Branches whose prepare was durable but never acknowledged: rolled
        // back, leaving nothing.
        for x in Txn::xa_recover(&p) {
            assert!(
                !finished.contains(&x),
                "round {round}: a finished branch is still listed"
            );
            Txn::xa_rollback(&p, &x).unwrap().wait().unwrap();
        }
        assert!(Txn::xa_recover(&p).is_empty());
        assert_eq!(
            e.txn_shared().locks().key_count(),
            0,
            "round {round}: locks left behind"
        );
        eprintln!(
            "xa kill9 round {round}: {} steps acknowledged, {} branches listed after the kill ({wait:?})",
            last.len(),
            listed.len()
        );
    }
    eprintln!(
        "xa kill9: {rounds} rounds; prepared and recovered with their locks: {n_prepared}; \
         committed: {n_committed}; rolled back: {n_rolled}; caught while being finished: {n_flight}; \
         open and gone: {n_open}"
    );
    assert!(n_prepared > 0 && n_committed > 0 && n_rolled > 0 && n_open > 0);
    let _ = std::fs::remove_dir_all(&dir);
}
