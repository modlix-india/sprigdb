//! kill -9 of a process whose queue owner publishes to and acknowledges
//! from the engine store: after the restart every confirmed message that
//! was not acknowledged is there (and is delivered again), and no
//! acknowledged message comes back.
//!
//! The test runs itself as the child (`kill9_queue_child`, ignored unless
//! the parent starts it), with the commit log's full syncs; the parent
//! kills it with SIGKILL, three rounds over the same directory.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::{Durability, Engine, EngineConfig};
use sprig_queues::enginestore::EngineQueues;
use sprig_queues::store::{MsgId, MsgRef, MsgStore, StoredMsg, WriteBatch};

const DIR_VAR: &str = "SPRIG_QKILL9_DIR";
const QUEUE: u64 = 77;

fn engine(dir: &Path) -> Engine {
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.budget.engine_mb = 64;
    Engine::open(cfg).unwrap()
}

fn id(n: u64) -> MsgId {
    let mut a = [0u8; 16];
    a[..8].copy_from_slice(&n.to_be_bytes());
    a[15] = 1;
    a
}

fn msg(seq: u64, n: u64) -> StoredMsg {
    StoredMsg {
        lane: 0,
        pclass: 0,
        seq,
        msg_id: id(n),
        fmt: 0,
        exchange: String::new(),
        routing_key: "q".into(),
        props: Bytes::new(),
        body: Bytes::from(format!("body-{n}")),
        expires_at_ms: None,
        delivery_count: 0,
        deaths: None,
    }
}

/// The child: one owner of one lane, publishing in batches and
/// acknowledging older messages, until killed.
#[test]
#[ignore = "run by kill9_keeps_confirmed_and_drops_acked"]
fn kill9_queue_child() {
    let Ok(dir) = std::env::var(DIR_VAR) else {
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let q = EngineQueues::new(engine(Path::new(&dir)), Durability::Local);
        let s = q.store(5, 1);
        let c = s.claim(QUEUE, 0).await.unwrap();
        let mut state = c.state;
        // What this owner holds: (seq, n) of unacknowledged messages.
        let mut held: Vec<(u64, u64)> = s
            .load(QUEUE, 0, None, usize::MAX)
            .await
            .unwrap()
            .iter()
            .map(|m| (m.seq, u64::from_be_bytes(m.msg_id[..8].try_into().unwrap())))
            .collect();
        let mut n = held
            .iter()
            .map(|h| h.1 + 1)
            .max()
            .unwrap_or(0)
            .max(state.next_seq * 4);
        // libtest has printed "test <name> ... " without a newline.
        println!();
        println!("READY {} {}", c.epoch, held.len());
        std::io::stdout().flush().unwrap();
        for round in 0u64.. {
            let mut b = WriteBatch {
                queue: QUEUE,
                lane: 0,
                epoch: c.epoch,
                ..WriteBatch::default()
            };
            let mut published = Vec::new();
            for _ in 0..=(round % 4) {
                let seq = state.next_seq;
                state.next_seq += 1;
                b.inserts.push(msg(seq, n));
                published.push(n);
                held.push((seq, n));
                n += 1;
            }
            // Acknowledge the oldest two of every three rounds (a run of
            // consecutive sequences: one range delete).
            let mut acked = Vec::new();
            if round % 3 != 2 && held.len() > 4 {
                for (seq, m) in held.drain(..2) {
                    b.deletes.push(MsgRef {
                        lane: 0,
                        pclass: 0,
                        seq,
                        msg_id: id(m),
                    });
                    acked.push(m);
                }
            }
            b.state = state;
            s.write(&b).await.unwrap();
            for m in published {
                println!("PUB {m}");
            }
            for m in acked {
                println!("ACK {m}");
            }
            std::io::stdout().flush().unwrap();
        }
    });
}

#[test]
fn kill9_keeps_confirmed_and_drops_acked() {
    struct Gone(PathBuf);
    impl Drop for Gone {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let d = std::env::temp_dir().join(format!("sprig-qkill9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let _gone = Gone(d.clone());
    let exe = std::env::current_exe().unwrap();
    let mut published: BTreeSet<u64> = BTreeSet::new();
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut last_epoch = 0u64;
    for round in 0..3u32 {
        let mut child = Command::new(&exe)
            .args([
                "kill9_queue_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DIR_VAR, &d)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let want = 200 + 150 * round as usize;
        let mut got = 0usize;
        let started = Instant::now();
        let mut take = |line: &str,
                        got: &mut usize,
                        published: &mut BTreeSet<u64>,
                        acked: &mut BTreeSet<u64>| {
            let f: Vec<&str> = line.split_whitespace().collect();
            match (f.first(), f.get(1).and_then(|x| x.parse::<u64>().ok())) {
                (Some(&"PUB"), Some(n)) => {
                    published.insert(n);
                    *got += 1;
                }
                (Some(&"ACK"), Some(n)) => {
                    acked.insert(n);
                }
                (Some(&"READY"), Some(e)) => {
                    assert!(e > last_epoch, "a new owner epoch after a restart");
                    last_epoch = e;
                }
                _ => {}
            }
        };
        while got < want && started.elapsed() < Duration::from_secs(60) {
            let Ok(line) = rx.recv_timeout(Duration::from_secs(60)) else {
                break;
            };
            take(&line, &mut got, &mut published, &mut acked);
        }
        assert!(got >= want, "round {round}: only {got} confirms");
        child.kill().unwrap(); // SIGKILL
        let _ = child.wait();
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(200)) {
            take(&line, &mut got, &mut published, &mut acked);
        }
        let _ = reader.join();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let q = EngineQueues::new(engine(&d), Durability::Local);
            let s = q.store(5, 1);
            let present: BTreeSet<u64> = s
                .load(QUEUE, 0, None, usize::MAX)
                .await
                .unwrap()
                .iter()
                .map(|m| {
                    let n = u64::from_be_bytes(m.msg_id[..8].try_into().unwrap());
                    assert_eq!(m.body, Bytes::from(format!("body-{n}")));
                    n
                })
                .collect();
            for n in published.difference(&acked) {
                assert!(
                    present.contains(n),
                    "round {round}: confirmed message {n} lost"
                );
            }
            for n in &acked {
                assert!(
                    !present.contains(n),
                    "round {round}: acknowledged message {n} came back"
                );
            }
            assert_eq!(s.stored(QUEUE).await.unwrap(), present.len() as u64);
        });
    }
    assert!(acked.len() > 100 && published.len() > 900);
}
