//! kill -9 of a process producing to engine-stored streams: every append
//! it acknowledged is there after the restart, with its offsets and bytes,
//! and new appends continue after them.
//!
//! The test runs itself as the child (`kill9_child`, ignored unless the
//! parent starts it), which produces from several tasks with the commit
//! log's full syncs and prints each acknowledgement; the parent kills it
//! with SIGKILL at a random moment, three rounds over the same directory.

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

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_engine::{Engine, EngineConfig};
use sprig_proto_kafka::topics::TopicDefaults;
use sprig_streams::seg::kafka::{EngineStreams, decode_stored};
use sprig_streams::seg::records::TopicRecord;
use sprig_streams::seg::testing::{TempDir, batch_with};
use sprig_streams::seg::{Key, SegConfig};

const DIR_VAR: &str = "SPRIG_KILL9_DIR";
const ROUND_VAR: &str = "SPRIG_KILL9_ROUND";
const TENANT: u64 = 9;
const PARTS: u32 = 4;

fn open(dir: &Path) -> (Engine, Arc<EngineStreams>) {
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.budget.engine_mb = 64;
    let e = Engine::open(cfg).unwrap();
    let s = EngineStreams::open(
        &e,
        dir.join("streams"),
        SegConfig {
            segment_bytes: 64 << 10,
            index_interval: 1024,
            flush_every: Duration::from_millis(50),
            ..SegConfig::default()
        },
        TopicDefaults::default(),
    )
    .unwrap();
    (e, Arc::new(s))
}

fn value(round: u32, task: u32, n: u32, i: u32) -> Vec<u8> {
    format!("r{round}-t{task}-n{n}-i{i}").into_bytes()
}

/// The child: produce until killed.
#[test]
#[ignore = "run by kill9_keeps_every_acknowledged_append"]
fn kill9_child() {
    let Ok(dir) = std::env::var(DIR_VAR) else {
        return;
    };
    let round: u32 = std::env::var(ROUND_VAR).unwrap().parse().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (_e, s) = open(Path::new(&dir));
        s.spawn_background();
        let rec = match s.topic(TENANT, "eng.k9").unwrap() {
            Some(r) => r,
            None => s
                .create(
                    TENANT,
                    TopicRecord {
                        name: "eng.k9".into(),
                        stream_id: 0,
                        partitions: 4,
                        retention_ms: None,
                        retention_bytes: None,
                        compact: None,
                        max_message_bytes: None,
                        created_ms: 0,
                    },
                )
                .await
                .unwrap()
                .unwrap(),
        };
        // libtest has printed "test <name> ... " without a newline.
        println!();
        println!("READY {}", rec.stream_id);
        std::io::stdout().flush().unwrap();
        let mut tasks = Vec::new();
        for task in 0..8u32 {
            let s = Arc::clone(&s);
            let stream = rec.stream_id;
            tasks.push(tokio::spawn(async move {
                for n in 0u32.. {
                    let part = (task + n) % PARTS;
                    let count = 1 + (n % 3);
                    let recs: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = (0..count)
                        .map(|i| (Some(b"k".to_vec()), Some(value(round, task, n, i))))
                        .collect();
                    let b = batch_with(&recs, 1_000 + i64::from(n), None);
                    let key = Key {
                        tenant: TENANT,
                        stream,
                        partition: part,
                    };
                    let p = s.propose(key, &b).unwrap();
                    let base = s
                        .outcome(p, tokio::time::Instant::now() + Duration::from_secs(30))
                        .await
                        .unwrap();
                    println!("ACK {part} {base} {count} {round} {task} {n}");
                    std::io::stdout().flush().unwrap();
                }
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
    });
}

#[derive(Debug, Clone)]
struct Ack {
    base: i64,
    count: u32,
    round: u32,
    task: u32,
    n: u32,
}

#[test]
fn kill9_keeps_every_acknowledged_append() {
    let d = TempDir::new("kill9-streams");
    let exe = std::env::current_exe().unwrap();
    let mut acked: BTreeMap<u32, Vec<Ack>> = BTreeMap::new();
    let mut total = 0usize;
    for round in 0..3u32 {
        let mut child = Command::new(&exe)
            .args([
                "kill9_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DIR_VAR, d.path())
            .env(ROUND_VAR, round.to_string())
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
        // Kill after a varying number of acknowledgements.
        let want = 150 + 120 * round as usize;
        let mut got = 0usize;
        let started = Instant::now();
        let mut ready = false;
        while got < want && started.elapsed() < Duration::from_secs(60) {
            let Ok(line) = rx.recv_timeout(Duration::from_secs(60)) else {
                break;
            };
            let f: Vec<&str> = line.split_whitespace().collect();
            match f.first() {
                Some(&"READY") => ready = true,
                Some(&"ACK") if f.len() == 7 => {
                    let part: u32 = f[1].parse().unwrap();
                    acked.entry(part).or_default().push(Ack {
                        base: f[2].parse().unwrap(),
                        count: f[3].parse().unwrap(),
                        round: f[4].parse().unwrap(),
                        task: f[5].parse().unwrap(),
                        n: f[6].parse().unwrap(),
                    });
                    got += 1;
                }
                _ => {}
            }
        }
        assert!(ready, "round {round}: the child never got ready");
        assert!(got >= want, "round {round}: only {got} acknowledgements");
        child.kill().unwrap(); // SIGKILL
        let _ = child.wait();
        // Acknowledgements already printed before the kill still count.
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(200)) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.first() == Some(&"ACK") && f.len() == 7 {
                let part: u32 = f[1].parse().unwrap();
                acked.entry(part).or_default().push(Ack {
                    base: f[2].parse().unwrap(),
                    count: f[3].parse().unwrap(),
                    round: f[4].parse().unwrap(),
                    task: f[5].parse().unwrap(),
                    n: f[6].parse().unwrap(),
                });
            }
        }
        let _ = reader.join();
        // Recover and check every acknowledged append.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (_e, s) = open(d.path());
            let rec = s.topic(TENANT, "eng.k9").unwrap().unwrap();
            for part in 0..PARTS {
                let key = Key {
                    tenant: TENANT,
                    stream: rec.stream_id,
                    partition: part,
                };
                let mut all = Vec::new();
                let mut at = 0i64;
                loop {
                    let (b, bytes) = s.log().read(key, at, 1 << 20).unwrap();
                    let recs = decode_stored(&bytes, at).unwrap();
                    if recs.is_empty() {
                        assert_eq!(at, b.high_watermark);
                        break;
                    }
                    at = recs.last().unwrap().offset + 1;
                    all.extend(recs);
                }
                let by_offset: BTreeMap<i64, Vec<u8>> = all
                    .into_iter()
                    .map(|r| (r.offset, r.value.unwrap()))
                    .collect();
                for a in acked.get(&part).into_iter().flatten() {
                    for i in 0..a.count {
                        let o = a.base + i64::from(i);
                        assert_eq!(
                            by_offset.get(&o),
                            Some(&value(a.round, a.task, a.n, i)),
                            "partition {part} offset {o} lost or changed after round {round}"
                        );
                    }
                }
                // Offsets are dense from 0 (no gap, no overlap).
                let offsets: Vec<i64> = by_offset.keys().copied().collect();
                assert_eq!(offsets, (0..offsets.len() as i64).collect::<Vec<_>>());
            }
        });
        total = acked.values().map(Vec::len).sum();
    }
    assert!(total >= 800, "{total} acknowledgements in all");
}
