use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::testutil::TempDir;

/// A state machine that remembers what was applied.
#[derive(Default)]
struct Recorder {
    applied: Mutex<BTreeMap<UnitId, Vec<Applied>>>,
}

/// An applied entry: its index and data.
type Applied = (u64, Vec<u8>);

impl StateMachine for Recorder {
    fn applied_index(&self, unit: UnitId) -> Result<u64> {
        Ok(self
            .applied
            .lock()
            .unwrap()
            .get(&unit)
            .and_then(|v| v.last())
            .map_or(0, |e| e.0))
    }

    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()> {
        let mut a = self.applied.lock().unwrap();
        let v = a.entry(unit).or_default();
        for e in entries {
            let expect = v.last().map_or(e.index, |l| l.0 + 1);
            assert_eq!(e.index, expect, "applied out of order");
            v.push((e.index, e.data.clone()));
        }
        Ok(())
    }
}

fn cfg() -> LogConfig {
    LogConfig {
        sync: SyncMode::Off,
        ..LogConfig::default()
    }
}

#[test]
fn propose_wait_apply_and_replay() {
    let dir = TempDir::new("log");
    let sm = Arc::new(Recorder::default());
    {
        let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
        let mut tickets = Vec::new();
        for i in 0..100u32 {
            let unit = UnitId(u64::from(i % 3) + 1);
            tickets.push(
                log.propose(unit, i.to_be_bytes().to_vec(), Durability::Local)
                    .unwrap(),
            );
        }
        for t in &tickets {
            t.wait().unwrap();
            t.wait_applied().unwrap();
        }
        assert_eq!(log.applied_index(UnitId(1)), 34);
        assert_eq!(log.durable_index(UnitId(2)), 33);
    }
    // A fresh state machine gets every entry back on reopen.
    let fresh = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), cfg(), fresh.clone()).unwrap();
    assert_eq!(log.stats().replayed, 100);
    assert_eq!(*fresh.applied.lock().unwrap(), *sm.applied.lock().unwrap());
    // New proposals continue after the replayed ones.
    let t = log
        .propose(UnitId(1), b"next".to_vec(), Durability::Local)
        .unwrap();
    assert_eq!(t.index(), 35);
    t.wait_applied().unwrap();
}

#[test]
fn concurrent_proposals_group() {
    let dir = TempDir::new("group");
    let sm = Arc::new(Recorder::default());
    let log = Arc::new(LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for i in 0..500u32 {
                    let tk = log
                        .propose(
                            UnitId(1),
                            [t, i].iter().flat_map(|x: &u32| x.to_be_bytes()).collect(),
                            Durability::Local,
                        )
                        .unwrap();
                    tk.wait().unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    log.wait_applied(UnitId(1), 4000).unwrap();
    let s = log.stats();
    assert_eq!(s.entries, 4000);
    assert!(s.groups < 4000, "concurrent proposals share groups: {s:?}");
    assert_eq!(sm.applied.lock().unwrap()[&UnitId(1)].len(), 4000);
}

#[test]
fn relaxed_entries_are_synced_by_the_timer() {
    let dir = TempDir::new("relaxed");
    let c = LogConfig {
        relaxed_sync: Duration::from_millis(30),
        ..cfg()
    };
    let log = LocalLog::open(dir.path(), c, Arc::new(Recorder::default())).unwrap();
    let t = log
        .propose(UnitId(1), b"x".to_vec(), Durability::LocalRelaxed)
        .unwrap();
    t.wait().unwrap();
    let before = log.stats().syncs;
    std::thread::sleep(Duration::from_millis(120));
    assert!(log.stats().syncs > before, "timer sync");
    assert_eq!(log.durable_index(UnitId(1)), 1);
}

#[tokio::test]
async fn async_wait() {
    let dir = TempDir::new("async");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    let t = log
        .propose(UnitId(7), b"a".to_vec(), Durability::Strict)
        .unwrap();
    t.wait_async().await.unwrap();
    t.wait_applied_async().await.unwrap();
    assert!(t.is_durable());
}

#[test]
fn closed_log_refuses() {
    let dir = TempDir::new("closed");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    log.close();
    assert!(log.propose(UnitId(1), vec![1], Durability::Local).is_err());
}

#[test]
fn checkpoint_trims_and_replay_resumes_after_it() {
    let dir = TempDir::new("ckpt");
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
    for i in 0..50u8 {
        log.propose(UnitId(1), vec![i], Durability::Local).unwrap();
    }
    log.wait_applied(UnitId(1), 50).unwrap();
    let trimmed = log.checkpoint(&|| Ok(())).unwrap();
    assert_eq!(trimmed, 50);
    for i in 50..60u8 {
        log.propose(UnitId(1), vec![i], Durability::Local).unwrap();
    }
    log.wait_applied(UnitId(1), 60).unwrap();
    drop(log);
    // The state machine kept 1..=60, so only nothing is replayed; a state
    // machine that "lost" 51..=60 gets exactly those back.
    {
        let mut a = sm.applied.lock().unwrap();
        a.get_mut(&UnitId(1)).unwrap().truncate(50);
    }
    let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
    assert_eq!(log.stats().replayed, 10);
    assert_eq!(sm.applied.lock().unwrap()[&UnitId(1)].len(), 60);
    drop(log);
    // A state machine behind the trim point is an error, not silent loss.
    let behind = Arc::new(Recorder::default());
    assert!(LocalLog::open(dir.path(), cfg(), behind).is_err());
}

/// Bytes that do not compress (a fixed xorshift stream).
fn noise(seed: u64, n: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect()
}

fn log_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|f| {
            let name = f.as_ref().unwrap().file_name();
            name.to_string_lossy().ends_with(".raftlog")
        })
        .count()
}

/// A checkpoint deletes the files it trimmed, down to the one being
/// written, so the log on disk stays near one file however much went
/// through it; what was written after the checkpoint still replays.
#[test]
fn checkpoint_deletes_trimmed_files() {
    let dir = TempDir::new("purge");
    let c = LogConfig {
        target_file_size: 1 << 20,
        ..cfg()
    };
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), c.clone(), sm.clone()).unwrap();
    let mut index = 0;
    for round in 0..3u64 {
        // One entry per group: a group is written to one file whole.
        for i in 0..40 {
            log.propose(
                UnitId(1),
                noise(round * 100 + i, 128 << 10),
                Durability::Local,
            )
            .unwrap()
            .wait()
            .unwrap();
        }
        index += 40;
        log.wait_applied(UnitId(1), index).unwrap();
        assert!(
            log_files(dir.path()) >= 4,
            "5 MiB of entries span several 1 MiB files"
        );
        assert_eq!(log.checkpoint(&|| Ok(())).unwrap(), 40);
        assert!(
            log_files(dir.path()) <= 2,
            "round {round}: {} files left after the checkpoint",
            log_files(dir.path())
        );
        assert!(log.disk_bytes() <= 3 << 20, "{}", log.disk_bytes());
    }
    for i in 0..5 {
        log.propose(UnitId(1), noise(1000 + i, 128 << 10), Durability::Local)
            .unwrap();
    }
    log.wait_applied(UnitId(1), index + 5).unwrap();
    drop(log);
    {
        let mut a = sm.applied.lock().unwrap();
        a.get_mut(&UnitId(1)).unwrap().truncate(120);
    }
    let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
    assert_eq!(log.stats().replayed, 5);
    let a = sm.applied.lock().unwrap();
    for (i, (idx, data)) in a[&UnitId(1)][120..].iter().enumerate() {
        assert_eq!(*idx, 121 + i as u64);
        assert_eq!(data, &noise(1000 + i as u64, 128 << 10));
    }
}

/// Groups at or above `compress_min_bytes` are compressed in the log: the
/// file is much smaller than what went in, and replay returns the same
/// bytes. With compression off the file holds every byte.
#[test]
fn large_groups_are_compressed_and_replay_unchanged() {
    let on_disk = |compress_min_bytes: u64| {
        let dir = TempDir::new("lz4");
        let c = LogConfig {
            compress_min_bytes,
            ..cfg()
        };
        let sm = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), c.clone(), sm.clone()).unwrap();
        let payload = |i: u8| {
            let mut v = noise(u64::from(i), 16);
            v.extend(std::iter::repeat_n(b'x', 32 << 10));
            v
        };
        for i in 0..32u8 {
            log.propose(UnitId(1), payload(i), Durability::Local)
                .unwrap();
        }
        log.wait_applied(UnitId(1), 32).unwrap();
        drop(log);
        let bytes: u64 = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|f| f.unwrap().metadata().unwrap().len())
            .sum();
        sm.applied.lock().unwrap().clear();
        let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
        assert_eq!(log.stats().replayed, 32);
        let a = sm.applied.lock().unwrap();
        for (i, (idx, data)) in a[&UnitId(1)].iter().enumerate() {
            assert_eq!(*idx, 1 + i as u64);
            assert_eq!(data, &payload(u8::try_from(i).unwrap()));
        }
        bytes
    };
    let plain = on_disk(0);
    let packed = on_disk(4 << 10);
    assert!(plain >= 1 << 20, "uncompressed: {plain} bytes");
    assert!(packed < plain / 8, "compressed: {packed} of {plain} bytes");
}

/// Under the crash simulation, every acknowledged `Local` entry survives,
/// and replay never yields a gap.
#[test]
fn sim_crash_keeps_acknowledged_entries() {
    let mut lost_total = 0;
    for seed in 1..=40u64 {
        let dir = TempDir::new("simcrash");
        let sim = SimDisk::new();
        let c = LogConfig {
            sim: Some(sim.clone()),
            ..cfg()
        };
        let sm = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), c.clone(), sm).unwrap();
        let mut acked = 0;
        for i in 0..(20 + seed * 3) {
            let d = if i % 3 == 0 {
                Durability::LocalRelaxed
            } else {
                Durability::Local
            };
            let t = log.propose(UnitId(1), i.to_be_bytes().to_vec(), d).unwrap();
            if d.needs_sync() {
                t.wait().unwrap();
                acked = t.index();
            }
        }
        // Simulate a power loss: stop without the closing sync, then cut
        // unsynced tails.
        log.halt();
        drop(log);
        lost_total += sim.crash(dir.path(), seed).unwrap();
        let fresh = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), LogConfig { sim: None, ..c }, fresh.clone()).unwrap();
        let got = fresh
            .applied
            .lock()
            .unwrap()
            .get(&UnitId(1))
            .cloned()
            .unwrap_or_default();
        assert!(
            got.len() as u64 >= acked,
            "seed {seed}: {} < acked {acked}",
            got.len()
        );
        for (n, (i, data)) in got.iter().enumerate() {
            assert_eq!(*i, n as u64 + 1);
            assert_eq!(data, &((n as u64).to_be_bytes().to_vec()));
        }
        drop(log);
    }
    // The simulation must actually lose unsynced bytes; raft-engine's sync
    // when the log is dropped comes after the power cut and keeps nothing.
    assert!(lost_total > 0, "the crash simulation lost nothing");
}

/// `entry` reads back what was written, and nothing past the end.
#[test]
fn entry_reads_one_index() {
    let dir = TempDir::new("entry");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    for i in 0..3u64 {
        log.propose(UnitId(1), i.to_be_bytes().to_vec(), Durability::Local)
            .unwrap()
            .wait()
            .unwrap();
    }
    let e = log.entry(UnitId(1), 2).unwrap().unwrap();
    assert_eq!((e.index, e.data.as_slice()), (2, &1u64.to_be_bytes()[..]));
    assert!(log.entry(UnitId(1), 9).unwrap().is_none());
}
