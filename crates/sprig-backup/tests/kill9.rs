//! Kill -9 during backups and restores: the test re-runs its own binary
//! as a child that takes backups (or restores) in a loop, kills it with
//! SIGKILL at a random moment, then opens the engine again and checks
//! that every backup listed restores to the right state, and that a
//! restore's target holds either all of it or nothing, and that a retried
//! restore finishes.

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use common::{Dir, TestKeys, backup_cfg_at, commit, get, key, open_engine, settle};
use sprig_backup::service::BackupService;
use sprig_engine::{StoreKind, UnitId};

const CHILD: &str = "NBACKUP_KILL9_CHILD";
const DIR: &str = "NBACKUP_KILL9_DIR";

/// The child: `backup` takes backups forever; `restore:<into>:<replace>`
/// restores tenant 7 into `<into>` once, then waits to be killed.
#[test]
#[ignore = "run by the kill -9 tests as a child process"]
fn kill9_child() {
    let Ok(what) = std::env::var(CHILD) else {
        return;
    };
    let dir = std::path::PathBuf::from(std::env::var(DIR).unwrap());
    let e = open_engine(&dir.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg_at(&dir), Arc::new(TestKeys)).unwrap();
    if what == "backup" {
        loop {
            svc.archive_pass(true).unwrap();
            svc.backup_now(7).unwrap();
        }
    }
    let mut parts = what.split(':');
    let _ = parts.next();
    let into: u64 = parts.next().unwrap().parse().unwrap();
    let replace = parts.next() == Some("true");
    svc.restore(7, None, into, replace).unwrap();
    println!("RESTORED");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_child(dir: &Path, what: &str, kill_after: Duration) {
    let mut c = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "kill9_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, what)
        .env(DIR, dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(kill_after);
    // SIGKILL.
    c.kill().unwrap();
    c.wait().unwrap();
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Tenant 7 with ~4 MB of rows; returns its checksum.
fn fill(dir: &Path) -> (u64, u64, u32) {
    let e = open_engine(&dir.join("engine"), StoreKind::Rocks);
    for b in 0..200u32 {
        let ops: Vec<_> = (0..100u32)
            .map(|i| {
                (
                    key(7, b * 100 + i),
                    Some(format!("{b}-{i}-{}", "y".repeat(180))),
                )
            })
            .collect();
        commit(&e, &ops);
    }
    // Tenant 11 exists already (a replace target).
    commit(&e, &[(key(11, 999_999), Some("bootstrap".into()))]);
    settle(&e, 7);
    settle(&e, 11);
    let s = e.unit_checksum(UnitId(7)).unwrap();
    (s.entries, s.bytes, s.crc)
}

#[test]
fn kill9_during_backups_leaves_only_whole_backups() {
    let d = Dir::new("k9b");
    let want = fill(&d.0);
    let mut rng = Rng(0xDEAD_BEEF_1234_5678);
    for _ in 0..15 {
        run_child(&d.0, "backup", Duration::from_millis(150 + rng.below(900)));
    }
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg_at(&d.0), Arc::new(TestKeys)).unwrap();
    let list = svc.list(7).unwrap();
    assert!(!list.is_empty(), "no backup finished in 15 runs");
    // Nothing changed meanwhile: every backup restores to the same state.
    for m in &list {
        let at = sprig_engine::Ts(m.ts).physical_ms();
        let r = svc.verify(7, Some(at)).unwrap();
        assert_eq!((r.entries, r.bytes, r.crc), want, "backup {}", m.taken_ms);
    }
    // Chunks left by killed backups are swept by retention.
    let rep = svc.retention_now(7, sprig_backup::now_ms()).unwrap();
    let r = svc.verify(7, None).unwrap();
    assert_eq!((r.entries, r.bytes, r.crc), want);
    println!(
        "kill9 backups: {} whole backups listed, {} orphan chunks swept",
        list.len(),
        rep.chunks
    );
}

#[test]
fn kill9_during_restores_leaves_all_or_nothing() {
    let d = Dir::new("k9r");
    let want = fill(&d.0);
    {
        let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
        let svc = BackupService::open(e.clone(), backup_cfg_at(&d.0), Arc::new(TestKeys)).unwrap();
        svc.backup_now(7).unwrap();
    }
    let mut rng = Rng(0x0123_4567_89AB_CDEF);
    let (mut whole, mut empty) = (0, 0);
    for round in 0..30u64 {
        // Even rounds: into a tenant that held nothing (one-step ingest);
        // odd rounds: replacing tenant 11 through its log.
        let (into, replace) = if round % 2 == 0 {
            (100 + round, false)
        } else {
            (11, true)
        };
        run_child(
            &d.0,
            &format!("restore:{into}:{replace}"),
            Duration::from_millis(40 + rng.below(500)),
        );
        let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
        let svc = BackupService::open(e.clone(), backup_cfg_at(&d.0), Arc::new(TestKeys)).unwrap();
        let s = e.unit_checksum(UnitId(into)).unwrap();
        let complete = if replace {
            // Old versions may differ; compare what readers see.
            s.entries > 0
                && (0..20_000u32)
                    .step_by(997)
                    .all(|i| get(&e, &key(into, i)) == get(&e, &key(7, i)))
                && get(&e, &key(into, 999_999)).is_none()
        } else {
            // Keys carry the tenant id, so compare counts and contents.
            (s.entries, s.bytes) == (want.0, want.1)
                && (0..20_000u32)
                    .step_by(997)
                    .all(|i| get(&e, &key(into, i)) == get(&e, &key(7, i)))
        };
        if complete {
            whole += 1;
        } else {
            assert_eq!(
                s.entries, 0,
                "round {round}: tenant {into} is half-restored ({} entries)",
                s.entries
            );
            empty += 1;
            // The retry finishes.
            svc.restore(7, None, into, replace).unwrap();
            if !replace {
                let s = e.unit_checksum(UnitId(into)).unwrap();
                assert_eq!(
                    (s.entries, s.bytes),
                    (want.0, want.1),
                    "round {round} retry"
                );
            }
            assert_eq!(get(&e, &key(into, 12_345)), get(&e, &key(7, 12_345)));
        }
        svc.stop();
    }
    println!("kill9 restores: {whole} finished before the kill, {empty} undone and retried");
    assert!(empty > 0, "no kill landed inside a restore");
}
