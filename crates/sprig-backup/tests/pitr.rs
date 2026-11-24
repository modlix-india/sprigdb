//! Backups and point-in-time restore on a real engine (RocksDB store,
//! raft-engine log): round trips, restore into a new tenant, retention,
//! and the gate of 20 restores to random moments, each equal to the
//! committed history (span checksums).

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

mod common;

use std::sync::Arc;

use common::{Dir, backup_cfg, commit, get, key, open_engine, settle};
use sprig_backup::archive::{ArchiveConfig, segments};
use sprig_backup::crypt::derive;
use sprig_backup::service::{BackupService, Policy};
use sprig_backup::{LocalDir, restore};
use sprig_engine::StoreKind;

/// A tiny deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn backup_then_restore_into_a_new_tenant() {
    let d = Dir::new("rt");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    for i in 0..500u32 {
        commit(&e, &[(key(7, i), Some(format!("v{i}")))]);
    }
    for i in (0..500u32).step_by(5) {
        commit(&e, &[(key(7, i), None)]);
    }
    settle(&e, 7);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    let b = svc.backup_now(7).unwrap();
    assert_eq!(b.manifest.units.len(), 1);
    assert!(b.manifest.new_chunks >= 1);
    // A second backup of unchanged data uploads (almost) nothing: only the
    // chunk holding the unit's bookkeeping can differ.
    let b2 = svc.backup_now(7).unwrap();
    assert!(b2.manifest.new_chunks <= 1, "{:?}", b2.manifest);
    // Later changes reach the restore through the archive.
    for i in 500..520u32 {
        commit(&e, &[(key(7, i), Some(format!("late{i}")))]);
    }
    settle(&e, 7);
    let r = svc.restore(7, None, 9, false).unwrap();
    assert_eq!(r.mode, "ingest");
    assert!(r.replayed >= 20, "{r:?}");
    assert_eq!(
        e.unit_checksum(sprig_engine::UnitId(9)).unwrap().entries,
        r.entries
    );
    for i in 0..520u32 {
        let want = get(&e, &key(7, i));
        assert_eq!(get(&e, &key(9, i)), want, "key {i}");
    }
    assert_eq!(get(&e, &key(9, 510)).unwrap(), "late510".as_bytes());
    assert!(get(&e, &key(9, 5)).is_none());
    // The new tenant is a tenant like any other: it takes writes.
    commit(&e, &[(key(9, 1), Some("after".into()))]);
    assert_eq!(get(&e, &key(9, 1)).unwrap(), "after".as_bytes());
    // It can't be restored over.
    assert!(svc.restore(7, None, 9, false).is_err());
    // Its first backup was taken by the restore.
    assert_eq!(svc.list(9).unwrap().len(), 1);
}

#[test]
fn twenty_random_moments_equal_the_history() {
    let d = Dir::new("pitr");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let mut cfg = backup_cfg(&d);
    cfg.archive = ArchiveConfig {
        close_after: std::time::Duration::from_millis(1),
        segment_bytes: 64 << 10,
        segment_entries: 97,
    };
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    // (commit physical ms, checksum after it)
    let mut history: Vec<(u64, (u64, u64, u32))> = Vec::new();
    let mut backups = Vec::new();
    for n in 0..900u32 {
        let mut ops = Vec::new();
        for _ in 0..=rng.below(4) {
            let k = key(7, rng.below(300) as u32);
            if rng.below(5) == 0 {
                ops.push((k, None));
            } else {
                ops.push((k, Some(format!("n{n}-{}", rng.next()))));
            }
        }
        commit(&e, &ops);
        settle(&e, 7);
        let ms = e.applied_max_ts().physical_ms();
        let s = e.unit_checksum(sprig_engine::UnitId(7)).unwrap();
        history.push((ms, (s.entries, s.bytes, s.crc)));
        if [60, 300, 610].contains(&n) {
            backups.push(svc.backup_now(7).unwrap().manifest.ts);
        }
        if n % 41 == 0 {
            svc.archive_pass(true).unwrap();
        }
        if n % 3 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    svc.archive_pass(true).unwrap();
    assert!(segments(&*svc.target(7).unwrap(), 7, 7).unwrap().len() > 5);
    let first_backup_ms = sprig_engine::Ts(backups[0]).physical_ms();
    let candidates: Vec<usize> = (0..history.len())
        .filter(|&i| history[i].0 >= first_backup_ms)
        .collect();
    let mut checked = 0;
    let (mut replayed, mut reached) = (0, 0);
    for _ in 0..20 {
        let i = candidates[rng.below(candidates.len() as u64) as usize];
        let t = history[i].0;
        // The state after the last commit in millisecond `t`.
        let want = history.iter().rev().find(|(ms, _)| *ms <= t).unwrap().1;
        let r = svc.verify(7, Some(t)).unwrap();
        assert_eq!((r.entries, r.bytes, r.crc), want, "restore to {t}: {r:?}");
        checked += 1;
        replayed += usize::from(r.replayed > 0);
        reached += usize::from(r.reached);
    }
    assert_eq!(checked, 20);
    // Most moments fall after a backup's point: they need the archive.
    assert!(
        replayed >= 10 && reached >= 10,
        "{replayed} replayed, {reached} reached"
    );
    // Before the first backup there's nothing to restore from.
    assert!(svc.verify(7, Some(first_backup_ms - 1_000)).is_err());
}

#[test]
fn retention_keeps_the_window_and_drops_the_rest() {
    let d = Dir::new("ret");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    let mut taken = Vec::new();
    for round in 0..4u32 {
        for i in 0..50u32 {
            commit(&e, &[(key(7, round * 50 + i), Some(format!("r{round}")))]);
        }
        settle(&e, 7);
        svc.archive_pass(true).unwrap();
        taken.push(svc.backup_now(7).unwrap().manifest.taken_ms);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let target = svc.target(7).unwrap();
    let segs_before = segments(&*target, 7, 7).unwrap().len();
    // Pretend 10 days passed with a 7-day window and 8-day daily keep:
    // only the newest backup stays (it is also the window's base).
    let now = sprig_backup::now_ms() + 10 * 86_400_000;
    let key = common::TestKeys.key(7);
    let rep = sprig_backup::retention::apply(&*target, &key, 7, now, 7, 8).unwrap();
    assert_eq!(rep.backups, taken[..3].to_vec());
    assert_eq!(rep.base, Some(taken[3]));
    assert!(rep.segments > 0 && rep.segments as usize <= segs_before);
    assert!(rep.chunks > 0);
    // What remains still restores.
    let r = svc.verify(7, None).unwrap();
    let live = e.unit_checksum(sprig_engine::UnitId(7)).unwrap();
    assert_eq!(
        (r.entries, r.bytes, r.crc),
        (live.entries, live.bytes, live.crc)
    );
}

#[test]
fn a_disabled_tenant_is_not_archived_or_held() {
    let d = Dir::new("off");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let mut cfg = backup_cfg(&d);
    cfg.policy = Arc::new(|t| Policy {
        enabled: t != 8,
        ..Policy::default()
    });
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    commit(
        &e,
        &[(key(7, 1), Some("a".into())), (key(8, 1), Some("b".into()))],
    );
    settle(&e, 7);
    settle(&e, 8);
    let p = svc.archive_pass(true).unwrap();
    assert!(p.failed.is_empty(), "{p:?}");
    let t = svc.target(7).unwrap();
    assert!(!segments(&*t, 7, 7).unwrap().is_empty());
    assert!(segments(&*t, 8, 8).unwrap().is_empty());
    assert_eq!(e.log_floor_of(sprig_engine::UnitId(8), "backup"), None);
}

#[test]
fn a_backup_from_another_installation_does_not_open() {
    let d = Dir::new("key");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    commit(&e, &[(key(7, 1), Some("a".into()))]);
    settle(&e, 7);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    svc.backup_now(7).unwrap();
    let other = derive(&[9u8; 32], 7).unwrap();
    let t = LocalDir::new(d.0.join("backups"));
    assert!(sprig_backup::backup::list(&t, &other, 7).is_err());
    assert!(restore::verify(&t, &other, 7, None, &d.0.join("w2")).is_err());
}

#[test]
fn restore_replaces_an_existing_tenant_through_its_log() {
    let d = Dir::new("replace");
    let dir = d.0.join("engine");
    let e = open_engine(&dir, StoreKind::Rocks);
    for i in 0..300u32 {
        commit(&e, &[(key(7, i), Some(format!("v{i}")))]);
    }
    // Tenant 11 exists already (what a catalog bootstrap would write).
    for i in 1_000..1_010u32 {
        commit(&e, &[(key(11, i), Some("bootstrap".into()))]);
    }
    settle(&e, 7);
    settle(&e, 11);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    svc.backup_now(7).unwrap();
    commit(&e, &[(key(7, 3), None)]);
    settle(&e, 7);
    assert!(svc.restore(7, None, 11, false).is_err(), "needs replace");
    let r = svc.restore(7, None, 11, true).unwrap();
    assert_eq!(r.mode, "log");
    let check = |e: &sprig_engine::Engine| {
        for i in 0..300u32 {
            assert_eq!(get(e, &key(11, i)), get(e, &key(7, i)), "key {i}");
        }
        assert!(
            get(e, &key(11, 1_005)).is_none(),
            "the old contents are gone"
        );
        assert!(get(e, &key(11, 3)).is_none());
    };
    check(&e);
    commit(&e, &[(key(11, 2), Some("after".into()))]);
    // Survives a restart (the restore went through the log).
    svc.stop();
    drop(svc);
    drop(e);
    let e = open_engine(&dir, StoreKind::Rocks);
    assert_eq!(get(&e, &key(11, 2)).unwrap(), "after".as_bytes());
    commit(&e, &[(key(11, 2), Some("v2".into()))]);
    let e2 = e.clone();
    for i in (0..300u32).filter(|i| *i != 2) {
        assert_eq!(
            get(&e2, &key(11, i)),
            get(&e2, &key(7, i)),
            "key {i} after restart"
        );
    }
}

#[test]
fn a_tenant_with_nothing_here_is_refused() {
    let d = Dir::new("none");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    let err = svc.backup_now(42).unwrap_err();
    assert!(err.to_string().contains("no data"), "{err}");
    assert!(svc.list(42).unwrap().is_empty());
}

#[test]
fn a_gap_in_the_archive_is_refused_not_skipped() {
    let d = Dir::new("gap");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let mut cfg = backup_cfg(&d);
    cfg.archive.segment_entries = 10;
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    commit(&e, &[(key(7, 0), Some("base".into()))]);
    settle(&e, 7);
    svc.backup_now(7).unwrap();
    let mut moments = Vec::new();
    for i in 1..60u32 {
        commit(&e, &[(key(7, i), Some(format!("v{i}")))]);
        settle(&e, 7);
        moments.push(e.applied_max_ts().physical_ms());
        if i % 10 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(3));
        }
    }
    svc.archive_pass(true).unwrap();
    let t = svc.target(7).unwrap();
    let segs = segments(&*t, 7, 7).unwrap();
    assert!(segs.len() >= 4, "{segs:?}");
    // Lose a segment in the middle.
    t.delete(&segs[2].2).unwrap();
    let err = svc.verify(7, Some(moments[55])).unwrap_err();
    assert!(err.to_string().contains("gap"), "{err}");
    // Before the gap still works.
    assert!(svc.verify(7, Some(moments[3])).is_ok());
    // A replace restore that fails before writing leaves the target alone.
    commit(&e, &[(key(12, 1), Some("keep".into()))]);
    settle(&e, 12);
    assert!(svc.restore(7, Some(moments[55]), 12, true).is_err());
    assert_eq!(get(&e, &key(12, 1)).unwrap(), "keep".as_bytes());
}

#[test]
fn a_storage_error_keeps_the_floor() {
    let d = Dir::new("floor");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let mut cfg = backup_cfg(&d);
    cfg.policy = Arc::new(|_| Policy {
        location: "relative/not-allowed".into(),
        ..Policy::default()
    });
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    commit(&e, &[(key(7, 1), Some("a".into()))]);
    settle(&e, 7);
    // First a good pass holds the floor below the entry...
    let good = {
        let mut c = backup_cfg(&d);
        c.work_dir = d.0.join("work2");
        BackupService::open(e.clone(), c, Arc::new(common::TestKeys)).unwrap()
    };
    good.archive_pass(false).unwrap();
    let held = e.log_floor_of(sprig_engine::UnitId(7), "backup").unwrap();
    commit(&e, &[(key(7, 2), Some("b".into()))]);
    settle(&e, 7);
    // ...then a pass whose storage can't open must not move it.
    let p = svc.archive_pass(true).unwrap();
    assert_eq!(p.failed.len(), 1, "{p:?}");
    assert_eq!(
        e.log_floor_of(sprig_engine::UnitId(7), "backup"),
        Some(held)
    );
}

#[test]
fn a_backup_holds_the_log_so_the_archive_follows_it() {
    let d = Dir::new("holdbk");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    for i in 0..50u32 {
        commit(&e, &[(key(7, i), Some(format!("a{i}")))]);
    }
    settle(&e, 7);
    // No archive pass has seen tenant 7 yet; the backup holds its log.
    svc.backup_now(7).unwrap();
    for i in 0..50u32 {
        commit(&e, &[(key(7, i), Some(format!("b{i}")))]);
    }
    settle(&e, 7);
    // A checkpoint would trim an unheld log here.
    e.checkpoint().unwrap();
    let r = svc.verify(7, None).unwrap();
    let live = e.unit_checksum(sprig_engine::UnitId(7)).unwrap();
    assert_eq!(
        (r.entries, r.bytes, r.crc),
        (live.entries, live.bytes, live.crc)
    );
    assert!(r.replayed >= 50, "{r:?}");
}

#[test]
fn the_daily_run_skips_a_tenant_with_no_keys_left() {
    let d = Dir::new("daily");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let svc = BackupService::open(e.clone(), backup_cfg(&d), Arc::new(common::TestKeys)).unwrap();
    commit(
        &e,
        &[(key(7, 1), Some("a".into())), (key(8, 1), Some("b".into()))],
    );
    settle(&e, 7);
    settle(&e, 8);
    // Tenant 8's keys are removed outright (as a tenant drop does).
    let r = sprig_engine::keys::tenant_range(8);
    let mut b = sprig_engine::store::StoreBatch::new();
    b.delete_range(
        sprig_engine::store::Cf::Data,
        r.start.clone(),
        r.end.clone().unwrap(),
    );
    e.store()
        .write(&b, sprig_engine::store::WalMode::Sync)
        .unwrap();
    let done = svc.daily(sprig_backup::now_ms());
    assert!(done.contains(&7) && !done.contains(&8), "{done:?}");
    assert_eq!(svc.list(7).unwrap().len(), 1);
    assert!(svc.list(8).unwrap().is_empty());
}
