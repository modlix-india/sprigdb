//! Backups on S3-compatible storage, live. Ignored unless run with
//! `NBACKUP_S3=s3://bucket/prefix?endpoint=http://127.0.0.1:PORT` and the
//! credentials in `SPRIGDB_BACKUP_S3_ACCESS_KEY` /
//! `SPRIGDB_BACKUP_S3_SECRET_KEY` (a `MinIO` container, for example):
//! `cargo test -p sprig-backup --test s3_live -- --ignored`.

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names
)]

mod common;

use std::sync::Arc;

use common::{Dir, backup_cfg, commit, get, key, open_engine, settle};
use sprig_backup::BackupTarget;
use sprig_backup::s3::S3Target;
use sprig_backup::service::BackupService;
use sprig_engine::{StoreKind, UnitId};

fn location() -> String {
    std::env::var("NBACKUP_S3").expect("NBACKUP_S3=s3://bucket/prefix?endpoint=...")
}

fn bucket() -> S3Target {
    let loc = location();
    let t = S3Target::from_url(loc.strip_prefix("s3://").unwrap()).unwrap();
    t.create_bucket().unwrap();
    t
}

#[test]
#[ignore = "needs S3-compatible storage (NBACKUP_S3)"]
fn s3_objects_round_trip() {
    let t = bucket();
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    t.put("rt/a b+c&d.bin", &big).unwrap();
    t.put("rt/x/y", b"small").unwrap();
    assert_eq!(t.get("rt/a b+c&d.bin").unwrap().unwrap(), big);
    assert!(t.get("rt/missing").unwrap().is_none());
    let l = t.list("rt/").unwrap();
    assert!(
        l.contains(&"rt/a b+c&d.bin".to_owned()) && l.contains(&"rt/x/y".to_owned()),
        "{l:?}"
    );
    // More than one page of listing.
    for i in 0..1_100u32 {
        t.put(&format!("page/{i:05}"), b"p").unwrap();
    }
    assert_eq!(t.list("page/").unwrap().len(), 1_100);
    for n in t
        .list("page/")
        .unwrap()
        .into_iter()
        .chain(t.list("rt/").unwrap())
    {
        t.delete(&n).unwrap();
    }
    assert!(t.list("rt/").unwrap().is_empty());
    t.delete("rt/never-there").unwrap();
}

#[test]
#[ignore = "needs S3-compatible storage (NBACKUP_S3)"]
fn backup_and_restore_through_s3() {
    let _ = bucket();
    let d = Dir::new("s3");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    let mut cfg = backup_cfg(&d);
    cfg.default_location = location();
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    // A tenant id of this run's own, so reruns don't meet old objects.
    let tenant = 1_000 + u64::from(std::process::id() % 50_000);
    let into = tenant + 100_000;
    for i in 0..3_000u32 {
        commit(
            &e,
            &[(key(tenant, i), Some(format!("{i}-{}", "x".repeat(200))))],
        );
    }
    settle(&e, tenant);
    let b = svc.backup_now(tenant).unwrap();
    assert!(b.manifest.total_bytes > 600_000, "{:?}", b.manifest);
    for i in 0..100u32 {
        commit(&e, &[(key(tenant, i), None)]);
    }
    settle(&e, tenant);
    let r = svc.restore(tenant, None, into, false).unwrap();
    assert!(r.replayed >= 100, "{r:?}");
    let live = e.unit_checksum(UnitId(tenant)).unwrap();
    assert_eq!(e.unit_checksum(UnitId(into)).unwrap().entries, live.entries);
    assert!(get(&e, &key(into, 50)).is_none());
    assert_eq!(get(&e, &key(into, 2_000)), get(&e, &key(tenant, 2_000)));
    let v = svc.verify(tenant, None).unwrap();
    assert_eq!(
        (v.entries, v.bytes, v.crc),
        (live.entries, live.bytes, live.crc)
    );
    // Clean up this run's objects.
    let t = svc.target(tenant).unwrap();
    for p in [format!("t{tenant}/"), format!("t{into}/")] {
        for n in t.list(&p).unwrap() {
            t.delete(&n).unwrap();
        }
    }
}
