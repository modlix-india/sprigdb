use std::path::Path;
use std::time::Duration;

use sprig_engine::keys::{self, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, StoreKind};

use super::*;
use crate::seg::testing::TempDir;

pub(crate) fn engine(dir: &Path) -> Engine {
    let mut c = EngineConfig::new(dir.join("engine"));
    c.store = StoreKind::Rocks;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match Engine::open(c.clone()) {
            Ok(e) => return e,
            Err(e)
                if e.message.contains("lock hold by current process")
                    && std::time::Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn cfg() -> ChangeConfig {
    ChangeConfig {
        segment_bytes: 2048,
        index_interval: 256,
        ..ChangeConfig::default()
    }
}

const T: u64 = 7;

fn row(id: u32) -> Vec<u8> {
    keys::encode(T, 0, 5, kind::PRIMARY, 0, &id.to_be_bytes(), None)
}

/// Commits puts (`Some`) and deletes (`None`) of rows, with a hint on
/// the first; returns the commit timestamp.
fn commit(e: &Engine, ops: &[(u32, Option<&[u8]>)]) -> u64 {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    for (i, (id, v)) in ops.iter().enumerate() {
        match v {
            Some(v) => t.put(row(*id), v.to_vec()).unwrap(),
            None => t.delete(row(*id)).unwrap(),
        }
        if i == 0 {
            t.add_note_for_tenant(T, encode_hint(&row(*id), HintOp::Replace, b"why"))
                .unwrap();
        }
    }
    // An index entry and a key of another kind are not changes.
    t.put(
        keys::encode(T, 0, 5, kind::SECONDARY, 1, b"ix", None),
        b"x".to_vec(),
    )
    .unwrap();
    let c = t.commit().unwrap();
    c.wait().unwrap();
    settle(e);
    c.cts().0
}

/// Waits until the unit's durable entries are applied (the change log
/// writes on the apply step).
fn settle(e: &Engine) {
    let u = UnitId(unit());
    let want = e.consensus().durable_index(u);
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while e.last_applied_of(u) < want && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn unit() -> u64 {
    Engine::unit_of_tenant(T).0
}

fn read_all(log: &ChangeLog, after: Pos, max: usize) -> (Vec<Change>, Pos) {
    let mut r = log.reader(unit(), after, None).unwrap();
    let mut out = Vec::new();
    loop {
        let p = r.read(max).unwrap();
        out.extend(p.changes);
        if p.at_end {
            return (out, p.pos);
        }
    }
}

#[test]
fn records_hints_images_and_pages() {
    let d = TempDir::new("chg-basic");
    let e = engine(d.path());
    let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
    let start = log.head(unit()).0;
    let c1 = commit(&e, &[(1, Some(b"a")), (2, Some(b"b"))]);
    let c2 = commit(&e, &[(1, Some(b"a2"))]);
    let c3 = commit(&e, &[(2, None)]);
    let (all, end) = read_all(&log, start, 1 << 20);
    assert_eq!(all.len(), 4, "{all:?}");
    assert_eq!(
        all.iter()
            .map(|c| (c.cts, c.value.clone(), c.last))
            .collect::<Vec<_>>(),
        vec![
            (c1, Some(b"a".to_vec()), false),
            (c1, Some(b"b".to_vec()), true),
            (c2, Some(b"a2".to_vec()), true),
            (c3, None, true),
        ]
    );
    assert_eq!(all[0].hint, Some((HintOp::Replace, b"why".to_vec())));
    assert_eq!(all[1].hint, None);
    assert_eq!(all[0].key, row(1));
    // Before-images from MVCC.
    assert_eq!(log.before_image(&all[0]).unwrap(), None);
    assert_eq!(log.before_image(&all[2]).unwrap(), Some(b"a".to_vec()));
    assert_eq!(log.before_image(&all[3]).unwrap(), Some(b"b".to_vec()));
    assert_eq!(log.resolved(unit()), c3);
    assert_eq!(end, log.head(unit()).0);
    // One change per page, resumed from each place.
    let mut at = start;
    let mut got = Vec::new();
    loop {
        let mut r = log.reader(unit(), at, None).unwrap();
        let p = r.read(1).unwrap();
        got.extend(p.changes);
        at = p.pos;
        if p.at_end {
            break;
        }
    }
    assert_eq!(got, all);
    // After the last: nothing.
    assert!(read_all(&log, end, 1 << 20).0.is_empty());
    // After the first record of a transaction: its rest.
    let (rest, _) = read_all(&log, all[0].pos(), 1 << 20);
    assert_eq!(rest, all[1..].to_vec());
}

#[test]
fn survives_restarts_without_gaps_or_repeats() {
    let d = TempDir::new("chg-restart");
    let mut want = Vec::new();
    let start;
    {
        let e = engine(d.path());
        let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
        start = log.head(unit()).0;
        for i in 0..40u32 {
            want.push(commit(&e, &[(i, Some(&[7u8; 100][..]))]));
        }
        log.flush().unwrap();
        // Not flushed: replayed from the floor on the next open.
        for i in 40..60u32 {
            want.push(commit(&e, &[(i, Some(&[8u8; 100][..]))]));
        }
    }
    for round in 0..3 {
        let e = engine(d.path());
        let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
        let (all, _) = read_all(&log, start, 700);
        assert_eq!(
            all.iter().map(|c| c.cts).collect::<Vec<_>>(),
            want,
            "round {round}"
        );
        for w in all.windows(2) {
            assert!(w[0].pos() < w[1].pos());
        }
        want.push(commit(&e, &[(1000 + round, Some(b"z"))]));
        if round == 1 {
            log.flush().unwrap();
        }
    }
}

#[test]
fn retention_drops_segments_and_reports_lost_history() {
    let d = TempDir::new("chg-retain");
    let e = engine(d.path());
    let log = ChangeLog::open(
        &e,
        d.path().join("changes"),
        ChangeConfig {
            retention_ms: 0,
            ..cfg()
        },
    )
    .unwrap();
    let start = log.head(unit()).0;
    for i in 0..60u32 {
        commit(&e, &[(i, Some(&[1u8; 200][..]))]);
    }
    std::thread::sleep(Duration::from_millis(5));
    log.maintain().unwrap();
    let e1 = log.reader(unit(), start, None).unwrap_err();
    assert!(is_history_lost(&e1), "{e1}");
    let head = log.head(unit()).0;
    let mut r = log.reader(unit(), head, None).unwrap();
    commit(&e, &[(99, Some(b"new"))]);
    let p = r.read(1 << 20).unwrap();
    assert_eq!(p.changes.len(), 1);
    assert_eq!(p.changes[0].value.as_deref(), Some(&b"new"[..]));
}

#[test]
fn snapshot_part_round_trip() {
    use sprig_engine::snapshot::UnitSnapshotPart;
    let d = TempDir::new("chg-snap");
    let e = engine(d.path());
    let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
    let start = log.head(unit()).0;
    for i in 0..30u32 {
        commit(&e, &[(i, Some(&[2u8; 150][..]))]);
    }
    let (all, end) = read_all(&log, start, 1 << 20);
    let part = SnapshotPart(Arc::downgrade(&log.inner));
    let mut bytes = Vec::new();
    part.export(UnitId(unit()), end.index, &mut bytes).unwrap();
    let d2 = TempDir::new("chg-snap2");
    let e2 = engine(d2.path());
    let log2 = ChangeLog::open(&e2, d2.path().join("changes"), cfg()).unwrap();
    let part2 = SnapshotPart(Arc::downgrade(&log2.inner));
    part2
        .ingest(UnitId(unit()), end.index, &mut bytes.as_slice())
        .unwrap();
    let (all2, _) = read_all(&log2, start, 1 << 20);
    assert_eq!(all2, all);
    assert_eq!(log2.resolved(unit()), log.resolved(unit()));
}

#[test]
fn waiting_readers_are_woken() {
    let d = TempDir::new("chg-watch");
    let e = engine(d.path());
    let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
    let mut w = log.watch(unit());
    let before = *w.borrow_and_update();
    commit(&e, &[(1, Some(b"x"))]);
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while !w.has_changed().unwrap() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(w.has_changed().unwrap());
    assert!(*w.borrow() > before);
}

#[test]
fn places_by_time_skip_older_segments() {
    let d = TempDir::new("chg-time");
    let e = engine(d.path());
    let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
    let mut stamps = Vec::new();
    for i in 0..60u32 {
        stamps.push(commit(&e, &[(i, Some(&[3u8; 200][..]))]));
    }
    let t = stamps[45];
    let p = log.place_at(unit(), t);
    assert!(p > log.oldest(unit()), "the place skips old segments");
    // Every change at or after `t` is after the place.
    let (all, _) = read_all(&log, p, 1 << 20);
    let got: Vec<u64> = all.iter().map(|c| c.cts).filter(|c| *c >= t).collect();
    assert_eq!(got, stamps[45..].to_vec());
    assert_eq!(log.place_at(unit(), 0), log.oldest(unit()));
}

#[test]
fn consumer_holds_outlive_a_restart() {
    let d = TempDir::new("chg-holds");
    let at;
    {
        let e = engine(d.path());
        let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
        at = commit(&e, &[(1, Some(b"a"))]);
        let later = (at >> 16) + 120_000;
        log.hold_for("trigger/1/2", Some(at));
        // Forward only.
        log.hold_for("trigger/1/2", Some(at - 5));
        let wm = e.txn_shared().gc_watermark(later);
        assert!(wm.0 < at, "held below {at}: {wm:?}");
    }
    {
        let e = engine(d.path());
        let log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
        let later = (at >> 16) + 120_000;
        // Held again at open, before anything collects.
        assert!(e.txn_shared().gc_watermark(later).0 < at);
        log.hold_for("trigger/1/2", None);
    }
    let e = engine(d.path());
    let _log = ChangeLog::open(&e, d.path().join("changes"), cfg()).unwrap();
    let later = (at >> 16) + 120_000;
    assert!(e.txn_shared().gc_watermark(later).0 >= at);
}
