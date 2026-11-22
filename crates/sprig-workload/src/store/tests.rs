//! The workload store on the in-memory record store: flushes, windows,
//! the rollup and day rows, retention, samples and shape rewrites.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::SystemRandom;
use sprig_catalog::records::mem::MemRecords;
use sprig_common::ids::{DbId, TenantId};
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::fingerprint::{SqlCtx, SqlFingerprinter};
use crate::keyring::MasterKeyring;
use crate::recorder::{ExecSummary, FlushRow, Recorder, RecorderConfig};
use crate::samples::{CaptureMeta, SampleContext, SamplePayload, seal};
use crate::shape::NoResolver;
use crate::window::SampledLiterals;

const T: TenantId = TenantId(1);

/// Runs `$body` with `$t` a transaction on `$st`, then commits.
macro_rules! tx {
    ($st:expr, |$t:ident| $body:expr) => {{
        let mut b = $st.begin(T).await.unwrap();
        let $t: &mut dyn RecordTx = &mut *b;
        let r = $body;
        b.commit().await.unwrap();
        r
    }};
}

fn hour() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now - now % 3600
}

fn row(fp: FingerprintId, bucket: u64, part: u16, runs: u64, us: u64) -> FlushRow {
    let mut stats = Stats::default();
    for _ in 0..runs {
        stats.exec_count += 1;
        stats.total_us += us;
        stats.max_us = stats.max_us.max(us);
        stats.latency_hist.record(us);
        stats.rows_returned += 2;
    }
    stats.examined_samples = 1;
    stats.rows_examined = 5000;
    stats.returned_sampled = 2;
    stats.scan_runs = 1;
    stats.scan_sessions.insert(bucket ^ u64::from(part));
    FlushRow {
        tenant: TenantId(1),
        fingerprint: fp,
        bucket_start: bucket,
        part,
        stats,
        last_seen: UNIX_EPOCH + Duration::from_secs(bucket + 10),
    }
}

fn new_fp(sql: &str) -> (crate::fingerprint::Fingerprint, NewFingerprint) {
    let f = SqlFingerprinter::default();
    let ctx = SqlCtx::new(Persona::MariaDb).current_db("shop", DbId(1));
    let fp = f.sql(TenantId(1), sql.as_bytes(), &ctx, &NoResolver);
    let clock = std::sync::Arc::new(sprig_common::time::SystemClock);
    let r = Recorder::new(RecorderConfig::default(), clock);
    r.record(TenantId(1), &fp, &ExecSummary::default());
    let n = r.drain_new().pop().unwrap();
    (fp, n)
}

async fn span(st: &MemRecords, from: u64, to: u64) -> Vec<FpWindowStats> {
    tx!(st, |t| WorkloadTables::window(
        t,
        T,
        UNIX_EPOCH + Duration::from_secs(from),
        UNIX_EPOCH + Duration::from_secs(to),
    )
    .await
    .unwrap())
}

async fn count(st: &MemRecords, table: RowTable) -> usize {
    tx!(st, |t| {
        let p = table.prefix();
        scan_all(t, &p, &prefix_end(&p)).await.unwrap().len()
    })
}

#[tokio::test]
async fn flushes_are_idempotent_and_windows_merge_gateways_and_buckets() {
    let st = MemRecords::new();
    let (fp, n) = new_fp("SELECT * FROM orders WHERE customer_id = 42");
    assert_eq!(
        tx!(st, |t| WorkloadTables::insert_fingerprints(
            t,
            std::slice::from_ref(&n)
        )
        .await
        .unwrap()),
        1
    );
    assert_eq!(
        tx!(st, |t| WorkloadTables::insert_fingerprints(t, &[n])
            .await
            .unwrap()),
        0
    );
    let h = hour();
    // Two gateways in this hour, one in the last; gateway 1 flushes
    // twice (cumulative values: the second replaces the first).
    for (g, r) in [
        (1, row(fp.id, h, 0, 3, 1000)),
        (1, row(fp.id, h, 0, 5, 1000)),
        (2, row(fp.id, h, 0, 4, 200_000)),
        (1, row(fp.id, h - 3600, 0, 1, 1000)),
    ] {
        tx!(st, |t| WorkloadTables::replace_stats(t, g, &[r])
            .await
            .unwrap());
    }
    let w = span(&st, h - 3600, h + 3600).await;
    assert_eq!(w.len(), 1);
    let w = &w[0];
    assert_eq!(w.id, fp.id);
    assert_eq!(&*w.text, &*fp.text);
    assert_eq!(w.runs(), 10);
    assert_eq!(w.stats.max_us, 200_000);
    assert!(w.p95_ms() >= 200.0, "{}", w.p95_ms());
    assert_eq!(w.stats.examined_samples, 3);
    assert_eq!(w.stats.scan_runs, 3);
    assert_eq!(w.stats.scan_sessions.len(), 2, "union of session sets");
    // Only this hour.
    assert_eq!(span(&st, h, h + 3600).await[0].runs(), 9);
    // last_seen never moves back (first_seen was now).
    let fr = tx!(st, |t| WorkloadTables::fingerprints(t, &[fp.id])
        .await
        .unwrap());
    assert!(fr[0].last_seen >= UNIX_EPOCH + Duration::from_secs(h + 10));
    assert!(fr[0].last_seen >= fr[0].first_seen);
    assert_eq!(fr[0].stmt_type, "SELECT");
    // The series of the fingerprint, per bucket.
    let series = tx!(st, |t| WorkloadTables::series(t, fp.id, h - 3600, h + 3600)
        .await
        .unwrap());
    assert_eq!(
        series
            .iter()
            .map(|(b, s)| (*b, s.exec_count))
            .collect::<Vec<_>>(),
        vec![(h - 3600, 1), (h, 9)]
    );
    let counts = tx!(st, |t| WorkloadTables::statement_counts(t).await.unwrap());
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].3, 1);
    let sums = tx!(st, |t| WorkloadTables::recent_sums(t, 0, 0).await.unwrap());
    assert_eq!(sums.map(|s| (s.0, s.3)), Some((9, h)));
}

#[tokio::test]
async fn rollup_merges_closed_buckets_once_and_keeps_windows_equal() {
    let st = MemRecords::new();
    let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 1");
    tx!(st, |t| WorkloadTables::insert_fingerprints(t, &[n])
        .await
        .unwrap());
    let h = hour();
    let old = h - 2 * 3600;
    for (g, part) in [(1, 0), (1, 1), (2, 0)] {
        tx!(st, |t| WorkloadTables::replace_stats(
            t,
            g,
            &[row(fp.id, old, part, 2, 1000)]
        )
        .await
        .unwrap());
    }
    // The open bucket isn't touched.
    tx!(st, |t| WorkloadTables::replace_stats(
        t,
        1,
        &[row(fp.id, h, 0, 1, 1000)]
    )
    .await
    .unwrap());
    let before = span(&st, old, h + 3600).await;
    let rep = WorkloadTables::rollup(&st, T, h + 60, &Retention::default())
        .await
        .unwrap();
    assert_eq!((rep.merged, rep.merged_rows), (1, 3));
    assert_eq!(count(&st, STATS).await, 2);
    let after = span(&st, old, h + 3600).await;
    assert_eq!(before[0].stats, after[0].stats);
    // A late partial row of the merged bucket merges into the same row.
    tx!(st, |t| WorkloadTables::replace_stats(
        t,
        3,
        &[row(fp.id, old, 0, 1, 1000)]
    )
    .await
    .unwrap());
    let rep = WorkloadTables::rollup(&st, T, h + 60, &Retention::default())
        .await
        .unwrap();
    assert_eq!((rep.merged, rep.merged_rows), (1, 1));
    let again = WorkloadTables::rollup(&st, T, h + 60, &Retention::default())
        .await
        .unwrap();
    assert_eq!(again.merged, 0, "idempotent");
    assert_eq!(count(&st, STATS).await, 2);
    assert_eq!(
        span(&st, old, h + 3600).await[0].runs(),
        before[0].runs() + 1
    );
}

#[tokio::test]
async fn old_hours_become_day_rows_and_retention_deletes() {
    let st = MemRecords::new();
    let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 2");
    tx!(st, |t| WorkloadTables::insert_fingerprints(t, &[n])
        .await
        .unwrap());
    let h = hour();
    let day = (h - 10 * 86_400) - (h - 10 * 86_400) % 86_400;
    for i in 0..5 {
        tx!(st, |t| WorkloadTables::replace_stats(
            t,
            1,
            &[row(fp.id, day + i * 3600, 0, 2, 1000)]
        )
        .await
        .unwrap());
    }
    // Past retention.
    tx!(st, |t| WorkloadTables::replace_stats(
        t,
        1,
        &[row(fp.id, h - 40 * 86_400, 0, 2, 1000)]
    )
    .await
    .unwrap());
    let rep = WorkloadTables::rollup(&st, T, h, &Retention::default())
        .await
        .unwrap();
    assert_eq!(rep.merged, 6, "partials of gateway 1 first");
    assert_eq!(rep.days, 6);
    assert_eq!(span(&st, day, day + 86_400).await[0].runs(), 10);
    let parts: Vec<u16> = tx!(st, |t| {
        let p = STATS.prefix();
        scan_all(t, &p, &prefix_end(&p))
            .await
            .unwrap()
            .iter()
            .map(|(k, _)| stat_key(&k[p.len()..]).unwrap().3)
            .collect()
    });
    assert_eq!(parts, vec![DAY_PART, DAY_PART]);
    // Rerun: day rows stay as they are.
    let rep = WorkloadTables::rollup(&st, T, h, &Retention::default())
        .await
        .unwrap();
    assert_eq!((rep.merged, rep.days), (0, 0));
    let rep = WorkloadTables::retain(&st, T, h, &Retention::default())
        .await
        .unwrap();
    assert_eq!(rep.expired_stats, 1);
    assert_eq!(rep.expired_fingerprints, 0, "seen recently");
    let rep = WorkloadTables::retain(&st, T, h + 60 * 86_400, &Retention::default())
        .await
        .unwrap();
    assert_eq!((rep.expired_stats, rep.expired_fingerprints), (1, 1));
}

#[tokio::test]
async fn samples_are_stored_sealed_capped_and_opened() {
    let st = MemRecords::new();
    let keys = MasterKeyring::new([(1, [3u8; 32])]).unwrap();
    let rng = SystemRandom::new();
    let (fp, _) = new_fp("SELECT * FROM orders WHERE customer_id = 'secret-value'");
    let sql = b"SELECT * FROM orders WHERE customer_id = 'secret-value'";
    let payload = SamplePayload {
        literals: SampledLiterals::from_execution(sql, &fp, &[]),
        context: SampleContext {
            database: Some("shop".into()),
            db_id: DbId(1),
            ..SampleContext::default()
        },
    };
    let now = SystemTime::now();
    let mut rows = Vec::new();
    for i in 0..60u64 {
        let mut s = seal(
            &keys,
            &rng,
            T,
            fp.id,
            &payload,
            CaptureMeta {
                captured_at: now - Duration::from_secs(i),
                latency_us: 1000,
                rows_examined: Some(10),
                rows_returned: 1,
                expires_at: now + Duration::from_secs(if i == 59 { 0 } else { 7 * 86_400 }),
            },
        )
        .unwrap();
        s.sample_id = i + 1;
        rows.push(s);
    }
    tx!(st, |t| WorkloadTables::replace_samples(t, &rows)
        .await
        .unwrap());
    // Stored sealed: the value isn't in the records.
    let stored = tx!(st, |t| {
        let p = SAMPLES.prefix();
        scan_all(t, &p, &prefix_end(&p)).await.unwrap()
    });
    assert!(
        !stored
            .iter()
            .any(|(_, v)| v.windows(6).any(|w| w == b"secret"))
    );
    let got = tx!(st, |t| WorkloadTables::samples(t, fp.id, 5).await.unwrap());
    assert_eq!(got.len(), 5);
    assert_eq!(got[0].sample_id, 1, "newest first");
    let (a, b) = (&got[0], &rows[0]);
    assert_eq!(
        (
            a.nonce,
            &a.payload,
            a.key_version,
            a.rows_examined,
            a.latency_us
        ),
        (
            b.nonce,
            &b.payload,
            b.key_version,
            b.rows_examined,
            b.latency_us
        )
    );
    let skew = |x: SystemTime, y: SystemTime| {
        x.duration_since(y)
            .or_else(|_| y.duration_since(x))
            .unwrap()
    };
    assert!(skew(a.captured_at, b.captured_at) < Duration::from_micros(1));
    assert!(skew(a.expires_at, b.expires_at) < Duration::from_secs(1));
    let opened = tx!(st, |t| WorkloadTables::open_samples(t, &keys, T, fp.id, 2)
        .await
        .unwrap());
    assert_eq!(opened.len(), 2);
    assert_eq!(opened[0].payload, payload);
    // Another tenant's keys open nothing.
    let none = tx!(st, |t| WorkloadTables::open_samples(
        t,
        &keys,
        TenantId(2),
        fp.id,
        2
    )
    .await
    .unwrap());
    assert!(none.is_empty());
    tokio::time::sleep(Duration::from_secs(1)).await;
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rep = WorkloadTables::retain(&st, T, secs, &Retention::default())
        .await
        .unwrap();
    assert_eq!(
        rep.expired_samples,
        1 + 9,
        "one expired, then the cap of 50"
    );
    assert_eq!(count(&st, SAMPLES).await, 50);
    assert_eq!(count(&st, SEXP).await, 50);
}

#[tokio::test]
async fn the_sample_cap_keeps_the_newest_of_each_fingerprint() {
    let st = MemRecords::new();
    let keys = MasterKeyring::new([(1, [3u8; 32])]).unwrap();
    let rng = SystemRandom::new();
    let now = SystemTime::now();
    let mut rows = Vec::new();
    for (f, sql) in ["SELECT * FROM a WHERE x = 1", "SELECT * FROM b WHERE y = 2"]
        .into_iter()
        .enumerate()
    {
        let (fp, _) = new_fp(sql);
        let payload = SamplePayload {
            literals: SampledLiterals::from_execution(sql.as_bytes(), &fp, &[]),
            context: SampleContext::default(),
        };
        for i in 0..5u64 {
            let mut s = seal(
                &keys,
                &rng,
                T,
                fp.id,
                &payload,
                CaptureMeta {
                    captured_at: now - Duration::from_secs(i),
                    latency_us: 1000,
                    rows_examined: Some(1),
                    rows_returned: 1,
                    expires_at: now + Duration::from_secs(86_400),
                },
            )
            .unwrap();
            s.sample_id = f as u64 * 100 + i + 1;
            rows.push(s);
        }
    }
    tx!(st, |t| WorkloadTables::replace_samples(t, &rows)
        .await
        .unwrap());
    let secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let r = Retention {
        max_samples_per_fingerprint: 2,
        ..Retention::default()
    };
    let rep = WorkloadTables::retain(&st, T, secs, &r).await.unwrap();
    assert_eq!(rep.expired_samples, 6, "three over the cap of each");
    // The newest two of each fingerprint stay.
    let mut left = Vec::new();
    for r in &rows {
        let pk = Pk::new().u64(r.fingerprint.0).u64(r.sample_id);
        if tx!(st, |t| t.get(&SAMPLES.key(&pk)).await.unwrap()).is_some() {
            left.push(r.sample_id);
        }
    }
    left.sort_unstable();
    assert_eq!(left, vec![1, 2, 101, 102]);
}

#[tokio::test]
async fn maintain_rolls_up_and_retains_through_the_store() {
    let st = MemRecords::new();
    let (fp, n) = new_fp("SELECT * FROM orders WHERE id = 3");
    tx!(st, |t| WorkloadTables::insert_fingerprints(t, &[n])
        .await
        .unwrap());
    let h = hour();
    for g in [1, 2] {
        tx!(st, |t| WorkloadTables::replace_stats(
            t,
            g,
            &[row(fp.id, h - 7200, 0, 2, 1000)]
        )
        .await
        .unwrap());
    }
    let rep = maintain(
        &st,
        T,
        UNIX_EPOCH + Duration::from_secs(h + 60),
        &Retention::default(),
    )
    .await
    .unwrap();
    assert_eq!((rep.merged, rep.merged_rows), (1, 2));
    let records: std::sync::Arc<dyn RecordStore> = std::sync::Arc::new(st);
    let store = CatalogWorkloadStore::new(std::sync::Arc::clone(&records));
    let w = store
        .window(
            T,
            UNIX_EPOCH + Duration::from_secs(h - 7200),
            UNIX_EPOCH + Duration::from_secs(h),
        )
        .await
        .unwrap();
    assert_eq!(w[0].runs(), 4);
    // A tenant with no workload records reads as empty.
    let empty = store
        .window(TenantId(2), UNIX_EPOCH, UNIX_EPOCH + Duration::from_secs(h))
        .await
        .unwrap();
    assert!(empty.is_empty());
}

/// A fingerprint recorded with an older shape (shape version 1,
/// `json_path` without `func` and `cmp`) is rewritten with this build's
/// shape the next time a gateway records the statement; one of the current
/// or a newer version is left alone.
#[tokio::test]
async fn an_old_shape_version_row_is_rewritten() {
    let st = MemRecords::new();
    let (fp, n) = new_fp("SELECT k FROM db0 WHERE JSON_VALUE(v, '$.email') = 'a@b.c'");
    assert!(!fp.shape.all_unsargable(), "{}", fp.shape.to_json());
    // The row as a build before shape version 2 wrote it.
    let mut old: serde_json::Value = serde_json::from_str(&fp.shape.to_json()).unwrap();
    old["v"] = 1.into();
    for p in old["predicates"].as_array_mut().unwrap() {
        let p = p.as_object_mut().unwrap();
        assert_eq!(p["op"], "json_path");
        p.remove("func");
        p.remove("cmp");
    }
    tx!(st, |t| WorkloadTables::insert_fingerprints(
        t,
        std::slice::from_ref(&n)
    )
    .await
    .unwrap());
    let pk = Pk::new().u64(fp.id.0);
    let set = |shape: String, version: u16, flags: Option<&str>| {
        let st = &st;
        let pk = pk.clone();
        let flags = flags.map(str::to_owned);
        async move {
            tx!(st, |t| {
                let mut r = FPS.get::<FpRecord>(t, &pk).await.unwrap().unwrap();
                r.shape = shape;
                r.shape_version = version;
                if let Some(f) = flags {
                    r.flags = f;
                }
                FPS.put(t, &pk, &r).await.unwrap();
            });
        }
    };
    set(old.to_string(), 1, Some("static")).await;
    let before = tx!(st, |t| WorkloadTables::fingerprints(t, &[fp.id])
        .await
        .unwrap());
    assert!(
        before[0].shape.all_unsargable(),
        "the old row: no field column"
    );
    // The statement is seen again: the row is rewritten (2 affected).
    assert_eq!(
        tx!(st, |t| WorkloadTables::insert_fingerprints(
            t,
            std::slice::from_ref(&n)
        )
        .await
        .unwrap()),
        2
    );
    let after = tx!(st, |t| WorkloadTables::fingerprints(t, &[fp.id])
        .await
        .unwrap());
    assert_eq!(*after[0].shape, *fp.shape);
    assert!(after[0].shape.predicates[0].op.is_field_sargable());
    assert_eq!(after[0].first_seen, before[0].first_seen);
    assert_eq!(after[0].flags, "static", "flags are kept");
    let v = tx!(st, |t| FPS
        .get::<FpRecord>(t, &pk)
        .await
        .unwrap()
        .unwrap()
        .shape_version);
    assert_eq!(v, SHAPE_VERSION);
    // Same version: nothing changes.
    assert_eq!(
        tx!(st, |t| WorkloadTables::insert_fingerprints(
            t,
            std::slice::from_ref(&n)
        )
        .await
        .unwrap()),
        0
    );
    // A newer build's row isn't taken back.
    set("{}".to_owned(), SHAPE_VERSION + 1, None).await;
    assert_eq!(
        tx!(st, |t| WorkloadTables::insert_fingerprints(t, &[n])
            .await
            .unwrap()),
        0
    );
    let s = tx!(st, |t| FPS
        .get::<FpRecord>(t, &pk)
        .await
        .unwrap()
        .unwrap()
        .shape);
    assert_eq!(s, "{}");
}

/// Batches: more (fingerprint, bucket) keys than one batch holds, in one
/// bucket and across buckets; every partial row is merged once and the
/// totals stay.
#[tokio::test]
async fn rollup_batches_cover_every_key_once() {
    let st = MemRecords::new();
    let h = hour();
    let buckets = [h - 4 * 3600, h - 3 * 3600, h - 2 * 3600];
    let fps: Vec<FingerprintId> = (1..=700u64).map(sprig_common::ids::Fingerprint).collect();
    for (i, b) in buckets.iter().enumerate() {
        // The first bucket holds every fingerprint (more than a batch).
        let n = if i == 0 { fps.len() } else { 300 };
        for g in [1, 2] {
            let rows: Vec<FlushRow> = fps[..n].iter().map(|f| row(*f, *b, 0, 1, 1000)).collect();
            tx!(st, |t| WorkloadTables::replace_stats(t, g, &rows).await.unwrap());
        }
    }
    let total = |m: &BTreeMap<u64, Stats>| m.values().map(|s| s.exec_count).sum::<u64>();
    let before = tx!(st, |t| WorkloadTables::merged_stats(t, 0, h).await.unwrap());
    let rep = WorkloadTables::rollup(&st, T, h + 60, &Retention::default())
        .await
        .unwrap();
    assert_eq!(rep.merged, 700 + 300 + 300);
    assert_eq!(rep.merged_rows, 2 * (700 + 300 + 300));
    let after = tx!(st, |t| WorkloadTables::merged_stats(t, 0, h).await.unwrap());
    assert_eq!(total(&before), total(&after));
    assert_eq!(before, after);
    assert_eq!(count(&st, STATS).await, 700 + 300 + 300);
    let again = WorkloadTables::rollup(&st, T, h + 60, &Retention::default())
        .await
        .unwrap();
    assert_eq!(again.merged, 0);
}
