//! Planner statistics on the engine: `ANALYZE TABLE`'s collection, the
//! estimates that read it, and skewed data getting the plan its values
//! ask for. The tables use object ids no other test uses (the statistics
//! cache is per process).

#![allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]

use super::*;
use crate::engine::stats::{self, collect};

const SKEW: &str = "CREATE TABLE `skew` (\n  `id` int(11) NOT NULL,\n  `st` int(11) NOT NULL,\n  `v` int(11) NOT NULL,\n  `tag` varchar(10) NOT NULL DEFAULT '',\n  `oid` int(11) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ks` (`st`),\n  KEY `ko` (`oid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const OTHER: &str = "CREATE TABLE `other` (\n  `id` int(11) NOT NULL,\n  `skew_id` int(11) NOT NULL,\n  `flag` int(11) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `kso` (`skew_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

/// Rows of `skew`: more in an optimized build, where the timings mean
/// something.
const SKEW_ROWS: i64 = if cfg!(debug_assertions) {
    30_000
} else {
    300_000
};
const OTHER_ROWS: i64 = 25_000;

fn fake() -> Fake {
    let mut f = Fake::new();
    f.tables.insert(
        "skew".into(),
        Arc::new(table::parse(ObjectId(901), 1, DB, SKEW)),
    );
    f.tables.insert(
        "other".into(),
        Arc::new(table::parse(ObjectId(902), 1, DB, OTHER)),
    );
    f
}

fn def(f: &Fake, name: &str) -> Arc<table::TableDef> {
    f.tables[name].as_ref().as_ref().unwrap().clone()
}

/// `skew`: `st` is 1 on 99% of rows, else the row's id; `tag` is `rare`
/// on one row in a thousand. `other`: `flag` is 2 on ten rows.
async fn load(e: &Engine, f: &Fake) {
    for start in (1..=SKEW_ROWS).step_by(5_000) {
        let mut sql = String::from("INSERT INTO skew(id, st, v, tag, oid) VALUES ");
        for id in start..start + 5_000 {
            if id > start {
                sql.push(',');
            }
            let st = if id % 100 == 0 { id } else { 1 };
            let tag = if id % 1000 == 7 { "rare" } else { "common" };
            let _ = write!(
                sql,
                "({id}, {st}, {id}, '{tag}', {})",
                (id * 7) % OTHER_ROWS + 1
            );
        }
        run(e, f, &sql, &[]).await.unwrap();
    }
    for start in (1..=OTHER_ROWS).step_by(5_000) {
        let mut sql = String::from("INSERT INTO other(id, skew_id, flag) VALUES ");
        for id in start..start + 5_000 {
            if id > start {
                sql.push(',');
            }
            let flag = if id % 2500 == 3 { 2 } else { 1 };
            let _ = write!(sql, "({id}, {}, {flag})", (id * 7) % SKEW_ROWS + 1);
        }
        run(e, f, &sql, &[]).await.unwrap();
    }
}

/// Runs `sql` on the general planner's path (as the gateway does for
/// joins and aggregates).
async fn general(e: &Engine, f: &Fake, sql: &str) -> crate::engine::general::Rows {
    use crate::engine::general::{RunArgs, Seed};
    let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
    let cx = BindCtx {
        tenant: TENANT,
        names: f,
        current: Some(DB),
        backslash: true,
    };
    let g = crate::engine::general::bind(&cx, &stmt, Some(sql), None)
        .unwrap()
        .expect("a general statement");
    let seed = Seed::of(false);
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &seed,
        deadline: None,
        stop: None,
    };
    e.general_run(
        &g,
        &a,
        TxnOptions::default(),
        crate::engine::assign::Env::STRICT,
    )
    .await
    .unwrap()
}

/// `EXPLAIN`'s table, type and key columns, one step per entry.
async fn explain(e: &Engine, f: &Fake, sql: &str) -> Vec<String> {
    let r = general(e, f, &format!("EXPLAIN {sql}")).await;
    r.rows
        .iter()
        .map(|row| {
            row.iter()
                .skip(2)
                .take(4)
                .chain(row.get(8))
                .map(|d| match d {
                    Datum::Str(b) | Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                    Datum::Null => "-".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(":")
        })
        .collect()
}

/// The median time of `n` runs, the last run's rows.
async fn timed(e: &Engine, f: &Fake, sql: &str, n: usize) -> (u128, crate::engine::general::Rows) {
    let mut times = Vec::with_capacity(n);
    let mut last = None;
    for _ in 0..n {
        let t = std::time::Instant::now();
        let o = general(e, f, sql).await;
        times.push(t.elapsed().as_micros());
        last = Some(o);
    }
    times.sort_unstable();
    (times[n / 2], last.unwrap())
}

#[tokio::test]
async fn analyze_collects_figures_of_every_column_and_index() {
    let (e, _d) = mem();
    let f = fake();
    load(&e, &f).await;
    let skew = def(&f, "skew");
    let st = collect::analyze_on(&e, TENANT.0, &skew, &collect::Ask::default())
        .await
        .unwrap();
    assert_eq!(st.rows, SKEW_ROWS as u64);
    assert_eq!(st.sampled, collect::SAMPLE as u64);
    assert_eq!(
        st.index("PRIMARY").unwrap().distinct,
        vec![SKEW_ROWS as f64]
    );
    // `st` has a value per hundred rows besides 1, each on one row.
    assert_eq!(
        st.index("ks").unwrap().distinct,
        vec![(SKEW_ROWS / 100 + 1) as f64]
    );
    let c = st.column("st").unwrap();
    assert!(c.histogram.is_some());
    let ty = &skew.columns[1].ty;
    let one = st
        .share(
            "st",
            ty,
            &[sprig_plan::access::KeyRange::point(vec![Datum::Int(1)])],
        )
        .unwrap();
    assert!((one - 0.99).abs() < 0.01, "{one}");
    // Stored with the table: read again as written.
    let stored = collect::stored(&e, TENANT.0, 901).unwrap().unwrap();
    assert_eq!(stored, *st);
    // UPDATE HISTOGRAM's bucket count and MANUAL UPDATE are kept.
    let ask = collect::Ask {
        histograms: vec![("v".into(), 8, true)],
    };
    let st2 = collect::analyze_on(&e, TENANT.0, &skew, &ask)
        .await
        .unwrap();
    let v = st2.column("v").unwrap();
    assert_eq!(v.buckets, 8);
    assert!(v.manual && v.requested);
    assert!(v.histogram.as_ref().unwrap().buckets.len() <= 9);
    let st3 = collect::analyze_on(&e, TENANT.0, &skew, &collect::Ask::default())
        .await
        .unwrap();
    assert_eq!(st3.column("v").unwrap().histogram, v.histogram);
}

#[tokio::test]
async fn row_writes_are_counted_per_table() {
    let (e, _d) = mem();
    let f = fake();
    let before = stats::counters(TENANT.0, 902);
    run(
        &e,
        &f,
        "INSERT INTO other(id, skew_id, flag) VALUES (1, 1, 1), (2, 1, 1), (3, 1, 1)",
        &[],
    )
    .await
    .unwrap();
    run(&e, &f, "UPDATE other SET flag = 5 WHERE id = 2", &[])
        .await
        .unwrap();
    run(&e, &f, "DELETE FROM other WHERE id = 3", &[])
        .await
        .unwrap();
    let after = stats::counters(TENANT.0, 902);
    // Three inserts, one update, one delete; two rows more.
    assert!(after.0 - before.0 >= 5, "{before:?} {after:?}");
    assert_eq!(after.1 - before.1, 2);
}

#[tokio::test]
async fn skewed_values_get_the_plan_their_share_asks_for() {
    let (e, _d) = mem();
    let f = fake();
    load(&e, &f).await;
    // A value on 99% of the rows: the index read every one of them and
    // fetched each row; with statistics the table is scanned.
    let common = "SELECT COUNT(*), SUM(v) FROM skew WHERE st = 1";
    // Two conditions without an index: `tag = 'common'` keeps 99.9% of
    // `skew`, `flag = 2` ten rows of `other`. The fixed guesses (a tenth
    // each, both tables "large") read `skew` first and look `other` up by
    // its primary key for each of its rows.
    let join = "SELECT COUNT(*) FROM skew JOIN other ON other.id = skew.oid \
                WHERE skew.tag = 'common' AND other.flag = 2";
    let (e0, j0x) = (explain(&e, &f, common).await, explain(&e, &f, join).await);
    let (t0, o0) = timed(&e, &f, common, 5).await;
    let (j0, p0) = timed(&e, &f, join, 3).await;
    for t in ["skew", "other"] {
        collect::analyze_on(&e, TENANT.0, &def(&f, t), &collect::Ask::default())
            .await
            .unwrap();
    }
    let (e1, j1x) = (explain(&e, &f, common).await, explain(&e, &f, join).await);
    let (t1, o1) = timed(&e, &f, common, 5).await;
    let (j1, p1) = timed(&e, &f, join, 3).await;
    eprintln!(
        "common value: {t0} us, {} examined {e0:?} -> {t1} us, {} examined {e1:?}",
        o0.examined, o1.examined
    );
    eprintln!(
        "join: {j0} us, {} examined {j0x:?} -> {j1} us, {} examined {j1x:?}",
        p0.examined, p1.examined
    );
    assert_eq!(o0.rows, o1.rows);
    assert!(e0[0].contains("ks"), "the guesses read the index: {e0:?}");
    assert!(e1[0].contains("ALL"), "statistics scan the table: {e1:?}");
    assert_eq!(p0.rows, p1.rows);
    // With statistics `other` (ten rows kept) leads and `skew` is reached
    // by its index on `oid`.
    assert!(j0x[0].starts_with("skew:ALL"), "{j0x:?}");
    assert!(j1x[0].starts_with("other:ALL"), "{j1x:?}");
    assert!(j1x[1].starts_with("skew:ref:ko"), "{j1x:?}");
    assert!(
        p1.examined <= p0.examined,
        "{} vs {}",
        p1.examined,
        p0.examined
    );
}
