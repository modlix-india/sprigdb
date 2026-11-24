//! Online index builds (automatic indexing): the index is published
//! `IGNORED` without entries, every later write keeps it, the backfill
//! writes the rest a batch at a time while writes go on, and making it
//! public touches no row.

use std::collections::BTreeSet;

use super::*;
use crate::engine::assign::Env;
use crate::engine::ddl::{ONLINE_BUILD, Session};
use crate::engine::{alter, keys};

const NAME: &str = "sprig_auto_00c0ffee";

fn sess() -> Session {
    Session {
        env: Env::STRICT,
        wait: Duration::from_secs(5),
        db: "sbtest".into(),
        conn: 1,
        account: ("root".into(), "%".into()),
    }
}

fn with_key(ignored: bool) -> String {
    with_named_key(NAME, ignored)
}

fn with_named_key(name: &str, ignored: bool) -> String {
    let key = format!(
        ",\n  KEY `{name}` (`k`){}",
        if ignored { " IGNORED" } else { "" }
    );
    SBTEST.replace("PRIMARY KEY (`id`)", &format!("PRIMARY KEY (`id`){key}"))
}

fn def_of(f: &Fake) -> Arc<table::TableDef> {
    f.tables["sbtest1"].as_ref().as_ref().unwrap().clone()
}

/// Changes `f`'s table to `text` through the engine's three steps.
async fn change(e: &Engine, f: &mut Fake, text: &str, sql: &str, online: bool) -> Option<u64> {
    let d0 = def_of(f);
    let d1 = table::parse(d0.object, d0.version + 1, DB, text);
    assert!(d1.is_ok(), "{d1:?}");
    let d1 = d1.unwrap();
    let a = alter::read(sql).unwrap();
    let s = sess();
    let staged = if online {
        ONLINE_BUILD
            .scope((), e.alter_stage(TENANT.0, &d0, &d1, &a, &s))
            .await
    } else {
        e.alter_stage(TENANT.0, &d0, &d1, &a, &s).await
    };
    let staged = staged.unwrap();
    let rows = staged.rows;
    e.alter_apply(staged, &d1).await.unwrap();
    f.tables.insert("sbtest1".into(), Arc::new(Ok(d1)));
    rows
}

/// The index's entries, as key bytes.
async fn entries(e: &Engine, d: &table::TableDef) -> BTreeSet<Vec<u8>> {
    let ix = d.indexes.iter().position(|x| x.name == NAME).unwrap();
    let sp = e.space(TENANT.0, d).await.unwrap();
    let p = sp.index(d, ix);
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let got = tx
        .scan(&p, &keys::prefix_end(&p), false, 1_000_000)
        .unwrap();
    got.into_iter().map(|(k, _)| k.to_vec()).collect()
}

/// The entries a fresh build would write: one per row, from its values now.
async fn expected(e: &Engine, d: &table::TableDef) -> BTreeSet<Vec<u8>> {
    let ix = d.indexes.iter().position(|x| x.name == NAME).unwrap();
    let sp = e.space(TENANT.0, d).await.unwrap();
    let p = sp.rows();
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let rows = tx
        .scan(&p, &keys::prefix_end(&p), false, 1_000_000)
        .unwrap();
    rows.iter()
        .map(|(k, v)| {
            let row = crate::engine::row::decode(d, &sp, k, v).unwrap();
            keys::index_entry(&sp, d, ix, &row).unwrap().0
        })
        .collect()
}

#[tokio::test]
async fn an_online_build_adds_an_index_while_writes_go_on() {
    let (e, mut f, _d) = sysbench(3000).await;
    // Step 1: published IGNORED, without entries (no rows copied).
    let rows = change(
        &e,
        &mut f,
        &with_key(true),
        &format!("ALTER TABLE sbtest1 ADD INDEX {NAME} (k) IGNORED"),
        true,
    )
    .await;
    assert_eq!(rows, None);
    let d1 = def_of(&f);
    assert!(d1.indexes[0].ignored);
    assert!(entries(&e, &d1).await.is_empty());
    // No plan reads it.
    let p = bind(&f, "SELECT id FROM sbtest1 WHERE k = 50")
        .unwrap()
        .unwrap();
    assert!(matches!(p.access, Access::Full), "{:?}", p.access);
    // Writes from now on keep it: a new row, and a row not yet backfilled
    // whose key changes.
    run(
        &e,
        &f,
        "INSERT INTO sbtest1(k, c, pad) VALUES (77777, 'x', 'y')",
        &[],
    )
    .await
    .unwrap();
    run(&e, &f, "UPDATE sbtest1 SET k = 88888 WHERE id = 2", &[])
        .await
        .unwrap();
    assert_eq!(entries(&e, &d1).await.len(), 2);
    // Step 2: the backfill, in batches of 128, while another task keeps
    // changing keys and deleting and inserting rows.
    let version = d1.version;
    let current = move || Some(version);
    let writer = async {
        for i in 0..200i64 {
            let id = 1 + (i * 37) % 3000;
            run(
                &e,
                &f,
                &format!("UPDATE sbtest1 SET k = k + 1 WHERE id = {id}"),
                &[],
            )
            .await
            .unwrap();
            if i % 20 == 0 {
                run(
                    &e,
                    &f,
                    &format!("DELETE FROM sbtest1 WHERE id = {}", 3000 - i),
                    &[],
                )
                .await
                .unwrap();
                run(
                    &e,
                    &f,
                    "INSERT INTO sbtest1(k, c, pad) VALUES (5, 'n', 'n')",
                    &[],
                )
                .await
                .unwrap();
            }
            tokio::task::yield_now().await;
        }
    };
    let backfill = e.backfill_index(TENANT.0, &d1, NAME, 128, Duration::from_millis(1), &current);
    let (done, ()) = tokio::join!(backfill, writer);
    let (n, batches) = done.unwrap();
    assert!(n >= 2990 && batches >= 23, "{n} rows in {batches} batches");
    // Every row has exactly its entry: what a fresh build would write.
    let got = entries(&e, &d1).await;
    let want = expected(&e, &d1).await;
    assert_eq!(got.len(), want.len());
    assert!(got == want, "the index differs from a fresh build");
    // Step 3: public, touching no row; plans read it.
    let rows = change(
        &e,
        &mut f,
        &with_key(false),
        &format!("ALTER TABLE sbtest1 ALTER INDEX {NAME} NOT IGNORED"),
        false,
    )
    .await;
    assert_eq!(rows, None);
    let d2 = def_of(&f);
    assert!(!d2.indexes[0].ignored);
    assert_eq!(entries(&e, &d2).await, expected(&e, &d2).await);
    let p = bind(&f, "SELECT id FROM sbtest1 WHERE k = 50")
        .unwrap()
        .unwrap();
    assert!(matches!(p.access, Access::IndexEq { .. }), "{:?}", p.access);
}

#[tokio::test]
async fn a_backfill_stops_when_the_definition_changes() {
    let (e, mut f, _d) = sysbench(500).await;
    change(
        &e,
        &mut f,
        &with_key(true),
        &format!("ALTER TABLE sbtest1 ADD INDEX {NAME} (k) IGNORED"),
        true,
    )
    .await;
    let d1 = def_of(&f);
    let newer = d1.version + 1;
    let r = e
        .backfill_index(TENANT.0, &d1, NAME, 100, Duration::ZERO, &move || {
            Some(newer)
        })
        .await;
    assert_eq!(r.err().map(|e| e.protocol().mysql_errno), Some(1412));
}

#[tokio::test]
async fn without_the_online_mode_an_added_index_is_built_at_once() {
    let (e, mut f, _d) = sysbench(300).await;
    change(
        &e,
        &mut f,
        &with_key(true),
        &format!("ALTER TABLE sbtest1 ADD INDEX {NAME} (k) IGNORED"),
        false,
    )
    .await;
    let d1 = def_of(&f);
    assert_eq!(entries(&e, &d1).await.len(), 300);
}

/// Runs `sql` `n` times: the median time (µs) and the rows examined.
async fn timed(e: &Engine, f: &Fake, sql: &str, n: usize) -> (u128, u64) {
    let mut times = Vec::with_capacity(n);
    let mut examined = 0;
    for _ in 0..n {
        let t = std::time::Instant::now();
        let o = run(e, f, sql, &[]).await.unwrap();
        times.push(t.elapsed().as_micros());
        examined = o.examined;
        assert_eq!(o.rows.len(), 1);
    }
    times.sort_unstable();
    (times[n / 2], examined)
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // the loop's steps, in order
async fn a_slow_query_gets_its_index_from_the_loop_and_speeds_up() {
    use sprig_advisor::autoindex::candidates::{self, ColumnInfo, ExistingIndex, TableInfo};
    use sprig_advisor::autoindex::policy::{self, Replay, Verdict};
    use sprig_advisor::autoindex::settings::AutoIndexSettings;
    use sprig_plan::catalog::{IndexDef, IndexPart};
    use sprig_plan::whatif::Hypothetical;
    use sprig_workload::shape::{ColumnRef, PredOp, Predicate, Shape, StmtType, TableRef};

    let (e, mut f, _d) = sysbench(20_000).await;
    let sql = "SELECT id FROM sbtest1 WHERE k = 123450";
    let (slow_us, slow_examined) = timed(&e, &f, sql, 9).await;
    assert_eq!(slow_examined, 20_000);
    // The pattern's shape, as the workload records it.
    let mut shape = Shape::empty(StmtType::Select);
    let t = TableRef {
        alias: "sbtest1".into(),
        object_id: None,
        db: Some("sbtest".into()),
        table: "sbtest1".into(),
        db_id: None,
        derived: false,
    };
    shape.tables = vec![t.clone()];
    shape.predicates = vec![Predicate {
        column: ColumnRef {
            table: None,
            col: "k".into(),
        },
        op: PredOp::Eq,
        args: vec![0],
    }];
    let d0 = def_of(&f);
    let info = TableInfo {
        db: "sbtest".into(),
        table: "sbtest1".into(),
        columns: d0
            .columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                indexable: true,
                distinct: None,
            })
            .collect(),
        pk: vec!["id".into()],
        indexes: Vec::<ExistingIndex>::new(),
        rows: 20_000,
    };
    // 1. Candidates.
    let specs = candidates::generate(&shape, &t, &info, None);
    assert_eq!(specs.len(), 1, "{specs:?}");
    let spec = &specs[0];
    assert_eq!(spec.canonical(), "secondary(k)");
    let name = spec.name();
    // 2. Validation: the statement planned without and with it.
    let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
    let cx = BindCtx {
        tenant: TENANT,
        names: &f,
        current: Some(DB),
        backslash: true,
    };
    let g = crate::engine::general::bind(&cx, &stmt, Some(sql), None)
        .unwrap()
        .unwrap();
    let hypo = Hypothetical {
        schema: "sbtest".into(),
        table: "sbtest1".into(),
        index: IndexDef::new(&name, false, vec![IndexPart::asc(1)]),
        sample_rows: 4096,
    };
    let seed = crate::engine::general::Seed::from_vars(false, &Env::STRICT, |_| None);
    let (before, after) = e
        .general_whatif(TENANT.0, &g, &[], &seed, Some(&hypo))
        .await
        .unwrap();
    let after = after.unwrap();
    assert!(after.reads(&name), "{after:?}");
    let replay = Replay {
        before: before.examined,
        after: after.examined,
        reads: true,
    };
    assert!(
        matches!(
            policy::verdict(&[replay], &AutoIndexSettings::default()),
            Verdict::Accept { .. }
        ),
        "{replay:?}"
    );
    // 3. The online build.
    change(
        &e,
        &mut f,
        &with_named_key(&name, true),
        &format!("ALTER TABLE sbtest1 {} IGNORED", spec.add_clause()),
        true,
    )
    .await;
    let d1 = def_of(&f);
    let current = move || Some(d1.version);
    let d1 = def_of(&f);
    let (rows, _) = e
        .backfill_index(TENANT.0, &d1, &name, 1024, Duration::ZERO, &current)
        .await
        .unwrap();
    assert_eq!(rows, 20_000);
    change(
        &e,
        &mut f,
        &with_named_key(&name, false),
        &format!("ALTER TABLE sbtest1 ALTER INDEX {name} NOT IGNORED"),
        false,
    )
    .await;
    // Latency and rows examined drop; plans that read it are counted.
    let reads0 = crate::autoindex::usage::reads(TENANT.0, d1.object.0, &name);
    let (fast_us, fast_examined) = timed(&e, &f, sql, 9).await;
    assert!(fast_examined <= 2, "{fast_examined}");
    assert!(fast_us * 3 < slow_us, "{slow_us} µs -> {fast_us} µs");
    assert_eq!(
        crate::autoindex::usage::reads(TENANT.0, d1.object.0, &name) - reads0,
        9
    );
    eprintln!(
        "auto index {name}: median {slow_us} µs / {slow_examined} rows -> {fast_us} µs / {fast_examined} rows"
    );
    // 4. Dropped again: back to the scan.
    change(
        &e,
        &mut f,
        SBTEST,
        &format!("ALTER TABLE sbtest1 DROP INDEX {name}"),
        false,
    )
    .await;
    let (_, examined) = timed(&e, &f, sql, 1).await;
    assert_eq!(examined, 20_000);
}
