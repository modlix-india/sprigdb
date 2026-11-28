//! Definition changes on engine tables: columns and keys that change under
//! rows. Each new definition is the text MariaDB 11.8 prints for the table
//! after the statement; the expected errors are what MariaDB 11.8 answered
//! (MySQL 8.4 where the test says so).

use std::fmt::Write as _;

use sprig_engine::row::record;

use super::*;
use crate::engine::assign::Env;
use crate::engine::ddl::Session;
use crate::engine::{alter, keys, meta};

const UTF8: meta::Results = meta::Results {
    cs: Some(meta::ResultsCs { id: 224, mbmax: 4 }),
    mysql: false,
};

fn sess() -> Session {
    Session {
        env: Env::STRICT,
        wait: Duration::from_millis(200),
        db: "sbtest".into(),
        conn: 7,
        account: ("root".into(), "%".into()),
    }
}

fn mysql() -> Session {
    Session {
        env: Env {
            mysql: true,
            ..Env::STRICT
        },
        ..sess()
    }
}

/// A catalog with table `t` (object 20) defined by `sql`.
fn with(sql: &str) -> Fake {
    let mut f = Fake::new();
    let parsed = table::parse(ObjectId(20), 1, DB, sql);
    assert!(parsed.is_ok(), "{parsed:?}");
    f.tables.insert("t".into(), Arc::new(parsed));
    f
}

fn def(f: &Fake) -> Arc<table::TableDef> {
    f.tables["t"].as_ref().as_ref().unwrap().clone()
}

async fn q(e: &Engine, f: &Fake, sql: &str) -> sprig_common::Result<Outcome> {
    run(e, f, sql, &[]).await
}

async fn ok(e: &Engine, f: &Fake, sql: &str) -> Outcome {
    let o = q(e, f, sql).await;
    assert!(o.is_ok(), "{sql}: {o:?}");
    o.unwrap()
}

/// The rows of a SELECT as text: columns joined by `,`, rows by ` | `.
async fn flat(e: &Engine, f: &Fake, sql: &str) -> String {
    let p = bind(f, sql).unwrap().expect("an engine statement");
    let o = exec(e, &p, &[]).await.unwrap();
    let plan::Op::Select { out } = &p.op else {
        panic!("not a SELECT");
    };
    o.rows
        .iter()
        .map(|r| {
            r.iter()
                .zip(out)
                .map(|(d, oc)| {
                    meta::text_in(&p.table.columns[oc.col], d, &Env::STRICT, UTF8).map_or_else(
                        || "NULL".into(),
                        |b| String::from_utf8_lossy(&b).into_owned(),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Runs `sql`, whose result on the MariaDB copy is the definition `new`:
/// stage, then (as the catalog would) publish and apply. Returns the rows
/// the servers would report.
async fn change_as(
    e: &Engine,
    f: &mut Fake,
    sql: &str,
    new: &str,
    sess: &Session,
) -> sprig_common::Result<Option<(u64, usize)>> {
    let d0 = def(f);
    let d1 = table::parse(ObjectId(20), d0.version + 1, DB, new).unwrap();
    let a = alter::read(sql).expect("an ALTER TABLE");
    let staged = e.alter_stage(TENANT.0, &d0, &d1, &a, sess).await?;
    let rows = staged.rows.map(|n| (n, staged.conds.len()));
    f.tables.insert("t".into(), Arc::new(Ok(Arc::clone(&d1))));
    e.alter_apply(staged, &d1).await?;
    Ok(rows)
}

async fn change(
    e: &Engine,
    f: &mut Fake,
    sql: &str,
    new: &str,
) -> sprig_common::Result<Option<(u64, usize)>> {
    change_as(e, f, sql, new, &sess()).await
}

fn failed(r: sprig_common::Result<Option<(u64, usize)>>) -> (u16, String) {
    let x = r.expect_err("the change must fail");
    (x.protocol().mysql_errno, x.message)
}

/// Every key of table `t` but its records, as (key kind, key index
/// number) with a count.
fn ranges(e: &Engine) -> Vec<(char, u16, usize)> {
    let p = keys::object_prefix(TENANT.0, 20);
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let all = tx.scan(&p, &keys::prefix_end(&p), false, 100_000).unwrap();
    let mut out: Vec<(char, u16, usize)> = Vec::new();
    for (k, _) in all {
        let kind = char::from(k[16]);
        if kind == 'M' {
            continue;
        }
        let id = u16::from_be_bytes([k[17], k[18]]);
        match out.last_mut() {
            Some((c, i, n)) if *c == kind && *i == id => *n += 1,
            _ => out.push((kind, id, 1)),
        }
    }
    out
}

/// The version of the column list the row with primary key `pk` carries.
async fn version_of(e: &Engine, f: &Fake, pk: i64) -> u32 {
    let d = def(f);
    let sp = e.space(TENANT.0, &d).await.unwrap();
    let key = keys::row_key(&sp, &d, &[Datum::Int(pk)]).unwrap();
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let raw = tx.get(&key).unwrap().unwrap();
    record::peek_header(&raw).unwrap().schema_version
}

const T0: &str = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

async fn three_rows() -> (Engine, Fake, TempDir) {
    let (e, d) = mem();
    let f = with(T0);
    ok(
        &e,
        &f,
        "INSERT INTO t VALUES (1, 10, 'one', 1), (2, NULL, NULL, 2), (3, 300, 'three33333', 3)",
    )
    .await;
    (e, f, d)
}

#[tokio::test]
async fn columns_are_added_dropped_renamed_and_widened_without_touching_rows() {
    let (e, mut f, _d) = three_rows().await;
    // ADD COLUMN with and without a DEFAULT, FIRST and AFTER.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD COLUMN b INT, ADD d VARCHAR(8) NOT NULL DEFAULT 'dd' AFTER id, ADD c INT NOT NULL FIRST",
        "CREATE TABLE `t` (\n  `c` int(11) NOT NULL,\n  `id` int(11) NOT NULL,\n  `d` varchar(8) NOT NULL DEFAULT 'dd',\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(rows, None);
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t").await,
        "0,1,dd,10,one,1,NULL | 0,2,dd,NULL,NULL,2,NULL | 0,3,dd,300,three33333,3,NULL"
    );
    // The rows weren't rewritten; a new row carries the new list.
    ok(&e, &f, "INSERT INTO t (c, id, a, b) VALUES (9, 4, 40, 44)").await;
    assert_eq!(
        (version_of(&e, &f, 1).await, version_of(&e, &f, 4).await),
        (1, 2)
    );
    assert_eq!(
        flat(&e, &f, "SELECT c, id, d, a, s, n, b FROM t WHERE id = 4").await,
        "9,4,dd,40,NULL,3,44"
    );
    // An UPDATE of an old row rewrites it under the new list.
    ok(&e, &f, "UPDATE t SET b = 7 WHERE id = 2").await;
    assert_eq!(version_of(&e, &f, 2).await, 2);
    assert_eq!(
        flat(&e, &f, "SELECT b, d FROM t WHERE id = 2").await,
        "7,dd"
    );
    // The index on `a` still finds its rows.
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a = 300").await, "3");

    // DROP, RENAME, CHANGE with a wider type, a new default: no row touched;
    // int to bigint is a copy on the servers, which report the rows.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t DROP COLUMN c, DROP s, CHANGE a a2 BIGINT, RENAME COLUMN b TO b2, ALTER COLUMN n SET DEFAULT 9, MODIFY d VARCHAR(20) NOT NULL DEFAULT 'dd'",
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `d` varchar(20) NOT NULL DEFAULT 'dd',\n  `a2` bigint(20) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 9,\n  `b2` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((4, 0)));
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t").await,
        "1,dd,10,1,NULL | 2,dd,NULL,2,7 | 3,dd,300,3,NULL | 4,dd,40,3,44"
    );
    assert_eq!(
        (version_of(&e, &f, 1).await, version_of(&e, &f, 4).await),
        (1, 2)
    );
    ok(&e, &f, "INSERT INTO t (id, a2) VALUES (5, 5000000000)").await;
    assert_eq!(
        flat(&e, &f, "SELECT a2, n FROM t WHERE id = 5").await,
        "5000000000,9"
    );
    // The index was built anew for the wider type (its old entries are gone).
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a2 = 300").await, "3");
    assert_eq!(
        flat(&e, &f, "SELECT id FROM t WHERE a2 = 5000000000").await,
        "5"
    );
    assert_eq!(ranges(&e), vec![('P', 0, 5), ('S', 2, 5)]);

    // A dropped column's name given to a new column: another column.
    change(
        &e,
        &mut f,
        "ALTER TABLE t DROP b2, ADD b2 VARCHAR(4) DEFAULT 'q'",
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `d` varchar(20) NOT NULL DEFAULT 'dd',\n  `a2` bigint(20) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 9,\n  `b2` varchar(4) DEFAULT 'q',\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a2`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT id, b2 FROM t").await,
        "1,q | 2,q | 3,q | 4,q | 5,q"
    );
}

#[tokio::test]
async fn values_that_do_not_fit_fail_the_change_and_leave_the_table_alone() {
    let (e, mut f, _d) = three_rows().await;
    let before = ranges(&e);
    let tiny = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` tinyint(4) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t MODIFY a TINYINT", tiny).await),
        (1264, "Out of range value for column 'a' at row 3".into())
    );
    let notnull = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` smallint(6) NOT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(
            change(
                &e,
                &mut f,
                "ALTER TABLE t MODIFY a SMALLINT NOT NULL",
                notnull
            )
            .await
        ),
        (1265, "Data truncated for column 'a' at row 2".into())
    );
    // MySQL 8.4 words NULL into NOT NULL differently when it changes the
    // table in place (no other type), and the same when it copies.
    assert_eq!(
        failed(
            change_as(
                &e,
                &mut f,
                "ALTER TABLE t MODIFY a SMALLINT NOT NULL",
                notnull,
                &mysql()
            )
            .await
        )
        .0,
        1265
    );
    assert_eq!(
        failed(
            change_as(
                &e,
                &mut f,
                "ALTER TABLE t MODIFY a INT NOT NULL",
                &notnull.replace("smallint(6)", "int(11)"),
                &mysql()
            )
            .await
        ),
        (1138, "Invalid use of NULL value".into())
    );
    // Its clause rules: a type change is neither INSTANT nor INPLACE.
    assert_eq!(
        failed(
            change_as(
                &e,
                &mut f,
                "ALTER TABLE t MODIFY a TINYINT, ALGORITHM=INPLACE",
                tiny,
                &mysql()
            )
            .await
        ),
        (
            1846,
            "ALGORITHM=INPLACE is not supported. Reason: Cannot change column type INPLACE. Try ALGORITHM=COPY.".into()
        )
    );
    assert_eq!(
        failed(
            change_as(
                &e,
                &mut f,
                "ALTER TABLE t MODIFY a INT NOT NULL, ALGORITHM=INSTANT",
                &notnull.replace("smallint(6)", "int(11)"),
                &mysql()
            )
            .await
        )
        .0,
        1845
    );
    let short = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(5) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t MODIFY s VARCHAR(5)", short).await),
        (1265, "Data truncated for column 's' at row 3".into())
    );
    let number = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` int(11) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t MODIFY s INT", number).await),
        (1292, "Truncated incorrect INTEGER value: 'one'".into())
    );
    // Nothing changed: the same keys, the old definition still serves.
    assert_eq!(ranges(&e), before);
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t").await,
        "1,10,one,1 | 2,NULL,NULL,2 | 3,300,three33333,3"
    );
    ok(&e, &f, "INSERT INTO t VALUES (4, 4, 'four', 4)").await;
}

#[tokio::test]
async fn other_changes_copy_the_rows_converted() {
    let (e, mut f, _d) = three_rows().await;
    ok(&e, &f, "UPDATE t SET a = 20, s = '22' WHERE id = 2").await;
    ok(&e, &f, "UPDATE t SET s = '1', a = 100 WHERE id = 1").await;
    ok(&e, &f, "UPDATE t SET s = '33', a = 30 WHERE id = 3").await;
    // Narrower, NOT NULL, another type family, all at once.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t MODIFY a TINYINT NOT NULL, MODIFY s INT, MODIFY n VARCHAR(5)",
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` tinyint(4) NOT NULL,\n  `s` int(11) DEFAULT NULL,\n  `n` varchar(5) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((3, 0)));
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t").await,
        "1,100,1,1 | 2,20,22,2 | 3,30,33,3"
    );
    // The rows live in the next generation under one column list; the old
    // generation and the old index entries are gone.
    assert_eq!(ranges(&e), vec![('P', 1, 3), ('S', 2, 3)]);
    assert_eq!(version_of(&e, &f, 1).await, 2);
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a = 30").await, "3");
    assert_eq!(
        code(q(&e, &f, "INSERT INTO t VALUES (4, 200, 1, 'x')").await),
        1264
    );
    assert_eq!(
        code(q(&e, &f, "INSERT INTO t VALUES (4, NULL, 1, 'x')").await),
        1048
    );
    ok(&e, &f, "INSERT INTO t VALUES (4, 40, 44, 'four')").await;

    // Another primary key type: the rows are keyed anew.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t MODIFY id BIGINT NOT NULL",
        "CREATE TABLE `t` (\n  `id` bigint(20) NOT NULL,\n  `a` tinyint(4) NOT NULL,\n  `s` int(11) DEFAULT NULL,\n  `n` varchar(5) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((4, 0)));
    ok(&e, &f, "INSERT INTO t VALUES (5000000000, 50, 5, 'big')").await;
    assert_eq!(
        flat(&e, &f, "SELECT id, n FROM t WHERE id IN (2, 5000000000)").await,
        "2,2 | 5000000000,big"
    );
    assert_eq!(
        flat(&e, &f, "SELECT id FROM t WHERE a = 50").await,
        "5000000000"
    );
    assert_eq!(ranges(&e), vec![('P', 2, 5), ('S', 3, 5)]);

    // Another primary key: a duplicate under it fails the change, naming it.
    let by_a = "CREATE TABLE `t` (\n  `id` bigint(20) NOT NULL,\n  `a` tinyint(4) NOT NULL,\n  `s` int(11) DEFAULT NULL,\n  `n` varchar(5) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    ok(&e, &f, "INSERT INTO t VALUES (6, 50, 6, 'dup')").await;
    assert_eq!(
        failed(
            change(
                &e,
                &mut f,
                "ALTER TABLE t DROP PRIMARY KEY, DROP INDEX ka, ADD PRIMARY KEY (a)",
                by_a
            )
            .await
        ),
        (1062, "Duplicate entry '50' for key 'PRIMARY'".into())
    );
    assert_eq!(ranges(&e), vec![('P', 2, 6), ('S', 3, 6)]);
    ok(&e, &f, "DELETE FROM t WHERE id = 6").await;
    change(
        &e,
        &mut f,
        "ALTER TABLE t DROP PRIMARY KEY, DROP INDEX ka, ADD PRIMARY KEY (a)",
        by_a,
    )
    .await
    .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT a, id FROM t").await,
        "20,2 | 30,3 | 40,4 | 50,5000000000 | 100,1"
    );
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a = 40").await, "4");
    assert_eq!(ranges(&e), vec![('P', 4, 5)]);
}

#[tokio::test]
async fn indexes_are_added_checked_renamed_and_dropped() {
    let (e, mut f, _d) = three_rows().await;
    ok(&e, &f, "INSERT INTO t VALUES (4, 300, 'four', 3)").await;
    // A unique key over values that repeat: 1062 naming the key; nothing kept.
    let dup = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `un` (`n`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t ADD UNIQUE KEY un (n)", dup).await),
        (1062, "Duplicate entry '3' for key 'un'".into())
    );
    assert_eq!(ranges(&e), vec![('P', 0, 4), ('S', 1, 4)]);
    // A unique key that holds (NULLs don't collide), an index, a renamed
    // index: the renamed one keeps its entries.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD UNIQUE us (s), ADD INDEX kn (n, a), RENAME INDEX ka TO ka2",
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `us` (`s`),\n  KEY `ka2` (`a`),\n  KEY `kn` (`n`,`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(rows, None);
    assert_eq!(
        ranges(&e),
        vec![('P', 0, 4), ('S', 1, 4), ('S', 4, 4), ('U', 3, 4)]
    );
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE s = 'four'").await, "4");
    assert_eq!(
        flat(&e, &f, "SELECT id FROM t WHERE a = 300").await,
        "3 | 4"
    );
    assert_eq!(
        code(q(&e, &f, "INSERT INTO t VALUES (5, 1, 'one', 1)").await),
        1062
    );
    // Dropped indexes leave no entry behind.
    change(
        &e,
        &mut f,
        "ALTER TABLE t DROP INDEX us, DROP KEY kn",
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka2` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    )
    .await
    .unwrap();
    assert_eq!(ranges(&e), vec![('P', 0, 4), ('S', 1, 4)]);
    ok(&e, &f, "INSERT INTO t VALUES (5, 1, 'one', 1)").await;
}

#[tokio::test]
async fn a_staged_change_survives_or_is_discarded_as_the_catalog_decides() {
    let (e, mut f, _d) = three_rows().await;
    let d0 = def(&f);
    let narrow = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` smallint(6) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    let d1 = table::parse(ObjectId(20), 2, DB, narrow).unwrap();
    let a = alter::read("ALTER TABLE t MODIFY a SMALLINT").unwrap();
    // Staged and abandoned (the catalog refused): nothing is left.
    let staged = e
        .alter_stage(TENANT.0, &d0, &d1, &a, &sess())
        .await
        .unwrap();
    assert_eq!(staged.rows, Some(3));
    assert_eq!(
        ranges(&e),
        vec![('P', 0, 3), ('P', 1, 3), ('S', 1, 3), ('S', 2, 3)]
    );
    e.alter_abandon(staged).await.unwrap();
    assert_eq!(ranges(&e), vec![('P', 0, 3), ('S', 1, 3)]);
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a = 300").await, "3");
    // Staged, and the process stops before applying: the old definition
    // still reads the table as it was; the new one, once the catalog has
    // it, applies the change.
    let staged = e
        .alter_stage(TENANT.0, &d0, &d1, &a, &sess())
        .await
        .unwrap();
    drop(staged);
    assert_eq!(flat(&e, &f, "SELECT a FROM t").await, "10 | NULL | 300");
    f.tables.insert("t".into(), Arc::new(Ok(Arc::clone(&d1))));
    assert_eq!(flat(&e, &f, "SELECT a FROM t").await, "10 | NULL | 300");
    assert_eq!(
        code(q(&e, &f, "UPDATE t SET a = 70000 WHERE id = 1").await),
        1264
    );
    assert_eq!(ranges(&e), vec![('P', 2, 3), ('S', 3, 3)]);
    // The old definition is no longer the table's.
    let old = with(T0);
    assert_eq!(code(q(&e, &old, "SELECT * FROM t").await), 1412);
    // A change staged and never published is discarded by the next one.
    let d2 = table::parse(ObjectId(20), 3, DB, T0).unwrap();
    let back = alter::read("ALTER TABLE t MODIFY a INT").unwrap();
    drop(
        e.alter_stage(TENANT.0, &d1, &d2, &back, &sess())
            .await
            .unwrap(),
    );
    let with_b = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` smallint(6) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`),\n  KEY `kb` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    change(
        &e,
        &mut f,
        "ALTER TABLE t ADD b INT, ADD KEY kb (b)",
        with_b,
    )
    .await
    .unwrap();
    assert_eq!(ranges(&e), vec![('P', 2, 3), ('S', 3, 3), ('S', 5, 3)]);
    assert_eq!(
        flat(&e, &f, "SELECT a, b FROM t").await,
        "10,NULL | NULL,NULL | 300,NULL"
    );
}

#[tokio::test]
async fn a_change_that_copies_waits_for_the_transactions_using_the_table() {
    use sprig_engine::txn::TxnOptions as O;
    let (e, mut f, _d) = three_rows().await;
    let narrow = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` smallint(6) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    // A transaction that read the table and stays open.
    let mut tx = e.begin_session(O::default()).unwrap();
    let p = bind(&f, "SELECT * FROM t WHERE id = 1").unwrap().unwrap();
    e.run_in(TENANT.0, &p, &[], false, &mut tx).await.unwrap();
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t MODIFY a SMALLINT", narrow).await),
        (
            1205,
            "Lock wait timeout exceeded; try restarting transaction".into()
        )
    );
    // Its next statements still run; nothing was left behind.
    e.run_in(TENANT.0, &p, &[], false, &mut tx).await.unwrap();
    assert_eq!(ranges(&e), vec![('P', 0, 3), ('S', 1, 3)]);
    // A change that touches no row waits for it too, as for a metadata
    // lock.
    let wider = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(20) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    assert_eq!(
        failed(change(&e, &mut f, "ALTER TABLE t MODIFY s VARCHAR(20)", wider).await).0,
        1205
    );
    e.run_in(TENANT.0, &p, &[], false, &mut tx).await.unwrap();
    tx.finish().await.unwrap();
    change(&e, &mut f, "ALTER TABLE t MODIFY s VARCHAR(20)", wider)
        .await
        .unwrap();
    let narrow20 = narrow.replace("varchar(10)", "varchar(20)");
    change(&e, &mut f, "ALTER TABLE t MODIFY a SMALLINT", &narrow20)
        .await
        .unwrap();
    assert_eq!(flat(&e, &f, "SELECT a FROM t").await, "10 | NULL | 300");
}

#[tokio::test]
async fn truncate_and_drop_leave_nothing_behind() {
    let (e, d) = mem();
    let _keep = d;
    let f = with(
        "CREATE TABLE `t` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(8) NOT NULL DEFAULT 'x',\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `us` (`s`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
    );
    ok(&e, &f, "INSERT INTO t (a, s) VALUES (1, 'a'), (2, 'b')").await;
    assert_eq!(flat(&e, &f, "SELECT id FROM t").await, "100 | 101");
    let t = def(&f);
    e.truncate(TENANT.0, &t, &sess()).await.unwrap();
    assert_eq!(ranges(&e), vec![]);
    assert_eq!(flat(&e, &f, "SELECT * FROM t").await, "");
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE a = 1").await, "");
    // The counter starts over at 1, as on both servers; the unique key
    // is free again.
    ok(&e, &f, "INSERT INTO t (a, s) VALUES (3, 'a')").await;
    assert_eq!(
        flat(&e, &f, "SELECT id, a FROM t WHERE s = 'a'").await,
        "1,3"
    );
    assert_eq!(ranges(&e), vec![('P', 1, 1), ('S', 4, 1), ('U', 3, 1)]);
    // A transaction using the table holds TRUNCATE back.
    let mut tx = e.begin_session(TxnOptions::default()).unwrap();
    let p = bind(&f, "SELECT * FROM t WHERE id = 1").unwrap().unwrap();
    e.run_in(TENANT.0, &p, &[], false, &mut tx).await.unwrap();
    let r = e.truncate(TENANT.0, &t, &sess()).await;
    assert_eq!(r.unwrap_err().protocol().mysql_errno, 1205);
    tx.finish().await.unwrap();
    assert_eq!(flat(&e, &f, "SELECT id FROM t").await, "1");
    // DROP TABLE: every key of the table goes, its records too.
    e.drop_table(TENANT.0, 20).await.unwrap();
    let p = keys::object_prefix(TENANT.0, 20);
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    assert!(
        tx.scan(&p, &keys::prefix_end(&p), false, 10)
            .unwrap()
            .is_empty()
    );
}

const CHECKED: &str = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL CHECK (`a` > 0),\n  `b` int(11) DEFAULT NULL CHECK (`b` > 0),\n  `c` int(11) DEFAULT (1 + 2) CHECK (`c` <> 5),\n  `d` varchar(10) DEFAULT 'x',\n  `e` int(11) DEFAULT (`id` * 2),\n  `g` int(11) NOT NULL DEFAULT 7,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `z1` CHECK (`a` < 100),\n  CONSTRAINT `a1` CHECK (`b` < 100),\n  CONSTRAINT `CONSTRAINT_1` CHECK (`a` + `b` <> 50),\n  CONSTRAINT `Mixed Case` CHECK (`a` is null or `a` not in (7,8)),\n  CONSTRAINT `CONSTRAINT_3` CHECK (`b` between 1 and 90 or `b` is null)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

async fn err_as(e: &Engine, f: &Fake, sql: &str, env: Env) -> (u16, String) {
    let p = bind(f, sql).unwrap().expect("an engine statement");
    let x = e
        .run_env(TENANT.0, &p, &[], false, TxnOptions::default(), env)
        .await
        .expect_err(sql);
    (x.protocol().mysql_errno, x.message)
}

#[tokio::test]
async fn check_constraints_fail_rows_as_each_server_does() {
    let (e, _d) = mem();
    let f = with(CHECKED);
    let maria = |name: &str| (4025, format!("CONSTRAINT `{name}` failed for `sbtest`.`t`"));
    let my = |name: &str| (3819, format!("Check constraint '{name}' is violated."));
    let m = Env {
        mysql: true,
        ..Env::STRICT
    };
    // Which constraint a row fails first: MariaDB judges the columns' own
    // checks, then the table's in the order they were written; MySQL
    // judges them by name.
    for (values, on_maria, on_mysql) in [
        ("(1, -1, -1)", "t.a", "t_chk_1"),
        ("(1, 200, 200)", "z1", "a1"),
        ("(1, 200, -1)", "t.b", "t_chk_2"),
        ("(1, -1, 200)", "t.a", "a1"),
        ("(1, 25, 25)", "CONSTRAINT_1", "t_chk_4"),
        ("(1, 7, 1)", "Mixed Case", "Mixed Case"),
        ("(1, 1, 95)", "CONSTRAINT_3", "t_chk_5"),
    ] {
        let sql = format!("INSERT INTO t (id, a, b) VALUES {values}");
        assert_eq!(
            err_as(&e, &f, &sql, Env::STRICT).await,
            maria(on_maria),
            "{sql}"
        );
        assert_eq!(err_as(&e, &f, &sql, m).await, my(on_mysql), "{sql}");
    }
    assert_eq!(
        err_as(
            &e,
            &f,
            "INSERT INTO t (id, a, b, c) VALUES (1, 25, 26, 5)",
            Env::STRICT
        )
        .await,
        maria("t.c")
    );
    // Rows that pass (NULL passes a check); DEFAULT expressions, one of
    // them over another column.
    ok(&e, &f, "INSERT INTO t (id, a, b) VALUES (1, 1, 2)").await;
    ok(&e, &f, "INSERT INTO t (id) VALUES (2)").await;
    ok(&e, &f, "INSERT INTO t (id, c) VALUES (4, NULL)").await;
    assert_eq!(
        flat(&e, &f, "SELECT id, a, b, c, d, e, g FROM t").await,
        "1,1,2,3,x,2,7 | 2,NULL,NULL,3,x,4,7 | 4,NULL,NULL,NULL,x,8,7"
    );
    // An UPDATE's new row is judged too; a row that fails stays as it was.
    assert_eq!(
        err_as(&e, &f, "UPDATE t SET a = 200 WHERE id = 1", Env::STRICT).await,
        maria("z1")
    );
    assert_eq!(
        err_as(
            &e,
            &f,
            "UPDATE t SET a = 49, b = 1 WHERE id = 1",
            Env::STRICT
        )
        .await,
        maria("CONSTRAINT_1")
    );
    assert_eq!(
        err_as(&e, &f, "UPDATE t SET c = c + 2 WHERE id = 1", m).await,
        my("t_chk_3")
    );
    assert_eq!(
        flat(&e, &f, "SELECT a, b, c FROM t WHERE id = 1").await,
        "1,2,3"
    );
}

#[tokio::test]
async fn added_constraints_and_default_expressions_reach_the_rows_there_are() {
    let (e, mut f, _d) = three_rows().await;
    // A constraint a row fails is not added.
    let with_check = |name: &str, expr: &str| {
        T0.replace(
            "  KEY `ka` (`a`)\n",
            &format!("  KEY `ka` (`a`),\n  CONSTRAINT `{name}` CHECK ({expr})\n"),
        )
    };
    assert_eq!(
        failed(
            change(
                &e,
                &mut f,
                "ALTER TABLE t ADD CONSTRAINT small CHECK (a < 100)",
                &with_check("small", "`a` < 100")
            )
            .await
        ),
        (4025, "CONSTRAINT `small` failed for `sbtest`.`t`".into())
    );
    assert_eq!(
        failed(
            change_as(
                &e,
                &mut f,
                "ALTER TABLE t ADD CONSTRAINT small CHECK (a < 100)",
                &with_check("small", "`a` < 100"),
                &mysql()
            )
            .await
        ),
        (3819, "Check constraint 'small' is violated.".into())
    );
    // One every row passes is added (the servers copy the table for it),
    // and new rows are judged by it.
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD CONSTRAINT big CHECK (a < 1000)",
        &with_check("big", "`a` < 1000"),
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((3, 0)));
    assert_eq!(
        code(q(&e, &f, "INSERT INTO t VALUES (4, 1000, 'x', 1)").await),
        4025
    );
    assert_eq!(
        code(q(&e, &f, "UPDATE t SET a = a * 10 WHERE id = 3").await),
        4025
    );
    // A new column with a constant expression touches no row; one over
    // the row is worked out for each (a copy, on the servers too).
    let more = with_check("big", "`a` < 1000").replace(
        "  PRIMARY KEY",
        "  `h` int(11) NOT NULL DEFAULT (3 * 3),\n  `i` int(11) DEFAULT (`id` + `n`),\n  PRIMARY KEY",
    );
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD COLUMN h INT NOT NULL DEFAULT (3 * 3), ADD COLUMN i INT DEFAULT (id + n)",
        &more,
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((3, 0)));
    assert_eq!(
        flat(&e, &f, "SELECT id, h, i FROM t").await,
        "1,9,2 | 2,9,4 | 3,9,6"
    );
    ok(&e, &f, "INSERT INTO t (id, n) VALUES (7, 5)").await;
    assert_eq!(
        flat(&e, &f, "SELECT h, i FROM t WHERE id = 7").await,
        "9,12"
    );
    // Dropping the constraint frees the rows again.
    change(
        &e,
        &mut f,
        "ALTER TABLE t DROP CONSTRAINT big",
        &more.replace(",\n  CONSTRAINT `big` CHECK (`a` < 1000)", ""),
    )
    .await
    .unwrap();
    ok(&e, &f, "UPDATE t SET a = a * 10 WHERE id = 3").await;
}

#[test]
fn the_sessions_wait_for_definition_changes_is_read() {
    use crate::engine::control::{LockWait, metadata_wait};
    assert_eq!(
        metadata_wait(b"SET lock_wait_timeout = 1"),
        Some(LockWait::Seconds(1))
    );
    assert_eq!(
        metadata_wait(b"set session lock_wait_timeout=30, autocommit=1"),
        Some(LockWait::Seconds(30))
    );
    assert_eq!(
        metadata_wait(b"SET @@lock_wait_timeout = DEFAULT"),
        Some(LockWait::Default)
    );
    assert_eq!(metadata_wait(b"SET innodb_lock_wait_timeout = 1"), None);
    assert_eq!(metadata_wait(b"SET GLOBAL lock_wait_timeout = 1"), None);
    assert_eq!(metadata_wait(b"SELECT 1"), None);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one session of statements
async fn generated_columns_follow_the_columns_they_read() {
    let (e, _d) = mem();
    let v1 = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL,\n  `s` int(11) GENERATED ALWAYS AS (`a` + `b`) STORED,\n  `v` int(11) GENERATED ALWAYS AS (`a` * 2) VIRTUAL,\n  `w` int(11) GENERATED ALWAYS AS (`a` - `b`) VIRTUAL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `us` (`w`),\n  KEY `ks` (`s`),\n  KEY `kv` (`v`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    let mut f = with(v1);
    let m = Env {
        mysql: true,
        ..Env::STRICT
    };
    ok(
        &e,
        &f,
        "INSERT INTO t (id, a, b) VALUES (1, 1, 2), (2, 10, NULL)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO t (id, a, b, s, v) VALUES (3, 3, 2, DEFAULT, DEFAULT)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO t VALUES (4, 4, 4, DEFAULT, DEFAULT, DEFAULT)",
    )
    .await;
    // A value for a generated column: each server's refusal.
    let given = "INSERT INTO t (id, a, b, s) VALUES (5, 1, 2, 99)";
    assert_eq!(
        err_as(&e, &f, given, Env::STRICT).await,
        (
            1906,
            "The value specified for generated column 's' in table 't' has been ignored".into()
        )
    );
    assert_eq!(
        err_as(&e, &f, given, m).await,
        (
            3105,
            "The value specified for generated column 's' in table 't' is not allowed.".into()
        )
    );
    // The unique key over a generated column holds.
    assert_eq!(
        code(q(&e, &f, "INSERT INTO t (id, a, b) VALUES (6, 1, 1)").await),
        1062
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t").await,
        "1,1,2,3,2,-1 | 2,10,NULL,NULL,20,NULL | 3,3,2,5,6,1 | 4,4,4,8,8,0"
    );
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE s = 8").await, "4");
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE v = 20").await, "2");
    // An UPDATE of a column they read changes them and their index entries.
    ok(&e, &f, "UPDATE t SET a = 7 WHERE id = 1").await;
    ok(&e, &f, "UPDATE t SET s = DEFAULT, a = 8 WHERE id = 1").await;
    assert_eq!(
        err_as(&e, &f, "UPDATE t SET s = 5 WHERE id = 1", Env::STRICT)
            .await
            .0,
        1906
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM t WHERE id = 1").await,
        "1,8,2,10,16,6"
    );
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE v = 16").await, "1");
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE v = 2").await, "");
    // New generated columns get their values for the rows there are (the
    // servers report the rows for a STORED one only); a changed expression
    // is worked out again.
    let v2 = v1
        .replace("(`a` - `b`) VIRTUAL", "(`a` + `b` + 1) VIRTUAL")
        .replace(
            "  PRIMARY KEY",
            "  `g` int(11) GENERATED ALWAYS AS (`a` + 100) STORED,\n  PRIMARY KEY",
        );
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD COLUMN g INT AS (a + 100) STORED, MODIFY w INT AS (a + b + 1) VIRTUAL",
        &v2,
    )
    .await
    .unwrap();
    assert_eq!(rows, Some((4, 0)));
    assert_eq!(
        flat(&e, &f, "SELECT id, w, g FROM t").await,
        "1,11,108 | 2,NULL,110 | 3,6,103 | 4,9,104"
    );
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE w = 9").await, "4");
    let v3 = v2.replace(
        "  PRIMARY KEY",
        "  `h` int(11) GENERATED ALWAYS AS (`b` + 100) VIRTUAL,\n  PRIMARY KEY",
    );
    let rows = change(
        &e,
        &mut f,
        "ALTER TABLE t ADD COLUMN h INT AS (b + 100) VIRTUAL",
        &v3,
    )
    .await
    .unwrap();
    assert_eq!(rows, None);
    assert_eq!(
        flat(&e, &f, "SELECT id, h FROM t").await,
        "1,102 | 2,NULL | 3,102 | 4,104"
    );
}

/// How long a keyspace lookup and a row decode take when many threads use
/// one table at once (the prepared point select's share of the engine's
/// bookkeeping): `cargo test -p sprig-gateway --lib -- --ignored
/// --nocapture keyspace_lookups`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn keyspace_lookups_from_many_threads() {
    let (e, _d) = mem();
    let f = with(T0);
    ok(&e, &f, "INSERT INTO t VALUES (1, 10, 'one', 1)").await;
    let d = def(&f);
    let sp = e.space(TENANT.0, &d).await.unwrap();
    let key = keys::row_key(&sp, &d, &[Datum::Int(1)]).unwrap();
    let raw = e
        .begin(TxnOptions::default())
        .unwrap()
        .get(&key)
        .unwrap()
        .unwrap();
    drop(sp);
    let rounds: u32 = 400_000;
    for threads in [1u32, 4, 16] {
        let start = std::time::Instant::now();
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..rounds {
                        let sp = e.cached(TENANT.0, 20, &d).unwrap();
                        let r = crate::engine::row::decode(&d, &sp, &key, &raw).unwrap();
                        std::hint::black_box(r);
                    }
                });
            }
        });
        println!(
            "{threads} threads: {:.0} ns per lookup and decode",
            start.elapsed().as_secs_f64() * 1e9 / f64::from(rounds)
        );
    }
}

/// `f`'s output and how many times it was polled: every poll after the
/// first is one return to the runtime.
async fn polled<F: std::future::Future>(f: F) -> (F::Output, usize) {
    let mut f = std::pin::pin!(f);
    let mut n = 0;
    let out = std::future::poll_fn(|cx| {
        n += 1;
        f.as_mut().poll(cx)
    })
    .await;
    (out, n)
}

/// Long loops over rows go back to the runtime between batches, whether
/// or not anything they await has to wait: a loop that ran to its end in
/// one poll held a runtime worker for minutes on a big table, and the
/// stall watchdog stopped it (1317). The in-memory store with syncs off
/// is the worst case: every await in these loops is ready at once.
#[tokio::test]
async fn long_loops_over_rows_go_back_to_the_runtime() {
    const ROWS: usize = 5_000;
    let (e, d) = mem();
    let _d = d;
    let mut f = with(T0);
    let mut sql = String::from("INSERT INTO t (id, a, s, n) VALUES ");
    for i in 1..=ROWS {
        if i > 1 {
            sql.push(',');
        }
        let _ = write!(sql, "({i}, {}, 's{i}', {})", i % 97, i % 7);
    }
    // At least one return per 1,024 rows.
    let least = ROWS / 1024;
    let (o, n) = polled(q(&e, &f, &sql)).await;
    assert_eq!(o.unwrap().affected, ROWS as u64);
    assert!(n > least, "a {ROWS}-row INSERT was polled {n} times");

    let (o, n) = polled(q(&e, &f, "SELECT * FROM t WHERE n > 100")).await;
    assert!(o.unwrap().rows.is_empty());
    assert!(n > least, "a {ROWS}-row scan was polled {n} times");
    let (o, n) = polled(q(&e, &f, "SELECT id FROM t WHERE a >= 0")).await;
    assert_eq!(o.unwrap().rows.len(), ROWS);
    assert!(n > least, "a {ROWS}-row index range was polled {n} times");
    let (o, n) = polled(q(&e, &f, "UPDATE t SET n = n + 1")).await;
    assert_eq!(o.unwrap().affected, ROWS as u64);
    assert!(n > 2 * least, "a {ROWS}-row UPDATE was polled {n} times");

    // An index build (rows stay) and a copying change (another primary key).
    let with_s = T0.replace(
        "  KEY `ka` (`a`)\n",
        "  KEY `ka` (`a`),\n  KEY `ks` (`s`)\n",
    );
    let (r, n) = polled(change(
        &e,
        &mut f,
        "ALTER TABLE t ADD INDEX ks (s)",
        &with_s,
    ))
    .await;
    r.unwrap();
    assert!(
        n > least,
        "an index build over {ROWS} rows was polled {n} times"
    );
    let rekeyed = with_s.replace("PRIMARY KEY (`id`)", "PRIMARY KEY (`n`,`id`)");
    let (r, n) = polled(change(
        &e,
        &mut f,
        "ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (n, id)",
        &rekeyed,
    ))
    .await;
    r.unwrap();
    assert!(n > least, "a {ROWS}-row table copy was polled {n} times");
    assert_eq!(flat(&e, &f, "SELECT id FROM t WHERE s = 's77'").await, "77");

    let (o, n) = polled(q(&e, &f, "DELETE FROM t")).await;
    assert_eq!(o.unwrap().affected, ROWS as u64);
    assert!(n > 2 * least, "a {ROWS}-row DELETE was polled {n} times");
}

/// The keys of table 20 and the drop marks of the tenant.
fn left(e: &Engine) -> (usize, usize) {
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let p = keys::object_prefix(TENANT.0, 20);
    let rows = tx
        .scan(&p, &keys::prefix_end(&p), false, 100_000)
        .unwrap()
        .len();
    let m = keys::drops_prefix(TENANT.0);
    let marks = tx
        .scan(&m, &keys::prefix_end(&m), false, 100)
        .unwrap()
        .len();
    tx.rollback();
    (rows, marks)
}

#[tokio::test]
async fn a_sweep_deletes_what_dropped_tables_left_behind() {
    let live = |v: &[u32]| {
        v.iter()
            .copied()
            .collect::<std::collections::HashSet<u32>>()
    };
    // Marked dropped, the process stopped before the deletion: the next
    // sweep deletes every key and the mark.
    let (e, f, _d) = three_rows().await;
    assert!(left(&e).0 > 3);
    e.mark_dropped(TENANT.0, &[20]).await.unwrap();
    assert_eq!(left(&e).1, 1);
    assert_eq!(e.sweep(TENANT.0, &live(&[]), &[]).await.unwrap(), 1);
    assert_eq!(left(&e), (0, 0));
    drop(f);
    // A mark whose drop the catalog never published: the table stays.
    let (e, f, _d) = three_rows().await;
    let before = left(&e).0;
    e.mark_dropped(TENANT.0, &[20]).await.unwrap();
    assert_eq!(e.sweep(TENANT.0, &live(&[20]), &[]).await.unwrap(), 0);
    assert_eq!(left(&e), (before, 1));
    assert_eq!(flat(&e, &f, "SELECT id FROM t").await, "1 | 2 | 3");
    // No mark (cut short before marks existed), but the catalog records
    // the table as dropped: deleted too, and only once.
    let (e, _f, _d) = three_rows().await;
    assert_eq!(e.sweep(TENANT.0, &live(&[]), &[20]).await.unwrap(), 1);
    assert_eq!(left(&e), (0, 0));
    assert_eq!(e.sweep(TENANT.0, &live(&[]), &[20]).await.unwrap(), 0);
    // A finished drop leaves no mark.
    let (e, _f, _d) = three_rows().await;
    e.mark_dropped(TENANT.0, &[20]).await.unwrap();
    e.drop_table(TENANT.0, 20).await.unwrap();
    assert_eq!(left(&e), (0, 0));
}

/// Not a test: a tool for the live proof that a sweep finishes a drop a
/// killed process left. With `RF_ENGINE_DIR` (an engine directory no
/// process has open) and `RF_TENANT` set, prints the number of keys of
/// each SQL table object of the tenant, and its drop marks.
#[tokio::test]
#[ignore = "reads a stopped node's engine directory named by RF_ENGINE_DIR"]
async fn engine_dir_keys_per_object() {
    let (Ok(dir), Ok(t)) = (std::env::var("RF_ENGINE_DIR"), std::env::var("RF_TENANT")) else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let tenant: u64 = t.parse().unwrap();
    let e = open(std::path::Path::new(&dir), StoreKind::Rocks, SyncMode::Off);
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let mut object: u32 = 1;
    let end = keys::object_prefix(tenant, u32::MAX);
    while let Some((k, _)) = tx
        .scan(&keys::object_prefix(tenant, object), &end, false, 1)
        .unwrap()
        .into_iter()
        .next()
    {
        let found = u32::from_be_bytes(k[12..16].try_into().unwrap());
        let p = keys::object_prefix(tenant, found);
        let n = tx
            .scan(&p, &keys::prefix_end(&p), false, 50_000_000)
            .unwrap()
            .len();
        println!("object {found}: {n} keys");
        object = found + 1;
    }
    let m = keys::drops_prefix(tenant);
    for (k, _) in tx.scan(&m, &keys::prefix_end(&m), false, 1000).unwrap() {
        let o = u32::from_be_bytes(k[m.len()..m.len() + 4].try_into().unwrap());
        println!("drop mark: object {o}");
    }
    tx.rollback();
}
