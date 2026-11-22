//! Foreign keys enforced on engine tables (the servers' errors and actions,
//! observed on MariaDB 11.8 and MySQL 8.4).

use super::*;
use crate::engine::assign::Env;

const P: &str = "CREATE TABLE `p` (\n  `id` int(11) NOT NULL,\n  `u` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `u` (`u`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const C: &str = "CREATE TABLE `c` (\n  `id` int(11) NOT NULL,\n  `pid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `fk1` (`pid`),\n  CONSTRAINT `fk1` FOREIGN KEY (`pid`) REFERENCES `p` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const CC: &str = "CREATE TABLE `cc` (\n  `id` int(11) NOT NULL,\n  `pid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `pid` (`pid`),\n  CONSTRAINT `cc_ibfk_1` FOREIGN KEY (`pid`) REFERENCES `p` (`id`) ON DELETE CASCADE ON UPDATE SET NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const S: &str = "CREATE TABLE `s` (\n  `id` int(11) NOT NULL,\n  `par` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `par` (`par`),\n  CONSTRAINT `s_ibfk_1` FOREIGN KEY (`par`) REFERENCES `s` (`id`) ON DELETE CASCADE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

fn with_tables() -> Fake {
    let mut f = Fake::new();
    for (n, o, sql) in [("p", 40, P), ("c", 41, C), ("cc", 42, CC), ("s", 43, S)] {
        let t = table::parse(ObjectId(o), 1, DB, sql);
        assert!(t.is_ok(), "{t:?}");
        f.tables.insert(n.into(), Arc::new(t));
    }
    f
}

async fn rows(e: &Engine, f: &Fake, sql: &str) -> Vec<Vec<Datum>> {
    run(e, f, sql, &[]).await.unwrap().rows
}

#[tokio::test]
async fn children_need_parents_and_parents_keep_children() {
    let (e, _d) = mem();
    let f = with_tables();
    let err = run(&e, &f, "INSERT INTO c VALUES (1, 10)", &[])
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1452);
    assert_eq!(
        err.message,
        "Cannot add or update a child row: a foreign key constraint fails (`sbtest`.`c`, CONSTRAINT `fk1` FOREIGN KEY (`pid`) REFERENCES `p` (`id`))"
    );
    // A NULL key needs no parent.
    run(&e, &f, "INSERT INTO c VALUES (1, NULL)", &[])
        .await
        .unwrap();
    run(&e, &f, "INSERT INTO p VALUES (10, 100), (20, 200)", &[])
        .await
        .unwrap();
    run(&e, &f, "INSERT INTO c VALUES (2, 10)", &[])
        .await
        .unwrap();
    // The duplicate key is reported before the missing parent.
    assert_eq!(
        code(run(&e, &f, "INSERT INTO c VALUES (2, 99)", &[]).await),
        1062
    );
    assert_eq!(
        code(run(&e, &f, "UPDATE c SET pid = 99 WHERE id = 2", &[]).await),
        1452
    );
    let del = run(&e, &f, "DELETE FROM p WHERE id = 10", &[])
        .await
        .unwrap_err();
    assert_eq!(del.protocol().mysql_errno, 1451);
    assert_eq!(
        code(run(&e, &f, "UPDATE p SET id = 11 WHERE id = 10", &[]).await),
        1451
    );
    // Columns no key uses change freely; an unreferenced parent goes.
    run(&e, &f, "UPDATE p SET u = 101 WHERE id = 10", &[])
        .await
        .unwrap();
    run(&e, &f, "DELETE FROM p WHERE id = 20", &[])
        .await
        .unwrap();
    // foreign_key_checks = 0.
    let off = Env {
        no_fk_checks: true,
        ..Env::STRICT
    };
    let p = bind(&f, "INSERT INTO c VALUES (3, 77)").unwrap().unwrap();
    e.run_env(TENANT.0, &p, &[], false, TxnOptions::default(), off)
        .await
        .unwrap();
}

#[tokio::test]
async fn cascades_and_set_null() {
    let (e, _d) = mem();
    let f = with_tables();
    run(&e, &f, "INSERT INTO p VALUES (10, 100), (20, 200)", &[])
        .await
        .unwrap();
    run(
        &e,
        &f,
        "INSERT INTO cc VALUES (1, 10), (2, 10), (3, 20)",
        &[],
    )
    .await
    .unwrap();
    let d = run(&e, &f, "DELETE FROM p WHERE id = 10", &[])
        .await
        .unwrap();
    assert_eq!(d.affected, 1);
    assert_eq!(
        rows(&e, &f, "SELECT id, pid FROM cc").await,
        vec![vec![Datum::Int(3), Datum::Int(20)]]
    );
    run(&e, &f, "UPDATE p SET id = 21 WHERE id = 20", &[])
        .await
        .unwrap();
    assert_eq!(
        rows(&e, &f, "SELECT id, pid FROM cc").await,
        vec![vec![Datum::Int(3), Datum::Null]]
    );
}

#[tokio::test]
async fn a_table_that_references_itself() {
    let (e, _d) = mem();
    let f = with_tables();
    run(
        &e,
        &f,
        "INSERT INTO s VALUES (1, NULL), (2, 1), (3, 2), (5, 5)",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        code(run(&e, &f, "INSERT INTO s VALUES (6, 7), (7, 6)", &[]).await),
        1452
    );
    let d = run(&e, &f, "DELETE FROM s WHERE id IN (1, 2)", &[])
        .await
        .unwrap();
    assert_eq!(d.affected, 1);
    assert_eq!(
        rows(&e, &f, "SELECT id FROM s").await,
        vec![vec![Datum::Int(5)]]
    );
}
