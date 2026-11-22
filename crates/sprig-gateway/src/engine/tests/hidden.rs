//! Tables without a primary key: a hidden row number keys the rows, or the
//! first unique key over whole `NOT NULL` columns stands for the primary
//! key, as on both servers (observed on MariaDB 11.8 and MySQL 8.4).

use sprig_proto_mysql::resultset::ColumnFlags;

use super::*;

const NOKEY: &str = "CREATE TABLE `nk` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(5) DEFAULT NULL,\n  KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const PROMOTED: &str = "CREATE TABLE `t2` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  UNIQUE KEY `ua` (`a`),\n  UNIQUE KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const PREFIXED: &str = "CREATE TABLE `t4` (\n  `v` varchar(10) NOT NULL,\n  UNIQUE KEY `v` (`v`(3))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

fn with_tables() -> Fake {
    let mut f = Fake::new();
    for (n, o, sql) in [
        ("nk", 30, NOKEY),
        ("t2", 31, PROMOTED),
        ("t4", 32, PREFIXED),
    ] {
        let p = table::parse(ObjectId(o), 1, DB, sql);
        assert!(p.is_ok(), "{p:?}");
        f.tables.insert(n.into(), Arc::new(p));
    }
    f
}

fn def_of(f: &Fake, n: &str) -> Arc<table::TableDef> {
    match &*f.tables[n] {
        Ok(d) => Arc::clone(d),
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn which_key_keys_the_rows() {
    let f = with_tables();
    let nk = def_of(&f, "nk");
    assert!(nk.hidden);
    assert_eq!(nk.pk, vec![2]);
    assert_eq!(nk.indexes.len(), 1);
    let t2 = def_of(&f, "t2");
    assert!(!t2.hidden);
    assert_eq!(t2.pk, vec![0]);
    assert_eq!(t2.pk_name, "ua");
    assert_eq!(t2.indexes.len(), 1);
    // The flags both servers report: `a` is the primary key, `b` unique.
    let pri = ColumnFlags::PRI_KEY.bits();
    assert_ne!(t2.columns[0].flags & pri, 0);
    assert_eq!(t2.columns[0].flags & ColumnFlags::UNIQUE_KEY.bits(), 0);
    assert_ne!(t2.columns[1].flags & ColumnFlags::UNIQUE_KEY.bits(), 0);
    // A key on a column prefix can't stand for the primary key.
    let t4 = def_of(&f, "t4");
    assert!(t4.hidden);
    assert_eq!(t4.columns[0].flags & pri, 0);
}

#[tokio::test]
async fn rows_without_a_key_are_numbered() {
    let (e, _d) = mem();
    let f = with_tables();
    let o = run(
        &e,
        &f,
        "INSERT INTO nk VALUES (3,'x'),(1,'y'),(2,'z'),(3,'x')",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.affected, 4);
    // In the order they were inserted, duplicates and all.
    let all = run(&e, &f, "SELECT a, b FROM nk", &[]).await.unwrap().rows;
    let want: Vec<Vec<Datum>> = [(3, "x"), (1, "y"), (2, "z"), (3, "x")]
        .iter()
        .map(|(a, b)| vec![Datum::Int(*a), s(b)])
        .collect();
    assert_eq!(all, want);
    // Through the index on `b`.
    let o = run(&e, &f, "SELECT a FROM nk WHERE b = 'x'", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(3)], vec![Datum::Int(3)]]);
    // An update keeps each row's place and moves its index entry.
    let u = run(&e, &f, "UPDATE nk SET b = 'w' WHERE a = 1", &[])
        .await
        .unwrap();
    assert_eq!(u.affected, 1);
    let o = run(&e, &f, "SELECT a FROM nk WHERE b = 'w'", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(1)]]);
    let o = run(&e, &f, "SELECT a FROM nk WHERE b = 'y'", &[])
        .await
        .unwrap();
    assert!(o.rows.is_empty());
    let d = run(&e, &f, "DELETE FROM nk WHERE a = 3", &[])
        .await
        .unwrap();
    assert_eq!(d.affected, 2);
    let all = run(&e, &f, "SELECT a, b FROM nk", &[]).await.unwrap().rows;
    assert_eq!(
        all,
        vec![vec![Datum::Int(1), s("w")], vec![Datum::Int(2), s("z")]]
    );
    assert!(
        run(&e, &f, "SELECT a FROM nk WHERE b = 'x'", &[])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    // A new process (the counter forgotten) numbers on after the last
    // row.
    e.rowids.lock().unwrap().clear();
    run(&e, &f, "INSERT INTO nk VALUES (9,'n')", &[])
        .await
        .unwrap();
    let all = run(&e, &f, "SELECT a FROM nk", &[]).await.unwrap().rows;
    assert_eq!(
        all,
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(9)]
        ]
    );
}

#[tokio::test]
async fn a_unique_key_stands_for_the_primary_key() {
    let (e, _d) = mem();
    let f = with_tables();
    run(&e, &f, "INSERT INTO t2 VALUES (3,NULL),(1,5),(2,4)", &[])
        .await
        .unwrap();
    // The rows are in the key's order.
    let all = run(&e, &f, "SELECT a FROM t2", &[]).await.unwrap().rows;
    assert_eq!(
        all,
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    let p = bind(&f, "SELECT b FROM t2 WHERE a = 1").unwrap().unwrap();
    assert!(matches!(p.access, Access::Point(_)));
    let dup = run(&e, &f, "INSERT INTO t2 VALUES (1,7)", &[]).await;
    assert_eq!(dup.unwrap_err().message, "Duplicate entry '1' for key 'ua'");
    let dup = run(&e, &f, "INSERT INTO t2 VALUES (8,5)", &[]).await;
    assert_eq!(dup.unwrap_err().message, "Duplicate entry '5' for key 'b'");
}
