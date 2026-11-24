//! Column types through the SQL backend: definitions, the stored row and
//! keys, the assignment rules, comparisons. The expected values and
//! conditions are what MariaDB 11.8 answered for the same statements
//! (MySQL 8.4 where the test says so).

use sprig_engine::row::record;

use super::*;
use crate::engine::assign::Level::{Note as N, Warning as W};
use crate::engine::assign::{Env, Level};
use crate::engine::{keys, meta};

/// Names over the given tables (`name`, object, definition).
fn names(tables: &[(&str, u64, &str)]) -> Fake {
    let mut f = Fake::new();
    for (name, object, sql) in tables {
        let parsed = table::parse(ObjectId(*object), 1, DB, sql);
        assert!(parsed.is_ok(), "{name}: {parsed:?}");
        f.tables.insert((*name).into(), Arc::new(parsed));
    }
    f
}

/// A session whose results are utf8mb4, on MariaDB's port.
const UTF8: meta::Results = meta::Results {
    cs: Some(meta::ResultsCs { id: 224, mbmax: 4 }),
    mysql: false,
};

/// Session rules without strict mode.
fn lax() -> Env {
    Env {
        strict: false,
        ..Env::STRICT
    }
}

async fn q(e: &Engine, f: &Fake, sql: &str) -> sprig_common::Result<Outcome> {
    run(e, f, sql, &[]).await
}

async fn q_env(e: &Engine, f: &Fake, sql: &str, env: Env) -> sprig_common::Result<Outcome> {
    let p = bind(f, sql)?.expect("an engine statement");
    e.run_env(TENANT.0, &p, &[], false, TxnOptions::default(), env)
        .await
}

/// Runs a statement that must succeed.
async fn ok(e: &Engine, f: &Fake, sql: &str) -> Outcome {
    let o = q(e, f, sql).await;
    assert!(o.is_ok(), "{sql}: {o:?}");
    o.unwrap()
}

/// A statement's rows as the text protocol sends them (`NULL` for NULL).
async fn text(e: &Engine, f: &Fake, sql: &str) -> Vec<Vec<String>> {
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
                .collect()
        })
        .collect()
}

/// The rows as one string: columns joined by `,`, rows by ` | `.
async fn flat(e: &Engine, f: &Fake, sql: &str) -> String {
    text(e, f, sql)
        .await
        .iter()
        .map(|r| r.join(","))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The error number and message of a statement that must fail.
async fn err(e: &Engine, f: &Fake, sql: &str) -> (u16, String) {
    let x = q(e, f, sql).await.expect_err(sql);
    (x.protocol().mysql_errno, x.message)
}

/// The conditions a statement raised outside strict mode: `(level, code)`.
async fn conds(e: &Engine, f: &Fake, sql: &str) -> Vec<(Level, u16)> {
    q_env(e, f, sql, lax())
        .await
        .unwrap()
        .conds
        .iter()
        .map(|c| (c.level, c.code))
        .collect()
}

const TI: &str = "CREATE TABLE `ti` (\n  `id` int(11) NOT NULL,\n  `a` tinyint(4) DEFAULT NULL,\n  `b` int(10) unsigned DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  `d` bigint(20) unsigned DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every rule the servers showed
async fn integers_are_stored_as_the_servers_store_them() {
    let (e, _d) = mem();
    let f = names(&[("ti", 20, TI)]);
    // The bounds.
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (1, 127, 4294967295, 9223372036854775807, 18446744073709551615)",
    )
    .await;
    for (sql, col) in [
        ("INSERT INTO ti VALUES (2, 128, 0, 0, 0)", "a"),
        ("INSERT INTO ti VALUES (2, -129, 0, 0, 0)", "a"),
        ("INSERT INTO ti VALUES (2, 1, -1, 0, 0)", "b"),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 9223372036854775808, 0)",
            "c",
        ),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 0, 18446744073709551616)",
            "d",
        ),
        (
            "INSERT INTO ti VALUES (2, 0, 0, '9223372036854775808', 0)",
            "c",
        ),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 0, '18446744073709551616')",
            "d",
        ),
        // A negative decimal is out of an unsigned column's range even
        // when it rounds to zero; the same number in a string is not.
        ("INSERT INTO ti VALUES (2, 0, 0, 0, -0.4)", "d"),
        ("INSERT INTO ti VALUES (2, 0, 0, 0, '-0.5')", "d"),
        ("INSERT INTO ti VALUES (2, -128.5, 0, 0, 0)", "a"),
        ("INSERT INTO ti VALUES (2, 127.5, 0, 0, 0)", "a"),
        ("INSERT INTO ti VALUES (2, '127.5', 0, 0, 0)", "a"),
        ("INSERT INTO ti VALUES (2, 1e2, 1e10, 0, 0)", "b"),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 0, 18446744073709551615e0)",
            "d",
        ),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 0xffffffffffffffff, 0)",
            "c",
        ),
        (
            "INSERT INTO ti VALUES (2, 0, 0, 0, 0x01ffffffffffffffff)",
            "d",
        ),
        ("INSERT INTO ti VALUES (2, '1e400', 0, 0, 0)", "a"),
    ] {
        assert_eq!(
            err(&e, &f, sql).await,
            (
                1264,
                format!("Out of range value for column '{col}' at row 1")
            ),
            "{sql}"
        );
    }
    // Decimals round half away from zero, doubles half to even, strings
    // as decimals.
    ok(&e, &f, "INSERT INTO ti VALUES (2, 1.5, 2.5, 3.5, 4.5)").await;
    ok(&e, &f, "INSERT INTO ti VALUES (3, 1.4, 2.4, -3.5, 4.4)").await;
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (4, 1.5e0, 2.5e0, 3.5e0, 4.5e0)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (5, '12', ' 13', '14.6', '1e2')",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (6, '1.5', '2.5', '-3.5', '4.5')",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (7, TRUE, FALSE, 0x10, b'11')",
    )
    .await;
    ok(&e, &f, "INSERT INTO ti VALUES (8, 1e2, 1e9, 1e18, 1e19)").await;
    ok(&e, &f, "INSERT INTO ti VALUES (9, 0, -0.4e0, 0, '-0.4')").await;
    ok(&e, &f, "INSERT INTO ti VALUES (10, -128.4, 4294967295.4, 9223372036854775807.4, 18446744073709551615.4)").await;
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (11, '-0', '.5', '5.', '1.5e1')",
    )
    .await;
    // 2^63 as a double is MariaDB's largest BIGINT.
    ok(
        &e,
        &f,
        "INSERT INTO ti VALUES (12, 0, 0, 9223372036854775807e0, 0)",
    )
    .await;
    assert_eq!(
        flat(&e, &f, "SELECT * FROM ti WHERE id > 1").await,
        "2,2,3,4,5 | 3,1,2,-4,4 | 4,2,2,4,4 | 5,12,13,15,100 | 6,2,3,-4,5 | 7,1,0,16,3 | \
         8,100,1000000000,1000000000000000000,10000000000000000000 | 9,0,0,0,0 | \
         10,-128,4294967295,9223372036854775807,18446744073709551615 | 11,0,1,5,15 | \
         12,0,0,9223372036854775807,0"
    );
    // Text that is no number, and a number followed by text.
    assert_eq!(
        err(&e, &f, "INSERT INTO ti VALUES (20, 'abc', 0, 0, 0)").await,
        (
            1366,
            "Incorrect integer value: 'abc' for column `sbtest`.`ti`.`a` at row 1".into()
        )
    );
    for bad in ["''", "' '", "'.'", "'-'", "'e5'", "'--1'"] {
        let sql = format!("INSERT INTO ti VALUES (20, {bad}, 0, 0, 0)");
        assert_eq!(err(&e, &f, &sql).await.0, 1366, "{bad}");
    }
    for bad in ["'12abc'", "'5 x'", "'0x10'", "'1e'"] {
        let sql = format!("INSERT INTO ti VALUES (20, {bad}, 0, 0, 0)");
        assert_eq!(
            err(&e, &f, &sql).await,
            (1265, "Data truncated for column 'a' at row 1".into()),
            "{bad}"
        );
    }
    // The row of a multi-row INSERT is named, and nothing of it stays.
    assert_eq!(
        err(
            &e,
            &f,
            "INSERT INTO ti VALUES (20, 1, 2, 3, 4), (21, 300, 2, 3, 4)"
        )
        .await,
        (1264, "Out of range value for column 'a' at row 2".into())
    );
    assert!(
        flat(&e, &f, "SELECT id FROM ti WHERE id = 20")
            .await
            .is_empty()
    );
    // Trailing white space: stored, with MariaDB's note.
    let o = ok(&e, &f, "INSERT INTO ti VALUES (22, '13 ', '1e2 ', 0, 0)").await;
    assert_eq!(o.warnings(), 2);
    assert!(
        o.conds
            .iter()
            .all(|c| c.level == Level::Note && c.code == 1265)
    );
    // MySQL says nothing about it.
    let my = Env {
        mysql: true,
        ..Env::STRICT
    };
    let o = q_env(&e, &f, "INSERT INTO ti VALUES (23, '13 ', 0, 0, 0)", my)
        .await
        .unwrap();
    assert_eq!(o.warnings(), 0);
    // And MySQL refuses 2^63 as a double.
    let r = q_env(
        &e,
        &f,
        "INSERT INTO ti VALUES (24, 0, 0, 9223372036854775807e0, 0)",
        my,
    )
    .await;
    assert_eq!(code(r), 1264);

    // Outside strict mode the nearest value is stored and the warning
    // counted.
    assert_eq!(
        conds(&e, &f, "INSERT INTO ti VALUES (30, 128, 0, 0, 0)").await,
        vec![(W, 1264)]
    );
    assert_eq!(
        conds(&e, &f, "INSERT INTO ti VALUES (31, -129, -1, 0, -1)").await,
        vec![(W, 1264), (W, 1264), (W, 1264)]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO ti VALUES (32, '12abc', 'abc', '', '1e30')"
        )
        .await,
        vec![(W, 1265), (W, 1366), (W, 1366), (W, 1264)]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO ti VALUES (33, 1e30, -1e30, 1e30, 1e30)"
        )
        .await,
        vec![(W, 1264); 4]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO ti VALUES (34, '1.5x', '-5', '99999999999999999999', '-1')"
        )
        .await,
        vec![(W, 1265), (W, 1264), (W, 1264), (W, 1264)]
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM ti WHERE id BETWEEN 30 AND 34").await,
        "30,127,0,0,0 | 31,-128,0,0,0 | 32,12,0,0,18446744073709551615 | \
         33,127,0,9223372036854775807,18446744073709551615 | 34,2,0,9223372036854775807,0"
    );
    let u = q_env(&e, &f, "UPDATE ti SET a = 300 WHERE id = 34", lax())
        .await
        .unwrap();
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 1  Warnings: 1")
    );
    assert_eq!(u.warnings(), 1);
    // NULL in a NOT NULL column: an error for one row in either mode, the
    // type's default with a warning in a multi-row INSERT outside strict
    // mode.
    let r = q_env(&e, &f, "INSERT INTO ti VALUES (NULL, 1, 1, 1, 1)", lax()).await;
    assert_eq!(code(r), 1048);
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO ti VALUES (NULL, 1, 1, 1, 1), (40, 1, 1, 1, 1)"
        )
        .await,
        vec![(W, 1048)]
    );
    assert_eq!(
        flat(&e, &f, "SELECT id FROM ti WHERE id IN (0, 40)").await,
        "0 | 40"
    );
}

#[tokio::test]
async fn integer_keys_take_constants_as_the_comparison_sees_them() {
    let (e, _d) = mem();
    let f = names(&[("ti", 20, TI)]);
    q(
        &e,
        &f,
        "INSERT INTO ti VALUES (1, 13, 1, 1, 1), (2, 13, 2, 2, 2), (3, 14, 3, 3, 3), \
         (12, 0, 0, 9223372036854775807, 18446744073709551615), (13, 0, 0, 0, 0)",
    )
    .await
    .unwrap();
    for (cond, want) in [
        ("id = '1'", "1"),
        ("id = '1x'", "1"),
        ("id = 1.0", "1"),
        ("id = '1.0'", "1"),
        ("id = 1.5", ""),
        ("id = '1.5'", ""),
        ("id = 1e0", "1"),
        ("id = 99999999999", ""),
        ("id = -1", ""),
        ("id = NULL", ""),
        ("id > 12.5", "13"),
        ("id >= '12.5'", "13"),
        ("id BETWEEN 2.5 AND '4x'", "3"),
        ("id IN (1, '2', 3.0, 4.5)", "1 | 2 | 3"),
        ("a = '13'", "1 | 2"),
        ("a = 13.0", "1 | 2"),
        ("a = 13.5", ""),
        ("a = '1.3e1'", "1 | 2"),
        ("d = 18446744073709551615", "12"),
        ("d = -1", ""),
        ("c = 9223372036854775807", "12"),
        ("c = 9223372036854775807.0", "12"),
        ("c = '9223372036854775807'", "12"),
        ("id < 3 AND a = 13", "1 | 2"),
    ] {
        let sql = format!("SELECT id FROM ti WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    // The key is read where the constant is a key value, and nothing is
    // read where no row can match.
    let o = q(&e, &f, "SELECT id FROM ti WHERE id = '2'").await.unwrap();
    assert_eq!((o.examined, o.no_index), (1, false));
    let o = q(&e, &f, "SELECT id FROM ti WHERE id = 1.5").await.unwrap();
    assert_eq!(o.examined, 0);
    // DML through the same paths.
    q(&e, &f, "UPDATE ti SET a = a + 1 WHERE id = '1'")
        .await
        .unwrap();
    assert_eq!(
        code(q(&e, &f, "UPDATE ti SET a = a * 20 WHERE id = 1").await),
        1264
    );
    q(&e, &f, "DELETE FROM ti WHERE id = '2.0'").await.unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT id, a FROM ti WHERE id < 3").await,
        "1,14"
    );
}

const STRS: &str = "CREATE TABLE `strs` (\n  `id` int(11) NOT NULL,\n  `c` char(4) DEFAULT NULL,\n  `v` varchar(5) DEFAULT NULL,\n  `t` tinytext DEFAULT NULL,\n  `x` text DEFAULT NULL,\n  `l` varchar(5) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,\n  `a` char(3) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n  `u` varchar(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,\n  `m` mediumtext DEFAULT NULL,\n  `g` longtext DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `l` (`l`),\n  KEY `x` (`x`(3))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every string rule
async fn strings_are_stored_in_their_columns_character_set() {
    let (e, _d) = mem();
    let f = names(&[("strs", 21, STRS)]);
    q(
        &e,
        &f,
        "INSERT INTO strs VALUES (1, 'ab  ', 'ab  ', 'tiny', 'text', 'été', 'asc', 'ça', 'med', 'long')",
    )
    .await
    .unwrap();
    // CHAR reads back without its trailing spaces, VARCHAR with them; a
    // latin1 column's text comes back as the session's UTF-8.
    assert_eq!(
        text(
            &e,
            &f,
            "SELECT c, v, t, x, l, a, u, m, g FROM strs WHERE id = 1"
        )
        .await,
        vec![vec![
            "ab", "ab  ", "tiny", "text", "été", "asc", "ça", "med", "long"
        ]]
    );
    // It is stored as latin1: one byte a character.
    let p = bind(&f, "SELECT l FROM strs WHERE id = 1")
        .unwrap()
        .unwrap();
    let o = exec(&e, &p, &[]).await.unwrap();
    assert_eq!(o.rows[0][0], Datum::Str(Bytes::from_static(b"\xE9t\xE9")));
    // The index on it finds it by the collation (accents and case fold
    // in latin1_swedish_ci as the server folds them).
    let o = q(&e, &f, "SELECT id FROM strs WHERE l = 'ÉTÉ'")
        .await
        .unwrap();
    assert_eq!((o.rows.len(), o.examined), (1, 1));
    // A prefix index finds the rows sharing the prefix; the filter is exact.
    q(
        &e,
        &f,
        "INSERT INTO strs (id, x) VALUES (2, 'texture'), (3, 'tex'), (4, 'other')",
    )
    .await
    .unwrap();
    let o = q(&e, &f, "SELECT id FROM strs WHERE x = 'texture'")
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(2)]]);
    assert_eq!(o.examined, 3);
    // Lengths count characters, not bytes.
    q(
        &e,
        &f,
        "INSERT INTO strs (id, c, v) VALUES (5, 'äöüß', 'äöüßé')",
    )
    .await
    .unwrap();
    for (sql, col) in [
        ("INSERT INTO strs (id, c) VALUES (9, 'abcde')", "c"),
        ("INSERT INTO strs (id, v) VALUES (9, 'abcdef')", "v"),
        ("INSERT INTO strs (id, l) VALUES (9, 'abcdef')", "l"),
        ("INSERT INTO strs (id, a) VALUES (9, 'abcd')", "a"),
    ] {
        assert_eq!(
            err(&e, &f, sql).await,
            (1406, format!("Data too long for column '{col}' at row 1")),
            "{sql}"
        );
    }
    let long = format!("INSERT INTO strs (id, t) VALUES (9, '{}')", "x".repeat(256));
    assert_eq!(err(&e, &f, &long).await.0, 1406);
    let fits = format!("INSERT INTO strs (id, t) VALUES (6, '{}')", "x".repeat(255));
    q(&e, &f, &fits).await.unwrap();
    // Only spaces cut: CHAR says nothing, VARCHAR notes it.
    let o = q(
        &e,
        &f,
        "INSERT INTO strs (id, c, v) VALUES (7, 'abcd   ', 'abcde  ')",
    )
    .await
    .unwrap();
    assert_eq!(
        o.conds
            .iter()
            .map(|c| (c.level, c.code))
            .collect::<Vec<_>>(),
        vec![(N, 1265)]
    );
    assert_eq!(
        text(&e, &f, "SELECT c, v FROM strs WHERE id = 7").await,
        vec![vec!["abcd", "abcde"]]
    );
    // A character the column's set doesn't have.
    assert_eq!(
        err(&e, &f, "INSERT INTO strs (id, u) VALUES (9, 'a😀')").await,
        (
            1366,
            "Incorrect string value: '\\xF0\\x9F\\x98\\x80' for column `sbtest`.`strs`.`u` at row 1"
                .into()
        )
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO strs (id, l) VALUES (9, 'a中b')").await,
        (
            1366,
            "Incorrect string value: '\\xE4\\xB8\\xADb' for column `sbtest`.`strs`.`l` at row 1"
                .into()
        )
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO strs (id, a) VALUES (9, 'é')")
            .await
            .0,
        1366
    );
    // Outside strict mode: cut, or `?`, with a warning.
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO strs (id, c, v, l, u) VALUES (8, 'abcde', 'abcdef', 'a中b', 'a😀')"
        )
        .await,
        vec![(W, 1265), (W, 1265), (W, 1366), (W, 1366)]
    );
    assert_eq!(
        text(&e, &f, "SELECT c, v, l, u FROM strs WHERE id = 8").await,
        vec![vec!["abcd", "abcde", "a?b", "a?"]]
    );
    // Numbers are stored as their text.
    q(&e, &f, "INSERT INTO strs (id, v, x) VALUES (10, 12, 1.50)")
        .await
        .unwrap();
    q(&e, &f, "INSERT INTO strs (id, v, x) VALUES (11, 1.5e0, -3)")
        .await
        .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT v, x FROM strs WHERE id IN (10, 11)").await,
        "12,1.50 | 1.5,-3"
    );
    // A string column compared with a number compares as numbers, row by
    // row.
    let o = q(&e, &f, "SELECT id FROM strs WHERE v = 12").await.unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(10)]]);
    // Hexadecimal and bit literals are bytes to a string column.
    q(
        &e,
        &f,
        "INSERT INTO strs (id, v, x) VALUES (12, 0x414243, b'01000001')",
    )
    .await
    .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT v, x FROM strs WHERE id = 12").await,
        "ABC,A"
    );
    // Statement text is compared in the column's character set; text the
    // set can't hold fails as on the servers.
    for (cond, want) in [
        ("l = 'ete'", "1"),
        ("u = 'ÇA'", "1"),
        ("a = 'ASC'", ""),
        ("a = 'asc'", "1"),
        ("c = 'AB'", "1"),
        ("c = 'ab   '", "1"),
        ("v = 'ab'", "1"),
        ("v > 'ab'", ""),
    ] {
        let sql = format!("SELECT id FROM strs WHERE {cond} AND id = 1");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    assert_eq!(
        err(&e, &f, "SELECT id FROM strs WHERE l = 'a中'").await,
        (
            1267,
            "Illegal mix of collations (latin1_swedish_ci,IMPLICIT) and \
             (utf8mb4_uca1400_ai_ci,COERCIBLE) for operation '='"
                .into()
        )
    );
    assert_eq!(
        err(&e, &f, "SELECT id FROM strs WHERE a = 'é'").await.0,
        1267
    );
    assert_eq!(
        err(&e, &f, "SELECT id FROM strs WHERE u = 'a😀'").await.1,
        "Illegal mix of collations (utf8mb3_general_ci,IMPLICIT) and \
         (utf8mb4_uca1400_ai_ci,COERCIBLE) for operation '='"
    );
}

const NAMED_DESC: &str = "CREATE TABLE `nd` (\n  `name` varchar(20) NOT NULL,\n  `k` int(11) NOT NULL,\n  PRIMARY KEY (`name`),\n  KEY `k` (`k` DESC)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
const INT_DESC: &str = "CREATE TABLE `idesc` (\n  `id` int(11) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id` DESC)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
async fn descending_keys_and_string_keys_with_numbers() {
    let (e, _d) = mem();
    let f = names(&[("nd", 22, NAMED_DESC), ("idesc", 23, INT_DESC)]);
    q(
        &e,
        &f,
        "INSERT INTO idesc VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    )
    .await
    .unwrap();
    // The key's own order is the order without ORDER BY.
    assert_eq!(flat(&e, &f, "SELECT id FROM idesc").await, "4 | 3 | 2 | 1");
    assert_eq!(
        flat(&e, &f, "SELECT id FROM idesc ORDER BY id").await,
        "1 | 2 | 3 | 4"
    );
    assert_eq!(
        flat(&e, &f, "SELECT id FROM idesc ORDER BY id DESC LIMIT 2").await,
        "4 | 3"
    );
    let o = q(&e, &f, "SELECT id FROM idesc WHERE id > 1 AND id <= 3")
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(3)], vec![Datum::Int(2)]]);
    assert_eq!(o.examined, 2);
    assert_eq!(
        flat(&e, &f, "SELECT id FROM idesc WHERE id >= 3 ORDER BY id").await,
        "3 | 4"
    );
    q(&e, &f, "UPDATE idesc SET v = v + 1 WHERE id < 3")
        .await
        .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT v FROM idesc ORDER BY id").await,
        "11 | 21 | 30 | 40"
    );
    // A descending index part.
    q(
        &e,
        &f,
        "INSERT INTO nd VALUES ('12', 1), ('012', 2), ('12abc', 2), ('x', 3)",
    )
    .await
    .unwrap();
    let o = q(&e, &f, "SELECT name FROM nd WHERE k = 2").await.unwrap();
    assert_eq!(o.rows.len(), 2);
    assert_eq!(o.examined, 2);
    // A string key compared with a number: every row whose text is that
    // number, found by a scan.
    let o = q(&e, &f, "SELECT name FROM nd WHERE name = 12")
        .await
        .unwrap();
    assert_eq!(o.rows.len(), 3);
    assert!(o.no_index);
    // With a string, the key.
    let o = q(&e, &f, "SELECT k FROM nd WHERE name = '12'")
        .await
        .unwrap();
    assert_eq!((o.rows.len(), o.examined, o.no_index), (1, 1, false));
    let d = q(&e, &f, "DELETE FROM nd WHERE name = 12").await.unwrap();
    assert_eq!(d.affected, 3);
}

#[tokio::test]
async fn new_tables_store_row_records_and_prototype_tables_are_refused() {
    let (e, _d) = mem();
    let f = Fake::new();
    // A table that still carries the prototype format's marker is refused,
    // never read wrongly.
    let named = f.tables["named"].as_ref().as_ref().unwrap().clone();
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    tx.put_new(
        Bytes::from(keys::format_key(TENANT.0, 12)),
        Bytes::copy_from_slice(&[keys::FORMAT_FIRST]),
    )
    .await
    .unwrap();
    tx.finish().await.unwrap();
    let err = e.space(TENANT.0, &named).await.unwrap_err();
    assert!(
        err.message.contains("prototype row format"),
        "{}",
        err.message
    );

    // A new table stores the engine's row record, version 1, and no marker.
    let owned = f.tables["owned"].as_ref().as_ref().unwrap().clone();
    q(&e, &f, "INSERT INTO owned VALUES (7, 'carol', NULL)")
        .await
        .unwrap();
    let sp = e.space(TENANT.0, &owned).await.unwrap();
    assert!(!sp.first_format);
    let key = keys::row_key(&sp, &owned, &[Datum::UInt(7)]).unwrap();
    let mut tx = e.begin(TxnOptions::default()).unwrap();
    let raw = tx.get(&key).unwrap().unwrap();
    let h = record::peek_header(&raw).unwrap();
    assert_eq!((raw[0], h.schema_version, h.expires_at), (1, 1, None));
    assert_eq!(
        record::decode(&owned.schema, &raw).unwrap(),
        vec![Datum::UInt(7), s("carol"), Datum::Null]
    );
    assert!(tx.get(&keys::format_key(TENANT.0, 11)).unwrap().is_none());
}

#[tokio::test]
async fn the_auto_increment_option_is_where_an_empty_table_starts() {
    let (e, _d) = mem();
    let sql = "CREATE TABLE `ai` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    let f = names(&[("ai", 24, sql)]);
    let o = q(&e, &f, "INSERT INTO ai (v) VALUES (1), (2)")
        .await
        .unwrap();
    assert_eq!(o.last_insert_id, 1000);
    // 0 asks for the next value, unless the session says it is a value.
    let o = q(&e, &f, "INSERT INTO ai VALUES (0, 3)").await.unwrap();
    assert_eq!(o.last_insert_id, 1002);
    let zero = Env {
        no_auto_value_on_zero: true,
        ..Env::STRICT
    };
    let o = q_env(&e, &f, "INSERT INTO ai VALUES (0, 4)", zero)
        .await
        .unwrap();
    assert_eq!(o.last_insert_id, 0);
    assert_eq!(
        flat(&e, &f, "SELECT id FROM ai").await,
        "0 | 1000 | 1001 | 1002"
    );
}

/// A node that takes the tenant's leadership loads the counter again from
/// the stored rows; a node that doesn't lead the tenant hands out nothing.
#[tokio::test]
async fn the_auto_increment_counter_is_loaded_again_by_a_new_leader() {
    let (e, _d) = mem();
    let sql = "CREATE TABLE `ai2` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    let f = names(&[("ai2", 25, sql)]);
    let o = q(&e, &f, "INSERT INTO ai2 (v) VALUES (1), (2), (3)")
        .await
        .unwrap();
    assert_eq!(o.last_insert_id, 1);
    // Rows another leader wrote, which this node's memory doesn't know:
    // written here behind the counter's back.
    let unit = sprig_engine::Engine::unit_of_tenant(TENANT.0);
    e.autoinc
        .lock()
        .unwrap()
        .get_mut(&(TENANT.0, 25))
        .unwrap()
        .next = 2;
    let epoch = e.eng.leader_epoch(unit);
    assert_ne!(e.eng.lead(unit), epoch);
    let o = q(&e, &f, "INSERT INTO ai2 (v) VALUES (4)").await.unwrap();
    assert_eq!(o.last_insert_id, 4);
    // Not led here: no value is handed out (retryable).
    e.eng.step_down(unit);
    let err = q(&e, &f, "INSERT INTO ai2 (v) VALUES (5)")
        .await
        .unwrap_err();
    assert!(err.retryable, "{err:?}");
    e.eng.lead(unit);
    let o = q(&e, &f, "INSERT INTO ai2 (v) VALUES (5)").await.unwrap();
    assert_eq!(o.last_insert_id, 5);
}

#[test]
fn session_rules_come_from_sql_mode_and_time_zone() {
    let m = Env::from_vars(
        false,
        Some(
            "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
        ),
        Some("SYSTEM"),
    );
    assert!(m.strict && !m.no_zero_date && !m.round_fraction);
    assert_eq!(m.tz, Some(0));
    let lax = Env::from_vars(false, Some(""), Some("+05:30"));
    assert!(!lax.strict);
    assert_eq!(lax.tz, Some(19_800));
    let t = Env::from_vars(
        true,
        Some("TRADITIONAL,NO_AUTO_VALUE_ON_ZERO"),
        Some("-08:00"),
    );
    assert!(t.strict && t.no_zero_date && t.no_zero_in_date && t.no_auto_value_on_zero);
    assert_eq!(t.tz, Some(-28_800));
    // A named zone can't be applied yet.
    assert_eq!(Env::from_vars(false, None, Some("Europe/Paris")).tz, None);
    // Nothing tracked: the port's defaults.
    let d = Env::from_vars(true, None, None);
    assert!(d.strict && d.mysql && d.no_zero_date && d.round_fraction);
}

const TD: &str = "CREATE TABLE `td` (\n  `id` int(11) NOT NULL,\n  `a` decimal(5,2) DEFAULT NULL,\n  `b` decimal(10,0) unsigned DEFAULT NULL,\n  `c` decimal(20,10) DEFAULT NULL,\n  `d` decimal(3,3) DEFAULT NULL,\n  `e` decimal(65,30) DEFAULT NULL,\n  `f` decimal(12,4) NOT NULL DEFAULT 1.5000,\n  PRIMARY KEY (`id`),\n  KEY `a` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every DECIMAL rule the servers showed
async fn decimals_round_clip_and_compare_exactly() {
    let (e, _d) = mem();
    let f = names(&[("td", 30, TD)]);
    ok(
        &e,
        &f,
        "INSERT INTO td VALUES (1, 123.45, 1234567890, 1234567890.0123456789, 0.999, 1, 2)",
    )
    .await;
    ok(&e, &f, "INSERT INTO td (id, a, b, c, d, e) VALUES (2, -999.99, 0, -9999999999.9999999999, -0.999, -1.5)").await;
    // Extra decimals round half away from zero, with a note.
    let o = ok(&e, &f, "INSERT INTO td (id, a, b, c, d, e) VALUES (3, 1.005, 1.5, 1.00000000005, 0.0005, 0.0000000000000000000000000000005)").await;
    assert_eq!(
        o.conds
            .iter()
            .map(|c| (c.level, c.code))
            .collect::<Vec<_>>(),
        vec![(N, 1265); 5]
    );
    ok(&e, &f, "INSERT INTO td (id, a, b, c, d, e) VALUES (4, 1.004, 2.5, 1.00000000004, 0.0004, 0.0000000000000000000000000000004)").await;
    let o = ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (5, -1.005, 0.4, -1.00000000005, -0.0005, 5)",
    )
    .await;
    assert_eq!(o.warnings(), 4);
    // Too many digits before the point, also after rounding; a negative
    // value in an unsigned column even when it rounds to zero.
    for (v, col) in [
        ("(6, 1000, 0, 0, 0, 0)", "a"),
        ("(6, 999.995, 0, 0, 0, 0)", "a"),
        ("(6, -1000, 0, 0, 0, 0)", "a"),
        ("(6, 0, -1, 0, 0, 0)", "b"),
        ("(6, 0, -0.4, 0, 0, 0)", "b"),
        ("(6, 0, 10000000000, 0, 0, 0)", "b"),
        ("(6, 0, 0, 0, 1, 0)", "d"),
        ("(6, 0, 0, 0, 0.9995, 0)", "d"),
        ("(6, 1e3, 0, 0, 0, 0)", "a"),
        ("(6, 0, 0, 0, 0, '1e40')", "e"),
    ] {
        let sql = format!("INSERT INTO td (id, a, b, c, d, e) VALUES {v}");
        assert_eq!(
            err(&e, &f, &sql).await,
            (
                1264,
                format!("Out of range value for column '{col}' at row 1")
            ),
            "{v}"
        );
    }
    ok(
        &e,
        &f,
        "INSERT INTO td (id, a, d) VALUES (6, 999.994, 0.9994)",
    )
    .await;
    // Strings: the number at the front; doubles through their shortest
    // digits.
    let o = ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (8, '12.345', '12', '1e3', '.5', '1.5e-3')",
    )
    .await;
    assert_eq!(o.warnings(), 1);
    assert_eq!(
        err(&e, &f, "INSERT INTO td (id, a) VALUES (9, 'abc')").await,
        (
            1366,
            "Incorrect decimal value: 'abc' for column `sbtest`.`td`.`a` at row 1".into()
        )
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO td (id, a) VALUES (9, '')").await.0,
        1366
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO td (id, a) VALUES (9, '12.3abc')")
            .await
            .0,
        1265
    );
    let o = ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c) VALUES (9, ' 12.5 ', ' 7', '8 ')",
    )
    .await;
    assert_eq!(o.warnings(), 2);
    ok(&e, &f, "INSERT INTO td (id, a, b, c, d, e) VALUES (10, 1.5e0, 2.5e0, 0.1e0, 1e-3, 1.23456789012345678e10)").await;
    ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (11, 1e2, 1e9, 1e-11, 0.3e0, 1e30)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (12, 12, 12, 12, 0, 12)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (13, TRUE, 0x10, b'11', 0, NULL)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO td (id, a, b, c, d, e) VALUES (14, '1e1', '1E2', '-1e-2', '5e-4', '1e30')",
    )
    .await;
    let nines = "0".repeat(30);
    assert_eq!(
        flat(&e, &f, "SELECT * FROM td").await,
        format!(
            "1,123.45,1234567890,1234567890.0123456789,0.999,1.{nines},2.0000 | \
             2,-999.99,0,-9999999999.9999999999,-0.999,-1.5{z29},1.5000 | \
             3,1.01,2,1.0000000001,0.001,0.{z29}1,1.5000 | \
             4,1.00,3,1.0000000000,0.000,0.{nines},1.5000 | \
             5,-1.01,0,-1.0000000001,-0.001,5.{nines},1.5000 | \
             6,999.99,NULL,NULL,0.999,NULL,1.5000 | \
             8,12.35,12,1000.0000000000,0.500,0.0015{z26},1.5000 | \
             9,12.50,7,8.0000000000,NULL,NULL,1.5000 | \
             10,1.50,3,0.1000000000,0.001,12345678901.234568{z24},1.5000 | \
             11,100.00,1000000000,0.0000000000,0.300,1{nines}.{nines},1.5000 | \
             12,12.00,12,12.0000000000,0.000,12.{nines},1.5000 | \
             13,1.00,16,3.0000000000,0.000,NULL,1.5000 | \
             14,10.00,100,-0.0100000000,0.001,1{nines}.{nines},1.5000",
            z29 = "0".repeat(29),
            z26 = "0".repeat(26),
            z24 = "0".repeat(24),
        )
    );
    // Comparisons: exact with exact numbers, a double's with doubles and
    // strings; the index is read where one value can be equal.
    for (cond, want) in [
        ("a = 123.45", "1"),
        ("a = 123.450", "1"),
        ("a = '123.45'", "1"),
        ("a = 123.45e0", "1"),
        ("a = 12", "12"),
        ("a = 12.0", "12"),
        ("a = 1.005", ""),
        ("a > 100", "1 | 6"),
        ("a BETWEEN 1 AND 1.01", "3 | 4 | 13"),
        ("a < 0", "2 | 5"),
        ("a IN (1.01, 12, '100')", "3 | 11 | 12"),
        ("b = 2", "3"),
        ("d = 0.001", "3 | 10 | 14"),
        ("d = 1e-3", "3 | 10 | 14"),
    ] {
        let sql = format!("SELECT id FROM td WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    let o = q(&e, &f, "SELECT id FROM td WHERE a = 12.0").await.unwrap();
    assert_eq!((o.examined, o.no_index), (1, false));
    let o = q(&e, &f, "SELECT id FROM td WHERE a = 1.005")
        .await
        .unwrap();
    assert_eq!(o.examined, 0);
    // Arithmetic is exact, then stored by the same rules.
    ok(&e, &f, "UPDATE td SET a = a + 1 WHERE id = 1").await;
    ok(&e, &f, "UPDATE td SET a = a * 2 WHERE id = 1").await;
    let u = ok(&e, &f, "UPDATE td SET a = a * 1.111 WHERE id = 1").await;
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 1  Warnings: 1")
    );
    let u = ok(&e, &f, "UPDATE td SET a = a - 0.005 WHERE id = 1").await;
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 0  Warnings: 1")
    );
    assert_eq!(
        code(q(&e, &f, "UPDATE td SET a = a + 1000 WHERE id = 1").await),
        1264
    );
    ok(&e, &f, "UPDATE td SET c = c * c WHERE id = 3").await;
    assert_eq!(
        code(q(&e, &f, "UPDATE td SET b = b - 100 WHERE id = 12").await),
        1264
    );
    ok(&e, &f, "UPDATE td SET a = -a WHERE id = 12").await;
    ok(&e, &f, "UPDATE td SET a = a + '1.5' WHERE id = 12").await;
    ok(&e, &f, "UPDATE td SET a = a + 1.5e0 WHERE id = 12").await;
    ok(&e, &f, "UPDATE td SET a = 5 - a WHERE id = 12").await;
    assert_eq!(
        flat(&e, &f, "SELECT id, a, b, c FROM td WHERE id IN (1, 3, 12)").await,
        "1,276.53,1234567890,1234567890.0123456789 | 3,1.01,2,1.0000000002 | 12,14.00,12,12.0000000000"
    );
    // Outside strict mode: the largest value, zero for text.
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO td (id, a, b, c, d, e) VALUES (20, 1000, -1, 99999999999.5, 1, 1e40)"
        )
        .await,
        vec![(W, 1264); 5]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO td (id, a, b, c, d, e) VALUES (21, -1000.555, 1e11, '1e30', '-5', 'x')"
        )
        .await,
        vec![(W, 1264), (W, 1264), (W, 1264), (W, 1264), (W, 1366)]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO td (id, a, b, c, d, e) VALUES (22, '12.3abc', '', 'abc', '0.5x', 1)"
        )
        .await,
        vec![(W, 1265), (W, 1366), (W, 1366), (W, 1265)]
    );
    assert_eq!(
        flat(&e, &f, "SELECT a, b, c, d, e FROM td WHERE id >= 20").await,
        format!(
            "999.99,0,9999999999.9999999999,0.999,{n35}.{n30} | \
             -999.99,9999999999,9999999999.9999999999,-0.999,0.{nines} | \
             12.30,0,0.0000000000,0.500,1.{nines}",
            n35 = "9".repeat(35),
            n30 = "9".repeat(30),
        )
    );
    // MySQL's port: a number then text is 1366 in strict mode.
    let my = Env {
        mysql: true,
        ..Env::STRICT
    };
    let r = q_env(&e, &f, "INSERT INTO td (id, a) VALUES (23, '12.3abc')", my).await;
    assert_eq!(code(r), 1366);
}

const TF: &str = "CREATE TABLE `tf` (\n  `id` int(11) NOT NULL,\n  `f` float DEFAULT NULL,\n  `d` double DEFAULT NULL,\n  `fm` float(7,3) DEFAULT NULL,\n  `dm` double(10,4) DEFAULT NULL,\n  `fu` float unsigned DEFAULT NULL,\n  `du` double unsigned DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `f` (`f`),\n  KEY `d` (`d`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every FLOAT and DOUBLE rule
async fn floats_and_doubles_store_round_and_print_as_the_servers_do() {
    let (e, _d) = mem();
    let f = names(&[("tf", 31, TF)]);
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (1, 1.5, 2.5, 1234.5678, 123456.78915, 1.5, 2.5)",
    )
    .await;
    let o = ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (2, '1.5', ' 2.5e3 ', '1.0005', '-0.00005', '3', '4')",
    )
    .await;
    assert_eq!(o.warnings(), 1);
    ok(&e, &f, "INSERT INTO tf VALUES (3, 12, 13, 14, 15, 16, 17)").await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (4, 1.23456789, 1.23456789012345678, 9999.9994, 999999.99994, 0, 0)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (5, 0, 0, 9999.9995, 0, 0, 0)",
    )
    .await;
    for (v, col) in [
        ("(6, 0, 0, 10000, 0, 0, 0)", "fm"),
        ("(6, 0, 0, 0, 1000000, 0, 0)", "dm"),
        ("(6, 0, 0, 0, 0, -1, 0)", "fu"),
        ("(6, 0, 0, 0, 0, 0, -0.5)", "du"),
        ("(6, 0, '1e400', 0, 0, 0, 0)", "d"),
        ("(6, 3.5e38, 0, 0, 0, 0, 0)", "f"),
        ("(6, '1e39', 0, 0, 0, 0, 0)", "f"),
        ("(6, 3.4028235e38, 0, 0, 0, 0, 0)", "f"),
    ] {
        let sql = format!("INSERT INTO tf VALUES {v}");
        assert_eq!(
            err(&e, &f, &sql).await,
            (
                1264,
                format!("Out of range value for column '{col}' at row 1")
            ),
            "{v}"
        );
    }
    assert_eq!(
        err(&e, &f, "INSERT INTO tf (id, f) VALUES (6, 'abc')").await,
        (
            1366,
            "Incorrect double value: 'abc' for column `sbtest`.`tf`.`f` at row 1".into()
        )
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO tf (id, f) VALUES (6, '1.5x')")
            .await
            .0,
        1265
    );
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (6, 0x10, b'11', TRUE, 0.5, 2.5, 3.5)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (7, 1.0005e0, 2.675e0, 2.6755e0, 1.00005e0, 0, 0)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (8, -1.5, -2.5, -1234.5675, -0.00005, 0, 0)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (9, 16777217, 9007199254740993, 0.0005, 0.00005, 0, 0)",
    )
    .await;
    ok(
        &e,
        &f,
        "INSERT INTO tf VALUES (10, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)",
    )
    .await;
    assert_eq!(
        flat(&e, &f, "SELECT * FROM tf").await,
        "1,1.5,2.5,1234.568,123456.7891,1.5,2.5 | 2,1.5,2500,1.000,0.0000,3,4 | \
         3,12,13,14.000,15.0000,16,17 | 4,1.23457,1.2345678901234567,9999.999,999999.9999,0,0 | \
         5,0,0,9999.999,0.0000,0,0 | 6,16,3,1.000,0.5000,2.5,3.5 | \
         7,1.0005,2.675,2.676,1.0001,0,0 | 8,-1.5,-2.5,-1234.568,0.0000,0,0 | \
         9,16777200,9.007199254740992e15,0.000,0.0000,0,0 | 10,0.1,0.1,0.100,0.1000,0.1,0.1"
    );
    // Comparisons are a double's: a FLOAT's 0.1 isn't 0.1, a FLOAT(M,D)'s
    // is.
    for (cond, want) in [
        ("f = 1.5", "1 | 2"),
        ("f = 0.1", ""),
        ("d = 0.1", "10"),
        ("f = '1.5'", "1 | 2"),
        ("d = 13", "3"),
        ("fm = 1234.568", "1"),
        ("fm = 0.1", "10"),
        ("dm = 0.1", "10"),
        ("d > 2 AND d <= 13", "1 | 3 | 6 | 7"),
        ("f > 1.4 AND f < 1.6", "1 | 2"),
        ("f = 16777216", "9"),
        ("f = 16777217", ""),
    ] {
        let sql = format!("SELECT id FROM tf WHERE {cond}");
        let mut got: Vec<String> = flat(&e, &f, &sql)
            .await
            .split(" | ")
            .map(str::to_owned)
            .collect();
        got.sort_by_key(|x| x.parse::<i64>().unwrap_or(0));
        assert_eq!(got.join(" | "), want, "{cond}");
    }
    let o = q(&e, &f, "SELECT id FROM tf WHERE d = 13").await.unwrap();
    assert_eq!((o.examined, o.no_index), (1, false));
    let o = q(&e, &f, "SELECT id FROM tf WHERE f = 0.1").await.unwrap();
    assert_eq!(o.examined, 0);
    ok(&e, &f, "UPDATE tf SET f = f + 1, d = d * 2 WHERE id = 1").await;
    assert_eq!(
        code(q(&e, &f, "UPDATE tf SET f = f * 1e38 WHERE id = 3").await),
        1264
    );
    assert_eq!(
        code(q(&e, &f, "UPDATE tf SET d = d * 1e308 WHERE id = 3").await),
        1690
    );
    let u = ok(&e, &f, "UPDATE tf SET fm = fm + 0.0004 WHERE id = 3").await;
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 0  Warnings: 0")
    );
    assert_eq!(
        flat(&e, &f, "SELECT id, f, d, fm FROM tf WHERE id IN (1, 3)").await,
        "1,2.5,5,1234.568 | 3,12,13,14.000"
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO tf VALUES (20, 3.5e38, '1e400', 10000, 1000000, -1, -0.5)"
        )
        .await,
        vec![(W, 1264); 6]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO tf VALUES (21, 'abc', '', '1.5x', -1e9, 0, 0)"
        )
        .await,
        vec![(W, 1366), (W, 1366), (W, 1265), (W, 1264)]
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM tf WHERE id >= 20").await,
        "20,3.40282e38,1.7976931348623157e308,9999.999,999999.9999,0,0 | 21,0,0,1.500,-999999.9999,0,0"
    );
}

#[tokio::test]
async fn decimal_and_double_keys_order_and_find_their_rows() {
    let (e, _d) = mem();
    let tdk = "CREATE TABLE `tdk` (\n  `k` decimal(10,3) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`k`)\n) ENGINE=InnoDB";
    let tk = "CREATE TABLE `tk` (\n  `f` double NOT NULL,\n  `g` float DEFAULT NULL,\n  PRIMARY KEY (`f`),\n  KEY `g` (`g`)\n) ENGINE=InnoDB";
    let f = names(&[("tdk", 32, tdk), ("tk", 33, tk)]);
    ok(&e, &f, "INSERT INTO tdk VALUES (1.5, 1), (-2.25, 2), (0, 3), (1000000.001, 4), (-0.001, 5), (10, 6)").await;
    assert_eq!(
        err(&e, &f, "INSERT INTO tdk VALUES (1.500, 9)").await,
        (1062, "Duplicate entry '1.500' for key 'PRIMARY'".into())
    );
    assert_eq!(
        flat(&e, &f, "SELECT k, v FROM tdk").await,
        "-2.250,2 | -0.001,5 | 0.000,3 | 1.500,1 | 10.000,6 | 1000000.001,4"
    );
    for (cond, want) in [
        ("k = 1.5", "1"),
        ("k = '1.5'", "1"),
        ("k = 1.5e0", "1"),
        ("k = 1.5001", ""),
        ("k > -1 AND k <= 10", "5 | 3 | 1 | 6"),
        ("k BETWEEN -2.25 AND 0", "2 | 5 | 3"),
    ] {
        let sql = format!("SELECT v FROM tdk WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    let o = q(&e, &f, "SELECT v FROM tdk WHERE k = 1.5e0")
        .await
        .unwrap();
    assert_eq!((o.examined, o.no_index), (1, false));
    ok(&e, &f, "UPDATE tdk SET k = k + 1 WHERE k = 10").await;
    ok(&e, &f, "DELETE FROM tdk WHERE k < 0").await;
    assert_eq!(
        flat(&e, &f, "SELECT k FROM tdk ORDER BY k DESC").await,
        "1000000.001 | 11.000 | 1.500 | 0.000"
    );
    ok(&e, &f, "INSERT INTO tk VALUES (1.5, 1.5), (-2.25, 0.1), (0, -0e0), (1e100, 1e30), (-1e-100, 16777216)").await;
    // -0 and 0 are one key.
    assert_eq!(
        code(q(&e, &f, "INSERT INTO tk VALUES (-0e0, 1)").await),
        1062
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM tk").await,
        "-2.25,0.1 | -1e-100,16777200 | 0,0 | 1.5,1.5 | 1e100,1e30"
    );
    for (cond, want) in [
        ("f = 1.5", "1.5"),
        ("f > -3 AND f < 2", "-2.25 | -1e-100 | 0 | 1.5"),
        ("g = 0.1", ""),
        ("g = 1.5", "1.5"),
        ("g = 16777217", ""),
    ] {
        let sql = format!("SELECT f FROM tk WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    assert_eq!(
        flat(&e, &f, "SELECT f FROM tk ORDER BY f DESC").await,
        "1e100 | 1.5 | 0 | -1e-100 | -2.25"
    );
}

const TT: &str = "CREATE TABLE `tt` (\n  `id` int(11) NOT NULL,\n  `d` date DEFAULT NULL,\n  `t` time DEFAULT NULL,\n  `t3` time(3) DEFAULT NULL,\n  `dt` datetime DEFAULT NULL,\n  `dt6` datetime(6) DEFAULT NULL,\n  `ts` timestamp NULL DEFAULT NULL,\n  `ts3` timestamp(3) NULL DEFAULT NULL,\n  `y` year(4) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `d` (`d`),\n  KEY `dt` (`dt`),\n  KEY `ts` (`ts`),\n  KEY `y` (`y`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every temporal rule the servers showed
async fn dates_and_times_are_read_checked_and_printed_as_the_servers_do() {
    let (e, _d) = mem();
    let f = names(&[("tt", 40, TT)]);
    ok(&e, &f, "INSERT INTO tt VALUES (1, '2024-02-29', '12:34:56', '12:34:56.789', '2024-02-29 13:14:15', '2024-02-29 13:14:15.123456', '2024-02-29 13:14:15', '2024-02-29 13:14:15.123', 2024)").await;
    let o = ok(&e, &f, "INSERT INTO tt VALUES (2, '2024-2-9', '1:2:3', '-838:59:59.999', '2024-2-9 1:2:3', '2024-02-29T13:14:15.1234567', '1970-01-01 00:00:01', '2038-01-19 03:14:07.999', '24')").await;
    assert_eq!(o.warnings(), 1);
    ok(&e, &f, "INSERT INTO tt VALUES (3, 20240229, 123456, 123456.789, 20240229131415, 20240229131415.123456, 20240229131415, 20240229131415.123, 70)").await;
    ok(&e, &f, "INSERT INTO tt VALUES (4, '20240229', '123456', '1234', '20240229131415', '240229131415', '20240229', '2024-02-29', '1999')").await;
    // A time of day cut from a DATE, a date cut from a TIME, more than
    // six fraction digits: notes. Fewer digits than given are cut silently.
    let o = ok(&e, &f, "INSERT INTO tt VALUES (5, '2024-02-29 13:14:15', '2024-02-29 13:14:15', '13:14:15.9999', '2024-02-29', '2024-02-29 13:14:15.9999999', '2024-02-29 13:14:15.9', '2024-02-29 13:14:15.9999', 0)").await;
    assert_eq!(o.warnings(), 3);
    ok(&e, &f, "INSERT INTO tt VALUES (6, '0000-00-00', '00:00:00', '0', '0000-00-00 00:00:00', 0, '0000-00-00 00:00:00', 0, '0')").await;
    ok(&e, &f, "INSERT INTO tt VALUES (7, '2024-00-10', '838:59:59', '838:59:59', '2024-05-00 00:00:00', '0000-01-01', NULL, NULL, '0000')").await;
    ok(&e, &f, "INSERT INTO tt VALUES (11, '2024.02.29', '12.5', '12:34', '2024/02/29 13.14.15', '2024-02-29 13:14', NULL, NULL, 1901)").await;
    for (col, v, kind) in [
        ("d", "'2023-02-29'", "date"),
        ("d", "'2024-13-01'", "date"),
        ("d", "'abc'", "date"),
        ("d", "''", "date"),
        ("d", "'2024-02-29abc'", "date"),
        ("d", "'1900-02-29'", "date"),
        ("d", "1.5e0", "date"),
        ("t", "'abc'", "time"),
        ("t", "'12:60:00'", "time"),
        ("t", "'12:34:56abc'", "time"),
        ("t", "'839:00:00'", "time"),
        ("t", "1260", "time"),
        ("t", "'1 2'", "time"),
        ("t", "'2024-02-29'", "time"),
        ("dt", "'2024-02-30 00:00:00'", "datetime"),
        ("dt", "'2024-02-29 24:00:00'", "datetime"),
        ("dt", "100", "datetime"),
        ("dt", "1000101", "datetime"),
        ("dt", "'2024-02'", "datetime"),
        ("dt", "'13:14:15'", "datetime"),
        ("dt", "'2024-02-29 13:14:15Z'", "datetime"),
        ("ts", "'1969-12-31 23:59:59'", "datetime"),
        ("ts", "'2106-02-07 06:28:16'", "datetime"),
    ] {
        let sql = format!("INSERT INTO tt (id, {col}) VALUES (99, {v})");
        let text = v.trim_matches('\'').replace("e0", "");
        assert_eq!(
            err(&e, &f, &sql).await,
            (
                1292,
                format!(
                    "Incorrect {kind} value: '{text}' for column `sbtest`.`tt`.`{col}` at row 1"
                )
            ),
            "{sql}"
        );
    }
    for (v, code) in [
        ("1900", 1264),
        ("2156", 1264),
        ("-1", 1264),
        ("100", 1264),
        ("'abc'", 1366),
        ("'2024abc'", 1265),
    ] {
        let sql = format!("INSERT INTO tt (id, y) VALUES (99, {v})");
        assert_eq!(err(&e, &f, &sql).await.0, code, "{v}");
    }
    ok(&e, &f, "INSERT INTO tt (id, y) VALUES (12, 2000.6), (13, '2000.6'), (14, 1.5e0), (15, '1'), (16, '00'), (17, '99'), (18, 69)").await;
    assert_eq!(
        text(&e, &f, "SELECT * FROM tt WHERE id <= 11").await,
        vec![
            vec![
                "1",
                "2024-02-29",
                "12:34:56",
                "12:34:56.789",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.123456",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.123",
                "2024"
            ],
            vec![
                "2",
                "2024-02-09",
                "01:02:03",
                "-838:59:59.999",
                "2024-02-09 01:02:03",
                "2024-02-29 13:14:15.123456",
                "1970-01-01 00:00:01",
                "2038-01-19 03:14:07.999",
                "2024"
            ],
            vec![
                "3",
                "2024-02-29",
                "12:34:56",
                "12:34:56.789",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.123456",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.123",
                "1970"
            ],
            vec![
                "4",
                "2024-02-29",
                "12:34:56",
                "00:12:34.000",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.000000",
                "2024-02-29 00:00:00",
                "2024-02-29 00:00:00.000",
                "1999"
            ],
            vec![
                "5",
                "2024-02-29",
                "13:14:15",
                "13:14:15.999",
                "2024-02-29 00:00:00",
                "2024-02-29 13:14:15.999999",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.999",
                "0000"
            ],
            vec![
                "6",
                "0000-00-00",
                "00:00:00",
                "00:00:00.000",
                "0000-00-00 00:00:00",
                "0000-00-00 00:00:00.000000",
                "0000-00-00 00:00:00",
                "0000-00-00 00:00:00.000",
                "2000"
            ],
            vec![
                "7",
                "2024-00-10",
                "838:59:59",
                "838:59:59.000",
                "2024-05-00 00:00:00",
                "0000-01-01 00:00:00.000000",
                "NULL",
                "NULL",
                "0000"
            ],
            vec![
                "11",
                "2024-02-29",
                "00:00:12",
                "12:34:00.000",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:00.000000",
                "NULL",
                "NULL",
                "1901"
            ],
        ]
    );
    assert_eq!(
        flat(&e, &f, "SELECT y FROM tt WHERE id >= 12").await,
        "2001 | 2001 | 2001 | 2001 | 2000 | 1999 | 2069"
    );
    // Comparisons read the other side as the column's kind; the keys are
    // read where one value can be equal.
    for (cond, want) in [
        ("d = '2024-02-29'", "1 | 3 | 4 | 5 | 11"),
        ("d = '2024-02-29 00:00:00'", "1 | 3 | 4 | 5 | 11"),
        ("d = '2024-02-29 13:14:15'", ""),
        ("d = 20240229", "1 | 3 | 4 | 5 | 11"),
        (
            "d > '2024-02-28' AND d < '2024-03-01'",
            "1 | 3 | 4 | 5 | 11",
        ),
        ("d = 'garbage'", "6"),
        ("dt = '2024-02-29 13:14:15'", "1 | 3 | 4 | 11"),
        ("dt = '2024-02-29'", "5"),
        ("dt = 20240229131415", "1 | 3 | 4 | 11"),
        ("dt6 = '2024-02-29 13:14:15.123456'", "1 | 2 | 3"),
        ("dt6 > '2024-02-29 13:14:15'", "1 | 2 | 3 | 5"),
        ("t = '12:34:56'", "1 | 3 | 4"),
        ("t = 123456", "1 | 3 | 4"),
        ("t > '01:00:00'", "1 | 2 | 3 | 4 | 5 | 7"),
        ("t = '00:12:60'", ""),
        ("ts = '2024-02-29 13:14:15'", "1 | 3 | 5"),
        ("ts3 > '2024-02-29'", "1 | 2 | 3 | 5"),
        ("y = 2024", "1 | 2"),
        ("y = '24'", "1 | 2"),
        ("y = 24", "1 | 2"),
        ("y > 2000 AND id < 12", "1 | 2"),
        ("y = '0'", "6 | 16"),
    ] {
        let sql = format!("SELECT id FROM tt WHERE {cond}");
        let mut got: Vec<String> = flat(&e, &f, &sql)
            .await
            .split(" | ")
            .map(str::to_owned)
            .collect();
        got.sort_by_key(|x| x.parse::<i64>().unwrap_or(0));
        assert_eq!(got.join(" | "), want, "{cond}");
    }
    let o = q(&e, &f, "SELECT id FROM tt WHERE dt = '2024-02-29'")
        .await
        .unwrap();
    assert_eq!((o.examined, o.no_index), (1, false));
    let o = q(&e, &f, "SELECT id FROM tt WHERE d = '2024-02-29 13:14:15'")
        .await
        .unwrap();
    assert_eq!(o.examined, 0);
    // MySQL's port refuses text that is no date in a comparison.
    let my = Env {
        mysql: true,
        ..Env::STRICT
    };
    let r = q_env(&e, &f, "SELECT id FROM tt WHERE d = 'garbage'", my)
        .await
        .unwrap_err();
    assert_eq!(
        (r.protocol().mysql_errno, r.message.as_str()),
        (1525, "Incorrect DATE value: 'garbage'")
    );
    // A TIMESTAMP is an instant: written and read in the session's zone.
    let ist = Env {
        tz: Some(19_800),
        ..Env::STRICT
    };
    let p = bind(&f, "SELECT ts, ts3, dt FROM tt WHERE id = 1")
        .unwrap()
        .unwrap();
    let o = e
        .run_env(TENANT.0, &p, &[], false, TxnOptions::default(), ist)
        .await
        .unwrap();
    let plan::Op::Select { out } = &p.op else {
        panic!()
    };
    let shown: Vec<String> = o.rows[0]
        .iter()
        .zip(out)
        .map(|(d, oc)| {
            String::from_utf8_lossy(
                &meta::text_in(&p.table.columns[oc.col], d, &ist, UTF8).unwrap(),
            )
            .into_owned()
        })
        .collect();
    assert_eq!(
        shown,
        vec![
            "2024-02-29 18:44:15",
            "2024-02-29 18:44:15.123",
            "2024-02-29 13:14:15"
        ]
    );
    q_env(
        &e,
        &f,
        "INSERT INTO tt (id, ts) VALUES (20, '2024-02-29 13:14:15')",
        ist,
    )
    .await
    .unwrap();
    assert_eq!(
        flat(&e, &f, "SELECT ts FROM tt WHERE id = 20").await,
        "2024-02-29 07:44:15"
    );
    let o = q_env(
        &e,
        &f,
        "SELECT id FROM tt WHERE ts = '2024-02-29 13:14:15'",
        ist,
    )
    .await
    .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(20)]]);
    // A named zone can't be applied yet.
    let named = Env {
        tz: None,
        ..Env::STRICT
    };
    let r = q_env(
        &e,
        &f,
        "INSERT INTO tt (id, ts) VALUES (21, '2024-02-29 13:14:15')",
        named,
    )
    .await;
    assert_eq!(code(r), NOT_ON_ENGINE);
    // Outside strict mode: the zero value or the bound, with a warning.
    assert_eq!(
        conds(&e, &f, "INSERT INTO tt VALUES (30, '2023-02-29', '839:00:00', '-839:00:00', '2024-02-30 00:00:00', 'abc', '1969-12-31 23:59:59', '2024-02-29 13:14:15.9996', 1900)").await,
        vec![(W, 1265), (W, 1264), (W, 1264), (W, 1265), (W, 1265), (W, 1264), (W, 1264)]
    );
    assert_eq!(
        conds(&e, &f, "INSERT INTO tt VALUES (32, '2024-02-29abc', '12:34:56abc', '1:2:3.4567', '2024-02-29 13:14:15.999999', '2024-02-29 13:14:15.9999999', 20240229, '2024-02-29 25:00:00', 2156)").await,
        vec![(W, 1265), (W, 1265), (N, 1265), (W, 1265), (W, 1264)]
    );
    assert_eq!(
        text(&e, &f, "SELECT * FROM tt WHERE id >= 30").await,
        vec![
            vec![
                "30",
                "0000-00-00",
                "838:59:59",
                "-838:59:59.999",
                "0000-00-00 00:00:00",
                "0000-00-00 00:00:00.000000",
                "0000-00-00 00:00:00",
                "2024-02-29 13:14:15.999",
                "0000"
            ],
            vec![
                "32",
                "2024-02-29",
                "12:34:56",
                "01:02:03.456",
                "2024-02-29 13:14:15",
                "2024-02-29 13:14:15.999999",
                "2024-02-29 00:00:00",
                "0000-00-00 00:00:00.000",
                "0000"
            ],
        ]
    );
    // MySQL's rules: fractions round, a TIME's bound has no fraction, the
    // zero date is refused in strict mode, a TIMESTAMP ends in 2038.
    let my = Env {
        mysql: true,
        round_fraction: true,
        no_zero_date: true,
        no_zero_in_date: true,
        ..Env::STRICT
    };
    q_env(&e, &f, "INSERT INTO tt VALUES (40, '2024-02-29', '12.5', '13:14:15.9999', '2024-02-29 13:14:15.5', '2024-12-31 23:59:59.9999999', '2024-02-29 13:14:15.9', '2024-02-29 13:14:15.9999', 0)", my).await.unwrap();
    assert_eq!(
        flat(
            &e,
            &f,
            "SELECT t, t3, dt, dt6, ts, ts3 FROM tt WHERE id = 40"
        )
        .await,
        "00:00:13,13:14:16.000,2024-02-29 13:14:16,2025-01-01 00:00:00.000000,2024-02-29 13:14:16,2024-02-29 13:14:16.000"
    );
    for v in [
        "(41, '0000-00-00', NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "(41, '2024-00-10', NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "(41, NULL, NULL, '-838:59:59.999', NULL, NULL, NULL, NULL, NULL)",
        "(41, NULL, NULL, NULL, NULL, NULL, '2038-01-19 03:14:08', NULL, NULL)",
    ] {
        let r = q_env(&e, &f, &format!("INSERT INTO tt VALUES {v}"), my).await;
        assert_eq!(code(r), 1292, "{v}");
    }
}

#[tokio::test]
async fn temporal_keys_and_current_timestamp_columns() {
    let (e, _d) = mem();
    let tk = "CREATE TABLE `tk` (\n  `d` date NOT NULL,\n  `t` time(2) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`d`,`t`)\n) ENGINE=InnoDB";
    let au = "CREATE TABLE `au` (\n  `id` int(11) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n  `created` datetime(3) NOT NULL DEFAULT current_timestamp(3),\n  `updated` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB";
    let f = names(&[("tk", 41, tk), ("au", 42, au)]);
    ok(&e, &f, "INSERT INTO tk VALUES ('2024-01-01', '10:00:00', 1), ('2024-01-01', '09:00:00.25', 2), ('2023-12-31', '23:59:59.99', 3), ('2024-01-02', '-01:00:00', 4)").await;
    assert_eq!(
        err(
            &e,
            &f,
            "INSERT INTO tk VALUES ('2024-01-01', '10:00:00.00', 9)"
        )
        .await,
        (
            1062,
            "Duplicate entry '2024-01-01-10:00:00.00' for key 'PRIMARY'".into()
        )
    );
    assert_eq!(
        flat(&e, &f, "SELECT * FROM tk").await,
        "2023-12-31,23:59:59.99,3 | 2024-01-01,09:00:00.25,2 | 2024-01-01,10:00:00.00,1 | 2024-01-02,-01:00:00.00,4"
    );
    let o = q(
        &e,
        &f,
        "SELECT v FROM tk WHERE d = '2024-01-01' AND t = '09:00:00.25'",
    )
    .await
    .unwrap();
    assert_eq!((o.rows.clone(), o.examined), (vec![vec![Datum::Int(2)]], 1));
    ok(&e, &f, "DELETE FROM tk WHERE d = 20240101 AND t = 100000").await;
    assert_eq!(flat(&e, &f, "SELECT v FROM tk").await, "3 | 2 | 4");
    // DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP take the
    // statement's time, the same for every row of it.
    let o = ok(&e, &f, "INSERT INTO au (id, v) VALUES (1, 1), (2, 2)").await;
    assert_eq!(o.affected, 2);
    let rows = text(&e, &f, "SELECT created, updated FROM au").await;
    assert_eq!(rows[0], rows[1]);
    assert_eq!(rows[0][0].len(), 23);
    assert_eq!(rows[0][1].len(), 19);
    assert!(rows[0][0].starts_with("20"), "{rows:?}");
    // An UPDATE that changes nothing leaves `updated`; one that does sets
    // it, unless the statement sets it itself.
    ok(
        &e,
        &f,
        "UPDATE au SET updated = '2001-01-01 00:00:00' WHERE id = 1",
    )
    .await;
    let u = ok(&e, &f, "UPDATE au SET v = 1 WHERE id = 1").await;
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 0  Warnings: 0")
    );
    assert_eq!(
        flat(&e, &f, "SELECT updated FROM au WHERE id = 1").await,
        "2001-01-01 00:00:00"
    );
    ok(&e, &f, "UPDATE au SET v = 5 WHERE id = 1").await;
    assert_ne!(
        flat(&e, &f, "SELECT updated FROM au WHERE id = 1").await,
        "2001-01-01 00:00:00"
    );
}

const TB: &str = "CREATE TABLE `tb` (\n  `id` int(11) NOT NULL,\n  `b` binary(4) DEFAULT NULL,\n  `v` varbinary(4) DEFAULT NULL,\n  `t` tinyblob DEFAULT NULL,\n  `e` enum('red','green','Blue') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `s` set('a','b','c') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `b1` bit(1) DEFAULT NULL,\n  `b9` bit(9) DEFAULT b'101',\n  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`)),\n  `en` enum('x','y') NOT NULL DEFAULT 'y',\n  PRIMARY KEY (`id`),\n  KEY `v` (`v`),\n  KEY `e` (`e`),\n  KEY `b9` (`b9`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

/// The values of one column, as hex for byte strings.
async fn hexes(e: &Engine, f: &Fake, sql: &str) -> String {
    let p = bind(f, sql).unwrap().expect("an engine statement");
    let o = exec(e, &p, &[]).await.unwrap();
    o.rows
        .iter()
        .map(|r| match &r[0] {
            Datum::Bytes(b) => b.iter().fold(String::new(), |mut s, x| {
                let _ = write!(s, "{x:02X}");
                s
            }),
            Datum::Null => "NULL".into(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, the rules of five type families
async fn byte_strings_members_bits_and_json() {
    let (e, _d) = mem();
    let f = names(&[("tb", 50, TB)]);
    // BINARY pads with zero bytes; every byte of a byte string counts.
    ok(&e, &f, "INSERT INTO tb (id, b, v) VALUES (1, 'ab', 'ab  '), (2, 'abcd', x'00FF00'), (3, 'ab  ', 1.5), (4, 0x0102, ''), (5, 12, NULL), (6, 'é', NULL), (7, '', NULL)").await;
    assert_eq!(
        hexes(&e, &f, "SELECT b FROM tb").await,
        "61620000 | 61626364 | 61622020 | 01020000 | 31320000 | C3A90000 | 00000000"
    );
    assert_eq!(
        hexes(&e, &f, "SELECT v FROM tb WHERE id < 5").await,
        "61622020 | 00FF00 | 312E35 | "
    );
    for (col, v) in [("b", "'abcde'"), ("v", "'abcde'"), ("v", "'abcd '")] {
        let sql = format!("INSERT INTO tb (id, {col}) VALUES (99, {v})");
        assert_eq!(err(&e, &f, &sql).await.0, 1406, "{sql}");
    }
    let long = format!("INSERT INTO tb (id, t) VALUES (99, '{}')", "x".repeat(256));
    assert_eq!(err(&e, &f, &long).await.0, 1406);
    for (cond, want) in [
        ("b = 'ab'", ""),
        ("b = 0x61620000", "1"),
        ("v = 'ab'", ""),
        ("v = 'ab  '", "1"),
        ("v = 'AB  '", ""),
        ("v > 'ab'", "1"),
    ] {
        let sql = format!("SELECT id FROM tb WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    // ENUM: a member by name under the collation, a number as an index.
    ok(&e, &f, "INSERT INTO tb (id, e) VALUES (11, 'red'), (12, 'GREEN'), (13, 'blue '), (14, 2), (15, '2'), (16, 1.6)").await;
    for v in ["0", "4", "''", "'pink'", "'1.6'", "-1"] {
        let sql = format!("INSERT INTO tb (id, e) VALUES (99, {v})");
        assert_eq!(
            err(&e, &f, &sql).await,
            (1265, "Data truncated for column 'e' at row 1".into()),
            "{v}"
        );
    }
    assert_eq!(
        flat(&e, &f, "SELECT id, e FROM tb WHERE id BETWEEN 11 AND 16").await,
        "11,red | 12,green | 13,Blue | 14,green | 15,green | 16,red"
    );
    for (cond, want) in [
        ("e = 'RED'", "11 | 16"),
        ("e = 2", "12 | 14 | 15"),
        ("e = '2'", ""),
        ("e = ''", ""),
        ("e > 'green'", "11 | 16"),
        ("e > 1", "12 | 13 | 14 | 15"),
        ("en = 'y' AND id = 1", "1"),
    ] {
        let sql = format!("SELECT id FROM tb WHERE {cond}");
        let mut got: Vec<String> = flat(&e, &f, &sql)
            .await
            .split(" | ")
            .map(str::to_owned)
            .collect();
        got.sort_by_key(|x| x.parse::<i64>().unwrap_or(0));
        assert_eq!(got.join(" | "), want, "{cond}");
    }
    let o = q(&e, &f, "SELECT id FROM tb WHERE e = 'RED'")
        .await
        .unwrap();
    assert_eq!((o.examined, o.no_index), (2, false));
    // SET: members in any order and case, a number as the mask.
    ok(&e, &f, "INSERT INTO tb (id, s) VALUES (21, 'a'), (22, 'c,a'), (23, 'A,B,a'), (24, ''), (25, 5), (26, '5'), (27, 0)").await;
    for v in ["'a,d'", "'a, b'", "8", "'a,,b'", "',a'"] {
        let sql = format!("INSERT INTO tb (id, s) VALUES (99, {v})");
        assert_eq!(err(&e, &f, &sql).await.0, 1265, "{v}");
    }
    assert_eq!(
        flat(&e, &f, "SELECT id, s FROM tb WHERE id BETWEEN 21 AND 27").await,
        "21,a | 22,a,c | 23,a,b | 24, | 25,a,c | 26,a,c | 27,"
    );
    for (cond, want) in [
        ("s = 'a,c'", "22 | 25 | 26"),
        ("s = 'c,a'", ""),
        ("s = 5", "22 | 25 | 26"),
        ("s = ''", "24 | 27"),
    ] {
        let sql = format!("SELECT id FROM tb WHERE {cond}");
        assert_eq!(flat(&e, &f, &sql).await, want, "{cond}");
    }
    // BIT: a number, or a string's bytes; too many bits is 1406.
    ok(&e, &f, "INSERT INTO tb (id, b1, b9) VALUES (31, 1, 511), (32, b'1', b'101010101'), (33, 0, 0), (34, NULL, '1'), (35, NULL, 1.5), (36, NULL, 0x01FF), (37, NULL, TRUE)").await;
    for (col, v) in [
        ("b1", "2"),
        ("b9", "512"),
        ("b9", "-1"),
        ("b9", "'ab'"),
        ("b1", "'1'"),
    ] {
        let sql = format!("INSERT INTO tb (id, {col}) VALUES (99, {v})");
        assert_eq!(
            err(&e, &f, &sql).await,
            (1406, format!("Data too long for column '{col}' at row 1")),
            "{sql}"
        );
    }
    let p = bind(&f, "SELECT b9 FROM tb WHERE id BETWEEN 31 AND 37")
        .unwrap()
        .unwrap();
    let o = exec(&e, &p, &[]).await.unwrap();
    let bits: Vec<Datum> = o.rows.into_iter().map(|mut r| r.remove(0)).collect();
    assert_eq!(
        bits,
        [511u64, 341, 0, 49, 2, 511, 1].map(Datum::Bit).to_vec()
    );
    // The text protocol sends a BIT's bytes.
    assert_eq!(
        meta::text(&p.table.columns[7], &Datum::Bit(341)).unwrap(),
        Bytes::from_static(&[1, 0x55])
    );
    for (cond, want) in [
        ("b9 = 511", "31 | 36"),
        ("b9 = b'101010101'", "32"),
        ("b9 = '1'", "37"),
        ("b1 = 1", "31 | 32"),
        ("b1 = TRUE", "31 | 32"),
    ] {
        let sql = format!("SELECT id FROM tb WHERE {cond}");
        let mut got: Vec<String> = flat(&e, &f, &sql)
            .await
            .split(" | ")
            .map(str::to_owned)
            .collect();
        got.sort_by_key(|x| x.parse::<i64>().unwrap_or(0));
        assert_eq!(got.join(" | "), want, "{cond}");
    }
    // The column's own default, a bit literal.
    assert_eq!(p.table.columns[7].default_value(), Some(&Datum::Bit(5)));
    // JSON: the text as written, when it is a document.
    ok(&e, &f, r#"INSERT INTO tb (id, j) VALUES (41, '{"a": 1, "b": [1, 2, {"c": null}]}'), (42, '  [1,2 , 3]  '), (43, '"str"'), (44, 12), (45, 'null')"#).await;
    assert_eq!(
        err(&e, &f, "INSERT INTO tb (id, j) VALUES (99, '{bad')").await,
        (4025, "CONSTRAINT `tb.j` failed for `sbtest`.`tb`".into())
    );
    assert_eq!(
        err(&e, &f, "INSERT INTO tb (id, j) VALUES (99, '')")
            .await
            .0,
        4025
    );
    assert_eq!(
        flat(&e, &f, "SELECT j FROM tb WHERE id BETWEEN 42 AND 45").await,
        "  [1,2 , 3]   | \"str\" | 12 | null"
    );
    // Outside strict mode.
    assert_eq!(
        conds(&e, &f, "INSERT INTO tb (id, b, v, e, s, b1, b9) VALUES (50, 'abcde', 'abcde', 'pink', 'a,d', 2, 512)").await,
        vec![(W, 1265), (W, 1265), (W, 1265), (W, 1265), (W, 1264), (W, 1264)]
    );
    assert_eq!(
        conds(
            &e,
            &f,
            "INSERT INTO tb (id, e, s, b9) VALUES (51, 4, 8, 'abc')"
        )
        .await,
        vec![(W, 1265), (W, 1265), (W, 1264)]
    );
    assert_eq!(
        flat(&e, &f, "SELECT e, s FROM tb WHERE id >= 50").await,
        ",a | ,"
    );
    assert_eq!(
        hexes(&e, &f, "SELECT v FROM tb WHERE id = 50").await,
        "61626364"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one statement after another, as a session would send them
async fn the_current_date_and_time_are_values() {
    let (e, _d) = mem();
    let nw = "CREATE TABLE `nw` (\n  `id` int(11) NOT NULL,\n  `d` date DEFAULT NULL,\n  `t` time DEFAULT NULL,\n  `t3` time(3) DEFAULT NULL,\n  `dt` datetime DEFAULT NULL,\n  `dt3` datetime(3) DEFAULT NULL,\n  `ts` timestamp NULL DEFAULT NULL,\n  `v` varchar(30) DEFAULT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `y` year(4) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `dt` (`dt`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
    let f = names(&[("nw", 43, nw)]);
    // One reading per statement: every row and column of it gets the
    // same time, whichever name asks for it.
    ok(&e, &f, "INSERT INTO nw (id, d, dt, dt3, ts, v, b) VALUES (1, CURDATE(), NOW(), NOW(3), CURRENT_TIMESTAMP, NOW(), UNIX_TIMESTAMP()), (2, CURRENT_DATE, LOCALTIME, LOCALTIMESTAMP(3), CURRENT_TIMESTAMP(), CURDATE(), UNIX_TIMESTAMP())").await;
    let rows = text(&e, &f, "SELECT d, dt, dt3, ts, v, b FROM nw").await;
    assert_eq!(rows[0][..4], rows[1][..4], "{rows:?}");
    assert_eq!(rows[0][5], rows[1][5]);
    let (d, dt) = (rows[0][0].clone(), rows[0][1].clone());
    assert_eq!((d.len(), dt.len(), rows[0][2].len()), (10, 19, 23));
    assert!(
        dt.starts_with(&d) && rows[0][2].starts_with(&dt),
        "{rows:?}"
    );
    assert_eq!(rows[0][3], dt);
    assert_eq!((&rows[0][4], &rows[1][4]), (&dt, &d));
    assert!(rows[0][5].parse::<i64>().unwrap() > 1_700_000_000);
    // The time of day, and the UTC forms (the session is in UTC here).
    ok(&e, &f, "INSERT INTO nw (id, d, t, t3, dt, v) VALUES (3, UTC_DATE(), CURTIME(), CURTIME(3), UTC_TIMESTAMP(), CURRENT_TIME), (4, NULL, UTC_TIME(), NULL, NULL, NULL)").await;
    let rows = text(&e, &f, "SELECT t, t3, v, d FROM nw WHERE id = 3").await;
    assert_eq!((rows[0][0].len(), rows[0][1].len()), (8, 12));
    assert!(rows[0][1].starts_with(&rows[0][0]), "{rows:?}");
    assert_eq!((&rows[0][2], &rows[0][3]), (&rows[0][0], &d));
    // In a session east of UTC the local forms move, the UTC forms don't.
    let east = Env {
        tz: Some(19_800),
        ..Env::STRICT
    };
    q_env(
        &e,
        &f,
        "INSERT INTO nw (id, dt, dt3) VALUES (5, NOW(), UTC_TIMESTAMP(3))",
        east,
    )
    .await
    .unwrap();
    let rows = text(&e, &f, "SELECT dt, dt3 FROM nw WHERE id = 5").await;
    assert_ne!(rows[0][0][..16], rows[0][1][..16], "{rows:?}");
    // A date and time into a DATE column loses its time, with a note.
    assert_eq!(
        conds(&e, &f, "INSERT INTO nw (id, d) VALUES (6, NOW())").await,
        [(Level::Note, 1265)]
    );
    // Compared with date and time columns.
    ok(&e, &f, "INSERT INTO nw (id, d, t, dt) VALUES (10, '2020-02-02', '00:00:00', '2020-02-02 02:02:02'), (11, '2099-01-01', NULL, '2099-01-01 00:00:00')").await;
    for (sql, want) in [
        ("SELECT id FROM nw WHERE dt <= NOW()", "1 | 2 | 3 | 10"),
        ("SELECT id FROM nw WHERE dt > CURRENT_TIMESTAMP", "5 | 11"),
        ("SELECT id FROM nw WHERE d = CURDATE()", "1 | 2 | 3 | 6"),
        ("SELECT id FROM nw WHERE d <> CURRENT_DATE", "10 | 11"),
        ("SELECT id FROM nw WHERE d <= NOW()", "1 | 2 | 3 | 6 | 10"),
        (
            "SELECT id FROM nw WHERE dt >= CURDATE()",
            "1 | 2 | 3 | 5 | 11",
        ),
        (
            "SELECT id FROM nw WHERE dt BETWEEN '2020-01-01' AND NOW()",
            "1 | 2 | 3 | 10",
        ),
        (
            "SELECT id FROM nw WHERE d IN (CURDATE(), '2020-02-02')",
            "1 | 2 | 3 | 6 | 10",
        ),
        ("SELECT id FROM nw WHERE t <= CURTIME()", "3 | 4 | 10"),
        ("SELECT id FROM nw WHERE b <= UNIX_TIMESTAMP()", "1 | 2"),
        (
            "SELECT id FROM nw WHERE ts <= NOW() AND ts > '2020-01-01'",
            "1 | 2",
        ),
    ] {
        assert_eq!(flat(&e, &f, sql).await, want, "{sql}");
    }
    // In UPDATE and DELETE.
    let u = ok(
        &e,
        &f,
        "UPDATE nw SET dt = NOW(), d = CURDATE() WHERE id = 10",
    )
    .await;
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 1  Warnings: 0")
    );
    assert_eq!(
        flat(&e, &f, "SELECT id FROM nw WHERE d = CURDATE() AND id = 10").await,
        "10"
    );
    assert_eq!(
        ok(&e, &f, "DELETE FROM nw WHERE dt > NOW() AND id > 5")
            .await
            .affected,
        1
    );
    // What the servers answer differently, or as numbers, is refused.
    for sql in [
        "INSERT INTO nw (id, y) VALUES (30, NOW())",
        "INSERT INTO nw (id, b) VALUES (30, NOW())",
        "INSERT INTO nw (id, t) VALUES (30, NOW())",
        "INSERT INTO nw (id, t) VALUES (30, CURDATE())",
        "INSERT INTO nw (id, d) VALUES (30, CURTIME())",
        "INSERT INTO nw (id, v) VALUES (30, NOW(3))",
        "INSERT INTO nw (id, dt) VALUES (30, NOW(7))",
        "INSERT INTO nw (id, dt) VALUES (30, SYSDATE())",
        "SELECT id FROM nw WHERE v <= NOW()",
        "SELECT id FROM nw WHERE id = NOW()",
        "SELECT id FROM nw WHERE t <= NOW()",
        "SELECT id FROM nw WHERE dt <= CURTIME()",
        "SELECT id FROM nw WHERE dt < NOW() + 1",
        "SELECT id FROM nw WHERE NOT NOW()",
        "SELECT id FROM nw WHERE NOW() > '2020-01-01'",
        "UPDATE nw SET b = NOW() + 1 WHERE id = 1",
        "UPDATE nw SET dt = -NOW() WHERE id = 1",
    ] {
        assert_eq!(err(&e, &f, sql).await.0, 39960, "{sql}");
    }
}
