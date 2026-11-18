//! Functional key parts on 3306 through a running deployment, as MySQL 8.4
//! answers them, over the binary protocol (`COM_STMT_PREPARE` and
//! `COM_STMT_EXECUTE`, which `exec_drop` with parameters sends):
//!
//! - a prepared `INSERT` whose value doesn't fit an unnamed functional
//!   index's cast fails with 3751 naming `functional_index`; one out of
//!   range of a named index's `DECIMAL(3,1)` with 3752 naming it; a
//!   prepared `UPDATE` likewise; nothing is written;
//! - an index added without a name gets the first free
//!   `functional_index_<n>`; `SHOW INDEX`, `SHOW CREATE TABLE` and
//!   `information_schema.STATISTICS` show MySQL's names;
//!   `DROP INDEX functional_index` works, and the next unnamed index takes
//!   the freed name.
//!
//! - values MySQL's strict hidden column refuses and MariaDB only warns
//!   about: `CAST(s AS CHAR(2))` of a longer value and `CAST(s AS SIGNED)`
//!   of text that isn't an integer, in `INSERT` with and without a column
//!   list, `INSERT ... SET`, `REPLACE` and `UPDATE`, as text and prepared,
//!   answer 3751 naming the index and write nothing; values MySQL takes
//!   (spaces around an integer, one above 2^63 - 1) are written; the hidden
//!   check shows nowhere; `CREATE INDEX` over a row it refuses answers
//!   3751; dropping or renaming the cast column answers 3837; after `DROP
//!   INDEX` the longer value is written.
//!
//! The expected answers were captured from MySQL 8.4.11; the test passes
//! against it too (`SPRIG_TEST_FX_URL=mysql://root:<pw>@127.0.0.1:<port>`).
//!
//! ```sh
//! export SPRIG_TEST_FX_URL='mysql://acme.app:sprig-dev-app@127.0.0.1:47306?require_ssl=true&verify_ca=false'
//! cargo test -p sprig-gateway --test fxindex_live -- --nocapture
//! ```
//!
//! Skipped without `SPRIG_TEST_FX_URL`. Makes and drops the database
//! `fx_live`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    missing_docs
)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

const DB: &str = "fx_live";

fn server_error(r: Result<(), mysql_async::Error>) -> (u16, String, String) {
    match r {
        Err(mysql_async::Error::Server(s)) => (s.code, s.state, s.message),
        other => panic!("expected a server error, got {other:?}"),
    }
}

/// The table's index names, as `information_schema.STATISTICS` gives them.
async fn index_names(c: &mut Conn) -> Vec<String> {
    c.query(format!(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = '{DB}' AND TABLE_NAME = 't' ORDER BY INDEX_NAME"
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn functional_indexes_as_mysql_answers_them() {
    let Ok(url) = std::env::var("SPRIG_TEST_FX_URL") else {
        eprintln!("skipped: SPRIG_TEST_FX_URL is not set");
        return;
    };
    let mut c = Conn::new(Opts::from_url(&url).expect("url"))
        .await
        .expect("login");
    c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    c.query_drop(
        "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(20), d INT, \
         INDEX ((CAST(s AS SIGNED))), INDEX dx ((CAST(d AS DECIMAL(3,1)))))",
    )
    .await
    .unwrap();

    // Prepared statements: MySQL's errors, naming the index.
    let insert = "INSERT INTO t (id, s, d) VALUES (?, ?, ?)";
    assert_eq!(
        server_error(c.exec_drop(insert, (1, "abc", 1)).await),
        (
            3751,
            "01000".to_owned(),
            "Data truncated for functional index 'functional_index' at row 1".to_owned()
        )
    );
    assert_eq!(
        server_error(c.exec_drop(insert, (2, "5", 12345)).await),
        (
            3752,
            "22003".to_owned(),
            "Value is out of range for functional index 'dx' at row 1".to_owned()
        )
    );
    c.exec_drop(insert, (3, "7", 2)).await.unwrap();
    assert_eq!(
        server_error(
            c.exec_drop("UPDATE t SET d = ? WHERE id = ?", (99_999, 3))
                .await
        )
        .0,
        3752
    );
    let rows: Vec<(i32, String, i32)> = c.query("SELECT id, s, d FROM t").await.unwrap();
    assert_eq!(rows, vec![(3, "7".to_owned(), 2)]);

    // Names: an unnamed index added later takes the first free name.
    c.query_drop("ALTER TABLE t ADD INDEX ((d + 1))")
        .await
        .unwrap();
    let shown: Vec<mysql_async::Row> = c.query("SHOW INDEX FROM t").await.unwrap();
    let keys: Vec<(String, Option<String>, Option<String>)> = shown
        .into_iter()
        .map(|mut r| {
            (
                r.take::<String, _>("Key_name").unwrap(),
                r.take::<Option<String>, _>("Column_name").unwrap(),
                r.take::<Option<String>, _>("Expression").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            ("PRIMARY".to_owned(), Some("id".to_owned()), None),
            (
                "functional_index".to_owned(),
                None,
                Some("cast(`s` as signed)".to_owned())
            ),
            (
                "dx".to_owned(),
                None,
                Some("cast(`d` as decimal(3,1))".to_owned())
            ),
            (
                "functional_index_2".to_owned(),
                None,
                Some("(`d` + 1)".to_owned())
            ),
        ]
    );
    let (_, create): (String, String) =
        c.query_first("SHOW CREATE TABLE t").await.unwrap().unwrap();
    let key_lines: Vec<&str> = create
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("KEY "))
        .map(|l| l.split(" (").next().unwrap_or(l))
        .collect();
    assert_eq!(
        key_lines,
        vec![
            "KEY `functional_index`",
            "KEY `dx`",
            "KEY `functional_index_2`"
        ],
        "{create}"
    );
    assert!(!create.contains("_sprig_"), "{create}");

    // The freed name is taken again.
    c.query_drop("ALTER TABLE t DROP INDEX functional_index")
        .await
        .unwrap();
    assert_eq!(
        index_names(&mut c).await,
        vec!["dx", "functional_index_2", "PRIMARY"]
    );
    c.query_drop("ALTER TABLE t ADD INDEX ((d + 2))")
        .await
        .unwrap();
    assert_eq!(
        index_names(&mut c).await,
        vec!["dx", "functional_index", "functional_index_2", "PRIMARY"]
    );
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    c.disconnect().await.unwrap();
}

/// MySQL's 3751 for functional index `index`, at row 1.
fn truncated(index: &str) -> (u16, String, String) {
    (
        3751,
        "01000".to_owned(),
        format!("Data truncated for functional index '{index}' at row 1"),
    )
}

#[tokio::test]
async fn casts_mysql_refuses_are_refused() {
    let Ok(url) = std::env::var("SPRIG_TEST_FX_URL") else {
        eprintln!("skipped: SPRIG_TEST_FX_URL is not set");
        return;
    };
    let db = "fx_live_cast";
    let mut c = Conn::new(Opts::from_url(&url).expect("url"))
        .await
        .expect("login");
    c.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    c.query_drop(format!("USE {db}")).await.unwrap();
    c.query_drop(
        "CREATE TABLE tc (id INT PRIMARY KEY, s VARCHAR(20), INDEX i ((CAST(s AS CHAR(2)))))",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE ts (id INT PRIMARY KEY, s VARCHAR(30), INDEX ((CAST(s AS SIGNED))))",
    )
    .await
    .unwrap();

    // CHAR(2): every statement form, text and prepared.
    for q in [
        "INSERT INTO tc VALUES (1, 'abc')",
        "INSERT INTO tc (id, s) VALUES (1, 'abc')",
        "INSERT INTO tc SET id = 1, s = 'abc'",
        "REPLACE INTO tc VALUES (1, 'abc')",
        "INSERT INTO tc VALUES (1, 'ab ')",
    ] {
        assert_eq!(server_error(c.query_drop(q).await), truncated("i"), "{q}");
    }
    assert_eq!(
        server_error(
            c.exec_drop("INSERT INTO tc VALUES (?, ?)", (1, "abc"))
                .await
        ),
        truncated("i")
    );
    c.query_drop("INSERT INTO tc VALUES (2, 'ab'), (3, NULL)")
        .await
        .unwrap();
    assert_eq!(
        server_error(c.query_drop("UPDATE tc SET s = 'abcd' WHERE id = 2").await),
        truncated("i")
    );
    assert_eq!(
        server_error(
            c.exec_drop("UPDATE tc SET s = ? WHERE id = ?", ("xyz", 2))
                .await
        ),
        truncated("i")
    );
    let rows: Vec<(i32, Option<String>)> =
        c.query("SELECT id, s FROM tc ORDER BY id").await.unwrap();
    assert_eq!(rows, vec![(2, Some("ab".to_owned())), (3, None)]);

    // SIGNED of text: integers between spaces, from -2^63 to 2^64 - 1.
    for (id, v) in [
        (1, "abc"),
        (2, "1.5"),
        (3, ""),
        (4, "-9223372036854775809"),
        (5, "1e3"),
    ] {
        assert_eq!(
            server_error(c.exec_drop("INSERT INTO ts VALUES (?, ?)", (id, v)).await),
            truncated("functional_index"),
            "{v:?}"
        );
    }
    assert_eq!(
        server_error(c.query_drop("INSERT INTO ts VALUES (6, '12abc')").await),
        truncated("functional_index")
    );
    for (id, v) in [
        (7, " 12 "),
        (8, "-7"),
        (9, "9223372036854775808"),
        (10, "00012"),
    ] {
        c.exec_drop("INSERT INTO ts VALUES (?, ?)", (id, v))
            .await
            .unwrap_or_else(|e| panic!("{v:?}: {e}"));
    }
    let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM ts").await.unwrap();
    assert_eq!(n, Some(4));

    // The check shows nowhere.
    for t in ["tc", "ts"] {
        let (_, create): (String, String) = c
            .query_first(format!("SHOW CREATE TABLE {t}"))
            .await
            .unwrap()
            .unwrap();
        assert!(!create.contains("_sprig_"), "{create}");
        assert!(!create.contains("CONSTRAINT"), "{create}");
    }
    let checks: Vec<String> = c
        .query(format!(
            "SELECT CONSTRAINT_NAME FROM information_schema.CHECK_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = '{db}'"
        ))
        .await
        .unwrap();
    assert!(checks.is_empty(), "{checks:?}");
    let constraints: Vec<(String, String)> = c
        .query(format!(
            "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.TABLE_CONSTRAINTS \
             WHERE TABLE_SCHEMA = '{db}' ORDER BY TABLE_NAME"
        ))
        .await
        .unwrap();
    assert_eq!(
        constraints,
        vec![
            ("PRIMARY".to_owned(), "PRIMARY KEY".to_owned()),
            ("PRIMARY".to_owned(), "PRIMARY KEY".to_owned())
        ]
    );

    // The cast column can't be dropped or renamed.
    for q in [
        "ALTER TABLE ts DROP COLUMN s",
        "ALTER TABLE ts RENAME COLUMN s TO s2",
    ] {
        assert_eq!(
            server_error(c.query_drop(q).await),
            (
                3837,
                "HY000".to_owned(),
                "Column 's' has a functional index dependency and cannot be dropped or renamed."
                    .to_owned()
            ),
            "{q}"
        );
    }

    // Dropping the index lets the longer value in; an index over a row
    // it refuses isn't made.
    c.query_drop("DROP INDEX i ON tc").await.unwrap();
    c.query_drop("INSERT INTO tc VALUES (4, 'abcdef')")
        .await
        .unwrap();
    assert_eq!(
        server_error(
            c.query_drop("CREATE INDEX i2 ON tc ((CAST(s AS CHAR(3))))")
                .await
        ),
        truncated("i2")
    );
    c.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
    c.disconnect().await.unwrap();
}
