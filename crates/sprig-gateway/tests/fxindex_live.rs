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
