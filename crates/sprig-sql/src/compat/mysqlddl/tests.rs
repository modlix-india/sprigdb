use super::*;

fn my() -> LexOptions {
    LexOptions::mysql84()
}

fn s(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8(b).unwrap())
}

const LONG: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn long_names_get_mysqls_1059() {
    assert_eq!(LONG.len(), 65);
    for sql in [
        format!("CREATE TABLE {LONG} (id int)"),
        format!("create table if not exists d.`{LONG}` (id int)"),
        format!("CREATE OR REPLACE ALGORITHM=MERGE VIEW {LONG} AS SELECT 1"),
        format!("CREATE DATABASE {LONG}"),
        format!("CREATE DEFINER=CURRENT_USER PROCEDURE {LONG}() SELECT 1"),
        format!("RENAME TABLE a TO b, c TO {LONG}"),
        format!("ALTER TABLE t RENAME TO {LONG}"),
        format!("ALTER TABLE t ADD COLUMN x int, RENAME AS d.{LONG}"),
    ] {
        let e = long_name(sql.as_bytes(), &my()).unwrap_or_else(|| panic!("{sql}"));
        assert_eq!(e.code, 1059, "{sql}");
        assert_eq!(e.message, format!("Identifier name '{LONG}' is too long"));
    }
    // 64 characters fit; other renames and other statements pass.
    let ok = &LONG[1..];
    for sql in [
        format!("CREATE TABLE {ok} (id int)"),
        format!("ALTER TABLE t RENAME COLUMN a TO {LONG}"),
        format!("CREATE INDEX {LONG} ON t (a)"),
        format!("SELECT 1 AS {LONG}"),
        "CREATE TABLE t (id int)".to_owned(),
    ] {
        assert_eq!(long_name(sql.as_bytes(), &my()), None, "{sql}");
    }
    // 3307: MariaDB's 1103 for tables, its 1102 already for databases.
    let m = LexOptions::mariadb();
    let e = long_name(format!("CREATE TABLE {LONG} (id int)").as_bytes(), &m).unwrap();
    assert_eq!(
        (e.code, e.message.as_str()),
        (1103, format!("Incorrect table name '{LONG}'").as_str())
    );
    assert_eq!(
        long_name(format!("CREATE DATABASE {LONG}").as_bytes(), &m),
        None
    );
}

#[test]
fn descending_parts_on_myisam() {
    let e = engine_descending(
        b"CREATE TABLE my (a int, b int, KEY k (a DESC, b)) ENGINE=MyISAM",
        &my(),
    );
    assert_eq!(e.map(|e| e.code), Some(1178));
    assert!(
        engine_descending(
            b"CREATE TABLE `m` (`a` int, UNIQUE `u` (`b`, `a`(5) DESC)) ENGINE MEMORY",
            &my()
        )
        .is_some()
    );
    for ok in [
        "CREATE TABLE my (a int, KEY k (a DESC)) ENGINE=InnoDB",
        "CREATE TABLE my (a int, KEY k (a)) ENGINE=MyISAM",
        "CREATE TABLE my (a int, KEY k (a DESC))",
        "CREATE TABLE my (a int, `desc` int, CHECK (a > 0)) ENGINE=MyISAM",
    ] {
        assert_eq!(engine_descending(ok.as_bytes(), &my()), None, "{ok}");
    }
    assert_eq!(
        engine_descending(
            b"CREATE TABLE my (a int, KEY k (a DESC)) ENGINE=MyISAM",
            &LexOptions::mariadb()
        ),
        None
    );
    assert_eq!(
        descending_index_table(b"CREATE UNIQUE INDEX k2 ON d.my2 (a DESC)", &my()).as_deref(),
        Some("d.my2")
    );
    assert_eq!(
        descending_index_table(
            b"ALTER TABLE `models` ADD UNIQUE INDEX `a_b_uniq` USING BTREE (`fieldB`, `fieldA`(5) DESC)",
            &my()
        )
        .as_deref(),
        Some("`models`")
    );
    assert_eq!(
        descending_index_table(b"ALTER TABLE t ADD INDEX i (a)", &my()),
        None
    );
    assert_eq!(
        descending_index_table(b"SELECT a FROM t ORDER BY a DESC", &my()),
        None
    );
}

#[test]
fn now_defaults_are_kept_apart() {
    let r = |q: &str| s(now_defaults(q.as_bytes(), &my()));
    assert_eq!(
        r("CREATE TABLE t (a datetime DEFAULT (now()), b datetime(6) DEFAULT (NOW(6)), c timestamp DEFAULT (CURRENT_TIMESTAMP))")
            .as_deref(),
        Some("CREATE TABLE t (a datetime DEFAULT (localtimestamp()), b datetime(6) DEFAULT (localtimestamp(6)), c timestamp DEFAULT (localtimestamp()))")
    );
    assert_eq!(
        r("ALTER TABLE `testings` ALTER COLUMN `updated_at` SET DEFAULT (now())").as_deref(),
        Some("ALTER TABLE `testings` ALTER COLUMN `updated_at` SET DEFAULT (localtimestamp())")
    );
    assert_eq!(
        r("CREATE TABLE t (a datetime(3) DEFAULT LOCALTIME(3) ON UPDATE localtimestamp(3), b datetime DEFAULT localtimestamp)")
            .as_deref(),
        Some("CREATE TABLE t (a datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), b datetime DEFAULT CURRENT_TIMESTAMP)")
    );
    // The special default, other expressions and other statements pass.
    for q in [
        "CREATE TABLE t (a datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE NOW())",
        "CREATE TABLE t (a datetime DEFAULT now())",
        "CREATE TABLE t (a datetime DEFAULT (now() + interval 1 day))",
        "INSERT INTO t VALUES (now())",
    ] {
        assert_eq!(r(q), None, "{q}");
    }
    assert_eq!(
        now_defaults(
            b"CREATE TABLE t (a datetime DEFAULT (now()))",
            &LexOptions::mariadb()
        ),
        None
    );
}

fn part(index: &str, seq: u32, column: &str, unique: bool) -> KeyPart {
    KeyPart {
        index: index.to_owned(),
        seq,
        column: Some(column.to_owned()),
        sub_part: None,
        desc: false,
        unique,
        index_type: "BTREE".to_owned(),
        comment: String::new(),
        visible: true,
    }
}

#[test]
fn dropping_a_column_of_a_unique_index() {
    let sql = b"ALTER TABLE `test_models` DROP COLUMN `hat_size`";
    let (table, at, cols) = dropped_columns(sql, &my()).unwrap();
    assert_eq!(table, "`test_models`");
    assert_eq!(cols, vec!["hat_size".to_owned()]);
    let mut b = part("u", 2, "b", true);
    b.sub_part = Some(10);
    b.desc = true;
    b.comment = "it's".to_owned();
    let parts = vec![
        part("PRIMARY", 1, "id", true),
        part("index_hat", 1, "hat_style", true),
        part("index_hat", 2, "hat_size", true),
        part("k", 1, "hat_size", false),
        part("k", 2, "c", false),
        part("u", 1, "hat_size", true),
        b,
        part("solo", 1, "hat_size", true),
    ];
    let out = s(unique_rebuild(sql, at, &cols, &parts, false)).unwrap();
    assert_eq!(
        out,
        "ALTER TABLE `test_models` DROP INDEX `index_hat`, ADD UNIQUE INDEX `index_hat` (`hat_style`), \
         DROP INDEX `u`, ADD UNIQUE INDEX `u` (`b`(10) DESC) COMMENT 'it''s', DROP COLUMN `hat_size`"
    );
    // No unique index with another column: nothing to do.
    assert_eq!(unique_rebuild(sql, at, &cols, &parts[3..5], false), None);
    // Other drops aren't columns.
    assert_eq!(
        dropped_columns(b"ALTER TABLE t DROP INDEX i, DROP FOREIGN KEY f", &my()),
        None
    );
    let (_, _, cols) =
        dropped_columns(b"alter table d.t drop a, drop column if exists `b`", &my()).unwrap();
    assert_eq!(cols, vec!["a".to_owned(), "b".to_owned()]);
}

#[test]
fn foreign_keys_and_their_errors() {
    let f = fk_refs(
        b"CREATE TABLE fc (id int, p int, CONSTRAINT fk1 FOREIGN KEY (p) REFERENCES missing_t(id), \
          FOREIGN KEY (id) REFERENCES o.p (id))",
        &my(),
    )
    .unwrap();
    assert_eq!(f.table, "fc");
    assert_eq!(
        f.refs,
        vec![
            (None, "missing_t".to_owned()),
            (Some("o".to_owned()), "p".to_owned())
        ]
    );
    assert_eq!(f.names, vec!["fk1".to_owned()]);
    let f = fk_refs(
        b"ALTER TABLE `fc` ADD CONSTRAINT `fk2` FOREIGN KEY (`id`) REFERENCES `nope` (`id`)",
        &my(),
    )
    .unwrap();
    assert_eq!((f.table.as_str(), f.names.len()), ("fc", 1));
    assert_eq!(fk_refs(b"CREATE TABLE t (id int)", &my()), None);
    assert!(is_fk_formed_error(
        1005,
        "Can't create table `d`.`fc` (errno: 150 \"Foreign key constraint is incorrectly formed\")"
    ));
    assert!(is_fk_duplicate_error(
        1005,
        "Can't create table `d`.`fc2` (errno: 121 \"Duplicate key on write or update\")"
    ));
    assert_eq!(
        fk_missing_table(None, "missing_t").message,
        "Failed to open the referenced table 'missing_t'"
    );
    assert_eq!(fk_duplicate_name("fk1").code, 1826);
}

// MariaDB 11.8.9's errors through the hidden columns, and MySQL 8.4.11's
// answers to the same statements (captured 15 November 2026; SQLancer
// found the first two).
#[test]
fn functional_index_errors_name_the_index() {
    let e = "( EXISTS (SELECT 1 wHERE FALSE)) IS NOT FALSE";
    let c = super::super::fxindex::fx_column(e);
    let msg = format!(
        "Function or expression 'select ...' cannot be used in the GENERATED ALWAYS AS clause of `{c}`"
    );
    let sql = format!("CREATE UNIQUE INDEX i1 ON t0(({e}))");
    let m = functional_index_error(1901, &msg, sql.as_bytes(), &my()).unwrap();
    assert_eq!(
        (m.code, m.sqlstate, m.message.as_str()),
        (
            3758,
            *b"HY000",
            "Expression of functional index 'i1' contains a disallowed function."
        )
    );
    // The index can't be found: MySQL's name for an unnamed one.
    let m = functional_index_error(1901, &msg, b"SELECT 1", &my()).unwrap();
    assert!(m.message.contains("'functional_index'"));
    assert!(!m.message.contains("_sprig_"));
    // A user's own generated column keeps MariaDB's error.
    assert_eq!(
        functional_index_error(
            1901,
            "Function or expression 'rand()' cannot be used in the GENERATED ALWAYS AS clause of `g`",
            b"ALTER TABLE t ADD g INT AS (RAND())",
            &my()
        ),
        None
    );
    assert_eq!(functional_index_error(1064, &msg, b"", &my()), None);
    // A hidden check refusing a row already there when the index is added.
    let c = super::super::fxindex::fx_column("CAST(s AS CHAR(2))");
    let msg = format!(
        "CONSTRAINT `{}` failed for `t9_d1_g1_s1`.`tc2`",
        super::super::fxindex::fx_check_name(&c)
    );
    let m = functional_index_error(
        4025,
        &msg,
        b"CREATE INDEX i2 ON tc2 ((CAST(s AS CHAR(2))))",
        &my(),
    )
    .unwrap();
    assert_eq!(
        (m.code, m.sqlstate, m.message.as_str()),
        (
            3751,
            *b"01000",
            "Data truncated for functional index 'i2' at row 1"
        )
    );
    let m = functional_index_error(
        4025,
        &msg,
        b"ALTER TABLE tc2 ADD INDEX ((CAST(s AS CHAR(2))))",
        &my(),
    )
    .unwrap();
    assert!(m.message.contains("'functional_index'"), "{}", m.message);
}

#[test]
fn dropping_a_column_something_generated_uses() {
    let m = "Unknown column 'c1' in 'GENERATED ALWAYS AS'";
    assert_eq!(generated_dependency_column(1054, m), Some("c1"));
    assert_eq!(
        generated_dependency_column(1054, "Unknown column 'c1' in 'field list'"),
        None
    );
    assert_eq!(generated_dependency_column(1146, m), None);
    assert!(expression_uses("`c0` + `C1`", "c1"));
    assert!(!expression_uses("`c10` * 2", "c1"));
    let f = generated_dependency("c1", false);
    assert_eq!(
        (f.code, f.message.as_str()),
        (
            3837,
            "Column 'c1' has a functional index dependency and cannot be dropped or renamed."
        )
    );
    let g = generated_dependency("c0", true);
    assert_eq!(
        (g.code, g.sqlstate, g.message.as_str()),
        (
            3108,
            *b"HY000",
            "Column 'c0' has a generated column dependency."
        )
    );
}

// MySQL 8.4.11's answers (captured 15 November 2026) to renaming, and to
// dropping every visible column, where a functional index uses the column;
// MariaDB 11.8 renames inside the hidden column's expression, and answers
// the drop with 1113.
#[test]
fn renaming_or_dropping_what_a_functional_index_uses() {
    let r = |sql: &str| renamed_columns(sql.as_bytes(), &my());
    assert_eq!(
        r("ALTER TABLE r1 RENAME COLUMN c1 TO c9"),
        Some(("r1".to_owned(), vec!["c1".to_owned()]))
    );
    assert_eq!(
        r("alter table d.`r1` change c1 c8 INT, RENAME COLUMN c0 TO c7"),
        Some(("d.`r1`".to_owned(), vec!["c1".to_owned(), "c0".to_owned()]))
    );
    assert_eq!(
        r("ALTER TABLE r1 CHANGE COLUMN IF EXISTS c1 c2 BIGINT"),
        Some(("r1".to_owned(), vec!["c1".to_owned()]))
    );
    // Same name (a type change only), MODIFY, a table rename: none.
    assert_eq!(r("ALTER TABLE r1 CHANGE COLUMN c1 C1 BIGINT"), None);
    assert_eq!(r("ALTER TABLE r1 MODIFY c1 BIGINT"), None);
    assert_eq!(r("ALTER TABLE r1 RENAME TO r2"), None);
    assert_eq!(
        renamed_columns(
            b"ALTER TABLE r1 RENAME COLUMN c1 TO c9",
            &LexOptions::mariadb()
        ),
        None
    );
    let fx = super::super::fxindex::fx_column("c0 + c1");
    let generated = vec![
        (fx.clone(), "`c0` + `c1`".to_owned()),
        ("g".to_owned(), "`c2` * 2".to_owned()),
    ];
    let cols = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let m = generated_dependency_of(&cols(&["c5", "c1", "c0"]), &generated).unwrap();
    assert_eq!(
        (m.code, m.sqlstate, m.message.as_str()),
        (
            3837,
            *b"HY000",
            "Column 'c1' has a functional index dependency and cannot be dropped or renamed."
        )
    );
    let m = generated_dependency_of(&cols(&["c2"]), &generated).unwrap();
    assert_eq!(
        (m.code, m.message.as_str()),
        (3108, "Column 'c2' has a generated column dependency.")
    );
    assert_eq!(generated_dependency_of(&cols(&["c5"]), &generated), None);
    // Other internal generated columns aren't the user's.
    assert_eq!(
        generated_dependency_of(&cols(&["a"]), &[("_sprig_x".to_owned(), "`a`".to_owned())]),
        None
    );
    // `ALTER TABLE d3 DROP COLUMN b, DROP COLUMN a` with `INDEX fx ((a + 1))`:
    // MariaDB's 1113, MySQL's 3837 naming `a`.
    let fa = super::super::fxindex::fx_column("a + 1");
    let g = vec![(fa, "`a` + 1".to_owned())];
    let m = fx_drop_all(1113, &cols(&["b", "a"]), &g).unwrap();
    assert_eq!(
        m.message,
        "Column 'a' has a functional index dependency and cannot be dropped or renamed."
    );
    assert_eq!(fx_drop_all(1113, &cols(&["b"]), &g), None);
    assert_eq!(fx_drop_all(1054, &cols(&["a"]), &g), None);
}

// MariaDB's errors for a value that doesn't fit a hidden column, and
// MySQL 8.4.11's 3752 and 3751 naming the index.
#[test]
fn values_that_dont_fit_a_functional_index() {
    let c = "_sprig_fx_0a1b2c3d";
    let e = fx_value_error(
        1264,
        &format!("Out of range value for column '{c}' at row 2"),
    )
    .unwrap();
    assert_eq!(
        e,
        FxValueError {
            column: c.to_owned(),
            row: 2,
            out_of_range: true
        }
    );
    let m = functional_index_value("ix", &e);
    assert_eq!(
        (m.code, m.sqlstate, m.message.as_str()),
        (
            3752,
            *b"22003",
            "Value is out of range for functional index 'ix' at row 2"
        )
    );
    let e = fx_value_error(
        1366,
        &format!("Incorrect integer value: 'abc' for column `d`.`t`.`{c}` at row 1"),
    )
    .unwrap();
    assert!(!e.out_of_range);
    let m = functional_index_value("functional_index", &e);
    assert_eq!(
        (m.code, m.sqlstate, m.message.as_str()),
        (
            3751,
            *b"01000",
            "Data truncated for functional index 'functional_index' at row 1"
        )
    );
    for code in [1265, 1406] {
        assert!(
            fx_value_error(code, &format!("Data truncated for column '{c}' at row 1")).is_some()
        );
    }
    // A user's column, another code, or a hidden column named elsewhere.
    // A hidden column's check (no row number in MariaDB's words).
    let e = fx_value_error(
        4025,
        "CONSTRAINT `_sprig_fxck_0a1b2c3d` failed for `t9_d1_g1_s1`.`t`",
    )
    .unwrap();
    assert_eq!(
        (e.column.as_str(), e.row, e.out_of_range),
        ("_sprig_fx_0a1b2c3d", 1, false)
    );
    assert_eq!(
        fx_value_error(4025, "CONSTRAINT `chk_1` failed for `db`.`t`"),
        None
    );
    assert_eq!(
        fx_value_error(1264, "Out of range value for column 'c0' at row 1"),
        None
    );
    assert_eq!(
        fx_value_error(1062, &format!("Duplicate entry '1' for key '{c}'")),
        None
    );
    assert_eq!(
        fx_value_error(1264, "Out of range value for column '(null)' at row 1"),
        None
    );
    // The index: its own name, or MySQL's for unnamed ones in key order.
    let parts = vec![
        (
            "_sprig_fx_00000001".to_owned(),
            "_sprig_fx_00000001".to_owned(),
        ),
        ("named".to_owned(), "_sprig_fx_00000002".to_owned()),
        (
            "_sprig_fx_00000003".to_owned(),
            "_sprig_fx_00000003".to_owned(),
        ),
        (
            "_sprig_fx_00000003".to_owned(),
            "_sprig_fx_00000004".to_owned(),
        ),
    ];
    assert_eq!(
        fx_index_name(&parts, "_sprig_fx_00000001").as_deref(),
        Some("functional_index")
    );
    assert_eq!(
        fx_index_name(&parts, "_SPRIG_FX_00000002").as_deref(),
        Some("named")
    );
    assert_eq!(
        fx_index_name(&parts, "_sprig_fx_00000004").as_deref(),
        Some("functional_index_2")
    );
    assert_eq!(fx_index_name(&parts, "_sprig_fx_00000009"), None);
}

// MariaDB's errors from computing a hidden column's expression (they name
// no hidden column), as MariaDB 11.8 raises them for an `UPDATE` or an
// `INSERT` naming its columns, and the key part MySQL 8.4.11 names in its
// 3751 or 3752.
#[test]
fn conversions_that_fail_inside_a_functional_index() {
    let s = |v: &str| v.to_owned();
    let int = fx_eval_error(1292, "Truncated incorrect INTEGER value: 'abc'").unwrap();
    assert_eq!(
        int,
        FxEvalError {
            kind: FxEvalKind::Integer,
            row: 1
        }
    );
    let dbl = fx_eval_error(1292, "Truncated incorrect DOUBLE value: 'x'").unwrap();
    assert_eq!(dbl.kind, FxEvalKind::Double);
    let dec = fx_eval_error(1264, "Out of range value for column '(null)' at row 3").unwrap();
    assert_eq!((dec.kind.clone(), dec.row), (FxEvalKind::DecimalRange, 3));
    let date = fx_eval_error(
        1292,
        "Incorrect datetime value: 'nope' for column `t1_d2`.`t8`.`s` at row 2",
    )
    .unwrap();
    assert_eq!(
        date,
        FxEvalError {
            kind: FxEvalKind::Temporal { column: s("s") },
            row: 2
        }
    );
    // Not conversions inside an expression.
    assert_eq!(
        fx_eval_error(1264, "Out of range value for column 'c0' at row 1"),
        None
    );
    assert_eq!(
        fx_eval_error(1292, "Truncated incorrect DECIMAL value: 'x'"),
        None
    );
    assert_eq!(
        fx_eval_error(
            1366,
            "Incorrect integer value: 'abc' for column `d`.`t`.`c` at row 1"
        ),
        None
    );
    let hidden = "_sprig_fx_0a1b2c3d";
    assert_eq!(
        fx_eval_error(
            1292,
            &format!("Incorrect date value: 'x' for column `d`.`t`.`{hidden}` at row 1")
        ),
        None
    );

    // t (s VARCHAR, d INT, k DATE, INDEX ix ((CAST(s AS SIGNED))),
    //    INDEX ((CAST(d AS DECIMAL(3,1)))), INDEX iy ((year(s))))
    let (h1, h2, h3) = (
        "_sprig_fx_00000001",
        "_sprig_fx_00000002",
        "_sprig_fx_00000003",
    );
    let columns = vec![
        (s("s"), s("varchar"), None),
        (s("d"), s("int"), None),
        (s("k"), s("date"), None),
        (s(h1), s("bigint"), Some(s("cast(`s` as signed)"))),
        (s(h2), s("decimal"), Some(s("cast(`d` as decimal(3,1))"))),
        (s(h3), s("int"), Some(s("year(`s`)"))),
    ];
    let parts = vec![(s("ix"), s(h1)), (s(h2), s(h2)), (s("iy"), s(h3))];
    assert_eq!(fx_eval_part(&int, &columns, &parts).as_deref(), Some(h1));
    assert_eq!(fx_eval_part(&dec, &columns, &parts).as_deref(), Some(h2));
    assert_eq!(
        fx_index_name(&parts, h2).as_deref(),
        Some("functional_index")
    );
    let year = FxEvalError {
        kind: FxEvalKind::Temporal { column: s("S") },
        row: 1,
    };
    assert_eq!(fx_eval_part(&year, &columns, &parts).as_deref(), Some(h3));
    // No key part casts to DOUBLE.
    assert_eq!(fx_eval_part(&dbl, &columns, &parts), None);
    // A date column the value was stored in: MariaDB's own error stands.
    let stored = FxEvalError {
        kind: FxEvalKind::Temporal { column: s("k") },
        row: 1,
    };
    assert_eq!(fx_eval_part(&stored, &columns, &parts), None);
    // A visible generated column with the same cast: MySQL answers as
    // MariaDB does (1292).
    let mut with_gen = columns.clone();
    with_gen.push((s("g"), s("bigint"), Some(s("cast(`s` as signed)"))));
    assert_eq!(fx_eval_part(&int, &with_gen, &parts), None);
}

#[test]
fn a_statements_own_cast_keeps_its_error() {
    let s = |v: &str| v.to_owned();
    let h1 = "_sprig_fx_00000001";
    assert!(statement_converts(
        b"UPDATE t SET d = CAST('x' AS SIGNED)",
        &FxEvalKind::Integer
    ));
    assert!(!statement_converts(
        b"UPDATE t SET s = 'zz'",
        &FxEvalKind::Integer
    ));
    let m = functional_index_value(
        "ix",
        &FxValueError {
            column: s(h1),
            row: 1,
            out_of_range: false,
        },
    );
    assert_eq!(
        (m.code, m.message.as_str()),
        (3751, "Data truncated for functional index 'ix' at row 1")
    );
}

// Renaming or dropping a column that a check or a default expression uses,
// as MySQL 8.4.11 answers (MariaDB renames it inside them, and refuses the
// drop with 1054).
#[test]
fn renames_and_drops_that_checks_and_defaults_use() {
    let col = |n: &str, g: Option<&str>, d: Option<&str>| TableColumn {
        name: n.to_owned(),
        generation: g.map(str::to_owned),
        default: d.map(str::to_owned),
    };
    let check = |n: &str, c: &str| TableCheck {
        name: n.to_owned(),
        clause: c.to_owned(),
    };
    let names = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    // r9 (a INT CHECK (a > 0), b INT, CHECK (b > 1), CONSTRAINT named
    // CHECK (a < b)): MySQL names `named` for b, the first by name.
    let columns = vec![col("A", None, None), col("b", None, None)];
    let checks = vec![
        check("r9_chk_2", "`b` > 1"),
        check("named", "`a` < `b`"),
        check("r9_chk_1", "`a` > 0"),
    ];
    let m = rename_dependency(&names(&["b"]), "r9", &columns, &checks).unwrap();
    assert_eq!(
        (m.code, m.message.as_str()),
        (
            3959,
            "Check constraint 'named' uses column 'b', hence column cannot be dropped or renamed."
        )
    );
    // The column as defined.
    let m = rename_dependency(&names(&["a"]), "r9", &columns, &checks).unwrap();
    assert!(m.message.contains("uses column 'A'"), "{}", m.message);
    // A default expression, then generated columns and functional parts.
    let columns = vec![
        col("a", None, None),
        col("b", None, Some("(`a` + 1)")),
        col("c", None, Some("'`a`'")),
        col("d", None, None),
        col("g", Some("`d` * 2"), None),
        col("_sprig_fx_00000001", Some("`c` + 1"), None),
    ];
    let m = rename_dependency(&names(&["a"]), "r2", &columns, &[]).unwrap();
    assert_eq!(
        (m.code, m.message.as_str()),
        (
            3766,
            "Column 'a' of table 'r2' has a default value expression dependency and cannot be dropped or renamed."
        )
    );
    assert_eq!(
        rename_dependency(&names(&["d"]), "r2", &columns, &[]).map(|m| m.code),
        Some(3108)
    );
    assert_eq!(
        rename_dependency(&names(&["c"]), "r2", &columns, &[]).map(|m| m.code),
        Some(3837)
    );
    // A literal default naming the column in quotes is no expression.
    assert_eq!(
        rename_dependency(&names(&["b", "g"]), "r2", &columns, &[]),
        None
    );
    // A check before the functional index (r7).
    let checks = vec![check("ck7", "`c` > 0")];
    assert_eq!(
        rename_dependency(&names(&["c"]), "r2", &columns, &checks).map(|m| m.code),
        Some(3959)
    );

    // Drops: MariaDB's 1054.
    assert_eq!(
        dropped_dependency_column(1054, "Unknown column 'a' in 'CHECK'"),
        Some(("a", true))
    );
    assert_eq!(
        dropped_dependency_column(1054, "Unknown column 'a' in 'DEFAULT'"),
        Some(("a", false))
    );
    assert_eq!(
        dropped_dependency_column(1054, "Unknown column 'a' in 'GENERATED ALWAYS AS'"),
        None
    );
    assert_eq!(
        dropped_dependency_column(1091, "Unknown column 'a' in 'CHECK'"),
        None
    );
    let checks = vec![check("r1b_chk_1", "`a` > 0"), check("ck1", "`a` > `b`")];
    let m = drop_dependency("a", true, "r1b", &checks).unwrap();
    assert_eq!(
        m.message,
        "Check constraint 'ck1' uses column 'a', hence column cannot be dropped or renamed."
    );
    assert_eq!(drop_dependency("a", true, "r1b", &checks[..1]), None);
    assert_eq!(
        drop_dependency("a", false, "r2", &[]).map(|m| m.code),
        Some(3766)
    );
}
