use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::lex::LexOptions;

fn run(sql: &str, persona: Persona, mode: Mode, default: Option<&str>) -> Outcome {
    let opts = match persona {
        Persona::MySql84 => LexOptions::mysql84(),
        Persona::MariaDb => LexOptions::mariadb(),
    };
    match plan(sql.as_bytes(), &opts) {
        Some(p) => p.apply(sql.as_bytes(), persona, mode, default),
        None => Outcome::Unchanged,
    }
}

fn mysql(sql: &str) -> Outcome {
    run(sql, Persona::MySql84, Mode::Innodb, None)
}

fn maria(sql: &str) -> Outcome {
    run(sql, Persona::MariaDb, Mode::Innodb, None)
}

/// The rewritten text and the notes as `level code message`.
fn text(o: &Outcome) -> (String, Vec<String>) {
    match o {
        Outcome::Rewritten { sql, notes } => (
            String::from_utf8(sql.clone()).unwrap(),
            notes
                .iter()
                .map(|n| format!("{} {} {}", n.level, n.code, n.message))
                .collect(),
        ),
        other => panic!("not rewritten: {other:?}"),
    }
}

fn refused(o: &Outcome) -> (u16, String) {
    match o {
        Outcome::Refused(e) => (e.code, e.message.clone()),
        other => panic!("not refused: {other:?}"),
    }
}

#[test]
fn engine_names_are_classified() {
    assert_eq!(classify("InnoDB"), Engine::InnoDb);
    assert_eq!(classify("myisam"), Engine::Substituted("MyISAM"));
    assert_eq!(classify("HEAP"), Engine::Substituted("MEMORY"));
    assert_eq!(classify("Aria"), Engine::Substituted("Aria"));
    assert_eq!(classify("merge"), Engine::Refused("MRG_MYISAM"));
    assert_eq!(classify("BLACKHOLE"), Engine::Refused("BLACKHOLE"));
    assert_eq!(classify("Foo"), Engine::Unknown);
    assert_eq!(Mode::parse("reject"), Mode::Reject);
    assert_eq!(Mode::parse("innodb"), Mode::Innodb);
}

#[test]
fn create_table_gets_innodb_with_mysqls_warnings() {
    let (sql, notes) = text(&mysql(
        "CREATE TABLE t (a int, engine varchar(10), KEY k (a DESC)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4",
    ));
    assert_eq!(
        sql,
        "CREATE TABLE t (a int, engine varchar(10), KEY k (a DESC)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );
    assert_eq!(
        notes,
        [
            "Warning 3161 Storage engine MyISAM is disabled (Table creation is disallowed).",
            "Warning 1266 Using storage engine InnoDB for table 't'",
        ]
    );
    // Every form of the clause: STORAGE, no `=`, quoted, a string, lower
    // case, a qualified name, IF NOT EXISTS, OR REPLACE.
    for (from, to) in [
        (
            "create table if not exists d.t2 (a int) storage engine memory",
            "create table if not exists d.t2 (a int) storage engine InnoDB",
        ),
        (
            "CREATE TABLE `t2` (a int) ENGINE = `Aria` COMMENT 'x'",
            "CREATE TABLE `t2` (a int) ENGINE = InnoDB COMMENT 'x'",
        ),
        (
            "CREATE TABLE t2 (a int) ENGINE='csv'",
            "CREATE TABLE t2 (a int) ENGINE=InnoDB",
        ),
        (
            "CREATE TABLE t2 (a int) /*!50100 ENGINE=HEAP */",
            "CREATE TABLE t2 (a int) /*!50100 ENGINE=InnoDB */",
        ),
    ] {
        assert_eq!(text(&mysql(from)).0, to, "{from}");
    }
    let (_, notes) = text(&mysql("CREATE TABLE d.t2 (a int) ENGINE=heap"));
    assert_eq!(
        notes[0],
        "Warning 3161 Storage engine MEMORY is disabled (Table creation is disallowed)."
    );
    assert_eq!(
        notes[1],
        "Warning 1266 Using storage engine InnoDB for table 't2'"
    );
}

#[test]
fn create_table_on_3307_gets_mariadbs_note() {
    let (sql, notes) = text(&maria("CREATE OR REPLACE TABLE t (a int) ENGINE=Aria"));
    assert_eq!(sql, "CREATE OR REPLACE TABLE t (a int) ENGINE=InnoDB");
    assert_eq!(
        notes,
        ["Note 1266 Using storage engine InnoDB for table 't'"]
    );
}

#[test]
fn select_and_partitions() {
    // The SELECT of CREATE TABLE ... SELECT isn't read.
    assert_eq!(
        text(&mysql(
            "CREATE TABLE t ENGINE=MyISAM AS SELECT engine FROM x WHERE engine = 'MyISAM'"
        ))
        .0,
        "CREATE TABLE t ENGINE=InnoDB AS SELECT engine FROM x WHERE engine = 'MyISAM'"
    );
    assert_eq!(
        mysql("CREATE TABLE t (a int) SELECT 1 AS a, 'ENGINE=MyISAM' AS b"),
        Outcome::Unchanged
    );
    // Partitions' engines become InnoDB too.
    assert_eq!(
        text(&mysql(
            "CREATE TABLE p (x int) ENGINE=MyISAM PARTITION BY RANGE (x) (PARTITION p0 VALUES LESS THAN (10) ENGINE = MyISAM, PARTITION p1 VALUES LESS THAN MAXVALUE STORAGE ENGINE MyISAM)"
        ))
        .0,
        "CREATE TABLE p (x int) ENGINE=InnoDB PARTITION BY RANGE (x) (PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB, PARTITION p1 VALUES LESS THAN MAXVALUE STORAGE ENGINE InnoDB)"
    );
    // A column named `engine` in a partition expression or a check.
    assert_eq!(
        mysql(
            "CREATE TABLE p (engine int, CHECK (engine = 'MyISAM')) PARTITION BY HASH (engine) PARTITIONS 2"
        ),
        Outcome::Unchanged
    );
}

#[test]
fn innodb_unknown_like_and_temporary_are_left_alone() {
    for sql in [
        "CREATE TABLE t (a int) ENGINE=InnoDB",
        "CREATE TABLE t (a int) ENGINE=Foo",
        "CREATE TABLE t LIKE u",
        "CREATE TABLE t (LIKE u)",
        "CREATE TEMPORARY TABLE t (a int) ENGINE=MEMORY",
        "CREATE TABLE t (a int, engine varchar(10))",
        "CREATE INDEX i ON t (engine)",
        "SELECT 'ENGINE=MyISAM'",
        "ALTER TABLE t ADD COLUMN engine int",
        "ALTER TABLE t ADD CONSTRAINT c CHECK (engine = 'MyISAM')",
    ] {
        assert_eq!(mysql(sql), Outcome::Unchanged, "{sql}");
        assert_eq!(maria(sql), Outcome::Unchanged, "{sql}");
    }
}

#[test]
fn alter_table_on_3306_keeps_the_engine_with_1286() {
    for (from, to) in [
        ("ALTER TABLE t ENGINE=MyISAM", "ALTER TABLE t "),
        (
            "ALTER TABLE t ENGINE=MyISAM, ADD COLUMN b int",
            "ALTER TABLE t  ADD COLUMN b int",
        ),
        (
            "ALTER TABLE t ADD COLUMN b int, ENGINE = MEMORY",
            "ALTER TABLE t ADD COLUMN b int",
        ),
        (
            "ALTER TABLE t COMMENT 'x' ENGINE=MyISAM, ADD COLUMN b int",
            "ALTER TABLE t COMMENT 'x' , ADD COLUMN b int",
        ),
        (
            "ALTER TABLE t ENGINE=MyISAM, ENGINE=MEMORY",
            "ALTER TABLE t ",
        ),
    ] {
        let (sql, notes) = text(&mysql(from));
        assert_eq!(sql, to, "{from}");
        assert_eq!(notes.len(), 1, "{from}");
        assert!(
            notes[0].starts_with("Warning 1286 Unknown storage engine '"),
            "{from}"
        );
    }
    assert_eq!(
        text(&mysql("ALTER TABLE t ENGINE=MyISAM")).1,
        ["Warning 1286 Unknown storage engine 'MyISAM'"]
    );
    // A partition's engine becomes InnoDB.
    assert_eq!(
        text(&mysql(
            "ALTER TABLE p ADD PARTITION (PARTITION p2 VALUES LESS THAN (30) ENGINE = MyISAM)"
        )),
        (
            "ALTER TABLE p ADD PARTITION (PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB)"
                .to_owned(),
            Vec::new()
        )
    );
}

#[test]
fn alter_table_on_3307_becomes_innodb_with_a_note() {
    let (sql, notes) = text(&maria("ALTER IGNORE TABLE d.t ENGINE=MyISAM"));
    assert_eq!(sql, "ALTER IGNORE TABLE d.t ENGINE=InnoDB");
    assert_eq!(
        notes,
        ["Note 1266 Using storage engine InnoDB for table 't'"]
    );
}

#[test]
fn reject_mode_and_refused_engines() {
    let r = |sql: &str, p: Persona, m: Mode| refused(&run(sql, p, m, None));
    let msg = "Storage engine MyISAM is disabled (Table creation is disallowed).";
    assert_eq!(
        r(
            "CREATE TABLE t (a int) ENGINE=MyISAM",
            Persona::MySql84,
            Mode::Reject
        ),
        (3161, msg.to_owned())
    );
    assert_eq!(
        r(
            "ALTER TABLE t ENGINE=MyISAM",
            Persona::MySql84,
            Mode::Reject
        ),
        (3161, msg.to_owned())
    );
    assert_eq!(
        r(
            "CREATE TABLE t (a int) ENGINE=MyISAM",
            Persona::MariaDb,
            Mode::Reject
        )
        .0,
        1290
    );
    // Refused engines fail in either mode.
    for m in [Mode::Innodb, Mode::Reject] {
        assert_eq!(
            r(
                "CREATE TABLE t (a int) ENGINE=BLACKHOLE",
                Persona::MySql84,
                m
            ),
            (
                3161,
                "Storage engine BLACKHOLE is disabled (Table creation is disallowed).".to_owned()
            )
        );
        assert_eq!(
            r(
                "CREATE TABLE t (a int) ENGINE=MRG_MyISAM UNION=(a,b)",
                Persona::MariaDb,
                m
            ),
            (1286, "Unknown storage engine 'MRG_MYISAM'".to_owned())
        );
    }
    // Reject mode leaves temporary tables alone too.
    assert_eq!(
        run(
            "CREATE TEMPORARY TABLE t (a int) ENGINE=MyISAM",
            Persona::MySql84,
            Mode::Reject,
            None
        ),
        Outcome::Unchanged
    );
}

#[test]
fn the_sessions_default_engine_counts() {
    let o = run(
        "CREATE TABLE t (a int)",
        Persona::MySql84,
        Mode::Innodb,
        Some("MyISAM"),
    );
    let (sql, notes) = text(&o);
    assert_eq!(sql, "CREATE TABLE t (a int) ENGINE=InnoDB");
    assert_eq!(notes.len(), 2);
    let o = run(
        "CREATE TABLE t AS SELECT 1 AS a",
        Persona::MariaDb,
        Mode::Innodb,
        Some("Aria"),
    );
    assert_eq!(text(&o).0, "CREATE TABLE t ENGINE=InnoDB AS SELECT 1 AS a");
    // A named engine wins; InnoDB as the default changes nothing.
    assert_eq!(
        run(
            "CREATE TABLE t (a int) ENGINE=InnoDB",
            Persona::MySql84,
            Mode::Innodb,
            Some("MyISAM")
        ),
        Outcome::Unchanged
    );
    assert_eq!(
        run(
            "CREATE TABLE t (a int)",
            Persona::MySql84,
            Mode::Innodb,
            Some("InnoDB")
        ),
        Outcome::Unchanged
    );
    assert_eq!(
        refused(&run(
            "CREATE TABLE t (a int)",
            Persona::MySql84,
            Mode::Reject,
            Some("MEMORY")
        ))
        .0,
        3161
    );
    // ALTER TABLE doesn't read the default.
    assert_eq!(
        run(
            "ALTER TABLE t ADD b int",
            Persona::MySql84,
            Mode::Innodb,
            Some("MyISAM")
        ),
        Outcome::Unchanged
    );
}

#[test]
fn plans_name_the_table() {
    let p = plan(
        b"ALTER TABLE `d`.`t x` ENGINE=MyISAM",
        &LexOptions::mysql84(),
    )
    .unwrap();
    assert_eq!((p.db.as_deref(), p.table.as_str()), (Some("d"), "t x"));
    assert_eq!(p.kind, Kind::Alter);
    assert_eq!(p.clause_spans(), [(22, 35)]);
    let p = plan(
        b"CREATE TEMPORARY TABLE t (a int) ENGINE=MEMORY",
        &LexOptions::mysql84(),
    )
    .unwrap();
    assert!(p.temporary);
}

#[test]
fn quick_filter_and_info_line() {
    assert!(may_apply(b"  create table t (a int) engine=myisam", false));
    assert!(!may_apply(b"CREATE TABLE t (a int)", false));
    assert!(may_apply(b"CREATE TABLE t (a int)", true));
    assert!(!may_apply(b"SELECT 'ENGINE'", true));
    assert_eq!(
        info_with_warnings("Records: 2  Duplicates: 0  Warnings: 0", 1),
        "Records: 2  Duplicates: 0  Warnings: 1"
    );
    assert_eq!(info_with_warnings("other", 1), "other");
}
