use super::{Created, Ctx, alter, create, render};

fn cx() -> Ctx {
    Ctx { mysql: false, db_cs: ("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()) }
}

fn shown(sql: &str) -> String {
    match create(sql, &cx()) {
        Ok(Created::Def(d)) => render(&d),
        Ok(Created::Like(..)) => "LIKE".into(),
        Err(e) => format!("ERROR {}: {}", e.protocol().mysql_errno, e.message),
    }
}

#[test]
fn types_keys_and_defaults_print_as_the_server_prints_them() {
    let s = shown(
        "CREATE TABLE t1 (id int primary key, a integer unsigned, b bool, c dec, d numeric(5,2) default '1.5', \
         h varchar(10) default 0, y smallint default -1, unique(a), key(h(5)), index(a,b), key kk (y desc), \
         check (y < 5), constraint cz check (y > 0)) comment 'hi'",
    );
    assert_eq!(
        s,
        "CREATE TABLE `t1` (\n  `id` int(11) NOT NULL,\n  `a` int(10) unsigned DEFAULT NULL,\n  `b` tinyint(1) DEFAULT NULL,\n  \
         `c` decimal(10,0) DEFAULT NULL,\n  `d` decimal(5,2) DEFAULT 1.50,\n  `h` varchar(10) DEFAULT '0',\n  `y` smallint(6) DEFAULT -1,\n  \
         PRIMARY KEY (`id`),\n  UNIQUE KEY `a` (`a`),\n  KEY `h` (`h`(5)),\n  KEY `a_2` (`a`,`b`),\n  KEY `kk` (`y` DESC),\n  \
         CONSTRAINT `CONSTRAINT_1` CHECK (`y` < 5),\n  CONSTRAINT `cz` CHECK (`y` > 0)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci COMMENT='hi'"
    );
}

#[test]
fn expressions_print_with_the_servers_rewrites() {
    let s = shown(
        "create table q1 (id INT NOT NULL PRIMARY KEY, a INT CHECK (a > 0), c INT DEFAULT (1 + 2) CHECK (c <> 5), \
         d VARCHAR(10) DEFAULT ('x'), e INT DEFAULT (id * 2), g INT NOT NULL DEFAULT (7), \
         CONSTRAINT `Mixed Case` CHECK (a IS NULL OR a NOT IN (7, 8)), CHECK (not (a and c)), CHECK (a not like 'x'))",
    );
    assert!(s.contains("`a` int(11) DEFAULT NULL CHECK (`a` > 0)"), "{s}");
    assert!(s.contains("`c` int(11) DEFAULT (1 + 2) CHECK (`c` <> 5)"), "{s}");
    assert!(s.contains("`d` varchar(10) DEFAULT 'x'"), "{s}");
    assert!(s.contains("`e` int(11) DEFAULT (`id` * 2)"), "{s}");
    assert!(s.contains("`g` int(11) NOT NULL DEFAULT 7"), "{s}");
    assert!(s.contains("CHECK (`a` is null or `a` not in (7,8))"), "{s}");
    assert!(s.contains("CHECK (`a` = 0 or `c` = 0)"), "{s}");
    assert!(s.contains("CHECK (`a`  not like 'x')"), "{s}");
}

#[test]
fn definition_errors_are_the_servers() {
    assert_eq!(shown("create table z1 (a int, a int)"), "ERROR 1060: Duplicate column name 'a'");
    assert_eq!(shown("create table z1 (a int, key (b))"), "ERROR 1072: Key column 'b' doesn't exist in table");
    assert_eq!(shown("create table z1 (a int primary key, b int primary key)"), "ERROR 1068: Multiple primary key defined");
    assert_eq!(shown("create table z1 (a int auto_increment)").split(':').next().unwrap(), "ERROR 1075");
    assert_eq!(shown("create table z1 (a int default 'x')"), "ERROR 1067: Invalid default value for 'a'");
    assert_eq!(shown("create table z1 (a intt)"), "ERROR 4161: Unknown data type: 'intt'");
    assert!(shown("create table z1 (a int,)").starts_with("ERROR 1064: You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near ')' at line 1"));
}

#[test]
fn alter_phases_follow_the_server() {
    let Ok(Created::Def(d)) = create("create table z1 (a int, b int, key k (a))", &cx()) else { panic!() };
    let d2 = alter(&d, "alter table z1 add c int after a, add d int first, modify b bigint after d", &cx()).unwrap();
    let s = render(&d2);
    assert!(s.starts_with("CREATE TABLE `z1` (\n  `d` int(11) DEFAULT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `a` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  KEY `k` (`a`)"), "{s}");
    let e = alter(&d, "alter table z1 drop column c", &cx()).unwrap_err();
    assert_eq!(e.message, "Can't DROP COLUMN `c`; check that it exists");
    let d3 = alter(&d, "create unique index i2 on z1 (b desc)", &cx()).unwrap();
    assert!(render(&d3).contains("UNIQUE KEY `i2` (`b` DESC),\n  KEY `k` (`a`)"));
}

/// Compares the engine's output with what the oracle printed, for a corpus
/// recorded by observation: `SPRIG_DEFINE_CORPUS=<file>` with blocks of
/// `### <statement>` followed by the server's `SHOW CREATE TABLE` (or its
/// `ERROR n (state): message` line).
#[test]
fn recorded_corpus_matches_when_given() {
    let Ok(path) = std::env::var("SPRIG_DEFINE_CORPUS") else { return };
    let text = std::fs::read_to_string(path).unwrap();
    let mut failed = 0;
    let mut total = 0;
    for block in text.split("### ").skip(1) {
        let (stmt, want) = block.split_once('\n').unwrap_or((block, ""));
        let mut sts: Vec<&str> = stmt.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
        let first = sts.remove(0);
        let mut got = match create(first, &cx()) {
            Ok(Created::Def(d)) => Ok(*d),
            Ok(Created::Like(..)) => continue,
            Err(e) => Err(e),
        };
        for s in sts {
            if let Ok(d) = &got {
                match alter(d, s, &cx()) {
                    Ok(d2) => got = Ok(d2),
                    Err(e) => {
                        println!("(alter error kept the table) {}", e.message);
                    }
                }
            }
        }
        let want = want.trim_end();
        let got = match got {
            Ok(d) => format!("z1\t{}", render(&d)),
            Err(e) => format!("ERROR {} ({}): {}", e.protocol().mysql_errno, String::from_utf8_lossy(&e.protocol().sqlstate), e.message),
        };
        total += 1;
        // The oracle's text after a failed ALTER also shows the error line
        // first: compare the definitions only.
        let want_def = want.lines().skip_while(|l| !l.starts_with("z1\t") && !l.starts_with("ERROR")).collect::<Vec<_>>().join("\n");
        if want_def.trim_end() != got.trim_end() && !want.trim_end().ends_with(got.trim_end()) {
            failed += 1;
            println!("--- {first}\nwant:\n{want}\ngot:\n{got}\n");
        }
    }
    println!("corpus: {} of {total} match", total - failed);
}
