//! A foreign key checked against its parent table when a table is created
//! or altered, as both servers check it (observed through their clients):
//!
//! - **The parent table** must exist, unless `foreign_key_checks = 0`.
//! - **Each referenced column** must exist in the parent.
//! - **Each pair of columns** must have compatible types. Integers
//!   (`ENUM`, `SET`, `YEAR` and `DATE` count as unsigned integers of their
//!   stored width) match only the same width and sign; `FLOAT` matches
//!   `FLOAT` and `DOUBLE` `DOUBLE`; `CHAR` and `VARCHAR` of any length
//!   match each other in the same character set and collation; `DECIMAL`,
//!   `DATETIME`, `TIMESTAMP`, `TIME`, `BIT`, `BINARY` and `VARBINARY` of
//!   any size match one another; `TEXT`, `BLOB` and `JSON` never match.
//! - **The parent needs a key on the referenced columns.** MariaDB takes
//!   any key whose first columns are the referenced ones, in order; MySQL
//!   8.4 wants a primary or unique key of exactly those columns.
//! - **`SET NULL`** needs child columns that may hold `NULL`.
//!
//! MariaDB answers every failure with 1005 errno 150 naming the child
//! table; MySQL names the failure (1824, 3734, 3780, 6125, 1830), checking
//! `SET NULL` first, then the parent, then each column pair in order (a
//! missing column, then its type), then the key.

use sprig_common::SprigError;

use super::types::Ty;
use super::{Def, Fk, KeyKind};

/// What a check is made for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct For<'a> {
    /// The MySQL port.
    pub mysql: bool,
    /// `foreign_key_checks` is on.
    pub checks: bool,
    /// The child's database, as MariaDB's 1005 names it.
    pub db: &'a str,
}

/// How InnoDB stores a column's values, for comparing the two sides of a
/// foreign key.
#[derive(Debug, PartialEq, Eq)]
enum Class<'a> {
    Int {
        bytes: u8,
        unsigned: bool,
    },
    Float,
    Double,
    /// Fixed-size and byte strings: compared as bytes, any size.
    Bytes,
    /// Character strings: character set and collation.
    Chars(&'a str, &'a str),
    /// Any other type: only the same printed type.
    Other(String),
    /// Large objects: never.
    Never,
}

fn class(t: &Ty) -> Class<'_> {
    let int = |bytes: u8, unsigned: bool| Class::Int { bytes, unsigned };
    match t.base.as_str() {
        "tinyint" => int(1, t.unsigned),
        "smallint" => int(2, t.unsigned),
        "mediumint" => int(3, t.unsigned),
        "int" | "integer" => int(4, t.unsigned),
        "bigint" => int(8, t.unsigned),
        "year" => int(1, true),
        "date" => int(3, true),
        "enum" => int(if t.members.len() > 255 { 2 } else { 1 }, true),
        "set" => int(
            match t.members.len() {
                0..=8 => 1,
                9..=16 => 2,
                17..=24 => 3,
                25..=32 => 4,
                _ => 8,
            },
            true,
        ),
        "float" => Class::Float,
        "double" | "real" => Class::Double,
        "decimal" | "numeric" | "datetime" | "timestamp" | "time" | "bit" | "binary"
        | "varbinary" => Class::Bytes,
        "char" | "varchar" => match &t.cs {
            Some((cs, co)) => Class::Chars(cs, co),
            None => Class::Other(t.print()),
        },
        _ if t.is_lob() || t.json => Class::Never,
        _ => Class::Other(t.print()),
    }
}

/// Whether columns of types `a` and `b` can be the two sides of one
/// foreign key.
pub(crate) fn compatible(a: &Ty, b: &Ty) -> bool {
    let ca = class(a);
    ca != Class::Never && ca == class(b)
}

/// Whether changing a key column's type from `a` to `b` is one the
/// servers make in place: a `VARCHAR` made longer in the same character
/// set and collation (on MySQL, keeping its length in the same number of
/// bytes). MariaDB also changes only a collation in place.
pub(crate) fn in_place(a: &Ty, b: &Ty, mysql: bool) -> bool {
    let longer = a.base == "varchar"
        && b.base == "varchar"
        && a.cs == b.cs
        && b.len >= a.len
        && (!mysql || (a.len * a.mbmax() < 256) == (b.len * b.mbmax() < 256));
    let collation_only = !mysql
        && a.base == b.base
        && a.args == b.args
        && a.unsigned == b.unsigned
        && a.cs.as_ref().map(|c| &c.0) == b.cs.as_ref().map(|c| &c.0)
        && a.cs != b.cs;
    longer || collation_only
}

/// MariaDB's answer to any foreign key it can't add to table `table`.
fn formed(db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1005,
        *b"HY000",
        format!(
            "Can't create table `{db}`.`{table}` (errno: 150 \"Foreign key constraint is incorrectly formed\")"
        ),
    )
}

fn mysql_err(code: u16, msg: String) -> SprigError {
    SprigError::standard(code, *b"HY000", msg)
}

/// The `SET NULL` check of every foreign key of `child` (MySQL makes it
/// before any parent is looked at).
///
/// # Errors
/// MariaDB's 1005 / MySQL's 1830.
pub(crate) fn set_null(cx: For<'_>, child: &Def, fks: &[&Fk]) -> Result<(), SprigError> {
    for f in fks {
        let sets_null = [&f.on_delete, &f.on_update]
            .iter()
            .any(|a| a.as_deref() == Some("SET NULL"));
        if !sets_null {
            continue;
        }
        for c in &f.cols {
            if child.col(c).is_some_and(|i| child.cols[i].not_null) {
                return Err(if cx.mysql {
                    mysql_err(
                        1830,
                        format!(
                            "Column '{c}' cannot be NOT NULL: needed in a foreign key constraint '{}' SET NULL",
                            f.name
                        ),
                    )
                } else {
                    formed(cx.db, &child.name)
                });
            }
        }
    }
    Ok(())
}

/// Foreign key `f` of `child` against its parent (`None`: the parent
/// table doesn't exist). A self-reference passes `child` as the parent.
///
/// # Errors
/// MariaDB's 1005 errno 150; MySQL's 1824, 3734, 3780 or 6125.
pub(crate) fn check(
    cx: For<'_>,
    child: &Def,
    f: &Fk,
    parent: Option<&Def>,
) -> Result<(), SprigError> {
    let fail = |code: u16, msg: String| {
        if cx.mysql {
            mysql_err(code, msg)
        } else {
            formed(cx.db, &child.name)
        }
    };
    let Some(p) = parent else {
        if !cx.checks {
            return Ok(());
        }
        return Err(fail(
            1824,
            format!("Failed to open the referenced table '{}'", f.ref_table),
        ));
    };
    let mut pcols = Vec::with_capacity(f.ref_cols.len());
    for (c, r) in f.cols.iter().zip(&f.ref_cols) {
        let Some(pi) = p.col(r) else {
            return Err(fail(
                3734,
                format!(
                    "Failed to add the foreign key constraint. Missing column '{r}' for constraint '{}' in the referenced table '{}'",
                    f.name, f.ref_table
                ),
            ));
        };
        pcols.push(p.cols[pi].name.as_str());
        let Some(ci) = child.col(c) else {
            continue;
        };
        if !compatible(&child.cols[ci].ty, &p.cols[pi].ty) {
            return Err(fail(
                3780,
                format!(
                    "Referencing column '{c}' and referenced column '{r}' in foreign key constraint '{}' are incompatible.",
                    f.name
                ),
            ));
        }
    }
    let usable = p.keys.iter().any(|k| {
        if matches!(k.kind, KeyKind::Fulltext | KeyKind::Spatial) {
            return false;
        }
        let exact = k.parts.len() == pcols.len();
        let unique = matches!(k.kind, KeyKind::Primary | KeyKind::Unique);
        (!cx.mysql || (exact && unique))
            && k.parts.len() >= pcols.len()
            && k.parts
                .iter()
                .zip(&pcols)
                .all(|(part, c)| part.len.is_none() && part.col.eq_ignore_ascii_case(c))
    });
    if !usable {
        return Err(fail(
            6125,
            format!(
                "Failed to add the foreign key constraint. Missing unique key for constraint '{}' in the referenced table '{}'",
                f.name, f.ref_table
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::define::{self, Ctx};

    fn def(sql: &str) -> Def {
        let cx = Ctx {
            mysql: false,
            db_cs: ("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()),
        };
        match define::create(sql, &cx).unwrap() {
            define::Created::Def(d) => *d,
            define::Created::Like(..) => panic!("LIKE"),
        }
    }

    fn code(r: Result<(), SprigError>) -> u16 {
        r.err().map_or(0, |e| e.protocol().mysql_errno)
    }

    #[test]
    fn parents_are_checked_as_each_server_checks_them() {
        let p = def(
            "CREATE TABLE p (id INT PRIMARY KEY, u INT UNIQUE, w INT, ti TINYINT UNIQUE, \
             d DECIMAL(10,2) UNIQUE, dt DATETIME UNIQUE, cv CHAR(10) UNIQUE, vb VARBINARY(4) UNIQUE, \
             a INT, b INT, KEY(w), KEY ab (a, b))",
        );
        let cases: &[(&str, u16, u16)] = &[
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (id)", 0, 0),
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (u)", 0, 0),
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (w)", 0, 6125),
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (a)", 0, 6125),
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (b)", 1005, 6125),
            ("pid INT, FOREIGN KEY (pid) REFERENCES p (nope)", 1005, 3734),
            (
                "pid BIGINT, FOREIGN KEY (pid) REFERENCES p (id)",
                1005,
                3780,
            ),
            (
                "pid INT UNSIGNED, FOREIGN KEY (pid) REFERENCES p (id)",
                1005,
                3780,
            ),
            (
                "pid SMALLINT, FOREIGN KEY (pid) REFERENCES p (ti)",
                1005,
                3780,
            ),
            (
                "pid DECIMAL(12,4), FOREIGN KEY (pid) REFERENCES p (d)",
                0,
                0,
            ),
            (
                "pid TIMESTAMP NULL, FOREIGN KEY (pid) REFERENCES p (dt)",
                0,
                0,
            ),
            ("pid VARCHAR(3), FOREIGN KEY (pid) REFERENCES p (cv)", 0, 0),
            (
                "pid VARCHAR(3) COLLATE utf8mb4_bin, FOREIGN KEY (pid) REFERENCES p (cv)",
                1005,
                3780,
            ),
            ("pid BINARY(4), FOREIGN KEY (pid) REFERENCES p (vb)", 0, 0),
            ("pid DATE, FOREIGN KEY (pid) REFERENCES p (dt)", 1005, 3780),
            (
                "pid ENUM('a'), FOREIGN KEY (pid) REFERENCES p (ti)",
                1005,
                3780,
            ),
        ];
        for &(cols, maria, mysql) in cases {
            let c = def(&format!("CREATE TABLE c (id INT PRIMARY KEY, {cols})"));
            for (is_mysql, want) in [(false, maria), (true, mysql)] {
                let cx = For {
                    mysql: is_mysql,
                    checks: true,
                    db: "d",
                };
                assert_eq!(
                    code(check(cx, &c, &c.fks[0], Some(&p))),
                    want,
                    "{cols} mysql={is_mysql}"
                );
            }
        }
        let c = def(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES missing (id))",
        );
        let on = For {
            mysql: true,
            checks: true,
            db: "d",
        };
        assert_eq!(code(check(on, &c, &c.fks[0], None)), 1824);
        assert_eq!(
            code(check(
                For {
                    checks: false,
                    ..on
                },
                &c,
                &c.fks[0],
                None
            )),
            0
        );
        let e = check(For { mysql: false, ..on }, &c, &c.fks[0], None).unwrap_err();
        assert_eq!(
            e.message,
            "Can't create table `d`.`c` (errno: 150 \"Foreign key constraint is incorrectly formed\")"
        );
        let n = def(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT NOT NULL, FOREIGN KEY (pid) REFERENCES p (id) ON DELETE SET NULL)",
        );
        let fks: Vec<&Fk> = n.fks.iter().collect();
        assert_eq!(code(set_null(on, &n, &fks)), 1830);
        assert_eq!(code(set_null(For { mysql: false, ..on }, &n, &fks)), 1005);
    }
}
