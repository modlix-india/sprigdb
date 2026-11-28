//! The planner against the servers. Generated tables (see
//! `common/world.rs`) are loaded into MariaDB 11.8 and MySQL 8.4 and into
//! memory; generated queries, feature by feature, are run on the servers
//! and through the planner, and the whole results are compared: in order
//! when the query fixes a total order, as multisets otherwise; an error
//! by its number.
//!
//! The servers are shell commands named by `SPRIG_PLAN_ORACLE_MARIADB`
//! and `SPRIG_PLAN_ORACLE_MYSQL` (they read SQL on stdin and print rows
//! tab-separated without a header); without them the test does nothing.
//! `SPRIG_PLAN_ORACLE_SEED` varies tables and queries,
//! `SPRIG_PLAN_ORACLE_CASES` sets the queries per generated feature,
//! `SPRIG_PLAN_ORACLE_ONLY` runs one feature, and
//! `SPRIG_PLAN_ORACLE_REPORT` names a directory for counts and
//! mismatches. The test works in its own database, `rp_diff_<seed>`, and
//! drops it at the end.

#![allow(
    clippy::explicit_auto_deref,
    clippy::manual_is_multiple_of,
    clippy::explicit_iter_loop,
    clippy::collapsible_if,
    clippy::default_trait_access,
    clippy::type_complexity,
    clippy::items_after_statements,
    clippy::redundant_closure_for_method_calls,
    clippy::match_same_arms,
    clippy::if_not_else,
    clippy::single_match_else,
    clippy::unnested_or_patterns,
    clippy::uninlined_format_args,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::format_push_string,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value
)]

mod common;

use std::fmt::Write as _;

use common::world::world;
use common::{Oracle, Outcome, Rng, load_mem, run_ours, same, session, show};
use sprig_plan::PlanOptions;

/// One generated query.
struct Q {
    sql: String,
    /// Whether the query fixes the order of its rows.
    ordered: bool,
}

fn q(sql: impl Into<String>) -> Q {
    Q {
        sql: sql.into(),
        ordered: false,
    }
}

fn qo(sql: impl Into<String>) -> Q {
    Q {
        sql: sql.into(),
        ordered: true,
    }
}

/// The kind of constants a column is compared with.
#[derive(Clone, Copy, PartialEq)]
enum K {
    /// Small integers around the data's range.
    Int,
    Tiny,
    Unsigned,
    Big,
    Str,
    Code,
    Bin,
    Dec,
    Dbl,
    Date,
    DateTime,
}

fn konst(rng: &mut Rng, k: K) -> String {
    let pick = |rng: &mut Rng, items: &[&str]| (*rng.pick(items)).to_owned();
    // Now and then a constant of another family, or NULL.
    // (Not for dates: what a number or a word means to a date column is
    // the expression engine's subject, and the servers disagree on it.)
    if rng.chance(1, 14) {
        if matches!(k, K::Date | K::DateTime) {
            return "NULL".to_owned();
        }
        return pick(
            rng,
            &[
                "NULL", "'5'", "5.5", "1e0", "'abc'", "0", "'2.5'", "'7 '", "'-1'", "'3e0'", "'4x'",
            ],
        );
    }
    match k {
        K::Int => {
            if rng.chance(1, 6) {
                pick(
                    rng,
                    &[
                        "2.0",
                        "2.5",
                        "-1",
                        "99999999999",
                        "-99999999999",
                        "3e0",
                        "4.999",
                    ],
                )
            } else {
                (rng.below(16) as i64 - 2).to_string()
            }
        }
        K::Tiny => pick(
            rng,
            &[
                "-128", "-129", "127", "128", "0", "1", "2", "3", "5", "100", "1.5", "-0.5", "1000",
            ],
        ),
        K::Unsigned => pick(
            rng,
            &[
                "0",
                "-1",
                "1",
                "2",
                "7",
                "100",
                "2147483648",
                "4294967295",
                "4294967296",
                "0.5",
            ],
        ),
        K::Big => pick(
            rng,
            &[
                "-9223372036854775808",
                "9223372036854775807",
                "9223372036854775808",
                "-9007199254740993",
                "9007199254740992",
                "9007199254740993",
                "0",
                "1",
                "7",
                "-1",
                "9007199254740993e0",
                "1.5",
            ],
        ),
        K::Str => {
            let s = rng.pick(common::world::STRINGS);
            if rng.chance(1, 8) {
                pick(rng, &["'zz'", "'a  '", "'AB'", "1", "''"])
            } else {
                format!("'{s}'")
            }
        }
        K::Code => pick(
            rng,
            &[
                "'c00'", "'C01'", "'c03'", "'d02'", "'c15'", "'C16'", "'zz'", "'c'", "'c074'",
                "'c111'",
            ],
        ),
        K::Bin => pick(rng, &["'a'", "'A'", "'ab'", "'x '", "''", "0x61", "'z'"]),
        // (No negative constant that rounds to a stored value: MySQL 8.4
        // reads `col <= -0.001` on a DECIMAL(10,2) column as
        // `col <= 0.00`, a matter of its own constant handling.)
        K::Dec => pick(
            rng,
            &[
                "0",
                "1.25",
                "1.255",
                "-5",
                "7",
                "2.50",
                "12.125",
                "-0.126",
                "2.5e0",
                "100000000",
                "19.75",
                "-2.375",
            ],
        ),
        K::Dbl => pick(
            rng,
            &[
                "0", "0.5", "-0.0", "1e15", "2", "2.5", "-2.25", "1024", "0.25", "-1e10",
            ],
        ),
        K::Date => pick(
            rng,
            &[
                "'2023-12-25'",
                "'2023-12-31'",
                "'2024-01-01'",
                "'2024-01-15'",
                "'2024-02-10'",
                "'2024-02-29'",
                "'2024-03-01'",
                "'2024-1-5'",
                "'2024-01-15 00:00:00'",
                "'2024-01-15 10:00:00'",
            ],
        ),
        K::DateTime => pick(
            rng,
            &[
                "'2023-12-25 00:00:00'",
                "'2024-01-01 11:30:00'",
                "'2024-01-15'",
                "'2024-01-15 22:00:00'",
                "'2024-02-01 11:00:00'",
                "'2024-02-10 00:30:00.5'",
                "'2024-01-20 11:00:00'",
            ],
        ),
    }
}

fn atom(rng: &mut Rng, col: &str, k: K) -> String {
    let c = |rng: &mut Rng| konst(rng, k);
    match rng.below(20) {
        0..=4 => format!("{col} = {}", c(rng)),
        5 => format!("{col} < {}", c(rng)),
        6 => format!("{col} <= {}", c(rng)),
        7 => format!("{col} > {}", c(rng)),
        8 => format!("{col} >= {}", c(rng)),
        9 => format!("{col} <> {}", c(rng)),
        10 => format!("{col} <=> {}", c(rng)),
        11 | 12 => format!("{col} BETWEEN {} AND {}", c(rng), c(rng)),
        13 => format!("{col} NOT BETWEEN {} AND {}", c(rng), c(rng)),
        14 | 15 => {
            let n = 2 + rng.below(4);
            let items: Vec<String> = (0..n).map(|_| c(rng)).collect();
            format!("{col} IN ({})", items.join(", "))
        }
        16 => {
            let items: Vec<String> = (0..3).map(|_| c(rng)).collect();
            format!("{col} NOT IN ({})", items.join(", "))
        }
        17 => format!("{col} IS NULL"),
        18 => format!("{col} IS NOT NULL"),
        _ => {
            if matches!(k, K::Str | K::Code | K::Bin) {
                format!(
                    "{col} LIKE {}",
                    rng.pick(&["'a%'", "'A%'", "'%b'", "'c0%'", "'x%'", "'_b%'", "'ab'"])
                )
            } else {
                format!("{} = {col}", c(rng))
            }
        }
    }
}

/// A table as the generators see it: its columns with their constant
/// kinds, and the columns that make an order total.
struct T {
    name: &'static str,
    cols: &'static [(&'static str, K)],
    tie: &'static [&'static str],
}

const TABLES: &[T] = &[
    T {
        name: "t1",
        cols: &[
            ("id", K::Int),
            ("k", K::Int),
            ("v", K::Str),
            ("d", K::Dec),
            ("dt", K::Date),
            ("f", K::Dbl),
        ],
        tie: &["id"],
    },
    T {
        name: "t2",
        cols: &[("a", K::Int), ("b", K::Int), ("c", K::Int), ("s", K::Str)],
        tie: &["a", "b"],
    },
    T {
        name: "t3",
        cols: &[("x", K::Int), ("y", K::Str), ("z", K::Int)],
        tie: &["x", "y", "z"],
    },
    T {
        name: "t4",
        cols: &[("x", K::Int), ("y", K::Int), ("z", K::Str)],
        tie: &["x", "y", "z"],
    },
    T {
        name: "t5",
        cols: &[
            ("id", K::Int),
            ("u", K::Int),
            ("un", K::Int),
            ("code", K::Code),
        ],
        tie: &["id"],
    },
    T {
        name: "t6",
        cols: &[
            ("id", K::Int),
            ("sb", K::Str),
            ("sg", K::Str),
            ("sd", K::Str),
            ("vb", K::Bin),
            ("sp", K::Str),
        ],
        tie: &["id"],
    },
    T {
        name: "t7",
        cols: &[("id", K::Int), ("a", K::Int), ("b", K::Int), ("c", K::Int)],
        tie: &["id"],
    },
    T {
        name: "t8",
        cols: &[
            ("ti", K::Tiny),
            ("ui", K::Unsigned),
            ("bi", K::Big),
            ("de", K::Dec),
            ("dd", K::Dbl),
            ("dy", K::Date),
            ("dtm", K::DateTime),
        ],
        tie: &["id"],
    },
    T {
        name: "big",
        cols: &[("id", K::Int), ("k1", K::Int), ("k2", K::Int)],
        tie: &["id"],
    },
    T {
        name: "spk",
        cols: &[("code", K::Code), ("n", K::Int)],
        tie: &["code"],
    },
];

fn predicate(rng: &mut Rng, t: &T) -> String {
    let a = |rng: &mut Rng| {
        let (c, k) = *rng.pick(t.cols);
        atom(rng, c, k)
    };
    match rng.below(10) {
        0..=3 => a(rng),
        4 | 5 => format!("{} AND {}", a(rng), a(rng)),
        6 => format!("{} OR {}", a(rng), a(rng)),
        7 => format!("({} OR {}) AND {}", a(rng), a(rng), a(rng)),
        8 => format!("({} AND {}) OR ({} AND {})", a(rng), a(rng), a(rng), a(rng)),
        _ => format!("{} AND {} AND {}", a(rng), a(rng), a(rng)),
    }
}

fn select_list(rng: &mut Rng, t: &T) -> String {
    match rng.below(4) {
        0 | 1 => "*".to_owned(),
        2 => t.cols[rng.below(t.cols.len())].0.to_owned(),
        _ => {
            let a = t.cols[rng.below(t.cols.len())].0;
            let b = t.tie[0];
            format!("{a}, {b}")
        }
    }
}

fn names(db: &str) -> Vec<Q> {
    let mut out: Vec<Q> = [
        "SELECT zz FROM t1",
        "SELECT t1.zz FROM t1",
        "SELECT t9.k FROM t1",
        "SELECT id FROM t1, t5",
        "SELECT k FROM t1 WHERE zz = 1",
        "SELECT * FROM t1 ORDER BY zz",
        "SELECT k FROM t1 GROUP BY zz",
        "SELECT k FROM t1 GROUP BY k HAVING zz > 1",
        "SELECT * FROM t1 JOIN t5 ON zz = 1",
        "SELECT * FROM t1 JOIN t5 ON id = 1",
        "SELECT * FROM nope",
        "SELECT * FROM t1, t1",
        "SELECT * FROM t1 JOIN t5 t1 ON 1",
        "SELECT k FROM t1 UNION SELECT u, id FROM t5",
        "SELECT k FROM t1 ORDER BY 5",
        "SELECT k FROM t1 GROUP BY 3",
        "SELECT k AS x FROM t1 WHERE x = 1",
        "SELECT -k AS k, id FROM t1 GROUP BY k, id",
        "SELECT COUNT(*) c, k FROM t1 GROUP BY k HAVING c > 3",
        "SELECT * FROM par JOIN chi USING (id)",
        "SELECT * FROM par NATURAL JOIN chi",
        "SELECT * FROM par LEFT JOIN chi USING (id)",
        "SELECT * FROM par RIGHT JOIN chi USING (id)",
        "SELECT par.*, chi.tag FROM par RIGHT JOIN chi ON par.id = chi.par_id",
        "SELECT id FROM par JOIN chi USING (id)",
        "SELECT par_id, name FROM par JOIN chi USING (id)",
        "SELECT grp FROM par JOIN chi USING (zz)",
        "SELECT t1.* FROM t1 WHERE id < 5",
        "SELECT x.id FROM t1 AS x WHERE x.id = 2",
        "SELECT t1.id FROM t1 AS x",
        "SELECT x.* FROM t1 AS x WHERE id = 3",
        "SELECT zz.* FROM t1",
        "SELECT 1 + 1, 'a', NULL",
        "SELECT id FROM t1 WHERE id = (SELECT id FROM t5)",
        "SELECT * FROM (SELECT 1 AS a, 2 AS a) d",
        "SELECT t1.id FROM t1 JOIN t5 ON t1.id = t5.id JOIN par x ON t3.x = 1",
        "SELECT * FROM t1, t5 JOIN par x ON t1.id = x.id",
        "SELECT d.a + 1 FROM (SELECT id AS a FROM t1 WHERE id < 4) d",
        "SELECT a FROM (SELECT id AS a, k FROM t1) d WHERE k = 3",
        "SELECT d.k FROM (SELECT id AS a, k FROM t1) d WHERE d.a = 3",
        "SELECT k FROM (SELECT id AS a, k FROM t1) d WHERE id = 3",
        "SELECT SUM(k) FROM t1 WHERE SUM(k) > 1",
        "SELECT COUNT(*) FROM t1 GROUP BY COUNT(*)",
        "SELECT DISTINCT k FROM t1",
        "SELECT k, COUNT(*) FROM t1 GROUP BY 1",
        "SELECT k + 1 AS x, COUNT(*) FROM t1 GROUP BY x",
        "SELECT k AS id, COUNT(*) FROM t1 GROUP BY id HAVING COUNT(*) < 100",
        "SELECT a, b FROM t2 WHERE t2.a = 1 AND b > 0",
        "SELECT t2.a, x.a FROM t2 JOIN t2 x ON t2.a = x.a AND t2.b = x.b WHERE t2.b = 0",
        "SELECT a FROM t2 JOIN t7 ON t2.a = t7.a",
        "SELECT t2.a FROM t2 JOIN t7 USING (a, b) WHERE c = 1",
        "SELECT t2.a, t7.id FROM t2 JOIN t7 USING (a, b) WHERE t2.c = 1",
        "SELECT * FROM t3 WHERE x IN (SELECT x FROM t3, t4)",
        "SELECT (SELECT 1, 2)",
        "SELECT * FROM t1 WHERE (id, k) = (1, 2)",
        "SELECT id FROM t1 WHERE id IN (SELECT id, u FROM t5)",
        "SELECT COUNT(*) FROM t1 AS a, t1 AS b WHERE a.id = b.id AND a.id < 3",
    ]
    .into_iter()
    .map(q)
    .collect();
    out.push(q(format!(
        "SELECT {db}.t1.id, t1.k FROM t1 WHERE {db}.t1.id = 3"
    )));
    out.push(q(format!("SELECT x.id FROM {db}.t1 AS x WHERE x.id = 4")));
    out.push(q(format!(
        "SELECT {db}.x.id FROM {db}.t1 AS x WHERE x.id = 4"
    )));
    for s in [
        "SELECT id, k AS x FROM t1 ORDER BY x DESC, id",
        "SELECT id, -k AS k FROM t1 ORDER BY k, id",
        "SELECT id, k FROM t1 ORDER BY 2, 1",
        "SELECT id, k FROM t1 ORDER BY 2 DESC, 1 DESC",
        "SELECT id AS a, k AS b FROM t1 WHERE k < 3 ORDER BY b, a",
        "SELECT id, k + 1 AS x FROM t1 ORDER BY x + 1, id",
        "SELECT id FROM par JOIN chi USING (id) ORDER BY id",
        "SELECT id, par.id, chi.id FROM par LEFT JOIN chi USING (id) ORDER BY id",
        "SELECT * FROM t1 WHERE id IN (1, 2, 3) ORDER BY id LIMIT 1 OFFSET 1",
        "SELECT * FROM t1 ORDER BY id LIMIT 2, 3",
    ] {
        out.push(qo(s));
    }
    out
}

fn point(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    let odd = [
        "0",
        "-5",
        "99999999999",
        "5.0",
        "5.5",
        "'7'",
        "NULL",
        "1e0",
        "'x'",
        "07",
        "+3",
        "TRUE",
    ];
    for i in 0..n {
        let id = |rng: &mut Rng| {
            if rng.chance(1, 4) {
                (*rng.pick(&odd)).to_owned()
            } else {
                (1 + rng.below(95)).to_string()
            }
        };
        out.push(q(match i % 12 {
            0 => format!("SELECT * FROM t1 WHERE id = {}", id(rng)),
            1 => format!("SELECT k, v FROM t1 WHERE {} = id", id(rng)),
            2 => format!(
                "SELECT * FROM t2 WHERE a = {} AND b = {}",
                rng.below(9),
                rng.below(14) as i64 * 3 - 6
            ),
            3 => format!(
                "SELECT * FROM t2 WHERE b = {} AND a = {}",
                id(rng),
                rng.below(9)
            ),
            4 => format!(
                "SELECT * FROM t5 WHERE u = {}",
                1000 - (rng.below(70) as i64) * 7
            ),
            5 => format!("SELECT * FROM t5 WHERE un = {}", id(rng)),
            6 => format!(
                "SELECT * FROM t5 WHERE code = {}",
                rng.pick(&[
                    "'c037'", "'C037'", "'c074'", "'c111'", "'c148 '", "'zz'", "37", "NULL"
                ])
            ),
            7 => format!(
                "SELECT * FROM spk WHERE code = {}",
                rng.pick(&[
                    "'c00'", "'C01'", "'c01'", "'d02'", "'D02'", "'c03 '", "''", "'c39'"
                ])
            ),
            8 => format!(
                "SELECT * FROM t1 WHERE id = {} AND k = {}",
                id(rng),
                rng.below(12)
            ),
            9 => format!(
                "SELECT id FROM t5 WHERE un IS NULL AND id < {}",
                10 + rng.below(50)
            ),
            10 => format!(
                "SELECT id, un FROM t5 WHERE un <=> {}",
                rng.pick(&["NULL", "4", "10", "12"])
            ),
            _ => format!(
                "SELECT * FROM t8 WHERE id = {}",
                (1 + rng.below(72) as i64) * 1_000_000_007
            ),
        }));
    }
    out
}

fn range(rng: &mut Rng, n: usize) -> Vec<Q> {
    (0..n)
        .map(|_| {
            let t = rng.pick(TABLES);
            let list = select_list(rng, t);
            q(format!(
                "SELECT {list} FROM {} WHERE {}",
                t.name,
                predicate(rng, t)
            ))
        })
        .collect()
}

fn order(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let t = rng.pick(TABLES);
        let mut keys: Vec<String> = Vec::new();
        let dir = |rng: &mut Rng| if rng.chance(1, 3) { " DESC" } else { "" };
        let first_dir = dir(rng);
        let ncols = 1 + rng.below(2);
        for j in 0..ncols {
            let c = t.cols[rng.below(t.cols.len())].0;
            if keys.iter().any(|k| k.split(' ').next() == Some(c)) {
                continue;
            }
            // Mostly one direction for all, sometimes mixed.
            let d = if j == 0 || rng.chance(3, 4) {
                first_dir
            } else {
                dir(rng)
            };
            keys.push(format!("{c}{d}"));
        }
        for c in t.tie {
            if keys.iter().any(|k| k.split(' ').next() == Some(*c)) {
                continue;
            }
            let d = if rng.chance(4, 5) {
                first_dir
            } else {
                dir(rng)
            };
            keys.push(format!("{c}{d}"));
        }
        // Tables with duplicate rows have no total order: compare those
        // as sets, and without LIMIT.
        let total = !matches!(t.name, "t3" | "t4");
        let filter = if rng.chance(1, 2) {
            format!(" WHERE {}", predicate(rng, t))
        } else {
            String::new()
        };
        let limit = if total && rng.chance(2, 3) {
            if rng.chance(1, 3) {
                format!(" LIMIT {} OFFSET {}", 1 + rng.below(8), rng.below(6))
            } else {
                format!(" LIMIT {}", 1 + rng.below(12))
            }
        } else {
            String::new()
        };
        let list = if i % 3 == 0 {
            "*".to_owned()
        } else {
            select_list(rng, t)
        };
        let sql = format!(
            "SELECT {list} FROM {}{filter} ORDER BY {}{limit}",
            t.name,
            keys.join(", ")
        );
        out.push(if total { qo(sql) } else { q(sql) });
    }
    // The shapes the index order must serve.
    for s in [
        "SELECT * FROM big WHERE k1 = 3 ORDER BY k2, id LIMIT 5",
        "SELECT * FROM big WHERE k1 = 3 ORDER BY k2 DESC, id DESC LIMIT 5",
        "SELECT id, k1, k2 FROM big WHERE k1 IN (3, 4) ORDER BY k2, id LIMIT 7",
        "SELECT id FROM big ORDER BY id DESC LIMIT 1",
        "SELECT id, k1 FROM big WHERE k1 >= 18 ORDER BY k1 DESC, k2 DESC, id DESC LIMIT 4",
        "SELECT * FROM t7 WHERE a = 2 ORDER BY b DESC, id LIMIT 6",
        "SELECT * FROM t7 WHERE a = 2 ORDER BY b, id DESC LIMIT 6",
        "SELECT * FROM t7 WHERE a = 2 ORDER BY b, id LIMIT 6",
        "SELECT id, c FROM t7 ORDER BY c DESC, id LIMIT 9",
        "SELECT id, c FROM t7 ORDER BY c, id DESC LIMIT 9",
        "SELECT id, c FROM t7 WHERE c IS NOT NULL ORDER BY c, id LIMIT 9",
        "SELECT * FROM t2 WHERE a = 3 ORDER BY b DESC LIMIT 3",
        "SELECT * FROM t2 ORDER BY a DESC, b DESC LIMIT 5",
        "SELECT * FROM t2 ORDER BY a, b DESC LIMIT 5",
        "SELECT id, sg FROM t6 WHERE sg >= 'b' ORDER BY sg, id LIMIT 8",
        "SELECT id, sb FROM t6 ORDER BY sb DESC, id DESC LIMIT 8",
        "SELECT id, sp FROM t6 WHERE sp = 'ab' ORDER BY sp, id",
        "SELECT * FROM t1 WHERE k = 4 ORDER BY id DESC LIMIT 3",
        "SELECT * FROM t1 WHERE k = 4 AND id > 30 ORDER BY id LIMIT 3",
        "SELECT * FROM spk ORDER BY code DESC LIMIT 4",
        "SELECT id, dt FROM t1 WHERE dt BETWEEN '2024-01-01' AND '2024-01-31' ORDER BY dt, id",
    ] {
        out.push(qo(s));
    }
    out
}

fn minmax(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let f = if rng.chance(1, 2) { "MIN" } else { "MAX" };
        out.push(q(match i % 14 {
            0 => format!("SELECT {f}(id) FROM t1"),
            1 => format!("SELECT {f}(k) FROM t1"),
            2 => format!("SELECT {f}(k) FROM t1 WHERE k > {}", rng.below(12)),
            3 => format!("SELECT {f}(k2) FROM big WHERE k1 = {}", rng.below(22)),
            4 => format!(
                "SELECT {f}(k2) FROM big WHERE k1 = {} AND k2 BETWEEN {} AND {}",
                rng.below(20),
                rng.below(30),
                20 + rng.below(40)
            ),
            5 => format!("SELECT {f}(b) FROM t7 WHERE a = {}", rng.below(7)),
            6 => format!("SELECT {f}(c) FROM t7"),
            7 => format!("SELECT {f}(c) FROM t7 WHERE c < {}", rng.below(30)),
            8 => format!("SELECT {f}(b) FROM t2 WHERE a = {}", rng.below(9)),
            9 => format!("SELECT {f}(sb) FROM t6"),
            10 => format!("SELECT {f}(dt), COUNT(*) FROM t1"),
            11 => format!("SELECT MIN(k), MAX(k) FROM t1 WHERE id < {}", rng.below(90)),
            12 => format!(
                "SELECT {f}(k2) + 1 FROM big WHERE k1 = {} AND k2 > 100",
                rng.below(20)
            ),
            _ => format!("SELECT {f}(un) FROM t5 WHERE un >= {}", rng.below(130)),
        }));
    }
    out
}

fn agg(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let w = |rng: &mut Rng, t: usize| {
            if rng.chance(1, 2) {
                format!(" WHERE {}", predicate(rng, &TABLES[t]))
            } else {
                String::new()
            }
        };
        out.push(q(match i % 16 {
            0 => format!("SELECT k, COUNT(*), SUM(d), COUNT(v), MAX(dt), SUM(f) FROM t1{} GROUP BY k", w(rng, 0)),
            1 => format!("SELECT COUNT(*), COUNT(k), COUNT(DISTINCT k), SUM(k), AVG(k) FROM t1{}", w(rng, 0)),
            2 => format!("SELECT DISTINCT k FROM t1{}", w(rng, 0)),
            3 => format!("SELECT COUNT(*), SUM(x.c) FROM (SELECT DISTINCT c, s FROM t2{}) x", w(rng, 1)),
            4 => format!("SELECT a, c, COUNT(*), MIN(b), MAX(b) FROM t2{} GROUP BY a, c", w(rng, 1)),
            5 => format!("SELECT k % 3, COUNT(*), AVG(d) FROM t1{} GROUP BY k % 3", w(rng, 0)),
            6 => format!("SELECT k1, COUNT(*), MIN(k2), MAX(k2), SUM(id) FROM big{} GROUP BY k1", w(rng, 8)),
            7 => format!("SELECT k1, k2, COUNT(*) FROM big WHERE k1 < {} GROUP BY k1, k2", rng.below(6)),
            8 => format!("SELECT k, COUNT(*) FROM t1 GROUP BY k HAVING COUNT(*) > {}", rng.below(10)),
            // Which spelling stands for a group of equal strings isn't
            // fixed: the group's key itself is left out.
            9 => format!("SELECT x, COUNT(*), SUM(z) FROM t3{} GROUP BY x, y", w(rng, 2)),
            10 => format!("SELECT COUNT(*), MIN(id), MAX(id), GROUP_CONCAT(DISTINCT 'x') FROM t6 WHERE id < {} GROUP BY sg", 5 + rng.below(20)),
            11 => format!("SELECT COUNT(*), COUNT(DISTINCT sb), MIN(id) FROM t6{} GROUP BY sd", w(rng, 5)),
            12 => format!("SELECT a, SUM(b), COUNT(c) FROM t7{} GROUP BY a HAVING SUM(b) > 5 OR a IS NULL", w(rng, 6)),
            13 => format!("SELECT COUNT(*) FROM big WHERE k1 = {} AND k2 < {}", rng.below(20), rng.below(50)),
            14 => format!("SELECT COUNT(DISTINCT k2) FROM big WHERE k1 BETWEEN {} AND {}", rng.below(10), 5 + rng.below(15)),
            _ => format!("SELECT x, MAX(y), MIN(z) FROM t4{} GROUP BY x", w(rng, 3)),
        }));
    }
    for s in [
        "SELECT k, COUNT(*) AS c FROM t1 GROUP BY k ORDER BY c DESC, k",
        "SELECT k1, SUM(k2) FROM big GROUP BY k1 ORDER BY k1 DESC LIMIT 5",
        "SELECT k1, SUM(k2) AS s FROM big GROUP BY k1 ORDER BY s, k1 LIMIT 5 OFFSET 2",
        "SELECT DISTINCT k FROM t1 ORDER BY k DESC",
        "SELECT DISTINCT a FROM t7 ORDER BY a LIMIT 3",
        "SELECT a, COUNT(*) FROM t2 GROUP BY a ORDER BY a",
        "SELECT a, b, c FROM t2 WHERE a < 3 GROUP BY a, b ORDER BY a, b",
        "SELECT id, k, COUNT(*) FROM t1 WHERE id < 10 GROUP BY id ORDER BY id",
    ] {
        out.push(qo(s));
    }
    out
}

fn join(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let g = rng.below(5);
        let x = rng.below(30) as i64 * 50 - 300;
        out.push(q(match i % 22 {
            0 => "SELECT p.id, p.name, c.id, c.amt FROM par p JOIN chi c ON c.par_id = p.id".to_owned(),
            1 => format!("SELECT p.id, c.id FROM par p JOIN chi c ON c.par_id = p.id WHERE p.grp = {g}"),
            2 => format!("SELECT p.id, c.id, c.amt FROM par p JOIN chi c ON c.par_id = p.id WHERE c.amt > {x}"),
            3 => "SELECT p.id, c.id, c.qty FROM par p JOIN chn c ON c.par_id = p.id".to_owned(),
            4 => format!("SELECT p.id, c.id FROM par p, chn c WHERE c.par_id = p.id AND p.grp <> {g}"),
            5 => format!("SELECT p.name, c.tag, t.k FROM par p JOIN chi c ON c.par_id = p.id JOIN t1 t ON t.id = c.id WHERE t.k < {}", rng.below(12)),
            6 => format!("SELECT a.id, b.id FROM t1 a JOIN t1 b ON a.k = b.k AND a.id < b.id WHERE a.id < {}", 5 + rng.below(20)),
            7 => "SELECT c.id, n.id FROM chi c JOIN chn n ON c.par_id = n.par_id WHERE c.id < 20".to_owned(),
            8 => "SELECT par.id, chi.tag FROM par JOIN chi USING (id)".to_owned(),
            9 => format!("SELECT p.id, c.id FROM par p JOIN chi c ON p.id < c.par_id WHERE p.id > 17 AND c.id < {}", 10 + rng.below(40)),
            10 => format!("SELECT t1.id, t5.u FROM t1 JOIN t5 ON t5.id = t1.k WHERE t1.id < {}", 10 + rng.below(80)),
            11 => "SELECT t1.id, t5.id FROM t1 JOIN t5 ON t5.un = t1.id".to_owned(),
            12 => "SELECT t1.id, t8.ti FROM t1 JOIN t8 ON t8.ti = t1.k".to_owned(),
            13 => "SELECT t1.id, c.id FROM t1 JOIN chi c ON c.amt = t1.d WHERE t1.id < 40".to_owned(),
            14 => "SELECT a.id, b.id FROM t6 a JOIN t6 b ON a.sg = b.sg WHERE a.id < 12 AND b.id < 12".to_owned(),
            15 => "SELECT a.id, b.id FROM t6 a JOIN t6 b ON a.sb = b.sg WHERE a.id < 12".to_owned(),
            16 => "SELECT a.id, b.id FROM t6 a JOIN t6 b ON a.sd = b.sd AND a.id <> b.id WHERE a.id < 9".to_owned(),
            17 => format!("SELECT t2.a, t2.b, t7.id FROM t2 JOIN t7 ON t7.a = t2.a AND t7.b = t2.b WHERE t2.c = {}", rng.below(6)),
            18 => format!("SELECT b1.id, b2.id FROM big b1 JOIN big b2 ON b2.k1 = b1.k1 AND b2.k2 = b1.k2 WHERE b1.id < {}", 5 + rng.below(30)),
            19 => format!("SELECT COUNT(*), SUM(c.amt) FROM par p JOIN chi c ON c.par_id = p.id WHERE p.id BETWEEN {} AND {}", rng.below(10), 8 + rng.below(14)),
            20 => "SELECT p.grp, COUNT(*), MAX(c.amt) FROM par p JOIN chi c ON c.par_id = p.id GROUP BY p.grp".to_owned(),
            _ => format!("SELECT p.id, c.id, n.id FROM par p JOIN chi c ON c.par_id = p.id JOIN chn n ON n.par_id = p.id WHERE p.id = {}", 1 + rng.below(22)),
        }));
    }
    for s in [
        "SELECT p.id, c.id FROM par p JOIN chi c ON c.par_id = p.id ORDER BY p.id, c.id LIMIT 12",
        "SELECT p.id, c.id FROM par p JOIN chi c ON c.par_id = p.id ORDER BY p.id DESC, c.id DESC LIMIT 12",
        "SELECT c.id, p.name FROM chi c JOIN par p ON p.id = c.par_id ORDER BY c.id LIMIT 9",
        "SELECT c.id, p.name FROM chi c JOIN par p ON p.id = c.par_id WHERE c.id > 40 ORDER BY c.id DESC LIMIT 9",
        "SELECT b1.id, b2.id FROM big b1 JOIN big b2 ON b2.id = b1.k2 WHERE b1.k1 = 5 ORDER BY b1.k2, b1.id LIMIT 10",
    ] {
        out.push(qo(s));
    }
    out
}

fn left(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let g = rng.below(5);
        let x = rng.below(30) as i64 * 50 - 300;
        out.push(q(match i % 20 {
            0 => "SELECT p.id, c.id, c.amt FROM par p LEFT JOIN chi c ON c.par_id = p.id".to_owned(),
            1 => format!("SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id AND c.amt > {x}"),
            2 => "SELECT p.id FROM par p LEFT JOIN chi c ON c.par_id = p.id WHERE c.id IS NULL".to_owned(),
            3 => format!("SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id WHERE c.amt > {x}"),
            4 => format!("SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id WHERE p.grp = {g}"),
            5 => "SELECT c.id, c.par_id, p.name FROM chi c LEFT JOIN par p ON p.id = c.par_id".to_owned(),
            6 => "SELECT c.id, p.id FROM chi c LEFT JOIN par p ON p.id = c.par_id WHERE p.id IS NULL".to_owned(),
            7 => "SELECT p.id, c.id FROM chi c RIGHT JOIN par p ON c.par_id = p.id".to_owned(),
            8 => "SELECT p.id, n.id, n.qty FROM par p LEFT JOIN chn n ON n.par_id = p.id".to_owned(),
            9 => format!("SELECT p.id, n.id FROM par p LEFT JOIN chn n ON n.par_id = p.id AND n.qty = {}", rng.below(9)),
            10 => "SELECT p.id, c.id, n.id FROM par p LEFT JOIN chi c ON c.par_id = p.id LEFT JOIN chn n ON n.par_id = c.par_id AND n.qty = 3 WHERE p.id < 6".to_owned(),
            11 => "SELECT p.id, c.id FROM par p LEFT JOIN chi c ON 1 = 0".to_owned(),
            12 => format!("SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.tag = 'a' WHERE p.id < {}", 2 + rng.below(4)),
            13 => "SELECT p.id, n.id FROM par p LEFT JOIN chn n ON n.par_id = p.id WHERE COALESCE(n.qty, 0) = 0".to_owned(),
            14 => format!("SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id AND p.grp = {g}"),
            15 => "SELECT p.id, COUNT(c.id), SUM(c.amt) FROM par p LEFT JOIN chi c ON c.par_id = p.id GROUP BY p.id".to_owned(),
            16 => "SELECT t1.id, t5.id FROM t1 LEFT JOIN t5 ON t5.un = t1.k WHERE t1.id < 30".to_owned(),
            17 => "SELECT a.id, b.id FROM t7 a LEFT JOIN t7 b ON b.a = a.b AND b.b = a.a WHERE a.id < 25".to_owned(),
            18 => "SELECT c.id, p.id, n.id FROM chi c LEFT JOIN par p ON p.id = c.par_id JOIN chn n ON n.id = c.id WHERE c.id < 30".to_owned(),
            _ => "SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id WHERE c.tag IS NULL OR c.tag = 'b'".to_owned(),
        }));
    }
    for s in [
        "SELECT p.id, c.id FROM par p LEFT JOIN chi c ON c.par_id = p.id ORDER BY p.id, c.id LIMIT 15",
        "SELECT c.id, p.id FROM chi c LEFT JOIN par p ON p.id = c.par_id ORDER BY c.id DESC LIMIT 15",
    ] {
        out.push(qo(s));
    }
    out
}

fn sub(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for i in 0..n {
        let g = rng.below(5);
        out.push(q(match i % 26 {
            0 => "SELECT id FROM par WHERE id IN (SELECT par_id FROM chi)".to_owned(),
            1 => format!("SELECT id FROM par WHERE id IN (SELECT par_id FROM chi WHERE amt > {})", rng.below(30) as i64 * 50),
            2 => "SELECT id FROM par WHERE id NOT IN (SELECT par_id FROM chi)".to_owned(),
            3 => "SELECT id FROM par WHERE id NOT IN (SELECT par_id FROM chi WHERE par_id IS NOT NULL)".to_owned(),
            4 => "SELECT id FROM par p WHERE EXISTS (SELECT 1 FROM chi c WHERE c.par_id = p.id)".to_owned(),
            5 => "SELECT id FROM par p WHERE NOT EXISTS (SELECT 1 FROM chi c WHERE c.par_id = p.id)".to_owned(),
            6 => format!("SELECT id FROM par p WHERE EXISTS (SELECT * FROM chn c WHERE c.par_id = p.id AND c.qty > {})", rng.below(9)),
            7 => "SELECT id, k FROM t1 WHERE k = (SELECT MAX(k) FROM t1)".to_owned(),
            8 => "SELECT id, (SELECT COUNT(*) FROM chi) FROM par WHERE id < 4".to_owned(),
            9 => format!("SELECT id FROM chi WHERE par_id IN (SELECT id FROM par WHERE grp = {g})"),
            10 => "SELECT d.par_id, d.n FROM (SELECT par_id, COUNT(*) AS n FROM chi GROUP BY par_id) d WHERE d.n > 2".to_owned(),
            11 => "SELECT p.id, d.n FROM par p JOIN (SELECT par_id, COUNT(*) AS n FROM chi GROUP BY par_id) d ON d.par_id = p.id".to_owned(),
            12 => "WITH c AS (SELECT par_id, SUM(amt) AS s FROM chi GROUP BY par_id) SELECT p.id, c.s FROM par p LEFT JOIN c ON c.par_id = p.id".to_owned(),
            13 => "SELECT id FROM par UNION SELECT par_id FROM chi".to_owned(),
            14 => "SELECT id FROM par UNION ALL SELECT par_id FROM chi WHERE par_id < 5".to_owned(),
            15 => format!("SELECT id FROM par WHERE grp = {g} OR id IN (SELECT par_id FROM chn WHERE qty = 1)"),
            16 => "SELECT a, b FROM t2 WHERE (a, b) IN (SELECT a, b FROM t7)".to_owned(),
            17 => "SELECT id FROM t1 WHERE k IN (SELECT un FROM t5)".to_owned(),
            18 => "SELECT id FROM t1 WHERE k NOT IN (SELECT un FROM t5 WHERE id < 20)".to_owned(),
            19 => "SELECT id FROM t6 WHERE sb IN (SELECT sg FROM t6 WHERE id < 30)".to_owned(),
            20 => "SELECT id FROM t6 WHERE sg IN (SELECT sg FROM t6 WHERE id < 10)".to_owned(),
            21 => "SELECT id FROM par WHERE EXISTS (SELECT 1 FROM chi WHERE amt > 100000)".to_owned(),
            22 => "SELECT id FROM par WHERE id > (SELECT AVG(par_id) FROM chi)".to_owned(),
            23 => "SELECT x.id FROM (SELECT id, k FROM t1 WHERE k < 5) x JOIN (SELECT id FROM t5 WHERE u > 800) y ON y.id = x.k".to_owned(),
            24 => "WITH a AS (SELECT id FROM par WHERE grp = 1), b AS (SELECT par_id FROM chi) SELECT a.id FROM a WHERE a.id IN (SELECT par_id FROM b)".to_owned(),
            _ => "SELECT id FROM par p WHERE EXISTS (SELECT 1 FROM chi c WHERE c.par_id = p.id) AND NOT EXISTS (SELECT 1 FROM chn n WHERE n.par_id = p.id)".to_owned(),
        }));
        let x = rng.below(30) as i64 * 50 - 300;
        out.push(q(match i % 22 {
            // Scalar subqueries tied to the outer row.
            0 => "SELECT p.id, (SELECT COUNT(*) FROM chi c WHERE c.par_id = p.id) FROM par p".to_owned(),
            1 => "SELECT p.id, (SELECT MAX(c.amt) FROM chi c WHERE c.par_id = p.id) FROM par p".to_owned(),
            2 => format!("SELECT p.id, (SELECT SUM(c.amt) FROM chi c WHERE c.par_id = p.id AND c.amt > {x}) FROM par p WHERE p.grp = {g}"),
            3 => "SELECT c.id, (SELECT p.name FROM par p WHERE p.id = c.par_id) FROM chi c".to_owned(),
            4 => "SELECT p.id FROM par p WHERE (SELECT COUNT(*) FROM chn n WHERE n.par_id = p.id) > 2".to_owned(),
            5 => "SELECT p.id FROM par p WHERE p.grp = (SELECT MIN(n.qty) FROM chn n WHERE n.par_id = p.id)".to_owned(),
            6 => "SELECT c.id FROM chi c WHERE c.amt = (SELECT MAX(d.amt) FROM chi d WHERE d.par_id = c.par_id)".to_owned(),
            7 => "SELECT p.id, (SELECT COUNT(*) FROM chi c WHERE c.par_id = p.id) AS n FROM par p WHERE (SELECT COUNT(*) FROM chn x WHERE x.par_id = p.id) = 0".to_owned(),
            8 => "SELECT t1.id, (SELECT t5.u FROM t5 WHERE t5.un = t1.k) FROM t1 WHERE t1.id < 30".to_owned(),
            9 => match rng.below(3) {
                0 => "SELECT c.id, (SELECT n.id FROM chn n WHERE n.par_id = c.par_id) FROM chi c WHERE c.id < 4".to_owned(),
                // The latest child of each parent.
                1 => "SELECT p.id, (SELECT c.id FROM chi c WHERE c.par_id = p.id ORDER BY c.id DESC LIMIT 1) FROM par p".to_owned(),
                _ => "SELECT p.id, (SELECT c.amt FROM chi c WHERE c.par_id = p.id AND c.amt IS NOT NULL ORDER BY c.amt, c.id LIMIT 1) FROM par p WHERE p.grp > 0".to_owned(),
            },
            // Derived tables that become their table, and conditions that
            // move into the ones that don't.
            10 => format!("SELECT d.id, d.k FROM (SELECT id, k, v FROM t1 WHERE k > 2) d WHERE d.id < {}", 5 + rng.below(80)),
            11 => format!("SELECT d.x FROM (SELECT id + 1 AS x, k FROM t1) d WHERE d.k = {}", rng.below(12)),
            12 => format!("SELECT * FROM (SELECT * FROM t2 WHERE a > 1) d WHERE d.a = {} AND d.b > 0", rng.below(9)),
            13 => "SELECT p.id, d.amt FROM par p LEFT JOIN (SELECT par_id, amt FROM chi WHERE amt > 100) d ON d.par_id = p.id".to_owned(),
            14 => "SELECT p.id, d.x FROM par p LEFT JOIN (SELECT par_id, COALESCE(amt, 0) AS x FROM chi) d ON d.par_id = p.id".to_owned(),
            15 => format!("SELECT d.par_id, d.n FROM (SELECT par_id, COUNT(*) AS n FROM chi GROUP BY par_id) d WHERE d.par_id = {}", 1 + rng.below(25)),
            16 => format!("SELECT d.par_id, d.n FROM (SELECT par_id, COUNT(*) AS n FROM chi GROUP BY par_id) d WHERE d.n = {} OR d.par_id IS NULL", 1 + rng.below(6)),
            17 => "SELECT d.k FROM (SELECT DISTINCT k FROM t1) d WHERE d.k < 5".to_owned(),
            18 => "SELECT x.id FROM (SELECT id, k FROM t1 ORDER BY id LIMIT 10) x WHERE x.k > 3".to_owned(),
            19 => "WITH c AS (SELECT id, par_id FROM chi WHERE par_id IS NOT NULL) SELECT p.id, c.id FROM par p JOIN c ON c.par_id = p.id WHERE p.id < 5".to_owned(),
            20 => "SELECT a.id, b.id FROM (SELECT id, k FROM t1 WHERE id < 20) a JOIN (SELECT id, k FROM t1 WHERE id > 70) b ON a.k = b.k".to_owned(),
            _ => format!("SELECT id FROM big WHERE id IN (SELECT k2 FROM big WHERE k1 = {} AND id < 100)", rng.below(20)),
        }));
    }
    for s in [
        "SELECT id FROM par UNION SELECT par_id FROM chi ORDER BY id LIMIT 10",
        "SELECT id, 'p' AS src FROM par UNION ALL SELECT id, 'c' FROM chi ORDER BY 1 DESC, 2 LIMIT 10",
        "SELECT id FROM par WHERE id IN (SELECT par_id FROM chi) ORDER BY id DESC LIMIT 5",
        "(SELECT id FROM par ORDER BY id LIMIT 3) UNION ALL (SELECT id FROM chi ORDER BY id DESC LIMIT 3) ORDER BY id",
    ] {
        out.push(qo(s));
    }
    out
}

/// A join the mixed generator builds on: its `FROM` text, the columns
/// conditions are put on, the columns that make an order total, and
/// integer columns to group by and to aggregate.
struct Skeleton {
    from: &'static str,
    cols: &'static [(&'static str, K)],
    tie: &'static [&'static str],
    group: &'static [&'static str],
    sum: &'static [&'static str],
}

const SKELETONS: &[Skeleton] = &[
    Skeleton {
        from: "par p JOIN chi c ON c.par_id = p.id",
        cols: &[
            ("p.id", K::Int),
            ("p.grp", K::Int),
            ("c.id", K::Int),
            ("c.amt", K::Dec),
            ("c.tag", K::Str),
            ("c.par_id", K::Int),
        ],
        tie: &["p.id", "c.id"],
        group: &["p.grp", "p.id", "c.par_id"],
        sum: &["c.amt", "c.id", "p.grp"],
    },
    Skeleton {
        from: "par p LEFT JOIN chi c ON c.par_id = p.id",
        cols: &[
            ("p.id", K::Int),
            ("p.grp", K::Int),
            ("c.id", K::Int),
            ("c.amt", K::Dec),
            ("c.tag", K::Str),
        ],
        tie: &["p.id", "c.id"],
        group: &["p.grp", "p.id"],
        sum: &["c.amt", "c.id"],
    },
    Skeleton {
        from: "chi c JOIN par p ON p.id = c.par_id JOIN chn n ON n.par_id = p.id",
        cols: &[
            ("p.id", K::Int),
            ("p.grp", K::Int),
            ("c.id", K::Int),
            ("c.amt", K::Dec),
            ("n.qty", K::Int),
            ("n.id", K::Int),
        ],
        tie: &["c.id", "n.id"],
        group: &["p.grp", "n.qty", "p.id"],
        sum: &["c.amt", "n.qty"],
    },
    Skeleton {
        from: "par p LEFT JOIN chi c ON c.par_id = p.id LEFT JOIN chn n ON n.par_id = p.id AND n.qty > 2",
        cols: &[
            ("p.id", K::Int),
            ("p.grp", K::Int),
            ("c.id", K::Int),
            ("c.amt", K::Dec),
            ("n.qty", K::Int),
            ("n.id", K::Int),
        ],
        tie: &["p.id", "c.id", "n.id"],
        group: &["p.grp", "p.id"],
        sum: &["c.amt", "n.qty"],
    },
    Skeleton {
        from: "t1 a JOIN t5 b ON b.id = a.k",
        cols: &[
            ("a.id", K::Int),
            ("a.k", K::Int),
            ("a.d", K::Dec),
            ("a.dt", K::Date),
            ("b.u", K::Int),
            ("b.un", K::Int),
            ("b.code", K::Code),
        ],
        tie: &["a.id"],
        group: &["a.k", "b.id"],
        sum: &["a.d", "b.u", "a.id"],
    },
    Skeleton {
        from: "t2 x JOIN t7 y ON y.a = x.a AND y.b = x.b",
        cols: &[
            ("x.a", K::Int),
            ("x.b", K::Int),
            ("x.c", K::Int),
            ("y.c", K::Int),
            ("y.id", K::Int),
        ],
        tie: &["x.a", "x.b", "y.id"],
        group: &["x.a", "x.c", "y.c"],
        sum: &["y.c", "x.b"],
    },
    Skeleton {
        from: "big b JOIN par p ON p.id = b.k1",
        cols: &[
            ("b.id", K::Int),
            ("b.k1", K::Int),
            ("b.k2", K::Int),
            ("p.grp", K::Int),
            ("p.id", K::Int),
        ],
        tie: &["b.id"],
        group: &["b.k1", "p.grp"],
        sum: &["b.k2", "b.id"],
    },
    Skeleton {
        from: "chi c LEFT JOIN par p ON p.id = c.par_id LEFT JOIN t5 f ON f.un = c.id",
        cols: &[
            ("c.id", K::Int),
            ("c.par_id", K::Int),
            ("c.amt", K::Dec),
            ("p.grp", K::Int),
            ("f.u", K::Int),
            ("f.id", K::Int),
        ],
        tie: &["c.id"],
        group: &["c.par_id", "p.grp"],
        sum: &["c.amt", "f.u"],
    },
    Skeleton {
        from: "par p JOIN (SELECT par_id, COUNT(*) AS n, MAX(amt) AS m FROM chi GROUP BY par_id) d ON d.par_id = p.id",
        cols: &[
            ("p.id", K::Int),
            ("p.grp", K::Int),
            ("d.n", K::Int),
            ("d.m", K::Dec),
            ("d.par_id", K::Int),
        ],
        tie: &["p.id"],
        group: &["p.grp", "d.n"],
        sum: &["d.m", "d.n"],
    },
    Skeleton {
        from: "t7 y",
        cols: &[
            ("y.id", K::Int),
            ("y.a", K::Int),
            ("y.b", K::Int),
            ("y.c", K::Int),
        ],
        tie: &["y.id"],
        group: &["y.a", "y.b"],
        sum: &["y.c", "y.id"],
    },
];

/// Statements that put several clauses together: a join, conditions on
/// either side, subquery predicates, grouping with HAVING, a total order
/// and a LIMIT.
fn mixed(rng: &mut Rng, n: usize) -> Vec<Q> {
    let mut out = Vec::new();
    for _ in 0..n {
        let s = rng.pick(SKELETONS);
        let mut conds: Vec<String> = Vec::new();
        for _ in 0..rng.below(3) {
            let (c, k) = *rng.pick(s.cols);
            conds.push(atom(rng, c, k));
        }
        let first = s.cols[0].0;
        match rng.below(8) {
            0 => conds.push(format!(
                "{first} IN (SELECT par_id FROM chn WHERE qty > {})",
                rng.below(8)
            )),
            1 => conds.push(format!(
                "NOT EXISTS (SELECT 1 FROM chn z WHERE z.par_id = {first} AND z.qty < {})",
                rng.below(8)
            )),
            2 => conds.push(format!("EXISTS (SELECT 1 FROM t1 z WHERE z.k = {first})")),
            3 => conds.push(format!(
                "{first} > (SELECT AVG(k) FROM t1 WHERE id < {})",
                5 + rng.below(80)
            )),
            _ => {}
        }
        let filter = if conds.is_empty() {
            String::new()
        } else if rng.chance(1, 5) && conds.len() > 1 {
            format!(" WHERE {}", conds.join(" OR "))
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        let limit = |rng: &mut Rng| {
            if rng.chance(1, 2) {
                format!(" LIMIT {} OFFSET {}", 1 + rng.below(9), rng.below(4))
            } else {
                String::new()
            }
        };
        if rng.chance(1, 2) {
            // Grouped: the order is by the grouping column, which is
            // unique among the groups.
            let g = *rng.pick(s.group);
            let a = *rng.pick(s.sum);
            let b = *rng.pick(s.sum);
            let having = match rng.below(4) {
                0 => format!(" HAVING COUNT(*) > {}", rng.below(4)),
                1 => format!(" HAVING SUM({a}) IS NOT NULL"),
                _ => String::new(),
            };
            let dir = if rng.chance(1, 2) { " DESC" } else { "" };
            out.push(qo(format!(
                "SELECT {g}, COUNT(*), SUM({a}), MIN({b}), MAX({b}), COUNT(DISTINCT {a}) FROM {}{filter} GROUP BY {g}{having} ORDER BY {g}{dir}{}",
                s.from,
                limit(rng)
            )));
        } else {
            let shown: Vec<&str> = s
                .cols
                .iter()
                .filter(|c| !matches!(c.1, K::Str))
                .map(|c| c.0)
                .collect();
            let dir = if rng.chance(1, 3) { " DESC" } else { "" };
            let order: Vec<String> = s.tie.iter().map(|t| format!("{t}{dir}")).collect();
            out.push(qo(format!(
                "SELECT {} FROM {}{filter} ORDER BY {}{}",
                shown.join(", "),
                s.from,
                order.join(", "),
                limit(rng)
            )));
        }
    }
    out
}

#[derive(Default, Clone)]
struct Count {
    cases: usize,
    same: usize,
    differ: usize,
    /// The planner answered "not supported yet" where the server answered.
    uncovered: usize,
    /// The server's own answer changes when it is kept from using its
    /// indexes and subquery strategies, and then equals the planner's.
    plan_dependent: usize,
    /// Statements the server answered with at least one row, with an
    /// error, and the rows it returned in all.
    with_rows: usize,
    errors: usize,
    rows: usize,
    /// Errors with the server's number whose text is the server's too.
    same_text: usize,
}

/// The statement with every table of a single-table `FROM` told to ignore
/// its indexes, for the servers.
fn without_indexes(sql: &str, specs: &[common::Spec]) -> String {
    let mut out = sql.to_owned();
    for s in specs {
        let mut keys: Vec<&str> = s.keys.iter().map(|k| k.0).collect();
        if !s.pk.is_empty() {
            keys.push("PRIMARY");
        }
        if keys.is_empty() {
            continue;
        }
        let hint = format!(" FROM {} IGNORE INDEX ({}) ", s.name, keys.join(", "));
        for next in ["WHERE", "ORDER", "GROUP"] {
            out = out.replace(
                &format!(" FROM {} {next}", s.name),
                &format!("{hint}{next}"),
            );
        }
    }
    out
}

/// Writes the generated tables and statements of one seed as text, for a
/// harness that runs them through a server's ports instead of the planner
/// in memory: one line per item, tab-separated (`D` a table's definition,
/// `I` an insert, `K` a table and its key names, `Q` a feature, whether
/// its order is fixed, and the statement).
fn dump(dir: &str, seed: u64, cases: usize) {
    use sprig_exec::Persona;
    let _ = std::fs::create_dir_all(dir);
    let db = std::env::var("SPRIG_PLAN_DUMP_DB").unwrap_or_else(|_| format!("rp_diff_{seed}"));
    for (persona, label) in [(Persona::MariaDb, "mariadb"), (Persona::MySql, "mysql")] {
        let specs = world(persona, seed);
        let mut out = String::new();
        for s in &specs {
            let _ = writeln!(out, "D\t{}", s.ddl(&db));
            for i in s.inserts(&db) {
                let _ = writeln!(out, "I\t{i}");
            }
            let mut keys: Vec<&str> = s.keys.iter().map(|k| k.0).collect();
            if !s.pk.is_empty() {
                keys.push("PRIMARY");
            }
            let _ = writeln!(out, "K\t{}\t{}", s.name, keys.join(","));
        }
        let mut rng = Rng::new(seed ^ 0xD1FF);
        let features: Vec<(&str, Vec<Q>)> = vec![
            ("names", names(&db)),
            ("point", point(&mut rng, cases)),
            ("range", range(&mut rng, cases * 4)),
            ("order", order(&mut rng, cases)),
            ("minmax", minmax(&mut rng, cases / 2)),
            ("agg", agg(&mut rng, cases)),
            ("join", join(&mut rng, cases)),
            ("left", left(&mut rng, cases)),
            ("sub", sub(&mut rng, cases)),
            ("mixed", mixed(&mut rng, cases * 2)),
        ];
        for (feature, queries) in features {
            for q in queries {
                let sql = q.sql.replace(['\n', '\t'], " ");
                let _ = writeln!(out, "Q\t{feature}\t{}\t{sql}", u8::from(q.ordered));
            }
        }
        let _ = std::fs::write(format!("{dir}/{label}-{seed}.tsv"), out);
    }
}

#[ignore = "live: needs SPRIG_PLAN_DUMP_DIR, SPRIG_PLAN_ORACLE_SEED, SPRIG_PLAN_ORACLE_CASES; tools/live/README.md"]
#[test]
fn planner_matches_the_servers() {
    // The generated tables and statements as text, for another harness.
    if let Ok(dir) = std::env::var("SPRIG_PLAN_DUMP_DIR") {
        let num = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(d)
        };
        dump(
            &dir,
            num("SPRIG_PLAN_ORACLE_SEED", 7),
            num("SPRIG_PLAN_ORACLE_CASES", 120) as usize,
        );
        return;
    }
    let oracles = Oracle::from_env();
    if oracles.is_empty() {
        eprintln!("no SPRIG_PLAN_ORACLE_* set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    }
    let seed: u64 = std::env::var("SPRIG_PLAN_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let cases: usize = std::env::var("SPRIG_PLAN_ORACLE_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let only = std::env::var("SPRIG_PLAN_ORACLE_ONLY").ok();
    let report_dir = std::env::var("SPRIG_PLAN_ORACLE_REPORT").ok();
    let db = format!("rp_diff_{seed}");
    let mut summary = String::new();
    let mut details = String::new();
    for oracle in &oracles {
        let specs = world(oracle.persona, seed);
        oracle.load(&db, &specs);
        let mem = load_mem(&db, &specs);
        let sess = session(oracle.persona);
        let mut rng = Rng::new(seed ^ 0xD1FF);
        let features: Vec<(&str, Vec<Q>)> = vec![
            ("names", names(&db)),
            ("point", point(&mut rng, cases)),
            ("range", range(&mut rng, cases * 4)),
            ("order", order(&mut rng, cases)),
            ("minmax", minmax(&mut rng, cases / 2)),
            ("agg", agg(&mut rng, cases)),
            ("join", join(&mut rng, cases)),
            ("left", left(&mut rng, cases)),
            ("sub", sub(&mut rng, cases)),
            ("mixed", mixed(&mut rng, cases * 2)),
        ];
        for (feature, queries) in features {
            if only.as_deref().is_some_and(|o| o != feature) {
                continue;
            }
            let sqls: Vec<String> = queries.iter().map(|x| x.sql.clone()).collect();
            let theirs = oracle.answers(&db, &sqls);
            let mut count = Count::default();
            let mut types: std::collections::BTreeMap<String, usize> = Default::default();
            for (query, their) in queries.iter().zip(&theirs) {
                count.cases += 1;
                match their {
                    Outcome::Rows(r) => {
                        count.with_rows += usize::from(!r.is_empty());
                        count.rows += r.len();
                    }
                    Outcome::Error(..) => count.errors += 1,
                }
                let ours = run_ours(&mem, &sess, &query.sql, None);
                for row in &ours.explain {
                    let t = match (&row.access, row.extra.first()) {
                        (Some(a), _) => a.clone(),
                        (None, Some(m)) => format!("[{m}]"),
                        (None, None) => "?".into(),
                    };
                    *types.entry(t).or_default() += 1;
                }
                // The same statement with every condition re-checked on
                // the rows read must give the same rows: a difference
                // means a key range dropped a row or a condition.
                let checked = run_ours(
                    &mem,
                    &sess,
                    &query.sql,
                    Some(PlanOptions {
                        keep_filters: true,
                        ..PlanOptions::for_persona(oracle.persona)
                    }),
                );
                let self_same = same(&ours.outcome, &checked.outcome, query.ordered);
                if same(&ours.outcome, their, query.ordered) && self_same {
                    count.same += 1;
                    if let (Outcome::Error(_, a), Outcome::Error(_, b)) = (&ours.outcome, their) {
                        if a == b {
                            count.same_text += 1;
                        } else {
                            let _ = writeln!(
                                details,
                                "[{} {feature} error text] {}\n  server: {b}\n  ours:   {a}\n",
                                oracle.label, query.sql
                            );
                        }
                    }
                    continue;
                }
                let uncovered = matches!(&ours.outcome, Outcome::Error(1235, _))
                    && !matches!(their, Outcome::Error(..));
                // Ask the server again with its indexes and subquery
                // strategies out of the way.
                let mut plan_dependent = false;
                if !uncovered && self_same {
                    let plain = oracle.answers(
                        &db,
                        &[
                            "SET optimizer_switch='semijoin=off,materialization=off'".to_owned(),
                            without_indexes(&query.sql, &specs),
                        ],
                    );
                    plan_dependent = same(&ours.outcome, &plain[1], query.ordered);
                }
                if uncovered {
                    count.uncovered += 1;
                } else if plan_dependent {
                    count.plan_dependent += 1;
                } else {
                    count.differ += 1;
                }
                let _ = writeln!(
                    details,
                    "[{} {feature}{}] {}\n  server: {}\n  ours:   {}{}\n",
                    oracle.label,
                    if uncovered {
                        " uncovered"
                    } else if plan_dependent {
                        " server-plan-dependent"
                    } else {
                        ""
                    },
                    query.sql,
                    show(their),
                    show(&ours.outcome),
                    if self_same {
                        String::new()
                    } else {
                        format!(
                            "\n  ours with every filter kept: {}",
                            show(&checked.outcome)
                        )
                    }
                );
            }
            let _ = writeln!(
                summary,
                "{:8} {:7} cases {:5}  same {:5}  differ {:4}  uncovered {:4}  server-plan-dependent {:3}",
                oracle.label,
                feature,
                count.cases,
                count.same,
                count.differ,
                count.uncovered,
                count.plan_dependent
            );
            let _ = writeln!(
                summary,
                "         {:7} server answers: {} with rows ({} rows in all), {} errors ({} with our text identical)",
                "", count.with_rows, count.rows, count.errors, count.same_text
            );
            let _ = writeln!(
                summary,
                "         {:7} our access types: {}",
                "",
                types
                    .iter()
                    .map(|(t, n)| format!("{t} {n}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        oracle.drop_db(&db);
    }
    println!("{summary}");
    if let Some(dir) = report_dir {
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(format!("{dir}/diff-summary-{seed}.txt"), &summary);
        let _ = std::fs::write(format!("{dir}/diff-mismatches-{seed}.txt"), &details);
    } else {
        println!("{details}");
    }
}
