//! Row operators against the servers. Two small generated tables are
//! loaded into MariaDB 11.8 and MySQL 8.4; queries over them (filters and
//! projections, sorts, limits, grouping with every aggregate, `DISTINCT`,
//! inner and left joins, `UNION`) are run there and through the operators
//! over in-memory copies of the same rows, and the whole results are
//! compared row by row as text.
//!
//! The servers are reached as in `expr_differential`
//! (`SPRIG_EXEC_ORACLE_MARIADB`, `SPRIG_EXEC_ORACLE_MYSQL`); without them
//! the test does nothing. `SPRIG_EXEC_ORACLE_SEED` varies the rows,
//! `SPRIG_EXEC_ORACLE_REPORT` names a directory for counts and
//! mismatches. The test works in its own database, `r_exec_ops`, and
//! drops it at the end.

#![allow(
    clippy::collapsible_if,
    clippy::assigning_clones,
    clippy::many_single_char_names,
    clippy::explicit_iter_loop,
    clippy::unnecessary_box_returns,
    clippy::explicit_auto_deref,
    clippy::needless_pass_by_value,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::format_push_string,
    clippy::similar_names,
    clippy::type_complexity
)]

mod diff_common;

use std::collections::BTreeMap;
use std::fmt::Write as _;

use diff_common::{Oracle, Rng, client_escape, drop_db, sql_literal};
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Date, Decimal, Value};
use sprig_exec::ops::{
    AggKind, Aggregate, Aggregation, Distinct, Filter, HashJoin, JoinKind, Limit, Memory,
    NestedLoopJoin, Project, Row, RowSource, Sort, SortKey, Union, VecSource, collect,
};
use sprig_exec::{BinOp, Compiled, Ctx, Expr, ExprType, Persona, Session, compile, value_text};

const DB: &str = "r_exec_ops";

struct Table {
    name: &'static str,
    cols: Vec<(&'static str, String, ColumnType)>,
    rows: Vec<Row>,
}

impl Table {
    fn types(&self) -> Vec<ExprType> {
        self.cols
            .iter()
            .enumerate()
            .map(|(i, c)| ExprType::of_column(&c.2, i > 0))
            .collect()
    }

    fn source(&self) -> Box<dyn RowSource> {
        Box::new(VecSource::new(self.rows.clone(), self.types()))
    }

    /// A column as an expression; `offset` shifts the slot for the right
    /// side of a join.
    fn col(&self, name: &str, offset: usize) -> Expr {
        let i = self.cols.iter().position(|c| c.0 == name).unwrap();
        Expr::column(
            i + offset,
            &format!("{}`.`{}", self.name, name),
            self.cols[i].2,
            i > 0,
        )
    }
}

fn tables(persona: Persona, seed: u64) -> (Table, Table) {
    let (coll, coll_name) = match persona {
        Persona::MariaDb => (Collation::Utf8mb4Uca1400AiCi, "utf8mb4_uca1400_ai_ci"),
        Persona::MySql => (Collation::Utf8mb4Mysql0900AiCi, "utf8mb4_0900_ai_ci"),
    };
    let mut rng = Rng::new(seed);
    let strs = ["a", "A", "b", "B", "\u{e1}", "ab", "c", "", "x", "X", "y"];
    let decs = [
        "1.50", "2.25", "-3.10", "100.00", "0.00", "2.00", "1.00", "7.77",
    ];
    let maybe = |rng: &mut Rng, v: Value| if rng.chance(1, 7) { Value::Null } else { v };
    let dec = |s: &str| Value::Decimal(Decimal::parse(s).unwrap());
    let vc = |chars: u32, collation: Collation| ColumnType::VarChar { chars, collation };
    let mut a_rows = Vec::new();
    for id in 1..=70i64 {
        let k = Value::Int(rng.below(7) as i64);
        let g = Value::str(*rng.pick(&strs[..8]));
        let n = dec(*rng.pick(&decs));
        // Sums of these are exact, so they don't depend on the order rows are added in.
        let f = Value::Double([0.5, 1.5, -2.25, 1e10, 0.125, 3.0][rng.below(6)]);
        let s = Value::str(*rng.pick(&strs));
        let d = Value::Date(Date::new(
            2023 + rng.below(3) as u16,
            1 + rng.below(12) as u8,
            1 + rng.below(28) as u8,
        ));
        a_rows.push(vec![
            Value::Int(id),
            maybe(&mut rng, k),
            maybe(&mut rng, g),
            maybe(&mut rng, n),
            maybe(&mut rng, f),
            maybe(&mut rng, s),
            maybe(&mut rng, d),
        ]);
    }
    let mut b_rows = Vec::new();
    for id in 1..=45i64 {
        let k = Value::Int(rng.below(9) as i64);
        let t = Value::str(*rng.pick(&strs));
        let v = Value::Int([1, 2, 100, -3, 7, 1_000_000_000_000][rng.below(6)]);
        b_rows.push(vec![
            Value::Int(id),
            maybe(&mut rng, k),
            maybe(&mut rng, t),
            maybe(&mut rng, v),
        ]);
    }
    let a = Table {
        name: "a",
        cols: vec![
            (
                "id",
                "INT NOT NULL PRIMARY KEY".to_owned(),
                ColumnType::int(false),
            ),
            ("k", "INT".to_owned(), ColumnType::int(false)),
            (
                "g",
                format!("VARCHAR(10) CHARACTER SET utf8mb4 COLLATE {coll_name}"),
                vc(10, coll),
            ),
            (
                "n",
                "DECIMAL(10,2)".to_owned(),
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            ("f", "DOUBLE".to_owned(), ColumnType::Double),
            (
                "s",
                "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci".to_owned(),
                vc(20, Collation::Utf8mb4GeneralCi),
            ),
            ("d", "DATE".to_owned(), ColumnType::Date),
        ],
        rows: a_rows,
    };
    let b = Table {
        name: "b",
        cols: vec![
            (
                "id",
                "INT NOT NULL PRIMARY KEY".to_owned(),
                ColumnType::int(false),
            ),
            ("k", "INT".to_owned(), ColumnType::int(false)),
            (
                "t",
                format!("VARCHAR(10) CHARACTER SET utf8mb4 COLLATE {coll_name}"),
                vc(10, coll),
            ),
            ("v", "BIGINT".to_owned(), ColumnType::bigint(false)),
        ],
        rows: b_rows,
    };
    (a, b)
}

fn load(oracle: &Oracle, tables: &[&Table]) {
    let mut sql = oracle.header();
    sql.push_str(&format!(
        "DROP DATABASE IF EXISTS {DB}; CREATE DATABASE {DB}; CREATE TABLE {DB}.one (x INT PRIMARY KEY); INSERT INTO {DB}.one VALUES (1);\n"
    ));
    for t in tables {
        let cols: Vec<String> = t
            .cols
            .iter()
            .map(|c| format!("`{}` {}", c.0, c.1))
            .collect();
        sql.push_str(&format!(
            "CREATE TABLE {DB}.{} ({});\n",
            t.name,
            cols.join(", ")
        ));
        for row in &t.rows {
            let vals: Vec<String> = row.iter().map(sql_literal).collect();
            sql.push_str(&format!(
                "INSERT INTO {DB}.{} VALUES ({});\n",
                t.name,
                vals.join(", ")
            ));
        }
    }
    let (_, err) = oracle.run(sql.as_bytes(), "");
    assert!(err.trim().is_empty(), "loading on {}: {err}", oracle.label);
}

/// Runs whole queries; each answer is the result's lines, or the error
/// number.
fn run_queries(oracle: &Oracle, queries: &[String]) -> Vec<Result<Vec<Vec<u8>>, u16>> {
    let mut sql = oracle.header().into_bytes();
    sql.extend_from_slice(format!("USE {DB};\n").as_bytes());
    for (i, q) in queries.iter().enumerate() {
        sql.extend_from_slice(format!("SELECT '\u{1}#{i}';\n{q};\n").as_bytes());
    }
    let (out, err) = oracle.run(&sql, "");
    let mut answers: Vec<Result<Vec<Vec<u8>>, u16>> = vec![Ok(Vec::new()); queries.len()];
    for line in err.lines() {
        if let Some(rest) = line.strip_prefix("ERROR ") {
            let code: u16 = rest
                .split(' ')
                .next()
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            if let Some(pos) = rest.find(" at line ") {
                let n: usize = rest[pos + 9..]
                    .split(':')
                    .next()
                    .and_then(|c| c.trim().parse().ok())
                    .unwrap_or(0);
                if n >= 4 {
                    answers[(n - 4) / 2] = Err(code);
                }
            }
        }
    }
    let mut cur: Option<usize> = None;
    let mut lines: Vec<&[u8]> = out.split(|&b| b == b'\n').collect();
    if lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    for line in lines {
        if line.starts_with(b"\x01#") {
            cur = std::str::from_utf8(&line[2..])
                .ok()
                .and_then(|t| t.parse().ok());
            continue;
        }
        if let Some(i) = cur {
            if let Ok(rows) = &mut answers[i] {
                rows.push(line.to_vec());
            }
        }
    }
    answers
}

fn render(rows: &[Row], types: &[ExprType], persona: Persona) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|row| {
            let mut line = Vec::new();
            for (i, (v, ty)) in row.iter().zip(types).enumerate() {
                if i > 0 {
                    line.push(b'\t');
                }
                line.extend(client_escape(value_text(v, ty, persona).as_deref()));
            }
            line
        })
        .collect()
}

struct Case {
    area: &'static str,
    /// The leading columns whose text is compared under their collation:
    /// which member of a group (or of equal minima) a server shows isn't
    /// defined.
    loose: usize,
    /// Whether rows are compared as a multiset: the order of rows equal
    /// under the sort key isn't defined.
    unordered: bool,
    sql: String,
    plan: Box<dyn FnOnce(&Session) -> sprig_exec::Result<Box<dyn RowSource>>>,
}

fn c(e: &Expr, s: &Session) -> sprig_exec::Result<Compiled> {
    compile(e, s)
}

fn bin(op: BinOp, a: Expr, b: Expr) -> Expr {
    Expr::binary(op, a, b)
}

fn slot(i: usize, ty: &ExprType) -> Expr {
    // A reference to an operator's output column, typed by a column type
    // that gives the same expression type.
    let column = match ty.kind {
        sprig_exec::Kind::Str => ColumnType::VarChar {
            chars: ty.len.max(1),
            collation: ty.coll,
        },
        sprig_exec::Kind::Decimal => ColumnType::Decimal {
            precision: ty.prec.max(1),
            scale: ty.dec,
        },
        sprig_exec::Kind::Double | sprig_exec::Kind::Float => ColumnType::Double,
        sprig_exec::Kind::Date => ColumnType::Date,
        _ => ColumnType::bigint(ty.unsigned),
    };
    Expr::column(i, &format!("c{i}"), column, true)
}

fn sort_by(
    input: Box<dyn RowSource>,
    cols: &[(usize, bool)],
    s: &Session,
) -> sprig_exec::Result<Box<dyn RowSource>> {
    let types = input.types();
    let mut keys = Vec::new();
    for (i, desc) in cols {
        keys.push(SortKey {
            expr: c(&slot(*i, &types[*i]), s)?,
            desc: *desc,
        });
    }
    Ok(Box::new(Sort::new(input, keys, Memory::unlimited())))
}

/// One `GROUP_CONCAT` or other aggregate of the long tail: its SQL, kind,
/// argument column, `DISTINCT`, `ORDER BY` columns with their direction,
/// separator, and `LIMIT`.
type MoreAgg = (
    &'static str,
    AggKind,
    &'static str,
    bool,
    &'static [(&'static str, bool)],
    &'static [u8],
    Option<(u64, u64)>,
);

/// The bit and standard-deviation aggregates, and `GROUP_CONCAT` with
/// `ORDER BY`, `SEPARATOR` and (MariaDB only) `LIMIT`, each alone per
/// query, without groups and over two group columns.
fn more_aggregates(a: &'static Table, persona: Persona) -> Vec<Case> {
    let ac = move |n: &str| a.col(n, 0);
    let mut list: Vec<MoreAgg> = vec![
        ("BIT_AND(a.k)", AggKind::BitAnd, "k", false, &[], b",", None),
        ("BIT_OR(a.k)", AggKind::BitOr, "k", false, &[], b",", None),
        ("BIT_XOR(a.k)", AggKind::BitXor, "k", false, &[], b",", None),
        ("BIT_OR(a.n)", AggKind::BitOr, "n", false, &[], b",", None),
        ("BIT_XOR(a.f)", AggKind::BitXor, "f", false, &[], b",", None),
        ("BIT_AND(a.s)", AggKind::BitAnd, "s", false, &[], b",", None),
        ("BIT_XOR(a.d)", AggKind::BitXor, "d", false, &[], b",", None),
        ("STD(a.n)", AggKind::StdPop, "n", false, &[], b",", None),
        ("STDDEV(a.f)", AggKind::StdPop, "f", false, &[], b",", None),
        (
            "STDDEV_POP(a.k)",
            AggKind::StdPop,
            "k",
            false,
            &[],
            b",",
            None,
        ),
        (
            "STDDEV_SAMP(a.n)",
            AggKind::StdSamp,
            "n",
            false,
            &[],
            b",",
            None,
        ),
        (
            "VARIANCE(a.k)",
            AggKind::VarPop,
            "k",
            false,
            &[],
            b",",
            None,
        ),
        ("VAR_POP(a.f)", AggKind::VarPop, "f", false, &[], b",", None),
        (
            "VAR_SAMP(a.k)",
            AggKind::VarSamp,
            "k",
            false,
            &[],
            b",",
            None,
        ),
        (
            "VAR_SAMP(a.n)",
            AggKind::VarSamp,
            "n",
            false,
            &[],
            b",",
            None,
        ),
        (
            "GROUP_CONCAT(a.s ORDER BY a.s, a.id SEPARATOR '|')",
            AggKind::GroupConcat,
            "s",
            false,
            &[("s", false), ("id", false)],
            b"|",
            None,
        ),
        (
            "GROUP_CONCAT(a.k ORDER BY a.k DESC, a.id SEPARATOR ';')",
            AggKind::GroupConcat,
            "k",
            false,
            &[("k", true), ("id", false)],
            b";",
            None,
        ),
        (
            "GROUP_CONCAT(a.s ORDER BY a.n DESC, a.id DESC SEPARATOR '')",
            AggKind::GroupConcat,
            "s",
            false,
            &[("n", true), ("id", true)],
            b"",
            None,
        ),
        (
            "GROUP_CONCAT(DISTINCT a.k ORDER BY a.k DESC)",
            AggKind::GroupConcat,
            "k",
            true,
            &[("k", true)],
            b",",
            None,
        ),
        (
            "GROUP_CONCAT(a.n ORDER BY a.d, a.id SEPARATOR ', ')",
            AggKind::GroupConcat,
            "n",
            false,
            &[("d", false), ("id", false)],
            b", ",
            None,
        ),
    ];
    if persona == Persona::MariaDb {
        list.extend::<Vec<MoreAgg>>(vec![
            (
                "GROUP_CONCAT(a.s ORDER BY a.id SEPARATOR '|' LIMIT 2)",
                AggKind::GroupConcat,
                "s",
                false,
                &[("id", false)],
                b"|",
                Some((0, 2)),
            ),
            (
                "GROUP_CONCAT(a.k ORDER BY a.k, a.id LIMIT 1, 3)",
                AggKind::GroupConcat,
                "k",
                false,
                &[("k", false), ("id", false)],
                b",",
                Some((1, 3)),
            ),
            (
                "GROUP_CONCAT(a.s ORDER BY a.id DESC LIMIT 0)",
                AggKind::GroupConcat,
                "s",
                false,
                &[("id", true)],
                b",",
                Some((0, 0)),
            ),
        ]);
    }
    let mut out = Vec::new();
    let group_sets: [&'static [&'static str]; 3] = [&[], &["g"], &["k"]];
    for groups in group_sets {
        for agg in &list {
            let (text, kind, arg, distinct, order, sep, limit) = *agg;
            let group_list: Vec<String> = groups.iter().map(|g| format!("a.{g}")).collect();
            let mut select = group_list.clone();
            select.push(text.to_owned());
            let mut sql = format!("SELECT {} FROM a", select.join(", "));
            if !groups.is_empty() {
                sql.push_str(&format!(
                    " GROUP BY {} ORDER BY {}",
                    group_list.join(", "),
                    group_list.join(", ")
                ));
            }
            out.push(Case {
                area: "aggregate-more",
                loose: groups.len(),
                unordered: false,
                sql,
                plan: Box::new(move |s| {
                    let mut group_by = Vec::new();
                    for g in groups {
                        group_by.push(c(&ac(g), s)?);
                    }
                    let mut keys = Vec::new();
                    for (col, desc) in order {
                        keys.push(SortKey {
                            expr: c(&ac(col), s)?,
                            desc: *desc,
                        });
                    }
                    let mut agg =
                        Aggregate::new(kind, Some(c(&ac(arg), s)?), distinct, s)?.separator(sep);
                    if !keys.is_empty() {
                        agg = agg.order_by(keys);
                    }
                    if let Some((off, n)) = limit {
                        agg = agg.limit(off, n);
                    }
                    let aggn =
                        Aggregation::hash(a.source(), group_by, vec![agg], Memory::unlimited());
                    let order: Vec<(usize, bool)> = (0..groups.len()).map(|i| (i, false)).collect();
                    sort_by(Box::new(aggn), &order, s)
                }),
            });
        }
    }
    out
}

fn cases(a: &'static Table, b: &'static Table, rng: &mut Rng) -> Vec<Case> {
    let mut out: Vec<Case> = Vec::new();
    let ac = |n: &str| a.col(n, 0);
    let bc = |n: &str| b.col(n, 0);
    let preds: Vec<Expr> = vec![
        bin(BinOp::Gt, ac("k"), Expr::int(2)),
        Expr::IsNull(Box::new(ac("k")), false),
        bin(BinOp::Eq, ac("g"), Expr::str("a")),
        Expr::Like(Box::new(ac("g")), Box::new(Expr::str("a%")), None, false),
        Expr::Between(
            Box::new(ac("n")),
            Box::new(Expr::int(1)),
            Box::new(Expr::int(50)),
            false,
        ),
        bin(
            BinOp::Lt,
            bin(
                BinOp::Mul,
                bin(BinOp::Add, ac("k"), Expr::int(1)),
                Expr::int(2),
            ),
            ac("n"),
        ),
        Expr::In(
            Box::new(ac("s")),
            vec![Expr::str("x"), Expr::str("y"), Expr::str("")],
            false,
        ),
        bin(
            BinOp::Or,
            bin(BinOp::Gt, ac("f"), Expr::Decimal("0.5".to_owned())),
            bin(BinOp::Eq, ac("k"), Expr::int(1)),
        ),
        Expr::Not(Box::new(bin(BinOp::NullSafeEq, ac("g"), Expr::str("b")))),
        bin(BinOp::Ge, ac("d"), Expr::str("2024-06-15")),
        Expr::Bool(true),
        bin(BinOp::Eq, ac("k"), Expr::int(99)),
    ];
    let projections: Vec<Expr> = vec![
        bin(BinOp::Add, ac("k"), Expr::int(1)),
        Expr::func("CONCAT", vec![ac("g"), Expr::str("-"), ac("s")]),
        bin(BinOp::Mul, ac("n"), Expr::int(2)),
        Expr::func("COALESCE", vec![ac("k"), Expr::int(-1)]),
        Expr::func("UPPER", vec![ac("g")]),
        bin(BinOp::Div, ac("n"), Expr::int(3)),
        Expr::func("ROUND", vec![ac("f"), Expr::int(1)]),
        Expr::func("IFNULL", vec![ac("s"), ac("g")]),
        Expr::func("YEAR", vec![ac("d")]),
    ];
    // Filters and projections.
    for _ in 0..40 {
        let p = rng.pick(&preds).clone();
        let e1 = rng.pick(&projections).clone();
        let e2 = rng.pick(&projections).clone();
        let sql = format!(
            "SELECT a.id, {}, {} FROM a WHERE {} ORDER BY a.id",
            e1.to_sql(),
            e2.to_sql(),
            p.to_sql()
        );
        out.push(Case {
            area: "filter-project",
            loose: 0,
            unordered: false,
            sql,
            plan: Box::new(move |s| {
                let f = Filter::new(a.source(), c(&p, s)?);
                Ok(Box::new(Project::new(
                    Box::new(f),
                    vec![c(&ac("id"), s)?, c(&e1, s)?, c(&e2, s)?],
                )))
            }),
        });
    }
    // Sorts, with and without a limit.
    let sort_cols = ["g", "s", "n", "f", "k", "d"];
    for i in 0..36 {
        let key = *rng.pick(&sort_cols);
        let desc = rng.chance(1, 2);
        let (limit, offset) = (rng.below(12) as u64, rng.below(5) as u64);
        let limited = i % 2 == 1;
        let mut sql = format!(
            "SELECT a.id, a.{key} FROM a ORDER BY a.{key}{}, a.id",
            if desc { " DESC" } else { "" }
        );
        if limited {
            sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
        }
        out.push(Case {
            area: if limited { "top-n" } else { "sort" },
            loose: 0,
            unordered: false,
            sql,
            plan: Box::new(move |s| {
                let keys = vec![
                    SortKey {
                        expr: c(&ac(key), s)?,
                        desc,
                    },
                    SortKey {
                        expr: c(&ac("id"), s)?,
                        desc: false,
                    },
                ];
                let sorted: Box<dyn RowSource> = if limited {
                    Box::new(Limit::new(
                        Box::new(Sort::top_n(
                            a.source(),
                            keys,
                            (limit + offset) as usize,
                            Memory::unlimited(),
                        )),
                        offset,
                        limit,
                    ))
                } else {
                    Box::new(Sort::new(a.source(), keys, Memory::unlimited()))
                };
                Ok(Box::new(Project::new(
                    sorted,
                    vec![c(&ac("id"), s)?, c(&ac(key), s)?],
                )))
            }),
        });
    }
    // Grouping: hash and streaming, every aggregate.
    let group_sets: [&[&str]; 5] = [&["g"], &["k"], &["g", "k"], &["s"], &[]];
    let aggs: Vec<(&str, AggKind, Option<&str>, bool)> = vec![
        ("COUNT(*)", AggKind::CountStar, None, false),
        ("COUNT(a.k)", AggKind::Count, Some("k"), false),
        ("COUNT(DISTINCT a.s)", AggKind::Count, Some("s"), true),
        ("SUM(a.n)", AggKind::Sum, Some("n"), false),
        ("SUM(a.k)", AggKind::Sum, Some("k"), false),
        ("SUM(DISTINCT a.k)", AggKind::Sum, Some("k"), true),
        ("SUM(a.f)", AggKind::Sum, Some("f"), false),
        ("AVG(a.n)", AggKind::Avg, Some("n"), false),
        ("AVG(a.k)", AggKind::Avg, Some("k"), false),
        ("AVG(a.f)", AggKind::Avg, Some("f"), false),
        ("MIN(a.s)", AggKind::Min, Some("s"), false),
        ("MAX(a.s)", AggKind::Max, Some("s"), false),
        ("MIN(a.n)", AggKind::Min, Some("n"), false),
        ("MAX(a.d)", AggKind::Max, Some("d"), false),
        ("MAX(a.k)", AggKind::Max, Some("k"), false),
        (
            "GROUP_CONCAT(a.k ORDER BY a.id SEPARATOR '|')",
            AggKind::GroupConcat,
            Some("k"),
            false,
        ),
        (
            "GROUP_CONCAT(a.s ORDER BY a.id SEPARATOR '|')",
            AggKind::GroupConcat,
            Some("s"),
            false,
        ),
    ];
    for i in 0..60 {
        let groups: &'static [&'static str] = group_sets[rng.below(group_sets.len())];
        let p = rng.pick(&preds).clone();
        let mut picked: Vec<(&str, AggKind, Option<&str>, bool)> =
            (0..3).map(|_| *rng.pick(&aggs)).collect();
        // MIN and MAX of strings come first: their text is compared under
        // the collation, like the group columns'.
        picked.sort_by_key(|x| !(matches!(x.1, AggKind::Min | AggKind::Max) && x.2 == Some("s")));
        let loose = groups.len()
            + picked
                .iter()
                .filter(|x| matches!(x.1, AggKind::Min | AggKind::Max) && x.2 == Some("s"))
                .count();
        let streaming = i % 2 == 1;
        let group_list: Vec<String> = groups.iter().map(|g| format!("a.{g}")).collect();
        let mut select: Vec<String> = group_list.clone();
        select.extend(picked.iter().map(|x| x.0.to_owned()));
        let mut sql = format!("SELECT {} FROM a WHERE {}", select.join(", "), p.to_sql());
        if !groups.is_empty() {
            sql.push_str(&format!(
                " GROUP BY {} ORDER BY {}",
                group_list.join(", "),
                group_list.join(", ")
            ));
        }
        out.push(Case {
            area: if streaming {
                "aggregate-stream"
            } else {
                "aggregate-hash"
            },
            loose,
            unordered: false,
            sql,
            plan: Box::new(move |s| {
                let mut input: Box<dyn RowSource> = Box::new(Filter::new(a.source(), c(&p, s)?));
                let mut group_by = Vec::new();
                for g in groups {
                    group_by.push(c(&ac(g), s)?);
                }
                let mut list = Vec::new();
                for (_, kind, arg, distinct) in &picked {
                    let arg = match arg {
                        Some(n) => Some(c(&ac(n), s)?),
                        None => None,
                    };
                    let mut agg = Aggregate::new(*kind, arg, *distinct, s)?;
                    if *kind == AggKind::GroupConcat {
                        agg = agg.separator(b"|");
                    }
                    list.push(agg);
                }
                let order: Vec<(usize, bool)> = (0..groups.len()).map(|i| (i, false)).collect();
                if streaming {
                    let mut keys = Vec::new();
                    for g in groups {
                        keys.push(SortKey {
                            expr: c(&ac(g), s)?,
                            desc: false,
                        });
                    }
                    input = Box::new(Sort::new(input, keys, Memory::unlimited()));
                    Ok(Box::new(Aggregation::streaming(input, group_by, list)))
                } else {
                    let agg = Aggregation::hash(input, group_by, list, Memory::unlimited());
                    sort_by(Box::new(agg), &order, s)
                }
            }),
        });
    }
    // DISTINCT.
    for cols in [
        vec!["g"],
        vec!["k"],
        vec!["s"],
        vec!["g", "k"],
        vec!["n", "s"],
        vec!["d"],
    ] {
        let list: Vec<String> = cols.iter().map(|x| format!("a.{x}")).collect();
        let sql = format!(
            "SELECT DISTINCT {} FROM a ORDER BY {}",
            list.join(", "),
            list.join(", ")
        );
        out.push(Case {
            area: "distinct",
            loose: 2,
            unordered: false,
            sql,
            plan: Box::new(move |s| {
                let mut exprs = Vec::new();
                for x in &cols {
                    exprs.push(c(&ac(x), s)?);
                }
                let d = Distinct::new(
                    Box::new(Project::new(a.source(), exprs)),
                    Memory::unlimited(),
                );
                let order: Vec<(usize, bool)> = (0..cols.len()).map(|i| (i, false)).collect();
                sort_by(Box::new(d), &order, s)
            }),
        });
    }
    // Joins: nested loop and hash, inner and left.
    let join_keys = [
        ("k", "k"),
        ("g", "t"),
        ("n", "v"),
        ("s", "t"),
        ("id", "v"),
        ("k", "v"),
    ];
    for i in 0..48 {
        let (lk, rk) = *rng.pick(&join_keys);
        let left = rng.chance(1, 2);
        let hash = i % 2 == 1;
        let residual = rng.chance(1, 3);
        let width = a.cols.len();
        let on = bin(BinOp::Eq, ac(lk), b.col(rk, width));
        let extra = bin(BinOp::Lt, ac("id"), b.col("id", width));
        let on_full = if residual {
            bin(BinOp::And, on.clone(), extra.clone())
        } else {
            on.clone()
        };
        let sql = format!(
            "SELECT a.id, b.id, a.{lk}, b.{rk} FROM a {} JOIN b ON {} ORDER BY a.id, b.id",
            if left { "LEFT" } else { "INNER" },
            on_full.to_sql()
        );
        out.push(Case {
            area: match (hash, left) {
                (false, false) => "join-loop-inner",
                (false, true) => "join-loop-left",
                (true, false) => "join-hash-inner",
                (true, true) => "join-hash-left",
            },
            loose: 0,
            unordered: false,
            sql,
            plan: Box::new(move |s| {
                let kind = if left {
                    JoinKind::Left
                } else {
                    JoinKind::Inner
                };
                let joined: Box<dyn RowSource> = if hash {
                    let residual = if residual { Some(c(&extra, s)?) } else { None };
                    Box::new(HashJoin::new(
                        a.source(),
                        b.source(),
                        vec![c(&ac(lk), s)?],
                        vec![c(&bc(rk), s)?],
                        residual,
                        kind,
                        s,
                        Memory::unlimited(),
                    )?)
                } else {
                    let inner = VecSource::new(b.rows.clone(), b.types());
                    Box::new(NestedLoopJoin::new(
                        a.source(),
                        Box::new(inner),
                        Some(c(&on_full, s)?),
                        kind,
                    ))
                };
                let projected = Project::new(
                    joined,
                    vec![
                        c(&ac("id"), s)?,
                        c(&b.col("id", width), s)?,
                        c(&ac(lk), s)?,
                        c(&b.col(rk, width), s)?,
                    ],
                );
                sort_by(Box::new(projected), &[(0, false), (1, false)], s)
            }),
        });
    }
    // UNION and UNION ALL, with columns of different types.
    let union_cols = [
        ("k", "v"),
        ("g", "t"),
        ("n", "v"),
        ("s", "t"),
        ("k", "t"),
        ("f", "k"),
        ("d", "t"),
    ];
    for i in 0..24 {
        let (lc, rc) = *rng.pick(&union_cols);
        let all = i % 2 == 0;
        let p = rng.pick(&preds).clone();
        let sql = format!(
            "SELECT a.{lc} AS u FROM a WHERE {} UNION{} SELECT b.{rc} FROM b ORDER BY u",
            p.to_sql(),
            if all { " ALL" } else { "" }
        );
        out.push(Case {
            area: if all { "union-all" } else { "union" },
            loose: 1,
            unordered: true,
            sql,
            plan: Box::new(move |s| {
                let left = Project::new(
                    Box::new(Filter::new(a.source(), c(&p, s)?)),
                    vec![c(&ac(lc), s)?],
                );
                let right = Project::new(b.source(), vec![c(&bc(rc), s)?]);
                let u = Union::new(
                    vec![Box::new(left), Box::new(right)],
                    all,
                    s,
                    Memory::unlimited(),
                )?;
                sort_by(Box::new(u), &[(0, false)], s)
            }),
        });
    }
    out
}

#[ignore = "live: needs SPRIG_EXEC_ORACLE_SEED, SPRIG_EXEC_ORACLE_REPORT; tools/live/README.md"]
#[test]
fn operators_match_the_servers() {
    let oracles = Oracle::from_env();
    if oracles.is_empty() {
        eprintln!("no SPRIG_EXEC_ORACLE_* command set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    }
    let seed = std::env::var("SPRIG_EXEC_ORACLE_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7u64);
    let report_dir = std::env::var("SPRIG_EXEC_ORACLE_REPORT").ok();
    let mut summary = String::new();
    for oracle in &oracles {
        let (a, b) = tables(oracle.persona, seed);
        // The plans borrow the tables for the whole run.
        let a: &'static Table = Box::leak(Box::new(a));
        let b: &'static Table = Box::leak(Box::new(b));
        load(oracle, &[a, b]);
        let mut session = oracle.session();
        session.fold_constants = true;
        let mut rng = Rng::new(seed + 1);
        let mut all = cases(a, b, &mut rng);
        all.extend(more_aggregates(a, oracle.persona));
        let queries: Vec<String> = all.iter().map(|c| c.sql.clone()).collect();
        let answers = run_queries(oracle, &queries);
        let mut tally: BTreeMap<&'static str, (usize, usize)> = BTreeMap::new();
        let mut mismatches = String::new();
        for (case, theirs) in all.into_iter().zip(answers) {
            let entry = tally.entry(case.area).or_default();
            entry.0 += 1;
            let mut types: Vec<ExprType> = Vec::new();
            let mut mine: Result<Vec<Vec<u8>>, u16> = (case.plan)(&session)
                .and_then(|mut plan| {
                    let mut ctx = Ctx::new(&session);
                    types = plan.types();
                    let rows = collect(plan.as_mut(), &mut ctx)?;
                    Ok(render(&rows, &types, oracle.persona))
                })
                .map_err(|e| e.code);
            let mut theirs = theirs;
            if case.unordered {
                if let (Ok(m), Ok(t)) = (&mut mine, &mut theirs) {
                    m.sort();
                    t.sort();
                }
            }
            let same = match (&mine, &theirs) {
                (Ok(m), Ok(t)) => {
                    m.len() == t.len()
                        && m.iter().zip(t).all(|(x, y)| {
                            let (xs, ys): (Vec<&[u8]>, Vec<&[u8]>) = (
                                x.split(|&b| b == b'\t').collect(),
                                y.split(|&b| b == b'\t').collect(),
                            );
                            xs.len() == ys.len()
                                && xs.iter().zip(&ys).enumerate().all(|(i, (p, q))| {
                                    p == q
                                        || (i < case.loose
                                            && types.get(i).is_some_and(|ty| {
                                                ty.kind == sprig_exec::Kind::Str
                                                    && *p != b"NULL"
                                                    && *q != b"NULL"
                                                    && ty.coll.compare(p, q)
                                                        == std::cmp::Ordering::Equal
                                            }))
                                })
                        })
                }
                (a, b) => a == b,
            };
            if same {
                entry.1 += 1;
            } else {
                let show = |r: &Result<Vec<Vec<u8>>, u16>| match r {
                    Err(code) => format!("ERROR {code}"),
                    Ok(rows) => rows
                        .iter()
                        .map(|l| String::from_utf8_lossy(l).replace('\t', ","))
                        .collect::<Vec<_>>()
                        .join(" | "),
                };
                let _ = writeln!(
                    mismatches,
                    "[{}] {}\n    server: {}\n    engine: {}",
                    case.area,
                    case.sql,
                    show(&theirs),
                    show(&mine)
                );
            }
        }
        let _ = writeln!(
            summary,
            "{:<8} {:<18} {:>6} {:>6}",
            oracle.label, "area", "cases", "equal"
        );
        for (area, (total, ok)) in &tally {
            let _ = writeln!(
                summary,
                "{:<8} {:<18} {:>6} {:>6}",
                oracle.label, area, total, ok
            );
        }
        if let Some(dir) = &report_dir {
            let _ = std::fs::create_dir_all(dir);
            let _ = std::fs::write(
                format!("{dir}/ops-mismatch-{}.txt", oracle.label),
                &mismatches,
            );
        }
        drop_db(oracle, DB);
    }
    println!("{summary}");
    if let Some(dir) = &report_dir {
        let _ = std::fs::write(format!("{dir}/ops-summary.txt"), &summary);
    }
}
