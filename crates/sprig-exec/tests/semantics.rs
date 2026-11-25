//! Fixed facts about expressions and row operators, each one observed on
//! MariaDB 11.8 and MySQL 8.4 (the differential tests compare at scale;
//! these run without a server).

#![allow(
    clippy::unnecessary_box_returns,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc
)]

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Decimal, Value};
use sprig_exec::ops::{
    AggKind, Aggregate, Aggregation, Distinct, Filter, HashJoin, JoinKind, Limit, Memory,
    NestedLoopJoin, Project, Row, RowSource, Sort, SortKey, Union, VecSource, collect,
};
use sprig_exec::{
    BinOp, CastTarget, Ctx, Expr, ExprType, FieldType, Level, Persona, Session, compile, value_text,
};

fn b(e: Expr) -> Box<Expr> {
    Box::new(e)
}

fn i(v: i64) -> Expr {
    Expr::int(v)
}

fn dec(t: &str) -> Expr {
    Expr::Decimal(t.to_owned())
}

fn dbl(t: &str) -> Expr {
    Expr::Double(t.to_owned())
}

fn s(t: &str) -> Expr {
    Expr::str(t)
}

fn bin(op: BinOp, a: Expr, c: Expr) -> Expr {
    Expr::binary(op, a, c)
}

fn f(name: &str, args: Vec<Expr>) -> Expr {
    Expr::func(name, args)
}

/// The value's text (`NULL` for null, `ERROR n` for an error) and how many
/// conditions were raised.
fn run(e: &Expr, persona: Persona) -> (String, usize) {
    let session = Session::new(persona);
    let compiled = match compile(e, &session) {
        Ok(c) => c,
        Err(err) => return (format!("ERROR {}", err.code), 0),
    };
    let mut ctx = Ctx::new(&session);
    match compiled.eval_text(&mut ctx, &[]) {
        Ok(Some(v)) => (
            String::from_utf8_lossy(&v).into_owned(),
            ctx.warnings().len(),
        ),
        Ok(None) => ("NULL".to_owned(), ctx.warnings().len()),
        Err(err) => (format!("ERROR {}", err.code), ctx.warnings().len()),
    }
}

fn both(e: &Expr, want: &str) {
    assert_eq!(run(e, Persona::MariaDb).0, want, "MariaDB: {}", e.to_sql());
    assert_eq!(run(e, Persona::MySql).0, want, "MySQL: {}", e.to_sql());
}

fn each(e: &Expr, maria: &str, mysql: &str) {
    assert_eq!(run(e, Persona::MariaDb).0, maria, "MariaDB: {}", e.to_sql());
    assert_eq!(run(e, Persona::MySql).0, mysql, "MySQL: {}", e.to_sql());
}

#[test]
fn arithmetic_types_and_values() {
    both(&bin(BinOp::Add, i(1), i(1)), "2");
    both(&bin(BinOp::Div, i(1), i(3)), "0.3333");
    both(&bin(BinOp::Div, i(2), i(3)), "0.6667");
    // A quotient is cut at nine digits, not rounded.
    both(&bin(BinOp::Div, dec("2.00000"), i(3)), "0.666666666");
    both(&bin(BinOp::Mul, dec("1.5"), dec("1.25")), "1.875");
    both(&bin(BinOp::Add, s("1.5"), i(1)), "2.5");
    both(&bin(BinOp::IntDiv, i(-7), i(2)), "-3");
    both(&bin(BinOp::Mod, i(-7), i(2)), "-1");
    both(&bin(BinOp::Mod, dec("7.5"), i(2)), "1.5");
    // An intermediate quotient keeps its digits: (1/3)*3 is not 0.9999.
    both(
        &bin(BinOp::Mul, bin(BinOp::Div, i(1), i(3)), i(3)),
        "1.0000",
    );
    both(
        &bin(BinOp::Add, bin(BinOp::Div, i(1), i(3)), dbl("0e0")),
        "0.333333333",
    );
    // Overflow is an error, even in a SELECT.
    both(
        &bin(BinOp::Add, Expr::Int("9223372036854775807".into()), i(1)),
        "ERROR 1690",
    );
    both(
        &bin(BinOp::Sub, Expr::Cast(b(i(1)), CastTarget::Unsigned), i(2)),
        "ERROR 1690",
    );
    both(&bin(BinOp::Mul, dbl("1e308"), i(10)), "ERROR 1690");
    // Division by zero is NULL with warning 1365.
    for e in [
        bin(BinOp::Div, i(1), i(0)),
        bin(BinOp::IntDiv, i(1), i(0)),
        bin(BinOp::Mod, dec("1.5"), i(0)),
    ] {
        assert_eq!(run(&e, Persona::MariaDb), ("NULL".to_owned(), 1));
        assert_eq!(run(&e, Persona::MySql), ("NULL".to_owned(), 1));
    }
    let meta = |e: &Expr, p: Persona| compile(e, &Session::new(p)).unwrap().meta();
    let m = meta(&bin(BinOp::Add, i(1), i(1)), Persona::MariaDb);
    assert_eq!((m.field, m.len, m.not_null), (FieldType::Long, 3, true));
    let m = meta(&bin(BinOp::Add, i(1), i(1)), Persona::MySql);
    assert_eq!((m.field, m.len), (FieldType::LongLong, 3));
    let m = meta(&bin(BinOp::Div, dec("1.5"), dec("1.25")), Persona::MySql);
    assert_eq!(
        (m.field, m.len, m.dec, m.not_null),
        (FieldType::NewDecimal, 10, 5, false)
    );
}

#[test]
fn strings_as_numbers_warn() {
    let (v, w) = run(&bin(BinOp::Add, s("12abc"), i(1)), Persona::MariaDb);
    assert_eq!((v.as_str(), w), ("13", 1));
    // Trailing spaces: a note on MariaDB, nothing on MySQL.
    let session = Session::new(Persona::MariaDb);
    let c = compile(&bin(BinOp::Add, s(" 12 "), i(1)), &session).unwrap();
    let mut ctx = Ctx::new(&session);
    c.eval(&mut ctx, &[]).unwrap();
    assert_eq!(ctx.warnings()[0].level, Level::Note);
    assert_eq!(run(&bin(BinOp::Add, s(" 12 "), i(1)), Persona::MySql).1, 0);
    // An empty string: a warning on MariaDB only.
    assert_eq!(run(&bin(BinOp::Add, s(""), i(1)), Persona::MariaDb).1, 1);
    assert_eq!(run(&bin(BinOp::Add, s(""), i(1)), Persona::MySql).1, 0);
}

#[test]
fn comparison_types() {
    // An integer against a string: exact on MariaDB, doubles on MySQL.
    each(
        &bin(
            BinOp::Eq,
            Expr::Int("9007199254740993".into()),
            s("9007199254740992"),
        ),
        "0",
        "1",
    );
    both(&bin(BinOp::Eq, s("a"), s("A")), "1");
    both(
        &bin(
            BinOp::Eq,
            Expr::Collate(b(s("a")), Collation::Utf8mb4Bin),
            s("A"),
        ),
        "0",
    );
    both(&bin(BinOp::NullSafeEq, Expr::Null, Expr::Null), "1");
    both(&bin(BinOp::Eq, Expr::Null, i(1)), "NULL");
    both(&Expr::In(b(i(1)), vec![i(2), Expr::Null], false), "NULL");
    both(&Expr::In(b(i(1)), vec![i(2), Expr::Null], true), "NULL");
    both(&Expr::Between(b(i(1)), b(Expr::Null), b(i(0)), false), "0");
    both(&Expr::Like(b(s("abc")), b(s("A_C")), None, false), "1");
    both(
        &Expr::Like(b(s("a%")), b(s("a|%")), Some(b(s("|"))), false),
        "1",
    );
    // Two different explicit collations can't be compared.
    both(
        &bin(
            BinOp::Eq,
            Expr::Collate(b(s("a")), Collation::Utf8mb4Bin),
            Expr::Collate(b(s("a")), Collation::Utf8mb4GeneralCi),
        ),
        "ERROR 1267",
    );
    // MariaDB reads CAST(x AS CHAR) as a whole number against an integer.
    each(
        &bin(
            BinOp::Eq,
            i(1),
            Expr::Cast(b(s("1.4")), CastTarget::Char(None)),
        ),
        "1",
        "0",
    );
}

#[test]
fn logic_is_three_valued() {
    both(&bin(BinOp::And, i(0), Expr::Null), "0");
    both(&bin(BinOp::And, i(1), Expr::Null), "NULL");
    both(&bin(BinOp::Or, i(1), Expr::Null), "1");
    both(&bin(BinOp::Xor, i(1), Expr::Null), "NULL");
    both(&Expr::Not(b(Expr::Null)), "NULL");
    both(&Expr::IsNull(b(Expr::Null), false), "1");
}

#[test]
fn control_flow_follows_each_server() {
    // MySQL's IF hands a decimal branch through; MariaDB pads it.
    each(&f("IF", vec![i(1), i(2), dec("1.50")]), "2.00", "2");
    both(&f("COALESCE", vec![i(1), dec("2.50")]), "1.00");
    both(&f("IFNULL", vec![Expr::Null, s("x")]), "x");
    both(&f("NULLIF", vec![i(1), i(1)]), "NULL");
    // A string among numbers: doubles on MariaDB, strings on MySQL.
    each(
        &f("GREATEST", vec![i(100), s("ab%"), i(32767)]),
        "32767",
        "ab%",
    );
    // Equal under the collation: MySQL's GREATEST keeps the later one.
    each(&f("GREATEST", vec![s("abc"), s("aBc")]), "abc", "aBc");
    both(&f("LEAST", vec![s("abc"), s("aBc")]), "abc");
    both(&f("GREATEST", vec![i(1), Expr::Null]), "NULL");
    both(
        &Expr::Case {
            operand: Some(b(i(2))),
            whens: vec![(i(1), s("one")), (i(2), s("two"))],
            otherwise: None,
        },
        "two",
    );
}

#[test]
fn casts() {
    both(&Expr::Cast(b(dec("1.5")), CastTarget::Signed), "2");
    both(&Expr::Cast(b(dbl("2.5e0")), CastTarget::Signed), "2");
    both(&Expr::Cast(b(s("12.7")), CastTarget::Signed), "12");
    both(
        &Expr::Cast(b(dec("123456.789")), CastTarget::Decimal(5, 2)),
        "999.99",
    );
    both(&Expr::Cast(b(s("abc")), CastTarget::Char(Some(2))), "ab");
    each(
        &Expr::Cast(b(dec("-1.5")), CastTarget::Unsigned),
        "0",
        "18446744073709551614",
    );
    both(&Expr::Cast(b(dbl("1e15")), CastTarget::Char(None)), "1e15");
    both(
        &Expr::Cast(b(dbl("1e14")), CastTarget::Char(None)),
        "100000000000000",
    );
    both(&Expr::Cast(b(s("2024-02-30")), CastTarget::Date), "NULL");
    both(
        &Expr::Cast(b(s("2024-1-5 3:4:5")), CastTarget::DateTime(0)),
        "2024-01-05 03:04:05",
    );
}

#[test]
fn bit_operators_differ() {
    both(&bin(BinOp::BitOr, dec("1.5"), i(0)), "2");
    each(&bin(BinOp::BitOr, dbl("2.5e0"), i(0)), "3", "2");
    each(&bin(BinOp::BitOr, s("1.50"), i(0)), "2", "1");
    each(
        &bin(BinOp::Shl, i(1), Expr::Int("4294967296".into())),
        "1",
        "0",
    );
    both(&bin(BinOp::Shr, i(-1), i(1)), "9223372036854775807");
}

#[test]
fn string_and_numeric_functions() {
    both(&f("CONCAT", vec![s("a"), i(1), dec("1.50")]), "a11.50");
    both(&f("CONCAT", vec![s("a"), Expr::Null]), "NULL");
    both(
        &f("CONCAT_WS", vec![s("-"), s("a"), Expr::Null, s("b")]),
        "a-b",
    );
    both(&f("CHAR_LENGTH", vec![s("h\u{e9}llo")]), "5");
    both(&f("LENGTH", vec![s("h\u{e9}llo")]), "6");
    both(&f("UPPER", vec![s("stra\u{df}e")]), "STRA\u{df}E");
    both(&f("SUBSTRING", vec![s("abcdef"), i(-3), i(2)]), "de");
    both(&f("SUBSTRING", vec![s("abcdef"), i(0)]), "");
    both(&f("LEFT", vec![s("abc"), i(-1)]), "");
    both(&f("LPAD", vec![s("5"), i(3), s("0")]), "005");
    both(&f("LPAD", vec![s("5"), i(-1), s("0")]), "NULL");
    both(&f("LOCATE", vec![s("B"), s("abc")]), "2");
    both(&f("REPLACE", vec![s("aAa"), s("a"), s("x")]), "xAx");
    both(&f("TRIM_BOTH", vec![s("x"), s("xxaxx")]), "a");
    both(&f("REPEAT", vec![s("ab"), i(0)]), "");
    // A count no string could reach: NULL with warning 1301, no overflow.
    let huge = f(
        "REPEAT",
        vec![s("ab"), Expr::Int("9223372036854775807".into())],
    );
    assert_eq!(run(&huge, Persona::MariaDb), ("NULL".to_owned(), 1));
    both(&f("ROUND", vec![dec("2.5")]), "3");
    both(&f("ROUND", vec![dbl("2.5e0")]), "2");
    both(&f("TRUNCATE", vec![dec("-2.57"), i(1)]), "-2.5");
    // MariaDB gives the asked scale; MySQL never more than the argument's.
    each(&f("ROUND", vec![dec("1.00"), i(4)]), "1.0000", "1.00");
    both(&f("ABS", vec![i(-5)]), "5");
    both(&f("SQRT", vec![i(-1)]), "NULL");
    both(&f("CEIL", vec![dec("1.2")]), "2");
    both(&f("FLOOR", vec![dec("-1.2")]), "-2");
    each(&f("COERCIBILITY", vec![s("a")]), "6", "4");
}

#[test]
fn date_functions() {
    both(&f("YEAR", vec![s("2024-02-29")]), "2024");
    both(&f("DAYOFWEEK", vec![s("2024-02-29")]), "5");
    both(&f("TO_DAYS", vec![s("1970-01-01")]), "719528");
    both(
        &f(
            "DATE_ADD",
            vec![s("2024-01-31"), i(1), Expr::Keyword("MONTH".into())],
        ),
        "2024-02-29",
    );
    both(
        &f(
            "DATE_SUB",
            vec![
                Expr::Cast(b(s("2024-03-01")), CastTarget::Date),
                i(1),
                Expr::Keyword("DAY".into()),
            ],
        ),
        "2024-02-29",
    );
    both(
        &f("DATEDIFF", vec![s("2024-03-01"), s("2024-02-01 23:00:00")]),
        "29",
    );
    both(
        &f(
            "DATE_FORMAT",
            vec![s("2024-01-05 15:04:05"), s("%W %D %M %Y %h:%i %p")],
        ),
        "Friday 5th January 2024 03:04 PM",
    );
    both(&f("LAST_DAY", vec![s("2023-02-10")]), "2023-02-28");
    // The clock and the zone are inputs.
    let mut session = Session::new(Persona::MySql);
    session.now_seconds = 1_700_000_000;
    session.now_micros = 123_456;
    session.time_zone_offset = 19_800;
    let text = |e: &Expr| {
        let c = compile(e, &session).unwrap();
        let mut ctx = Ctx::new(&session);
        String::from_utf8_lossy(&c.eval_text(&mut ctx, &[]).unwrap().unwrap()).into_owned()
    };
    assert_eq!(text(&f("NOW", vec![i(3)])), "2023-11-15 03:43:20.123");
    assert_eq!(text(&f("UTC_TIMESTAMP", vec![])), "2023-11-14 22:13:20");
    assert_eq!(text(&f("FROM_UNIXTIME", vec![i(0)])), "1970-01-01 05:30:00");
    assert_eq!(
        text(&f("UNIX_TIMESTAMP", vec![s("1970-01-02 05:30:00")])),
        "86400"
    );
}

fn int_col(slot: usize, name: &str) -> Expr {
    Expr::column(slot, name, ColumnType::int(false), true)
}

fn str_col(slot: usize, name: &str) -> Expr {
    Expr::column(
        slot,
        name,
        ColumnType::VarChar {
            chars: 10,
            collation: Collation::Utf8mb4GeneralCi,
        },
        true,
    )
}

fn table() -> (Vec<Row>, Vec<ExprType>) {
    let rows = vec![
        vec![Value::Int(1), Value::str("b"), Value::Int(10)],
        vec![Value::Int(2), Value::str("A"), Value::Int(20)],
        vec![Value::Int(3), Value::Null, Value::Int(30)],
        vec![Value::Int(4), Value::str("a"), Value::Null],
        vec![Value::Int(5), Value::str("B"), Value::Int(50)],
    ];
    let types = vec![
        ExprType::of_column(&ColumnType::int(false), false),
        ExprType::of_column(
            &ColumnType::VarChar {
                chars: 10,
                collation: Collation::Utf8mb4GeneralCi,
            },
            true,
        ),
        ExprType::of_column(&ColumnType::int(false), true),
    ];
    (rows, types)
}

fn source() -> Box<dyn RowSource> {
    let (rows, types) = table();
    Box::new(VecSource::new(rows, types))
}

fn texts(plan: &mut dyn RowSource, session: &Session) -> Vec<String> {
    let mut ctx = Ctx::new(session);
    let types = plan.types();
    collect(plan, &mut ctx)
        .unwrap()
        .iter()
        .map(|row| {
            row.iter()
                .zip(&types)
                .map(|(v, ty)| {
                    value_text(v, ty, session.persona).map_or("NULL".to_owned(), |b| {
                        String::from_utf8_lossy(&b).into_owned()
                    })
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

#[test]
fn filter_project_sort_limit() {
    let session = Session::new(Persona::MariaDb);
    let c = |e: &Expr| compile(e, &session).unwrap();
    let filter = Filter::new(source(), c(&bin(BinOp::Gt, int_col(2, "n"), i(10))));
    let mut plan = Project::new(
        Box::new(filter),
        vec![
            c(&int_col(0, "id")),
            c(&bin(BinOp::Mul, int_col(2, "n"), i(2))),
        ],
    );
    assert_eq!(texts(&mut plan, &session), ["2,40", "3,60", "5,100"]);
    // NULL sorts first ascending; equal keys keep their input order.
    let key = |desc| SortKey {
        expr: c(&str_col(1, "g")),
        desc,
    };
    let mut plan = Project::new(
        Box::new(Sort::new(source(), vec![key(false)], Memory::unlimited())),
        vec![c(&int_col(0, "id"))],
    );
    assert_eq!(texts(&mut plan, &session), ["3", "2", "4", "1", "5"]);
    let mut plan = Project::new(
        Box::new(Limit::new(
            Box::new(Sort::top_n(
                source(),
                vec![key(true)],
                3,
                Memory::unlimited(),
            )),
            1,
            2,
        )),
        vec![c(&int_col(0, "id"))],
    );
    assert_eq!(texts(&mut plan, &session), ["5", "2"]);
    // A sort charges what it keeps, and a limit on memory is an error.
    let memory = Memory::with_limit(64);
    let mut plan = Sort::new(source(), vec![key(false)], memory.clone());
    let mut ctx = Ctx::new(&session);
    assert_eq!(collect(&mut plan, &mut ctx).unwrap_err().code, 1038);
    assert!(memory.peak() > 64);
}

#[test]
fn aggregation_distinct_and_union() {
    let session = Session::new(Persona::MySql);
    let c = |e: &Expr| compile(e, &session).unwrap();
    let aggs = || {
        vec![
            Aggregate::new(AggKind::CountStar, None, false, &session).unwrap(),
            Aggregate::new(AggKind::Sum, Some(c(&int_col(2, "n"))), false, &session).unwrap(),
            Aggregate::new(AggKind::Avg, Some(c(&int_col(2, "n"))), false, &session).unwrap(),
            Aggregate::new(
                AggKind::GroupConcat,
                Some(c(&int_col(0, "id"))),
                false,
                &session,
            )
            .unwrap()
            .separator(b"|"),
        ]
    };
    // Groups compare by the collation: a and A are one group.
    let mut plan = Aggregation::hash(
        source(),
        vec![c(&str_col(1, "g"))],
        aggs(),
        Memory::unlimited(),
    );
    assert_eq!(
        texts(&mut plan, &session),
        [
            "b,2,60,30.0000,1|5",
            "A,2,20,20.0000,2|4",
            "NULL,1,30,30.0000,3"
        ]
    );
    let sorted = Sort::new(
        source(),
        vec![SortKey {
            expr: c(&str_col(1, "g")),
            desc: false,
        }],
        Memory::unlimited(),
    );
    let mut plan = Aggregation::streaming(Box::new(sorted), vec![c(&str_col(1, "g"))], aggs());
    assert_eq!(
        texts(&mut plan, &session),
        [
            "NULL,1,30,30.0000,3",
            "A,2,20,20.0000,2|4",
            "b,2,60,30.0000,1|5"
        ]
    );
    // No rows and no GROUP BY: one row.
    let empty = Filter::new(source(), c(&Expr::Bool(false)));
    let mut plan = Aggregation::hash(Box::new(empty), vec![], aggs(), Memory::unlimited());
    assert_eq!(texts(&mut plan, &session), ["0,NULL,NULL,NULL"]);
    let mut plan = Distinct::new(
        Box::new(Project::new(source(), vec![c(&str_col(1, "g"))])),
        Memory::unlimited(),
    );
    assert_eq!(texts(&mut plan, &session), ["b", "A", "NULL"]);
    // UNION merges the column types: an integer with a decimal.
    let dec_rows = vec![vec![Value::Decimal(Decimal::parse("10.50").unwrap())]];
    let dec_ty = ExprType::of_column(
        &ColumnType::Decimal {
            precision: 6,
            scale: 2,
        },
        true,
    );
    let left = Project::new(source(), vec![c(&int_col(2, "n"))]);
    let right = VecSource::new(dec_rows, vec![dec_ty]);
    let mut plan = Union::new(
        vec![Box::new(left), Box::new(right)],
        false,
        &session,
        Memory::unlimited(),
    )
    .unwrap();
    assert_eq!(
        texts(&mut plan, &session),
        ["10.00", "20.00", "30.00", "NULL", "50.00", "10.50"]
    );
}

#[test]
fn joins() {
    let session = Session::new(Persona::MariaDb);
    let c = |e: &Expr| compile(e, &session).unwrap();
    let other = || {
        let rows = vec![
            vec![Value::str("a"), Value::Int(100)],
            vec![Value::str("B"), Value::Int(200)],
            vec![Value::Null, Value::Int(300)],
        ];
        let (_, types) = table();
        VecSource::new(rows, vec![types[1], types[2]])
    };
    let on = bin(BinOp::Eq, str_col(1, "g"), str_col(3, "h"));
    let out = |plan: Box<dyn RowSource>| {
        let mut p = Project::new(plan, vec![c(&int_col(0, "id")), c(&int_col(4, "m"))]);
        texts(&mut p, &session)
    };
    let inner = ["1,200", "2,100", "4,100", "5,200"];
    let left = ["1,200", "2,100", "3,NULL", "4,100", "5,200"];
    for (kind, want) in [(JoinKind::Inner, &inner[..]), (JoinKind::Left, &left[..])] {
        let nl = NestedLoopJoin::new(source(), Box::new(other()), Some(c(&on)), kind);
        assert_eq!(out(Box::new(nl)), want);
        let hj = HashJoin::new(
            source(),
            Box::new(other()),
            vec![c(&str_col(1, "g"))],
            vec![c(&str_col(0, "h"))],
            None,
            kind,
            &session,
            Memory::unlimited(),
        )
        .unwrap();
        assert_eq!(out(Box::new(hj)), want);
    }
}

fn js(t: &str) -> Expr {
    f("JSON_EXTRACT", vec![s(t), s("$")])
}

fn mysql(e: &Expr) -> String {
    run(e, Persona::MySql).0
}

#[test]
fn json_values_are_a_type_on_mysql_and_text_on_mariadb() {
    // MySQL's cast, and the JSON_COMPACT its port's rewrite makes of it.
    assert_eq!(
        mysql(&Expr::Cast(
            b(s(r#"{"b":1,"a":[1, 2.50]}"#)),
            CastTarget::Json
        )),
        r#"{"a": [1, 2.5], "b": 1}"#
    );
    assert_eq!(mysql(&Expr::Cast(b(i(7)), CastTarget::Json)), "7");
    assert_eq!(
        mysql(&Expr::Cast(b(s("nope")), CastTarget::Json)),
        "ERROR 3141"
    );
    each(
        &f("JSON_COMPACT", vec![s(r#"{ "b" : 1, "a" : [1, 2] }"#)]),
        r#"{"b":1,"a":[1,2]}"#,
        r#"{"a": [1, 2], "b": 1}"#,
    );
    // MySQL compares JSON by type, then value: numbers < strings <
    // objects < arrays < booleans; MariaDB compares the text.
    each(&bin(BinOp::Lt, js("12.5"), js("100")), "0", "1");
    each(&bin(BinOp::Gt, js("[1,2]"), js(r#"{"a":1}"#)), "0", "1");
    each(&bin(BinOp::Gt, js("true"), js("[9]")), "1", "1");
    each(&bin(BinOp::Lt, js(r#""b""#), js(r#"{"a":1}"#)), "1", "1");
    each(&bin(BinOp::Eq, js("1.0"), js("1")), "0", "1");
    // A non-JSON operand is a JSON scalar: 'b' is the string "b".
    each(&bin(BinOp::Eq, js(r#""b""#), s("b")), "0", "1");
    each(&bin(BinOp::Lt, js(r#""b""#), i(50)), "1", "0");
}

#[test]
fn json_value_returning_converts_or_gives_null() {
    let jv = |doc: &str, ret: &str| {
        f(
            "JSON_VALUE",
            vec![s(doc), s("$.a"), Expr::Keyword(format!("RETURNING {ret}"))],
        )
    };
    assert_eq!(mysql(&jv(r#"{"a":12.7}"#, "SIGNED")), "13");
    assert_eq!(mysql(&jv(r#"{"a":"12abc"}"#, "SIGNED")), "NULL");
    assert_eq!(mysql(&jv(r#"{"a":[1]}"#, "SIGNED")), "NULL");
    assert_eq!(mysql(&jv(r#"{"a":true}"#, "SIGNED")), "1");
    assert_eq!(mysql(&jv(r#"{"a":-3}"#, "UNSIGNED")), "NULL");
    assert_eq!(
        mysql(&jv(r#"{"a":99999999999999999999}"#, "SIGNED")),
        "NULL"
    );
    assert_eq!(mysql(&jv(r#"{"a":12.756}"#, "DECIMAL(5,2)")), "12.76");
    assert_eq!(mysql(&jv(r#"{"a":"1.5"}"#, "DECIMAL")), "2");
    assert_eq!(mysql(&jv(r#"{"a":1.5}"#, "DOUBLE")), "1.5");
    assert_eq!(mysql(&jv(r#"{"a":"abcdef"}"#, "CHAR(3)")), "NULL");
    assert_eq!(mysql(&jv(r#"{"a":"abc"}"#, "CHAR(3)")), "abc");
    assert_eq!(mysql(&jv(r#"{"a":"2024-01-02"}"#, "DATE")), "2024-01-02");
    assert_eq!(mysql(&jv(r#"{"a":"x"}"#, "DATE")), "NULL");
    assert_eq!(mysql(&jv(r#"{"a":1.5}"#, "YEAR")), "NULL");
    assert_eq!(mysql(&jv(r#"{"a":{"b":1}}"#, "JSON")), r#"{"b": 1}"#);
    assert_eq!(mysql(&jv(r#"{"a":null}"#, "SIGNED")), "NULL");
}

#[test]
fn json_columns_sort_group_and_type_as_mysql_does() {
    let ty = ExprType::of_column(&ColumnType::Json, true);
    assert!(ty.json);
    assert_eq!(ty.meta(Persona::MySql).field, FieldType::Json);
    assert_ne!(ty.meta(Persona::MariaDb).field, FieldType::Json);
    let rows: Vec<Row> = [
        "true",
        "[1,2]",
        r#"{"a":1}"#,
        r#""b""#,
        "100.25",
        "12.5",
        "null",
    ]
    .iter()
    .map(|t| vec![Value::Json(bytes::Bytes::copy_from_slice(t.as_bytes()))])
    .collect();
    let col = Expr::column(0, "j", ColumnType::Json, true);
    let sorted = |persona| {
        let session = Session::new(persona);
        let key = SortKey {
            expr: compile(&col, &session).unwrap(),
            desc: false,
        };
        let src = Box::new(VecSource::new(rows.clone(), vec![ty]));
        texts(
            &mut Sort::new(src, vec![key], Memory::unlimited()),
            &session,
        )
    };
    assert_eq!(
        sorted(Persona::MySql),
        [
            "null",
            "12.5",
            "100.25",
            "\"b\"",
            "{\"a\": 1}",
            "[1, 2]",
            "true"
        ]
    );
    assert_eq!(
        sorted(Persona::MariaDb),
        [
            "\"b\"",
            "100.25",
            "12.5",
            "[1,2]",
            "null",
            "true",
            "{\"a\":1}"
        ]
    );
}

#[test]
fn json_pretty_and_json_search_per_server() {
    each(
        &f(
            "JSON_PRETTY",
            vec![s(r#"{"b":[1,{"c":2},[]],"a":{},"d":"x"}"#)],
        ),
        "{\n    \"b\": \n    [\n        1,\n        {\n            \"c\": 2\n        },\n        []\n    ],\n    \"a\": \n    {\n    },\n    \"d\": \"x\"\n}",
        "{\n  \"a\": {},\n  \"b\": [\n    1,\n    {\n      \"c\": 2\n    },\n    []\n  ],\n  \"d\": \"x\"\n}",
    );
    each(&f("JSON_PRETTY", vec![s("[1]")]), "[1]", "[\n  1\n]");
    each(&f("JSON_PRETTY", vec![s("{}")]), "{\n}", "{}");
    let doc = r#"{"b":["Abc",{"c":"abc"}],"a":"abc","x y":"abd"}"#;
    each(
        &f("JSON_SEARCH", vec![s(doc), s("all"), s("ab%")]),
        r#"["$.b[0]", "$.b[1].c", "$.a", "$.x y"]"#,
        r#"["$.a", "$.b[1].c", "$.\"x y\""]"#,
    );
    each(
        &f("JSON_SEARCH", vec![s(doc), s("one"), s("abc")]),
        r#""$.b[0]""#,
        r#""$.a""#,
    );
    both(
        &f(
            "JSON_SEARCH",
            vec![s(r#"{"a":"a_c","b":"abc"}"#), s("all"), s("a|_c"), s("|")],
        ),
        r#""$.a""#,
    );
    both(
        &f(
            "JSON_SEARCH",
            vec![
                s(r#"{"a":["x","x"]}"#),
                s("all"),
                s("x"),
                Expr::Null,
                s("$.a[*]"),
            ],
        ),
        r#"["$.a[0]", "$.a[1]"]"#,
    );
    each(
        &f("JSON_SEARCH", vec![s(r#"{"a":1}"#), s("all"), s("1")]),
        r#""$.a""#,
        "NULL",
    );
    each(
        &f("JSON_SEARCH", vec![s(r#"{"a":"x"}"#), s("any"), s("x")]),
        "NULL",
        "ERROR 3154",
    );
}
