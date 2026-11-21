//! The generated tables the planner's tests run on: thirteen tables per
//! seed, with and without primary keys, with unique, composite, prefix
//! and descending indexes, over integers, decimals, doubles, dates and
//! strings of three collations. `NULL`s, duplicates and unmatched join
//! keys are put in on purpose.

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, Value};
use sprig_exec::Persona;

use super::{Rng, Spec, col, col_nn, default_collation, text_col};

/// Strings that tell the collations apart: case, accents, a trailing
/// space, `ß` against `ss`.
pub const STRINGS: &[&str] = &[
    "a", "A", "b", "B", "ab", "Ab", "abc", "abd", "ABC", "\u{e1}", "\u{c1}", "e", "\u{e9}", "x",
    "x ", "z", "Z", "", "m", "M", "ss", "\u{df}", "s", "ac", "b ",
];

pub fn int(v: i64) -> Value {
    Value::Int(v)
}

pub fn dec(units: i64, scale: u8) -> Value {
    // `units` hundredths (or thousandths) as an exact decimal.
    let neg = units < 0;
    let abs = units.unsigned_abs();
    let pow = 10u64.pow(u32::from(scale));
    let text = format!(
        "{}{}.{:0width$}",
        if neg { "-" } else { "" },
        abs / pow,
        abs % pow,
        width = usize::from(scale)
    );
    Value::Decimal(Decimal::parse(&text).unwrap())
}

pub fn date(day: i64) -> Date {
    // Day 0 is 2023-12-20; the range crosses a year end and a leap day.
    let mut y = 2023u16;
    let mut m = 12u8;
    let mut d = 20 + day;
    loop {
        let len = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if y % 4 == 0 {
                    29
                } else {
                    28
                }
            }
        };
        if d <= len {
            break;
        }
        d -= len;
        m += 1;
        if m > 12 {
            m = 1;
            y += 1;
        }
    }
    Date::new(y, m, d as u8)
}

/// The value, or `NULL` one time in `$n`.
macro_rules! maybe {
    ($rng:expr, $v:expr, $n:expr $(,)?) => {{
        let v = $v;
        if $rng.chance(1, $n) { Value::Null } else { v }
    }};
}

/// The tables of one seed.
pub fn world(persona: Persona, seed: u64) -> Vec<Spec> {
    let mut rng = Rng::new(seed);
    let dflt = default_collation(persona);
    let vc = |chars: u32, collation: Collation| ColumnType::VarChar { chars, collation };
    let d102 = ColumnType::Decimal {
        precision: 10,
        scale: 2,
    };
    let mut out = Vec::new();

    // t1: a single-column primary key and two secondary indexes.
    let mut rows = Vec::new();
    for id in 1..=90i64 {
        rows.push(vec![
            int(id),
            maybe!(rng, int(rng.below(12) as i64), 8),
            maybe!(rng, Value::str(*rng.pick(STRINGS)), 9),
            maybe!(rng, dec(rng.below(100) as i64 * 25 - 500, 2), 9),
            maybe!(rng, Value::Date(date(rng.below(80) as i64)), 9),
            maybe!(
                rng,
                Value::Double([0.5, 1.5, -2.25, 1024.0, 0.125, 3.0, -0.5][rng.below(7)]),
                9,
            ),
        ]);
    }
    out.push(Spec {
        name: "t1",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("k", "INT", ColumnType::int(false)),
            text_col("v", 20, dflt),
            col("d", "DECIMAL(10,2)", d102),
            col("dt", "DATE", ColumnType::Date),
            col("f", "DOUBLE", ColumnType::Double),
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("k_k", false, vec![("k", None, false)]),
            ("k_dt", false, vec![("dt", None, false)]),
        ],
        rows,
    });

    // t2: a composite primary key and a composite secondary index.
    let mut rows = Vec::new();
    for a in 0..8i64 {
        for b in 0..14i64 {
            if rng.chance(1, 4) {
                continue;
            }
            rows.push(vec![
                int(a),
                int(b * 3 - 6),
                maybe!(rng, int(rng.below(6) as i64), 7),
                maybe!(rng, Value::str(*rng.pick(STRINGS)), 7),
            ]);
        }
    }
    out.push(Spec {
        name: "t2",
        cols: vec![
            col_nn("a", "INT", ColumnType::int(false)),
            col_nn("b", "INT", ColumnType::int(false)),
            col("c", "INT", ColumnType::int(false)),
            text_col("s", 10, Collation::Utf8mb4GeneralCi),
        ],
        pk: vec![("a", None, false), ("b", None, false)],
        keys: vec![("k_cs", false, vec![("c", None, false), ("s", None, false)])],
        rows,
    });

    // t3: no key at all, with duplicate rows.
    let mut rows = Vec::new();
    for _ in 0..50 {
        rows.push(vec![
            maybe!(rng, int(rng.below(6) as i64), 6),
            maybe!(rng, Value::str(*rng.pick(&STRINGS[..8])), 6),
            maybe!(rng, int(rng.below(4) as i64 * 10), 6),
        ]);
    }
    out.push(Spec {
        name: "t3",
        cols: vec![
            col("x", "INT", ColumnType::int(false)),
            text_col("y", 10, dflt),
            col("z", "INT", ColumnType::int(false)),
        ],
        pk: vec![],
        keys: vec![],
        rows,
    });

    // t4: secondary indexes without a primary key.
    let mut rows = Vec::new();
    for _ in 0..60 {
        rows.push(vec![
            maybe!(rng, int(rng.below(9) as i64), 7),
            maybe!(rng, int(rng.below(5) as i64), 7),
            maybe!(rng, Value::str(*rng.pick(STRINGS)), 7),
        ]);
    }
    out.push(Spec {
        name: "t4",
        cols: vec![
            col("x", "INT", ColumnType::int(false)),
            col("y", "INT", ColumnType::int(false)),
            text_col("z", 8, Collation::Utf8mb4Bin),
        ],
        pk: vec![],
        keys: vec![
            ("k_x", false, vec![("x", None, false)]),
            ("k_xy", false, vec![("x", None, false), ("y", None, false)]),
        ],
        rows,
    });

    // t5: unique keys, one of them over a nullable column.
    let mut rows = Vec::new();
    for id in 1..=60i64 {
        rows.push(vec![
            int(id),
            int(1000 - id * 7),
            if id % 5 == 0 {
                Value::Null
            } else {
                int(id * 2)
            },
            Value::str(&format!("c{:03}", (id * 37) % 211)),
        ]);
    }
    out.push(Spec {
        name: "t5",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col_nn("u", "INT", ColumnType::int(false)),
            col("un", "INT", ColumnType::int(false)),
            {
                let mut c = text_col("code", 8, Collation::Utf8mb4Bin);
                c.sql.push_str(" NOT NULL");
                c.nullable = false;
                c
            },
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("uq_u", true, vec![("u", None, false)]),
            ("uq_un", true, vec![("un", None, false)]),
            ("uq_code", true, vec![("code", None, false)]),
        ],
        rows,
    });

    // t6: strings under three collations, a binary string, a prefix index.
    let mut rows = Vec::new();
    for id in 1..=70i64 {
        let s = |rng: &mut Rng| maybe!(rng, Value::str(*rng.pick(STRINGS)), 9);
        let (a, b, c, e) = (s(&mut rng), s(&mut rng), s(&mut rng), s(&mut rng));
        let bin = maybe!(rng, Value::bytes(rng.pick(STRINGS).as_bytes()), 9);
        rows.push(vec![int(id), a, b, c, bin, e]);
    }
    out.push(Spec {
        name: "t6",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            text_col("sb", 12, Collation::Utf8mb4Bin),
            text_col("sg", 12, Collation::Utf8mb4GeneralCi),
            text_col("sd", 12, dflt),
            col("vb", "VARBINARY(12)", ColumnType::VarBinary { bytes: 12 }),
            text_col("sp", 12, Collation::Utf8mb4GeneralCi),
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("k_sb", false, vec![("sb", None, false)]),
            ("k_sg", false, vec![("sg", None, false)]),
            ("k_sd", false, vec![("sd", None, false)]),
            ("k_vb", false, vec![("vb", None, false)]),
            ("k_sp", false, vec![("sp", Some(2), false)]),
        ],
        rows,
    });

    // t7: descending key parts.
    let mut rows = Vec::new();
    for id in 1..=80i64 {
        rows.push(vec![
            int(id),
            maybe!(rng, int(rng.below(6) as i64), 8),
            maybe!(rng, int(rng.below(10) as i64), 8),
            maybe!(rng, int(rng.below(30) as i64), 8),
        ]);
    }
    out.push(Spec {
        name: "t7",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("a", "INT", ColumnType::int(false)),
            col("b", "INT", ColumnType::int(false)),
            col("c", "INT", ColumnType::int(false)),
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("k_ab", false, vec![("a", None, false), ("b", None, true)]),
            ("k_c", false, vec![("c", None, true)]),
        ],
        rows,
    });

    // t8: one indexed column per numeric and temporal type, with the
    // types' extreme values.
    let mut rows = Vec::new();
    let tinies = [-128i64, -1, 0, 1, 2, 3, 5, 100, 127];
    let bigs = [
        i64::MIN,
        -9_007_199_254_740_993,
        -1,
        0,
        1,
        7,
        9_007_199_254_740_992,
        9_007_199_254_740_993,
        i64::MAX,
    ];
    let uis = [0u64, 1, 2, 7, 100, 2_147_483_648, 4_294_967_295];
    for id in 1..=70i64 {
        let day = rng.below(60) as i64;
        rows.push(vec![
            int(id * 1_000_000_007),
            maybe!(rng, int(*rng.pick(&tinies)), 8),
            maybe!(rng, Value::UInt(*rng.pick(&uis)), 8),
            maybe!(rng, int(*rng.pick(&bigs)), 8),
            maybe!(rng, dec(rng.below(41) as i64 * 125 - 2500, 3), 8),
            maybe!(
                rng,
                Value::Double([-1e10, -2.5, -0.0, 0.0, 0.25, 1.0, 2.5, 1e15][rng.below(8)]),
                8,
            ),
            maybe!(rng, Value::Date(date(day)), 8),
            maybe!(
                rng,
                Value::DateTime(DateTime::new(
                    date(day),
                    rng.below(3) as u8 * 11,
                    rng.below(2) as u8 * 30,
                    0,
                    0,
                )),
                8,
            ),
        ]);
    }
    out.push(Spec {
        name: "t8",
        cols: vec![
            col_nn("id", "BIGINT", ColumnType::bigint(false)),
            col("ti", "TINYINT", ColumnType::tinyint(false)),
            col("ui", "INT UNSIGNED", ColumnType::int(true)),
            col("bi", "BIGINT", ColumnType::bigint(false)),
            col(
                "de",
                "DECIMAL(8,3)",
                ColumnType::Decimal {
                    precision: 8,
                    scale: 3,
                },
            ),
            col("dd", "DOUBLE", ColumnType::Double),
            col("dy", "DATE", ColumnType::Date),
            col("dtm", "DATETIME", ColumnType::DateTime { fsp: 0 }),
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("k_ti", false, vec![("ti", None, false)]),
            ("k_ui", false, vec![("ui", None, false)]),
            ("k_bi", false, vec![("bi", None, false)]),
            ("k_de", false, vec![("de", None, false)]),
            ("k_dd", false, vec![("dd", None, false)]),
            ("k_dy", false, vec![("dy", None, false)]),
            ("k_dtm", false, vec![("dtm", None, false)]),
        ],
        rows,
    });

    // par, chi, chn: a parent and two child tables; chi has an index on
    // the parent id, chn has none. Some children have no parent.
    let mut rows = Vec::new();
    for id in 1..=20i64 {
        rows.push(vec![
            int(id),
            maybe!(rng, Value::str(*rng.pick(STRINGS)), 8),
            maybe!(rng, int(rng.below(4) as i64), 8),
        ]);
    }
    out.push(Spec {
        name: "par",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            text_col("name", 10, dflt),
            col("grp", "INT", ColumnType::int(false)),
        ],
        pk: vec![("id", None, false)],
        keys: vec![("k_grp", false, vec![("grp", None, false)])],
        rows,
    });
    let mut rows = Vec::new();
    for id in 1..=90i64 {
        rows.push(vec![
            int(id),
            maybe!(rng, int(1 + rng.below(25) as i64), 9),
            maybe!(rng, dec(rng.below(60) as i64 * 50 - 300, 2), 9),
            maybe!(rng, Value::str(*rng.pick(&STRINGS[..6])), 9),
        ]);
    }
    out.push(Spec {
        name: "chi",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("par_id", "INT", ColumnType::int(false)),
            col("amt", "DECIMAL(10,2)", d102),
            text_col("tag", 5, Collation::Utf8mb4GeneralCi),
        ],
        pk: vec![("id", None, false)],
        keys: vec![("k_par", false, vec![("par_id", None, false)])],
        rows,
    });
    let mut rows = Vec::new();
    for id in 1..=70i64 {
        rows.push(vec![
            int(id),
            maybe!(rng, int(1 + rng.below(25) as i64), 9),
            maybe!(rng, int(rng.below(9) as i64), 9),
        ]);
    }
    out.push(Spec {
        name: "chn",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("par_id", "INT", ColumnType::int(false)),
            col("qty", "INT", ColumnType::int(false)),
        ],
        pk: vec![("id", None, false)],
        keys: vec![],
        rows,
    });

    // big: enough rows for ranges, limits and estimates to matter.
    let mut rows = Vec::new();
    for id in 1..=500i64 {
        rows.push(vec![
            int(id),
            int(rng.below(20) as i64),
            int(rng.below(50) as i64),
            Value::str(&format!("p{}", rng.below(1000))),
        ]);
    }
    out.push(Spec {
        name: "big",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col_nn("k1", "INT", ColumnType::int(false)),
            col_nn("k2", "INT", ColumnType::int(false)),
            text_col("pad", 20, dflt),
        ],
        pk: vec![("id", None, false)],
        keys: vec![("k12", false, vec![("k1", None, false), ("k2", None, false)])],
        rows,
    });

    // spk: a string primary key.
    let mut rows = Vec::new();
    for i in 0..40i64 {
        rows.push(vec![
            Value::str(&format!("{}{:02}", ["c", "C", "d"][(i % 3) as usize], i)),
            maybe!(rng, int(rng.below(10) as i64), 8),
        ]);
    }
    out.push(Spec {
        name: "spk",
        cols: vec![
            {
                let mut c = text_col("code", 8, Collation::Utf8mb4Bin);
                c.sql.push_str(" NOT NULL");
                c.nullable = false;
                c
            },
            col("n", "INT", ColumnType::int(false)),
        ],
        pk: vec![("code", None, false)],
        keys: vec![],
        rows,
    });
    let _ = vc;
    out
}
