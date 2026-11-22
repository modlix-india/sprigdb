//! The generator of test expressions: typed and nested, over literals of
//! every kind and over the columns of the shared test table, by area
//! (arithmetic, comparison, logic, predicates, control flow, casts, bit
//! operators, string, numeric and date functions, collations).

use sprig_engine::keys::collation::Collation;
use sprig_exec::{BinOp, CastTarget, Expr, Truth};

use super::{Col, Rng};

const INTS: &[&str] = &[
    "0",
    "1",
    "2",
    "3",
    "5",
    "7",
    "10",
    "100",
    "127",
    "128",
    "255",
    "256",
    "1000",
    "32767",
    "65535",
    "65536",
    "2147483647",
    "2147483648",
    "4294967295",
    "4294967296",
    "9007199254740993",
    "9223372036854775807",
    "9223372036854775808",
    "18446744073709551615",
];
const DECS: &[&str] = &[
    "0.0",
    "0.5",
    "1.5",
    "2.5",
    "0.10",
    "1.25",
    "3.14159",
    "99.99",
    "0.001",
    "123456789.123456789",
    "1.0000000001",
    "12345678901234567890.5",
    ".5",
    "1.00",
    "0.05",
    "7.0",
    "1000000.000001",
    "18446744073709551616",
    "0.4",
    "0.6",
];
const DBLS: &[&str] = &[
    "0e0",
    "1e0",
    "1.5e0",
    "2.5e0",
    "1e10",
    "1.5e-5",
    "1e15",
    "1e16",
    "1e-15",
    "1e-16",
    "1e308",
    "3.141592653589793e0",
    "1e22",
    "0.1e0",
    "2e-3",
    "123456.789e0",
    "9.007199254740993e15",
    "0.5e0",
    "3.5e0",
];
const STRS: &[&str] = &[
    "",
    " ",
    "a",
    "A",
    "abc",
    "ABC",
    "abd",
    "b",
    "1",
    "0",
    "1.5",
    "-1",
    " 12 ",
    "12abc",
    "1e2",
    "abc ",
    "ab%",
    "a_c",
    "\u{e9}",
    "e",
    "\u{c9}",
    "\u{df}",
    "ss",
    "0x10",
    ".5",
    "-",
    "00012",
    "9223372036854775808",
    "1.50",
    "x",
    "ab",
    "a%",
    "%",
    "_",
    "aBc",
    "+3",
    "1.",
    "5e-1",
    "0.0",
    "-0.0",
    "2",
    "10",
    "abc\u{e9}",
    "  ",
    "1 ",
    "18446744073709551616",
    "1e30",
    "-1e30",
];
const COLLS: &[Collation] = &[
    Collation::Utf8mb4Bin,
    Collation::Utf8mb4GeneralCi,
    Collation::Utf8mb4UnicodeCi,
];
const DEC_TARGETS: &[(u8, u8)] = &[
    (10, 2),
    (5, 0),
    (20, 5),
    (30, 10),
    (3, 3),
    (65, 30),
    (38, 0),
];

pub struct Generator<'a> {
    rng: Rng,
    cols: &'a [Col],
    with_cols: bool,
    temporal: bool,
    /// The one explicit collation this expression uses. Two different
    /// `COLLATE` clauses under `GREATEST` inside `LIKE` crash MariaDB
    /// 11.8.9, so an expression never has two.
    coll: Collation,
    /// Above zero while the arguments of `GREATEST` or `LEAST` are
    /// generated: no string column, `COLLATE` or binary cast there, for
    /// the same reason.
    plain: usize,
}

fn b(e: Expr) -> Box<Expr> {
    Box::new(e)
}

impl<'a> Generator<'a> {
    /// A generator over `cols`; with `with_cols` its expressions read
    /// columns, and with `temporal` the date and time columns too.
    pub fn new(rng: Rng, cols: &'a [Col], with_cols: bool, temporal: bool) -> Self {
        Generator {
            rng,
            cols,
            with_cols,
            temporal,
            coll: COLLS[0],
            plain: 0,
        }
    }

    fn neg_maybe(&mut self, e: Expr, den: usize) -> Expr {
        if self.rng.chance(1, den) {
            Expr::Neg(b(e))
        } else {
            e
        }
    }

    fn int(&mut self) -> Expr {
        let e = if self.rng.chance(1, 4) {
            Expr::Int((self.rng.next() % 1000).to_string())
        } else {
            Expr::Int((*self.rng.pick(INTS)).to_owned())
        };
        self.neg_maybe(e, 4)
    }

    fn dec(&mut self) -> Expr {
        let e = Expr::Decimal((*self.rng.pick(DECS)).to_owned());
        self.neg_maybe(e, 4)
    }

    fn dbl(&mut self) -> Expr {
        let e = Expr::Double((*self.rng.pick(DBLS)).to_owned());
        self.neg_maybe(e, 5)
    }

    fn str_lit(&mut self) -> Expr {
        Expr::Str((*self.rng.pick(STRS)).to_owned())
    }

    fn column(&mut self, pick: impl Fn(&Col) -> bool) -> Option<Expr> {
        if !self.with_cols {
            return None;
        }
        let candidates: Vec<(usize, &Col)> = self
            .cols
            .iter()
            .enumerate()
            .filter(|(i, c)| *i > 0 && pick(c))
            .filter(|(_, c)| self.temporal || !is_temporal_col(c))
            .filter(|(_, c)| self.plain == 0 || !is_string_col(c))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let (slot, c) = candidates[self.rng.below(candidates.len())];
        Some(Expr::column(slot, c.name, c.ty, true))
    }

    /// A numeric operand without nesting.
    fn num_leaf(&mut self) -> Expr {
        if self.with_cols && self.rng.chance(1, 2) {
            if let Some(c) = self.column(|c| !is_string_col(c) && !is_charset_col(c)) {
                return c;
            }
        }
        match self.rng.below(100) {
            0..=34 => self.int(),
            35..=59 => self.dec(),
            60..=79 => self.dbl(),
            80..=91 => self.str_lit(),
            92..=95 => Expr::Null,
            96..=97 => Expr::Bool(self.rng.chance(1, 2)),
            _ => self.typed_leaf(),
        }
    }

    fn str_leaf(&mut self) -> Expr {
        if self.plain > 0 {
            return if self.rng.chance(1, 8) {
                Expr::Cast(b(self.num_leaf()), CastTarget::Char(None))
            } else {
                self.str_lit()
            };
        }
        if self.with_cols && self.rng.chance(1, 2) {
            if let Some(c) = self.column(is_string_col) {
                return c;
            }
        }
        match self.rng.below(100) {
            0..=74 => self.str_lit(),
            75..=84 => Expr::Collate(b(self.str_lit()), self.coll),
            85..=89 => Expr::Cast(b(self.str_lit()), CastTarget::Binary(None)),
            90..=94 => Expr::Cast(b(self.num_leaf()), CastTarget::Char(None)),
            _ => Expr::Null,
        }
    }

    /// A literal given a type by a cast.
    fn typed_leaf(&mut self) -> Expr {
        let inner = match self.rng.below(4) {
            0 => self.int(),
            1 => self.dec(),
            2 => self.dbl(),
            _ => self.str_lit(),
        };
        let target = self.cast_target();
        Expr::Cast(b(inner), target)
    }

    fn cast_target(&mut self) -> CastTarget {
        match self.rng.below(12) {
            0 | 1 => CastTarget::Signed,
            2 | 3 => CastTarget::Unsigned,
            4..=6 => {
                let (p, s) = *self.rng.pick(DEC_TARGETS);
                CastTarget::Decimal(p, s)
            }
            7 => CastTarget::Double,
            8 => CastTarget::Char(None),
            9 => CastTarget::Char(Some(self.rng.below(6) as u32)),
            _ if self.plain > 0 => CastTarget::Double,
            10 => CastTarget::Binary(None),
            _ => CastTarget::Binary(Some(self.rng.below(6) as u32)),
        }
    }

    fn arith_op(&mut self) -> BinOp {
        *self.rng.pick(&[
            BinOp::Add,
            BinOp::Add,
            BinOp::Sub,
            BinOp::Sub,
            BinOp::Mul,
            BinOp::Mul,
            BinOp::Div,
            BinOp::Div,
            BinOp::IntDiv,
            BinOp::Mod,
        ])
    }

    fn cmp_op(&mut self) -> BinOp {
        *self.rng.pick(&[
            BinOp::Eq,
            BinOp::Eq,
            BinOp::Ne,
            BinOp::Lt,
            BinOp::Le,
            BinOp::Gt,
            BinOp::Ge,
            BinOp::NullSafeEq,
        ])
    }

    fn num(&mut self, depth: usize) -> Expr {
        if depth == 0 {
            return self.num_leaf();
        }
        match self.rng.below(100) {
            0..=59 => {
                let op = self.arith_op();
                Expr::binary(op, self.num(depth - 1), self.num(depth - 1))
            }
            60..=66 => match self.num(depth - 1) {
                // A doubled sign has its own typing quirks on MySQL.
                e @ Expr::Neg(_) => e,
                e => Expr::Neg(b(e)),
            },
            67..=74 => {
                let t = self.cast_target();
                Expr::Cast(b(self.num(depth - 1)), t)
            }
            75..=86 => self.control_of(depth - 1, false),
            _ => self.num_leaf(),
        }
    }

    fn string(&mut self, depth: usize) -> Expr {
        if depth == 0 || self.rng.chance(1, 2) {
            return self.str_leaf();
        }
        match self.rng.below(3) {
            0 => self.control_of(depth - 1, true),
            1 => Expr::Cast(
                b(self.any(depth - 1)),
                CastTarget::Char(Some(1 + self.rng.below(8) as u32)),
            ),
            _ => {
                let inner = self.string(depth - 1);
                if self.plain > 0 {
                    inner
                } else {
                    Expr::Collate(b(inner), self.coll)
                }
            }
        }
    }

    fn any(&mut self, depth: usize) -> Expr {
        match self.rng.below(10) {
            0..=4 => self.num(depth),
            5..=7 => self.string(depth),
            _ => {
                if depth == 0 {
                    self.num_leaf()
                } else {
                    self.pred(depth - 1)
                }
            }
        }
    }

    /// Operands likely to be comparable: mostly of one family.
    fn pair(&mut self, depth: usize) -> (Expr, Expr) {
        match self.rng.below(10) {
            0..=3 => (self.num(depth), self.num(depth)),
            4..=6 => (self.string(depth), self.string(depth)),
            _ => (self.any(depth), self.any(depth)),
        }
    }

    fn pred(&mut self, depth: usize) -> Expr {
        match self.rng.below(100) {
            0..=39 => {
                let (x, y) = self.pair(depth);
                Expr::binary(self.cmp_op(), x, y)
            }
            40..=54 => self.logic(depth),
            55..=64 => self.is_test(depth),
            _ => self.in_between_like(depth),
        }
    }

    fn logic(&mut self, depth: usize) -> Expr {
        let d = depth.saturating_sub(1);
        match self.rng.below(7) {
            0 | 1 => Expr::binary(BinOp::And, self.truthy(d), self.truthy(d)),
            2 | 3 => Expr::binary(BinOp::Or, self.truthy(d), self.truthy(d)),
            4 => Expr::binary(BinOp::Xor, self.truthy(d), self.truthy(d)),
            _ => Expr::Not(b(self.truthy(d))),
        }
    }

    fn truthy(&mut self, depth: usize) -> Expr {
        if self.rng.chance(1, 2) {
            self.any(depth)
        } else {
            self.pred(depth)
        }
    }

    fn is_test(&mut self, depth: usize) -> Expr {
        let e = self.any(depth);
        let not = self.rng.chance(1, 3);
        match self.rng.below(5) {
            0 | 1 => Expr::IsNull(b(e), not),
            2 => Expr::IsTruth(b(e), Truth::True, not),
            3 => Expr::IsTruth(b(e), Truth::False, not),
            _ => Expr::IsTruth(b(e), Truth::Unknown, not),
        }
    }

    fn in_between_like(&mut self, depth: usize) -> Expr {
        let not = self.rng.chance(1, 4);
        match self.rng.below(3) {
            0 => {
                let n = 1 + self.rng.below(4);
                let strings = self.rng.chance(1, 3);
                let mixed = self.rng.chance(1, 4);
                let item = |g: &mut Self| {
                    if mixed {
                        g.any(depth)
                    } else if strings {
                        g.string(depth)
                    } else {
                        g.num(depth)
                    }
                };
                let x = item(self);
                let list = (0..n).map(|_| item(self)).collect();
                Expr::In(b(x), list, not)
            }
            1 => {
                let (x, lo) = self.pair(depth);
                let hi = if self.rng.chance(1, 2) {
                    self.num(depth)
                } else {
                    self.any(depth)
                };
                Expr::Between(b(x), b(lo), b(hi), not)
            }
            _ => {
                let subject = if self.rng.chance(4, 5) {
                    self.string(depth)
                } else {
                    self.any(depth)
                };
                let pattern = if self.rng.chance(4, 5) {
                    Expr::Str(
                        (*self.rng.pick(&[
                            "a%", "%", "_", "a_c", "%b%", "ab\\%", "A%", "%c", "", "abc", "1%",
                            "_2%", "%\u{e9}", "E", "a|%", "%%", "_%_", "ab ", "\\_", "%ss%",
                        ]))
                        .to_owned(),
                    )
                } else {
                    self.string(depth)
                };
                let escape = if self.rng.chance(1, 5) {
                    Some(b(Expr::Str(
                        (*self.rng.pick(&["|", "\\", "", "a"])).to_owned(),
                    )))
                } else {
                    None
                };
                Expr::Like(b(subject), b(pattern), escape, not)
            }
        }
    }

    /// A control-flow expression with numeric or string branches.
    fn control_of(&mut self, depth: usize, strings: bool) -> Expr {
        let mixed = self.rng.chance(1, 5);
        let branch = |g: &mut Self| {
            if mixed {
                g.any(depth)
            } else if strings {
                g.string(depth)
            } else {
                g.num(depth)
            }
        };
        match self.rng.below(9) {
            0 => {
                let c = self.truthy(depth);
                Expr::func("IF", vec![c, branch(self), branch(self)])
            }
            1 => Expr::func("IFNULL", vec![branch(self), branch(self)]),
            2 => Expr::func("NULLIF", vec![branch(self), branch(self)]),
            3 => {
                let n = 1 + self.rng.below(4);
                Expr::func("COALESCE", (0..n).map(|_| branch(self)).collect())
            }
            pick @ (4 | 5) => {
                let n = 2 + self.rng.below(3);
                self.plain += 1;
                let args = (0..n).map(|_| branch(self)).collect();
                self.plain -= 1;
                Expr::func(if pick == 4 { "GREATEST" } else { "LEAST" }, args)
            }
            6 | 7 => {
                let n = 1 + self.rng.below(3);
                let whens = (0..n).map(|_| (self.truthy(depth), branch(self))).collect();
                let otherwise = if self.rng.chance(2, 3) {
                    Some(b(branch(self)))
                } else {
                    None
                };
                Expr::Case {
                    operand: None,
                    whens,
                    otherwise,
                }
            }
            _ => {
                let n = 1 + self.rng.below(3);
                let operand = self.any(depth);
                let whens = (0..n).map(|_| (self.any(depth), branch(self))).collect();
                let otherwise = if self.rng.chance(2, 3) {
                    Some(b(branch(self)))
                } else {
                    None
                };
                Expr::Case {
                    operand: Some(b(operand)),
                    whens,
                    otherwise,
                }
            }
        }
    }

    fn bit(&mut self, depth: usize) -> Expr {
        let op = *self.rng.pick(&[
            BinOp::BitAnd,
            BinOp::BitOr,
            BinOp::BitXor,
            BinOp::Shl,
            BinOp::Shr,
        ]);
        if self.rng.chance(1, 6) {
            Expr::BitNot(b(self.num(depth)))
        } else {
            Expr::binary(op, self.num(depth), self.num(depth))
        }
    }

    fn small_int(&mut self) -> Expr {
        match self.rng.below(20) {
            0 => Expr::Null,
            1 => Expr::Decimal("1.5".to_owned()),
            2 => Expr::Str("2".to_owned()),
            3 => self.num_leaf(),
            _ => Expr::int(self.rng.below(14) as i64 - 2),
        }
    }

    fn str_arg(&mut self, depth: usize) -> Expr {
        if self.rng.chance(1, 6) {
            self.any(depth)
        } else {
            self.string(depth)
        }
    }

    fn string_fn(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(24) {
            0 | 1 => {
                let n = 1 + self.rng.below(4);
                f("CONCAT", (0..n).map(|_| self.str_arg(depth)).collect())
            }
            2 => {
                let n = 2 + self.rng.below(3);
                f("CONCAT_WS", (0..n).map(|_| self.str_arg(depth)).collect())
            }
            3 => f("LENGTH", vec![self.str_arg(depth)]),
            4 => f("CHAR_LENGTH", vec![self.str_arg(depth)]),
            5 => f("UPPER", vec![self.str_arg(depth)]),
            6 => f("LOWER", vec![self.str_arg(depth)]),
            7 => f("REVERSE", vec![self.str_arg(depth)]),
            8 => {
                let name = *self.rng.pick(&["LTRIM", "RTRIM", "TRIM"]);
                f(name, vec![self.str_arg(depth)])
            }
            9 => {
                let name = *self
                    .rng
                    .pick(&["TRIM_BOTH", "TRIM_LEADING", "TRIM_TRAILING"]);
                f(name, vec![self.str_lit(), self.str_arg(depth)])
            }
            10 | 11 => {
                let mut args = vec![self.str_arg(depth), self.small_int()];
                if self.rng.chance(1, 2) {
                    args.push(self.small_int());
                }
                f("SUBSTRING", args)
            }
            12 => f("LEFT", vec![self.str_arg(depth), self.small_int()]),
            13 => f("RIGHT", vec![self.str_arg(depth), self.small_int()]),
            14 => f(
                "REPLACE",
                vec![self.str_arg(depth), self.str_lit(), self.str_lit()],
            ),
            15 => f(
                "LPAD",
                vec![self.str_arg(depth), self.small_int(), self.str_lit()],
            ),
            16 => f(
                "RPAD",
                vec![self.str_arg(depth), self.small_int(), self.str_lit()],
            ),
            17 => {
                let mut args = vec![self.str_lit(), self.str_arg(depth)];
                if self.rng.chance(1, 3) {
                    args.push(self.small_int());
                }
                f("LOCATE", args)
            }
            18 => f("INSTR", vec![self.str_arg(depth), self.str_lit()]),
            19 => f("REPEAT", vec![self.str_arg(depth), self.small_int()]),
            20 => f("SPACE", vec![self.small_int()]),
            21 => f("STRCMP", vec![self.string(depth), self.string(depth)]),
            22 => f("HEX", vec![self.any(depth)]),
            _ => f("ASCII", vec![self.str_arg(depth)]),
        }
    }

    fn numeric_fn(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(16) {
            0 | 1 => f("ABS", vec![self.num(depth)]),
            2 => f("SIGN", vec![self.num(depth)]),
            3 => f("CEIL", vec![self.num(depth)]),
            4 => f("FLOOR", vec![self.num(depth)]),
            5 | 6 => f("ROUND", vec![self.num(depth)]),
            7 | 8 => f("ROUND", vec![self.num(depth), self.small_int()]),
            9 | 10 => f("TRUNCATE", vec![self.num(depth), self.small_int()]),
            11 => f("MOD", vec![self.num(depth), self.num(depth)]),
            12 => f("POW", vec![self.num(depth), self.small_int()]),
            13 => f("SQRT", vec![self.num(depth)]),
            14 => {
                let name = *self.rng.pick(&["LN", "LOG2", "LOG10", "LOG"]);
                f(name, vec![self.num(depth)])
            }
            _ => f("EXP", vec![self.small_int()]),
        }
    }

    fn temporal_lit(&mut self) -> Expr {
        Expr::Str(
            (*self.rng.pick(&[
                "2024-01-15",
                "2024-02-29",
                "2023-02-29",
                "2024-01-15 10:30:00",
                "2024-01-15 10:30:00.123456",
                "2024-12-31 23:59:59.999999",
                "1999-12-31 23:59:59",
                "0000-00-00",
                "2024-00-10",
                "20240115",
                "240115",
                "20240115103000",
                "10:30:00",
                "-12:00:01.5",
                "838:59:59",
                "25:61:00",
                "2024-01-15T10:30:00",
                "2024-1-5 3:4:5",
                "70-01-01",
                "2024-01-15 abc",
                "abc",
                "",
                "2024-13-01",
                "9999-12-31 23:59:59",
                "1000-01-01",
                "2021-03-28 02:30:00",
                "2001-09-09 01:46:40",
            ]))
            .to_owned(),
        )
    }

    fn temporal_arg(&mut self, depth: usize) -> Expr {
        if self.with_cols && self.rng.chance(3, 5) {
            if let Some(c) = self.column(is_temporal_col) {
                return c;
            }
        }
        match self.rng.below(12) {
            0..=6 => self.temporal_lit(),
            7 => Expr::Int(
                (*self
                    .rng
                    .pick(&["20240115", "240115", "20240115103000", "0", "1"]))
                .to_owned(),
            ),
            8 => Expr::Cast(b(self.temporal_lit()), CastTarget::Date),
            9 => Expr::Cast(
                b(self.temporal_lit()),
                CastTarget::DateTime(self.rng.below(7) as u8),
            ),
            10 => Expr::Cast(
                b(self.temporal_lit()),
                CastTarget::Time(self.rng.below(7) as u8),
            ),
            _ => {
                if depth > 0 {
                    self.temporal_fn(depth - 1)
                } else {
                    Expr::Null
                }
            }
        }
    }

    fn temporal_fn(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(32) {
            24 => {
                let unit = *self.rng.pick(&[
                    "YEAR",
                    "MONTH",
                    "DAY",
                    "QUARTER",
                    "WEEK",
                    "HOUR",
                    "MINUTE",
                    "SECOND",
                    "MICROSECOND",
                    "YEAR_MONTH",
                    "DAY_HOUR",
                    "DAY_MINUTE",
                    "DAY_SECOND",
                    "DAY_MICROSECOND",
                    "HOUR_MINUTE",
                    "HOUR_SECOND",
                    "HOUR_MICROSECOND",
                    "MINUTE_SECOND",
                    "MINUTE_MICROSECOND",
                    "SECOND_MICROSECOND",
                ]);
                Expr::Func(
                    "EXTRACT".to_owned(),
                    vec![Expr::Keyword(unit.to_owned()), self.temporal_arg(depth)],
                )
            }
            25 => {
                let unit = *self.rng.pick(&[
                    "MICROSECOND",
                    "SECOND",
                    "MINUTE",
                    "HOUR",
                    "DAY",
                    "WEEK",
                    "MONTH",
                    "QUARTER",
                    "YEAR",
                ]);
                Expr::Func(
                    "TIMESTAMPDIFF".to_owned(),
                    vec![
                        Expr::Keyword(unit.to_owned()),
                        self.temporal_arg(depth),
                        self.temporal_arg(depth),
                    ],
                )
            }
            26 => {
                let name = *self.rng.pick(&["WEEK", "YEARWEEK"]);
                let mut args = vec![self.temporal_arg(depth)];
                if self.rng.chance(3, 4) {
                    args.push(Expr::int(self.rng.below(10) as i64 - 1));
                }
                f(name, args)
            }
            27 => {
                let (text, fmt) = *self.rng.pick(&[
                    ("2024-01-15", "%Y-%m-%d"),
                    ("15/01/2024 10:30:45", "%d/%m/%Y %H:%i:%s"),
                    ("10:30:45", "%H:%i:%s"),
                    ("10:30:45.5", "%H:%i:%s.%f"),
                    ("Jan 15 2024", "%b %d %Y"),
                    ("January 15th 2024", "%M %D %Y"),
                    ("24-1-5", "%y-%c-%e"),
                    ("2024-01-15 abc", "%Y-%m-%d"),
                    ("2024-01", "%Y-%m-%d"),
                    ("2024-13-01", "%Y-%m-%d"),
                    ("2024-02-30", "%Y-%m-%d"),
                    ("2024-00-10", "%Y-%m-%d"),
                    ("0000-00-00", "%Y-%m-%d"),
                    ("abc", "%Y-%m-%d"),
                    ("10:30 PM", "%h:%i %p"),
                    ("12:30 AM", "%h:%i %p"),
                    ("13:30 AM", "%h:%i %p"),
                    ("2024 045", "%Y %j"),
                    ("20240115", "%Y%m%d"),
                    ("2024-01-15 10", "%Y-%m-%d %H"),
                ]);
                let text = if self.rng.chance(1, 6) {
                    self.temporal_lit()
                } else {
                    Expr::str(text)
                };
                f("STR_TO_DATE", vec![text, Expr::str(fmt)])
            }
            28 => {
                let zone = |g: &mut Self| {
                    Expr::str(*g.rng.pick(&[
                        "+00:00", "+05:30", "-08:00", "+13:00", "-12:59", "+14:00", "+1:00", "abc",
                        "SYSTEM",
                    ]))
                };
                let d = self.temporal_arg(depth);
                f("CONVERT_TZ", vec![d, zone(self), zone(self)])
            }
            29..=31 => {
                let name = *self.rng.pick(&["DATE_ADD", "DATE_SUB"]);
                let (unit, amount) = *self.rng.pick(&[
                    ("DAY_SECOND", "1 02:03:04"),
                    ("DAY_SECOND", "02:03"),
                    ("DAY_MICROSECOND", "1:02:03:04.5"),
                    ("SECOND_MICROSECOND", "1.5"),
                    ("SECOND_MICROSECOND", "1.000005"),
                    ("DAY_HOUR", "-1 10"),
                    ("DAY_HOUR", "40 1"),
                    ("DAY_MINUTE", "1 2:3"),
                    ("HOUR_MINUTE", "1:30"),
                    ("HOUR_MINUTE", "a1b2"),
                    ("HOUR_SECOND", "1:2:3"),
                    ("HOUR_MICROSECOND", "1:2:3.4"),
                    ("MINUTE_SECOND", "90:90"),
                    ("MINUTE_MICROSECOND", "1:1.5"),
                    ("YEAR_MONTH", "1-2"),
                    ("YEAR_MONTH", "2"),
                    ("YEAR_MONTH", "-1-11"),
                    ("HOUR_MINUTE", ""),
                    ("DAY_SECOND", "1:2:3:4:5:6"),
                ]);
                let amount = if self.rng.chance(1, 6) {
                    self.small_int()
                } else {
                    Expr::str(amount)
                };
                f(
                    name,
                    vec![
                        self.temporal_arg(depth),
                        amount,
                        Expr::Keyword(unit.to_owned()),
                    ],
                )
            }
            0..=3 => {
                let name = *self.rng.pick(&[
                    "YEAR",
                    "MONTH",
                    "DAY",
                    "QUARTER",
                    "HOUR",
                    "MINUTE",
                    "SECOND",
                    "MICROSECOND",
                    "DAYOFWEEK",
                    "WEEKDAY",
                    "DAYOFYEAR",
                    "TO_DAYS",
                    "WEEKOFYEAR",
                ]);
                f(name, vec![self.temporal_arg(depth)])
            }
            4 | 5 => {
                let name = *self
                    .rng
                    .pick(&["DATE", "TIME", "LAST_DAY", "DAYNAME", "MONTHNAME"]);
                f(name, vec![self.temporal_arg(depth)])
            }
            6..=9 => {
                let name = *self.rng.pick(&["DATE_ADD", "DATE_SUB"]);
                let unit = *self.rng.pick(&[
                    "MICROSECOND",
                    "SECOND",
                    "MINUTE",
                    "HOUR",
                    "DAY",
                    "WEEK",
                    "MONTH",
                    "QUARTER",
                    "YEAR",
                ]);
                let amount = match self.rng.below(8) {
                    0 => Expr::Decimal("1.5".to_owned()),
                    1 => Expr::Str("2".to_owned()),
                    2 => Expr::int(100_000),
                    3 => Expr::Null,
                    _ => Expr::int(self.rng.below(80) as i64 - 30),
                };
                f(
                    name,
                    vec![
                        self.temporal_arg(depth),
                        amount,
                        Expr::Keyword(unit.to_owned()),
                    ],
                )
            }
            10 | 11 => f(
                "DATEDIFF",
                vec![self.temporal_arg(depth), self.temporal_arg(depth)],
            ),
            12..=14 => {
                let fmt = *self.rng.pick(&[
                    "%Y-%m-%d",
                    "%H:%i:%s",
                    "%d/%m/%y %h:%i %p",
                    "%W %M %e %Y",
                    "%a %b %c %D",
                    "%j %U %u %V %v %X %x %w",
                    "%T %r %f",
                    "%k %l %I",
                    "%%Y %q",
                    "",
                    "%",
                ]);
                f(
                    "DATE_FORMAT",
                    vec![self.temporal_arg(depth), Expr::str(fmt)],
                )
            }
            15 | 16 => f("UNIX_TIMESTAMP", vec![self.temporal_arg(depth)]),
            17 | 18 => {
                let n = match self.rng.below(6) {
                    0 => Expr::Int("0".to_owned()),
                    1 => Expr::Decimal("1700000000.123456".to_owned()),
                    2 => Expr::int(-1),
                    3 => Expr::Int("4294967296".to_owned()),
                    4 => Expr::Int("2147483647".to_owned()),
                    _ => Expr::Int((self.rng.next() % 2_000_000_000).to_string()),
                };
                f("FROM_UNIXTIME", vec![n])
            }
            19 => {
                let name = *self
                    .rng
                    .pick(&["NOW", "CURDATE", "CURTIME", "UTC_TIMESTAMP"]);
                f(name, vec![])
            }
            20 => f("NOW", vec![Expr::int(self.rng.below(7) as i64)]),
            21 => Expr::binary(
                self.cmp_op(),
                self.temporal_arg(depth),
                self.temporal_arg(depth),
            ),
            22 => Expr::binary(self.arith_op(), self.temporal_arg(depth), self.num_leaf()),
            _ => {
                let t = *self.rng.pick(&[
                    CastTarget::Signed,
                    CastTarget::Char(None),
                    CastTarget::Decimal(20, 6),
                    CastTarget::Double,
                ]);
                Expr::Cast(b(self.temporal_arg(depth)), t)
            }
        }
    }

    /// A count argument with the wide values the long-tail functions
    /// read in their own ways.
    fn wide_int(&mut self) -> Expr {
        match self.rng.below(10) {
            0 => Expr::Int(
                (*self.rng.pick(&[
                    "4294967295",
                    "4294967296",
                    "4294967298",
                    "18446744073709551615",
                    "9223372036854775807",
                ]))
                .to_owned(),
            ),
            1 => Expr::int(-(self.rng.below(5) as i64) - 1),
            _ => self.small_int(),
        }
    }

    /// The less common string functions.
    fn string_fn2(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(22) {
            0 | 1 => {
                let delim = if self.rng.chance(1, 5) {
                    self.str_arg(depth)
                } else {
                    Expr::str(
                        *self
                            .rng
                            .pick(&[".", "a", "", "ab", "b", ",", " ", "\u{e9}", "A", "1"]),
                    )
                };
                f(
                    "SUBSTRING_INDEX",
                    vec![self.str_arg(depth), delim, self.wide_int()],
                )
            }
            2 | 3 => f(
                "INSERT",
                vec![
                    self.str_arg(depth),
                    self.wide_int(),
                    self.wide_int(),
                    self.str_arg(depth),
                ],
            ),
            4 | 5 => {
                let n = 2 + self.rng.below(4);
                f("FIELD", (0..n).map(|_| self.any(depth)).collect())
            }
            6 => {
                let n = 1 + self.rng.below(4);
                let mut args = vec![self.wide_int()];
                args.extend((0..n).map(|_| self.str_arg(depth)));
                f("ELT", args)
            }
            7 | 8 => {
                let list = if self.rng.chance(1, 4) {
                    self.str_arg(depth)
                } else {
                    Expr::str(*self.rng.pick(&[
                        "a,b,c", "a,,b", "", "abc,ABC", "1,2,3", "\u{e9},e", "a ,b", ",a", "A,b",
                        "1.5,2",
                    ]))
                };
                f("FIND_IN_SET", vec![self.str_arg(depth), list])
            }
            9 | 10 => {
                let mut args = vec![self.num(depth), self.small_int()];
                if self.rng.chance(1, 3) {
                    args.push(Expr::str(*self.rng.pick(&[
                        "de_DE", "en_US", "fr_FR", "xx_XX", "en_IN", "ru_RU", "de_CH", "bg_BG",
                        "ka_GE", "sr_YU", "it_CH", "ar_SA",
                    ])));
                }
                f("FORMAT", args)
            }
            11 => f("QUOTE", vec![self.str_arg(depth)]),
            12 => {
                let a = if self.rng.chance(1, 2) {
                    Expr::str(
                        *self
                            .rng
                            .pick(&["41", "4142", "abc", "GG", "e9", "0", "", "c3a9", "4G"]),
                    )
                } else {
                    self.str_arg(depth)
                };
                f("UNHEX", vec![a])
            }
            13 => {
                let name = *self.rng.pick(&["BIN", "OCT"]);
                f(name, vec![self.any(depth)])
            }
            14 | 15 => {
                let base = |g: &mut Self| {
                    if g.rng.chance(1, 8) {
                        g.num_leaf()
                    } else {
                        Expr::int(*g.rng.pick(&[2, 8, 10, 16, 36, -10, -16, -2, 62, 1, 37, 0]))
                    }
                };
                let a = if self.rng.chance(1, 2) {
                    Expr::str(*self.rng.pick(&[
                        "ff",
                        "-ff",
                        "zz",
                        "1g",
                        "g",
                        " 12",
                        "",
                        "-",
                        "7fffffffffffffff",
                        "18446744073709551616",
                        "Zz",
                    ]))
                } else {
                    self.any(depth)
                };
                f("CONV", vec![a, base(self), base(self)])
            }
            16 => {
                let n = 1 + self.rng.below(3);
                let mut args: Vec<Expr> = (0..n)
                    .map(|_| {
                        if self.rng.chance(1, 2) {
                            Expr::int(*self.rng.pick(&[65, 0, 256, 233, 0xC3A9, 65536, 127, 200]))
                        } else {
                            self.num(depth)
                        }
                    })
                    .collect();
                if self.rng.chance(1, 3) {
                    let cs = *self.rng.pick(&["utf8mb4", "latin1", "ascii", "binary"]);
                    args.push(Expr::Keyword(format!("USING {cs}")));
                }
                f("CHAR", args)
            }
            17 => f("MD5", vec![self.any(depth)]),
            18 => f("SHA1", vec![self.any(depth)]),
            19 => {
                let bits = if self.rng.chance(1, 6) {
                    self.num_leaf()
                } else {
                    Expr::int(*self.rng.pick(&[0, 224, 256, 384, 512, 1]))
                };
                f("SHA2", vec![self.any(depth), bits])
            }
            _ => f("CRC32", vec![self.any(depth)]),
        }
    }

    /// Trigonometry, logarithms at their edges, seeded random numbers,
    /// `INTERVAL()`.
    fn numeric_fn2(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(16) {
            0..=3 => {
                let name = *self
                    .rng
                    .pick(&["SIN", "COS", "TAN", "COT", "ASIN", "ACOS", "ATAN"]);
                f(name, vec![self.num(depth)])
            }
            4 => {
                let name = *self.rng.pick(&["ATAN", "ATAN2"]);
                f(name, vec![self.num(depth), self.num(depth)])
            }
            5 | 6 => {
                let name = *self.rng.pick(&["DEGREES", "RADIANS"]);
                f(name, vec![self.num(depth)])
            }
            7 => Expr::binary(self.arith_op(), f("PI", vec![]), self.num(depth)),
            8 | 9 => {
                let edge = |g: &mut Self| {
                    if g.rng.chance(1, 2) {
                        match g.rng.below(8) {
                            0 => Expr::int(0),
                            1 => Expr::int(1),
                            2 => Expr::int(-1),
                            3 => Expr::Decimal("0.0".to_owned()),
                            4 => Expr::int(2),
                            5 => Expr::Double("1e0".to_owned()),
                            6 => Expr::Decimal("0.5".to_owned()),
                            _ => Expr::Neg(b(Expr::Decimal("0.0".to_owned()))),
                        }
                    } else {
                        g.num(depth)
                    }
                };
                match self.rng.below(4) {
                    0 => f("LOG", vec![edge(self), edge(self)]),
                    1 => f("LN", vec![edge(self)]),
                    2 => f("LOG2", vec![edge(self)]),
                    _ => f("LOG10", vec![edge(self)]),
                }
            }
            10 | 11 => {
                let seed = if self.rng.chance(1, 2) {
                    self.wide_int()
                } else {
                    self.num(depth)
                };
                f("RAND", vec![seed])
            }
            _ => {
                let n = 1 + self.rng.below(5);
                let mut args = vec![self.num(depth)];
                args.extend((0..n).map(|_| self.num_leaf()));
                f("INTERVAL", args)
            }
        }
    }

    /// A row value of `n` items, sometimes nested.
    fn row_of(&mut self, n: usize, depth: usize) -> Expr {
        let items = (0..n)
            .map(|_| {
                if depth > 0 && self.rng.chance(1, 12) {
                    self.row_of(2, 0)
                } else {
                    match self.rng.below(10) {
                        0..=3 => self.num(depth.min(1)),
                        4..=6 => self.string(depth.min(1)),
                        7 => Expr::Null,
                        _ => self.num_leaf(),
                    }
                }
            })
            .collect();
        Expr::Func("ROW".to_owned(), items)
    }

    /// Row comparisons and row lists.
    fn row_cmp(&mut self, depth: usize) -> Expr {
        let n = 2 + self.rng.below(2);
        // Now and then a row of another size, or a single value.
        let other = |g: &mut Self| {
            if g.rng.chance(1, 25) {
                if g.rng.chance(1, 2) {
                    g.num_leaf()
                } else {
                    g.row_of(n + 1, 0)
                }
            } else {
                g.row_of(n, depth)
            }
        };
        if self.rng.chance(1, 3) {
            let a = self.row_of(n, depth);
            let k = 1 + self.rng.below(3);
            let list = (0..k).map(|_| other(self)).collect();
            Expr::In(b(a), list, self.rng.chance(1, 3))
        } else {
            let a = self.row_of(n, depth);
            let op = self.cmp_op();
            Expr::binary(op, a, other(self))
        }
    }

    /// A string in another character set: a latin1 or ascii column, an
    /// introducer, a conversion.
    fn cs_leaf(&mut self, depth: usize) -> Expr {
        let lit = |g: &mut Self| -> Expr {
            Expr::str(*g.rng.pick(&[
                "a", "\u{e9}", "\u{c9}", "e", "\u{df}", "ss", "\u{20ac}", "\u{4e2d}", "abc ", "",
                "1", "x", "\u{e4}", "ABC",
            ]))
        };
        // (Under GREATEST and LEAST: no column and no COLLATE, see `plain`.)
        let pick = if self.plain > 0 {
            *self.rng.pick(&[0, 1, 2, 6, 7])
        } else {
            self.rng.below(12)
        };
        match pick {
            0..=3 if self.with_cols && self.plain == 0 => {
                self.column(is_charset_col).unwrap_or_else(|| lit(self))
            }
            0 | 1 => Expr::Func(
                "_CHARSET".to_owned(),
                vec![
                    Expr::Keyword("latin1".to_owned()),
                    Expr::Hex(vec![
                        *self.rng.pick(&[0xE9u8, 0x80, 0xDF, 0x41, 0xC9, 0xFF, 0x81]),
                    ]),
                ],
            ),
            2 => Expr::Func(
                "_CHARSET".to_owned(),
                vec![
                    Expr::Keyword(
                        (*self.rng.pick(&["latin1", "ascii", "utf8mb4", "binary"])).to_owned(),
                    ),
                    lit(self),
                ],
            ),
            3 | 4 => {
                let cs = *self.rng.pick(&["latin1", "ascii", "utf8mb4", "binary"]);
                let inner = if self.rng.chance(1, 3) {
                    self.any(depth)
                } else {
                    lit(self)
                };
                Expr::Func(
                    "CONVERT_USING".to_owned(),
                    vec![inner, Expr::Keyword(cs.to_owned())],
                )
            }
            5 => Expr::Collate(
                b(Expr::Func(
                    "CONVERT_USING".to_owned(),
                    vec![lit(self), Expr::Keyword("latin1".to_owned())],
                )),
                *self.rng.pick(&[
                    Collation::Latin1Bin,
                    Collation::Latin1GeneralCi,
                    Collation::Latin1SwedishCi,
                ]),
            ),
            6 | 7 => lit(self),
            _ => self.str_arg(depth),
        }
    }

    /// Character sets inside expressions: latin1, ascii and utf8mb4
    /// values through string functions, comparisons and conversions.
    fn charset_expr(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        match self.rng.below(14) {
            0..=2 => {
                let (x, y) = (self.cs_leaf(depth), self.cs_leaf(depth));
                Expr::binary(self.cmp_op(), x, y)
            }
            3 => {
                let n = 2 + self.rng.below(2);
                f("CONCAT", (0..n).map(|_| self.cs_leaf(depth)).collect())
            }
            4 => {
                let name = *self.rng.pick(&[
                    "LENGTH",
                    "CHAR_LENGTH",
                    "UPPER",
                    "LOWER",
                    "REVERSE",
                    "HEX",
                    "ASCII",
                    "ORD",
                ]);
                f(name, vec![self.cs_leaf(depth)])
            }
            5 => {
                let name = *self.rng.pick(&["CHARSET", "COLLATION", "COERCIBILITY"]);
                f(name, vec![self.cs_leaf(depth)])
            }
            6 => f(
                "REPLACE",
                vec![self.cs_leaf(depth), self.cs_leaf(0), self.cs_leaf(0)],
            ),
            7 => f("LOCATE", vec![self.cs_leaf(0), self.cs_leaf(depth)]),
            8 => {
                let name = *self.rng.pick(&["LPAD", "RPAD"]);
                f(
                    name,
                    vec![self.cs_leaf(depth), self.small_int(), self.cs_leaf(0)],
                )
            }
            9 => Expr::Like(
                b(self.cs_leaf(depth)),
                b(Expr::str(
                    *self.rng.pick(&["%\u{e9}%", "a%", "_", "%", "\u{c9}", "e%"]),
                )),
                None,
                self.rng.chance(1, 4),
            ),
            10 => {
                let n = 1 + self.rng.below(3);
                Expr::In(
                    b(self.cs_leaf(depth)),
                    (0..n).map(|_| self.cs_leaf(0)).collect(),
                    false,
                )
            }
            11 => {
                let n = 2 + self.rng.below(2);
                let name = *self.rng.pick(&["COALESCE", "GREATEST", "LEAST"]);
                let plain = name != "COALESCE";
                self.plain += usize::from(plain);
                let args = (0..n).map(|_| self.cs_leaf(0)).collect();
                self.plain -= usize::from(plain);
                f(name, args)
            }
            12 => f("SUBSTRING", vec![self.cs_leaf(depth), self.small_int()]),
            _ => f("STRCMP", vec![self.cs_leaf(depth), self.cs_leaf(depth)]),
        }
    }

    /// Named time zones: `CONVERT_TZ` between names and offsets, at
    /// ordinary times, at the clocks' changes and at the range's ends; and
    /// the functions that read the session's zone.
    fn tz_expr(&mut self, depth: usize) -> Expr {
        let f = |name: &str, args: Vec<Expr>| Expr::func(name, args);
        let zone = |g: &mut Self| {
            Expr::str(*g.rng.pick(&[
                "Europe/Berlin",
                "America/New_York",
                "Asia/Kolkata",
                "Australia/Lord_Howe",
                "Pacific/Chatham",
                "America/St_Johns",
                "Europe/London",
                "UTC",
                "Etc/GMT+5",
                "Etc/GMT-14",
                "EST",
                "MET",
                "Asia/Kathmandu",
                "Africa/Casablanca",
                "America/Sao_Paulo",
                "Pacific/Apia",
                "europe/berlin",
                "Asia/Calcutta",
                "US/Pacific",
                "posix/Europe/Berlin",
                "Europe/Nowhere",
                "+05:30",
                "-08:00",
                "SYSTEM",
                "",
            ]))
        };
        let when = |g: &mut Self| -> Expr {
            if g.rng.chance(1, 3) {
                g.temporal_arg(depth)
            } else {
                Expr::str(*g.rng.pick(&[
                    "2024-03-10 02:30:00",
                    "2024-03-10 01:59:59",
                    "2024-03-31 02:30:00",
                    "2024-11-03 01:30:00",
                    "2024-10-27 02:30:00",
                    "2024-01-15 10:30:00.123456",
                    "2024-07-04 12:00:00",
                    "1970-01-01 00:00:01",
                    "1970-01-01 05:30:00",
                    "2038-01-19 03:14:07",
                    "2106-02-07 06:28:15",
                    "3001-01-19 03:14:07",
                    "1969-12-31 23:59:59",
                    "2024-04-07 02:15:00",
                    "2011-12-30 00:00:00",
                ]))
            }
        };
        match self.rng.below(10) {
            0..=5 => f("CONVERT_TZ", vec![when(self), zone(self), zone(self)]),
            6 => f("UNIX_TIMESTAMP", vec![when(self)]),
            7 => {
                let n = Expr::Int(
                    (*self.rng.pick(&[
                        "1710052200",
                        "1730611800",
                        "1700000000",
                        "0",
                        "1",
                        "2147483647",
                        "1711846800",
                    ]))
                    .to_owned(),
                );
                f("FROM_UNIXTIME", vec![n])
            }
            8 => f(
                *self
                    .rng
                    .pick(&["NOW", "CURDATE", "CURTIME", "UTC_TIMESTAMP"]),
                vec![],
            ),
            _ => Expr::binary(
                BinOp::Sub,
                f(
                    "UNIX_TIMESTAMP",
                    vec![f("CONVERT_TZ", vec![when(self), zone(self), zone(self)])],
                ),
                f("UNIX_TIMESTAMP", vec![when(self)]),
            ),
        }
    }

    /// One expression of an area.
    pub fn area(&mut self, area: &str) -> Expr {
        self.coll = *self.rng.pick(COLLS);
        let depth = self.rng.below(3);
        match area {
            "arith" => {
                let op = self.arith_op();
                Expr::binary(op, self.num(depth), self.num(depth))
            }
            "cmp" => {
                let (x, y) = self.pair(depth);
                Expr::binary(self.cmp_op(), x, y)
            }
            "logic" => {
                if self.rng.chance(2, 3) {
                    self.logic(depth + 1)
                } else {
                    self.is_test(depth)
                }
            }
            "pred" => self.in_between_like(depth),
            "control" => {
                let strings = self.rng.chance(1, 3);
                self.control_of(depth, strings)
            }
            "cast" => {
                let t = self.cast_target();
                Expr::Cast(b(self.any(depth)), t)
            }
            "bit" => self.bit(depth),
            "strfn" => self.string_fn(depth),
            "temporal" => self.temporal_fn(depth.min(1) + 1),
            "numfn" => self.numeric_fn(depth),
            "strfn2" => self.string_fn2(depth),
            "numfn2" => self.numeric_fn2(depth),
            "rowcmp" => self.row_cmp(depth),
            "charset" => self.charset_expr(depth),
            "tzname" => self.tz_expr(depth.min(1)),
            "coll" => {
                let name = *self.rng.pick(&["COLLATION", "COERCIBILITY", "CHARSET"]);
                let arg = if self.rng.chance(1, 4) {
                    self.string_fn(depth)
                } else if self.rng.chance(1, 5) {
                    self.any(depth)
                } else {
                    self.string(depth + 1)
                };
                Expr::func(name, vec![arg])
            }
            _ => self.any(3),
        }
    }
}

fn is_string_col(c: &Col) -> bool {
    c.name.starts_with('v') || c.name == "bn"
}

/// The latin1 and ascii columns: only the character-set area reads them.
fn is_charset_col(c: &Col) -> bool {
    matches!(c.name, "l1" | "lb" | "a1")
}

fn is_temporal_col(c: &Col) -> bool {
    matches!(c.name, "dt" | "tm" | "dtm" | "dt0")
}

pub const AREAS: &[&str] = &[
    "arith", "cmp", "logic", "pred", "control", "cast", "bit", "mixed", "strfn", "numfn", "coll",
    "temporal", "strfn2", "numfn2", "rowcmp", "charset", "tzname",
];
