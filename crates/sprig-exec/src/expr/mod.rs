//! Expressions: the resolved tree a planner hands over, and its
//! compilation, once per plan, into a typed closure over row slots.

pub(crate) mod arith;
pub(crate) mod cast;
pub(crate) mod cmp;
pub(crate) mod control;
pub(crate) mod like;
pub(crate) mod row;

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::{ColumnType, DateTime, Value};

use crate::conv::{self, Info, IntVal};
use crate::error::{Result, SqlError, Warning};
use crate::num::Dec;
use crate::session::{Ctx, Persona, Session};
use crate::types::{Derivation, ExprType, Meta};

pub(crate) use cast::{agg_result_type, coerce_fn, string_node};
pub(crate) use cmp::CmpKind;

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `DIV`
    IntDiv,
    /// `%` and `MOD`
    Mod,
    /// `=`
    Eq,
    /// `<>` and `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `<=>`
    NullSafeEq,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `XOR`
    Xor,
    /// `&`
    BitAnd,
    /// `|`
    BitOr,
    /// `^`
    BitXor,
    /// `<<`
    Shl,
    /// `>>`
    Shr,
}

impl BinOp {
    /// The operator as SQL text.
    #[must_use]
    pub fn sql(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::IntDiv => "DIV",
            BinOp::Mod => "%",
            BinOp::Eq => "=",
            BinOp::Ne => "<>",
            BinOp::Lt => "<",
            BinOp::Le => "<=",
            BinOp::Gt => ">",
            BinOp::Ge => ">=",
            BinOp::NullSafeEq => "<=>",
            BinOp::And => "AND",
            BinOp::Or => "OR",
            BinOp::Xor => "XOR",
            BinOp::BitAnd => "&",
            BinOp::BitOr => "|",
            BinOp::BitXor => "^",
            BinOp::Shl => "<<",
            BinOp::Shr => ">>",
        }
    }
}

/// The target of a `CAST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CastTarget {
    /// `SIGNED`
    Signed,
    /// `UNSIGNED`
    Unsigned,
    /// `DECIMAL(precision, scale)`
    Decimal(u8, u8),
    /// `DOUBLE`
    Double,
    /// `CHAR` or `CHAR(n)`
    Char(Option<u32>),
    /// `BINARY` or `BINARY(n)`
    Binary(Option<u32>),
    /// `DATE`
    Date,
    /// `TIME(fsp)`
    Time(u8),
    /// `DATETIME(fsp)`
    DateTime(u8),
}

/// The truth value an `IS` test asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Truth {
    /// `IS TRUE`
    True,
    /// `IS FALSE`
    False,
    /// `IS UNKNOWN`
    Unknown,
}

/// A column a row source provides: its position in the row and its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    /// The position in the row.
    pub slot: usize,
    /// The declared type.
    pub ty: ColumnType,
    /// Whether the column can hold `NULL`.
    pub nullable: bool,
    /// The name, for SQL text and messages.
    pub name: String,
}

/// A resolved expression. Names are already bound: a column is a slot, a
/// function is its upper-case name with its arguments.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `NULL`
    Null,
    /// `TRUE` or `FALSE`
    Bool(bool),
    /// An unsigned integer literal as written, for example `42`.
    Int(String),
    /// A decimal literal as written, for example `1.50`.
    Decimal(String),
    /// A floating-point literal as written, for example `1e0`.
    Double(String),
    /// A character-string literal in the connection character set.
    Str(String),
    /// A hexadecimal literal's bytes (`0x4142`, `X'4142'`).
    Hex(Vec<u8>),
    /// A column of the input row.
    Column(ColumnRef),
    /// The `?` parameter with this index (from 0).
    Param(usize),
    /// Unary minus.
    Neg(Box<Expr>),
    /// `NOT`
    Not(Box<Expr>),
    /// `~`
    BitNot(Box<Expr>),
    /// A binary operator.
    Binary(BinOp, Box<Expr>, Box<Expr>),
    /// `IS [NOT] NULL`; the flag is the `NOT`.
    IsNull(Box<Expr>, bool),
    /// `IS [NOT] TRUE | FALSE | UNKNOWN`; the flag is the `NOT`.
    IsTruth(Box<Expr>, Truth, bool),
    /// `[NOT] IN (list)`; the flag is the `NOT`.
    In(Box<Expr>, Vec<Expr>, bool),
    /// `[NOT] BETWEEN low AND high`; the flag is the `NOT`.
    Between(Box<Expr>, Box<Expr>, Box<Expr>, bool),
    /// `[NOT] LIKE pattern [ESCAPE escape]`; the flag is the `NOT`.
    Like(Box<Expr>, Box<Expr>, Option<Box<Expr>>, bool),
    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`
    Case {
        /// The operand of a simple `CASE`.
        operand: Option<Box<Expr>>,
        /// The `WHEN` and `THEN` pairs.
        whens: Vec<(Expr, Expr)>,
        /// The `ELSE` result.
        otherwise: Option<Box<Expr>>,
    },
    /// `CAST(expr AS target)`
    Cast(Box<Expr>, CastTarget),
    /// `expr COLLATE collation`
    Collate(Box<Expr>, Collation),
    /// A built-in function call: upper-case name and arguments.
    Func(String, Vec<Expr>),
    /// A keyword argument of a function, printed bare: an interval unit, a
    /// `TRIM` side.
    Keyword(String),
}

impl Expr {
    /// An integer literal.
    #[must_use]
    pub fn int(v: i64) -> Self {
        if v < 0 {
            Expr::Neg(Box::new(Expr::Int(v.unsigned_abs().to_string())))
        } else {
            Expr::Int(v.to_string())
        }
    }

    /// A string literal.
    #[must_use]
    pub fn str(s: &str) -> Self {
        Expr::Str(s.to_owned())
    }

    /// A column reference.
    #[must_use]
    pub fn column(slot: usize, name: &str, ty: ColumnType, nullable: bool) -> Self {
        Expr::Column(ColumnRef {
            slot,
            ty,
            nullable,
            name: name.to_owned(),
        })
    }

    /// A binary operation.
    #[must_use]
    pub fn binary(op: BinOp, left: Expr, right: Expr) -> Self {
        Expr::Binary(op, Box::new(left), Box::new(right))
    }

    /// A function call.
    #[must_use]
    pub fn func(name: &str, args: Vec<Expr>) -> Self {
        Expr::Func(name.to_ascii_uppercase(), args)
    }

    /// The expression as SQL text both servers accept, fully parenthesized.
    #[must_use]
    pub fn to_sql(&self) -> String {
        let mut out = String::new();
        self.write_sql(&mut out);
        out
    }

    fn write_sql(&self, out: &mut String) {
        match self {
            Expr::Null => out.push_str("NULL"),
            Expr::Bool(b) => out.push_str(if *b { "TRUE" } else { "FALSE" }),
            Expr::Int(t) | Expr::Decimal(t) | Expr::Double(t) => out.push_str(t),
            Expr::Str(s) => write_string_literal(out, s),
            Expr::Hex(b) => {
                out.push_str("0x");
                for x in b {
                    let _ = write!(out, "{x:02X}");
                }
            }
            Expr::Column(c) => {
                let _ = write!(out, "`{}`", c.name);
            }
            Expr::Param(_) => out.push('?'),
            Expr::Neg(e) => {
                if matches!(**e, Expr::Int(_) | Expr::Decimal(_) | Expr::Double(_)) {
                    out.push('-');
                    e.write_sql(out);
                } else {
                    out.push_str("(-");
                    e.write_sql(out);
                    out.push(')');
                }
            }
            Expr::Not(e) => {
                out.push_str("(NOT ");
                e.write_sql(out);
                out.push(')');
            }
            Expr::BitNot(e) => {
                out.push_str("(~");
                e.write_sql(out);
                out.push(')');
            }
            Expr::Binary(op, a, b) => {
                out.push('(');
                a.write_sql(out);
                let _ = write!(out, " {} ", op.sql());
                b.write_sql(out);
                out.push(')');
            }
            Expr::IsNull(e, not) => {
                out.push('(');
                e.write_sql(out);
                out.push_str(if *not { " IS NOT NULL)" } else { " IS NULL)" });
            }
            Expr::IsTruth(e, t, not) => {
                out.push('(');
                e.write_sql(out);
                out.push_str(if *not { " IS NOT " } else { " IS " });
                out.push_str(match t {
                    Truth::True => "TRUE)",
                    Truth::False => "FALSE)",
                    Truth::Unknown => "UNKNOWN)",
                });
            }
            Expr::In(e, list, not) => {
                out.push('(');
                e.write_sql(out);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                write_list(out, list);
                out.push_str("))");
            }
            Expr::Between(e, lo, hi, not) => {
                out.push('(');
                e.write_sql(out);
                out.push_str(if *not { " NOT BETWEEN " } else { " BETWEEN " });
                lo.write_sql(out);
                out.push_str(" AND ");
                hi.write_sql(out);
                out.push(')');
            }
            Expr::Like(e, p, esc, not) => {
                out.push('(');
                e.write_sql(out);
                out.push_str(if *not { " NOT LIKE " } else { " LIKE " });
                p.write_sql(out);
                if let Some(esc) = esc {
                    out.push_str(" ESCAPE ");
                    esc.write_sql(out);
                }
                out.push(')');
            }
            Expr::Case {
                operand,
                whens,
                otherwise,
            } => {
                out.push_str("(CASE");
                if let Some(o) = operand {
                    out.push(' ');
                    o.write_sql(out);
                }
                for (w, t) in whens {
                    out.push_str(" WHEN ");
                    w.write_sql(out);
                    out.push_str(" THEN ");
                    t.write_sql(out);
                }
                if let Some(e) = otherwise {
                    out.push_str(" ELSE ");
                    e.write_sql(out);
                }
                out.push_str(" END)");
            }
            Expr::Cast(e, t) => {
                out.push_str("CAST(");
                e.write_sql(out);
                out.push_str(" AS ");
                match t {
                    CastTarget::Signed => out.push_str("SIGNED"),
                    CastTarget::Unsigned => out.push_str("UNSIGNED"),
                    CastTarget::Decimal(p, s) => {
                        let _ = write!(out, "DECIMAL({p},{s})");
                    }
                    CastTarget::Double => out.push_str("DOUBLE"),
                    CastTarget::Char(None) => out.push_str("CHAR"),
                    CastTarget::Char(Some(n)) => {
                        let _ = write!(out, "CHAR({n})");
                    }
                    CastTarget::Binary(None) => out.push_str("BINARY"),
                    CastTarget::Binary(Some(n)) => {
                        let _ = write!(out, "BINARY({n})");
                    }
                    CastTarget::Date => out.push_str("DATE"),
                    CastTarget::Time(f) => {
                        let _ = write!(out, "TIME({f})");
                    }
                    CastTarget::DateTime(f) => {
                        let _ = write!(out, "DATETIME({f})");
                    }
                }
                out.push(')');
            }
            Expr::Collate(e, c) => {
                out.push('(');
                e.write_sql(out);
                let _ = write!(out, " COLLATE {})", c.name());
            }
            Expr::Func(name, args) => crate::func::write_sql(out, name, args),
            Expr::Keyword(k) => out.push_str(k),
        }
    }
}

pub(crate) fn write_list(out: &mut String, list: &[Expr]) {
    for (i, e) in list.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        e.write_sql(out);
    }
}

fn write_string_literal(out: &mut String, s: &str) {
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            '\u{1a}' => out.push_str("\\Z"),
            c => out.push(c),
        }
    }
    out.push('\'');
}

pub(crate) type EvalFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Value> + Send + Sync>;

/// A compiled sub-expression: its static type and its closure.
#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) ty: ExprType,
    pub(crate) f: EvalFn,
    /// The value, when the expression is a literal.
    pub(crate) lit: Option<Value>,
    /// The expression's text, for messages.
    pub(crate) text: Arc<str>,
    /// Reading this node's strings as numbers raises no condition.
    pub(crate) quiet: bool,
    /// A double computed by an operator or function (see [`Info`]).
    pub(crate) strict_real: bool,
    /// A predicate (comparison, logic, `IN`, `LIKE`, `IS`).
    pub(crate) pred: bool,
    /// Whether the expression reads no row.
    pub(crate) konst: bool,
    /// How the value is computed when a number is wanted, where that
    /// differs from converting the value.
    pub(crate) alt: Option<Arc<Alt>>,
    /// MariaDB sends this string to the client as if it were latin1: a
    /// result's bytes above ASCII arrive encoded once more (`RPAD` of a
    /// number). Only the statement's own result shows it.
    pub(crate) latin1_send: bool,
    /// The expression holds a cast to `CHAR(n)` or `BINARY(n)`, which
    /// both servers compute again for every row (warnings and all), so it
    /// is not computed once per statement even when constant.
    pub(crate) sized: bool,
}

pub(crate) type F64Fn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<f64>> + Send + Sync>;
pub(crate) type DecFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<Dec>> + Send + Sync>;
pub(crate) type IntFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<IntVal>> + Send + Sync>;
pub(crate) type BoolFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<bool>> + Send + Sync>;

/// Other ways to compute a node's value in a numeric context. MariaDB's
/// `LEAST('10', '9') + 0` compares numbers though `LEAST('10', '9')`
/// compares strings; MySQL's `IF` and `CASE` ask the chosen branch for its
/// own number instead of converting their string result.
#[derive(Clone, Default)]
pub(crate) struct Alt {
    pub(crate) f64: Option<F64Fn>,
    pub(crate) dec: Option<DecFn>,
    pub(crate) int: Option<IntFn>,
    pub(crate) truth: Option<BoolFn>,
    /// The value as a string (a `Str`, `Bytes` or `Null` value), where a
    /// string reader gets something else than the value's own text:
    /// MySQL's `IF` and `CASE` hand over the chosen branch's text.
    pub(crate) text: Option<EvalFn>,
    /// The value a comparison of dates or times reads, where it is not
    /// the node's value (MariaDB's cast of what is no date).
    pub(crate) date_key: Option<EvalFn>,
    /// The date or datetime behind a text value, for a reader that wants
    /// a date or time: the text answer of `DATE_ADD` is read as the date
    /// it was computed as, not parsed again.
    pub(crate) temporal: Option<EvalFn>,
    /// The fraction digits such a text value brings as a date or time.
    pub(crate) temporal_fsp: Option<u8>,
    /// The value a reader that wants a time of day gets, where it is not
    /// the node's value (MariaDB's cast to a date).
    pub(crate) as_time: Option<EvalFn>,
}

impl Node {
    pub(crate) fn new(
        ty: ExprType,
        text: impl Into<Arc<str>>,
        f: impl Fn(&mut Ctx<'_>, &[Value]) -> Result<Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            ty,
            f: Arc::new(f),
            lit: None,
            text: text.into(),
            quiet: false,
            strict_real: false,
            pred: false,
            konst: false,
            alt: None,
            latin1_send: false,
            sized: false,
        }
    }

    pub(crate) fn literal(ty: ExprType, text: impl Into<Arc<str>>, v: Value) -> Self {
        let held = v.clone();
        Self {
            ty,
            f: Arc::new(move |_, _| Ok(held.clone())),
            lit: Some(v),
            text: text.into(),
            quiet: false,
            strict_real: false,
            pred: false,
            konst: false,
            alt: None,
            latin1_send: false,
            sized: false,
        }
    }

    pub(crate) fn eval(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Value> {
        (self.f)(ctx, row)
    }

    /// What a consumer needs to know to convert this node's values.
    pub(crate) fn info(&self) -> Info {
        Info {
            hex: self.ty.hex,
            quiet: self.quiet,
            strict_real: self.strict_real,
            literal: self.lit.is_some(),
            column: self.ty.column,
        }
    }

    /// The value for a reader that wants a date or time.
    pub(crate) fn temporal(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Value> {
        match self.alt.as_ref().and_then(|a| a.temporal.as_ref()) {
            Some(f) => f(ctx, row),
            None => self.eval(ctx, row),
        }
    }

    /// The value for a reader that wants a time of day.
    pub(crate) fn time_arg(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Value> {
        match self.alt.as_ref().and_then(|a| a.as_time.as_ref()) {
            Some(f) => f(ctx, row),
            None => self.temporal(ctx, row),
        }
    }

    /// The value as a double.
    pub(crate) fn f64(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<f64>> {
        if let Some(f) = self.alt.as_ref().and_then(|a| a.f64.as_ref()) {
            return f(ctx, row);
        }
        let v = self.eval(ctx, row)?;
        Ok(conv::conv_f64(ctx, &v, self.info()))
    }

    /// The value as a decimal.
    pub(crate) fn dec(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<Dec>> {
        if let Some(f) = self.alt.as_ref().and_then(|a| a.dec.as_ref()) {
            return f(ctx, row);
        }
        let v = self.eval(ctx, row)?;
        Ok(conv::conv_dec(ctx, &v, self.info()))
    }

    /// The value as an integer.
    pub(crate) fn int(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<IntVal>> {
        if let Some(f) = self.alt.as_ref().and_then(|a| a.int.as_ref()) {
            return f(ctx, row);
        }
        let v = self.eval(ctx, row)?;
        conv::conv_int(ctx, &v, self.info(), &self.text)
    }

    /// Whether the value is true.
    pub(crate) fn truth(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<bool>> {
        if let Some(f) = self.alt.as_ref().and_then(|a| a.truth.as_ref()) {
            return f(ctx, row);
        }
        let v = self.eval(ctx, row)?;
        Ok(conv::conv_bool(ctx, &v, self.info()))
    }

    /// The value of a constant expression as an integer, computed while
    /// the statement is prepared, as the servers do for a count that
    /// decides a result's type or length: its conditions are raised there
    /// (and again when the expression runs), and an error fails the
    /// statement. `None` when the node isn't constant or is `NULL`.
    pub(crate) fn const_i64(&self, env: Env<'_>) -> Result<Option<i64>> {
        if let Some(v) = self.lit_i64() {
            return Ok(Some(v));
        }
        if !self.konst {
            return Ok(None);
        }
        let i = env.at_prepare(|ctx| self.int_typed(ctx, &[]))?;
        Ok(i.map(|i| {
            if i.unsigned {
                i64::try_from(i.v as u64).unwrap_or(i64::MAX)
            } else {
                i.v
            }
        }))
    }

    /// The value as an integer with the signedness of the node's type: a
    /// string's digits beyond the signed range (which read as the unsigned
    /// maximum when they overflow) wrap around to a negative number, as a
    /// function's integer argument reads them on both servers.
    pub(crate) fn int_typed(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<IntVal>> {
        let i = self.int(ctx, row)?;
        Ok(i.map(|i| {
            if i.unsigned && !self.ty.unsigned && !self.ty.hex {
                IntVal::signed(i.v)
            } else {
                i
            }
        }))
    }

    /// [`Node::int_typed`] as an `i64`: an unsigned value beyond the
    /// signed range sticks at the signed maximum.
    pub(crate) fn int_arg(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<i64>> {
        Ok(self.int_typed(ctx, row)?.map(|i| {
            if i.unsigned {
                i64::try_from(i.v as u64).unwrap_or(i64::MAX)
            } else {
                i.v
            }
        }))
    }

    /// The literal as an integer, if the node is an integer literal.
    pub(crate) fn lit_i64(&self) -> Option<i64> {
        match self.lit.as_ref()? {
            Value::Int(i) => Some(*i),
            Value::UInt(u) => Some(i64::try_from(*u).unwrap_or(i64::MAX)),
            _ => None,
        }
    }
}

/// What compilation reads from the session, and where it leaves the
/// conditions raised while the statement is prepared.
#[derive(Clone, Copy)]
pub(crate) struct Env<'a> {
    pub(crate) s: &'a Session,
    /// The conditions of the prepare step; `None` drops them.
    pub(crate) prep: Option<&'a std::cell::RefCell<Vec<Warning>>>,
    /// The prepare step's negative zeros and wide decimals (see
    /// [`Ctx::negative_zeros`]); `None` drops them.
    pub(crate) counts: Option<&'a std::cell::Cell<[usize; 2]>>,
}

impl Env<'_> {
    /// Evaluates something the way the servers do while they prepare a
    /// statement: the conditions it raises are the statement's first.
    pub(crate) fn at_prepare<T>(&self, f: impl FnOnce(&mut Ctx<'_>) -> T) -> T {
        let mut ctx = Ctx::new(self.s);
        let out = f(&mut ctx);
        if let Some(sink) = self.prep {
            sink.borrow_mut().extend(ctx.take_warnings());
        }
        if let Some(c) = self.counts {
            let [nz, wd] = c.get();
            c.set([nz + ctx.negative_zeros, wd + ctx.wide_decimals]);
        }
        out
    }

    pub(crate) fn maria(&self) -> bool {
        self.s.persona.is_maria()
    }

    pub(crate) fn conn(&self) -> Collation {
        self.s.collation_connection
    }

    /// The most digits after the point a `DECIMAL` has: 38 on MariaDB, 30
    /// on MySQL.
    pub(crate) fn max_scale(&self) -> u32 {
        if self.maria() { 38 } else { 30 }
    }

    /// `DECIMAL(prec, scale)` within the persona's limits.
    pub(crate) fn decimal(&self, prec: u32, scale: u32, unsigned: bool) -> ExprType {
        let cut = scale.saturating_sub(self.max_scale());
        ExprType::decimal(prec.saturating_sub(cut), scale - cut, unsigned)
    }
}

/// An expression compiled for one session's settings: evaluate it against
/// rows as often as needed.
#[derive(Clone)]
pub struct Compiled {
    node: Node,
    persona: Persona,
    prepare_warnings: Vec<Warning>,
    /// Negative zeros and wide decimals met while preparing.
    prepare_counts: [usize; 2],
}

impl std::fmt::Debug for Compiled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compiled")
            .field("ty", &self.node.ty)
            .field("text", &self.node.text)
            .finish()
    }
}

impl Compiled {
    /// The expression's static type.
    #[must_use]
    pub fn ty(&self) -> &ExprType {
        &self.node.ty
    }

    /// The result-set metadata a client is told for this expression.
    #[must_use]
    pub fn meta(&self) -> Meta {
        self.node.ty.meta(self.persona)
    }

    /// The conditions the servers raise while they prepare a statement
    /// with this expression (constants they evaluate ahead of the rows).
    /// The caller puts them in front of the statement's list.
    #[must_use]
    pub fn prepare_warnings(&self) -> &[Warning] {
        &self.prepare_warnings
    }

    /// How many negative zeros (see [`Ctx::negative_zeros`]) the
    /// constants computed while preparing produced.
    #[must_use]
    pub fn prepare_negative_zeros(&self) -> usize {
        self.prepare_counts[0]
    }

    /// How many decimals wider than the servers' working space (see
    /// [`Ctx::wide_decimals`]) the constants computed while preparing
    /// produced.
    #[must_use]
    pub fn prepare_wide_decimals(&self) -> usize {
        self.prepare_counts[1]
    }

    /// The value for one row.
    pub fn eval(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Value> {
        self.node.eval(ctx, row)
    }

    /// The value for one row as the text protocol sends it; `None` is
    /// `NULL`.
    pub fn eval_text(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<Bytes>> {
        let v = self.node.eval(ctx, row)?;
        let text = value_text(&v, &self.node.ty, self.persona);
        let latin1_typed = self.node.ty.kind == crate::types::Kind::Str
            && self.node.ty.coll.charset() == Charset::Latin1;
        Ok(match text {
            Some(b) if self.node.latin1_send && !latin1_typed && !b.is_ascii() => {
                Some(Bytes::from(crate::func::latin1_as_utf8(&b)))
            }
            other => other,
        })
    }

    /// Whether the value for one row is true (a `WHERE` or `ON` test):
    /// `NULL` counts as false.
    pub fn eval_bool(&self, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<bool> {
        Ok(self.node.truth(ctx, row)?.unwrap_or(false))
    }

    pub(crate) fn node(&self) -> &Node {
        &self.node
    }
}

/// Compiles an expression for a session: types it as the session's persona
/// does and builds its closure. Errors a server raises when it prepares a
/// statement (an illegal mix of collations, a wrong argument count) are
/// returned here.
pub fn compile(expr: &Expr, session: &Session) -> Result<Compiled> {
    let prep = std::cell::RefCell::new(Vec::new());
    let counts = std::cell::Cell::new([0, 0]);
    let env = Env {
        s: session,
        prep: Some(&prep),
        counts: Some(&counts),
    };
    let node = build(expr, env)?;
    let [nz, wd] = counts.get();
    Ok(Compiled {
        node,
        persona: session.persona,
        prepare_warnings: prep.into_inner(),
        prepare_counts: [nz, wd],
    })
}

/// The text value of `v` under static type `ty`, as `persona` prints it;
/// `None` is `NULL`.
#[must_use]
pub fn value_text(v: &Value, ty: &ExprType, persona: Persona) -> Option<Bytes> {
    // A latin1 string goes to the client in its utf8mb4.
    if let Value::Str(b) = v {
        let cs = ty.coll.charset();
        if ty.kind == crate::types::Kind::Str && matches!(cs, Charset::Latin1 | Charset::Ascii) {
            let (out, _) = crate::charset::convert(b, cs, Charset::Utf8mb4);
            return Some(Bytes::from(out));
        }
    }
    conv::to_text(v, ty, persona.is_maria())
}

static NEXT_CONST: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Functions whose value differs from call to call.
const VOLATILE: &[&str] = &["RAND", "UUID", "UUID_SHORT", "SYSDATE", "RANDOM_BYTES"];

/// Whether an expression reads no row: its value is the same for every row
/// of one statement.
pub(crate) fn is_const(e: &Expr) -> bool {
    match e {
        Expr::Column(_) => false,
        Expr::Null
        | Expr::Bool(_)
        | Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Double(_)
        | Expr::Str(_)
        | Expr::Hex(_)
        | Expr::Param(_)
        | Expr::Keyword(_) => true,
        Expr::Neg(a)
        | Expr::Not(a)
        | Expr::BitNot(a)
        | Expr::IsNull(a, _)
        | Expr::IsTruth(a, _, _)
        | Expr::Cast(a, _)
        | Expr::Collate(a, _) => is_const(a),
        Expr::Binary(_, a, b) => is_const(a) && is_const(b),
        Expr::In(a, list, _) => is_const(a) && list.iter().all(is_const),
        Expr::Between(a, b, c, _) => is_const(a) && is_const(b) && is_const(c),
        Expr::Like(a, b, c, _) => {
            is_const(a) && is_const(b) && c.as_ref().is_none_or(|c| is_const(c))
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            operand.as_ref().is_none_or(|o| is_const(o))
                && whens.iter().all(|(w, t)| is_const(w) && is_const(t))
                && otherwise.as_ref().is_none_or(|o| is_const(o))
        }
        Expr::Func(name, args) => !VOLATILE.contains(&name.as_str()) && args.iter().all(is_const),
    }
}

/// Whether an expression holds a cast to `CHAR(n)` or `BINARY(n)`.
fn has_sized_cast(e: &Expr) -> bool {
    match e {
        Expr::Cast(_, CastTarget::Char(Some(_)) | CastTarget::Binary(Some(_))) => true,
        Expr::Neg(a)
        | Expr::Not(a)
        | Expr::BitNot(a)
        | Expr::IsNull(a, _)
        | Expr::IsTruth(a, _, _)
        | Expr::Cast(a, _)
        | Expr::Collate(a, _) => has_sized_cast(a),
        Expr::Binary(_, a, b) => has_sized_cast(a) || has_sized_cast(b),
        Expr::In(a, list, _) => has_sized_cast(a) || list.iter().any(has_sized_cast),
        Expr::Between(a, b, c, _) => has_sized_cast(a) || has_sized_cast(b) || has_sized_cast(c),
        Expr::Like(a, b, c, _) => {
            has_sized_cast(a) || has_sized_cast(b) || c.as_ref().is_some_and(|c| has_sized_cast(c))
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            operand.as_ref().is_some_and(|o| has_sized_cast(o))
                || whens
                    .iter()
                    .any(|(w, t)| has_sized_cast(w) || has_sized_cast(t))
                || otherwise.as_ref().is_some_and(|o| has_sized_cast(o))
        }
        Expr::Func(_, args) => args.iter().any(has_sized_cast),
        _ => false,
    }
}

/// Compiles one sub-expression. With constant folding on, a constant
/// sub-expression that isn't a literal is computed once per statement.
pub(crate) fn build(expr: &Expr, env: Env<'_>) -> Result<Node> {
    let mut n = build_inner(expr, env)?;
    n.pred = matches!(
        expr,
        Expr::Not(_)
            | Expr::IsNull(..)
            | Expr::IsTruth(..)
            | Expr::In(..)
            | Expr::Between(..)
            | Expr::Like(..)
            | Expr::Binary(
                BinOp::Eq
                    | BinOp::Ne
                    | BinOp::Lt
                    | BinOp::Le
                    | BinOp::Gt
                    | BinOp::Ge
                    | BinOp::NullSafeEq
                    | BinOp::And
                    | BinOp::Or
                    | BinOp::Xor,
                _,
                _
            )
    ) || matches!(expr, Expr::Func(f, _) if f == "REGEXP" || f == "REGEXP_LIKE");
    n.konst = is_const(expr);
    n.sized = has_sized_cast(expr);
    // MySQL settles a constant predicate once per statement that reads
    // tables.
    if n.konst && n.pred {
        n = cached(n, env);
    }
    Ok(n)
}

/// The node's strings in the character set of collation `to`: nothing
/// changes when the sets agree, for values that aren't strings (a
/// number's text is ASCII), and when either side is binary.
pub(crate) fn recode_node(n: Node, to: Collation) -> Node {
    let Some((from, tc)) = cmp::recode_for(&n.ty, to) else {
        return n;
    };
    let mut ty = n.ty;
    ty.coll = to;
    let recode = move |v: Value| match v {
        Value::Str(b) => Value::Str(crate::charset::convert(&b, from, tc).0.into()),
        other => other,
    };
    if let Some(v) = n.lit.clone() {
        let mut out = Node::literal(ty, n.text.clone(), recode(v));
        out.konst = n.konst;
        return out;
    }
    let konst = n.konst;
    let text = n.text.clone();
    let mut out = Node::new(ty, text, move |ctx, row| Ok(recode(n.eval(ctx, row)?)));
    out.konst = konst;
    out
}

/// Whether the strings of `n` may be converted to the character set of
/// `to` where collations merge: always into utf8mb4 (except a literal
/// whose bytes are no text of its own set), and otherwise when `n` holds
/// only ASCII, or is a literal that converts without loss.
pub(crate) fn convertible(n: &Node, to: Collation, env: Env<'_>) -> bool {
    let Some((from, tc)) = cmp::recode_for(&n.ty, to) else {
        return true;
    };
    // A constant's value decides (computed aside, its conditions dropped).
    let value = match &n.lit {
        Some(v) => Some(v.clone()),
        None if n.konst => {
            let mut ctx = Ctx::new(env.s);
            n.eval(&mut ctx, &[]).ok()
        }
        None => None,
    };
    if let Some(v) = value {
        return match v {
            Value::Str(b) => {
                let bad_source = from == Charset::Ascii && !b.is_ascii();
                !bad_source && crate::charset::convert(&b, from, tc).1.is_none()
            }
            _ => true,
        };
    }
    tc == Charset::Utf8mb4 || n.ty.ascii
}

/// [`recode_node`] for the operands of `op` merged under `dtc`, refusing
/// (error 1267 and its siblings) an operand that cannot be converted.
pub(crate) fn recode_checked(
    nodes: Vec<Node>,
    to: Collation,
    op: &str,
    env: Env<'_>,
) -> Result<Vec<Node>> {
    if nodes.iter().any(|n| !convertible(n, to, env)) {
        let dtcs: Vec<crate::types::Dtc> = nodes
            .iter()
            .map(|n| crate::types::Dtc::of(&n.ty, env.conn()))
            .collect();
        return Err(crate::types::illegal_mix(&dtcs, op));
    }
    Ok(nodes.into_iter().map(|n| recode_node(n, to)).collect())
}

/// For a statement that reads tables on MySQL: the node computed once per
/// statement, if it is constant and not a literal. MySQL does this for
/// constant predicates and for the constant operands of comparisons;
/// MariaDB computes them for every row.
pub(crate) fn cached(mut n: Node, env: Env<'_>) -> Node {
    if !env.s.fold_constants
        || env.maria()
        || !n.konst
        || n.sized
        || n.lit.is_some()
        || n.alt.is_some()
    {
        return n;
    }
    let id = NEXT_CONST.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let inner = n.f.clone();
    n.f = Arc::new(move |ctx, row| {
        if let Some(v) = ctx.consts.get(&id) {
            return Ok(v.clone());
        }
        let v = inner(ctx, row)?;
        ctx.consts.insert(id, v.clone());
        Ok(v)
    });
    n
}

fn build_inner(expr: &Expr, env: Env<'_>) -> Result<Node> {
    match expr {
        Expr::Null => Ok(Node::literal(ExprType::null(), "NULL", Value::Null)),
        Expr::Bool(b) => {
            let ty = ExprType {
                prec: 1,
                ..ExprType::int(if env.maria() { 5 } else { 1 }, false)
            };
            Ok(Node::literal(
                ty,
                if *b { "TRUE" } else { "FALSE" },
                Value::Int(i64::from(*b)),
            ))
        }
        Expr::Int(t) => lit_int(t, false, env),
        Expr::Decimal(t) => lit_decimal(t, false, env),
        Expr::Double(t) => lit_double(t, false, env),
        Expr::Str(s) => {
            let coll = env.conn();
            let ty = ExprType {
                ascii: s.is_ascii(),
                ..ExprType::string(s.chars().count() as u32, coll, Derivation::Coercible)
            };
            let v = match coll.charset() {
                Charset::Binary => Value::bytes(s.as_bytes()),
                Charset::Utf8mb4 => Value::str(s),
                // The statement's text, in the client's utf8mb4, is
                // converted to the connection's character set.
                cs => Value::Str(
                    crate::charset::convert(s.as_bytes(), Charset::Utf8mb4, cs)
                        .0
                        .into(),
                ),
            };
            let mut text = String::new();
            write_string_literal(&mut text, s);
            Ok(Node::literal(ty, text, v))
        }
        Expr::Hex(b) => {
            let ty = ExprType {
                unsigned: true,
                dec: 0,
                hex: true,
                ..ExprType::string(b.len() as u32, Collation::Binary, Derivation::Coercible)
            };
            Ok(Node::literal(ty, expr.to_sql(), Value::bytes(b)))
        }
        Expr::Column(c) => Ok(column(c, env)),
        Expr::Param(i) => {
            let i = *i;
            // A parameter is typed as a nullable string of unknown length;
            // its value decides how it converts at run time.
            let ty = ExprType::string(1 << 20, env.conn(), Derivation::Coercible).nullable();
            Ok(Node::new(ty, "?", move |ctx, _| {
                Ok(ctx.params.get(i).cloned().unwrap_or(Value::Null))
            }))
        }
        Expr::Neg(e) => match &**e {
            Expr::Int(t) => lit_int(t, true, env),
            Expr::Decimal(t) => lit_decimal(t, true, env),
            Expr::Double(t) => lit_double(t, true, env),
            // MariaDB's parser folds a sign into a number: `-(-5)` is
            // the literal 5 again.
            Expr::Neg(inner) if env.maria() => match &**inner {
                Expr::Int(t) if t.trim_start_matches('0') == "9223372036854775808" => {
                    lit_decimal(t, false, env)
                }
                Expr::Int(t) => lit_int(t, false, env),
                Expr::Decimal(t) => lit_decimal(t, false, env),
                Expr::Double(t) => lit_double(t, false, env),
                _ => arith::neg(build(e, env)?, env),
            },
            other => arith::neg(build(other, env)?, env),
        },
        // Both servers turn NOT (x IS NULL) into x IS NOT NULL, and the
        // other way round (so `NOT (x IS NOT NULL)` of an expression that
        // cannot be NULL is false without evaluating it).
        Expr::Not(e) if matches!(**e, Expr::IsNull(..)) => match &**e {
            Expr::IsNull(inner, not) => is_null(build(inner, env)?, !*not, env),
            _ => build(e, env),
        },
        Expr::Not(e) => {
            let a = build(e, env)?;
            let ty = ExprType::boolean().with_null(a.ty.nullable);
            let text = format!("(not {})", a.text);
            Ok(Node::new(ty, text, move |ctx, row| {
                Ok(match a.truth(ctx, row)? {
                    None => Value::Null,
                    Some(b) => Value::Int(i64::from(!b)),
                })
            }))
        }
        Expr::BitNot(e) => Ok(arith::bit_not(build(e, env)?, env)),
        Expr::Binary(
            op @ (BinOp::Eq
            | BinOp::Ne
            | BinOp::Lt
            | BinOp::Le
            | BinOp::Gt
            | BinOp::Ge
            | BinOp::NullSafeEq),
            a,
            b,
        ) if row::is_row(a) || row::is_row(b) => row::compare(*op, a, b, env),
        Expr::Binary(op, a, b) => {
            let (a, b) = (build(a, env)?, build(b, env)?);
            match op {
                BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::IntDiv | BinOp::Mod => {
                    Ok(arith::binary(*op, a, b, env))
                }
                BinOp::Eq
                | BinOp::Ne
                | BinOp::Lt
                | BinOp::Le
                | BinOp::Gt
                | BinOp::Ge
                | BinOp::NullSafeEq => cmp::compare(*op, a, b, env),
                BinOp::And | BinOp::Or | BinOp::Xor => Ok(logic(*op, a, b)),
                BinOp::BitAnd | BinOp::BitOr | BinOp::BitXor | BinOp::Shl | BinOp::Shr => {
                    Ok(arith::bit_op(*op, a, b, env))
                }
            }
        }
        Expr::IsNull(e, not) => is_null(build(e, env)?, *not, env),
        Expr::IsTruth(e, Truth::Unknown, not) => is_null(build(e, env)?, *not, env),
        Expr::IsTruth(e, t, not) => {
            let a = build(e, env)?;
            let (t, not) = (*t, *not);
            let text = format!("({} is {t:?})", a.text);
            Ok(Node::new(ExprType::boolean(), text, move |ctx, row| {
                let b = a.truth(ctx, row)?;
                let hit = match t {
                    Truth::True => b == Some(true),
                    _ => b == Some(false),
                };
                Ok(Value::Int(i64::from(hit != not)))
            }))
        }
        Expr::In(e, list, not) if row::is_row(e) || list.iter().any(row::is_row) => {
            row::in_list(e, list, *not, env)
        }
        Expr::Between(e, lo, hi, _) if row::is_row(e) || row::is_row(lo) || row::is_row(hi) => {
            Err(row::operand_columns(1))
        }
        Expr::In(e, list, not) => {
            let a = build(e, env)?;
            let items = list
                .iter()
                .map(|x| build(x, env))
                .collect::<Result<Vec<_>>>()?;
            cmp::in_list(a, items, *not, env)
        }
        Expr::Between(e, lo, hi, not) => {
            cmp::between(build(e, env)?, build(lo, env)?, build(hi, env)?, *not, env)
        }
        Expr::Like(e, p, esc, not) => {
            let esc = match esc {
                Some(x) => Some(build(x, env)?),
                None => None,
            };
            like::like(build(e, env)?, build(p, env)?, esc, *not, env)
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            // The servers resolve the parts in their own order, which
            // decides which error a statement with several reports:
            // MariaDB the operand, every WHEN, every THEN, the ELSE; MySQL
            // each WHEN with its THEN, then the operand, then the ELSE.
            let opt = |o: &Option<Box<Expr>>| -> Result<Option<Node>> {
                match o {
                    Some(o) => Ok(Some(build(o, env)?)),
                    None => Ok(None),
                }
            };
            let (operand, pairs) = if env.maria() {
                let operand = opt(operand)?;
                let ws = whens
                    .iter()
                    .map(|(w, _)| build(w, env))
                    .collect::<Result<Vec<_>>>()?;
                let ts = whens
                    .iter()
                    .map(|(_, t)| build(t, env))
                    .collect::<Result<Vec<_>>>()?;
                (operand, ws.into_iter().zip(ts).collect::<Vec<_>>())
            } else {
                let mut pairs = Vec::with_capacity(whens.len());
                for (w, t) in whens {
                    pairs.push((build(w, env)?, build(t, env)?));
                }
                (opt(operand)?, pairs)
            };
            let otherwise = opt(otherwise)?;
            control::case(operand, pairs, otherwise, env)
        }
        Expr::Cast(e, target) => cast::cast(build(e, env)?, *target, env),
        Expr::Collate(e, coll) => {
            let a = build(e, env)?;
            // MariaDB lets `NULL` take any collation; MySQL doesn't.
            let from_null = a.ty.kind == crate::types::Kind::Null && env.maria();
            let a = string_node(a, env);
            if !from_null && a.ty.coll.charset() != coll.charset() {
                return Err(SqlError::new(
                    1253,
                    "42000",
                    format!(
                        "COLLATION '{}' is not valid for CHARACTER SET '{}'",
                        coll.name(),
                        charset_name(a.ty.coll.charset())
                    ),
                ));
            }
            let mut n = a;
            // MariaDB reads a collated value as a number from its string:
            // `GREATEST('aBc', '2') COLLATE utf8mb4_bin` is 'aBc' whoever
            // asks.
            if env.maria() {
                if let Some(alt) = &n.alt {
                    if alt.f64.is_some() || alt.dec.is_some() || alt.int.is_some() {
                        let mut own = (**alt).clone();
                        own.f64 = None;
                        own.dec = None;
                        own.int = None;
                        n.alt = Some(Arc::new(own));
                    }
                }
            }
            n.ty.coll = *coll;
            n.ty.deriv = Derivation::Explicit;
            n.ty.dec = crate::types::NOT_FIXED;
            n.ty.column = false;
            n.ty.cast_char = false;
            // Both servers report a collated value as nullable.
            n.ty.nullable = true;
            n.lit = None;
            Ok(n)
        }
        Expr::Func(name, args) => crate::func::build(name, args, env),
        Expr::Keyword(k) => Err(SqlError::new(
            1064,
            "42000",
            format!("You have an error in your SQL syntax near '{k}'"),
        )),
    }
}

pub(crate) fn charset_name(c: Charset) -> &'static str {
    match c {
        Charset::Binary => "binary",
        Charset::Ascii => "ascii",
        Charset::Latin1 => "latin1",
        Charset::Utf8mb4 => "utf8mb4",
    }
}

fn column(c: &ColumnRef, env: Env<'_>) -> Node {
    let ty = ExprType::of_column(&c.ty, c.nullable);
    let slot = c.slot;
    let text = format!("`{}`", c.name);
    let _ = env;
    match c.ty {
        ColumnType::Timestamp { .. } => Node::new(ty, text, move |ctx, row| {
            Ok(match row.get(slot) {
                Some(Value::Timestamp(t)) => {
                    if t.is_zero() {
                        Value::DateTime(DateTime::default())
                    } else {
                        let secs = i64::from(t.seconds);
                        Value::DateTime(crate::temporal::datetime_from_unix(
                            secs + ctx.session.zone_offset_at(secs),
                            t.micros,
                        ))
                    }
                }
                Some(v) => v.clone(),
                None => Value::Null,
            })
        }),
        _ => Node::new(ty, text, move |_, row| {
            Ok(row.get(slot).cloned().unwrap_or(Value::Null))
        }),
    }
}

fn lit_int(text: &str, neg: bool, env: Env<'_>) -> Result<Node> {
    let digits = text.trim_start_matches('0');
    let nd = digits.len().max(1) as u32;
    let shown = if neg {
        format!("-{text}")
    } else {
        text.to_owned()
    };
    let wide: Option<u128> = if digits.len() <= 38 {
        digits.parse::<u128>().ok().or(Some(0))
    } else {
        None
    };
    match wide {
        Some(v) if !neg && v <= i64::MAX as u128 => {
            let len = if env.maria() { nd } else { nd + 1 };
            let ty = ExprType {
                prec: nd as u8,
                ..ExprType::int(len, false)
            };
            Ok(Node::literal(ty, shown, Value::Int(v as i64)))
        }
        Some(v) if !neg && v <= u128::from(u64::MAX) => {
            let ty = ExprType {
                prec: nd as u8,
                ..ExprType::int(nd, true)
            };
            Ok(Node::literal(ty, shown, Value::UInt(v as u64)))
        }
        Some(v) if neg && v <= 1u128 << 63 => {
            let ty = ExprType {
                prec: nd as u8,
                ..ExprType::int(nd + 1, false)
            };
            Ok(Node::literal(
                ty,
                shown,
                Value::Int((v as i128).wrapping_neg() as i64),
            ))
        }
        _ => lit_decimal(text, neg, env),
    }
}

fn lit_decimal(text: &str, neg: bool, env: Env<'_>) -> Result<Node> {
    let shown = if neg {
        format!("-{text}")
    } else {
        text.to_owned()
    };
    let bad = || SqlError::new(1064, "42000", format!("Bad number '{text}'"));
    let d = Dec::parse(&shown).ok_or_else(bad)?;
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    let int_digits = int
        .trim_start_matches('0')
        .len()
        .max(usize::from(!int.is_empty()));
    if int_digits > 65 {
        // More digits than a decimal holds: the largest one, with a
        // warning raised while the statement is prepared.
        let maria = env.maria();
        let cut: String = text.chars().take(81).collect();
        env.at_prepare(|ctx| {
            if maria {
                ctx.warn(
                    1916,
                    "Got overflow when converting '' to DECIMAL. Value truncated",
                );
            } else {
                ctx.warn(1292, format!("Truncated incorrect DECIMAL value: '{cut}'"));
            }
        });
        let max = Dec::max_of(65, 0, neg);
        let v = max.to_decimal().ok_or_else(bad)?;
        return Ok(Node::literal(
            ExprType::decimal(65, 0, false),
            shown,
            Value::Decimal(v),
        ));
    }
    let prec = (int_digits + frac.len()) as u32;
    let ty = ExprType::decimal(prec, frac.len() as u32, false);
    let v = d.to_decimal().ok_or_else(bad)?;
    Ok(Node::literal(ty, shown, Value::Decimal(v)))
}

fn lit_double(text: &str, neg: bool, env: Env<'_>) -> Result<Node> {
    let v: f64 = text.parse().unwrap_or(f64::INFINITY);
    if !v.is_finite() {
        return Err(SqlError::new(
            1367,
            "22007",
            format!("Illegal double '{text}' value found during parsing"),
        ));
    }
    let v = if neg { -v } else { v };
    let len = if neg && !env.maria() {
        23
    } else {
        text.len() as u32 + u32::from(neg)
    };
    let ty = ExprType {
        len,
        ..ExprType::double()
    };
    let shown = if neg {
        format!("-{text}")
    } else {
        text.to_owned()
    };
    let mut n = Node::literal(ty, shown, Value::Double(v));
    // MySQL reads `-1e30` as the negation of `1e30`: a computed double.
    n.strict_real = neg && !env.maria();
    Ok(n)
}

/// `IS [NOT] NULL` and `IS [NOT] UNKNOWN`. `IS NULL` of an expression
/// that cannot be `NULL` is false without evaluating it, as on both
/// servers.
///
/// MySQL settles `IS NULL` of a constant while it prepares the statement:
/// the operand is evaluated once, there (an error in it fails the
/// statement even when no row would reach the test).
pub(crate) fn is_null(a: Node, not: bool, env: Env<'_>) -> Result<Node> {
    let text = format!("({} is {}null)", a.text, if not { "not " } else { "" });
    if !a.ty.nullable && !not {
        return Ok(Node::new(ExprType::boolean(), text, |_, _| {
            Ok(Value::Int(0))
        }));
    }
    // ... and IS NOT NULL of one that reads a row is true, unevaluated (a
    // constant is evaluated: its error fails the statement).
    if !a.ty.nullable && not && !a.konst {
        return Ok(Node::new(ExprType::boolean(), text, |_, _| {
            Ok(Value::Int(1))
        }));
    }
    if !env.maria() && !not && a.konst && a.lit.is_none() {
        let v = env.at_prepare(|ctx| a.eval(ctx, &[]))?;
        let answer = Value::Int(i64::from(v.is_null()));
        return Ok(Node::new(ExprType::boolean(), text, move |_, _| {
            Ok(answer.clone())
        }));
    }
    Ok(Node::new(ExprType::boolean(), text, move |ctx, row| {
        let v = a.eval(ctx, row)?;
        Ok(Value::Int(i64::from(v.is_null() != not)))
    }))
}

/// `AND`, `OR` and `XOR` in three-valued logic. `AND` stops at the first
/// false operand and `OR` at the first true one; a `NULL` operand doesn't
/// stop either.
fn logic(op: BinOp, a: Node, b: Node) -> Node {
    let ty = ExprType::boolean().with_null(a.ty.nullable || b.ty.nullable);
    let text = format!("({} {} {})", a.text, op.sql().to_ascii_lowercase(), b.text);
    Node::new(ty, text, move |ctx, row| {
        let x = a.truth(ctx, row)?;
        match op {
            BinOp::And => {
                if x == Some(false) {
                    return Ok(Value::Int(0));
                }
                let y = b.truth(ctx, row)?;
                Ok(match (x, y) {
                    (_, Some(false)) => Value::Int(0),
                    (Some(true), Some(true)) => Value::Int(1),
                    _ => Value::Null,
                })
            }
            BinOp::Or => {
                if x == Some(true) {
                    return Ok(Value::Int(1));
                }
                let y = b.truth(ctx, row)?;
                Ok(match (x, y) {
                    (_, Some(true)) => Value::Int(1),
                    (Some(false), Some(false)) => Value::Int(0),
                    _ => Value::Null,
                })
            }
            _ => {
                // A NULL left operand ends the evaluation.
                let Some(x) = x else {
                    return Ok(Value::Null);
                };
                Ok(match b.truth(ctx, row)? {
                    Some(y) => Value::Int(i64::from(x != y)),
                    None => Value::Null,
                })
            }
        }
    })
}
