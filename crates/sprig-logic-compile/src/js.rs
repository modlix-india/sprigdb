//! The JavaScript front end and the JavaScript subset it compiles.
//!
//! `oxc_parser` reads the source; this module lowers the compilable subset
//! into the shared IR, which the checks and the PL/SQL generator then
//! treat exactly as KIRun's. Anything outside the subset is a placement
//! reason (`js.*`), never an error: the version then runs in the engine
//! tier on QuickJS (`sprig-logic-js::js`), which defines the language.
//!
//! The two forms of a body are QuickJS's: a module whose default export
//! is the function (`import { db, sprig } from 'sprig:db'` is the only
//! import), or the function's body alone with the parameters in scope by
//! name (MySQL 9). Parameter types come from the declaration; a
//! validation's or after-commit trigger's come from its table, and its
//! code sees the row by name ([`compile_with`]).
//!
//! What compiles, and how it keeps JavaScript's meaning:
//!
//! - `const`, `let`, `var` with an initializer; `if`/`else`, `while`,
//!   `for (init; test; update)`, `for (const x of rows)`, `break`,
//!   `continue` (a `for` loop's update runs first), `return`, `throw`,
//!   blocks; `switch` without fall-through (cases with no statements
//!   share the next one's; `===` against each case, `default` last);
//! - numbers, strings, booleans, `null` and `undefined` (both `NULL`),
//!   template literals, object and array literals;
//! - arithmetic, where a possibly-`null` number reads as 0 as in
//!   JavaScript (`COALESCE(x, 0)`); `+` with text concatenates (`null`
//!   reads as `"null"`); `/` is always a double; `**`;
//! - `=== !==` (operands of different kinds are never equal), `== !=`
//!   against `null` or between values of one kind, `< <= > >=` on numbers
//!   (`null` as 0) or on two non-null strings (by code point);
//! - `&& || ??` and `?:`, with JavaScript's truthiness (`''`, `0` and
//!   `null` are false);
//! - `db.sql`, `db.sqlOne`, `db.exec` with a literal SQL text and an
//!   array literal of arguments, as a declaration's initializer or a
//!   statement; a `sqlOne` row's columns, an `exec`'s `affected`; the
//!   columns of each row of `for (const r of db.sql(...))`;
//! - `Math.abs floor ceil round sqrt pow max min` (`Math.random`,
//!   `Date.now` and other non-deterministic calls are the reason
//!   `js.nondeterministic`, which computed columns refuse); `length`,
//!   `toUpperCase`, `toLowerCase`, `includes`, `startsWith`, `endsWith`,
//!   `indexOf` on non-null text; `String(x)`;
//! - `return {fields}`, `return sprig.event(name, {fields})`,
//!   `sprig.reject(message)`, `throw new Error(message)`;
//! - `do { } while (test)`;
//! - `try`/`catch`/`finally` and `throw e` ([`Stmt::Try`] says what a
//!   `catch` catches); a `catch` reads `e.message` and `e.name` only when
//!   the `try` raises just the code's own error objects (a database
//!   error's text can differ between the tiers);
//! - helper functions (`function f() {}`, `const f = (x) => ...`), at
//!   module level or inside the function, inlined at each call in the
//!   scope they were declared in, so closures read and write the variables
//!   they see; one whose body is one `return` is an expression, any other
//!   runs as statements before the statement that calls it.
//!
//! Reading a column of a `sqlOne` row that may be `null` throws a
//! `TypeError` in JavaScript, so the compiled code checks the row first
//! and fails with 39865 (a guard before the statement) unless the code has
//! already tested it (`if (p === null) return ...`, `p !== null && ...`).
//! A read that is conditional on something else is a reason.
//!
//! Known differences (listed by `EXPLAIN FUNCTION`):
//! whole numbers past 2^63 overflow (39866) where JavaScript loses
//! precision; `length` and `indexOf` count code points; number-to-text is
//! exact for integers and up to 15 significant digits; `ctx.log` and
//! `console.log` lines are dropped.

use std::collections::{BTreeMap, BTreeSet};

use oxc_allocator::Allocator;
use oxc_ast::ast::{
    Argument, AssignmentTarget, BindingPattern, ExportDefaultDeclarationKind, Expression,
    ForStatementInit, ForStatementLeft, FormalParameters, ImportDeclarationSpecifier,
    ObjectPropertyKind, Program, SimpleAssignmentTarget, Statement, VariableDeclaration,
};
use oxc_parser::{ParseOptions, Parser};
use oxc_span::{GetSpan, SourceType, Span};
use oxc_syntax::operator::{
    AssignmentOperator, BinaryOperator, LogicalOperator, UnaryOperator, UpdateOperator,
};
use sprig_common::Result;

use crate::check::Analysis;
use crate::ir::{
    BinOp, Block, Bound, Builtin, Caught, DbOp, ErrorSpec, EventDef, Expr, Function, Literal,
    LocalId, Param, PathSeg, SqlShape, Stmt, Ty, UnOp,
};
use crate::link::{CallDep, StreamTarget};
use crate::sql::{self, Columns, Want};
use crate::{Compiled, Options, Reason};

/// A JavaScript function's signature, from its declaration.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Signature {
    /// Parameters and their types, in call order.
    pub params: Vec<(String, Ty)>,
    /// `RETURNS t`: the result is `output {value}`.
    pub returns: Option<Ty>,
    /// Validations and after-commit triggers: the table whose row the code
    /// sees. `params` and `returns` are then unused: the code's own
    /// parameters bind by name (see [`compile_with`]).
    pub row: Option<String>,
    /// Triggers on streams and queues: what the code's `ctx` holds. The
    /// IR's parameters are then the fields of `ctx.record` or
    /// `ctx.message` the code reads (see [`MsgSource::fields`]).
    pub on: Option<MsgSource>,
}

/// What a trigger on a stream or queue gets: `ctx.record` or
/// `ctx.message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgSource {
    /// `ctx.record {stream, partition, offset, key, value, headers,
    /// timestamp}`.
    Stream,
    /// `ctx.message {queue, vhost, exchange, routingKey, body, properties,
    /// redelivered, deliveryCount}`.
    Queue,
}

impl MsgSource {
    /// `record` or `message`.
    #[must_use]
    pub const fn member(self) -> &'static str {
        match self {
            MsgSource::Stream => "record",
            MsgSource::Queue => "message",
        }
    }

    /// The fields the code can read, their types, and whether each is
    /// never `null`. A field's parameter is its name in lower case.
    /// `headers` and `properties` are parsed JSON objects.
    #[must_use]
    pub const fn fields(self) -> &'static [(&'static str, Ty, bool)] {
        match self {
            MsgSource::Stream => &[
                ("stream", Ty::Str, true),
                ("partition", Ty::Int, true),
                ("offset", Ty::Int, true),
                ("key", Ty::Str, false),
                ("value", Ty::Str, false),
                ("headers", Ty::Json, true),
                ("timestamp", Ty::Int, true),
            ],
            MsgSource::Queue => &[
                ("queue", Ty::Str, true),
                ("vhost", Ty::Str, true),
                ("exchange", Ty::Str, false),
                ("routingKey", Ty::Str, false),
                ("body", Ty::Str, true),
                ("properties", Ty::Json, true),
                ("redelivered", Ty::Bool, true),
                ("deliveryCount", Ty::Int, true),
            ],
        }
    }
}

impl Signature {
    /// The signature of a validation or after-commit trigger on `table`.
    #[must_use]
    pub fn row_of(table: &str) -> Self {
        Self {
            row: Some(table.to_ascii_lowercase()),
            ..Self::default()
        }
    }

    /// The signature of a trigger on a stream or queue.
    #[must_use]
    pub fn message(on: MsgSource) -> Self {
        Self {
            on: Some(on),
            ..Self::default()
        }
    }
}

/// JavaScript without its declaration's signature: always the engine
/// tier (callers with a declaration use [`compile_with`]).
#[must_use]
pub fn compile(_source: &str) -> Compiled {
    refused(Reason::new(
        "js.signature",
        "source",
        "JavaScript compiles only with its declaration's parameter types",
    ))
}

/// The names of a module's default-export function's parameters, in
/// order; `None` for a body alone, or source that doesn't parse. Test runs
/// of validations and triggers bind their arguments by these names, as
/// [`compile_with`] does.
#[must_use]
pub fn entry_params(source: &str) -> Option<Vec<String>> {
    let alloc = Allocator::default();
    let parsed = Parser::new(&alloc, source, SourceType::mjs())
        .with_options(ParseOptions {
            allow_return_outside_function: true,
            ..ParseOptions::default()
        })
        .parse();
    if !parsed.errors.is_empty() {
        return None;
    }
    parsed.program.body.iter().find_map(|s| {
        let Statement::ExportDefaultDeclaration(d) = s else {
            return None;
        };
        let ps = match &d.declaration {
            ExportDefaultDeclarationKind::FunctionDeclaration(f)
            | ExportDefaultDeclarationKind::FunctionExpression(f) => &f.params,
            ExportDefaultDeclarationKind::ArrowFunctionExpression(a) => &a.params,
            _ => return None,
        };
        Some(
            ps.items
                .iter()
                .map(|p| match &p.pattern {
                    BindingPattern::BindingIdentifier(b) => b.name.as_str().to_owned(),
                    _ => String::new(),
                })
                .collect(),
        )
    })
}

fn refused(r: Reason) -> Compiled {
    Compiled {
        ir: None,
        analysis: Analysis::refused(r),
        plsql: None,
    }
}

/// Compiles `source`, a JavaScript function `name` with signature `sig`,
/// against the shard schema's columns `cat`. A source outside the subset
/// gives reasons (the engine tier), not an error.
///
/// **Validations and after-commit triggers** (`sig.row` names the table)
/// see the written row as `row` and `old`, or as a test run's `ctx` gives
/// it, or by column name as KIRun's parameters do:
///
/// ```text
/// export default function (row, old) { ... }       row.c, old.c
/// export default function (ctx) { ... }            ctx.trigger.after.c, .before.c, .op
/// export default function (qty, old_qty, op) {...} columns, old_<column>, op by name
/// a body alone                                     row, old, op and ctx in scope
/// ```
///
/// `row` (also `ctx.trigger.after`, `ctx.row`) is the row after the write,
/// the deleted row for a `DELETE`; `old` (`ctx.trigger.before`, `ctx.old`) the row
/// before it, `null` for an `INSERT`, so reading a column of it there
/// throws JavaScript's `TypeError` unless the code tests it first. The IR's
/// parameters are the columns the code reads, `c` and `old_c`, and `op`
/// when it tests `old`: [`crate::Function`]'s parameters, which the
/// enforcement binds to `NEW.c`, `OLD.c` and the operation by name.
///
/// # Errors
/// 39874 when embedded SQL and its arguments disagree, or a validation's
/// or trigger's parameter names nothing it can bind.
pub fn compile_with(
    name: &str,
    source: &str,
    sig: &Signature,
    cat: &dyn Columns,
    opts: &Options,
) -> Result<Compiled> {
    // Locals whose declared type later assignments widen: the lowering
    // runs again with the wider type (numbers are all doubles in
    // JavaScript, so `let t = 0; t += 1.5` is one variable).
    let mut widen: BTreeMap<u32, Ty> = BTreeMap::new();
    for _ in 0..6 {
        let alloc = Allocator::default();
        let parsed = Parser::new(&alloc, source, SourceType::mjs())
            .with_options(ParseOptions {
                allow_return_outside_function: true,
                ..ParseOptions::default()
            })
            .parse();
        if let Some(e) = parsed.errors.first() {
            return Ok(refused(Reason::new(
                "js.parse",
                "source",
                format!("the JavaScript parser refused it: {e}"),
            )));
        }
        let mut l = L::new(name, source, sig, cat, widen.clone());
        l.module_id = opts.module_id;
        l.program(&parsed.program);
        if let Some(e) = l.fatal.take() {
            return Err(e);
        }
        if l.widen.len() > widen.len() && l.widen != widen {
            widen = l.widen;
            continue;
        }
        let calls = std::mem::take(&mut l.calls);
        let reaches = std::mem::take(&mut l.reaches);
        let depth = l.depth;
        let (f, reasons, lints) = l.finish();
        let mut c = crate::finish(f, reasons, lints, opts);
        c.analysis.calls = calls;
        c.analysis.reaches = reaches.into_iter().collect();
        c.analysis.call_depth = depth;
        return Ok(c);
    }
    Ok(refused(Reason::new(
        "js.types",
        "source",
        "the variables' types don't settle",
    )))
}

/// What a name stands for.
#[derive(Debug, Clone)]
enum Var {
    Param(u16),
    Local(LocalId),
    /// A `db.sqlOne` result: `found`, and each column's local.
    Row {
        found: LocalId,
        cols: Vec<(String, LocalId)>,
    },
    /// A `db.exec` or `db.publish` result.
    Exec {
        affected: LocalId,
    },
    /// A `db.call` result: the event's name, and each field's local in
    /// the callee's wrapper order.
    Call {
        event: LocalId,
        cols: Vec<(String, LocalId)>,
    },
    /// A `db.sql` result (a JSON array of rows) and its columns.
    Rows {
        local: LocalId,
        cols: Vec<(String, Ty)>,
    },
    /// One row of [`Var::Rows`] in `for ... of`.
    Elem {
        local: LocalId,
        cols: Vec<(String, Ty)>,
    },
    /// `db` or `sprig` from `sprig:db` (or the globals).
    Module(&'static str),
    /// A validation's or trigger's row: after the write (`found` is
    /// `None`: always there) or before it (`found` says whether there is
    /// one).
    Image {
        old: bool,
        found: Option<LocalId>,
    },
    /// A validation's or trigger's `ctx`: only `ctx.trigger.*` compiles.
    Ctx,
    /// A trigger on a stream or queue's `ctx`: `ctx.record` or
    /// `ctx.message`, and `ctx.log`.
    Msg,
    /// `ctx.record` or `ctx.message` (also given a name of its own).
    Event,
    /// A validation's or trigger's scalar: a column, `old_<column>` or
    /// `op`, made a parameter when first read.
    Field(String),
    /// A helper function: its source's offsets, the scope it was declared
    /// in, and whether it is an expression (`const f = (a) => ...`).
    Helper {
        start: u32,
        end: u32,
        depth: usize,
        expr: bool,
    },
    /// An expression-form helper's parameter: its argument's value.
    Val(V),
    /// A `catch`'s binding: where the error is kept, and whether its
    /// `message` and `name` read the same in both tiers (the `try` body
    /// raises only the code's own errors).
    Exc {
        caught: Caught,
        readable: bool,
    },
}

/// What a jump can reach.
#[derive(Debug, Clone)]
enum JFrame {
    /// A loop, and what `continue` runs first (a counted loop's update,
    /// a `do`'s test).
    Loop { cont: Block },
    /// An inlined helper: its block's label, and the local its value
    /// lands in (made at its first `return` of a value).
    Helper {
        label: u32,
        result: Option<LocalId>,
        /// Every `return` gave a non-null value.
        nn: bool,
    },
    /// A `try` with a `finally`: a jump out of it leaves its block with
    /// `cmp` set to the jump's number, the `finally` runs, and the jump
    /// goes on from there.
    Finally {
        label: u32,
        cmp: LocalId,
        jumps: Vec<Jump>,
    },
}

/// A jump, as it goes on after `finally` blocks.
#[derive(Debug, Clone)]
enum Jump {
    Break,
    Continue,
    /// The function's `return`: the event and its fields, already
    /// evaluated.
    Return(String, Vec<(String, Expr)>),
    /// A helper's `return`: leave its block.
    Leave(u32),
}

/// A fact key: a parameter or a local (a row's `found`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    P(u16),
    L(LocalId),
}

/// A lowered value: its IR, its type, and whether it is never `NULL`.
#[derive(Debug, Clone)]
struct V {
    e: Expr,
    ty: Ty,
    nn: bool,
}

impl V {
    fn lit(l: Literal) -> Self {
        let nn = l != Literal::Null;
        V {
            ty: l.ty(),
            e: Expr::Lit(l),
            nn,
        }
    }
    fn null() -> Self {
        V::lit(Literal::Null)
    }
}

/// A failed lowering: the reason is recorded; the caller gives up on the
/// statement.
struct Stop;

type R<T> = std::result::Result<T, Stop>;

struct L<'s, 'c> {
    src: &'s str,
    sig: &'s Signature,
    cat: &'c dyn Columns,
    f: Function,
    reasons: Vec<Reason>,
    lints: Vec<Reason>,
    scopes: Vec<Vec<(String, Var)>>,
    facts: BTreeSet<Key>,
    /// Statements that must run before the statement being lowered, in
    /// order: the checks of rows it reads unconditionally (a `TypeError`
    /// when one is `null`) and the bodies of helper functions it calls.
    pre: Block,
    /// Inside a conditionally evaluated operand.
    cond: u32,
    /// `db.call` and `db.publish` inside larger expressions, already run
    /// into `pre`, by where they are in the source (an expression may be
    /// lowered twice).
    inlined: BTreeMap<u32, Var>,
    /// What jumps can reach, innermost last: loops, inlined helpers and
    /// `try` statements with a `finally`.
    jframes: Vec<JFrame>,
    /// Labels of [`Stmt::Block`]s, and `try` statements, numbered.
    labels: u32,
    /// Helper functions being inlined (their start offsets): a call to
    /// one of them again is recursion.
    inlining: Vec<u32>,
    /// Added to span offsets: an inlined helper's source is parsed alone,
    /// so its spans start at 0.
    span_base: u32,
    /// Added to declaration offsets for widening, per inlined call, so two
    /// calls of one helper widen their locals apart.
    salt: u32,
    /// Inlined calls so far (for `salt`).
    inlines: u32,
    /// Declaration offset → wider type for the next pass.
    widen: BTreeMap<u32, Ty>,
    /// Event name → fields in first-seen order.
    events: Vec<(String, Vec<(String, Ty)>)>,
    tmp: u32,
    /// Each local's declaration offset (for widening).
    decls: BTreeMap<LocalId, u32>,
    /// An error that isn't a reason (39874).
    fatal: Option<sprig_common::SprigError>,
    /// Validations and triggers: the table, and the local saying whether
    /// the row before the write exists (set from `op` once it is read).
    table: Option<String>,
    old_found: Option<LocalId>,
    old_used: std::cell::Cell<bool>,
    /// The module being compiled (a `db.call` back into it is recursion).
    module_id: u64,
    /// What compiled `db.call`s were compiled against.
    calls: Vec<CallDep>,
    /// Modules the compiled calls reach.
    reaches: BTreeSet<u64>,
    /// The longest chain of compiled calls below this code.
    depth: u32,
    /// Triggers on streams and queues: what `ctx` holds.
    msg: Option<MsgSource>,
    /// Locals and parameters holding parsed JSON ([`Builtin::JsonParse`]):
    /// their `NULL` is `undefined`, and JSON `null` is the text `null`.
    dyn_locals: BTreeSet<LocalId>,
    dyn_params: BTreeSet<u16>,
}

/// Whether control can't continue after the lowered statements.
fn exits(b: &Block) -> bool {
    match b.last() {
        Some(Stmt::Return | Stmt::Throw(_) | Stmt::Rethrow(_) | Stmt::Leave(_)) => true,
        Some(Stmt::If(_, a, c)) => exits(a) && exits(c),
        _ => false,
    }
}

/// The locals a block assigns.
fn assigned(b: &Block, out: &mut BTreeSet<LocalId>) {
    for s in b {
        match s {
            Stmt::Assign(l, _) => {
                out.insert(*l);
            }
            Stmt::If(_, a, c) => {
                assigned(a, out);
                assigned(c, out);
            }
            Stmt::While { body, .. } | Stmt::Block(_, body) | Stmt::Try { body, .. } => {
                assigned(body, out);
            }
            Stmt::Db(DbOp::Sql { shape, .. }) => match shape {
                SqlShape::One { found, into } => {
                    out.insert(*found);
                    out.extend(into.iter().map(|(_, l)| *l));
                }
                SqlShape::Many { into, .. } => {
                    out.insert(*into);
                }
                SqlShape::Exec { affected } | SqlShape::Publish { affected, .. } => {
                    out.insert(*affected);
                }
                SqlShape::Call {
                    event,
                    result,
                    into,
                } => {
                    out.insert(*event);
                    out.insert(*result);
                    out.extend(into.iter().map(|(_, l)| *l));
                }
            },
            _ => {}
        }
    }
}

/// Whether a `catch` can read the error's `message` and `name` in the
/// compiled tier: the `try` body raises only the code's own error objects
/// (a database error's text, or a thrown value that isn't an error, can
/// read differently than in the engine tier).
fn catch_readable(b: &Block) -> bool {
    b.iter().all(|s| match s {
        Stmt::Db(_) => false,
        Stmt::Throw(e) => !matches!(&e.catch, Some((n, _)) if n.is_empty()),
        Stmt::If(_, a, c) => catch_readable(a) && catch_readable(c),
        Stmt::While { body, .. } | Stmt::Block(_, body) | Stmt::Try { body, .. } => {
            catch_readable(body)
        }
        _ => true,
    })
}

impl<'s, 'c> L<'s, 'c> {
    fn new(
        name: &str,
        src: &'s str,
        sig: &'s Signature,
        cat: &'c dyn Columns,
        widen: BTreeMap<u32, Ty>,
    ) -> Self {
        let mut f = Function {
            name: name.to_owned(),
            ..Function::default()
        };
        // A validation's or trigger's parameters are the columns it reads,
        // added as they are read.
        if sig.row.is_none() && sig.on.is_none() {
            f.params = sig
                .params
                .iter()
                .map(|(n, t)| Param {
                    name: n.clone(),
                    ty: *t,
                    min: None,
                    max: None,
                })
                .collect();
        }
        let old_found = sig.row.as_ref().map(|_| f.local("old_found", Ty::Bool));
        L {
            src,
            sig,
            cat,
            f,
            reasons: Vec::new(),
            lints: Vec::new(),
            scopes: vec![Vec::new()],
            facts: BTreeSet::new(),
            pre: Vec::new(),
            cond: 0,
            inlined: BTreeMap::new(),
            jframes: Vec::new(),
            labels: 0,
            inlining: Vec::new(),
            span_base: 0,
            salt: 0,
            inlines: 0,
            widen,
            events: vec![("output".to_owned(), Vec::new())],
            tmp: 0,
            decls: BTreeMap::new(),
            fatal: None,
            table: sig.row.clone(),
            old_found,
            old_used: std::cell::Cell::new(false),
            module_id: 0,
            calls: Vec::new(),
            reaches: BTreeSet::new(),
            depth: 0,
            msg: sig.on,
            dyn_locals: BTreeSet::new(),
            dyn_params: BTreeSet::new(),
        }
    }

    fn at(&self, span: Span) -> String {
        let start = ((span.start + self.span_base) as usize).min(self.src.len());
        let before = &self.src[..start];
        let line = before.matches('\n').count() + 1;
        let col = start - before.rfind('\n').map_or(0, |i| i + 1) + 1;
        format!("line {line}, column {col}")
    }

    fn stop<T>(&mut self, code: &str, span: Span, detail: impl Into<String>) -> R<T> {
        let at = self.at(span);
        self.reasons.push(Reason::new(code, &at, detail));
        Err(Stop)
    }

    fn finish(mut self) -> (Function, Vec<Reason>, Vec<Reason>) {
        if let (Some(found), true) = (self.old_found, self.old_used.get()) {
            // `old` is there for an update and a delete, not an insert.
            if self.column_ty("op").is_some() {
                self.reasons.push(Reason::new(
                    "js.old",
                    "source",
                    "the table has a column named op, so old can't tell an insert from an \
                     update; read old_<column> parameters instead",
                ));
            }
            let op = self.param_named("op", Ty::Str);
            self.f.body.insert(
                0,
                Stmt::Assign(
                    found,
                    Expr::bin(
                        BinOp::Ne,
                        Expr::Param(op),
                        Expr::Lit(Literal::Str("insert".to_owned())),
                    ),
                ),
            );
        }
        if self.sig.returns.is_some() {
            let ty = self.sig.returns.unwrap_or(Ty::Json);
            self.events[0].1 = vec![("value".to_owned(), ty)];
        }
        self.f.events = self
            .events
            .into_iter()
            .map(|(name, fields)| EventDef { name, fields })
            .collect();
        (self.f, self.reasons, self.lints)
    }

    // -- names --------------------------------------------------------------

    fn bind(&mut self, name: &str, v: Var) {
        if let Some(s) = self.scopes.last_mut() {
            s.push((name.to_owned(), v));
        }
    }

    fn lookup(&self, name: &str) -> Option<Var> {
        self.scopes
            .iter()
            .rev()
            .flat_map(|s| s.iter().rev())
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    }

    fn key(v: &Var) -> Option<Key> {
        match v {
            Var::Param(i) => Some(Key::P(*i)),
            Var::Local(l) => Some(Key::L(*l)),
            Var::Row { found, .. }
            | Var::Image {
                found: Some(found), ..
            } => Some(Key::L(*found)),
            _ => None,
        }
    }

    // -- validations and triggers: the row ------------------------------------

    /// The type of the bound table's column `c`.
    fn column_ty(&self, c: &str) -> Option<Ty> {
        let t = self.table.as_deref()?;
        self.cat.column(t, &c.to_ascii_lowercase())
    }

    /// The type of a scalar a validation or trigger can bind: a column,
    /// `old_<column>`, or `op`.
    fn field_ty(&self, name: &str) -> Option<Ty> {
        let n = name.to_ascii_lowercase();
        if let Some(t) = self.column_ty(&n) {
            return Some(t);
        }
        if let Some(c) = n.strip_prefix("old_")
            && let Some(t) = self.column_ty(c)
        {
            return Some(t);
        }
        (n == "op").then_some(Ty::Str)
    }

    /// The parameter `name`, added on first use.
    fn param_named(&mut self, name: &str, ty: Ty) -> u16 {
        let n = name.to_ascii_lowercase();
        if let Some(i) = self.f.params.iter().position(|p| p.name == n) {
            return u16::try_from(i).unwrap_or(u16::MAX);
        }
        self.f.params.push(Param {
            name: n,
            ty,
            min: None,
            max: None,
        });
        u16::try_from(self.f.params.len() - 1).unwrap_or(u16::MAX)
    }

    /// A scalar field's value (see [`Var::Field`]).
    fn field_val(&mut self, name: &str, span: Span) -> R<V> {
        let Some(ty) = self.field_ty(name) else {
            return self.stop("js.unknown_name", span, format!("unknown name {name}"));
        };
        let i = self.param_named(name, ty);
        Ok(V {
            e: Expr::Param(i),
            ty,
            nn: name.eq_ignore_ascii_case("op") || self.facts.contains(&Key::P(i)),
        })
    }

    /// Which row `e` names: `Some(false)` for the row after the write,
    /// `Some(true)` for the row before it.
    fn image_of(&self, e: &Expression<'_>) -> Option<bool> {
        match strip_parens(e) {
            Expression::Identifier(id) => match self.lookup(id.name.as_str()) {
                Some(Var::Image { old, .. }) => Some(old),
                _ => None,
            },
            Expression::StaticMemberExpression(m) if self.ctx_trigger(&m.object) => {
                match m.property.name.as_str() {
                    "after" => Some(false),
                    "before" => Some(true),
                    _ => None,
                }
            }
            // A test run's `ctx.row` and `ctx.old`.
            Expression::StaticMemberExpression(m)
                if matches!(strip_parens(&m.object), Expression::Identifier(id)
                    if matches!(self.lookup(id.name.as_str()), Some(Var::Ctx))) =>
            {
                match m.property.name.as_str() {
                    "row" => Some(false),
                    "old" => Some(true),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Whether `e` is `ctx.trigger`.
    fn ctx_trigger(&self, e: &Expression<'_>) -> bool {
        match strip_parens(e) {
            Expression::StaticMemberExpression(m) if m.property.name.as_str() == "trigger" => {
                matches!(strip_parens(&m.object), Expression::Identifier(id)
                    if matches!(self.lookup(id.name.as_str()), Some(Var::Ctx)))
            }
            _ => false,
        }
    }

    /// The local saying whether the row before the write exists.
    fn old_flag(&self) -> Option<LocalId> {
        self.old_used.set(true);
        self.old_found
    }

    /// Column `col` of a validation's or trigger's row.
    fn image_col(&mut self, old: bool, col: &str, span: Span) -> R<V> {
        let Some(ty) = self.column_ty(col) else {
            let t = self.table.clone().unwrap_or_default();
            return self.stop("js.member", span, format!("{t} has no column {col}"));
        };
        if !old {
            return self.field_val(col, span);
        }
        let Some(found) = self.old_flag() else {
            return self.stop("js.member", span, format!("reading .{col} here"));
        };
        if !self.facts.contains(&Key::L(found)) {
            if self.cond > 0 {
                return self.stop(
                    "js.null_row",
                    span,
                    "old may be null here (it is null for an insert)",
                );
            }
            self.guard(found, col);
        }
        let i = self.param_named(&format!("old_{col}"), ty);
        Ok(V {
            e: Expr::Param(i),
            ty,
            nn: false,
        })
    }

    fn scoped<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.scopes.push(Vec::new());
        let r = f(self);
        self.scopes.pop();
        r
    }

    // -- the program ------------------------------------------------------------

    fn program(&mut self, p: &Program<'_>) {
        let module = p.body.iter().any(|s| {
            matches!(
                s,
                Statement::ImportDeclaration(_)
                    | Statement::ExportDefaultDeclaration(_)
                    | Statement::ExportNamedDeclaration(_)
                    | Statement::ExportAllDeclaration(_)
            )
        });
        if !module {
            if self.table.is_some() {
                self.bind_row_names();
            } else if self.msg.is_some() {
                self.bind("ctx", Var::Msg);
            } else {
                for (i, (n, _)) in self.sig.params.iter().enumerate() {
                    let n = n.clone();
                    self.bind(&n, Var::Param(u16::try_from(i).unwrap_or(u16::MAX)));
                }
            }
            self.bind("db", Var::Module("db"));
            self.bind("sprig", Var::Module("sprig"));
            let body = self.block(&p.body);
            self.f.body = self.close(body);
            return;
        }
        let mut entry = None;
        // Helper functions at module level are hoisted too.
        for s in &p.body {
            if let Statement::FunctionDeclaration(f) = s
                && let Some(id) = &f.id
            {
                let v = Var::Helper {
                    start: f.span.start,
                    end: f.span.end,
                    depth: 0,
                    expr: false,
                };
                self.bind(id.name.as_str(), v);
            }
        }
        for s in &p.body {
            match s {
                Statement::FunctionDeclaration(f) if f.id.is_some() => {}
                Statement::VariableDeclaration(d)
                    if matches!(d.kind, oxc_ast::ast::VariableDeclarationKind::Const)
                        && d.declarations.iter().all(|x| {
                            matches!(&x.id, BindingPattern::BindingIdentifier(_))
                                && x.init
                                    .as_ref()
                                    .is_some_and(|i| self.helper_def(i).is_some())
                        }) =>
                {
                    for x in &d.declarations {
                        if let (BindingPattern::BindingIdentifier(id), Some(i)) = (&x.id, &x.init)
                            && let Some(h) = self.helper_def(i)
                        {
                            self.bind(id.name.as_str(), h);
                        }
                    }
                }
                Statement::ImportDeclaration(d) => {
                    if d.source.value.as_str() != "sprig:db" {
                        let _ =
                            self.stop::<()>("js.import", d.span, "only sprig:db can be imported");
                        continue;
                    }
                    for sp in d.specifiers.iter().flatten() {
                        match sp {
                            ImportDeclarationSpecifier::ImportSpecifier(x)
                                if matches!(x.imported.name().as_str(), "db" | "sprig") =>
                            {
                                let m = if x.imported.name().as_str() == "db" {
                                    "db"
                                } else {
                                    "sprig"
                                };
                                self.bind(x.local.name.as_str(), Var::Module(m));
                            }
                            _ => {
                                let _ = self.stop::<()>(
                                    "js.import",
                                    d.span,
                                    "sprig:db exports db and sprig",
                                );
                            }
                        }
                    }
                }
                Statement::ExportDefaultDeclaration(d) => match &d.declaration {
                    ExportDefaultDeclarationKind::FunctionDeclaration(func)
                    | ExportDefaultDeclarationKind::FunctionExpression(func) => {
                        if func.r#async || func.generator {
                            let _ = self.stop::<()>(
                                "js.async",
                                d.span,
                                "async functions and generators run in the engine tier",
                            );
                        } else if let Some(b) = &func.body {
                            entry = Some((&*func.params, &b.statements, d.span));
                        }
                    }
                    ExportDefaultDeclarationKind::ArrowFunctionExpression(a)
                        if !a.r#async && !a.expression =>
                    {
                        entry = Some((&*a.params, &a.body.statements, d.span));
                    }
                    _ => {
                        let _ = self.stop::<()>(
                            "js.export",
                            d.span,
                            "the default export must be a function",
                        );
                    }
                },
                other => {
                    let _ = self.stop::<()>(
                        "js.module_scope",
                        other.span(),
                        "only imports, helper functions and the default export compile at \
                         module level",
                    );
                }
            }
        }
        let Some((params, body, span)) = entry else {
            if self.reasons.is_empty() {
                let _ = self.stop::<()>("js.export", p.span, "no default export function");
            }
            return;
        };
        if self.params(params, span).is_err() {
            return;
        }
        let body = self.scoped(|l| l.block(body));
        self.f.body = self.close(body);
    }

    /// A validation's or trigger's body alone: `row`, `old`, `op` and
    /// `ctx` are in scope.
    fn bind_row_names(&mut self) {
        self.bind(
            "row",
            Var::Image {
                old: false,
                found: None,
            },
        );
        let found = self.old_found;
        self.bind("old", Var::Image { old: true, found });
        self.bind("op", Var::Field("op".to_owned()));
        self.bind("ctx", Var::Ctx);
    }

    /// A validation's or trigger's parameters, bound by name (see
    /// [`compile_with`]).
    fn row_params(&mut self, ps: &FormalParameters<'_>) -> R<()> {
        if let Some(r) = &ps.rest {
            return self.stop("js.params", r.span, "a rest parameter");
        }
        for p in &ps.items {
            let BindingPattern::BindingIdentifier(b) = &p.pattern else {
                return self.stop("js.params", p.span, "parameters must be plain names");
            };
            if p.initializer.is_some() {
                return self.stop("js.params", p.span, "parameters must be plain names");
            }
            let n = b.name.as_str();
            let var = match n {
                "row" => Var::Image {
                    old: false,
                    found: None,
                },
                "old" => Var::Image {
                    old: true,
                    found: self.old_found,
                },
                "ctx" => Var::Ctx,
                _ if self.field_ty(n).is_some() => Var::Field(n.to_owned()),
                _ => {
                    let t = self.table.clone().unwrap_or_default();
                    self.fatal = Some(sprig_common::SprigError::new(
                        sprig_common::SprigCode::LogicSignatureMismatch,
                        format!(
                            "{}: parameter {n} names nothing of {t} (use row, old, ctx, op, \
                             a column or old_<column>)",
                            self.at(p.span)
                        ),
                    ));
                    return Err(Stop);
                }
            };
            self.bind(n, var);
        }
        Ok(())
    }

    fn params(&mut self, ps: &FormalParameters<'_>, span: Span) -> R<()> {
        if self.table.is_some() {
            return self.row_params(ps);
        }
        if self.msg.is_some() {
            // The engine tier calls it with `ctx` alone.
            if ps.rest.is_some() || ps.items.len() > 1 {
                return self.stop(
                    "js.params",
                    span,
                    "a trigger on a stream or queue takes one parameter, ctx",
                );
            }
            match ps.items.first() {
                None => self.bind("ctx", Var::Msg),
                Some(p) => match &p.pattern {
                    BindingPattern::BindingIdentifier(b) if p.initializer.is_none() => {
                        self.bind(b.name.as_str(), Var::Msg);
                    }
                    _ => return self.stop("js.params", p.span, "parameters must be plain names"),
                },
            }
            return Ok(());
        }
        if ps.rest.is_some() || ps.items.len() != self.sig.params.len() {
            return self.stop(
                "js.params",
                span,
                format!(
                    "the function takes {} parameters; the declaration lists {}",
                    ps.items.len(),
                    self.sig.params.len()
                ),
            );
        }
        for (i, p) in ps.items.iter().enumerate() {
            match &p.pattern {
                BindingPattern::BindingIdentifier(b) if p.initializer.is_none() => {
                    self.bind(
                        b.name.as_str(),
                        Var::Param(u16::try_from(i).unwrap_or(u16::MAX)),
                    );
                }
                _ => return self.stop("js.params", p.span, "parameters must be plain names"),
            }
        }
        Ok(())
    }

    /// Ends the body: falling off the end is `output {}` (or
    /// `output {value: null}` for `RETURNS t`).
    fn close(&mut self, mut body: Block) -> Block {
        if !exits(&body) {
            let fields = if self.sig.returns.is_some() {
                vec![("value".to_owned(), Expr::Lit(Literal::Null))]
            } else {
                Vec::new()
            };
            body.push(Stmt::Emit("output".to_owned(), fields));
            body.push(Stmt::Return);
        }
        body
    }

    // -- statements ----------------------------------------------------------------

    fn block(&mut self, stmts: &[Statement<'_>]) -> Block {
        let mut out = Vec::new();
        // Helper functions are hoisted: callable from the block's start.
        for s in stmts {
            if let Statement::FunctionDeclaration(f) = s
                && let Some(id) = &f.id
            {
                let depth = self.scopes.len() - 1;
                let v = Var::Helper {
                    start: self.span_base + f.span.start,
                    end: self.span_base + f.span.end,
                    depth,
                    expr: false,
                };
                self.bind(id.name.as_str(), v);
            }
        }
        let outer = std::mem::take(&mut self.pre);
        for s in stmts {
            if exits(&out) {
                break;
            }
            let mut piece = Vec::new();
            let r = self.stmt(s, &mut piece);
            let pre = std::mem::take(&mut self.pre);
            if r.is_err() {
                continue;
            }
            out.extend(pre);
            out.extend(piece);
        }
        self.pre = outer;
        out
    }

    /// The check, before the statement, of a row it reads
    /// unconditionally; after it the row is known to be there.
    fn guard(&mut self, found: LocalId, col: &str) {
        if !self.facts.insert(Key::L(found)) {
            return;
        }
        self.pre.push(Stmt::If(
            Expr::Unary(UnOp::Not, Box::new(Expr::Local(found))),
            vec![Stmt::Throw(ErrorSpec {
                code: 39865,
                message: Expr::Lit(Literal::Str(format!(
                    "TypeError: cannot read properties of null (reading '{col}')"
                ))),
                // QuickJS's own words, for a `catch`.
                catch: Some((
                    "TypeError".to_owned(),
                    Expr::Lit(Literal::Str(format!(
                        "cannot read property '{col}' of null"
                    ))),
                )),
            })],
            Vec::new(),
        ));
    }

    /// Moves the statements that must run first into `out`.
    fn flush(&mut self, out: &mut Block) {
        out.append(&mut self.pre);
    }

    fn sub_block(&mut self, s: &Statement<'_>) -> Block {
        self.scoped(|l| match s {
            Statement::BlockStatement(b) => l.block(&b.body),
            other => l.block(std::slice::from_ref(other)),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn stmt(&mut self, s: &Statement<'_>, out: &mut Block) -> R<()> {
        match s {
            // A helper is hoisted when its block starts (`block`); `async`
            // and generators are refused at their call.
            Statement::EmptyStatement(_) | Statement::FunctionDeclaration(_) => Ok(()),
            Statement::BlockStatement(b) => {
                let inner = self.scoped(|l| l.block(&b.body));
                out.extend(inner);
                Ok(())
            }
            Statement::VariableDeclaration(d) => self.declare(d, out),
            Statement::ExpressionStatement(e) => self.expr_stmt(&e.expression, out),
            Statement::IfStatement(i) => {
                let test = self.test(&i.test)?;
                let before = self.facts.clone();
                self.flush(out);
                let base = self.facts.clone();
                let (t_true, t_false) =
                    (self.facts_if(&i.test, true), self.facts_if(&i.test, false));
                self.facts.extend(t_true);
                let a = self.sub_block(&i.consequent);
                let after_a = std::mem::replace(&mut self.facts, base.clone());
                self.facts.extend(t_false);
                let b = match &i.alternate {
                    Some(alt) => self.sub_block(alt),
                    None => Vec::new(),
                };
                let after_b = std::mem::take(&mut self.facts);
                self.facts = match (exits(&a), exits(&b)) {
                    (true, true) => before,
                    (true, false) => after_b,
                    (false, true) => after_a,
                    (false, false) => after_a.intersection(&after_b).copied().collect(),
                };
                out.push(Stmt::If(test, a, b));
                Ok(())
            }
            Statement::WhileStatement(w) => {
                self.flush(out);
                self.loop_entry();
                let (tpre, test) = self.loop_test(&w.test)?;
                self.jframes.push(JFrame::Loop { cont: Vec::new() });
                let body = self.sub_block(&w.body);
                self.jframes.pop();
                self.loop_entry();
                out.push(Self::looped(tpre, test, body, Bound::Guarded));
                Ok(())
            }
            Statement::DoWhileStatement(dw) => {
                // `do { body } while (test)`: the body, then the test; a
                // `continue` goes to the test.
                self.flush(out);
                self.loop_entry();
                let (mut check, test) = self.loop_test(&dw.test)?;
                check.push(Stmt::If(
                    Expr::Unary(UnOp::Not, Box::new(test)),
                    vec![Stmt::Break],
                    Vec::new(),
                ));
                self.jframes.push(JFrame::Loop {
                    cont: check.clone(),
                });
                let mut body = self.sub_block(&dw.body);
                self.jframes.pop();
                self.loop_entry();
                if !exits(&body) {
                    body.extend(check);
                }
                out.push(Stmt::While {
                    cond: Expr::bool(true),
                    body,
                    bound: Bound::Guarded,
                });
                Ok(())
            }
            Statement::ForStatement(fs) => {
                self.scopes.push(Vec::new());
                let r = (|| {
                    let mut static_from = None;
                    match &fs.init {
                        Some(ForStatementInit::VariableDeclaration(d)) => {
                            if let [one] = d.declarations.as_slice()
                                && let Some(Expression::NumericLiteral(n)) = &one.init
                            {
                                static_from = Some(n.value);
                            }
                            self.declare(d, out)?;
                        }
                        Some(other) => {
                            if let Some(e) = other.as_expression() {
                                self.expr_stmt(e, out)?;
                            }
                        }
                        None => {}
                    }
                    self.flush(out);
                    self.loop_entry();
                    let (tpre, test) = match &fs.test {
                        Some(t) => self.loop_test(t)?,
                        None => (Vec::new(), Expr::bool(true)),
                    };
                    let bound = match (static_from, &fs.test, &fs.update) {
                        (
                            Some(from),
                            Some(Expression::BinaryExpression(b)),
                            Some(Expression::UpdateExpression(u)),
                        ) if u.operator == UpdateOperator::Increment
                            && matches!(b.operator, BinaryOperator::LessThan)
                            && from.fract() == 0.0 =>
                        {
                            match &b.right {
                                Expression::NumericLiteral(to) if to.value.fract() == 0.0 => {
                                    Bound::Static((to.value - from).max(0.0) as u64)
                                }
                                _ => Bound::Guarded,
                            }
                        }
                        _ => Bound::Guarded,
                    };
                    // `continue` runs the update first: lowered once here
                    // for it (facts unchanged), and again after the body.
                    let mut cont = Vec::new();
                    if let Some(u) = &fs.update {
                        let facts = self.facts.clone();
                        self.expr_stmt(u, &mut cont)?;
                        self.flush(&mut cont);
                        self.facts = facts;
                    }
                    self.jframes.push(JFrame::Loop { cont });
                    let mut body = self.sub_block(&fs.body);
                    self.jframes.pop();
                    if let Some(u) = &fs.update {
                        if exits(&body) {
                            // The update never runs; nothing to add.
                        } else {
                            let mut upd = Vec::new();
                            self.expr_stmt(u, &mut upd)?;
                            self.flush(&mut upd);
                            body.extend(upd);
                        }
                    }
                    self.loop_entry();
                    out.push(Self::looped(tpre, test, body, bound));
                    Ok(())
                })();
                self.scopes.pop();
                r
            }
            Statement::ForOfStatement(fo) => self.for_of(fo, out),
            Statement::BreakStatement(b) => {
                if b.label.is_some() || self.loop_cont().is_none() {
                    return self.stop("js.label", b.span, "labelled break");
                }
                self.jump(Jump::Break, out);
                Ok(())
            }
            Statement::ReturnStatement(r) => self.ret(r.argument.as_ref(), r.span, out),
            Statement::ThrowStatement(t) => self.throw(&t.argument, t.span, out),
            Statement::ContinueStatement(c) => {
                if c.label.is_some() || self.loop_cont().is_none() {
                    return self.stop("js.label", c.span, "labelled continue");
                }
                self.jump(Jump::Continue, out);
                Ok(())
            }
            Statement::TryStatement(t) => self.try_stmt(t, out),
            Statement::SwitchStatement(sw) => self.switch(sw, out),
            other => self.stop(
                "js.statement",
                other.span(),
                "this statement runs in the engine tier",
            ),
        }
    }

    /// `switch` without fall-through: the discriminant
    /// once, then `if ... else if ...` on `===`, `default` last. Cases
    /// with no statements share the next case's body; any other case
    /// ends with `break`, `return`, `throw` or `continue`, and has no
    /// other `break` of the switch inside it. Anything else is
    /// `js.switch`.
    fn switch(&mut self, sw: &oxc_ast::ast::SwitchStatement<'_>, out: &mut Block) -> R<()> {
        let d = self.val(&sw.discriminant)?;
        self.flush(out);
        let d = match &d.e {
            Expr::Lit(_) | Expr::Param(_) | Expr::Local(_) => d,
            _ => {
                if d.ty == Ty::Null {
                    return self.stop("js.switch", sw.span, "a switch on null");
                }
                self.tmp += 1;
                let l = self.f.local(&format!("switch{}", self.tmp), d.ty);
                out.push(Stmt::Assign(l, d.e.clone()));
                V {
                    e: Expr::Local(l),
                    ty: d.ty,
                    nn: d.nn,
                }
            }
        };
        // (tests, has default, body without its final break)
        let mut groups: Vec<(Vec<&Expression<'_>>, bool, &[Statement<'_>])> = Vec::new();
        let mut tests: Vec<&Expression<'_>> = Vec::new();
        let mut default = false;
        let n = sw.cases.len();
        for (i, case) in sw.cases.iter().enumerate() {
            match &case.test {
                Some(t) => tests.push(t),
                None => default = true,
            }
            let body = case.consequent.as_slice();
            if body.is_empty() && i + 1 < n {
                continue;
            }
            let ends = match body.last() {
                Some(Statement::BreakStatement(b)) if b.label.is_none() => Some(true),
                Some(
                    Statement::ReturnStatement(_)
                    | Statement::ThrowStatement(_)
                    | Statement::ContinueStatement(_),
                ) => Some(false),
                _ if i + 1 == n => Some(false),
                _ => None,
            };
            let Some(strip) = ends else {
                return self.stop(
                    "js.switch",
                    case.span,
                    "a case falls through to the next one",
                );
            };
            let body = if strip { &body[..body.len() - 1] } else { body };
            if body.iter().any(breaks_switch) {
                return self.stop(
                    "js.switch",
                    case.span,
                    "a break inside a case (not at its end)",
                );
            }
            groups.push((std::mem::take(&mut tests), default, body));
            default = false;
        }
        let before = self.facts.clone();
        let mut arms: Vec<(Expr, Block)> = Vec::new();
        let mut otherwise: Option<Block> = None;
        let mut after: Option<BTreeSet<Key>> = None;
        let mut all_exit = true;
        for (tests, is_default, body) in &groups {
            let mut cond: Option<Expr> = None;
            for t in tests {
                let tv = self.val(t)?;
                let eq = self.binary(BinaryOperator::StrictEquality, d.clone(), tv, t.span())?;
                let eq = self.truthy(eq, t.span())?;
                cond = Some(match cond {
                    None => eq,
                    Some(c) => Expr::bin(BinOp::Or, c, eq),
                });
            }
            if !self.pre.is_empty() {
                self.pre.clear();
                return self.stop(
                    "js.switch",
                    sw.span,
                    "a case's test reads a row that may be null or calls a helper",
                );
            }
            self.facts.clone_from(&before);
            let block = self.scoped(|l| l.block(body));
            if exits(&block) {
                // Facts after the switch come from arms that go on.
            } else {
                all_exit = false;
                after = Some(match after {
                    None => self.facts.clone(),
                    Some(a) => a.intersection(&self.facts).copied().collect(),
                });
            }
            if let Some(c) = cond {
                arms.push((c, block.clone()));
            }
            if *is_default {
                otherwise = Some(block);
            }
        }
        let mut chain = otherwise.unwrap_or_else(|| {
            // No default: falling out of the switch keeps the facts before.
            all_exit = false;
            after = Some(match after.take() {
                None => before.clone(),
                Some(a) => a.intersection(&before).copied().collect(),
            });
            Vec::new()
        });
        for (c, b) in arms.into_iter().rev() {
            chain = vec![Stmt::If(c, b, chain)];
        }
        out.extend(chain);
        self.facts = if all_exit {
            before
        } else {
            after.unwrap_or(before)
        };
        Ok(())
    }

    /// A loop's entry: facts about locals may not hold on every
    /// iteration.
    fn loop_entry(&mut self) {
        let rows: BTreeSet<LocalId> = self
            .scopes
            .iter()
            .flatten()
            .filter_map(|(_, v)| match v {
                Var::Row { found, .. }
                | Var::Image {
                    found: Some(found), ..
                } => Some(*found),
                _ => None,
            })
            .collect();
        self.facts.retain(|k| match k {
            Key::P(_) => true,
            Key::L(l) => rows.contains(l),
        });
    }

    /// A loop's test, and the statements that must run before it each
    /// time (row checks, helpers' bodies).
    fn loop_test(&mut self, e: &Expression<'_>) -> R<(Block, Expr)> {
        let t = self.test(e)?;
        Ok((std::mem::take(&mut self.pre), t))
    }

    /// A `while` whose test may need statements first: `while (true) {
    /// pre; if (!test) break; body }` (a `continue` goes back to the top,
    /// so it runs them again).
    fn looped(pre: Block, test: Expr, body: Block, bound: Bound) -> Stmt {
        if pre.is_empty() {
            return Stmt::While {
                cond: test,
                body,
                bound,
            };
        }
        let mut full = pre;
        full.push(Stmt::If(
            Expr::Unary(UnOp::Not, Box::new(test)),
            vec![Stmt::Break],
            Vec::new(),
        ));
        full.extend(body);
        Stmt::While {
            cond: Expr::bool(true),
            body: full,
            bound: Bound::Guarded,
        }
    }

    /// What `continue` runs first in the innermost loop; `None` outside
    /// loops (of this function or helper).
    fn loop_cont(&self) -> Option<Block> {
        for f in self.jframes.iter().rev() {
            match f {
                JFrame::Loop { cont } => return Some(cont.clone()),
                JFrame::Helper { .. } => return None,
                JFrame::Finally { .. } => {}
            }
        }
        None
    }

    /// Whether `j` leaves a `try` with a `finally` on its way.
    fn crosses_finally(&self, j: &Jump) -> bool {
        for f in self.jframes.iter().rev() {
            match (f, j) {
                (JFrame::Finally { .. }, _) => return true,
                // A loop ends a `break` or `continue`; a helper ends its own
                // `return`s (a function's `return` inside it is the helper's).
                (JFrame::Loop { .. }, Jump::Break | Jump::Continue)
                | (JFrame::Helper { .. }, _) => {
                    return false;
                }
                _ => {}
            }
        }
        false
    }

    /// Lowers a jump: directly, or through the `finally` blocks it leaves.
    fn jump(&mut self, j: Jump, out: &mut Block) {
        self.flush(out);
        for i in (0..self.jframes.len()).rev() {
            match (&mut self.jframes[i], &j) {
                (JFrame::Finally { label, cmp, jumps }, _) => {
                    jumps.push(j.clone());
                    let n = i64::try_from(jumps.len()).unwrap_or(i64::MAX);
                    out.push(Stmt::Assign(*cmp, Expr::int(n)));
                    out.push(Stmt::Leave(*label));
                    return;
                }
                (JFrame::Loop { .. }, Jump::Break | Jump::Continue)
                | (JFrame::Helper { .. }, _) => break,
                _ => {}
            }
        }
        match j {
            Jump::Break => out.push(Stmt::Break),
            Jump::Continue => {
                out.extend(self.loop_cont().unwrap_or_default());
                out.push(Stmt::Continue);
            }
            Jump::Return(name, fields) => {
                out.push(Stmt::Emit(name, fields));
                out.push(Stmt::Return);
            }
            Jump::Leave(l) => out.push(Stmt::Leave(l)),
        }
    }

    fn next_label(&mut self) -> u32 {
        self.labels += 1;
        self.labels
    }

    /// The locals a `try` keeps a caught error in, named `<base>_*`.
    fn caught_locals(&mut self, base: &str) -> Caught {
        let mut l = |what: &str, ty: Ty| self.f.local(&format!("{base}_{what}"), ty);
        Caught {
            flag: l("caught", Ty::Bool),
            code: l("code", Ty::Int),
            state: l("state", Ty::Str),
            text: l("text", Ty::Str),
            message: l("message", Ty::Str),
            name: l("name", Ty::Str),
        }
    }

    /// `try { ... } catch (e) { ... } finally { ... }`: the error is kept
    /// ([`Stmt::Try`]), the `catch` and `finally` run as ordinary
    /// statements after it, and a jump out of the `try` or `catch` leaves
    /// through the `finally`. A `finally` that ends normally raises a kept
    /// error again ([`Stmt::Rethrow`]).
    fn try_stmt(&mut self, t: &oxc_ast::ast::TryStatement<'_>, out: &mut Block) -> R<()> {
        self.flush(out);
        let n = self.next_label();
        let caught = self.caught_locals(&format!("try{n}"));
        // What held before the `try` holds at its start; in its `catch`,
        // `finally` and after it, only for what the blocks before didn't
        // assign (an error can come anywhere in them).
        let entry = self.facts.clone();
        let Some(fin) = &t.finalizer else {
            let body = self.scoped(|l| l.block(&t.block.body));
            out.push(Stmt::Try {
                body: body.clone(),
                caught,
            });
            let mut changed = BTreeSet::new();
            assigned(&body, &mut changed);
            if let Some(h) = &t.handler {
                self.facts = Self::unchanged(&entry, &changed);
                let c = self.catch_body(h, caught, &body)?;
                assigned(&c, &mut changed);
                out.push(Stmt::If(Expr::Local(caught.flag), c, Vec::new()));
            }
            self.facts = Self::unchanged(&entry, &changed);
            return Ok(());
        };
        let cmp = self.f.local(&format!("try{n}_jump"), Ty::Int);
        self.jframes.push(JFrame::Finally {
            label: n,
            cmp,
            jumps: Vec::new(),
        });
        let mut changed = BTreeSet::new();
        let lowered = (|| {
            let body = self.scoped(|l| l.block(&t.block.body));
            assigned(&body, &mut changed);
            let mut inner = vec![Stmt::Try {
                body: body.clone(),
                caught,
            }];
            let mut last = caught;
            if let Some(h) = &t.handler {
                self.facts = Self::unchanged(&entry, &changed);
                let c = self.catch_body(h, caught, &body)?;
                assigned(&c, &mut changed);
                let again = self.caught_locals(&format!("catch{n}"));
                inner.push(Stmt::If(
                    Expr::Local(caught.flag),
                    vec![Stmt::Try {
                        body: c,
                        caught: again,
                    }],
                    Vec::new(),
                ));
                last = again;
            }
            Ok((inner, last))
        })();
        let Some(JFrame::Finally { jumps, .. }) = self.jframes.pop() else {
            return Err(Stop);
        };
        let (inner, last) = lowered?;
        self.facts = Self::unchanged(&entry, &changed);
        let fin_body = self.scoped(|l| l.block(&fin.body));
        assigned(&fin_body, &mut changed);
        if jumps.is_empty() {
            out.extend(inner);
        } else {
            out.push(Stmt::Assign(cmp, Expr::int(0)));
            out.push(Stmt::Block(n, inner));
        }
        let fin_exits = exits(&fin_body);
        out.extend(fin_body);
        if !fin_exits {
            out.push(Stmt::If(
                Expr::Local(last.flag),
                vec![Stmt::Rethrow(last)],
                Vec::new(),
            ));
            for (i, j) in jumps.into_iter().enumerate() {
                let mut go = Vec::new();
                self.jump(j, &mut go);
                out.push(Stmt::If(
                    Expr::bin(
                        BinOp::Eq,
                        Expr::Local(cmp),
                        Expr::int(i64::try_from(i + 1).unwrap_or(i64::MAX)),
                    ),
                    go,
                    Vec::new(),
                ));
            }
        }
        self.facts = Self::unchanged(&entry, &changed);
        Ok(())
    }

    /// `facts` without those about the locals in `changed`.
    fn unchanged(facts: &BTreeSet<Key>, changed: &BTreeSet<LocalId>) -> BTreeSet<Key> {
        facts
            .iter()
            .filter(|k| !matches!(k, Key::L(l) if changed.contains(l)))
            .copied()
            .collect()
    }

    /// A `catch` clause's body, with its binding.
    fn catch_body(
        &mut self,
        h: &oxc_ast::ast::CatchClause<'_>,
        caught: Caught,
        body: &Block,
    ) -> R<Block> {
        let readable = catch_readable(body);
        self.scopes.push(Vec::new());
        let r = match h.param.as_ref().map(|p| &p.pattern) {
            None => Ok(()),
            Some(BindingPattern::BindingIdentifier(b)) => {
                self.bind(b.name.as_str(), Var::Exc { caught, readable });
                Ok(())
            }
            Some(_) => self.stop("js.destructuring", h.span, "a destructured catch binding"),
        };
        let b = r.map(|()| self.block(&h.body.body));
        self.scopes.pop();
        b
    }

    /// `throw`: an error object, a value, or a caught error again.
    fn throw(&mut self, arg: &Expression<'_>, span: Span, out: &mut Block) -> R<()> {
        if let Expression::Identifier(id) = strip_parens(arg)
            && let Some(Var::Exc { caught, .. }) = self.lookup(id.name.as_str())
        {
            self.flush(out);
            out.push(Stmt::Rethrow(caught));
            return Ok(());
        }
        let (msg, catch) = match strip_parens(arg) {
            Expression::NewExpression(n) => match (&n.callee, n.arguments.first()) {
                (Expression::Identifier(id), a)
                    if id.name.as_str().ends_with("Error")
                        && self.lookup(id.name.as_str()).is_none() =>
                {
                    let m = match a.and_then(Argument::as_expression) {
                        // `new Error(undefined)` has an empty message.
                        Some(Expression::Identifier(u)) if u.name.as_str() == "undefined" => {
                            Expr::Lit(Literal::Str(String::new()))
                        }
                        Some(m) => {
                            let v = self.val(m)?;
                            self.text_of(v, m.span())?
                        }
                        None => Expr::Lit(Literal::Str(String::new())),
                    };
                    let name = id.name.as_str();
                    let catch = (name != "Error").then(|| (name.to_owned(), m.clone()));
                    (m, catch)
                }
                _ => return self.stop("js.throw", span, "throw new Error(message)"),
            },
            other => {
                let v = self.val(other)?;
                let m = self.text_of(v, other.span())?;
                // A thrown value that isn't an error object: a `catch`
                // sees the value itself.
                (m.clone(), Some((String::new(), m)))
            }
        };
        self.flush(out);
        out.push(Stmt::Throw(ErrorSpec {
            code: 39865,
            message: msg,
            catch,
        }));
        Ok(())
    }

    fn declare(&mut self, d: &VariableDeclaration<'_>, out: &mut Block) -> R<()> {
        let constant = matches!(d.kind, oxc_ast::ast::VariableDeclarationKind::Const);
        for decl in &d.declarations {
            // `const r = ctx.record;` and `const { key, value } = ctx.record;`.
            if let Some(init) = &decl.init
                && constant
                && self.event_of(init)
            {
                match &decl.id {
                    BindingPattern::BindingIdentifier(id) => {
                        self.bind(id.name.as_str(), Var::Event);
                        continue;
                    }
                    BindingPattern::ObjectPattern(o) if o.rest.is_none() => {
                        for p in &o.properties {
                            let (Some(k), BindingPattern::BindingIdentifier(id)) =
                                (p.key.static_name(), &p.value)
                            else {
                                return self.stop("js.destructuring", p.span, "destructuring");
                            };
                            if p.computed {
                                return self.stop("js.destructuring", p.span, "destructuring");
                            }
                            let v = self.msg_field(k.as_ref(), p.span)?;
                            self.bind(id.name.as_str(), Var::Val(v));
                        }
                        continue;
                    }
                    _ => {}
                }
            }
            let BindingPattern::BindingIdentifier(id) = &decl.id else {
                return self.stop("js.destructuring", decl.span, "destructuring");
            };
            let name = id.name.as_str();
            let Some(init) = &decl.init else {
                return self.stop(
                    "js.untyped_local",
                    decl.span,
                    format!("give {name} a value where it is declared"),
                );
            };
            if let Some(v) = self.db_call(init, name, out)? {
                self.bind(name, v);
                continue;
            }
            if let Some(h) = self.helper_def(init) {
                if !matches!(d.kind, oxc_ast::ast::VariableDeclarationKind::Const) {
                    return self.stop(
                        "js.helper",
                        decl.span,
                        "a function value in a variable that can change: use const",
                    );
                }
                self.bind(name, h);
                continue;
            }
            let v = self.val(init)?;
            let key = self.salt + self.span_base + decl.span.start;
            let ty = match (self.widen.get(&key), v.ty) {
                (Some(w), _) => *w,
                (None, Ty::Null) => {
                    return self.stop(
                        "js.untyped_local",
                        decl.span,
                        format!("{name} starts as null: give it a typed value"),
                    );
                }
                (None, t) => t,
            };
            let l = self.f.local(name, ty);
            if self.is_dyn(&v) {
                self.dyn_locals.insert(l);
            }
            let e = self.coerce(v.clone(), ty, decl.span)?;
            self.flush(out);
            out.push(Stmt::Assign(l, e));
            self.facts.remove(&Key::L(l));
            if v.nn {
                self.facts.insert(Key::L(l));
            }
            self.bind(name, Var::Local(l));
            // Remember where it was declared, for widening.
            self.decls.insert(l, key);
        }
        Ok(())
    }

    fn expr_stmt(&mut self, e: &Expression<'_>, out: &mut Block) -> R<()> {
        match e {
            Expression::ParenthesizedExpression(p) => self.expr_stmt(&p.expression, out),
            Expression::CallExpression(c) => {
                if self.db_call(e, "", out)?.is_some() {
                    return Ok(());
                }
                match self.callee_path(&c.callee).as_deref() {
                    Some(["sprig", "reject"]) => {
                        let m = match c.arguments.first().and_then(Argument::as_expression) {
                            Some(a) => {
                                let v = self.val(a)?;
                                self.text_of(v, a.span())?
                            }
                            None => Expr::Lit(Literal::Str(String::new())),
                        };
                        self.flush(out);
                        out.push(Stmt::Throw(ErrorSpec::new(39863, m)));
                        Ok(())
                    }
                    Some(["ctx" | "console", "log"]) if self.msg.is_some() => {
                        // Triggers on streams and queues keep their log
                        // lines for the run log.
                        let line = self.log_line(c)?;
                        self.flush(out);
                        out.push(Stmt::Log(line));
                        Ok(())
                    }
                    Some(["ctx" | "console", "log"]) => {
                        let at = self.at(c.span);
                        self.lints.push(Reason::new(
                            "js.log",
                            &at,
                            "log lines are dropped in the compiled tier",
                        ));
                        Ok(())
                    }
                    _ if self.helper_of(&c.callee).is_some() => {
                        // A helper called for what it does; its value is
                        // dropped.
                        self.call(c)?;
                        self.flush(out);
                        Ok(())
                    }
                    _ => self.stop("js.call", c.span, "this call runs in the engine tier"),
                }
            }
            Expression::AssignmentExpression(a) => {
                let AssignmentTarget::AssignmentTargetIdentifier(id) = &a.left else {
                    return self.stop("js.assign", a.span, "only variables can be assigned");
                };
                let rhs = self.val(&a.right)?;
                let op = match a.operator {
                    AssignmentOperator::Assign => None,
                    AssignmentOperator::Addition => Some(BinaryOperator::Addition),
                    AssignmentOperator::Subtraction => Some(BinaryOperator::Subtraction),
                    AssignmentOperator::Multiplication => Some(BinaryOperator::Multiplication),
                    AssignmentOperator::Division => Some(BinaryOperator::Division),
                    AssignmentOperator::Remainder => Some(BinaryOperator::Remainder),
                    _ => return self.stop("js.assign", a.span, "this assignment operator"),
                };
                self.assign(id.name.as_str(), op, rhs, a.span, out)
            }
            Expression::UpdateExpression(u) => {
                let SimpleAssignmentTarget::AssignmentTargetIdentifier(id) = &u.argument else {
                    return self.stop("js.assign", u.span, "only variables can be updated");
                };
                let op = if u.operator == UpdateOperator::Increment {
                    BinaryOperator::Addition
                } else {
                    BinaryOperator::Subtraction
                };
                self.assign(
                    id.name.as_str(),
                    Some(op),
                    V::lit(Literal::Int(1)),
                    u.span,
                    out,
                )
            }
            other => {
                // Evaluated for nothing: only plain values compile.
                self.val(other).map(|_| ())
            }
        }
    }

    fn assign(
        &mut self,
        name: &str,
        op: Option<BinaryOperator>,
        rhs: V,
        span: Span,
        out: &mut Block,
    ) -> R<()> {
        let Some(Var::Local(l)) = self.lookup(name) else {
            return self.stop(
                "js.assign",
                span,
                format!("{name} can't be assigned in the compiled tier"),
            );
        };
        let cur = V {
            e: Expr::Local(l),
            ty: self.f.local_def(l).ty,
            nn: self.facts.contains(&Key::L(l)),
        };
        let v = match op {
            None => rhs,
            Some(op) => self.binary(op, cur.clone(), rhs, span)?,
        };
        let ty = self.f.local_def(l).ty;
        if ty == Ty::Json && self.dyn_locals.contains(&l) != self.is_dyn(&v) {
            return self.stop(
                "js.types",
                span,
                format!("{name} holds a parsed JSON value and another kind of value"),
            );
        }
        self.flush(out);
        let e = match self.coerce(v.clone(), ty, span) {
            Ok(e) => e,
            Err(Stop) => {
                // A wider type: run the lowering again with it.
                if let (Some(w), Some(key)) = (ty.join(v.ty), self.decls.get(&l).copied())
                    && w != ty
                {
                    self.reasons.pop();
                    self.widen.insert(key, w);
                }
                return Err(Stop);
            }
        };
        out.push(Stmt::Assign(l, e));
        self.facts.remove(&Key::L(l));
        if v.nn {
            self.facts.insert(Key::L(l));
        }
        Ok(())
    }

    fn for_of(&mut self, fo: &oxc_ast::ast::ForOfStatement<'_>, out: &mut Block) -> R<()> {
        if fo.r#await {
            return self.stop("js.async", fo.span, "for await");
        }
        let ForStatementLeft::VariableDeclaration(d) = &fo.left else {
            return self.stop("js.for_of", fo.span, "for (const x of ...)");
        };
        let [decl] = d.declarations.as_slice() else {
            return self.stop("js.for_of", fo.span, "one variable");
        };
        let BindingPattern::BindingIdentifier(id) = &decl.id else {
            return self.stop("js.destructuring", decl.span, "destructuring");
        };
        // An array of parsed JSON: its elements are parsed values too.
        let mut parsed = false;
        let (src, cols) = match &fo.right {
            Expression::Identifier(i) => match self.lookup(i.name.as_str()) {
                Some(Var::Rows { local, cols }) => (Expr::Local(local), Some(cols)),
                Some(Var::Local(l)) if self.f.local_def(l).ty == Ty::Json => {
                    parsed = self.dyn_locals.contains(&l);
                    (Expr::Local(l), None)
                }
                _ => return self.stop("js.for_of", fo.span, "for ... of rows from db.sql"),
            },
            other => {
                let mut pre = Vec::new();
                match self.db_call(other, "rows", &mut pre)? {
                    Some(Var::Rows { local, cols }) => {
                        out.extend(pre);
                        (Expr::Local(local), Some(cols))
                    }
                    Some(_) => {
                        return self.stop("js.for_of", fo.span, "for ... of rows from db.sql");
                    }
                    None if self.msg.is_some() => {
                        let v = self.val(other)?;
                        if !self.is_dyn(&v) {
                            return self.stop("js.for_of", fo.span, "for ... of rows from db.sql");
                        }
                        parsed = true;
                        (v.e, None)
                    }
                    None => return self.stop("js.for_of", fo.span, "for ... of rows from db.sql"),
                }
            }
        };
        self.flush(out);
        self.tmp += 1;
        let n = self.tmp;
        let arr = self.f.local(&format!("of{n}_source"), Ty::Json);
        let len = self.f.local(&format!("of{n}_length"), Ty::Int);
        let cur = self.f.local(&format!("of{n}_index"), Ty::Int);
        out.push(Stmt::Assign(arr, src));
        if parsed {
            // JavaScript iterates an array; a string's characters, or a
            // TypeError for anything else: the engine tier.
            self.dyn_locals.insert(arr);
            let not_array = Expr::Unary(
                UnOp::Not,
                Box::new(Expr::call(
                    Builtin::JsonKind,
                    vec![
                        Expr::Local(arr),
                        Expr::Lit(Literal::Str("array".to_owned())),
                    ],
                )),
            );
            let msg = format!(
                "{}: for ... of over a value that isn't an array",
                crate::ir::ENGINE_RERUN_TEXT
            );
            out.push(Stmt::If(
                not_array,
                vec![Stmt::Throw(ErrorSpec::new(
                    crate::ir::ENGINE_RERUN,
                    Expr::Lit(Literal::Str(msg)),
                ))],
                Vec::new(),
            ));
            out.push(Stmt::Assign(
                len,
                Expr::call(Builtin::JsonLen, vec![Expr::Local(arr)]),
            ));
        } else {
            out.push(Stmt::Assign(
                len,
                Expr::bin(
                    BinOp::Coalesce,
                    Expr::call(Builtin::JsonLength, vec![Expr::Local(arr)]),
                    Expr::int(0),
                ),
            ));
        }
        out.push(Stmt::Assign(cur, Expr::int(0)));
        let each = self.f.local(id.name.as_str(), Ty::Json);
        if parsed {
            self.dyn_locals.insert(each);
        }
        self.loop_entry();
        let body = self.scoped(|l| {
            l.bind(
                id.name.as_str(),
                match cols {
                    Some(cols) => Var::Elem { local: each, cols },
                    None => Var::Local(each),
                },
            );
            // The index moves at the top of each iteration.
            l.jframes.push(JFrame::Loop { cont: Vec::new() });
            let b = l.sub_block(&fo.body);
            l.jframes.pop();
            b
        });
        self.loop_entry();
        let element = if parsed {
            Expr::call(
                Builtin::JsonGet,
                vec![Expr::Local(arr), Expr::Local(cur), Expr::bool(false)],
            )
        } else {
            Expr::Path(
                Box::new(Expr::Local(arr)),
                vec![PathSeg::Index(Box::new(Expr::Local(cur)))],
            )
        };
        let mut full = vec![
            Stmt::Assign(each, element),
            Stmt::Assign(cur, Expr::bin(BinOp::Add, Expr::Local(cur), Expr::int(1))),
        ];
        full.extend(body);
        out.push(Stmt::While {
            cond: Expr::bin(BinOp::Lt, Expr::Local(cur), Expr::Local(len)),
            body: full,
            bound: Bound::Guarded,
        });
        Ok(())
    }

    fn ret(&mut self, arg: Option<&Expression<'_>>, span: Span, out: &mut Block) -> R<()> {
        if let Some(i) = self
            .jframes
            .iter()
            .rposition(|f| matches!(f, JFrame::Helper { .. }))
        {
            return self.helper_return(i, arg, span, out);
        }
        let scalar = self.sig.returns;
        let Some(arg) = arg else {
            let fields = if scalar.is_some() {
                vec![("value".to_owned(), Expr::Lit(Literal::Null))]
            } else {
                Vec::new()
            };
            self.finish_return("output".to_owned(), fields, out);
            return Ok(());
        };
        let arg = strip_parens(arg);
        // `return sprig.event(name, {fields})`.
        if let Expression::CallExpression(c) = arg
            && self.callee_path(&c.callee).as_deref() == Some(&["sprig", "event"])
        {
            let name = match c.arguments.first().and_then(Argument::as_expression) {
                Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
                _ => return self.stop("js.event", c.span, "the event's name must be a literal"),
            };
            let fields = match c.arguments.get(1).and_then(Argument::as_expression) {
                None => Vec::new(),
                Some(Expression::ObjectExpression(o)) => self.fields(o)?,
                Some(other) => {
                    return self.stop(
                        "js.event",
                        other.span(),
                        "the fields must be an object literal",
                    );
                }
            };
            self.emit(&name, fields, span, out)?;
            return Ok(());
        }
        if let Some(ty) = scalar {
            let v = self.val(arg)?;
            let e = self.coerce(v, ty, arg.span())?;
            self.finish_return("output".to_owned(), vec![("value".to_owned(), e)], out);
            return Ok(());
        }
        match arg {
            Expression::ObjectExpression(o) => {
                let fields = self.fields(o)?;
                self.emit("output", fields, span, out)
            }
            other => {
                let v = self.val(other)?;
                if v.ty == Ty::Json {
                    return self.stop(
                        "js.return",
                        other.span(),
                        "a JSON value's members become fields only at run time",
                    );
                }
                self.emit("output", vec![("value".to_owned(), v)], span, out)
            }
        }
    }

    fn fields(&mut self, o: &oxc_ast::ast::ObjectExpression<'_>) -> R<Vec<(String, V)>> {
        let mut out: Vec<(String, V)> = Vec::new();
        for p in &o.properties {
            let ObjectPropertyKind::ObjectProperty(p) = p else {
                return self.stop("js.spread", o.span, "spread in an object literal");
            };
            if p.computed || p.method {
                return self.stop("js.object", p.span, "computed keys and methods");
            }
            let Some(k) = p.key.static_name() else {
                return self.stop("js.object", p.span, "the key must be a name");
            };
            let v = self.val(&p.value)?;
            out.retain(|(n, _)| n.as_str() != k.as_ref());
            out.push((k.into_owned(), v));
        }
        Ok(out)
    }

    fn emit(&mut self, name: &str, fields: Vec<(String, V)>, span: Span, out: &mut Block) -> R<()> {
        let idx = match self.events.iter().position(|(n, _)| n == name) {
            Some(i) => i,
            None => {
                self.events.push((name.to_owned(), Vec::new()));
                self.events.len() - 1
            }
        };
        let mut exprs = Vec::new();
        for (k, v) in fields {
            if self.is_dyn(&v) {
                return self.stop(
                    "js.return",
                    span,
                    format!("field {k} is a parsed JSON value (JSON.stringify drops undefined)"),
                );
            }
            let ty = if v.ty == Ty::Null { None } else { Some(v.ty) };
            let decl = self.events[idx].1.iter().position(|(n, _)| *n == k);
            match (decl, ty) {
                (None, t) => self.events[idx].1.push((k.clone(), t.unwrap_or(Ty::Null))),
                (Some(i), Some(t)) => {
                    let old = self.events[idx].1[i].1;
                    match old.join(t) {
                        Some(j) => self.events[idx].1[i].1 = j,
                        None => {
                            return self.stop(
                                "js.field_type",
                                span,
                                format!("field {k} is {old} in one place and {t} in another"),
                            );
                        }
                    }
                }
                (Some(_), None) => {}
            }
            exprs.push((k, v));
        }
        let fields: Vec<(String, Expr)> = exprs
            .into_iter()
            .map(|(k, v)| {
                let ty = self.events[idx]
                    .1
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map_or(v.ty, |(_, t)| *t);
                let e = if ty == Ty::Float && v.ty == Ty::Int {
                    Expr::call(Builtin::ToFloat, vec![v.e])
                } else {
                    v.e
                };
                (k, e)
            })
            .collect();
        self.finish_return(name.to_owned(), fields, out);
        Ok(())
    }

    /// The function's `return` of an event: at once, or, when it leaves a
    /// `try` with a `finally`, with its fields evaluated first (JavaScript
    /// evaluates the value before the `finally` runs).
    fn finish_return(&mut self, name: String, fields: Vec<(String, Expr)>, out: &mut Block) {
        let j = Jump::Return(name.clone(), Vec::new());
        if !self.crosses_finally(&j) {
            self.jump(Jump::Return(name, fields), out);
            return;
        }
        self.flush(out);
        let mut held = Vec::with_capacity(fields.len());
        for (k, e) in fields {
            let ty = self.f.ty_of(&e);
            self.tmp += 1;
            let l = self.f.local(&format!("ret{}_{k}", self.tmp), ty);
            out.push(Stmt::Assign(l, e));
            held.push((k, Expr::Local(l)));
        }
        self.jump(Jump::Return(name, held), out);
    }

    /// A helper's `return`: its value into the helper's result local,
    /// then leave the helper's block.
    fn helper_return(
        &mut self,
        frame: usize,
        arg: Option<&Expression<'_>>,
        span: Span,
        out: &mut Block,
    ) -> R<()> {
        let JFrame::Helper { label, result, .. } = self.jframes[frame].clone() else {
            return Err(Stop);
        };
        let v = match arg {
            None => V::null(),
            Some(e) => {
                if let Expression::CallExpression(c) = strip_parens(e)
                    && self.callee_path(&c.callee).as_deref() == Some(&["sprig", "event"])
                {
                    return self.stop(
                        "js.helper",
                        c.span,
                        "a helper returning sprig.event(...): return the event from the function",
                    );
                }
                self.val(e)?
            }
        };
        // The result's type is the join of every `return`'s: a wider one
        // runs the lowering again (as for variables).
        let key = self.salt + self.span_base;
        let ty = match (result, self.widen.get(&key)) {
            (Some(l), _) => self.f.local_def(l).ty,
            (None, Some(w)) => *w,
            (None, None) => v.ty,
        };
        let l = match result {
            Some(l) => l,
            None => {
                let l = self.f.local(&format!("h{label}_result"), ty);
                if let JFrame::Helper { result, .. } = &mut self.jframes[frame] {
                    *result = Some(l);
                }
                l
            }
        };
        let e = match self.coerce(v.clone(), ty, span) {
            Ok(e) => e,
            Err(Stop) => {
                if let Some(w) = ty.join(v.ty)
                    && w != ty
                {
                    self.reasons.pop();
                    self.widen.insert(key, w);
                }
                return Err(Stop);
            }
        };
        if let JFrame::Helper { nn, .. } = &mut self.jframes[frame] {
            *nn &= v.nn;
        }
        self.flush(out);
        out.push(Stmt::Assign(l, e));
        self.jump(Jump::Leave(label), out);
        Ok(())
    }

    // -- data access --------------------------------------------------------------

    fn callee_path(&self, e: &Expression<'_>) -> Option<Vec<&'static str>> {
        let Expression::StaticMemberExpression(m) = strip_parens(e) else {
            return None;
        };
        let Expression::Identifier(obj) = strip_parens(&m.object) else {
            return None;
        };
        let base: &'static str = match self.lookup(obj.name.as_str()) {
            Some(Var::Module(m)) => m,
            Some(Var::Ctx | Var::Msg) => "ctx",
            None => match obj.name.as_str() {
                "Math" => "Math",
                // QuickJS's globals, in a module too.
                "db" => "db",
                "sprig" => "sprig",
                "ctx" => "ctx",
                "console" => "console",
                "JSON" => "JSON",
                "Array" => "Array",
                _ => return None,
            },
            Some(_) => return None,
        };
        let prop: &'static str = match m.property.name.as_str() {
            "sql" => "sql",
            "sqlOne" => "sqlOne",
            "exec" => "exec",
            "call" => "call",
            "publish" => "publish",
            "reject" => "reject",
            "event" => "event",
            "log" => "log",
            "abs" => "abs",
            "floor" => "floor",
            "ceil" => "ceil",
            "round" => "round",
            "sqrt" => "sqrt",
            "pow" => "pow",
            "max" => "max",
            "min" => "min",
            "parse" => "parse",
            "isArray" => "isArray",
            _ => "?",
        };
        Some(vec![base, prop])
    }

    /// `db.sql | sqlOne | exec(text, [args])`: the data operation, and
    /// what the result is bound as. `None` when `e` isn't one.
    fn db_call(&mut self, e: &Expression<'_>, name: &str, out: &mut Block) -> R<Option<Var>> {
        let Expression::CallExpression(c) = strip_parens(e) else {
            return Ok(None);
        };
        let Some(path) = self.callee_path(&c.callee) else {
            return Ok(None);
        };
        let kind = match path.as_slice() {
            ["db", "sql"] => "sql",
            ["db", "sqlOne"] => "sqlOne",
            ["db", "exec"] => "exec",
            ["db", "call"] => return self.db_invoke(c, name, out).map(Some),
            ["db", "publish"] => return self.db_publish(c, name, out).map(Some),
            ["db", _] => {
                return self.stop(
                    "js.db",
                    c.span,
                    "db.commitOffset and db.mq run in the engine tier: the gateway itself \
                     answers them, so MariaDB has nothing to call",
                );
            }
            _ => return Ok(None),
        };
        let at = self.at(c.span);
        let text = match c.arguments.first().and_then(Argument::as_expression) {
            Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
            Some(Expression::TemplateLiteral(t)) if t.expressions.is_empty() => t
                .quasis
                .iter()
                .filter_map(|q| q.value.cooked.as_ref().map(|c| c.as_str().to_owned()))
                .collect(),
            _ => return self.stop("db.dynamic_sql", c.span, "the SQL text must be a literal"),
        };
        let mut binds = Vec::new();
        // Parsed JSON arguments: bound as their column's type, once the
        // statement says which.
        let mut parsed: Vec<(usize, V, Span)> = Vec::new();
        match c.arguments.get(1).map(|a| a.as_expression()) {
            None => {}
            Some(Some(Expression::ArrayExpression(a))) => {
                for el in &a.elements {
                    let Some(x) = el.as_expression() else {
                        return self.stop("js.spread", a.span, "spread in the arguments");
                    };
                    let v = self.val(x)?;
                    if self.is_dyn(&v) {
                        parsed.push((binds.len(), v, x.span()));
                        binds.push(Expr::Lit(Literal::Null));
                        continue;
                    }
                    if v.ty == Ty::Json {
                        return self.stop(
                            "db.json_bind",
                            x.span(),
                            "JSON values can't be bound yet",
                        );
                    }
                    binds.push(v.e);
                }
            }
            Some(_) => {
                return self.stop("js.db", c.span, "the arguments must be an array literal");
            }
        }
        if c.arguments.len() > 2 {
            return self.stop("js.db", c.span, "db calls take a text and an array");
        }
        let want = if kind == "exec" {
            Want::Write
        } else {
            Want::Read
        };
        let bound = match sql::bind(&text, want, self.cat, &at) {
            Ok(b) => b,
            Err(r) => {
                self.reasons.push(r);
                return Err(Stop);
            }
        };
        if bound.binds != binds.len() {
            self.fatal = Some(sprig_common::SprigError::new(
                sprig_common::SprigCode::LogicSignatureMismatch,
                format!(
                    "{at}: the SQL has {} placeholders but {} arguments",
                    bound.binds,
                    binds.len()
                ),
            ));
            return Err(Stop);
        }
        for (i, v, span) in parsed {
            // The engine tier binds the value's own kind; read as the
            // column's type, another kind runs the record in the engine
            // tier (`null` and `undefined` bind as NULL in both).
            let ty = bound.targets.get(i).copied().flatten();
            let Some(r) = ty.and_then(|t| Self::undyn(v, t, true)) else {
                return self.stop(
                    "db.json_bind",
                    span,
                    "a parsed JSON value bound where the statement doesn't name its column \
                     (or the column is JSON): convert it first, with String(x) or Number(x)",
                );
            };
            binds[i] = r.e;
        }
        self.tmp += 1;
        let base = if name.is_empty() {
            format!("db{}", self.tmp)
        } else {
            name.to_owned()
        };
        let (shape, var) = match kind {
            "sqlOne" => {
                let found = self.f.local(&format!("{base}_found"), Ty::Bool);
                let cols: Vec<(String, LocalId)> = bound
                    .columns
                    .iter()
                    .map(|(c, t)| (c.clone(), self.f.local(&format!("{base}_{c}"), *t)))
                    .collect();
                (
                    SqlShape::One {
                        found,
                        into: cols.clone(),
                    },
                    Var::Row { found, cols },
                )
            }
            "sql" => {
                let into = self.f.local(&format!("{base}_rows"), Ty::Json);
                (
                    SqlShape::Many {
                        into,
                        cols: bound.columns.clone(),
                    },
                    Var::Rows {
                        local: into,
                        cols: bound.columns.clone(),
                    },
                )
            }
            _ => {
                let affected = self.f.local(&format!("{base}_affected"), Ty::Int);
                (SqlShape::Exec { affected }, Var::Exec { affected })
            }
        };
        self.flush(out);
        out.push(Stmt::Db(DbOp::Sql {
            stmt: bound.stmt,
            binds,
            shape,
            origin: at,
        }));
        if let Var::Row { found, .. } = &var {
            self.facts.remove(&Key::L(*found));
        }
        Ok(Some(var))
    }
}

impl L<'_, '_> {
    /// `db.call(name, ...args)` into a callee's call entry
    /// ([`crate::link`]): the callee must be a compiled function or
    /// procedure of this database the deploy knows.
    fn db_invoke(
        &mut self,
        c: &oxc_ast::ast::CallExpression<'_>,
        name: &str,
        out: &mut Block,
    ) -> R<Var> {
        let at = self.at(c.span);
        let target = match c.arguments.first().and_then(Argument::as_expression) {
            Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
            _ => return self.stop("db.call", c.span, "the function's name must be a literal"),
        };
        let callee = match self.cat.callee(&target) {
            None => {
                return self.stop(
                    "db.call",
                    c.span,
                    format!("{target} isn't a logic function or procedure of this database"),
                );
            }
            Some(Err(why)) => return self.stop("db.call", c.span, why),
            Some(Ok(k)) => k,
        };
        if callee.module_id == self.module_id || callee.reaches.contains(&self.module_id) {
            return self.stop(
                "js.recursion",
                c.span,
                format!("{target} calls back into this code"),
            );
        }
        if callee.depth + 1 > MAX_CALL_DEPTH {
            return self.stop(
                "db.call",
                c.span,
                format!("calls through {target} nest deeper than logic.max_call_depth ({MAX_CALL_DEPTH})"),
            );
        }
        let args = &c.arguments[1..];
        if args.len() != callee.params.len() {
            return self.stop(
                "db.call",
                c.span,
                format!(
                    "{target} takes {} arguments, not {}",
                    callee.params.len(),
                    args.len()
                ),
            );
        }
        let mut binds = Vec::new();
        for a in args {
            let Some(x) = a.as_expression() else {
                return self.stop("js.spread", c.span, "spread in the arguments");
            };
            let v = self.val(x)?;
            if self.is_dyn(&v) {
                // Read as the parameter's type (MariaDB converts the engine
                // tier's value to it); another kind runs in the engine tier.
                let ty = callee.params.get(binds.len()).copied().unwrap_or(Ty::Json);
                match Self::undyn(v, ty, true) {
                    Some(r) => binds.push(r.e),
                    None => {
                        return self.stop(
                            "db.json_bind",
                            x.span(),
                            "a parsed JSON value passed as a JSON parameter",
                        );
                    }
                }
                continue;
            }
            if v.ty == Ty::Json {
                return self.stop("db.json_bind", x.span(), "JSON values can't be passed yet");
            }
            binds.push(v.e);
        }
        self.tmp += 1;
        let base = if name.is_empty() {
            format!("call{}", self.tmp)
        } else {
            name.to_owned()
        };
        let event = self.f.local(&format!("{base}_event"), Ty::Str);
        let result = self.f.local(&format!("{base}_result"), Ty::Json);
        let cols: Vec<(String, LocalId)> = callee
            .fields
            .iter()
            .map(|(n, t)| {
                let t = if *t == Ty::Bool { Ty::Int } else { *t };
                (n.clone(), self.f.local(&format!("{base}_{n}"), t))
            })
            .collect();
        let mut parts = vec![format!("CALL `{}`(", callee.entry())];
        for i in 1..binds.len() {
            let _ = i;
            parts.push(", ".to_owned());
        }
        if !binds.is_empty() {
            parts.push(String::new());
        }
        self.flush(out);
        out.push(Stmt::Db(DbOp::Sql {
            stmt: crate::ir::BoundSql {
                parts,
                reads: callee.reads.clone(),
                writes: callee.writes.clone(),
            },
            binds,
            shape: SqlShape::Call {
                event,
                result,
                into: cols.clone(),
            },
            origin: at,
        }));
        self.reaches.insert(callee.module_id);
        self.reaches.extend(callee.reaches.iter().copied());
        self.depth = self.depth.max(callee.depth + 1);
        let dep = CallDep::of(&callee);
        if !self.calls.contains(&dep) {
            self.calls.push(dep);
        }
        Ok(Var::Call { event, cols })
    }

    /// `db.call(...)` or `db.publish(...)` inside a larger expression
    /// (`return db.call('add_tax', x).fields.value;`): the call runs
    /// first, before the statement it is in (as a helper's statements do),
    /// and its result is read as a `const` holding it would be: the whole
    /// result, `.event`, `.fields`, `.fields.<name>` or `.affected`.
    /// `None` when `e` is none of those.
    fn inline_db(&mut self, e: &Expression<'_>) -> R<Option<V>> {
        let e = strip_parens(e);
        // The call, and the members read from it.
        let (call, path): (&Expression<'_>, Vec<&str>) = match e {
            Expression::CallExpression(_) => (e, Vec::new()),
            Expression::StaticMemberExpression(m) => match strip_parens(&m.object) {
                o @ Expression::CallExpression(_) => (o, vec![m.property.name.as_str()]),
                Expression::StaticMemberExpression(i) => match strip_parens(&i.object) {
                    o @ Expression::CallExpression(_) => {
                        (o, vec![i.property.name.as_str(), m.property.name.as_str()])
                    }
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let Expression::CallExpression(c) = call else {
            return Ok(None);
        };
        if !matches!(
            self.callee_path(&c.callee).as_deref(),
            Some(["db", "call" | "publish"])
        ) {
            return Ok(None);
        }
        let key = self.salt + self.span_base + c.span.start;
        let var = if let Some(v) = self.inlined.get(&key) {
            v.clone()
        } else {
            if self.cond > 0 {
                return self.stop(
                    "js.db",
                    c.span,
                    "db.call or db.publish where its call may not run (an operand of &&, ||, ?? \
                     or ?:): call it in a statement of its own first",
                );
            }
            let mut first = Vec::new();
            let Some(v) = self.db_call(call, "", &mut first)? else {
                return Ok(None);
            };
            self.pre.append(&mut first);
            self.inlined.insert(key, v.clone());
            v
        };
        let v = match (var, path.as_slice()) {
            (Var::Call { event, cols }, []) => V {
                e: Expr::ObjectLit(vec![
                    ("event".to_owned(), Expr::Local(event)),
                    ("fields".to_owned(), Self::call_fields(&cols).e),
                ]),
                ty: Ty::Json,
                nn: true,
            },
            (Var::Call { event, .. }, ["event"]) => V {
                e: Expr::Local(event),
                ty: Ty::Str,
                nn: true,
            },
            (Var::Call { cols, .. }, ["fields"]) => Self::call_fields(&cols),
            (Var::Call { cols, .. }, ["fields", f]) => {
                let Some((_, l)) = cols.iter().find(|(c, _)| c == f) else {
                    return self.stop(
                        "js.member",
                        e.span(),
                        format!("the call returns no field {f}"),
                    );
                };
                V {
                    e: Expr::Local(*l),
                    ty: self.f.local_def(*l).ty,
                    nn: false,
                }
            }
            (Var::Exec { affected }, ["affected"]) => V {
                e: Expr::Local(affected),
                ty: Ty::Int,
                nn: true,
            },
            (Var::Exec { .. }, _) => {
                return self.stop(
                    "js.member",
                    e.span(),
                    "of db.publish's result only .affected compiles",
                );
            }
            _ => {
                return self.stop(
                    "js.member",
                    e.span(),
                    "of db.call's result only .event, .fields and .fields.<name> compile",
                );
            }
        };
        Ok(Some(v))
    }

    /// A `db.call` result's `fields` as a value: every field, in the
    /// wrapper's order, as the engine tier's row gives them.
    fn call_fields(cols: &[(String, LocalId)]) -> V {
        V {
            e: Expr::ObjectLit(
                cols.iter()
                    .map(|(n, l)| (n.clone(), Expr::Local(*l)))
                    .collect(),
            ),
            ty: Ty::Json,
            nn: true,
        }
    }

    /// `db.publish(stream, key, value, headers?)` into the stream's
    /// outbox, with the gateway's checks first.
    fn db_publish(
        &mut self,
        c: &oxc_ast::ast::CallExpression<'_>,
        name: &str,
        out: &mut Block,
    ) -> R<Var> {
        let at = self.at(c.span);
        let stream = match c.arguments.first().and_then(Argument::as_expression) {
            Some(Expression::StringLiteral(s)) => s.value.as_str().to_owned(),
            _ => return self.stop("db.publish", c.span, "the stream's name must be a literal"),
        };
        if c.arguments.len() > 4 {
            return self.stop(
                "db.publish",
                c.span,
                "publishing to a member tenant's stream needs the gateway",
            );
        }
        let target: StreamTarget = match self.cat.stream(&stream) {
            None => {
                return self.stop(
                    "db.publish",
                    c.span,
                    format!("{stream} isn't a stream of this database"),
                );
            }
            Some(Err(why)) => return self.stop("db.publish", c.span, why),
            Some(Ok(t)) => t,
        };
        let mut binds = Vec::new();
        for i in 1..=2 {
            let e = match c.arguments.get(i) {
                None => Expr::Lit(Literal::Null),
                Some(a) => {
                    let Some(x) = a.as_expression() else {
                        return self.stop("js.spread", c.span, "spread in the arguments");
                    };
                    let v = self.val(x)?;
                    // The engine tier's text of a value: text as it is,
                    // `null` as NULL, anything else as its JSON text.
                    if self.is_dyn(&v) {
                        binds.push(Expr::call(Builtin::JsonToStr, vec![v.e, Expr::bool(true)]));
                        continue;
                    }
                    match v.ty {
                        Ty::Str => v.e,
                        Ty::Null => Expr::Lit(Literal::Null),
                        Ty::Json => Expr::call(Builtin::JsonText, vec![v.e]),
                        Ty::Int | Ty::Float | Ty::Bool => {
                            if v.nn {
                                Expr::call(Builtin::ToStr, vec![v.e])
                            } else {
                                Expr::Cond(
                                    Box::new(Expr::bin(
                                        BinOp::Eq,
                                        v.e.clone(),
                                        Expr::Lit(Literal::Null),
                                    )),
                                    Box::new(Expr::Lit(Literal::Null)),
                                    Box::new(Expr::call(Builtin::ToStr, vec![v.e])),
                                )
                            }
                        }
                    }
                }
            };
            binds.push(e);
        }
        let absent = |x: &Expression<'_>| match x {
            Expression::NullLiteral(_) => true,
            Expression::Identifier(id) => id.name.as_str() == "undefined",
            _ => false,
        };
        let given = c
            .arguments
            .get(3)
            .and_then(Argument::as_expression)
            .filter(|x| !absent(x));
        let headers = match given {
            Some(x @ Expression::ObjectExpression(_)) => {
                let v = self.val(x)?;
                Expr::call(Builtin::JsonText, vec![v.e])
            }
            None => Expr::Lit(Literal::Null),
            Some(x) => {
                return self.stop(
                    "db.publish",
                    x.span(),
                    "headers must be an object literal (or null) to compile",
                );
            }
        };
        binds.push(headers);
        self.tmp += 1;
        let base = if name.is_empty() {
            format!("pub{}", self.tmp)
        } else {
            name.to_owned()
        };
        let affected = self.f.local(&format!("{base}_affected"), Ty::Int);
        self.flush(out);
        out.push(Stmt::Db(DbOp::Sql {
            stmt: crate::ir::BoundSql {
                parts: vec![
                    format!(
                        "INSERT INTO `{}` (k, v, headers) VALUES (",
                        target.outbox.replace('`', "``")
                    ),
                    ", ".to_owned(),
                    ", ".to_owned(),
                    ")".to_owned(),
                ],
                reads: Vec::new(),
                writes: vec![target.name.clone()],
            },
            binds,
            shape: SqlShape::Publish {
                affected,
                max_bytes: target.max_bytes,
                compact: target.compact,
            },
            origin: at,
        }));
        Ok(Var::Exec { affected })
    }
}

/// `logic.max_call_depth`'s default: compiled calls nest at most this deep.
const MAX_CALL_DEPTH: u32 = 8;

/// Whether `s` holds a `break` that would leave an enclosing `switch`
/// (one not inside a loop or another switch of its own).
fn breaks_switch(s: &Statement<'_>) -> bool {
    match s {
        Statement::BreakStatement(b) => b.label.is_none(),
        Statement::BlockStatement(b) => b.body.iter().any(breaks_switch),
        Statement::IfStatement(i) => {
            breaks_switch(&i.consequent) || i.alternate.as_ref().is_some_and(breaks_switch)
        }
        Statement::LabeledStatement(l) => breaks_switch(&l.body),
        Statement::TryStatement(t) => {
            t.block.body.iter().any(breaks_switch)
                || t.handler
                    .as_ref()
                    .is_some_and(|h| h.body.body.iter().any(breaks_switch))
                || t.finalizer
                    .as_ref()
                    .is_some_and(|f| f.body.iter().any(breaks_switch))
        }
        _ => false,
    }
}

fn strip_parens<'a, 'b>(e: &'b Expression<'a>) -> &'b Expression<'a> {
    match e {
        Expression::ParenthesizedExpression(p) => strip_parens(&p.expression),
        other => other,
    }
}

impl L<'_, '_> {
    // -- expressions ----------------------------------------------------------------

    /// A condition, as `Bool` with JavaScript's truthiness.
    fn test(&mut self, e: &Expression<'_>) -> R<Expr> {
        if let Some(found) = self.row_of(e) {
            return Ok(Expr::Local(found));
        }
        // `a && b`, `a || b` as conditions: only their truth matters, so
        // a row (`old && old.qty > 0`) may be an operand (jsbind).
        if let Expression::LogicalExpression(l) = strip_parens(e)
            && l.operator != LogicalOperator::Coalesce
        {
            let and = l.operator == LogicalOperator::And;
            let a = self.test(&l.left)?;
            let facts = self.facts_if(&l.left, and);
            let saved = self.facts.clone();
            self.facts.extend(facts);
            self.cond += 1;
            let b = self.test(&l.right);
            self.cond -= 1;
            self.facts = saved;
            let b = b?;
            return Ok(if and {
                Expr::Cond(Box::new(a), Box::new(b), Box::new(Expr::bool(false)))
            } else {
                Expr::Cond(Box::new(a), Box::new(Expr::bool(true)), Box::new(b))
            });
        }
        let v = self.val(e)?;
        self.truthy(v, e.span())
    }

    /// The `found` flag when `e` names a `db.sqlOne` row.
    fn row_of(&self, e: &Expression<'_>) -> Option<LocalId> {
        if self.image_of(e) == Some(true) {
            return self.old_flag();
        }
        match strip_parens(e) {
            Expression::Identifier(id) => match self.lookup(id.name.as_str()) {
                Some(Var::Row { found, .. }) => Some(found),
                _ => None,
            },
            _ => None,
        }
    }

    /// `row === null`, `row != null`, `!row`: tests of the row's presence.
    fn row_test(&self, e: &Expression<'_>) -> Option<Expr> {
        match strip_parens(e) {
            Expression::UnaryExpression(u) if u.operator == UnaryOperator::LogicalNot => {
                let f = self.row_of(&u.argument)?;
                Some(Expr::Unary(UnOp::Not, Box::new(Expr::Local(f))))
            }
            Expression::BinaryExpression(b) => {
                let is_null = |x: &Expression<'_>| match strip_parens(x) {
                    Expression::NullLiteral(_) => true,
                    Expression::Identifier(u) => u.name.as_str() == "undefined",
                    _ => false,
                };
                let f = if is_null(&b.right) {
                    self.row_of(&b.left)?
                } else if is_null(&b.left) {
                    self.row_of(&b.right)?
                } else {
                    return None;
                };
                match b.operator {
                    BinaryOperator::StrictEquality | BinaryOperator::Equality => {
                        Some(Expr::Unary(UnOp::Not, Box::new(Expr::Local(f))))
                    }
                    BinaryOperator::StrictInequality | BinaryOperator::Inequality => {
                        Some(Expr::Local(f))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn truthy(&mut self, v: V, span: Span) -> R<Expr> {
        Ok(match v.ty {
            Ty::Bool | Ty::Int | Ty::Float => Expr::call(Builtin::Truthy, vec![v.e]),
            Ty::Null => Expr::bool(false),
            // `''` is false in JavaScript.
            Ty::Str => Expr::bin(
                BinOp::And,
                Expr::call(Builtin::Present, vec![v.e.clone()]),
                Expr::bin(BinOp::Ne, Expr::call(Builtin::Len, vec![v.e]), Expr::int(0)),
            ),
            Ty::Json if self.dyn_expr(&v.e) => Expr::call(Builtin::JsonTruthy, vec![v.e]),
            Ty::Json => match &v.e {
                Expr::ObjectLit(_) | Expr::ArrayLit(_) => Expr::bool(true),
                _ => {
                    return self.stop(
                        "js.json_truthiness",
                        span,
                        "the truth of a JSON value depends on its kind at run time",
                    );
                }
            },
        })
    }

    /// Facts that hold when `e` is `truth`.
    fn facts_if(&self, e: &Expression<'_>, truth: bool) -> BTreeSet<Key> {
        let mut s = BTreeSet::new();
        match strip_parens(e) {
            Expression::UnaryExpression(u) if u.operator == UnaryOperator::LogicalNot => {
                return self.facts_if(&u.argument, !truth);
            }
            Expression::LogicalExpression(l) => match (l.operator, truth) {
                (LogicalOperator::And, true) | (LogicalOperator::Or, false) => {
                    s.extend(self.facts_if(&l.left, truth));
                    s.extend(self.facts_if(&l.right, truth));
                }
                _ => {}
            },
            Expression::BinaryExpression(b) => {
                let (x, null) = match (strip_parens(&b.left), strip_parens(&b.right)) {
                    (x, Expression::NullLiteral(_)) | (Expression::NullLiteral(_), x) => (x, true),
                    (x, Expression::Identifier(u)) if u.name.as_str() == "undefined" => (x, true),
                    _ => return s,
                };
                let not_null_when = match b.operator {
                    BinaryOperator::StrictInequality | BinaryOperator::Inequality => true,
                    BinaryOperator::StrictEquality | BinaryOperator::Equality => false,
                    _ => return s,
                };
                if null
                    && not_null_when == truth
                    && let Expression::Identifier(id) = x
                    && let Some(k) = self.lookup(id.name.as_str()).as_ref().and_then(Self::key)
                {
                    s.insert(k);
                } else if null
                    && not_null_when == truth
                    && self.image_of(x) == Some(true)
                    && let Some(f) = self.old_found
                {
                    s.insert(Key::L(f));
                }
            }
            Expression::Identifier(id) if truth => {
                if let Some(k) = self.lookup(id.name.as_str()).as_ref().and_then(Self::key) {
                    s.insert(k);
                }
            }
            x @ Expression::StaticMemberExpression(_)
                if truth && self.image_of(x) == Some(true) =>
            {
                if let Some(f) = self.old_found {
                    s.insert(Key::L(f));
                }
            }
            _ => {}
        }
        s
    }

    /// Lowers `e` with `facts` added and as a conditionally evaluated
    /// operand.
    fn cond_val(&mut self, e: &Expression<'_>, facts: BTreeSet<Key>) -> R<V> {
        let saved = self.facts.clone();
        self.facts.extend(facts);
        self.cond += 1;
        let r = self.val(e);
        self.cond -= 1;
        self.facts = saved;
        r
    }

    #[allow(clippy::too_many_lines)]
    fn val(&mut self, e: &Expression<'_>) -> R<V> {
        if let Some(t) = self.row_test(e) {
            return Ok(V {
                e: t,
                ty: Ty::Bool,
                nn: true,
            });
        }
        if let Some(v) = self.inline_db(e)? {
            return Ok(v);
        }
        match e {
            Expression::ParenthesizedExpression(p) => self.val(&p.expression),
            Expression::NullLiteral(_) => Ok(V::null()),
            Expression::BooleanLiteral(b) => Ok(V::lit(Literal::Bool(b.value))),
            Expression::NumericLiteral(n) => Ok(V::lit(number(n.value))),
            Expression::StringLiteral(s) => Ok(V::lit(Literal::Str(s.value.as_str().to_owned()))),
            Expression::TemplateLiteral(t) => {
                let mut acc: Option<Expr> = None;
                for (i, q) in t.quasis.iter().enumerate() {
                    let Some(text) = q.value.cooked.as_ref() else {
                        return self.stop("js.template", t.span, "an invalid escape");
                    };
                    let mut parts = Vec::new();
                    if !text.as_str().is_empty() {
                        parts.push(Expr::Lit(Literal::Str(text.as_str().to_owned())));
                    }
                    if let Some(x) = t.expressions.get(i) {
                        let v = self.val(x)?;
                        parts.push(self.text_of(v, x.span())?);
                    }
                    for p in parts {
                        acc = Some(match acc {
                            None => p,
                            Some(a) => Expr::bin(BinOp::Concat, a, p),
                        });
                    }
                }
                Ok(V {
                    e: acc.unwrap_or(Expr::Lit(Literal::Str(String::new()))),
                    ty: Ty::Str,
                    nn: true,
                })
            }
            Expression::Identifier(id) => {
                let name = id.name.as_str();
                match self.lookup(name) {
                    Some(Var::Param(i)) => Ok(V {
                        e: Expr::Param(i),
                        ty: self.f.params[i as usize].ty,
                        nn: self.facts.contains(&Key::P(i)),
                    }),
                    Some(Var::Local(l)) => Ok(V {
                        e: Expr::Local(l),
                        ty: self.f.local_def(l).ty,
                        nn: self.facts.contains(&Key::L(l)),
                    }),
                    Some(Var::Rows { local, .. }) => Ok(V {
                        e: Expr::Local(local),
                        ty: Ty::Json,
                        nn: true,
                    }),
                    Some(Var::Field(f)) => self.field_val(&f, id.span),
                    Some(Var::Val(v)) => Ok(v),
                    Some(Var::Call { event, cols }) => {
                        let fields = Self::call_fields(&cols);
                        Ok(V {
                            e: Expr::ObjectLit(vec![
                                ("event".to_owned(), Expr::Local(event)),
                                ("fields".to_owned(), fields.e),
                            ]),
                            ty: Ty::Json,
                            nn: true,
                        })
                    }
                    Some(Var::Helper { .. }) => self.stop(
                        "js.helper",
                        id.span,
                        format!("{name} is a function: only calls of it compile"),
                    ),
                    Some(Var::Exc { .. }) => self.stop(
                        "js.catch_value",
                        id.span,
                        format!("only {name}.message, {name}.name and throw {name} compile"),
                    ),
                    Some(
                        Var::Row { .. }
                        | Var::Exec { .. }
                        | Var::Elem { .. }
                        | Var::Module(_)
                        | Var::Image { .. }
                        | Var::Ctx
                        | Var::Msg
                        | Var::Event,
                    ) => self.stop(
                        "js.value",
                        id.span,
                        format!("{name} can't be used as a value in the compiled tier"),
                    ),
                    None if name == "undefined" => Ok(V::null()),
                    None => self.stop("js.unknown_name", id.span, format!("unknown name {name}")),
                }
            }
            Expression::StaticMemberExpression(m) => self.static_member(m),
            Expression::UnaryExpression(u) => {
                let v = self.val(&u.argument)?;
                match u.operator {
                    UnaryOperator::LogicalNot => {
                        let t = self.truthy(v, u.span)?;
                        Ok(V {
                            e: Expr::Unary(UnOp::Not, Box::new(t)),
                            ty: Ty::Bool,
                            nn: true,
                        })
                    }
                    UnaryOperator::UnaryNegation => {
                        let v = self.num(v, u.span)?;
                        Ok(V {
                            ty: v.ty,
                            e: Expr::Unary(UnOp::Neg, Box::new(v.e)),
                            nn: true,
                        })
                    }
                    UnaryOperator::UnaryPlus => self.num(v, u.span),
                    _ => self.stop("js.operator", u.span, "this operator"),
                }
            }
            Expression::BinaryExpression(b) => {
                if let Some(v) = self.dyn_test(b)? {
                    return Ok(v);
                }
                let x = self.val(&b.left)?;
                let y = self.val(&b.right)?;
                self.binary(b.operator, x, y, b.span)
            }
            Expression::LogicalExpression(l) => {
                let x = self.val(&l.left)?;
                let y = match l.operator {
                    LogicalOperator::Or => {
                        self.cond_val(&l.right, self.facts_if(&l.left, false))?
                    }
                    LogicalOperator::And => {
                        self.cond_val(&l.right, self.facts_if(&l.left, true))?
                    }
                    LogicalOperator::Coalesce => self.cond_val(&l.right, BTreeSet::new())?,
                };
                let (xd, yd) = (self.is_dyn(&x), self.is_dyn(&y));
                if xd || yd {
                    return self.dyn_logical(l.operator, x, y, xd, yd, l.span);
                }
                if l.operator == LogicalOperator::Coalesce {
                    let ty = self.join(x.ty, y.ty, l.span)?;
                    let (a, b) = (
                        self.coerce(x.clone(), ty, l.span)?,
                        self.coerce(y.clone(), ty, l.span)?,
                    );
                    return Ok(V {
                        e: Expr::bin(BinOp::Coalesce, a, b),
                        ty,
                        nn: x.nn || y.nn,
                    });
                }
                if x.ty == Ty::Bool && y.ty == Ty::Bool {
                    let op = if l.operator == LogicalOperator::Or {
                        BinOp::Or
                    } else {
                        BinOp::And
                    };
                    let tx = self.truthy(x.clone(), l.span)?;
                    let ty_ = self.truthy(y.clone(), l.span)?;
                    // A Bool operand that is `NULL` would be returned as
                    // itself in JavaScript; only non-null ones fold.
                    if x.nn && y.nn {
                        return Ok(V {
                            e: Expr::bin(op, tx, ty_),
                            ty: Ty::Bool,
                            nn: true,
                        });
                    }
                }
                // The operand values themselves, as JavaScript returns.
                let ty = self.join(x.ty, y.ty, l.span)?;
                let t = self.truthy(x.clone(), l.span)?;
                let (a, b) = (
                    self.coerce(x.clone(), ty, l.span)?,
                    self.coerce(y.clone(), ty, l.span)?,
                );
                let (e, nn) = if l.operator == LogicalOperator::Or {
                    (Expr::Cond(Box::new(t), Box::new(a), Box::new(b)), y.nn)
                } else {
                    (
                        Expr::Cond(Box::new(t), Box::new(b), Box::new(a)),
                        x.nn && y.nn,
                    )
                };
                Ok(V { e, ty, nn })
            }
            Expression::ConditionalExpression(c) => {
                let t = self.test(&c.test)?;
                let a = self.cond_val(&c.consequent, self.facts_if(&c.test, true))?;
                let b = self.cond_val(&c.alternate, self.facts_if(&c.test, false))?;
                let (a, b) = match (self.is_dyn(&a), self.is_dyn(&b)) {
                    (true, true) => {
                        return Ok(V {
                            e: Expr::Cond(Box::new(t), Box::new(a.e), Box::new(b.e)),
                            ty: Ty::Json,
                            nn: false,
                        });
                    }
                    (true, false) => match Self::undyn(a, Self::read_as(b.ty), false) {
                        Some(a) => (a, b),
                        None => return self.stop("js.types", c.span, "a parsed JSON value here"),
                    },
                    (false, true) => match Self::undyn(b, Self::read_as(a.ty), false) {
                        Some(b) => (a, b),
                        None => return self.stop("js.types", c.span, "a parsed JSON value here"),
                    },
                    (false, false) => (a, b),
                };
                let ty = self.join(a.ty, b.ty, c.span)?;
                let nn = a.nn && b.nn;
                let (a, b) = (self.coerce(a, ty, c.span)?, self.coerce(b, ty, c.span)?);
                Ok(V {
                    e: Expr::Cond(Box::new(t), Box::new(a), Box::new(b)),
                    ty,
                    nn,
                })
            }
            Expression::ObjectExpression(o) => {
                let fields = self.fields(o)?;
                let mut m = Vec::new();
                for (k, v) in fields {
                    if self.is_dyn(&v) {
                        // JSON.stringify drops an `undefined` member.
                        return self.stop(
                            "js.dynamic_types",
                            o.span,
                            "a parsed JSON value inside an object literal",
                        );
                    }
                    let e = v.e;
                    m.push((k, e));
                }
                Ok(V {
                    e: Expr::ObjectLit(m),
                    ty: Ty::Json,
                    nn: true,
                })
            }
            Expression::ArrayExpression(a) => {
                let mut items = Vec::new();
                for el in &a.elements {
                    let Some(x) = el.as_expression() else {
                        return self.stop("js.spread", a.span, "spread or holes in an array");
                    };
                    let v = self.val(x)?;
                    if self.is_dyn(&v) {
                        return self.stop(
                            "js.dynamic_types",
                            a.span,
                            "a parsed JSON value inside an array literal",
                        );
                    }
                    items.push(v.e);
                }
                Ok(V {
                    e: Expr::ArrayLit(items),
                    ty: Ty::Json,
                    nn: true,
                })
            }
            Expression::CallExpression(c) => self.call(c),
            Expression::NewExpression(n)
                if n.arguments.is_empty()
                    && matches!(strip_parens(&n.callee), Expression::Identifier(id)
                        if id.name.as_str() == "Date" && self.lookup("Date").is_none()) =>
            {
                self.stop(
                    "js.nondeterministic",
                    n.span,
                    "new Date() gives a different value on each call: it runs in the engine \
                     tier, and a computed column, validation or trigger can't use it",
                )
            }
            Expression::ChainExpression(c) => match &c.expression {
                oxc_ast::ast::ChainElement::StaticMemberExpression(m) => self.static_member(m),
                oxc_ast::ast::ChainElement::ComputedMemberExpression(m) => self.computed_member(m),
                _ => self.stop("js.optional_chain", c.span, "optional chaining"),
            },
            Expression::ComputedMemberExpression(m) => self.computed_member(m),
            other => self.stop(
                "js.expression",
                other.span(),
                "this expression runs in the engine tier",
            ),
        }
    }

    /// `obj.prop`, also inside an optional chain (`m.optional` is `?.`).
    #[allow(clippy::too_many_lines)]
    fn static_member(&mut self, m: &oxc_ast::ast::StaticMemberExpression<'_>) -> R<V> {
        let prop = m.property.name.as_str();
        if self.event_of(&m.object) {
            return self.msg_field(prop, m.span);
        }
        if m.optional {
            // `?.` compiles on parsed JSON only.
            let v = self.val(&m.object)?;
            if !self.is_dyn(&v) {
                return self.stop("js.optional_chain", m.span, "optional chaining");
            }
            return self.json_member(v, prop, true, m.span);
        }
        if let Some(old) = self.image_of(&m.object) {
            return self.image_col(old, prop, m.span);
        }
        if prop == "op" && self.ctx_trigger(&m.object) {
            return self.field_val("op", m.span);
        }
        if let Expression::StaticMemberExpression(inner) = strip_parens(&m.object)
            && inner.property.name.as_str() == "fields"
            && let Expression::Identifier(obj) = strip_parens(&inner.object)
            && let Some(Var::Call { cols, .. }) = self.lookup(obj.name.as_str())
        {
            // A field the callee doesn't return is `undefined`,
            // which the IR has no value for.
            let Some((_, l)) = cols.iter().find(|(c, _)| c == prop) else {
                return self.stop(
                    "js.member",
                    m.span,
                    format!("the call returns no field {prop}"),
                );
            };
            return Ok(V {
                e: Expr::Local(*l),
                ty: self.f.local_def(*l).ty,
                nn: false,
            });
        }
        if let Expression::Identifier(obj) = strip_parens(&m.object)
            && let Some(var) = self.lookup(obj.name.as_str())
        {
            match var {
                Var::Call { event, .. } if prop == "event" => {
                    return Ok(V {
                        e: Expr::Local(event),
                        ty: Ty::Str,
                        nn: true,
                    });
                }
                Var::Call { cols, .. } if prop == "fields" => {
                    return Ok(Self::call_fields(&cols));
                }
                Var::Row { found, cols } => {
                    let Some((_, l)) = cols.iter().find(|(c, _)| c == prop).cloned() else {
                        return self.stop(
                            "js.member",
                            m.span,
                            format!("the row has no column {prop}"),
                        );
                    };
                    if !self.facts.contains(&Key::L(found)) {
                        if self.cond > 0 {
                            return self.stop(
                                "js.null_row",
                                m.span,
                                format!("{} may be null here", obj.name.as_str()),
                            );
                        }
                        self.guard(found, prop);
                    }
                    return Ok(V {
                        e: Expr::Local(l),
                        ty: self.f.local_def(l).ty,
                        nn: false,
                    });
                }
                Var::Exec { affected } if prop == "affected" => {
                    return Ok(V {
                        e: Expr::Local(affected),
                        ty: Ty::Int,
                        nn: true,
                    });
                }
                Var::Elem { local, cols } => {
                    let Some((_, ty)) = cols.iter().find(|(c, _)| c == prop).cloned() else {
                        return self.stop(
                            "js.member",
                            m.span,
                            format!("the rows have no column {prop}"),
                        );
                    };
                    let path = Expr::Path(
                        Box::new(Expr::Local(local)),
                        vec![PathSeg::Key(prop.to_owned())],
                    );
                    let (e, ty) = match ty {
                        Ty::Int | Ty::Float => {
                            (Expr::call(Builtin::JsonNum, vec![path]), Ty::Float)
                        }
                        Ty::Str => (Expr::call(Builtin::JsonStr, vec![path]), Ty::Str),
                        _ => (path, Ty::Json),
                    };
                    return Ok(V { e, ty, nn: false });
                }
                Var::Exc { caught, readable } => {
                    if !matches!(prop, "message" | "name") {
                        return self.stop(
                            "js.catch_value",
                            m.span,
                            format!("reading .{prop} of a caught error"),
                        );
                    }
                    if !readable {
                        return self.stop(
                            "js.catch_value",
                            m.span,
                            "the try can raise a database error (or a thrown value that \
                             isn't an error), whose text can differ between MariaDB and the \
                             engine tier",
                        );
                    }
                    let l = if prop == "message" {
                        caught.message
                    } else {
                        caught.name
                    };
                    return Ok(V {
                        e: Expr::Local(l),
                        ty: Ty::Str,
                        nn: true,
                    });
                }
                Var::Rows { local, .. } if prop == "length" => {
                    return Ok(V {
                        e: Expr::bin(
                            BinOp::Coalesce,
                            Expr::call(Builtin::JsonLength, vec![Expr::Local(local)]),
                            Expr::int(0),
                        ),
                        ty: Ty::Int,
                        nn: true,
                    });
                }
                _ => {}
            }
        }
        let v = self.val(&m.object)?;
        if self.is_dyn(&v) {
            return self.json_member(v, prop, false, m.span);
        }
        match (v.ty, prop) {
            (Ty::Str, "length") => {
                self.need_nn(&v, m.span)?;
                Ok(V {
                    e: Expr::call(Builtin::Len, vec![v.e]),
                    ty: Ty::Int,
                    nn: true,
                })
            }
            _ => self.stop("js.member", m.span, format!("reading .{prop} here")),
        }
    }

    /// `Math.random()`, `Date.now()`, `Date()`, `performance.now()`,
    /// `crypto.*()`: a different value on each call.
    fn nondeterministic(&self, callee: &Expression<'_>) -> Option<String> {
        let global = |n: &str| self.lookup(n).is_none();
        match strip_parens(callee) {
            Expression::Identifier(id) if id.name.as_str() == "Date" && global("Date") => {
                Some("Date()".to_owned())
            }
            Expression::StaticMemberExpression(m) => {
                let Expression::Identifier(obj) = strip_parens(&m.object) else {
                    return None;
                };
                let (o, p) = (obj.name.as_str(), m.property.name.as_str());
                let hit = matches!((o, p), ("Math", "random") | ("Date" | "performance", "now"))
                    || o == "crypto";
                (hit && global(o)).then(|| format!("{o}.{p}()"))
            }
            _ => None,
        }
    }

    fn call(&mut self, c: &oxc_ast::ast::CallExpression<'_>) -> R<V> {
        if let Some(h) = self.helper_of(&c.callee) {
            return self.inline(h, c);
        }
        if let Some(what) = self.nondeterministic(&c.callee) {
            return self.stop(
                "js.nondeterministic",
                c.span,
                format!(
                    "{what} gives a different value on each call: it runs in the engine tier, \
                     and a computed column, validation or trigger can't use it"
                ),
            );
        }
        let mut args = Vec::new();
        for a in &c.arguments {
            let Some(x) = a.as_expression() else {
                return self.stop("js.spread", c.span, "spread arguments");
            };
            args.push(x);
        }
        if let Some(v) = self.json_call(c, &args)? {
            return Ok(v);
        }
        if let Some(path) = self.callee_path(&c.callee) {
            let b = match path.as_slice() {
                ["Math", "abs"] => Some(Builtin::Abs),
                ["Math", "floor"] => Some(Builtin::Floor),
                ["Math", "ceil"] => Some(Builtin::Ceil),
                ["Math", "round"] => Some(Builtin::Round),
                ["Math", "sqrt"] => Some(Builtin::Sqrt),
                ["Math", "pow"] => Some(Builtin::Pow),
                ["Math", "max"] => Some(Builtin::Max),
                ["Math", "min"] => Some(Builtin::Min),
                _ => None,
            };
            let Some(b) = b else {
                return self.stop("js.call", c.span, "this call runs in the engine tier");
            };
            let want = match b {
                Builtin::Pow => 2,
                Builtin::Max | Builtin::Min => args.len().max(1),
                _ => 1,
            };
            if args.len() != want {
                return self.stop("js.call", c.span, "the number of arguments");
            }
            let mut xs = Vec::new();
            let mut ty = Ty::Int;
            for a in args {
                let v = self.val(a)?;
                let v = self.num(v, a.span())?;
                if v.ty == Ty::Float {
                    ty = Ty::Float;
                }
                xs.push(v);
            }
            let e = match b {
                Builtin::Abs => Expr::call(b, vec![xs.remove(0).e]),
                Builtin::Max | Builtin::Min => {
                    let es = xs
                        .into_iter()
                        .map(|v| {
                            if ty == Ty::Float && v.ty == Ty::Int {
                                Expr::call(Builtin::ToFloat, vec![v.e])
                            } else {
                                v.e
                            }
                        })
                        .collect();
                    Expr::call(b, es)
                }
                _ => Expr::call(
                    b,
                    xs.into_iter()
                        .map(|v| {
                            if v.ty == Ty::Int {
                                Expr::call(Builtin::ToFloat, vec![v.e])
                            } else {
                                v.e
                            }
                        })
                        .collect(),
                ),
            };
            let out_ty = match b {
                Builtin::Abs | Builtin::Max | Builtin::Min => ty,
                _ => Ty::Float,
            };
            return Ok(V {
                e,
                ty: out_ty,
                nn: b != Builtin::Sqrt,
            });
        }
        // `String(x)`.
        if let Expression::Identifier(id) = strip_parens(&c.callee)
            && id.name.as_str() == "String"
            && self.lookup("String").is_none()
            && args.len() == 1
        {
            let v = self.val(args[0])?;
            let e = self.text_of(v, c.span)?;
            return Ok(V {
                e,
                ty: Ty::Str,
                nn: true,
            });
        }
        // String methods.
        if let Expression::StaticMemberExpression(m) = strip_parens(&c.callee) {
            let method = m.property.name.as_str();
            let b = match method {
                "toUpperCase" => Some((Builtin::Upper, 0)),
                "toLowerCase" => Some((Builtin::Lower, 0)),
                "includes" => Some((Builtin::Contains, 1)),
                "startsWith" => Some((Builtin::StartsWith, 1)),
                "endsWith" => Some((Builtin::EndsWith, 1)),
                "indexOf" => Some((Builtin::IndexOf, 1)),
                _ => None,
            };
            if let Some((b, n)) = b
                && args.len() == n
            {
                let s = self.val(&m.object)?;
                // On parsed JSON: a string, or the engine tier (a method of
                // another kind, or a TypeError).
                let s = if self.is_dyn(&s) {
                    Self::undyn(s, Ty::Str, false).unwrap_or_else(V::null)
                } else {
                    s
                };
                if s.ty != Ty::Str {
                    return self.stop("js.call", c.span, format!("{method} on a non-string"));
                }
                self.need_nn(&s, m.span)?;
                let mut es = vec![s.e];
                for a in args {
                    let v = self.val(a)?;
                    let v = if self.is_dyn(&v) {
                        Self::undyn(v, Ty::Str, false).unwrap_or_else(V::null)
                    } else {
                        v
                    };
                    if v.ty != Ty::Str || !v.nn {
                        return self.stop(
                            "js.call",
                            a.span(),
                            format!("{method} takes a non-null string here"),
                        );
                    }
                    es.push(v.e);
                }
                let ty = match b {
                    Builtin::Upper | Builtin::Lower => Ty::Str,
                    Builtin::IndexOf => Ty::Int,
                    _ => Ty::Bool,
                };
                return Ok(V {
                    e: Expr::call(b, es),
                    ty,
                    nn: true,
                });
            }
        }
        self.stop("js.call", c.span, "this call runs in the engine tier")
    }

    /// The helper `init` declares (`const f = (a) => ...`,
    /// `const f = function (a) { ... }`).
    fn helper_def(&self, init: &Expression<'_>) -> Option<Var> {
        let span = match strip_parens(init) {
            Expression::ArrowFunctionExpression(a) => a.span,
            Expression::FunctionExpression(f) => f.span,
            _ => return None,
        };
        Some(Var::Helper {
            start: self.span_base + span.start,
            end: self.span_base + span.end,
            depth: self.scopes.len() - 1,
            expr: true,
        })
    }

    /// The helper a call's callee names.
    fn helper_of(&self, callee: &Expression<'_>) -> Option<(u32, u32, usize, bool)> {
        let Expression::Identifier(id) = strip_parens(callee) else {
            return None;
        };
        match self.lookup(id.name.as_str()) {
            Some(Var::Helper {
                start,
                end,
                depth,
                expr,
            }) => Some((start, end, depth, expr)),
            _ => None,
        }
    }

    /// A call of a helper, inlined: its source is parsed again on its own
    /// and lowered in the scope it was declared in (a closure reads and
    /// writes the variables it sees there), with the arguments evaluated
    /// first. A helper whose body is one `return` is an expression; any
    /// other runs as statements before the statement that calls it, in a
    /// block its `return`s leave.
    fn inline(
        &mut self,
        (start, end, depth, is_expr): (u32, u32, usize, bool),
        c: &oxc_ast::ast::CallExpression<'_>,
    ) -> R<V> {
        if self.inlining.contains(&start) {
            return self.stop(
                "js.recursion",
                c.span,
                "a helper that calls itself runs in the engine tier",
            );
        }
        if self.inlining.len() >= 8 || self.inlines >= 200 {
            return self.stop(
                "js.helper",
                c.span,
                "helpers nested more than 8 deep, or more than 200 helper calls",
            );
        }
        let mut args = Vec::with_capacity(c.arguments.len());
        for a in &c.arguments {
            let Some(x) = a.as_expression() else {
                return self.stop("js.spread", c.span, "spread arguments");
            };
            args.push(self.val(x)?);
        }
        let text = self
            .src
            .get(start as usize..end as usize)
            .unwrap_or_default();
        let (text, base) = if is_expr {
            (format!("({text})"), start.saturating_sub(1))
        } else {
            (text.to_owned(), start)
        };
        let alloc = Allocator::default();
        let parsed = Parser::new(&alloc, &text, SourceType::mjs()).parse();
        let func = parsed.program.body.first().and_then(|st| match st {
            Statement::FunctionDeclaration(f) => {
                Some((&f.params, f.body.as_ref(), None, f.r#async || f.generator))
            }
            Statement::ExpressionStatement(es) => match strip_parens(&es.expression) {
                Expression::FunctionExpression(f) => {
                    Some((&f.params, f.body.as_ref(), None, f.r#async || f.generator))
                }
                Expression::ArrowFunctionExpression(a) => {
                    let concise = a.expression.then(|| match a.body.statements.first() {
                        Some(Statement::ExpressionStatement(e)) => Some(&e.expression),
                        _ => None,
                    });
                    Some((&a.params, Some(&a.body), concise.flatten(), a.r#async))
                }
                _ => None,
            },
            _ => None,
        });
        let Some((params, Some(body), concise, unusual)) =
            func.filter(|_| parsed.errors.is_empty())
        else {
            return self.stop("js.helper", c.span, "this helper's form");
        };
        if unusual {
            return self.stop(
                "js.async",
                c.span,
                "async helpers and generators run in the engine tier",
            );
        }
        if params.rest.is_some()
            || params.items.iter().any(|p| {
                p.initializer.is_some()
                    || !matches!(p.pattern, BindingPattern::BindingIdentifier(_))
            })
        {
            return self.stop(
                "js.params",
                c.span,
                "a helper's parameters must be plain names",
            );
        }
        // One `return expr` (or an arrow's expression): an expression.
        let single = concise.or(match body.statements.as_slice() {
            [Statement::ReturnStatement(r)] => r.argument.as_ref(),
            _ => None,
        });
        // Enter the helper's scope.
        let saved = (self.span_base, self.salt);
        self.inlines += 1;
        self.span_base = base;
        self.salt = self.inlines.wrapping_mul(0x0100_0000);
        let stash = self.scopes.split_off((depth + 1).min(self.scopes.len()));
        self.scopes.push(Vec::new());
        self.inlining.push(start);
        let r = if let Some(e) = single {
            for (i, p) in params.items.iter().enumerate() {
                if let BindingPattern::BindingIdentifier(b) = &p.pattern {
                    let v = args.get(i).cloned().unwrap_or_else(V::null);
                    self.bind(b.name.as_str(), Var::Val(v));
                }
            }
            self.val(e)
        } else if self.cond > 0 {
            self.stop(
                "js.helper",
                c.span,
                "a helper with statements, called where its call may not run",
            )
        } else {
            self.inline_body(params, &body.statements, &args)
        };
        self.inlining.pop();
        self.scopes.pop();
        self.scopes.extend(stash);
        (self.span_base, self.salt) = saved;
        r
    }

    /// A statement-form helper's body, into `pre`: the parameters as
    /// locals, then the body in a block its `return`s leave.
    fn inline_body(
        &mut self,
        params: &FormalParameters<'_>,
        stmts: &[Statement<'_>],
        args: &[V],
    ) -> R<V> {
        let before = self.facts.clone();
        for (i, p) in params.items.iter().enumerate() {
            let BindingPattern::BindingIdentifier(b) = &p.pattern else {
                continue;
            };
            let v = args.get(i).cloned().unwrap_or_else(V::null);
            let key = self.salt + self.span_base + p.span.start;
            let ty = self.widen.get(&key).copied().unwrap_or(v.ty);
            let l = self.f.local(b.name.as_str(), ty);
            let e = self.coerce(v.clone(), ty, p.span)?;
            self.pre.push(Stmt::Assign(l, e));
            self.facts.remove(&Key::L(l));
            if v.nn {
                self.facts.insert(Key::L(l));
            }
            self.decls.insert(l, key);
            self.bind(b.name.as_str(), Var::Local(l));
        }
        let label = self.next_label();
        self.jframes.push(JFrame::Helper {
            label,
            result: None,
            nn: true,
        });
        let body = self.block(stmts);
        let Some(JFrame::Helper { result, nn, .. }) = self.jframes.pop() else {
            return Err(Stop);
        };
        // What the helper assigned may not hold its old facts; facts it
        // learned may not hold either (a `return` can leave first).
        let mut changed = BTreeSet::new();
        assigned(&body, &mut changed);
        self.facts = before;
        self.facts
            .retain(|k| !matches!(k, Key::L(l) if changed.contains(l)));
        let always = exits(&body);
        match result {
            Some(l) => {
                self.pre.push(Stmt::Assign(l, Expr::Lit(Literal::Null)));
                self.pre.push(Stmt::Block(label, body));
                Ok(V {
                    e: Expr::Local(l),
                    ty: self.f.local_def(l).ty,
                    nn: nn && always,
                })
            }
            None => {
                self.pre.push(Stmt::Block(label, body));
                Ok(V::null())
            }
        }
    }

    fn need_nn(&mut self, v: &V, span: Span) -> R<()> {
        if v.nn {
            Ok(())
        } else {
            self.stop(
                "js.nullable_receiver",
                span,
                "the value may be null here (JavaScript would throw a TypeError)",
            )
        }
    }

    /// A number operand: `null` reads as 0, as in JavaScript.
    fn num(&mut self, v: V, span: Span) -> R<V> {
        match v.ty {
            Ty::Int | Ty::Float => Ok(if v.nn {
                v
            } else {
                let zero = if v.ty == Ty::Int {
                    Expr::int(0)
                } else {
                    Expr::Lit(Literal::Float(0.0))
                };
                V {
                    e: Expr::bin(BinOp::Coalesce, v.e, zero),
                    ty: v.ty,
                    nn: true,
                }
            }),
            Ty::Null => Ok(V::lit(Literal::Int(0))),
            // JavaScript's `+x` of anything but a number converts it: run
            // the record in the engine tier then.
            Ty::Json if self.dyn_expr(&v.e) => Ok(V {
                e: Expr::call(Builtin::JsonAsNum, vec![v.e, Expr::bool(false)]),
                ty: Ty::Float,
                nn: true,
            }),
            other => self.stop(
                "js.dynamic_types",
                span,
                format!("arithmetic on a {other} value"),
            ),
        }
    }

    fn widen_pair(a: V, b: V) -> (Expr, Expr, Ty) {
        if a.ty == Ty::Int && b.ty == Ty::Int {
            (a.e, b.e, Ty::Int)
        } else {
            let f = |v: V| {
                if v.ty == Ty::Int {
                    Expr::call(Builtin::ToFloat, vec![v.e])
                } else {
                    v.e
                }
            };
            (f(a), f(b), Ty::Float)
        }
    }

    #[allow(clippy::too_many_lines)]
    fn binary(&mut self, op: BinaryOperator, x: V, y: V, span: Span) -> R<V> {
        use BinaryOperator as B;
        match op {
            B::Addition if x.ty == Ty::Str || y.ty == Ty::Str => {
                let a = self.text_of(x, span)?;
                let b = self.text_of(y, span)?;
                Ok(V {
                    e: Expr::bin(BinOp::Concat, a, b),
                    ty: Ty::Str,
                    nn: true,
                })
            }
            B::Addition | B::Subtraction | B::Multiplication | B::Remainder => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let (a, b, ty) = Self::widen_pair(a, b);
                let o = match op {
                    B::Addition => BinOp::Add,
                    B::Subtraction => BinOp::Sub,
                    B::Multiplication => BinOp::Mul,
                    _ => BinOp::Mod,
                };
                Ok(V {
                    e: Expr::bin(o, a, b),
                    ty,
                    nn: true,
                })
            }
            B::Division => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let (a, b, _) = Self::widen_pair(a, b);
                Ok(V {
                    e: Expr::bin(BinOp::Div, a, b),
                    ty: Ty::Float,
                    nn: true,
                })
            }
            B::Exponential => {
                let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                let f = |v: V| {
                    if v.ty == Ty::Int {
                        Expr::call(Builtin::ToFloat, vec![v.e])
                    } else {
                        v.e
                    }
                };
                Ok(V {
                    e: Expr::call(Builtin::Pow, vec![f(a), f(b)]),
                    ty: Ty::Float,
                    nn: true,
                })
            }
            B::StrictEquality | B::StrictInequality | B::Equality | B::Inequality => {
                let negate = matches!(op, B::StrictInequality | B::Inequality);
                let strict = matches!(op, B::StrictEquality | B::StrictInequality);
                let same_kind = x.ty == y.ty
                    || (x.ty.is_number() && y.ty.is_number())
                    || x.ty == Ty::Null
                    || y.ty == Ty::Null;
                let (xd, yd) = (self.is_dyn(&x), self.is_dyn(&y));
                if xd != yd {
                    let (d, o) = if xd { (x, y) } else { (y, x) };
                    let e = self.dyn_equals(d, o, strict, span)?;
                    return Ok(V {
                        e: if negate {
                            Expr::Unary(UnOp::Not, Box::new(e))
                        } else {
                            e
                        },
                        ty: Ty::Bool,
                        nn: true,
                    });
                }
                if x.ty == Ty::Json || y.ty == Ty::Json {
                    return self.stop(
                        "js.dynamic_types",
                        span,
                        "comparing JSON values depends on their kind at run time",
                    );
                }
                let e = if same_kind {
                    let (a, b) = if x.ty.is_number() && y.ty.is_number() {
                        let (a, b, _) = Self::widen_pair(x, y);
                        (a, b)
                    } else {
                        (x.e, y.e)
                    };
                    Expr::bin(BinOp::Eq, a, b)
                } else if strict {
                    Expr::bool(false)
                } else {
                    return self.stop(
                        "js.loose_equality",
                        span,
                        "== between values of different kinds converts them",
                    );
                };
                Ok(V {
                    e: if negate {
                        Expr::Unary(UnOp::Not, Box::new(e))
                    } else {
                        e
                    },
                    ty: Ty::Bool,
                    nn: true,
                })
            }
            B::LessThan | B::LessEqualThan | B::GreaterThan | B::GreaterEqualThan => {
                let o = match op {
                    B::LessThan => BinOp::Lt,
                    B::LessEqualThan => BinOp::Le,
                    B::GreaterThan => BinOp::Gt,
                    _ => BinOp::Ge,
                };
                let (xd, yd) = (self.is_dyn(&x), self.is_dyn(&y));
                let (x, y) = match (xd, yd) {
                    // Text with text compares as text; anything else as
                    // numbers (another kind runs in the engine tier).
                    (true, false) if y.ty == Ty::Str && y.nn => {
                        (Self::undyn(x, Ty::Str, false).unwrap_or_else(V::null), y)
                    }
                    (false, true) if x.ty == Ty::Str && x.nn => {
                        let y = Self::undyn(y, Ty::Str, false).unwrap_or_else(V::null);
                        (x, y)
                    }
                    (true, true) => {
                        return self.stop("js.compare", span, "comparing two parsed JSON values");
                    }
                    (true, false) => (self.num(x, span)?, y),
                    (false, true) => {
                        let y = self.num(y, span)?;
                        (x, y)
                    }
                    _ => (x, y),
                };
                let (a, b) = if x.ty == Ty::Str && y.ty == Ty::Str && x.nn && y.nn {
                    (x.e, y.e)
                } else if matches!(x.ty, Ty::Int | Ty::Float | Ty::Null)
                    && matches!(y.ty, Ty::Int | Ty::Float | Ty::Null)
                {
                    let (a, b) = (self.num(x, span)?, self.num(y, span)?);
                    let (a, b, _) = Self::widen_pair(a, b);
                    (a, b)
                } else {
                    return self.stop(
                        "js.compare",
                        span,
                        "this comparison converts its operands at run time",
                    );
                };
                Ok(V {
                    e: Expr::bin(o, a, b),
                    ty: Ty::Bool,
                    nn: true,
                })
            }
            _ => self.stop("js.operator", span, "this operator runs in the engine tier"),
        }
    }

    /// Text for `+` and templates: JavaScript's `String(x)`.
    fn text_of(&mut self, v: V, span: Span) -> R<Expr> {
        Ok(match v.ty {
            Ty::Str => v.e,
            Ty::Null => Expr::Lit(Literal::Str("null".to_owned())),
            Ty::Int | Ty::Float | Ty::Bool => {
                if v.nn {
                    Expr::call(Builtin::ToStr, vec![v.e])
                } else {
                    Expr::bin(
                        BinOp::Coalesce,
                        Expr::call(Builtin::ToStr, vec![v.e]),
                        Expr::Lit(Literal::Str("null".to_owned())),
                    )
                }
            }
            Ty::Json if self.dyn_expr(&v.e) => {
                Expr::call(Builtin::JsonToStr, vec![v.e, Expr::bool(false)])
            }
            Ty::Json => {
                return self.stop(
                    "js.dynamic_types",
                    span,
                    "a JSON value as text depends on its kind",
                );
            }
        })
    }

    fn join(&mut self, a: Ty, b: Ty, span: Span) -> R<Ty> {
        match a.join(b) {
            Some(Ty::Null) => Ok(Ty::Null),
            Some(t) => Ok(t),
            None => self.stop(
                "js.types",
                span,
                format!("a value is {a} on one path and {b} on another"),
            ),
        }
    }

    fn coerce(&mut self, v: V, ty: Ty, span: Span) -> R<Expr> {
        if v.ty == ty || v.ty == Ty::Null {
            return Ok(v.e);
        }
        if self.is_dyn(&v)
            && let Some(r) = Self::undyn(v.clone(), ty, false)
        {
            return Ok(r.e);
        }
        if ty == Ty::Float && v.ty == Ty::Int {
            return Ok(Expr::call(Builtin::ToFloat, vec![v.e]));
        }
        if ty == Ty::Null {
            // A local first holding null: its type comes from this value.
            return self.stop("js.types", span, "a value of a new type");
        }
        self.stop(
            "js.types",
            span,
            format!("a {} value where {ty} is expected", v.ty),
        )
    }
}

/// Members every JavaScript object inherits: reading one of a parsed
/// object gives a function where MariaDB finds no member.
const PROTO_NAMES: &[&str] = &[
    "constructor",
    "__proto__",
    "hasOwnProperty",
    "isPrototypeOf",
    "propertyIsEnumerable",
    "toLocaleString",
    "toString",
    "valueOf",
    "__defineGetter__",
    "__defineSetter__",
    "__lookupGetter__",
    "__lookupSetter__",
];

/// Triggers on streams and queues: `ctx.record`, `ctx.message`,
/// `JSON.parse` and parsed values. A parsed value is JSON text (its JSON
/// `null` the text `null`, `undefined` a `NULL`); wherever JavaScript's
/// meaning depends on a kind the code can't know until it runs, the
/// generated code checks the kind and, when it isn't the one compiled
/// for, raises 39876 so the record runs again in the engine tier.
impl L<'_, '_> {
    /// Whether `e` is `ctx.record` or `ctx.message` (or a name given to
    /// it).
    fn event_of(&self, e: &Expression<'_>) -> bool {
        let Some(on) = self.msg else {
            return false;
        };
        match strip_parens(e) {
            Expression::Identifier(id) => {
                matches!(self.lookup(id.name.as_str()), Some(Var::Event))
            }
            Expression::StaticMemberExpression(m) if !m.optional => {
                m.property.name.as_str() == on.member()
                    && matches!(strip_parens(&m.object), Expression::Identifier(id)
                        if matches!(self.lookup(id.name.as_str()), Some(Var::Msg)))
            }
            _ => false,
        }
    }

    /// Field `name` of `ctx.record` or `ctx.message`: its parameter.
    fn msg_field(&mut self, name: &str, span: Span) -> R<V> {
        let Some(on) = self.msg else {
            return self.stop("js.member", span, format!("reading .{name} here"));
        };
        let Some((n, ty, nn)) = on.fields().iter().find(|(n, _, _)| *n == name).copied() else {
            return self.stop(
                "js.member",
                span,
                format!("ctx.{} has no field {name}", on.member()),
            );
        };
        let i = self.param_named(n, ty);
        if ty == Ty::Json {
            self.dyn_params.insert(i);
        }
        Ok(V {
            e: Expr::Param(i),
            ty,
            nn: nn || self.facts.contains(&Key::P(i)),
        })
    }

    fn dyn_expr(&self, e: &Expr) -> bool {
        match e {
            Expr::Builtin(Builtin::JsonParse | Builtin::JsonGet, _) => true,
            Expr::Local(l) => self.dyn_locals.contains(l),
            Expr::Param(i) => self.dyn_params.contains(i),
            Expr::Cond(_, a, b) => self.dyn_expr(a) && self.dyn_expr(b),
            _ => false,
        }
    }

    /// Whether `v` is a parsed JSON value.
    fn is_dyn(&self, v: &V) -> bool {
        v.ty == Ty::Json && self.dyn_expr(&v.e)
    }

    /// `v.prop` (`v?.prop` with `optional`) of a parsed value.
    fn json_member(&mut self, v: V, prop: &str, optional: bool, span: Span) -> R<V> {
        if prop == "length" {
            if optional {
                return self.stop("js.optional_chain", span, "?.length");
            }
            return Ok(V {
                e: Expr::call(Builtin::JsonLen, vec![v.e]),
                ty: Ty::Int,
                nn: true,
            });
        }
        if PROTO_NAMES.contains(&prop) {
            return self.stop(
                "js.member",
                span,
                format!("reading .{prop} of a parsed JSON value gives JavaScript's own member"),
            );
        }
        Ok(V {
            e: Expr::call(
                Builtin::JsonGet,
                vec![
                    v.e,
                    Expr::Lit(Literal::Str(prop.to_owned())),
                    Expr::bool(optional),
                ],
            ),
            ty: Ty::Json,
            nn: false,
        })
    }

    /// `obj[expr]`: `ctx.record['key']`, or a member or element of a
    /// parsed value.
    fn computed_member(&mut self, m: &oxc_ast::ast::ComputedMemberExpression<'_>) -> R<V> {
        if !m.optional
            && self.event_of(&m.object)
            && let Expression::StringLiteral(k) = strip_parens(&m.expression)
        {
            return self.msg_field(k.value.as_str(), m.span);
        }
        let v = self.val(&m.object)?;
        if !self.is_dyn(&v) {
            return self.stop("js.member", m.span, "a computed member");
        }
        if let Expression::StringLiteral(k) = strip_parens(&m.expression) {
            return self.json_member(v, k.value.as_str(), m.optional, m.span);
        }
        let i = self.val(&m.expression)?;
        if i.ty != Ty::Int {
            return self.stop(
                "js.member",
                m.span,
                "an index that isn't a whole number or a literal name",
            );
        }
        Ok(V {
            e: Expr::call(Builtin::JsonGet, vec![v.e, i.e, Expr::bool(m.optional)]),
            ty: Ty::Json,
            nn: false,
        })
    }

    /// A parsed value read as `ty` (numbers as doubles, or `Int` for an
    /// `Int` local): any other kind runs the record in the engine tier;
    /// with `nullable`, `null` and `undefined` are `NULL`.
    fn undyn(v: V, ty: Ty, nullable: bool) -> Option<V> {
        let (b, t) = match ty {
            Ty::Str => (Builtin::JsonAsStr, Ty::Str),
            Ty::Float => (Builtin::JsonAsNum, Ty::Float),
            Ty::Int => (Builtin::JsonAsInt, Ty::Int),
            Ty::Bool => (Builtin::JsonAsBool, Ty::Bool),
            Ty::Json | Ty::Null => return None,
        };
        Some(V {
            e: Expr::call(b, vec![v.e, Expr::bool(nullable)]),
            ty: t,
            nn: !nullable,
        })
    }

    /// The other side of a comparison with a parsed value, as the type
    /// the parsed value is read as: numbers as doubles.
    fn read_as(t: Ty) -> Ty {
        if t == Ty::Int { Ty::Float } else { t }
    }

    /// `d === o`, `d == o` (and negated by the caller) where `d` is parsed
    /// and `o` isn't.
    fn dyn_equals(&mut self, d: V, o: V, strict: bool, span: Span) -> R<Expr> {
        if !o.nn {
            return self.stop(
                "js.dynamic_types",
                span,
                "comparing a parsed JSON value with a value that may be null",
            );
        }
        let ty = Self::read_as(o.ty);
        let kind = match ty {
            Ty::Str => "string",
            Ty::Float => "number",
            Ty::Bool => "boolean",
            _ => {
                return self.stop(
                    "js.dynamic_types",
                    span,
                    "comparing a parsed JSON value with this value",
                );
            }
        };
        let o_e = if o.ty == Ty::Int {
            Expr::call(Builtin::ToFloat, vec![o.e])
        } else {
            o.e
        };
        // `==` converts across kinds in JavaScript: any other kind runs
        // the record in the engine tier.
        let Some(read) = Self::undyn(d.clone(), ty, strict) else {
            return self.stop("js.dynamic_types", span, "this comparison");
        };
        let eq = Expr::bin(BinOp::Eq, read.e, o_e);
        if !strict {
            return Ok(eq);
        }
        // Another kind is never equal.
        Ok(Expr::Cond(
            Box::new(Expr::call(
                Builtin::JsonKind,
                vec![d.e, Expr::Lit(Literal::Str(kind.to_owned()))],
            )),
            Box::new(eq),
            Box::new(Expr::bool(false)),
        ))
    }

    /// `x === null`, `x === undefined`, `x == null` (and their negations)
    /// of a parsed value; `typeof x === 'kind'`; `None` otherwise (nothing
    /// lowered).
    fn dyn_test(&mut self, b: &oxc_ast::ast::BinaryExpression<'_>) -> R<Option<V>> {
        use BinaryOperator as B;
        if self.msg.is_none() {
            return Ok(None);
        }
        let (negate, strict) = match b.operator {
            B::StrictEquality => (false, true),
            B::StrictInequality => (true, true),
            B::Equality => (false, false),
            B::Inequality => (true, false),
            _ => return Ok(None),
        };
        let undefined = |x: &Expression<'_>| {
            matches!(strip_parens(x), Expression::Identifier(u)
                if u.name.as_str() == "undefined" && self.lookup("undefined").is_none())
        };
        let null = |x: &Expression<'_>| matches!(strip_parens(x), Expression::NullLiteral(_));
        let wrap = |e: Expr| {
            let e = if negate {
                Expr::Unary(UnOp::Not, Box::new(e))
            } else {
                e
            };
            V {
                e,
                ty: Ty::Bool,
                nn: true,
            }
        };
        let kind = |e: &Expr, k: &str| {
            Expr::call(
                Builtin::JsonKind,
                vec![e.clone(), Expr::Lit(Literal::Str(k.to_owned()))],
            )
        };
        // `typeof x === 'kind'`.
        for (t, lit) in [(&b.left, &b.right), (&b.right, &b.left)] {
            if let Expression::UnaryExpression(u) = strip_parens(t)
                && u.operator == UnaryOperator::Typeof
                && let Expression::StringLiteral(k) = strip_parens(lit)
            {
                let v = self.val(&u.argument)?;
                if !self.is_dyn(&v) {
                    return self.stop(
                        "js.operator",
                        u.span,
                        "typeof of a value that isn't parsed JSON",
                    );
                }
                let e = match k.value.as_str() {
                    "object" => Expr::Binary(
                        BinOp::Or,
                        Box::new(Expr::Binary(
                            BinOp::Or,
                            Box::new(kind(&v.e, "object")),
                            Box::new(kind(&v.e, "array")),
                        )),
                        Box::new(kind(&v.e, "null")),
                    ),
                    k @ ("string" | "number" | "boolean" | "undefined") => kind(&v.e, k),
                    _ => Expr::bool(false),
                };
                return Ok(Some(wrap(e)));
            }
        }
        // `x === null` and the like.
        for (x, lit) in [(&b.left, &b.right), (&b.right, &b.left)] {
            let (is_null, is_undef) = (null(lit), undefined(lit));
            if !is_null && !is_undef {
                continue;
            }
            if null(x) || undefined(x) {
                return Ok(None);
            }
            let v = self.val(x)?;
            if !self.is_dyn(&v) {
                // Not parsed: the ordinary comparison with null.
                let n = V::null();
                let (l, r) = if std::ptr::eq(x, &b.left) {
                    (v, n)
                } else {
                    (n, v)
                };
                return self.binary(b.operator, l, r, b.span).map(Some);
            }
            let e = if !strict {
                Expr::Binary(
                    BinOp::Or,
                    Box::new(kind(&v.e, "null")),
                    Box::new(kind(&v.e, "undefined")),
                )
            } else if is_null {
                kind(&v.e, "null")
            } else {
                kind(&v.e, "undefined")
            };
            return Ok(Some(wrap(e)));
        }
        Ok(None)
    }

    /// `JSON.parse(text)`, `Number(x)` and `Array.isArray(x)` of parsed
    /// JSON; `None` for any other call.
    fn json_call(
        &mut self,
        c: &oxc_ast::ast::CallExpression<'_>,
        args: &[&Expression<'_>],
    ) -> R<Option<V>> {
        let global = |s: &Self, n: &str| s.lookup(n).is_none();
        let path = self.callee_path(&c.callee);
        let name = match strip_parens(&c.callee) {
            Expression::Identifier(id) if global(self, id.name.as_str()) => Some(id.name.as_str()),
            _ => None,
        };
        if path.as_deref() == Some(&["JSON", "parse"]) {
            if self.msg.is_none() {
                return self.stop(
                    "js.json_parse",
                    c.span,
                    "JSON.parse compiles only in triggers on streams and queues, which can run \
                     a record again in the engine tier",
                );
            }
            let [x] = args else {
                return self.stop("js.json_parse", c.span, "JSON.parse with a reviver");
            };
            let v = self.val(x)?;
            let text = if self.is_dyn(&v) {
                Self::undyn(v, Ty::Str, false).map_or(Expr::Lit(Literal::Null), |v| v.e)
            } else {
                match v.ty {
                    Ty::Str | Ty::Null => v.e,
                    _ => {
                        return self.stop(
                            "js.json_parse",
                            x.span(),
                            "JSON.parse of a value that isn't text",
                        );
                    }
                }
            };
            return Ok(Some(V {
                e: Expr::call(Builtin::JsonParse, vec![text]),
                ty: Ty::Json,
                nn: true,
            }));
        }
        if path.as_deref() == Some(&["Array", "isArray"]) && global(self, "Array") {
            let [x] = args else {
                return self.stop("js.call", c.span, "the number of arguments");
            };
            let v = self.val(x)?;
            if !self.is_dyn(&v) {
                return self.stop(
                    "js.call",
                    c.span,
                    "Array.isArray of a value that isn't parsed JSON",
                );
            }
            return Ok(Some(V {
                e: Expr::call(
                    Builtin::JsonKind,
                    vec![v.e, Expr::Lit(Literal::Str("array".to_owned()))],
                ),
                ty: Ty::Bool,
                nn: true,
            }));
        }
        if name == Some("Number") {
            let [x] = args else {
                return self.stop("js.call", c.span, "the number of arguments");
            };
            let v = self.val(x)?;
            if self.is_dyn(&v) || v.ty.is_number() {
                return self.num(v, x.span()).map(Some);
            }
            return self.stop("js.call", c.span, "Number() of a value that isn't a number");
        }
        Ok(None)
    }

    /// `x ?? y`, `x || y`, `x && y` where `x` (`xd`) or `y` (`yd`) is
    /// parsed JSON: the parsed side is read as the other side's type, or
    /// both stay parsed (`||` and `&&` only).
    fn dyn_logical(
        &mut self,
        op: LogicalOperator,
        x: V,
        y: V,
        xd: bool,
        yd: bool,
        span: Span,
    ) -> R<V> {
        let typed = |s: &mut Self, d: V, ty: Ty, nullable: bool| -> R<V> {
            match Self::undyn(d, ty, nullable) {
                Some(v) => Ok(v),
                None => s.stop(
                    "js.dynamic_types",
                    span,
                    "a parsed JSON value with a value of no kind it can be read as",
                ),
            }
        };
        if op == LogicalOperator::Coalesce {
            if xd && yd {
                return self.stop("js.dynamic_types", span, "?? of two parsed JSON values");
            }
            let ty = Self::read_as(if xd { y.ty } else { x.ty });
            let nn = xd && y.nn;
            let (a, b) = if xd {
                let a = typed(self, x, ty, true)?;
                (a.e, self.coerce(y, ty, span)?)
            } else {
                let b = typed(self, y, ty, true)?;
                (self.coerce(x, ty, span)?, b.e)
            };
            return Ok(V {
                e: Expr::bin(BinOp::Coalesce, a, b),
                ty,
                nn,
            });
        }
        let t = self.truthy(x.clone(), span)?;
        let (a, b, ty) = if xd && yd {
            (x.e, y.e, Ty::Json)
        } else if xd {
            let ty = Self::read_as(y.ty);
            let a = typed(self, x, ty, false)?;
            (a.e, self.coerce(y, ty, span)?, ty)
        } else {
            let ty = Self::read_as(x.ty);
            let b = typed(self, y, ty, false)?;
            (self.coerce(x, ty, span)?, b.e, ty)
        };
        let e = if op == LogicalOperator::Or {
            Expr::Cond(Box::new(t), Box::new(a), Box::new(b))
        } else {
            Expr::Cond(Box::new(t), Box::new(b), Box::new(a))
        };
        Ok(V { e, ty, nn: false })
    }

    /// One `console.log` or `ctx.log` line: the arguments' text, joined
    /// by spaces (QuickJS's formatting for text, numbers, booleans and
    /// `null`).
    fn log_line(&mut self, c: &oxc_ast::ast::CallExpression<'_>) -> R<Expr> {
        let mut acc: Option<Expr> = None;
        for a in &c.arguments {
            let Some(x) = a.as_expression() else {
                return self.stop("js.spread", c.span, "spread arguments");
            };
            let v = self.val(x)?;
            if v.ty == Ty::Json && !self.is_dyn(&v) {
                return self.stop(
                    "js.log",
                    x.span(),
                    "logging an object: QuickJS writes its JSON",
                );
            }
            let t = self.text_of(v, x.span())?;
            acc = Some(match acc {
                None => t,
                Some(p) => Expr::bin(
                    BinOp::Concat,
                    Expr::bin(BinOp::Concat, p, Expr::Lit(Literal::Str(" ".to_owned()))),
                    t,
                ),
            });
        }
        Ok(acc.unwrap_or(Expr::Lit(Literal::Str(String::new()))))
    }
}

/// A JavaScript number literal: whole numbers within ±2^53 are `Int`.
fn number(x: f64) -> Literal {
    if x.fract() == 0.0 && x.abs() < 9_007_199_254_740_992.0 {
        Literal::Int(x as i64)
    } else {
        Literal::Float(x)
    }
}

#[cfg(test)]
mod tests;
