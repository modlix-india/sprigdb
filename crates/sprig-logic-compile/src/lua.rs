//! The Lua front end and the Lua subset it compiles.
//!
//! [`ast`] parses Lua 5.1 (our own parser: `mlua`, the engine tier's
//! runtime, has no syntax tree); this module lowers the compilable subset
//! into the shared IR, which the checks and the PL/SQL generator then
//! treat as KIRun's and JavaScript's. Anything outside the subset is a
//! placement reason (`lua.*`), never an error: the version then runs in the
//! engine tier on the shared Lua runtime (`sprig-logic-js::lua`), which
//! defines the language.
//!
//! The two forms of a body are the engine tier's: a chunk run with the
//! parameters as locals by name, or a chunk that is one
//! `return function (a, b) ... end`, called with the arguments.
//! Parameter types come from the declaration ([`Signature`], shared with
//! the JavaScript front end).
//!
//! **Lua's meaning, kept.** The engine tier hands SQL `NULL` to Lua as
//! `sprig.null`, a value distinct from `nil`: arguments, columns of rows
//! and fields of call results are `sprig.null` when `NULL`, and
//! `sprig.null` is *true* in a condition (only `nil` and `false` are
//! false). So every value the lowering makes knows which "null" its `NULL`
//! stands for ([`Nul`]), and conditions, `==`, `and`, `or` and fields of
//! results follow it:
//!
//! - `nil` and `false` are false, everything else is true (`0` and `''`
//!   too); `x == nil` and `x == sprig.null` test the right one;
//! - arithmetic, `..`, `<` and friends on `nil` or `sprig.null` raise
//!   Lua's error (39865), through a check before the statement; a value
//!   tested first (`if x ~= sprig.null then`) needs none;
//! - `and` and `or` return an operand; `c and x or y` is a conditional
//!   when `x` can't be false or `nil`;
//! - `%` takes the divisor's sign (`a - floor(a/b)*b`); `/` and `^` give a
//!   double; `math.floor(a / b)` of whole numbers is a whole number;
//! - a table built with names (`{total = t}`) is a record: its fields are
//!   locals, read and written by name, and it becomes an event's fields
//!   or a JSON object; a field that may be `nil` where the record leaves
//!   the code is a reason, since Lua drops `nil` fields;
//! - `db.sql` rows are a JSON array, iterated with `ipairs`; an empty one
//!   leaves the code as `{}`, as the engine tier's `cjson` writes it;
//! - a trigger on a stream or queue reads `ctx.record` or `ctx.message`
//!   (a `null` field is `sprig.null`), and `cjson.decode(text)` is the
//!   text itself, read where it is used as the kind the use needs
//!   (`expr::dval`); a value Lua would treat another way (text in
//!   arithmetic, a number joined with `..`) raises 39876, and the record
//!   runs in the engine tier. Its `print` and `log` lines go to the run log.
//!
//! What compiles: `local` (several names, several values), assignment
//! (several targets), `if`/`elseif`/`else`, `while`, `repeat ... until`
//! (the condition sees the body's locals), numeric `for`, `for i, r in
//! ipairs(rows)`, `break`, `do ... end`, `return`; numbers, strings,
//! booleans, `nil`, `sprig.null`; `+ - * / % ^ ..`, comparisons, `and`,
//! `or`, `not`, `#` of text and of rows; `math.abs floor ceil sqrt max min
//! pow fmod pi`, `string.upper lower len` (and as methods), `tostring`,
//! `tonumber` of numbers, `type`; helper functions (`local function f`,
//! `local f = function`, `function f`), inlined at each call in the scope
//! they were declared in; `error(message [, level])`, `assert(v, m)`,
//! `sprig.reject(m)`, `sprig.event(name, fields)`; `db.sql`, `db.sqlOne`,
//! `db.exec` with literal SQL text.
//!
//! Known differences (listed by `EXPLAIN FUNCTION`): whole numbers past
//! 2^63 overflow (39866) where Lua's doubles lose precision; division by
//! zero and other non-finite results are 39866 where Lua gives `inf` or
//! `nan`; a fraction as text has MariaDB's shortest digits where Lua writes
//! 14 significant digits (`0.1 + 0.2` is `0.30000000000000004`, not `0.3`);
//! `string.upper` and `string.lower` change non-ASCII letters too, where
//! Lua 5.1 changes ASCII only; `print` and `log` lines are dropped (except
//! in triggers on streams and queues).

pub mod ast;
mod expr;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use sprig_common::Result;

use crate::check::Analysis;
use crate::ir::{
    BinOp, Block, Bound, ErrorSpec, EventDef, Expr, Function, Literal, LocalId, Param, Stmt, Ty,
    UnOp,
};
use crate::link::CallDep;
use crate::sql::Columns;
use crate::{Compiled, Options, Reason};

use ast::{ExprKind, Func, Pos, Stat, StatKind};

use crate::js::MsgSource;
pub use crate::js::Signature;

/// Lua without its declaration's signature: always the engine tier
/// (callers with a declaration use [`compile_with`]).
#[must_use]
pub fn compile(_source: &str) -> Compiled {
    refused(Reason::new(
        "lua.signature",
        "source",
        "Lua compiles only with its declaration's parameter types",
    ))
}

fn refused(r: Reason) -> Compiled {
    Compiled {
        ir: None,
        analysis: Analysis::refused(r),
        plsql: None,
    }
}

/// Compiles `source`, a Lua function `name` with signature `sig`, against
/// the shard schema's columns `cat`. A source outside the subset gives
/// reasons (the engine tier), not an error.
///
/// # Errors
/// 39874 when embedded SQL and its arguments disagree.
pub fn compile_with(
    name: &str,
    source: &str,
    sig: &Signature,
    cat: &dyn Columns,
    opts: &Options,
) -> Result<Compiled> {
    let chunk = match ast::parse(source) {
        Ok(c) => c,
        Err(e) => {
            return Ok(refused(Reason::new(
                "lua.parse",
                "source",
                format!("the Lua parser refused it: {e}"),
            )));
        }
    };
    // Types, null kinds and record fields that later code widens: the
    // lowering runs again with what it learned.
    let mut learnt = Learnt::default();
    for _ in 0..8 {
        let mut l = L::new(name, sig, cat, learnt.clone());
        l.module_id = opts.module_id;
        l.program(&chunk);
        if let Some(e) = l.fatal.take() {
            return Err(e);
        }
        if l.again {
            learnt = std::mem::take(&mut l.learnt);
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
        "lua.types",
        "source",
        "the variables' types don't settle",
    )))
}

/// Which "null" a value's `NULL` stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Nul {
    /// Never `NULL`.
    Never,
    /// `nil` (false in a condition).
    Nil,
    /// `sprig.null` (true in a condition).
    SNull,
    /// Either: only tests that don't care compile.
    Both,
}

impl Nul {
    fn join(self, o: Nul) -> Nul {
        match (self, o) {
            (a, b) if a == b => a,
            (Nul::Never, b) => b,
            (a, Nul::Never) => a,
            _ => Nul::Both,
        }
    }

    /// The part a falsy test can return (`nil`, not `sprig.null`).
    fn nil_part(self) -> Nul {
        match self {
            Nul::Nil | Nul::Both => Nul::Nil,
            _ => Nul::Never,
        }
    }

    /// The part a truthy test keeps (`sprig.null`, not `nil`).
    fn snull_part(self) -> Nul {
        match self {
            Nul::SNull | Nul::Both => Nul::SNull,
            _ => Nul::Never,
        }
    }
}

/// What a pass learnt for the next one.
#[derive(Debug, Clone, Default, PartialEq)]
struct Learnt {
    /// Declaration key → the wider type.
    types: BTreeMap<WKey, Ty>,
    /// Declaration key → the null kind.
    kinds: BTreeMap<WKey, Nul>,
    /// A record's declaration key → its fields in first-assigned order.
    fields: BTreeMap<u64, Vec<String>>,
}

/// A declaration's key: the inlining salt and its position, and a name
/// (a record's field, a helper's result).
type WKey = (u64, String);

/// A record table: its fields' locals, shared by every name for it.
#[derive(Debug)]
struct Rec {
    key: u64,
    fields: Vec<(String, LocalId)>,
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
    /// One row of [`Var::Rows`] in `ipairs`.
    Elem {
        local: LocalId,
        cols: Vec<(String, Ty)>,
    },
    /// A record table.
    Record(Rc<RefCell<Rec>>),
    /// A helper function and the scope it was declared in.
    Helper {
        func: Rc<Func>,
        depth: usize,
    },
    /// An expression helper's parameter: its argument's value.
    Val(V),
    /// `pcall`'s second result where it can't be read in compiled code,
    /// and why.
    Unreadable(String),
    /// A validation's or trigger's `ctx`: `ctx.row`, `ctx.old` and
    /// `ctx.trigger` compile.
    Ctx,
    /// `ctx.trigger`: `.after`, `.before` and `.op`.
    Trigger,
    /// A validation's or trigger's row: after the write (`ctx.row`,
    /// `ctx.trigger.after`) or before it (`ctx.old`, `ctx.trigger.before`;
    /// `sprig.null` for an insert).
    Image(bool),
    /// A trigger on a stream or queue's `ctx`: `ctx.record` or
    /// `ctx.message` compiles.
    Msg,
    /// `ctx.record` or `ctx.message`: its fields by name.
    Event,
    /// A local holding a value `cjson.decode` made, as JSON text: `NULL`
    /// is `nil` (a missing member), the JSON `null` is `cjson.null`.
    Dyn(LocalId),
}

/// What a jump can reach.
#[derive(Debug, Clone)]
enum JFrame {
    Loop,
    /// An inlined helper: its block's label, and the local its value
    /// lands in.
    Helper {
        label: u32,
        result: Option<LocalId>,
        /// Whether every `return` gave a value, and the null kinds they
        /// gave.
        all_values: bool,
        kind: Nul,
    },
}

/// A fact key: a parameter or a local (a row's `found`) known non-null.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    P(u16),
    L(LocalId),
}

/// A lowered value: its IR, its type, and which null it may be.
#[derive(Debug, Clone)]
struct V {
    e: Expr,
    ty: Ty,
    n: Nul,
}

impl V {
    fn lit(l: Literal) -> Self {
        V {
            ty: l.ty(),
            e: Expr::Lit(l),
            n: Nul::Never,
        }
    }

    fn nil() -> Self {
        V {
            e: Expr::Lit(Literal::Null),
            ty: Ty::Null,
            n: Nul::Nil,
        }
    }

    fn snull() -> Self {
        V {
            e: Expr::Lit(Literal::Null),
            ty: Ty::Null,
            n: Nul::SNull,
        }
    }

    /// Never `nil` or `false`.
    fn never_falsy(&self) -> bool {
        self.ty != Ty::Bool && self.ty != Ty::Null && matches!(self.n, Nul::Never | Nul::SNull)
    }
}

/// A failed lowering: the reason is recorded; the caller gives up on the
/// statement.
struct Stop;

type R<T> = std::result::Result<T, Stop>;

/// `x IS NULL`.
fn is_null(e: Expr) -> Expr {
    Expr::bin(BinOp::Eq, e, Expr::Lit(Literal::Null))
}

fn not(e: Expr) -> Expr {
    Expr::Unary(UnOp::Not, Box::new(e))
}

fn str_lit(s: &str) -> Expr {
    Expr::Lit(Literal::Str(s.to_owned()))
}

/// Whether control can't continue after the lowered statements.
fn exits(b: &Block) -> bool {
    match b.last() {
        Some(Stmt::Return | Stmt::Throw(_) | Stmt::Rethrow(_) | Stmt::Leave(_)) => true,
        Some(Stmt::If(_, a, c)) => exits(a) && exits(c),
        _ => false,
    }
}

/// Whether a `pcall`'s body raises only errors whose message reads the
/// same in both tiers: the code's own `error` and `assert` (not a data
/// operation's error, nor Lua's own runtime errors, whose texts differ).
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
            Stmt::Db(crate::ir::DbOp::Sql { shape, .. }) => match shape {
                crate::ir::SqlShape::One { found, into } => {
                    out.insert(*found);
                    out.extend(into.iter().map(|(_, l)| *l));
                }
                crate::ir::SqlShape::Many { into, .. } => {
                    out.insert(*into);
                }
                crate::ir::SqlShape::Exec { affected }
                | crate::ir::SqlShape::Publish { affected, .. } => {
                    out.insert(*affected);
                }
                crate::ir::SqlShape::Call {
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

struct L<'s, 'c> {
    sig: &'s Signature,
    cat: &'c dyn Columns,
    f: Function,
    reasons: Vec<Reason>,
    lints: Vec<Reason>,
    scopes: Vec<Vec<(String, Var)>>,
    facts: BTreeSet<Key>,
    /// Statements that must run before the statement being lowered, in
    /// order: checks of values it reads unconditionally (Lua's error when
    /// one is `nil`) and the bodies of helper functions it calls.
    pre: Block,
    /// Inside conditionally evaluated operands: the conditions under
    /// which the operand being lowered runs. Checks and helper bodies it
    /// needs run under them.
    conds: Vec<Expr>,
    /// The call a statement consists of (`f(x)`, `local r = f(x)`,
    /// `return f(x)`), by address: a helper that writes the caller's
    /// locals may be inlined only there.
    sole: Option<usize>,
    jframes: Vec<JFrame>,
    labels: u32,
    /// Helpers being inlined: a call of one of them again is recursion.
    inlining: Vec<usize>,
    /// Added to declaration keys per inlined call, so two calls of one
    /// helper learn apart.
    salt: u64,
    inlines: u32,
    learnt: Learnt,
    /// Set when the pass learnt something: run again.
    again: bool,
    /// Event name → fields in first-seen order.
    events: Vec<(String, Vec<(String, Ty)>)>,
    tmp: u32,
    /// Each local's declaration key, and its null kind.
    decls: BTreeMap<LocalId, WKey>,
    kinds: BTreeMap<LocalId, Nul>,
    /// An error that isn't a reason (39874).
    fatal: Option<sprig_common::SprigError>,
    module_id: u64,
    calls: Vec<CallDep>,
    reaches: BTreeSet<u64>,
    depth: u32,
    /// Lowering a `return`'s value: tables in it can't change any more.
    returning: bool,
    /// Validations and triggers: the table, and the local saying whether
    /// the row before the write exists (set from `op` once it is read).
    table: Option<String>,
    old_found: Option<LocalId>,
    old_used: std::cell::Cell<bool>,
    /// Triggers on streams and queues: what `ctx` holds.
    msg: Option<MsgSource>,
}

impl<'s, 'c> L<'s, 'c> {
    fn new(name: &str, sig: &'s Signature, cat: &'c dyn Columns, learnt: Learnt) -> Self {
        // A validation's or trigger's parameters are the columns it reads,
        // added as they are read.
        let mut f = Function {
            name: name.to_owned(),
            params: if sig.row.is_some() {
                Vec::new()
            } else {
                sig.params
                    .iter()
                    .map(|(n, t)| Param {
                        name: n.clone(),
                        ty: *t,
                        min: None,
                        max: None,
                    })
                    .collect()
            },
            ..Function::default()
        };
        let old_found = sig.row.as_ref().map(|_| f.local("old_found", Ty::Bool));
        L {
            sig,
            cat,
            f,
            reasons: Vec::new(),
            lints: Vec::new(),
            scopes: vec![Vec::new()],
            facts: BTreeSet::new(),
            pre: Vec::new(),
            conds: Vec::new(),
            sole: None,
            jframes: Vec::new(),
            labels: 0,
            inlining: Vec::new(),
            salt: 0,
            inlines: 0,
            learnt,
            again: false,
            events: vec![("output".to_owned(), Vec::new())],
            tmp: 0,
            decls: BTreeMap::new(),
            kinds: BTreeMap::new(),
            fatal: None,
            module_id: 0,
            calls: Vec::new(),
            reaches: BTreeSet::new(),
            depth: 0,
            returning: false,
            table: sig.row.clone(),
            old_found,
            old_used: std::cell::Cell::new(false),
            msg: sig.on,
        }
    }

    fn at(pos: Pos) -> String {
        format!("line {}, column {}", pos.line, pos.col)
    }

    fn stop<T>(&mut self, code: &str, pos: Pos, detail: impl Into<String>) -> R<T> {
        self.reasons.push(Reason::new(code, &Self::at(pos), detail));
        Err(Stop)
    }

    fn finish(mut self) -> (Function, Vec<Reason>, Vec<Reason>) {
        if let (Some(found), true) = (self.old_found, self.old_used.get()) {
            // The row before the write is there for an update and a
            // delete, not an insert.
            if self.column_ty("op").is_some() {
                self.reasons.push(Reason::new(
                    "lua.old",
                    "source",
                    "the table has a column named op, so ctx.old can't tell an insert from an \
                     update",
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
        if let Some(ty) = self.sig.returns {
            self.events[0].1 = vec![("value".to_owned(), ty)];
        }
        // A local that only ever held `nil` holds text, never set.
        for l in &mut self.f.locals {
            if l.ty == Ty::Null {
                l.ty = Ty::Str;
            }
        }
        self.f.events = self
            .events
            .into_iter()
            .map(|(name, fields)| EventDef { name, fields })
            .collect();
        (self.f, self.reasons, self.lints)
    }

    // -- names ----------------------------------------------------------------

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

    fn scoped<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.scopes.push(Vec::new());
        let r = f(self);
        self.scopes.pop();
        r
    }

    /// A declaration's key in this inlining.
    fn wkey(&self, pos: Pos, name: &str) -> WKey {
        (self.salt | u64::from(pos.key()), name.to_owned())
    }

    /// A new local declared at `key` for a value of type `ty` and null
    /// kind `n`, with what earlier passes learnt.
    fn new_local(&mut self, name: &str, key: WKey, ty: Ty, n: Nul) -> LocalId {
        let ty = match self.learnt.types.get(&key) {
            Some(w) => *w,
            None => ty,
        };
        let n = self.learnt.kinds.get(&key).map_or(n, |k| k.join(n));
        let l = self.f.local(name, ty);
        self.decls.insert(l, key);
        self.kinds.insert(l, n);
        l
    }

    /// A local's null kind as read now.
    fn local_null(&self, l: LocalId) -> Nul {
        if self.facts.contains(&Key::L(l)) {
            Nul::Never
        } else {
            self.kinds.get(&l).copied().unwrap_or(Nul::Nil)
        }
    }

    fn local_val(&self, l: LocalId) -> V {
        V {
            e: Expr::Local(l),
            ty: self.f.local_def(l).ty,
            n: self.local_null(l),
        }
    }

    /// Assigns `v` to local `l`: a wider type or null kind than the
    /// local's is learnt and the lowering runs again.
    fn set_local(&mut self, l: LocalId, v: V, pos: Pos, out: &mut Block) -> R<()> {
        let ty = self.f.local_def(l).ty;
        let kind = self.kinds.get(&l).copied().unwrap_or(Nul::Nil);
        let want_kind = kind.join(v.n);
        let mut learn = false;
        if want_kind != kind
            && let Some(k) = self.decls.get(&l).cloned()
        {
            self.learnt.kinds.insert(k, want_kind);
            learn = true;
        }
        let (vty, vn) = (v.ty, v.n);
        let e = match self.coerce(v, ty, pos) {
            Ok(e) => e,
            Err(Stop) => {
                if let (Some(w), Some(k)) = (ty.join(vty), self.decls.get(&l).cloned())
                    && w != ty
                {
                    self.reasons.pop();
                    self.learnt.types.insert(k, w);
                    learn = true;
                } else {
                    return Err(Stop);
                }
                Expr::Lit(Literal::Null)
            }
        };
        if learn {
            self.again = true;
        }
        self.flush(out);
        out.push(Stmt::Assign(l, e));
        self.facts.remove(&Key::L(l));
        if vn == Nul::Never {
            self.facts.insert(Key::L(l));
        }
        Ok(())
    }

    fn coerce(&mut self, v: V, ty: Ty, pos: Pos) -> R<Expr> {
        if v.ty == ty || v.ty == Ty::Null {
            return Ok(v.e);
        }
        if ty == Ty::Float && v.ty == Ty::Int {
            return Ok(Expr::call(crate::ir::Builtin::ToFloat, vec![v.e]));
        }
        self.stop(
            "lua.types",
            pos,
            format!("a {} value where {ty} is expected", v.ty),
        )
    }

    fn join(&mut self, a: Ty, b: Ty, pos: Pos) -> R<Ty> {
        match a.join(b) {
            Some(t) => Ok(t),
            None => self.stop(
                "lua.types",
                pos,
                format!("a value is {a} on one path and {b} on another"),
            ),
        }
    }

    // -- validations and triggers: the row ----------------------------------------

    /// The type of the bound table's column `c`.
    fn column_ty(&self, c: &str) -> Option<Ty> {
        let t = self.table.as_deref()?;
        self.cat.column(t, &c.to_ascii_lowercase())
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

    /// The local saying whether the row before the write exists.
    fn old_flag(&self) -> Option<LocalId> {
        self.old_used.set(true);
        self.old_found
    }

    /// What `e` names of a validation's or trigger's `ctx`.
    fn ctx_path(&self, e: &ast::Expr) -> Option<Var> {
        match &e.k {
            ExprKind::Paren(x) => self.ctx_path(x),
            ExprKind::Name(n) => match self.lookup(n) {
                Some(v @ (Var::Ctx | Var::Trigger | Var::Image(_) | Var::Msg | Var::Event)) => {
                    Some(v)
                }
                _ => None,
            },
            ExprKind::Field(o, f) => match (self.ctx_path(o)?, f.as_str()) {
                (Var::Ctx, "row") | (Var::Trigger, "after") => Some(Var::Image(false)),
                (Var::Ctx, "old") | (Var::Trigger, "before") => Some(Var::Image(true)),
                (Var::Ctx, "trigger") => Some(Var::Trigger),
                (Var::Msg, m) if self.msg.is_some_and(|on| on.member() == m) => Some(Var::Event),
                _ => None,
            },
            _ => None,
        }
    }

    /// `f` of a `ctx` value: a column of a row, `ctx.trigger.op`.
    fn ctx_field(&mut self, base: &Var, f: &str, pos: Pos) -> R<V> {
        match base {
            Var::Event => {
                let Some(on) = self.msg else {
                    return Err(Stop);
                };
                let Some((name, ty, never_null)) =
                    on.fields().iter().find(|(n, _, _)| *n == f).copied()
                else {
                    // A field the record or message hasn't: nil, as in Lua.
                    return Ok(V::nil());
                };
                if ty == Ty::Json {
                    return self.stop(
                        "lua.json",
                        pos,
                        format!(
                            "ctx.{}.{f} is a table: read its members by name",
                            on.member()
                        ),
                    );
                }
                let i = self.param_named(name, ty);
                // A JSON `null` field is `sprig.null` in the engine tier.
                Ok(V {
                    e: Expr::Param(i),
                    ty,
                    n: if never_null || self.facts.contains(&Key::P(i)) {
                        Nul::Never
                    } else {
                        Nul::SNull
                    },
                })
            }
            Var::Msg => {
                let what = self.msg.map_or("record", MsgSource::member);
                self.stop(
                    "lua.ctx",
                    pos,
                    format!("ctx.{f} in compiled code isn't built: only ctx.{what} compiles"),
                )
            }
            Var::Trigger if f == "op" => {
                let i = self.param_named("op", Ty::Str);
                Ok(V {
                    e: Expr::Param(i),
                    ty: Ty::Str,
                    n: Nul::Never,
                })
            }
            Var::Image(old) => {
                let Some(ty) = self.column_ty(f) else {
                    // A column the table hasn't: nil, as in Lua.
                    return Ok(V::nil());
                };
                if *old {
                    let Some(found) = self.old_flag() else {
                        return Err(Stop);
                    };
                    if !self.facts.contains(&Key::L(found)) {
                        let spec =
                            lua_error(&self.f.name, pos, "attempt to index a userdata value");
                        self.guard(not(Expr::Local(found)), spec);
                        self.facts.insert(Key::L(found));
                    }
                }
                let name = if *old {
                    format!("old_{}", f.to_ascii_lowercase())
                } else {
                    f.to_ascii_lowercase()
                };
                let i = self.param_named(&name, ty);
                Ok(V {
                    e: Expr::Param(i),
                    ty,
                    n: if self.facts.contains(&Key::P(i)) {
                        Nul::Never
                    } else {
                        Nul::SNull
                    },
                })
            }
            Var::Ctx if matches!(f, "row" | "old" | "trigger") => self.stop(
                "lua.value",
                pos,
                format!("ctx.{f} as a value: read its fields by name"),
            ),
            Var::Trigger if matches!(f, "after" | "before") => self.stop(
                "lua.value",
                pos,
                format!("ctx.trigger.{f} as a value: read its columns by name"),
            ),
            _ => self.stop(
                "lua.ctx",
                pos,
                format!(
                    "ctx.{f} in compiled code isn't built: only ctx.row, ctx.old and \
                     ctx.trigger compile"
                ),
            ),
        }
    }

    // -- the program ------------------------------------------------------------

    fn program(&mut self, chunk: &[Stat]) {
        // `return function (a, b) ... end`: called with the arguments.
        let entry = match chunk {
            [
                Stat {
                    k: StatKind::Return(v),
                    ..
                },
            ] if v.len() == 1 => match &v[0].k {
                ExprKind::Function(f) => Some(Rc::clone(f)),
                _ => None,
            },
            _ => None,
        };
        // Parameters are locals in Lua: one the code assigns becomes a
        // local copy.
        let changed = ast::assigned_names(chunk);
        let mut head = Vec::new();
        if self.msg.is_some() {
            // A trigger on a stream or queue gets one argument, `ctx`.
            self.bind("ctx", Var::Msg);
            if changed.contains("ctx") {
                let _ = self.stop::<()>("lua.assign", Pos::default(), "ctx is assigned");
                return;
            }
            let body = match &entry {
                Some(f) => {
                    if f.vararg || f.params.len() > 1 {
                        let _ = self.stop::<()>(
                            "lua.params",
                            f.pos,
                            "a trigger on a stream or queue takes one parameter, ctx",
                        );
                        return;
                    }
                    self.scoped(|l| {
                        if let Some(p) = f.params.first() {
                            l.bind(p, Var::Msg);
                        }
                        l.block(&f.body)
                    })
                }
                None => self.scoped(|l| l.block(chunk)),
            };
            self.f.body = self.close(body);
            return;
        }
        if self.table.is_some() {
            // The engine tier calls a validation or trigger with one
            // argument, `ctx` (also the global `ctx`).
            self.bind("ctx", Var::Ctx);
            let body = match &entry {
                Some(f) => {
                    if f.vararg || f.params.len() > 1 {
                        let _ = self.stop::<()>(
                            "lua.params",
                            f.pos,
                            "a validation or trigger takes one parameter, ctx",
                        );
                        return;
                    }
                    self.scoped(|l| {
                        if let Some(p) = f.params.first() {
                            l.bind(p, Var::Ctx);
                        }
                        l.block(&f.body)
                    })
                }
                None => self.scoped(|l| l.block(chunk)),
            };
            self.f.body = self.close(body);
            return;
        }
        let names: Vec<String> = self.sig.params.iter().map(|(n, _)| n.clone()).collect();
        self.bind_params(&names, &changed, &mut head);
        let body = match &entry {
            Some(f) => {
                if f.vararg {
                    let _ = self.stop::<()>("lua.vararg", f.pos, "a function of ...");
                    return;
                }
                // The function's own parameters get the same arguments;
                // the chunk's are its upvalues.
                self.scoped(|l| {
                    l.bind_params(&f.params, &changed, &mut head);
                    l.block(&f.body)
                })
            }
            None => self.scoped(|l| l.block(chunk)),
        };
        head.extend(body);
        self.f.body = self.close(head);
    }

    /// Binds `names` to the arguments by position.
    fn bind_params(&mut self, names: &[String], changed: &BTreeSet<String>, head: &mut Block) {
        for (i, n) in names.iter().enumerate() {
            let Some((_, ty)) = self.sig.params.get(i) else {
                // Fewer arguments than parameters: the rest are nil.
                let l = self.new_local(n, (0, format!("extra {n}")), Ty::Null, Nul::Nil);
                head.push(Stmt::Assign(l, Expr::Lit(Literal::Null)));
                self.bind(n, Var::Local(l));
                continue;
            };
            let p = u16::try_from(i).unwrap_or(u16::MAX);
            if *ty == Ty::Json {
                // A JSON argument is a Lua table: only the engine tier
                // reads it.
                self.bind(n, Var::Param(p));
                continue;
            }
            if changed.contains(n) {
                let key = (u64::from(u32::MAX), format!("param {i} {n}"));
                let l = self.new_local(n, key, *ty, Nul::SNull);
                head.push(Stmt::Assign(l, Expr::Param(p)));
                self.bind(n, Var::Local(l));
            } else {
                self.bind(n, Var::Param(p));
            }
        }
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

    fn block(&mut self, stmts: &[Stat]) -> Block {
        let mut out = Vec::new();
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

    fn flush(&mut self, out: &mut Block) {
        out.append(&mut self.pre);
    }

    fn sub_block(&mut self, b: &[Stat]) -> Block {
        self.scoped(|l| l.block(b))
    }

    fn stmt(&mut self, s: &Stat, out: &mut Block) -> R<()> {
        self.sole = None;
        match &s.k {
            StatKind::Local(names, vals) => self.local(names, vals, s.pos, out),
            StatKind::LocalFunction(name, f) => {
                let depth = self.scopes.len() - 1;
                self.bind(
                    name,
                    Var::Helper {
                        func: Rc::clone(f),
                        depth,
                    },
                );
                Ok(())
            }
            StatKind::Function { path, method, func } => {
                if path.len() != 1 || method.is_some() {
                    return self.stop(
                        "lua.function",
                        s.pos,
                        "a function stored in a table runs in the engine tier",
                    );
                }
                // A global function: visible from here on, at the top.
                let v = Var::Helper {
                    func: Rc::clone(func),
                    depth: 0,
                };
                if let Some(top) = self.scopes.first_mut() {
                    top.push((path[0].clone(), v));
                }
                Ok(())
            }
            StatKind::Assign(targets, vals) => self.assign(targets, vals, s.pos, out),
            StatKind::Call(e) => self.call_stmt(e, out),
            StatKind::Do(b) => {
                let inner = self.sub_block(b);
                out.extend(inner);
                Ok(())
            }
            StatKind::If(arms, other) => self.if_stmt(arms, other.as_deref(), out),
            StatKind::While(c, b) => {
                self.flush(out);
                self.loop_entry();
                let (tpre, test) = self.loop_test(c)?;
                self.jframes.push(JFrame::Loop);
                let body = self.sub_block(b);
                self.jframes.pop();
                self.loop_entry();
                out.push(Self::looped(tpre, test, body, Bound::Guarded));
                Ok(())
            }
            StatKind::Repeat(b, c) => {
                // The condition sees the body's locals; it runs after the
                // body, and `true` ends the loop.
                self.flush(out);
                self.loop_entry();
                self.jframes.push(JFrame::Loop);
                let r = self.scoped(|l| {
                    let mut body = l.block(b);
                    if !exits(&body) {
                        let t = l.test(c)?;
                        l.flush(&mut body);
                        body.push(Stmt::If(t, vec![Stmt::Break], Vec::new()));
                    }
                    Ok(body)
                });
                self.jframes.pop();
                self.loop_entry();
                let body = r?;
                out.push(Stmt::While {
                    cond: Expr::bool(true),
                    body,
                    bound: Bound::Guarded,
                });
                Ok(())
            }
            StatKind::NumFor {
                var,
                start,
                limit,
                step,
                body,
            } => self.num_for(var, start, limit, step.as_ref(), body, s.pos, out),
            StatKind::GenFor { names, exprs, body } => self.gen_for(names, exprs, body, s.pos, out),
            StatKind::Return(vals) => self.ret(vals, s.pos, out),
            StatKind::Break => {
                if !matches!(self.jframes.last(), Some(JFrame::Loop)) {
                    return self.stop("lua.break", s.pos, "break outside a loop");
                }
                self.flush(out);
                out.push(Stmt::Break);
                Ok(())
            }
        }
    }

    fn if_stmt(
        &mut self,
        arms: &[(ast::Expr, ast::Block)],
        other: Option<&[Stat]>,
        out: &mut Block,
    ) -> R<()> {
        let Some(((c, b), rest)) = arms.split_first() else {
            return Ok(());
        };
        let test = self.test(c)?;
        let before = self.facts.clone();
        self.flush(out);
        let base = self.facts.clone();
        let (t_true, t_false) = (self.facts_if(c, true), self.facts_if(c, false));
        self.facts.extend(t_true);
        let a = self.sub_block(b);
        let after_a = std::mem::replace(&mut self.facts, base);
        self.facts.extend(t_false);
        let e = if rest.is_empty() {
            match other {
                Some(o) => self.sub_block(o),
                None => Vec::new(),
            }
        } else {
            let mut e = Vec::new();
            // An `elseif` lowers as an `if` in the `else`.
            let r = self.if_stmt(rest, other, &mut e);
            let p = std::mem::take(&mut self.pre);
            let mut full = p;
            full.extend(e);
            r?;
            full
        };
        let after_b = std::mem::take(&mut self.facts);
        self.facts = match (exits(&a), exits(&e)) {
            (true, true) => before,
            (true, false) => after_b,
            (false, true) => after_a,
            (false, false) => after_a.intersection(&after_b).copied().collect(),
        };
        out.push(Stmt::If(test, a, e));
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
                Var::Row { found, .. } => Some(*found),
                _ => None,
            })
            .collect();
        self.facts.retain(|k| match k {
            Key::P(_) => true,
            Key::L(l) => rows.contains(l),
        });
    }

    /// A loop's test, and the statements that must run before it each
    /// time.
    fn loop_test(&mut self, e: &ast::Expr) -> R<(Block, Expr)> {
        let t = self.test(e)?;
        Ok((std::mem::take(&mut self.pre), t))
    }

    fn looped(pre: Block, test: Expr, body: Block, bound: Bound) -> Stmt {
        if pre.is_empty() {
            return Stmt::While {
                cond: test,
                body,
                bound,
            };
        }
        let mut full = pre;
        full.push(Stmt::If(not(test), vec![Stmt::Break], Vec::new()));
        full.extend(body);
        Stmt::While {
            cond: Expr::bool(true),
            body: full,
            bound: Bound::Guarded,
        }
    }

    fn next_label(&mut self) -> u32 {
        self.labels += 1;
        self.labels
    }

    /// `local a, b = x, y`: every value first, then the names.
    fn local(&mut self, names: &[String], vals: &[ast::Expr], pos: Pos, out: &mut Block) -> R<()> {
        if vals.len() > names.len() {
            // Extra values are evaluated for their errors and dropped.
            for v in &vals[names.len()..] {
                if !matches!(v.k, ExprKind::Call(..) | ExprKind::Method(..)) {
                    self.val(v)?;
                }
            }
        }
        if let [x] = vals
            && names.len() <= 2
            && let ExprKind::Call(callee, args) = &x.k
            && self.is_lib(callee, &["pcall"])
        {
            self.sole = Some(std::ptr::from_ref(x) as usize);
            let sole = self.sole.take() == Some(std::ptr::from_ref(x) as usize);
            let (ok, second) = self.pcall(args, x.pos, sole)?;
            let mut binds = Vec::new();
            for (name, v) in names.iter().zip([Some(ok), None]) {
                let v = match (v, &second) {
                    (Some(v), _) => v,
                    (None, expr::Second::Val(v)) => v.clone(),
                    (None, expr::Second::Unreadable(why)) => {
                        binds.push((name.clone(), Var::Unreadable(why.clone())));
                        continue;
                    }
                };
                let key = self.wkey(pos, name);
                let l = self.new_local(name, key, v.ty, v.n);
                self.set_local(l, v, x.pos, out)?;
                binds.push((name.clone(), Var::Local(l)));
            }
            for (n, v) in binds {
                self.bind(&n, v);
            }
            return Ok(());
        }
        if let Some(last) = vals.last()
            && names.len() > vals.len()
            && matches!(last.k, ExprKind::Call(..) | ExprKind::Method(..))
        {
            return self.stop(
                "lua.multiple_results",
                last.pos,
                "several names from one call's results",
            );
        }
        let mut binds: Vec<(String, Var)> = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let Some(x) = vals.get(i) else {
                // No value: nil.
                let key = self.wkey(pos, name);
                let l = self.new_local(name, key, Ty::Null, Nul::Nil);
                self.flush(out);
                out.push(Stmt::Assign(l, Expr::Lit(Literal::Null)));
                self.facts.remove(&Key::L(l));
                binds.push((name.clone(), Var::Local(l)));
                continue;
            };
            let var = self.bound_value(x, name, pos, out)?;
            binds.push((name.clone(), var));
        }
        for (n, v) in binds {
            self.bind(&n, v);
        }
        Ok(())
    }

    /// What a declaration binds its name to: a data operation's result, a
    /// helper, a record, an alias of a table, or a new local.
    fn bound_value(&mut self, x: &ast::Expr, name: &str, pos: Pos, out: &mut Block) -> R<Var> {
        if let Some(v) = self.db_call(x, name, out)? {
            return Ok(v);
        }
        if let Some(v) = self.ctx_path(x) {
            return Ok(v);
        }
        match &x.k {
            ExprKind::Function(f) => {
                return Ok(Var::Helper {
                    func: Rc::clone(f),
                    depth: self.scopes.len() - 1,
                });
            }
            ExprKind::Table(fields) if Self::is_record(fields) => {
                let key = self.wkey(x.pos, name);
                return self.record(fields, &key, out);
            }
            // Tables are references: another name for the same one.
            ExprKind::Name(n) => {
                if let Some(
                    v @ (Var::Record(_)
                    | Var::Row { .. }
                    | Var::Rows { .. }
                    | Var::Exec { .. }
                    | Var::Call { .. }
                    | Var::Elem { .. }
                    | Var::Helper { .. }),
                ) = self.lookup(n)
                {
                    return Ok(v);
                }
            }
            _ => {}
        }
        // A decoded value (or a member of one): a local of its own.
        if let Some(d) = self.dval(x)? {
            let key = self.wkey(pos, name);
            let l = self.new_local(name, key, Ty::Json, Nul::Nil);
            self.flush(out);
            out.push(Stmt::Assign(l, d));
            return Ok(Var::Dyn(l));
        }
        self.sole = Some(std::ptr::from_ref(x) as usize);
        let v = self.val(x)?;
        let key = self.wkey(pos, name);
        let l = self.new_local(name, key, v.ty, v.n);
        self.set_local(l, v, x.pos, out)?;
        Ok(Var::Local(l))
    }

    /// Whether a table constructor is a record: named fields only (none
    /// at all is an empty record).
    fn is_record(fields: &[ast::TableField]) -> bool {
        fields.iter().all(|f| match f {
            ast::TableField::Named(..) => true,
            ast::TableField::Keyed(k, _) => matches!(k.k, ExprKind::Str(_)),
            ast::TableField::Item(_) => false,
        })
    }

    /// A record from a constructor: every field it will ever have gets a
    /// local, `nil` unless the constructor sets it.
    fn record(&mut self, fields: &[ast::TableField], key: &WKey, out: &mut Block) -> R<Var> {
        let rkey = key.0 ^ (u64::from(crc(&key.1)) << 40);
        let mut given: Vec<(String, V, Pos)> = Vec::new();
        for f in fields {
            let (n, x) = match f {
                ast::TableField::Named(n, x) => (n.clone(), x),
                ast::TableField::Keyed(k, x) => match &k.k {
                    ExprKind::Str(s) => (s.clone(), x),
                    _ => return self.stop("lua.table", k.pos, "a table key that isn't a name"),
                },
                ast::TableField::Item(x) => {
                    return self.stop("lua.table", x.pos, "a table with array items");
                }
            };
            let v = self.table_item(x)?;
            given.retain(|(g, _, _)| *g != n);
            given.push((n, v, x.pos));
        }
        let mut names: Vec<String> = given.iter().map(|(n, _, _)| n.clone()).collect();
        for n in self.learnt.fields.get(&rkey).cloned().unwrap_or_default() {
            if !names.contains(&n) {
                names.push(n);
            }
        }
        self.learnt.fields.insert(rkey, names.clone());
        let mut locals = Vec::new();
        for n in names {
            let fk = (rkey, n.clone());
            let (ty, kind) = match given.iter().find(|(g, _, _)| *g == n) {
                Some((_, v, _)) => (v.ty, v.n),
                None => (Ty::Null, Nul::Nil),
            };
            let l = self.new_local(&format!("{}_{n}", key.1), fk, ty, kind);
            match given.iter().find(|(g, _, _)| *g == n).cloned() {
                Some((_, v, p)) => self.set_local(l, v, p, out)?,
                None => {
                    self.flush(out);
                    out.push(Stmt::Assign(l, Expr::Lit(Literal::Null)));
                    self.facts.remove(&Key::L(l));
                    let k = self.kinds.get(&l).copied().unwrap_or(Nul::Nil);
                    self.kinds.insert(l, k.join(Nul::Nil));
                }
            }
            locals.push((n, l));
        }
        Ok(Var::Record(Rc::new(RefCell::new(Rec {
            key: rkey,
            fields: locals,
        }))))
    }

    /// `a, b.c = x, y`: every value first, then the targets, in order.
    fn assign(
        &mut self,
        targets: &[ast::Expr],
        vals: &[ast::Expr],
        pos: Pos,
        out: &mut Block,
    ) -> R<()> {
        if targets.len() > vals.len()
            && let Some(last) = vals.last()
            && matches!(last.k, ExprKind::Call(..) | ExprKind::Method(..))
        {
            return self.stop(
                "lua.multiple_results",
                last.pos,
                "several targets from one call's results",
            );
        }
        let mut values = Vec::new();
        for (i, x) in vals.iter().enumerate() {
            if targets.len() == 1 && vals.len() == 1 {
                self.sole = Some(std::ptr::from_ref(x) as usize);
            }
            let v = self.val(x)?;
            if i < targets.len() {
                values.push((v, x.pos));
            }
        }
        // With several targets, a value reading a target must see its old
        // value: hold every value in a temporary first.
        if targets.len() > 1 {
            self.flush(out);
            for (v, p) in &mut values {
                if matches!(v.e, Expr::Lit(_)) {
                    continue;
                }
                self.tmp += 1;
                let name = format!("tmp{}", self.tmp);
                let key = self.wkey(*p, &name);
                let l = self.new_local(&name, key, v.ty, v.n);
                self.set_local(l, v.clone(), *p, out)?;
                v.e = Expr::Local(l);
            }
        }
        for (i, t) in targets.iter().enumerate() {
            let (v, p) = values.get(i).cloned().unwrap_or((V::nil(), pos));
            self.assign_one(t, v, p, out)?;
        }
        Ok(())
    }

    fn assign_one(&mut self, t: &ast::Expr, v: V, pos: Pos, out: &mut Block) -> R<()> {
        match &t.k {
            ExprKind::Name(n) => match self.lookup(n) {
                Some(Var::Local(l)) => self.set_local(l, v, pos, out),
                Some(Var::Param(_)) => {
                    // `program` copies assigned parameters into locals.
                    self.stop("lua.assign", t.pos, format!("{n} can't be assigned here"))
                }
                Some(_) => self.stop(
                    "lua.assign",
                    t.pos,
                    format!("{n} holds a table or function: give the new value its own name"),
                ),
                None => self.stop(
                    "lua.global",
                    t.pos,
                    format!("{n} isn't a local: creating a global runs in the engine tier"),
                ),
            },
            ExprKind::Field(obj, field) => {
                let ExprKind::Name(n) = &obj.k else {
                    return self.stop("lua.table", t.pos, "assigning a field of this table");
                };
                let Some(Var::Record(r)) = self.lookup(n) else {
                    return self.stop(
                        "lua.table",
                        t.pos,
                        format!("{n}.{field} can't be assigned in the compiled tier"),
                    );
                };
                let found = r
                    .borrow()
                    .fields
                    .iter()
                    .find(|(f, _)| f == field)
                    .map(|x| x.1);
                match found {
                    Some(l) => self.set_local(l, v, pos, out),
                    None => {
                        // A new field: the next pass gives it a local from
                        // the record's start.
                        let key = r.borrow().key;
                        let mut fs = self.learnt.fields.get(&key).cloned().unwrap_or_default();
                        if !fs.contains(field) {
                            fs.push(field.clone());
                        }
                        self.learnt.fields.insert(key, fs);
                        self.again = true;
                        Err(Stop)
                    }
                }
            }
            _ => self.stop("lua.table", t.pos, "assigning a table element"),
        }
    }

    /// `for v = a, b, c do ... end`: the start, limit and step once, then
    /// a fresh `v` each time round.
    #[allow(clippy::too_many_arguments)]
    fn num_for(
        &mut self,
        var: &str,
        start: &ast::Expr,
        limit: &ast::Expr,
        step: Option<&ast::Expr>,
        body: &[Stat],
        pos: Pos,
        out: &mut Block,
    ) -> R<()> {
        let a = self.for_number(start, "initial")?;
        let b = self.for_number(limit, "limit")?;
        let c = match step {
            Some(x) => self.for_number(x, "step")?,
            None => V::lit(Literal::Int(1)),
        };
        let ty = if a.ty == Ty::Int && b.ty == Ty::Int && c.ty == Ty::Int {
            Ty::Int
        } else {
            Ty::Float
        };
        let lit = |v: &V| match &v.e {
            Expr::Lit(Literal::Int(i)) => Some(*i as f64),
            Expr::Lit(Literal::Float(x)) => Some(*x),
            _ => None,
        };
        let bound = match (lit(&a), lit(&b), lit(&c)) {
            (Some(x), Some(y), Some(s)) if s != 0.0 => {
                Bound::Static((((y - x) / s).floor() + 1.0).max(0.0) as u64)
            }
            _ => Bound::Guarded,
        };
        self.flush(out);
        self.tmp += 1;
        let n = self.tmp;
        let hold = |l: &mut Self, v: V, what: &str, out: &mut Block| -> R<Expr> {
            if matches!(v.e, Expr::Lit(_)) {
                return Ok(v.e);
            }
            let key = l.wkey(pos, &format!("for{n}_{what}"));
            let local = l.new_local(&format!("for{n}_{what}"), key, ty, Nul::Never);
            let e = l.coerce(v, ty, pos)?;
            out.push(Stmt::Assign(local, e));
            Ok(Expr::Local(local))
        };
        let counter = {
            let key = self.wkey(pos, &format!("for{n}_at"));
            let l = self.new_local(&format!("for{n}_at"), key, ty, Nul::Never);
            let e = self.coerce(a, ty, pos)?;
            out.push(Stmt::Assign(l, e));
            l
        };
        let lim = hold(self, b, "limit", out)?;
        let stp = hold(self, c.clone(), "step", out)?;
        let (lim, stp) = if ty == Ty::Float {
            let f = |e: Expr| match e {
                Expr::Lit(Literal::Int(i)) => Expr::Lit(Literal::Float(i as f64)),
                other => other,
            };
            (f(lim), f(stp))
        } else {
            (lim, stp)
        };
        let up = Expr::bin(BinOp::Le, Expr::Local(counter), lim.clone());
        let down = Expr::bin(BinOp::Ge, Expr::Local(counter), lim);
        let zero = if ty == Ty::Int {
            Expr::int(0)
        } else {
            Expr::Lit(Literal::Float(0.0))
        };
        let cond = match lit(&c) {
            Some(s) if s > 0.0 => up,
            Some(_) => down,
            None => Expr::bin(
                BinOp::Or,
                Expr::bin(
                    BinOp::And,
                    Expr::bin(BinOp::Gt, stp.clone(), zero.clone()),
                    up,
                ),
                Expr::bin(BinOp::And, Expr::bin(BinOp::Le, stp.clone(), zero), down),
            ),
        };
        self.loop_entry();
        // The variable is a fresh local each time round: the body may
        // change it without changing the count.
        let changes = ast::assigned_names(body).contains(var);
        let r = self.scoped(|l| {
            let mut head = Vec::new();
            if changes {
                let key = l.wkey(pos, var);
                let v = l.new_local(var, key, ty, Nul::Never);
                head.push(Stmt::Assign(v, Expr::Local(counter)));
                l.facts.insert(Key::L(v));
                l.bind(var, Var::Local(v));
            } else {
                l.facts.insert(Key::L(counter));
                l.bind(var, Var::Local(counter));
            }
            l.jframes.push(JFrame::Loop);
            let b = l.block(body);
            l.jframes.pop();
            head.extend(b);
            head
        });
        self.loop_entry();
        let mut full = r;
        if !exits(&full) {
            full.push(Stmt::Assign(
                counter,
                Expr::bin(BinOp::Add, Expr::Local(counter), stp),
            ));
        }
        out.push(Stmt::While {
            cond,
            body: full,
            bound,
        });
        Ok(())
    }

    /// A numeric `for`'s start, limit or step: a non-null number, else
    /// Lua's error.
    fn for_number(&mut self, x: &ast::Expr, what: &str) -> R<V> {
        let v = self.val(x)?;
        match v.ty {
            Ty::Int | Ty::Float => {}
            Ty::Str => {
                return self.stop(
                    "lua.dynamic_types",
                    x.pos,
                    "a for loop over text converts it to a number at run time",
                );
            }
            _ => {
                return self.stop(
                    "lua.for",
                    x.pos,
                    format!("the for loop's {what} value isn't a number"),
                );
            }
        }
        self.non_null(v, x.pos, &format!("'for' {what} value must be a number"))
    }

    /// `for i, r in ipairs(rows) do ... end` over `db.sql` rows.
    fn gen_for(
        &mut self,
        names: &[String],
        exprs: &[ast::Expr],
        body: &[Stat],
        pos: Pos,
        out: &mut Block,
    ) -> R<()> {
        let [it] = exprs else {
            return self.stop("lua.for", pos, "for ... in with several expressions");
        };
        let ExprKind::Call(f, args) = &it.k else {
            return self.stop("lua.for", pos, "for ... in ipairs(rows)");
        };
        let fname = match &f.k {
            ExprKind::Name(n) if self.lookup(n).is_none() => n.as_str(),
            _ => "",
        };
        if fname == "pairs" {
            return self.stop(
                "lua.pairs",
                pos,
                "pairs visits a table in no defined order: it runs in the engine tier",
            );
        }
        if fname != "ipairs" || args.len() != 1 || names.len() > 2 {
            return self.stop("lua.for", pos, "for i, r in ipairs(rows)");
        }
        // `ipairs` of a decoded array: each item is a decoded value.
        let decoded = self.dval(&args[0])?;
        let (src, cols) = match &args[0].k {
            _ if decoded.is_some() => (
                decoded.clone().unwrap_or(Expr::Lit(Literal::Null)),
                Vec::new(),
            ),
            ExprKind::Name(n) => match self.lookup(n) {
                Some(Var::Rows { local, cols }) => (Expr::Local(local), cols),
                _ => return self.stop("lua.for", pos, "ipairs over rows from db.sql"),
            },
            _ => {
                let mut first = Vec::new();
                match self.db_call(&args[0], "rows", &mut first)? {
                    Some(Var::Rows { local, cols }) => {
                        out.extend(first);
                        (Expr::Local(local), cols)
                    }
                    _ => return self.stop("lua.for", pos, "ipairs over rows from db.sql"),
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
        if decoded.is_some() {
            // `ipairs` of anything but an array (an object's table, a
            // string, nil): the engine tier.
            out.push(Stmt::If(
                not(Expr::call(
                    crate::ir::Builtin::JsonKind,
                    vec![Expr::Local(arr), str_lit("array")],
                )),
                vec![Stmt::Throw(rerun("ipairs of a value that isn't an array"))],
                Vec::new(),
            ));
        }
        out.push(Stmt::Assign(
            len,
            Expr::bin(
                BinOp::Coalesce,
                Expr::call(crate::ir::Builtin::JsonLength, vec![Expr::Local(arr)]),
                Expr::int(0),
            ),
        ));
        out.push(Stmt::Assign(cur, Expr::int(0)));
        let each_name = names.get(1).cloned().unwrap_or_else(|| "row".to_owned());
        let each = self.f.local(&each_name, Ty::Json);
        self.loop_entry();
        let index_name = names[0].clone();
        // The index is 1-based: the counter after it moves.
        let index_changes = ast::assigned_names(body).contains(&index_name);
        let body = self.scoped(|l| {
            let mut head = Vec::new();
            if index_changes {
                let key = l.wkey(pos, &index_name);
                let v = l.new_local(&index_name, key, Ty::Int, Nul::Never);
                head.push(Stmt::Assign(v, Expr::Local(cur)));
                l.facts.insert(Key::L(v));
                l.bind(&index_name, Var::Local(v));
            } else {
                l.facts.insert(Key::L(cur));
                l.bind(&index_name, Var::Local(cur));
            }
            if names.len() == 2 {
                l.bind(
                    &each_name,
                    if decoded.is_some() {
                        Var::Dyn(each)
                    } else {
                        Var::Elem {
                            local: each,
                            cols: cols.clone(),
                        }
                    },
                );
            }
            l.jframes.push(JFrame::Loop);
            let b = l.block(body);
            l.jframes.pop();
            head.extend(b);
            head
        });
        self.loop_entry();
        let item = if decoded.is_some() {
            Expr::call(
                crate::ir::Builtin::JsonGet,
                vec![Expr::Local(arr), Expr::Local(cur), Expr::bool(false)],
            )
        } else {
            Expr::Path(
                Box::new(Expr::Local(arr)),
                vec![crate::ir::PathSeg::Index(Box::new(Expr::Local(cur)))],
            )
        };
        let mut full = vec![
            Stmt::Assign(each, item),
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

    /// `return` from the function or from an inlined helper.
    fn ret(&mut self, vals: &[ast::Expr], pos: Pos, out: &mut Block) -> R<()> {
        self.returning = true;
        let r = self.ret_inner(vals, pos, out);
        self.returning = false;
        r
    }

    fn ret_inner(&mut self, vals: &[ast::Expr], pos: Pos, out: &mut Block) -> R<()> {
        if let Some(i) = self
            .jframes
            .iter()
            .rposition(|f| matches!(f, JFrame::Helper { .. }))
        {
            return self.helper_return(i, vals, pos, out);
        }
        if vals.len() > 1 {
            return self.stop(
                "lua.return",
                pos,
                "only the first value is the result: return one value",
            );
        }
        let scalar = self.sig.returns;
        let Some(x) = vals.first() else {
            let fields = if scalar.is_some() {
                vec![("value".to_owned(), Expr::Lit(Literal::Null))]
            } else {
                Vec::new()
            };
            self.finish_return("output".to_owned(), fields, out);
            return Ok(());
        };
        // `return sprig.event(name, {fields})`.
        if let ExprKind::Call(callee, args) = &x.k
            && self.is_lib(callee, &["sprig", "event"])
        {
            let name = match args.first().map(|a| &a.k) {
                Some(ExprKind::Str(s)) => s.clone(),
                _ => return self.stop("lua.event", x.pos, "the event's name must be a literal"),
            };
            let fields = match args.get(1) {
                None => Vec::new(),
                Some(a) => self.fields_of(a)?,
            };
            return self.emit(&name, fields, pos, out);
        }
        if let Some(ty) = scalar {
            self.sole = Some(std::ptr::from_ref(x) as usize);
            let v = self.val(x)?;
            let e = self.coerce(v, ty, x.pos)?;
            self.finish_return("output".to_owned(), vec![("value".to_owned(), e)], out);
            return Ok(());
        }
        let is_table = match &x.k {
            ExprKind::Table(fs) => Self::is_record(fs),
            ExprKind::Name(n) => matches!(self.lookup(n), Some(Var::Record(_))),
            _ => false,
        };
        if is_table {
            let fields = self.fields_of(x)?;
            return self.emit("output", fields, pos, out);
        }
        let v = self.val(x)?;
        match v.ty {
            Ty::Json => self.stop(
                "lua.return",
                x.pos,
                "a JSON value's members become fields only at run time",
            ),
            // `return nil` and `return sprig.null` are `output {}`.
            Ty::Null => self.emit("output", Vec::new(), pos, out),
            _ if v.n != Nul::Never => self.stop(
                "lua.return",
                x.pos,
                "a value that may be nil or sprig.null gives output {} when it is: test it first",
            ),
            _ => self.emit("output", vec![("value".to_owned(), v)], pos, out),
        }
    }

    /// The fields of a returned record or table constructor, in order.
    fn fields_of(&mut self, x: &ast::Expr) -> R<Vec<(String, V)>> {
        match &x.k {
            ExprKind::Table(fs) if Self::is_record(fs) => {
                let mut out: Vec<(String, V)> = Vec::new();
                for f in fs {
                    let (n, e) = match f {
                        ast::TableField::Named(n, e) => (n.clone(), e),
                        ast::TableField::Keyed(k, e) => match &k.k {
                            ExprKind::Str(s) => (s.clone(), e),
                            _ => return self.stop("lua.table", k.pos, "a table key"),
                        },
                        ast::TableField::Item(e) => {
                            return self.stop("lua.table", e.pos, "array items");
                        }
                    };
                    let v = self.table_item(e)?;
                    out.retain(|(m, _)| *m != n);
                    out.push((n, v));
                }
                Ok(out)
            }
            ExprKind::Name(n) => match self.lookup(n) {
                Some(Var::Record(r)) => {
                    let fs = r.borrow().fields.clone();
                    Ok(fs
                        .into_iter()
                        .map(|(n, l)| {
                            let v = self.local_val(l);
                            (n, v)
                        })
                        .collect())
                }
                _ => self.stop("lua.event", x.pos, "the fields must be a table"),
            },
            _ => self.stop("lua.event", x.pos, "the fields must be a table"),
        }
    }

    /// An event's fields: a field that is `nil` is left out (Lua drops
    /// it); one that may be `nil` is a reason.
    fn emit(&mut self, name: &str, fields: Vec<(String, V)>, pos: Pos, out: &mut Block) -> R<()> {
        let idx = match self.events.iter().position(|(n, _)| n == name) {
            Some(i) => i,
            None => {
                self.events.push((name.to_owned(), Vec::new()));
                self.events.len() - 1
            }
        };
        let mut exprs = Vec::new();
        for (k, v) in fields {
            if v.ty == Ty::Null && v.n == Nul::Nil {
                continue;
            }
            if matches!(v.n, Nul::Nil | Nul::Both) {
                return self.stop(
                    "lua.nil_field",
                    pos,
                    format!(
                        "field {k} may be nil, and Lua leaves a nil field out: give it a value \
                         (or sprig.null) on every path"
                    ),
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
                                "lua.field_type",
                                pos,
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
                    Expr::call(crate::ir::Builtin::ToFloat, vec![v.e])
                } else {
                    v.e
                };
                (k, e)
            })
            .collect();
        self.finish_return(name.to_owned(), fields, out);
        Ok(())
    }

    fn finish_return(&mut self, name: String, fields: Vec<(String, Expr)>, out: &mut Block) {
        self.flush(out);
        out.push(Stmt::Emit(name, fields));
        out.push(Stmt::Return);
    }

    /// A helper's `return`: its value into the helper's result local,
    /// then leave the helper's block.
    fn helper_return(
        &mut self,
        frame: usize,
        vals: &[ast::Expr],
        pos: Pos,
        out: &mut Block,
    ) -> R<()> {
        let JFrame::Helper { label, result, .. } = self.jframes[frame].clone() else {
            return Err(Stop);
        };
        if vals.len() > 1 {
            return self.stop(
                "lua.multiple_results",
                pos,
                "a helper returning several values",
            );
        }
        let v = match vals.first() {
            None => None,
            Some(x) => {
                if let ExprKind::Call(callee, _) = &x.k
                    && self.is_lib(callee, &["sprig", "event"])
                {
                    return self.stop(
                        "lua.helper",
                        x.pos,
                        "a helper returning sprig.event(...): return the event from the function",
                    );
                }
                self.sole = Some(std::ptr::from_ref(x) as usize);
                let v = self.val(x)?;
                Some(v)
            }
        };
        let Some(v) = v else {
            if let JFrame::Helper { all_values, .. } = &mut self.jframes[frame] {
                *all_values = false;
            }
            self.flush(out);
            out.push(Stmt::Leave(label));
            return Ok(());
        };
        let key = (self.salt, "#result".to_owned());
        let l = match result {
            Some(l) => l,
            None => {
                let l = self.new_local(&format!("h{label}_result"), key, v.ty, v.n);
                if let JFrame::Helper { result, .. } = &mut self.jframes[frame] {
                    *result = Some(l);
                }
                l
            }
        };
        if let JFrame::Helper { kind, .. } = &mut self.jframes[frame] {
            *kind = kind.join(v.n);
        }
        self.set_local(l, v, pos, out)?;
        out.push(Stmt::Leave(label));
        Ok(())
    }
}

/// A small, stable hash of a name (record keys).
fn crc(s: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in s.bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h & 0x00ff_ffff
}

/// The error a guard raises: Lua's runtime error, 39865, with the
/// position Lua adds; a `pcall` sees a message that may differ between
/// the tiers, so it isn't readable there.
fn lua_error(name: &str, pos: Pos, what: &str) -> ErrorSpec {
    let m = str_lit(&format!("{name}:{}: {what}", pos.line));
    ErrorSpec {
        code: 39865,
        message: m.clone(),
        catch: Some((String::new(), m)),
    }
}

/// The error that hands a record to the engine tier (39876): its decoded
/// JSON is something compiled code can't read as Lua would. No `pcall`
/// catches it.
fn rerun(why: &str) -> ErrorSpec {
    ErrorSpec::new(
        crate::ir::ENGINE_RERUN,
        str_lit(&format!("{}: {why}", crate::ir::ENGINE_RERUN_TEXT)),
    )
}

#[cfg(test)]
mod tests;
