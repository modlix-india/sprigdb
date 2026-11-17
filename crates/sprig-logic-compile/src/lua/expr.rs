//! The Lua front end's expressions: values and their null kinds,
//! conditions, operators, calls, helpers and data access.

use std::collections::BTreeSet;
use std::rc::Rc;

use crate::ir::{
    BinOp, Builtin, DbOp, ErrorSpec, Expr, Literal, LocalId, PathSeg, SqlShape, Stmt, Ty, UnOp,
};
use crate::sql::{self, Want};

use super::ast::{self, BinOp as LBin, ExprKind, Func, Pos, StatKind, UnOp as LUn};
use super::{Block, JFrame, Key, L, Nul, R, Stop, V, Var, exits, is_null, lua_error, not, str_lit};

/// A Lua number literal: whole numbers within ±2^53 are `Int`.
fn number(x: f64) -> Literal {
    if x.fract() == 0.0 && x.abs() < 9_007_199_254_740_992.0 {
        Literal::Int(x as i64)
    } else {
        Literal::Float(x)
    }
}

fn widen_pair(a: V, b: V) -> (Expr, Expr, Ty) {
    if a.ty == Ty::Int && b.ty == Ty::Int {
        (a.e, b.e, Ty::Int)
    } else {
        (to_float(a), to_float(b), Ty::Float)
    }
}

fn to_float(v: V) -> Expr {
    match v.e {
        Expr::Lit(Literal::Int(i)) => Expr::Lit(Literal::Float(i as f64)),
        e if v.ty == Ty::Int => Expr::call(Builtin::ToFloat, vec![e]),
        e => e,
    }
}

impl L<'_, '_> {
    // -- conditions -----------------------------------------------------------

    /// A condition, as `Bool` with Lua's truthiness.
    pub(super) fn test(&mut self, x: &ast::Expr) -> R<Expr> {
        match &x.k {
            ExprKind::Paren(e) => self.test(e),
            // Only the truth of `a and b`, `a or b` matters here.
            ExprKind::Bin(op @ (LBin::And | LBin::Or), a, b) => {
                let and = *op == LBin::And;
                let ta = self.test(a)?;
                let facts = self.facts_if(a, and);
                let saved = self.facts.clone();
                self.facts.extend(facts);
                self.conds
                    .push(if and { ta.clone() } else { not(ta.clone()) });
                let tb = self.test(b);
                self.conds.pop();
                self.facts = saved;
                let tb = tb?;
                Ok(if and {
                    Expr::Cond(Box::new(ta), Box::new(tb), Box::new(Expr::bool(false)))
                } else {
                    Expr::Cond(Box::new(ta), Box::new(Expr::bool(true)), Box::new(tb))
                })
            }
            ExprKind::Un(LUn::Not, e) => Ok(not(self.test(e)?)),
            // A row table and sprig.null are both true.
            _ if self.row_of(x).is_some() => Ok(Expr::bool(true)),
            _ => {
                if let Some(d) = self.dval(x)? {
                    return Ok(Self::dtruth(&d));
                }
                let v = self.val(x)?;
                self.truth(&v, x.pos)
            }
        }
    }

    /// Whether the row `x` names is there, when `x` names a `db.sqlOne`
    /// result (a row table, or `sprig.null` when there was none) or a
    /// validation's or trigger's row (`ctx.old` is `sprig.null` for an
    /// insert; `ctx.row` is always there).
    pub(super) fn row_of(&self, x: &ast::Expr) -> Option<Expr> {
        match self.ctx_path(x) {
            Some(Var::Image(true)) => return self.old_flag().map(Expr::Local),
            Some(Var::Image(false)) => return Some(Expr::bool(true)),
            _ => {}
        }
        match &x.k {
            ExprKind::Paren(e) => self.row_of(e),
            ExprKind::Name(n) => match self.lookup(n) {
                Some(Var::Row { found, .. }) => Some(Expr::Local(found)),
                _ => None,
            },
            _ => None,
        }
    }

    /// `p == sprig.null`, `p ~= nil` and the like of a `db.sqlOne`
    /// result: `None` when `a` and `b` aren't a row and a null.
    fn row_equals(&self, a: &ast::Expr, b: &ast::Expr) -> Option<Expr> {
        let (found, other) = match (self.row_of(a), self.row_of(b)) {
            (Some(f), None) => (f, b),
            (None, Some(f)) => (f, a),
            _ => return None,
        };
        if self.is_snull(other) {
            Some(not(found))
        } else if matches!(other.k, ExprKind::Nil) {
            // A row is a table or sprig.null, never nil.
            Some(Expr::bool(false))
        } else {
            None
        }
    }

    /// Lua's truth of a value: `nil` and `false` are false; `sprig.null`
    /// and everything else is true.
    pub(super) fn truth(&mut self, v: &V, pos: Pos) -> R<Expr> {
        if v.n == Nul::Both {
            return self.stop(
                "lua.null_kinds",
                pos,
                "a value that may be nil or sprig.null: its truth depends on which",
            );
        }
        Ok(match v.ty {
            Ty::Null => Expr::bool(v.n == Nul::SNull),
            Ty::Bool if v.n == Nul::SNull => {
                not(Expr::bin(BinOp::Eq, v.e.clone(), Expr::bool(false)))
            }
            Ty::Bool => Expr::call(Builtin::Present, vec![v.e.clone()]),
            _ if v.n == Nul::Nil => not(is_null(v.e.clone())),
            _ => Expr::bool(true),
        })
    }

    /// The key a name's non-null fact is about, when `x` is a name or a
    /// record's field whose null is `kind`.
    fn fact_key(&self, x: &ast::Expr, kind: Nul) -> Option<Key> {
        // `ctx.old ~= sprig.null`, `ctx.row.qty ~= sprig.null`.
        if kind == Nul::SNull {
            if let Some(Var::Image(true)) = self.ctx_path(x) {
                return self.old_flag().map(Key::L);
            }
            if let ExprKind::Field(o, f) = &x.k
                && let Some(Var::Image(old)) = self.ctx_path(o)
            {
                let name = if old {
                    format!("old_{}", f.to_ascii_lowercase())
                } else {
                    f.to_ascii_lowercase()
                };
                let i = self.f.params.iter().position(|p| p.name == name)?;
                return Some(Key::P(u16::try_from(i).ok()?));
            }
        }
        match &x.k {
            ExprKind::Paren(e) => self.fact_key(e, kind),
            ExprKind::Name(n) => match self.lookup(n)? {
                Var::Param(i) => (kind == Nul::SNull).then_some(Key::P(i)),
                Var::Local(l) => (self.kinds.get(&l) == Some(&kind)).then_some(Key::L(l)),
                Var::Row { found, .. } => (kind == Nul::SNull).then_some(Key::L(found)),
                _ => None,
            },
            ExprKind::Field(obj, f) => match &obj.k {
                ExprKind::Name(n) => match self.lookup(n)? {
                    Var::Record(r) => {
                        let l = r.borrow().fields.iter().find(|(g, _)| g == f)?.1;
                        (self.kinds.get(&l) == Some(&kind)).then_some(Key::L(l))
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    /// Facts that hold when `x` is `truth`.
    pub(super) fn facts_if(&self, x: &ast::Expr, truth: bool) -> BTreeSet<Key> {
        let mut s = BTreeSet::new();
        match &x.k {
            ExprKind::Paren(e) => return self.facts_if(e, truth),
            ExprKind::Un(LUn::Not, e) => return self.facts_if(e, !truth),
            ExprKind::Bin(op @ (LBin::And | LBin::Or), a, b) => {
                if (*op == LBin::And) == truth {
                    s.extend(self.facts_if(a, truth));
                    s.extend(self.facts_if(b, truth));
                }
            }
            ExprKind::Bin(op @ (LBin::Eq | LBin::Ne), a, b) => {
                let which = |e: &ast::Expr| match &e.k {
                    ExprKind::Nil => Some(Nul::Nil),
                    _ if self.is_snull(e) => Some(Nul::SNull),
                    _ => None,
                };
                let (other, kind) = match (which(a), which(b)) {
                    (None, Some(k)) => (a, k),
                    (Some(k), None) => (b, k),
                    _ => return s,
                };
                // Not null when `~=` is true or `==` is false.
                if (*op == LBin::Ne) == truth
                    && let Some(k) = self.fact_key(other, kind)
                {
                    s.insert(k);
                }
            }
            _ if truth => {
                if let Some(k) = self.fact_key(x, Nul::Nil) {
                    s.insert(k);
                }
            }
            _ => {}
        }
        s
    }

    /// Whether `e` is `sprig.null` (or `cjson.null`).
    fn is_snull(&self, e: &ast::Expr) -> bool {
        matches!(&e.k, ExprKind::Field(o, f) if f == "null"
            && matches!(&o.k, ExprKind::Name(n) if matches!(n.as_str(), "sprig" | "cjson")
                && self.lookup(n).is_none()))
    }

    /// Lowers `x` with `facts` added, as an operand that runs only when
    /// `when` is true.
    fn cond_val(&mut self, x: &ast::Expr, facts: BTreeSet<Key>, when: Expr) -> R<V> {
        let saved = self.facts.clone();
        self.facts.extend(facts);
        self.conds.push(when);
        let r = self.val(x);
        self.conds.pop();
        self.facts = saved;
        r
    }

    /// Statements the expression being lowered needs first, under the
    /// conditions of the operand it is in.
    pub(super) fn needs(&mut self, stmts: Block) {
        if stmts.is_empty() {
            return;
        }
        match self
            .conds
            .iter()
            .cloned()
            .reduce(|a, b| Expr::bin(BinOp::And, a, b))
        {
            None => self.pre.extend(stmts),
            Some(c) => self.pre.push(Stmt::If(c, stmts, Vec::new())),
        }
    }

    /// Raises `spec` before the statement when `check` is true.
    pub(super) fn guard(&mut self, check: Expr, spec: ErrorSpec) {
        self.needs(vec![Stmt::If(check, vec![Stmt::Throw(spec)], Vec::new())]);
    }

    /// `v`, checked non-null before the statement: Lua's error (39865)
    /// when it is `nil` or `sprig.null`.
    pub(super) fn non_null(&mut self, v: V, pos: Pos, what: &str) -> R<V> {
        if v.n == Nul::Never {
            return Ok(v);
        }
        if v.ty == Ty::Null {
            return self.stop(
                "lua.nil_value",
                pos,
                format!("{what}: the value is always nil"),
            );
        }
        let spec = lua_error(&self.f.name, pos, &format!("attempt to {what}"));
        self.guard(is_null(v.e.clone()), spec);
        // After the check, a name read again is known non-null.
        match &v.e {
            Expr::Param(i) => {
                self.facts.insert(Key::P(*i));
            }
            Expr::Local(l) => {
                self.facts.insert(Key::L(*l));
            }
            _ => {}
        }
        Ok(V { n: Nul::Never, ..v })
    }

    // -- values ---------------------------------------------------------------

    #[allow(clippy::too_many_lines)]
    pub(super) fn val(&mut self, x: &ast::Expr) -> R<V> {
        match &x.k {
            ExprKind::Nil => Ok(V::nil()),
            ExprKind::True => Ok(V::lit(Literal::Bool(true))),
            ExprKind::False => Ok(V::lit(Literal::Bool(false))),
            ExprKind::Num(n) => Ok(V::lit(number(*n))),
            ExprKind::Str(s) => Ok(V::lit(Literal::Str(s.clone()))),
            ExprKind::Vararg => self.stop("lua.vararg", x.pos, "... runs in the engine tier"),
            ExprKind::Function(_) => self.stop(
                "lua.function_value",
                x.pos,
                "a function as a value runs in the engine tier",
            ),
            ExprKind::Paren(e) => self.val(e),
            ExprKind::Name(n) => self.name_val(n, x.pos),
            ExprKind::Field(obj, f) => self.field(obj, f, x.pos),
            ExprKind::Index(..) => self.stop(
                "lua.index",
                x.pos,
                "t[k] runs in the engine tier (read fields by name)",
            ),
            ExprKind::Call(..) | ExprKind::Method(..) => self.call(x),
            ExprKind::Table(fields) => self.table_value(fields, x.pos),
            ExprKind::Un(op, e) => match op {
                LUn::Not => {
                    let t = self.test(e)?;
                    Ok(V {
                        e: not(t),
                        ty: Ty::Bool,
                        n: Nul::Never,
                    })
                }
                LUn::Neg => {
                    if let ExprKind::Num(n) = &e.k {
                        return Ok(V::lit(number(-n)));
                    }
                    let v = self.val(e)?;
                    let v = self.arith_operand(v, e.pos)?;
                    Ok(V {
                        ty: v.ty,
                        e: Expr::Unary(UnOp::Neg, Box::new(v.e)),
                        n: Nul::Never,
                    })
                }
                LUn::Len => self.length(e),
            },
            ExprKind::Bin(op, a, b) => self.binary(*op, a, b, x.pos),
        }
    }

    fn name_val(&mut self, n: &str, pos: Pos) -> R<V> {
        match self.lookup(n) {
            Some(Var::Param(i)) => Ok(V {
                e: Expr::Param(i),
                ty: self.f.params[i as usize].ty,
                n: if self.facts.contains(&Key::P(i)) {
                    Nul::Never
                } else {
                    Nul::SNull
                },
            })
            .and_then(|v| {
                if v.ty == Ty::Json {
                    self.stop("lua.json", pos, format!("{n} is a JSON value: a Lua table"))
                } else {
                    Ok(v)
                }
            }),
            Some(Var::Local(l)) => Ok(self.local_val(l)),
            Some(Var::Val(v)) => Ok(v),
            Some(Var::Rows { local, .. }) => Ok(Self::rows_value(local)),
            Some(Var::Record(r)) => {
                let fs = r.borrow().fields.clone();
                let mut m = Vec::new();
                for (f, l) in fs {
                    let v = self.local_val(l);
                    if v.ty == Ty::Null && v.n == Nul::Nil {
                        continue;
                    }
                    if matches!(v.n, Nul::Nil | Nul::Both) {
                        return self.stop(
                            "lua.nil_field",
                            pos,
                            format!("field {f} of {n} may be nil, and Lua leaves a nil field out"),
                        );
                    }
                    m.push((f, v.e));
                }
                Ok(V {
                    e: Expr::ObjectLit(m),
                    ty: Ty::Json,
                    n: Nul::Never,
                })
            }
            Some(Var::Ctx | Var::Trigger | Var::Image(_) | Var::Msg | Var::Event) => self.stop(
                "lua.value",
                pos,
                format!("{n} is a table the engine tier makes: read its fields by name"),
            ),
            Some(Var::Dyn(_)) => self.stop(
                "lua.json",
                pos,
                format!(
                    "{n} holds a decoded JSON value: compiled code reads it where it is used \
                     (a number in arithmetic, text with .., a test, a bind argument)"
                ),
            ),
            Some(Var::Unreadable(why)) => {
                self.stop("lua.pcall_value", pos, format!("{n} is {why}"))
            }
            Some(Var::Helper { .. }) => self.stop(
                "lua.function_value",
                pos,
                format!("{n} is a function: only calls of it compile"),
            ),
            Some(v @ (Var::Exec { .. } | Var::Call { .. })) => Ok(Self::result_value(&v)),
            Some(Var::Row { .. } | Var::Elem { .. }) => self.stop(
                "lua.value",
                pos,
                format!("{n} can't be used as a value in the compiled tier"),
            ),
            None => self.stop("lua.unknown_name", pos, format!("unknown name {n}")),
        }
    }

    /// What `e` stands for when it is a name, or a data call (which then
    /// runs before the statement, under the conditions of the operand it
    /// is in).
    pub(super) fn var_of(&mut self, e: &ast::Expr) -> R<Option<Var>> {
        match &e.k {
            ExprKind::Paren(x) => self.var_of(x),
            ExprKind::Name(n) => Ok(self.lookup(n)),
            ExprKind::Call(..) => {
                let mut first = Vec::new();
                let v = self.db_call(e, "", &mut first)?;
                self.needs(first);
                Ok(v)
            }
            _ => Ok(None),
        }
    }

    /// A `db.exec`, `db.publish` or `db.call` result as a value:
    /// `{affected}`, `{event, fields}`.
    fn result_value(v: &Var) -> V {
        let e = match v {
            Var::Exec { affected } => {
                Expr::ObjectLit(vec![("affected".to_owned(), Expr::Local(*affected))])
            }
            Var::Call { event, cols } => Expr::ObjectLit(vec![
                ("event".to_owned(), Expr::Local(*event)),
                ("fields".to_owned(), Self::fields_value(cols).e),
            ]),
            _ => Expr::Lit(Literal::Null),
        };
        V {
            e,
            ty: Ty::Json,
            n: Nul::Never,
        }
    }

    /// A `db.call` result's `fields`: every field the callee returns, in
    /// the wrapper's order (`NULL` ones as JSON `null`).
    fn fields_value(cols: &[(String, LocalId)]) -> V {
        V {
            e: Expr::ObjectLit(
                cols.iter()
                    .map(|(n, l)| (n.clone(), Expr::Local(*l)))
                    .collect(),
            ),
            ty: Ty::Json,
            n: Nul::Never,
        }
    }

    /// Rows as a value (the engine tier keeps an empty array as `[]`).
    fn rows_value(local: LocalId) -> V {
        V {
            e: Expr::Local(local),
            ty: Ty::Json,
            n: Nul::Never,
        }
    }

    /// A value inside a table constructor.
    pub(super) fn table_item(&mut self, x: &ast::Expr) -> R<V> {
        // At a `return` nothing can change the table any more, so a copy
        // of it is exact.
        if let ExprKind::Name(n) = &x.k
            && let Some(Var::Record(_)) = self.lookup(n)
            && !self.returning
        {
            return self.stop(
                "lua.table",
                x.pos,
                format!("the table {n} inside another table: build it in place"),
            );
        }
        self.val(x)
    }

    /// A table constructor as a value: an object (named fields) or an
    /// array (items).
    fn table_value(&mut self, fields: &[ast::TableField], pos: Pos) -> R<V> {
        if Self::is_record(fields) {
            let fs = self.fields_of(&ast::Expr {
                pos,
                k: ExprKind::Table(fields.to_vec()),
            })?;
            let mut m = Vec::new();
            for (k, v) in fs {
                if v.ty == Ty::Null && v.n == Nul::Nil {
                    continue;
                }
                if matches!(v.n, Nul::Nil | Nul::Both) {
                    return self.stop(
                        "lua.nil_field",
                        pos,
                        format!("field {k} may be nil, and Lua leaves a nil field out"),
                    );
                }
                m.push((k, v.e));
            }
            return Ok(V {
                e: Expr::ObjectLit(m),
                ty: Ty::Json,
                n: Nul::Never,
            });
        }
        let mut items = Vec::new();
        for f in fields {
            let ast::TableField::Item(x) = f else {
                return self.stop("lua.table", pos, "a table with both names and items");
            };
            if matches!(
                x.k,
                ExprKind::Call(..) | ExprKind::Method(..) | ExprKind::Vararg
            ) && std::ptr::eq(f, fields.last().unwrap_or(f))
            {
                return self.stop(
                    "lua.multiple_results",
                    x.pos,
                    "a call as a table's last item adds all its results",
                );
            }
            let v = self.table_item(x)?;
            if v.n != Nul::Never && v.n != Nul::SNull {
                return self.stop(
                    "lua.table",
                    x.pos,
                    "an array item that may be nil (Lua's length is then undefined)",
                );
            }
            items.push(v.e);
        }
        Ok(V {
            e: Expr::ArrayLit(items),
            ty: Ty::Json,
            n: Nul::Never,
        })
    }

    /// `obj.f`.
    fn field(&mut self, obj: &ast::Expr, f: &str, pos: Pos) -> R<V> {
        if self.is_snull(&ast::Expr {
            pos,
            k: ExprKind::Field(Box::new(obj.clone()), f.to_owned()),
        }) {
            return Ok(V::snull());
        }
        if let Some(base) = self.ctx_path(obj) {
            return self.ctx_field(&base, f, pos);
        }
        // `r.fields.x` of a `db.call` result.
        if let ExprKind::Field(inner, g) = &obj.k
            && g == "fields"
            && let Some(Var::Call { cols, .. }) = self.var_of(inner)?
        {
            // A field the callee doesn't return is nil; a NULL one is
            // sprig.null.
            return Ok(match cols.iter().find(|(c, _)| c == f) {
                Some((_, l)) => {
                    let mut v = self.local_val(*l);
                    v.n = Nul::SNull;
                    v
                }
                None => V::nil(),
            });
        }
        let n = match &obj.k {
            ExprKind::Name(n) => n.clone(),
            _ => "the result".to_owned(),
        };
        let var = match &obj.k {
            ExprKind::Name(n) => self.lookup(n),
            _ => match self.var_of(obj)? {
                Some(v) => Some(v),
                None => return self.stop("lua.member", pos, format!("reading .{f} here")),
            },
        };
        match var {
            Some(Var::Call { event, cols }) => match f {
                "event" => Ok(V {
                    e: Expr::Local(event),
                    ty: Ty::Str,
                    n: Nul::Never,
                }),
                "fields" => Ok(Self::fields_value(&cols)),
                _ => Ok(V::nil()),
            },
            Some(Var::Row { found, cols }) => {
                let Some((_, l)) = cols.iter().find(|(c, _)| c == f).cloned() else {
                    // A missing column of a row is nil.
                    return Ok(V::nil());
                };
                if !self.facts.contains(&Key::L(found)) {
                    let spec = lua_error(&self.f.name, pos, "attempt to index a userdata value");
                    self.guard(not(Expr::Local(found)), spec);
                    self.facts.insert(Key::L(found));
                }
                let mut v = self.local_val(l);
                if v.n != Nul::Never {
                    v.n = Nul::SNull;
                }
                Ok(v)
            }
            Some(Var::Exec { affected }) => {
                if f == "affected" {
                    Ok(V {
                        e: Expr::Local(affected),
                        ty: Ty::Int,
                        n: Nul::Never,
                    })
                } else {
                    Ok(V::nil())
                }
            }
            Some(Var::Elem { local, cols }) => {
                let Some((_, ty)) = cols.iter().find(|(c, _)| c == f).cloned() else {
                    return Ok(V::nil());
                };
                let path = Expr::Path(
                    Box::new(Expr::Local(local)),
                    vec![PathSeg::Key(f.to_owned())],
                );
                let (e, ty) = match ty {
                    Ty::Int | Ty::Float => (Expr::call(Builtin::JsonNum, vec![path]), Ty::Float),
                    Ty::Str => (Expr::call(Builtin::JsonStr, vec![path]), Ty::Str),
                    _ => {
                        return self.stop(
                            "lua.json",
                            pos,
                            format!("column {f} is JSON: a Lua table"),
                        );
                    }
                };
                Ok(V {
                    e,
                    ty,
                    n: Nul::SNull,
                })
            }
            Some(Var::Record(r)) => {
                let l = r.borrow().fields.iter().find(|(g, _)| g == f).map(|x| x.1);
                match l {
                    Some(l) => Ok(self.local_val(l)),
                    None => Ok(V::nil()),
                }
            }
            None if n == "math" && matches!(obj.k, ExprKind::Name(_)) => match f {
                "pi" => Ok(V::lit(Literal::Float(std::f64::consts::PI))),
                "huge" => self.stop(
                    "lua.math",
                    pos,
                    "math.huge is infinite: MariaDB has no such value",
                ),
                _ => self.stop("lua.member", pos, format!("math.{f} as a value")),
            },
            _ => self.stop("lua.member", pos, format!("reading {n}.{f} here")),
        }
    }

    /// `#x`: the byte length of text, the number of rows.
    fn length(&mut self, e: &ast::Expr) -> R<V> {
        if let Some(d) = self.dval(e)? {
            // Text's bytes, an array's items; a table made from an object,
            // and anything else, in the engine tier.
            return Ok(V {
                e: Expr::Cond(
                    Box::new(Self::dkind(&d, "string")),
                    Box::new(Expr::call(
                        Builtin::ByteLen,
                        vec![Expr::call(
                            Builtin::JsonAsStr,
                            vec![d.clone(), Expr::bool(false)],
                        )],
                    )),
                    Box::new(Expr::call(Builtin::JsonLen, vec![d])),
                ),
                ty: Ty::Int,
                n: Nul::Never,
            });
        }
        let rows = match &e.k {
            ExprKind::Name(_) | ExprKind::Call(..) | ExprKind::Paren(_) => self.var_of(e)?,
            _ => None,
        };
        if let Some(Var::Rows { local, .. }) = rows {
            return Ok(V {
                e: Expr::bin(
                    BinOp::Coalesce,
                    Expr::call(Builtin::JsonLength, vec![Expr::Local(local)]),
                    Expr::int(0),
                ),
                ty: Ty::Int,
                n: Nul::Never,
            });
        }
        let v = self.val(e)?;
        if v.ty != Ty::Str {
            return self.stop("lua.length", e.pos, format!("# of a {} value", v.ty));
        }
        let v = self.non_null(v, e.pos, "get length of a nil value")?;
        Ok(V {
            e: Expr::call(Builtin::ByteLen, vec![v.e]),
            ty: Ty::Int,
            n: Nul::Never,
        })
    }

    // -- decoded JSON (triggers on streams and queues) -------------------------
    //
    // `cjson.decode(text)` in compiled code is the text itself, checked as
    // the engine tier's decoder reads it: a decoded value is JSON text,
    // `NULL` is `nil` (a missing member) and the JSON `null` is
    // `cjson.null` (`sprig.null`). A value is read as the kind its use
    // needs (arithmetic a number, `..` text, a bind its column's type);
    // where it is another kind, or Lua would convert it, the record runs in
    // the engine tier (39876), which then gives Lua's answer.

    /// The JSON text of a decoded value when `x` is one: `cjson.decode(s)`,
    /// a name holding one, a member or item of one (`e.items[1]`), or
    /// `ctx.record.headers` and `ctx.message.properties`.
    pub(super) fn dval(&mut self, x: &ast::Expr) -> R<Option<Expr>> {
        let Some(on) = self.msg else {
            return Ok(None);
        };
        match &x.k {
            ExprKind::Paren(e) => self.dval(e),
            ExprKind::Name(n) => Ok(match self.lookup(n) {
                Some(Var::Dyn(l)) => Some(Expr::Local(l)),
                _ => None,
            }),
            ExprKind::Field(o, f) => {
                if let Some(Var::Event) = self.ctx_path(o) {
                    return Ok(on
                        .fields()
                        .iter()
                        .find(|(n, t, _)| n == f && *t == Ty::Json)
                        .map(|(n, _, _)| Expr::Param(self.param_named(n, Ty::Json))));
                }
                let Some(d) = self.dval(o)? else {
                    return Ok(None);
                };
                Ok(Some(Expr::call(
                    Builtin::JsonGet,
                    vec![d, str_lit(f), Expr::bool(false)],
                )))
            }
            ExprKind::Index(o, k) => {
                let Some(d) = self.dval(o)? else {
                    return Ok(None);
                };
                if let ExprKind::Str(name) = &k.k {
                    return Ok(Some(Expr::call(
                        Builtin::JsonGet,
                        vec![d, str_lit(name), Expr::bool(false)],
                    )));
                }
                let i = self.val(k)?;
                if i.ty != Ty::Int {
                    return self.stop(
                        "lua.json",
                        k.pos,
                        "an index that isn't a whole number or a literal name",
                    );
                }
                // Lua counts from 1; a missing item is nil.
                Ok(Some(Expr::call(
                    Builtin::JsonGet,
                    vec![
                        d,
                        Expr::bin(BinOp::Sub, i.e, Expr::int(1)),
                        Expr::bool(false),
                    ],
                )))
            }
            ExprKind::Call(callee, args) if self.is_lib(callee, &["cjson", "decode"]) => {
                let [a] = args.as_slice() else {
                    return self.stop("lua.json", x.pos, "cjson.decode takes one argument");
                };
                let v = self.val(a)?;
                if !matches!(v.ty, Ty::Str | Ty::Null) {
                    return self.stop(
                        "lua.json",
                        a.pos,
                        format!("cjson.decode of a {} value", v.ty),
                    );
                }
                if v.n != Nul::Never {
                    // `cjson.decode(nil)` fails: the engine tier says how.
                    self.guard(
                        is_null(v.e.clone()),
                        super::rerun("cjson.decode of a value that isn't text"),
                    );
                }
                Ok(Some(Expr::call(
                    Builtin::JsonParse,
                    vec![v.e, Expr::bool(true)],
                )))
            }
            _ => Ok(None),
        }
    }

    /// A decoded value read as `ty`: another kind runs the record in the
    /// engine tier; with `nullable`, `nil` and `cjson.null` are `NULL`.
    pub(super) fn dread(d: Expr, ty: Ty, nullable: bool) -> Option<V> {
        let (b, t) = match ty {
            Ty::Str => (Builtin::JsonAsStr, Ty::Str),
            Ty::Float => (Builtin::JsonAsNum, Ty::Float),
            Ty::Int => (Builtin::JsonAsInt, Ty::Int),
            Ty::Bool => (Builtin::JsonAsBool, Ty::Bool),
            Ty::Json | Ty::Null => return None,
        };
        Some(V {
            e: Expr::call(b, vec![d, Expr::bool(nullable)]),
            ty: t,
            n: if nullable { Nul::SNull } else { Nul::Never },
        })
    }

    /// Whether a decoded value is of a kind (`string`, `number`,
    /// `boolean`, `null`, `undefined` for nil, `object`, `array`).
    fn dkind(d: &Expr, k: &str) -> Expr {
        Expr::call(Builtin::JsonKind, vec![d.clone(), str_lit(k)])
    }

    /// Lua's truth of a decoded value: only `nil` and `false` are false
    /// (`cjson.null`, `0` and `''` are true).
    pub(super) fn dtruth(d: &Expr) -> Expr {
        Expr::Cond(
            Box::new(Self::dkind(d, "boolean")),
            Box::new(Expr::call(
                Builtin::JsonAsBool,
                vec![d.clone(), Expr::bool(false)],
            )),
            Box::new(not(Self::dkind(d, "undefined"))),
        )
    }

    /// `d == o` where `d` is decoded and `o` isn't: values of different
    /// types are never equal; `nil` and `cjson.null` each equal only
    /// themselves.
    fn dequals(&mut self, d: &Expr, o: &ast::Expr, pos: Pos) -> R<Expr> {
        if matches!(o.k, ExprKind::Nil) {
            return Ok(Self::dkind(d, "undefined"));
        }
        if self.is_snull(o) {
            return Ok(Self::dkind(d, "null"));
        }
        if self.dval(o)?.is_some() {
            return self.stop(
                "lua.compare",
                pos,
                "comparing two decoded values (tables compare by identity)",
            );
        }
        let v = self.val(o)?;
        let (kind, ty) = match v.ty {
            Ty::Str => ("string", Ty::Str),
            Ty::Int | Ty::Float => ("number", Ty::Float),
            Ty::Bool => ("boolean", Ty::Bool),
            Ty::Null => {
                return Ok(Self::dkind(
                    d,
                    if v.n == Nul::SNull {
                        "null"
                    } else {
                        "undefined"
                    },
                ));
            }
            Ty::Json => {
                return self.stop(
                    "lua.compare",
                    pos,
                    "tables compare by identity: it runs in the engine tier",
                );
            }
        };
        let read = Self::dread(d.clone(), ty, false).map_or(Expr::Lit(Literal::Null), |r| r.e);
        let o_e = if ty == Ty::Float {
            to_float(v.clone())
        } else {
            v.e.clone()
        };
        let typed = Expr::Cond(
            Box::new(Self::dkind(d, kind)),
            Box::new(Expr::bin(BinOp::Eq, read, o_e)),
            Box::new(Expr::bool(false)),
        );
        Ok(match v.n {
            Nul::Never => typed,
            Nul::Both => {
                return self.stop(
                    "lua.null_kinds",
                    pos,
                    "comparing a value that may be nil or sprig.null",
                );
            }
            k => Expr::Cond(
                Box::new(is_null(v.e)),
                Box::new(Self::dkind(
                    d,
                    if k == Nul::SNull { "null" } else { "undefined" },
                )),
                Box::new(typed),
            ),
        })
    }

    /// Lowers `x` for an operator: a decoded value read as `want` (its
    /// kind checked as it is read), anything else as usual.
    fn operand(&mut self, x: &ast::Expr, want: Ty) -> R<V> {
        match self.dval(x)? {
            Some(d) => Ok(Self::dread(d, want, false).unwrap_or_else(V::nil)),
            None => self.val(x),
        }
    }

    /// A decoded value as Lua writes it with `..` and `tostring`: text as
    /// it is, a whole number below 10^14 in digits (Lua's `%.14g` and
    /// compiled code agree there); anything else (a fraction, `-0`, a
    /// table, nil) runs the record in the engine tier.
    fn dtext(&mut self, d: &Expr) -> V {
        let num = Expr::call(Builtin::JsonAsNum, vec![d.clone(), Expr::bool(false)]);
        let plain = Expr::bin(
            BinOp::And,
            Expr::bin(
                BinOp::Eq,
                Expr::call(Builtin::Floor, vec![num.clone()]),
                num.clone(),
            ),
            Expr::bin(
                BinOp::And,
                Expr::bin(
                    BinOp::Lt,
                    Expr::call(Builtin::Abs, vec![num.clone()]),
                    Expr::Lit(Literal::Float(1e14)),
                ),
                not(Expr::call(
                    Builtin::StartsWith,
                    vec![d.clone(), str_lit("-0")],
                )),
            ),
        );
        let ok = Expr::Cond(
            Box::new(Self::dkind(d, "string")),
            Box::new(Expr::bool(true)),
            Box::new(Expr::Cond(
                Box::new(Self::dkind(d, "number")),
                Box::new(plain),
                Box::new(Expr::bool(false)),
            )),
        );
        self.guard(
            not(ok),
            super::rerun("a value Lua writes as text in its own way"),
        );
        V {
            e: Expr::Cond(
                Box::new(Self::dkind(d, "string")),
                Box::new(Expr::call(
                    Builtin::JsonAsStr,
                    vec![d.clone(), Expr::bool(false)],
                )),
                Box::new(Expr::call(Builtin::ToStr, vec![num])),
            ),
            ty: Ty::Str,
            n: Nul::Never,
        }
    }

    /// `x` as text when it is decoded ([`Self::dtext`]), else lowered.
    fn dtext_or_val(&mut self, x: &ast::Expr) -> R<V> {
        match self.dval(x)? {
            Some(d) => Ok(self.dtext(&d)),
            None => self.val(x),
        }
    }

    /// Whether `x` is a decoded value, without lowering it.
    fn is_decoded(&self, x: &ast::Expr) -> bool {
        if self.msg.is_none() {
            return false;
        }
        match &x.k {
            ExprKind::Paren(e) => self.is_decoded(e),
            ExprKind::Name(n) => matches!(self.lookup(n), Some(Var::Dyn(_))),
            ExprKind::Field(o, f) => {
                if let Some(Var::Event) = self.ctx_path(o) {
                    return matches!(f.as_str(), "headers" | "properties");
                }
                self.is_decoded(o)
            }
            ExprKind::Index(o, _) => self.is_decoded(o),
            ExprKind::Call(c, _) => self.is_lib(c, &["cjson", "decode"]),
            _ => false,
        }
    }

    /// The text a `print` or `log` argument becomes in the run log, as
    /// the engine tier writes it: text as it is, whole numbers below 10^15
    /// without a fraction, `nil`, `true`, `lightuserdata` for `sprig.null`,
    /// `table` for a table.
    fn log_text(&mut self, x: &ast::Expr) -> R<Expr> {
        if let Some(d) = self.dval(x)? {
            let num = Expr::call(Builtin::JsonAsNum, vec![d.clone(), Expr::bool(false)]);
            self.exotic_number(&num, Some(&d));
            let kinds = [
                (
                    "string",
                    Expr::call(Builtin::JsonAsStr, vec![d.clone(), Expr::bool(false)]),
                ),
                ("number", Expr::call(Builtin::ToStr, vec![num])),
                (
                    "boolean",
                    Expr::call(
                        Builtin::ToStr,
                        vec![Expr::call(
                            Builtin::JsonAsBool,
                            vec![d.clone(), Expr::bool(false)],
                        )],
                    ),
                ),
                ("undefined", str_lit("nil")),
                ("null", str_lit("lightuserdata")),
            ];
            let mut e = str_lit("table");
            for (k, t) in kinds.into_iter().rev() {
                e = Expr::Cond(Box::new(Self::dkind(&d, k)), Box::new(t), Box::new(e));
            }
            return Ok(e);
        }
        let v = self.val(x)?;
        let text = match v.ty {
            Ty::Str => v.e.clone(),
            Ty::Int | Ty::Bool => Expr::call(Builtin::ToStr, vec![v.e.clone()]),
            Ty::Float => {
                self.exotic_number(&v.e, None);
                Expr::call(Builtin::ToStr, vec![v.e.clone()])
            }
            Ty::Json => str_lit("table"),
            Ty::Null => str_lit(if v.n == Nul::SNull {
                "lightuserdata"
            } else {
                "nil"
            }),
        };
        Ok(match v.n {
            Nul::Never => text,
            Nul::Both => {
                return self.stop(
                    "lua.null_kinds",
                    x.pos,
                    "logging a value that may be nil or sprig.null",
                );
            }
            k if v.ty == Ty::Null => {
                let _ = k;
                text
            }
            k => Expr::Cond(
                Box::new(is_null(v.e)),
                Box::new(str_lit(if k == Nul::SNull {
                    "lightuserdata"
                } else {
                    "nil"
                })),
                Box::new(text),
            ),
        })
    }

    /// A number the run log writes differently than compiled code (from
    /// 10^21, or a fraction below 10^-6, where one uses an exponent and
    /// the other doesn't): the record runs in the engine tier.
    fn exotic_number(&mut self, x: &Expr, decoded: Option<&Expr>) {
        let big = Expr::Lit(Literal::Float(1e21));
        let tiny = Expr::Lit(Literal::Float(1e-6));
        let abs = Expr::call(Builtin::Abs, vec![x.clone()]);
        let exotic = Expr::bin(
            BinOp::Or,
            Expr::bin(BinOp::Ge, abs.clone(), big),
            Expr::bin(
                BinOp::And,
                Expr::bin(BinOp::Ne, x.clone(), Expr::Lit(Literal::Float(0.0))),
                Expr::bin(BinOp::Lt, abs, tiny),
            ),
        );
        let check = match decoded {
            Some(d) => Expr::Cond(
                Box::new(Self::dkind(d, "number")),
                Box::new(exotic),
                Box::new(Expr::bool(false)),
            ),
            None => Expr::bin(BinOp::And, not(is_null(x.clone())), exotic),
        };
        self.guard(
            check,
            super::rerun("a number the run log writes with an exponent"),
        );
    }

    /// One `print` or `log` line: the arguments' text joined by spaces.
    pub(super) fn log_line(&mut self, args: &[ast::Expr]) -> R<Expr> {
        let mut acc: Option<Expr> = None;
        for a in args {
            if matches!(a.k, ExprKind::Vararg) {
                return self.stop("lua.vararg", a.pos, "... as arguments");
            }
            let t = self.log_text(a)?;
            acc = Some(match acc {
                None => t,
                Some(p) => Expr::bin(BinOp::Concat, Expr::bin(BinOp::Concat, p, str_lit(" ")), t),
            });
        }
        Ok(acc.unwrap_or_else(|| str_lit("")))
    }

    // -- operators ------------------------------------------------------------

    /// An arithmetic operand: a number, checked non-null.
    fn arith_operand(&mut self, v: V, pos: Pos) -> R<V> {
        match v.ty {
            Ty::Int | Ty::Float => self.non_null(v, pos, "perform arithmetic on a nil value"),
            Ty::Str => self.stop(
                "lua.dynamic_types",
                pos,
                "arithmetic on text converts it to a number at run time",
            ),
            Ty::Null => self.stop(
                "lua.nil_value",
                pos,
                "arithmetic on nil: the value is always nil",
            ),
            other => self.stop(
                "lua.dynamic_types",
                pos,
                format!("arithmetic on a {other} value"),
            ),
        }
    }

    /// Text for `..`: text and numbers, checked non-null.
    fn concat_operand(&mut self, v: V, pos: Pos) -> R<Expr> {
        match v.ty {
            Ty::Str => Ok(self.non_null(v, pos, "concatenate a nil value")?.e),
            Ty::Int | Ty::Float => {
                let v = self.non_null(v, pos, "concatenate a nil value")?;
                Ok(Expr::call(Builtin::ToStr, vec![v.e]))
            }
            other => self.stop("lua.concat", pos, format!("concatenating a {other} value")),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn binary(&mut self, op: LBin, a: &ast::Expr, b: &ast::Expr, pos: Pos) -> R<V> {
        match op {
            LBin::And => return self.and(a, b, pos),
            LBin::Or => return self.or(a, b, pos),
            LBin::Eq | LBin::Ne => {
                if let Some(e) = self.row_equals(a, b) {
                    return Ok(V {
                        e: if op == LBin::Ne { not(e) } else { e },
                        ty: Ty::Bool,
                        n: Nul::Never,
                    });
                }
            }
            _ => {}
        }
        if (self.is_decoded(a) || self.is_decoded(b)) && matches!(op, LBin::Eq | LBin::Ne) {
            let e = match self.dval(a)? {
                Some(d) => self.dequals(&d, b, pos)?,
                None => match self.dval(b)? {
                    Some(d) => self.dequals(&d, a, pos)?,
                    None => return Err(Stop),
                },
            };
            return Ok(V {
                e: if op == LBin::Ne { not(e) } else { e },
                ty: Ty::Bool,
                n: Nul::Never,
            });
        }
        // A decoded operand is read as the operator needs: text for `..`
        // and for comparing with text, a number otherwise.
        let cmp = matches!(op, LBin::Lt | LBin::Le | LBin::Gt | LBin::Ge);
        let (da, db_) = (self.is_decoded(a), self.is_decoded(b));
        let (x, y) = if cmp && da && !db_ {
            let y = self.val(b)?;
            let w = if y.ty == Ty::Str { Ty::Str } else { Ty::Float };
            (self.operand(a, w)?, y)
        } else if cmp && db_ && !da {
            let x = self.val(a)?;
            let w = if x.ty == Ty::Str { Ty::Str } else { Ty::Float };
            let y = self.operand(b, w)?;
            (x, y)
        } else if op == LBin::Concat {
            let x = self.dtext_or_val(a)?;
            (x, self.dtext_or_val(b)?)
        } else {
            let x = self.operand(a, Ty::Float)?;
            (x, self.operand(b, Ty::Float)?)
        };
        match op {
            LBin::Add | LBin::Sub | LBin::Mul => {
                let (x, y) = (self.arith_operand(x, a.pos)?, self.arith_operand(y, b.pos)?);
                let (p, q, ty) = widen_pair(x, y);
                let o = match op {
                    LBin::Add => BinOp::Add,
                    LBin::Sub => BinOp::Sub,
                    _ => BinOp::Mul,
                };
                Ok(V {
                    e: Expr::bin(o, p, q),
                    ty,
                    n: Nul::Never,
                })
            }
            LBin::Div => {
                let (x, y) = (self.arith_operand(x, a.pos)?, self.arith_operand(y, b.pos)?);
                let (p, q, _) = widen_pair(x, y);
                Ok(V {
                    e: Expr::bin(BinOp::Div, p, q),
                    ty: Ty::Float,
                    n: Nul::Never,
                })
            }
            LBin::Mod => {
                // Lua's `a % b` is `a - floor(a/b)*b`: the divisor's sign.
                let (x, y) = (self.arith_operand(x, a.pos)?, self.arith_operand(y, b.pos)?);
                let (p, q, ty) = widen_pair(x, y);
                let e = if ty == Ty::Int {
                    Expr::bin(
                        BinOp::Mod,
                        Expr::bin(BinOp::Add, Expr::bin(BinOp::Mod, p, q.clone()), q.clone()),
                        q,
                    )
                } else {
                    Expr::bin(
                        BinOp::Sub,
                        p.clone(),
                        Expr::bin(
                            BinOp::Mul,
                            Expr::call(Builtin::Floor, vec![Expr::bin(BinOp::Div, p, q.clone())]),
                            q,
                        ),
                    )
                };
                Ok(V {
                    e,
                    ty,
                    n: Nul::Never,
                })
            }
            LBin::Pow => {
                let (x, y) = (self.arith_operand(x, a.pos)?, self.arith_operand(y, b.pos)?);
                Ok(V {
                    e: Expr::call(Builtin::Pow, vec![to_float(x), to_float(y)]),
                    ty: Ty::Float,
                    n: Nul::Never,
                })
            }
            LBin::Concat => {
                let p = self.concat_operand(x, a.pos)?;
                let q = self.concat_operand(y, b.pos)?;
                Ok(V {
                    e: Expr::bin(BinOp::Concat, p, q),
                    ty: Ty::Str,
                    n: Nul::Never,
                })
            }
            LBin::Eq | LBin::Ne => {
                let e = self.equals(x, y, pos)?;
                Ok(V {
                    e: if op == LBin::Ne { not(e) } else { e },
                    ty: Ty::Bool,
                    n: Nul::Never,
                })
            }
            LBin::Lt | LBin::Le | LBin::Gt | LBin::Ge => {
                let o = match op {
                    LBin::Lt => BinOp::Lt,
                    LBin::Le => BinOp::Le,
                    LBin::Gt => BinOp::Gt,
                    _ => BinOp::Ge,
                };
                let both_numbers = x.ty.is_number() && y.ty.is_number();
                let both_text = x.ty == Ty::Str && y.ty == Ty::Str;
                if !both_numbers && !both_text {
                    return self.stop(
                        "lua.compare",
                        pos,
                        format!("comparing a {} value with a {} value", x.ty, y.ty),
                    );
                }
                let x = self.non_null(x, a.pos, "compare a nil value")?;
                let y = self.non_null(y, b.pos, "compare a nil value")?;
                let (p, q) = if both_numbers {
                    let (p, q, _) = widen_pair(x, y);
                    (p, q)
                } else {
                    (x.e, y.e)
                };
                Ok(V {
                    e: Expr::bin(o, p, q),
                    ty: Ty::Bool,
                    n: Nul::Never,
                })
            }
            LBin::And | LBin::Or => Err(Stop),
        }
    }

    /// Lua's `==`: values of different types are never equal; `nil`
    /// equals only `nil` and `sprig.null` only `sprig.null`.
    fn equals(&mut self, x: V, y: V, pos: Pos) -> R<Expr> {
        if x.ty == Ty::Json || y.ty == Ty::Json {
            return self.stop(
                "lua.compare",
                pos,
                "tables compare by identity: it runs in the engine tier",
            );
        }
        if x.n == Nul::Both && y.n != Nul::Never || y.n == Nul::Both && x.n != Nul::Never {
            return self.stop(
                "lua.null_kinds",
                pos,
                "comparing a value that may be nil or sprig.null",
            );
        }
        // A null literal against a value: whether it is that null.
        match (x.ty == Ty::Null, y.ty == Ty::Null) {
            (true, true) => return Ok(Expr::bool(x.n == y.n)),
            (true, false) | (false, true) => {
                let (lit, v) = if x.ty == Ty::Null { (x, y) } else { (y, x) };
                return Ok(if v.n == lit.n {
                    is_null(v.e)
                } else {
                    Expr::bool(false)
                });
            }
            _ => {}
        }
        let same_type = x.ty == y.ty || (x.ty.is_number() && y.ty.is_number());
        let kinds_differ = x.n != Nul::Never && y.n != Nul::Never && x.n != y.n;
        if !same_type {
            // Equal only when both are the same null.
            return Ok(if x.n != Nul::Never && x.n == y.n {
                Expr::bin(BinOp::And, is_null(x.e), is_null(y.e))
            } else {
                Expr::bool(false)
            });
        }
        let guard = is_null(x.e.clone());
        let (p, q) = if x.ty.is_number() {
            let (p, q, _) = widen_pair(x, y);
            (p, q)
        } else {
            (x.e, y.e)
        };
        let eq = Expr::bin(BinOp::Eq, p, q);
        Ok(if kinds_differ {
            Expr::bin(BinOp::And, eq, not(guard))
        } else {
            eq
        })
    }

    /// `a and b`: `b` when `a` is true, else `a`.
    fn and(&mut self, a: &ast::Expr, b: &ast::Expr, pos: Pos) -> R<V> {
        if self.row_of(a).is_some() {
            // A row (or sprig.null) is true: the value is `b`.
            return self.val(b);
        }
        let x = self.val(a)?;
        if x.never_falsy() {
            let facts = self.facts_if(a, true);
            self.facts.extend(facts);
            return self.val(b);
        }
        let t = self.truth(&x, a.pos)?;
        let y = self.cond_val(b, self.facts_if(a, true), t.clone())?;
        if x.ty == Ty::Bool && y.ty == Ty::Bool && x.n == Nul::Never && y.n == Nul::Never {
            let (tx, ty_) = (self.truth(&x, pos)?, self.truth(&y, pos)?);
            return Ok(V {
                e: Expr::bin(BinOp::And, tx, ty_),
                ty: Ty::Bool,
                n: Nul::Never,
            });
        }
        let ty = if x.ty == Ty::Null {
            y.ty
        } else {
            self.join(x.ty, y.ty, pos)?
        };
        let n = y.n.join(x.n.nil_part());
        let (p, q) = (self.coerce(y, ty, pos)?, self.coerce(x, ty, pos)?);
        Ok(V {
            e: Expr::Cond(Box::new(t), Box::new(p), Box::new(q)),
            ty,
            n,
        })
    }

    /// `a or b`: `a` when it is true, else `b`; `c and x or y` with an `x`
    /// that is never false is `c ? x : y`.
    fn or(&mut self, a: &ast::Expr, b: &ast::Expr, pos: Pos) -> R<V> {
        if let ExprKind::Bin(LBin::And, c, x) = &a.k {
            let mark = self.pre.len();
            let tc = self.test(c)?;
            let xv = self.cond_val(x, self.facts_if(c, true), tc.clone())?;
            if xv.never_falsy() {
                let yv = self.cond_val(b, self.facts_if(c, false), not(tc.clone()))?;
                let ty = if yv.ty == Ty::Null {
                    xv.ty
                } else {
                    self.join(xv.ty, yv.ty, pos)?
                };
                let n = xv.n.join(yv.n);
                let (p, q) = (self.coerce(xv, ty, pos)?, self.coerce(yv, ty, pos)?);
                return Ok(V {
                    e: Expr::Cond(Box::new(tc), Box::new(p), Box::new(q)),
                    ty,
                    n,
                });
            }
            // Lowered again below as `(c and x) or b`.
            self.pre.truncate(mark);
        }
        if let Some(d) = self.dval(a)? {
            // `e.name or 'anon'`: the decoded value read as the other
            // side's type when it is true.
            let t = Self::dtruth(&d);
            let y = self.cond_val(b, BTreeSet::new(), not(t.clone()))?;
            let ty = if y.ty == Ty::Int { Ty::Float } else { y.ty };
            let Some(x) = Self::dread(d, ty, false) else {
                return self.stop(
                    "lua.json",
                    a.pos,
                    "a decoded value or a table: it runs in the engine tier",
                );
            };
            let q = self.coerce(y.clone(), ty, pos)?;
            return Ok(V {
                e: Expr::Cond(Box::new(t), Box::new(x.e), Box::new(q)),
                ty,
                n: y.n,
            });
        }
        let x = self.val(a)?;
        if x.never_falsy() {
            return Ok(x);
        }
        let t = self.truth(&x, a.pos)?;
        let y = self.cond_val(b, self.facts_if(a, false), not(t.clone()))?;
        if x.ty == Ty::Bool && y.ty == Ty::Bool && x.n == Nul::Never && y.n == Nul::Never {
            let (tx, ty_) = (self.truth(&x, pos)?, self.truth(&y, pos)?);
            return Ok(V {
                e: Expr::bin(BinOp::Or, tx, ty_),
                ty: Ty::Bool,
                n: Nul::Never,
            });
        }
        let ty = if x.ty == Ty::Null {
            y.ty
        } else {
            self.join(x.ty, y.ty, pos)?
        };
        let n = x.n.snull_part().join(y.n);
        let (p, q) = (self.coerce(x, ty, pos)?, self.coerce(y, ty, pos)?);
        Ok(V {
            e: Expr::Cond(Box::new(t), Box::new(p), Box::new(q)),
            ty,
            n,
        })
    }

    // -- calls ----------------------------------------------------------------

    /// The library function a callee names: `["math", "floor"]`,
    /// `["print"]`. `None` for anything else.
    pub(super) fn lib_path(&self, e: &ast::Expr) -> Option<Vec<String>> {
        const LIBS: &[&str] = &[
            "db",
            "sprig",
            "math",
            "string",
            "table",
            "ctx",
            "cjson",
            "coroutine",
            "os",
            "io",
            "debug",
            "package",
            "redis",
        ];
        const GLOBALS: &[&str] = &[
            "print",
            "log",
            "error",
            "assert",
            "pcall",
            "xpcall",
            "tostring",
            "tonumber",
            "type",
            "ipairs",
            "pairs",
            "select",
            "unpack",
            "next",
            "rawget",
            "rawset",
            "rawequal",
            "setmetatable",
            "getmetatable",
            "loadstring",
            "load",
            "dofile",
            "require",
            "fetch",
            "setfenv",
            "getfenv",
            "collectgarbage",
            "newproxy",
            "gcinfo",
            "_G",
        ];
        match &e.k {
            ExprKind::Name(n) if self.lookup(n).is_none() && GLOBALS.contains(&n.as_str()) => {
                Some(vec![n.clone()])
            }
            ExprKind::Field(o, f) => match &o.k {
                ExprKind::Name(n) if self.lookup(n).is_none() && LIBS.contains(&n.as_str()) => {
                    Some(vec![n.clone(), f.clone()])
                }
                // The code's own `ctx` (a trigger's or a validation's) logs
                // as the global one does.
                ExprKind::Name(n)
                    if f == "log" && matches!(self.lookup(n), Some(Var::Msg | Var::Ctx)) =>
                {
                    Some(vec!["ctx".to_owned(), "log".to_owned()])
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Whether `e` names the library function `path`.
    pub(super) fn is_lib(&self, e: &ast::Expr, path: &[&str]) -> bool {
        self.lib_path(e)
            .is_some_and(|p| p.iter().map(String::as_str).eq(path.iter().copied()))
    }

    /// A call as a statement.
    pub(super) fn call_stmt(&mut self, e: &ast::Expr, out: &mut Block) -> R<()> {
        self.sole = Some(std::ptr::from_ref(e) as usize);
        if self.db_call(e, "", out)?.is_some() {
            return Ok(());
        }
        let ExprKind::Call(callee, args) = &e.k else {
            // A method call: its value is dropped.
            self.call(e)?;
            self.flush(out);
            return Ok(());
        };
        let path = self.lib_path(callee);
        let path: Option<Vec<&str>> = path
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        match path.as_deref() {
            Some(["sprig", "reject"]) => {
                let m = match args.first() {
                    Some(a) => {
                        let v = self.val(a)?;
                        self.message_text(v, a.pos)?
                    }
                    None => str_lit("nil"),
                };
                self.flush(out);
                out.push(Stmt::Throw(ErrorSpec::new(39863, m)));
                Ok(())
            }
            Some(["error"]) => {
                let spec = self.error_spec(args, e.pos)?;
                self.flush(out);
                out.push(Stmt::Throw(spec));
                Ok(())
            }
            Some(["assert"]) => {
                self.assert(args, e.pos)?;
                self.flush(out);
                Ok(())
            }
            Some(["print" | "log"] | ["ctx", "log"]) if self.msg.is_some() => {
                // Triggers on streams and queues keep their log lines.
                let line = self.log_line(args)?;
                self.flush(out);
                out.push(Stmt::Log(line));
                Ok(())
            }
            Some(["print" | "log"] | ["ctx", "log"]) => {
                self.lints.push(crate::Reason::new(
                    "lua.log",
                    &Self::at(e.pos),
                    "log lines are dropped in the compiled tier",
                ));
                Ok(())
            }
            _ => {
                // A helper (or anything else) called for what it does.
                self.call(e)?;
                self.flush(out);
                Ok(())
            }
        }
    }

    /// The text `sprig.reject` and `error` carry: text as it is, numbers
    /// as Lua writes them.
    fn message_text(&mut self, v: V, pos: Pos) -> R<Expr> {
        match v.ty {
            Ty::Str | Ty::Int | Ty::Float if v.n == Nul::Never => Ok(match v.ty {
                Ty::Str => v.e,
                _ => Expr::call(Builtin::ToStr, vec![v.e]),
            }),
            Ty::Str | Ty::Int | Ty::Float if v.n == Nul::Nil => Ok(Expr::bin(
                BinOp::Coalesce,
                if v.ty == Ty::Str {
                    v.e
                } else {
                    Expr::call(Builtin::ToStr, vec![v.e])
                },
                str_lit("nil"),
            )),
            Ty::Null if v.n == Nul::Nil => Ok(str_lit("nil")),
            _ => self.stop(
                "lua.error_value",
                pos,
                "the message must be text or a number that is never sprig.null",
            ),
        }
    }

    /// `error(message [, level])`: 39865 with Lua's position before the
    /// message (level 1), or the message alone (level 0).
    fn error_spec(&mut self, args: &[ast::Expr], pos: Pos) -> R<ErrorSpec> {
        let level = match args.get(1).map(|a| &a.k) {
            None => 1,
            Some(ExprKind::Num(n)) if *n == 0.0 || *n == 1.0 => *n as i64,
            Some(_) => {
                return self.stop("lua.error_value", pos, "error's level must be 0 or 1");
            }
        };
        let Some(a) = args.first() else {
            return self.stop("lua.error_value", pos, "error() without a message");
        };
        let v = self.val(a)?;
        if !matches!(v.ty, Ty::Str | Ty::Int | Ty::Float) || v.n != Nul::Never {
            return self.stop(
                "lua.error_value",
                a.pos,
                "error's message must be text or a number that is never nil: a pcall sees \
                 other values as they are",
            );
        }
        let text = if v.ty == Ty::Str {
            v.e
        } else {
            Expr::call(Builtin::ToStr, vec![v.e])
        };
        let m = if level == 1 {
            Expr::bin(
                BinOp::Concat,
                str_lit(&format!("{}:{}: ", self.f.name, pos.line)),
                text,
            )
        } else {
            text
        };
        Ok(ErrorSpec {
            code: 39865,
            message: m.clone(),
            catch: Some(("Error".to_owned(), m)),
        })
    }

    /// `assert(v [, message])`: Lua's error with the position and the
    /// message when `v` is `nil` or `false`; the value is `v`.
    fn assert(&mut self, args: &[ast::Expr], pos: Pos) -> R<V> {
        let Some(a) = args.first() else {
            return self.stop("lua.call", pos, "assert() without a value");
        };
        let v = self.val(a)?;
        let t = self.truth(&v, a.pos)?;
        let m = match args.get(1) {
            None => str_lit("assertion failed!"),
            Some(x) => {
                let mv = self.val(x)?;
                if !matches!(mv.ty, Ty::Str | Ty::Int | Ty::Float) || mv.n != Nul::Never {
                    return self.stop(
                        "lua.error_value",
                        x.pos,
                        "assert's message must be text or a number that is never nil",
                    );
                }
                if mv.ty == Ty::Str {
                    mv.e
                } else {
                    Expr::call(Builtin::ToStr, vec![mv.e])
                }
            }
        };
        // The engine tier's Lua puts the position before the message, as
        // `error` does.
        let m = Expr::bin(
            BinOp::Concat,
            str_lit(&format!("{}:{}: ", self.f.name, pos.line)),
            m,
        );
        self.guard(
            not(t),
            ErrorSpec {
                code: 39865,
                message: m.clone(),
                catch: Some(("Error".to_owned(), m)),
            },
        );
        // After it, a name is known to be there.
        let facts = self.facts_if(a, true);
        self.facts.extend(facts);
        Ok(V {
            n: v.n.snull_part(),
            ..v
        })
    }

    /// A call in an expression.
    #[allow(clippy::too_many_lines)]
    pub(super) fn call(&mut self, x: &ast::Expr) -> R<V> {
        let (callee, args, method) = match &x.k {
            ExprKind::Call(c, a) => (c.as_ref(), a.as_slice(), None),
            ExprKind::Method(o, m, a) => (o.as_ref(), a.as_slice(), Some(m.as_str())),
            _ => return Err(Stop),
        };
        if let Some(m) = method {
            return self.method(callee, m, args, x.pos);
        }
        let sole = self.sole.take() == Some(std::ptr::from_ref(x) as usize);
        if let ExprKind::Name(n) = &callee.k
            && let Some(Var::Helper { func, depth }) = self.lookup(n)
        {
            return self.inline(&func, depth, args, x.pos, sole);
        }
        if let ExprKind::Field(o, _) = &callee.k
            && let ExprKind::Field(d, m) = &o.k
            && m == "mq"
            && matches!(&d.k, ExprKind::Name(n) if n == "db" && self.lookup(n).is_none())
        {
            return self.stop(
                "lua.db",
                x.pos,
                "db.mq runs in the engine tier: the gateway itself answers it",
            );
        }
        if matches!(&callee.k, ExprKind::Field(o, _) if matches!(&o.k, ExprKind::Name(n) if n == "db" && self.lookup(n).is_none()))
        {
            // A data call inside a larger expression: it runs first.
            return match self.var_of(x)? {
                Some(v @ (Var::Exec { .. } | Var::Call { .. })) => Ok(Self::result_value(&v)),
                Some(Var::Rows { local, .. }) => Ok(Self::rows_value(local)),
                _ => self.stop(
                    "lua.db",
                    x.pos,
                    "db.sqlOne's result is a table: give it a local first",
                ),
            };
        }
        let Some(path) = self.lib_path(callee) else {
            return self.stop("lua.call", x.pos, "this call runs in the engine tier");
        };
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        // `type`, `tostring` and `tonumber` of a decoded value.
        if let [a] = args
            && self.is_decoded(a)
        {
            match path.as_slice() {
                ["type"] => {
                    let Some(d) = self.dval(a)? else {
                        return Err(Stop);
                    };
                    let mut e = str_lit("table");
                    for (k, t) in [
                        ("string", "string"),
                        ("number", "number"),
                        ("boolean", "boolean"),
                        ("undefined", "nil"),
                        ("null", "userdata"),
                    ]
                    .into_iter()
                    .rev()
                    {
                        e = Expr::Cond(
                            Box::new(Self::dkind(&d, k)),
                            Box::new(str_lit(t)),
                            Box::new(e),
                        );
                    }
                    return Ok(V {
                        e,
                        ty: Ty::Str,
                        n: Nul::Never,
                    });
                }
                // Text as it is; a number's text is Lua's own, in the
                // engine tier.
                ["tostring"] => return self.dtext_or_val(a),
                // A number as it is; text Lua converts, in the engine tier.
                ["tonumber"] => return self.operand(a, Ty::Float),
                _ => {}
            }
        }
        // One argument; a decoded one is read as a number (`math.*`).
        let one = |l: &mut Self, args: &[ast::Expr]| -> R<V> {
            match args {
                [a] => l.operand(a, Ty::Float),
                [a, rest @ ..] => {
                    for r in rest {
                        l.val(r)?;
                    }
                    l.val(a)
                }
                [] => Ok(V::nil()),
            }
        };
        match path.as_slice() {
            ["math", f @ ("abs" | "floor" | "ceil" | "sqrt")] => {
                // `math.floor(a / b)` of whole numbers is a whole number.
                if *f == "floor"
                    && let [a] = args
                    && let ExprKind::Bin(LBin::Div, p, q) = &a.k
                {
                    let (pv, qv) = (self.val(p)?, self.val(q)?);
                    if pv.ty == Ty::Int && qv.ty == Ty::Int {
                        let pv = self.arith_operand(pv, p.pos)?;
                        let qv = self.arith_operand(qv, q.pos)?;
                        return Ok(V {
                            e: Expr::bin(BinOp::IntDiv, pv.e, qv.e),
                            ty: Ty::Int,
                            n: Nul::Never,
                        });
                    }
                }
                let v = one(self, args)?;
                let v = self.math_arg(v, x.pos)?;
                Ok(match *f {
                    "abs" => V {
                        ty: v.ty,
                        e: Expr::call(Builtin::Abs, vec![v.e]),
                        n: Nul::Never,
                    },
                    "floor" | "ceil" if v.ty == Ty::Int => v,
                    "floor" => V {
                        e: Expr::call(Builtin::Floor, vec![v.e]),
                        ty: Ty::Float,
                        n: Nul::Never,
                    },
                    "ceil" => V {
                        e: Expr::call(Builtin::Ceil, vec![v.e]),
                        ty: Ty::Float,
                        n: Nul::Never,
                    },
                    _ => {
                        return self.stop(
                            "lua.math",
                            x.pos,
                            "math.sqrt of a negative number is nan in Lua",
                        );
                    }
                })
            }
            ["math", f @ ("max" | "min")] => {
                if args.is_empty() {
                    return self.stop("lua.call", x.pos, "math.max() without arguments");
                }
                let mut vs = Vec::new();
                for a in args {
                    let v = self.val(a)?;
                    vs.push(self.math_arg(v, a.pos)?);
                }
                let ty = if vs.iter().all(|v| v.ty == Ty::Int) {
                    Ty::Int
                } else {
                    Ty::Float
                };
                let es = vs
                    .into_iter()
                    .map(|v| if ty == Ty::Float { to_float(v) } else { v.e })
                    .collect();
                let b = if *f == "max" {
                    Builtin::Max
                } else {
                    Builtin::Min
                };
                Ok(V {
                    e: Expr::call(b, es),
                    ty,
                    n: Nul::Never,
                })
            }
            ["math", f @ ("pow" | "fmod")] => {
                let [p, q] = args else {
                    return self.stop("lua.call", x.pos, format!("math.{f} takes two numbers"));
                };
                let (pv, qv) = (self.val(p)?, self.val(q)?);
                let pv = self.math_arg(pv, p.pos)?;
                let qv = self.math_arg(qv, q.pos)?;
                if *f == "pow" {
                    return Ok(V {
                        e: Expr::call(Builtin::Pow, vec![to_float(pv), to_float(qv)]),
                        ty: Ty::Float,
                        n: Nul::Never,
                    });
                }
                // C's fmod: the dividend's sign, as the IR's `Mod`.
                let (a, b, ty) = widen_pair(pv, qv);
                Ok(V {
                    e: Expr::bin(BinOp::Mod, a, b),
                    ty,
                    n: Nul::Never,
                })
            }
            ["math", "random" | "randomseed"] => self.stop(
                "lua.nondeterministic",
                x.pos,
                "math.random gives a different value on each call: it runs in the engine tier, \
                 and a computed column, validation or trigger can't use it",
            ),
            ["math", f] => self.stop(
                "lua.math",
                x.pos,
                format!("math.{f} runs in the engine tier"),
            ),
            ["string", f @ ("upper" | "lower" | "len")] => {
                let v = one(self, args)?;
                self.string_fn(v, f, x.pos)
            }
            ["string", f @ ("find" | "match" | "gmatch" | "gsub")] => self.stop(
                "lua.pattern",
                x.pos,
                format!("string.{f} uses Lua patterns: it runs in the engine tier"),
            ),
            ["string", f] => self.stop(
                "lua.string",
                x.pos,
                format!("string.{f} runs in the engine tier"),
            ),
            ["tostring"] => {
                let v = one(self, args)?;
                self.tostring(v, x.pos)
            }
            ["tonumber"] => {
                if args.len() > 1 {
                    return self.stop("lua.call", x.pos, "tonumber with a base");
                }
                let v = one(self, args)?;
                match v.ty {
                    // `tonumber(sprig.null)` is nil.
                    Ty::Int | Ty::Float => Ok(V {
                        n: if v.n == Nul::Never {
                            Nul::Never
                        } else {
                            Nul::Nil
                        },
                        ..v
                    }),
                    Ty::Null => Ok(V::nil()),
                    _ => self.stop(
                        "lua.dynamic_types",
                        x.pos,
                        "tonumber of text parses it at run time",
                    ),
                }
            }
            ["type"] => {
                let v = one(self, args)?;
                let name = match v.ty {
                    Ty::Int | Ty::Float => "number",
                    Ty::Str => "string",
                    Ty::Bool => "boolean",
                    Ty::Null => {
                        return Ok(V::lit(Literal::Str(
                            if v.n == Nul::SNull { "userdata" } else { "nil" }.to_owned(),
                        )));
                    }
                    Ty::Json => "table",
                };
                let lit = str_lit(name);
                Ok(match v.n {
                    Nul::Never => V {
                        e: lit,
                        ty: Ty::Str,
                        n: Nul::Never,
                    },
                    Nul::Both => {
                        return self.stop(
                            "lua.null_kinds",
                            x.pos,
                            "type of a value that may be nil or sprig.null",
                        );
                    }
                    k => V {
                        e: Expr::Cond(
                            Box::new(is_null(v.e)),
                            Box::new(str_lit(if k == Nul::SNull { "userdata" } else { "nil" })),
                            Box::new(lit),
                        ),
                        ty: Ty::Str,
                        n: Nul::Never,
                    },
                })
            }
            ["assert"] => self.assert(args, x.pos),
            ["error"] => self.stop(
                "lua.error_value",
                x.pos,
                "error as a value: call it as a statement",
            ),
            ["pcall"] => self.pcall(args, x.pos, sole).map(|(ok, _)| ok),
            ["xpcall"] => self.stop(
                "lua.pcall",
                x.pos,
                "xpcall's message handler runs in the engine tier: use pcall",
            ),
            ["ipairs" | "pairs" | "next"] => self.stop(
                "lua.pairs",
                x.pos,
                "iterators compile only in for ... in ipairs(rows)",
            ),
            ["setmetatable" | "getmetatable" | "rawget" | "rawset" | "rawequal"] => self.stop(
                "lua.metatable",
                x.pos,
                "metatables and raw access run in the engine tier",
            ),
            ["coroutine", _] => {
                self.stop("lua.coroutine", x.pos, "coroutines run in the engine tier")
            }
            ["loadstring" | "load" | "dofile" | "require"] => self.stop(
                "lua.loadstring",
                x.pos,
                "loading code runs in the engine tier",
            ),
            ["fetch"] => self.stop("lua.fetch", x.pos, "outbound calls run in the engine tier"),
            ["cjson", _] => self.stop("lua.json", x.pos, "cjson runs in the engine tier"),
            ["table", f] => self.stop(
                "lua.table",
                x.pos,
                format!("table.{f} runs in the engine tier"),
            ),
            ["sprig", "event"] => self.stop(
                "lua.event",
                x.pos,
                "sprig.event compiles only as return sprig.event(name, {fields})",
            ),
            ["ctx", _] => self.stop(
                "lua.ctx",
                x.pos,
                "ctx in compiled code isn't built: it runs in the engine tier",
            ),
            _ => self.stop("lua.call", x.pos, "this call runs in the engine tier"),
        }
    }

    /// A `math.*` argument: a number, checked non-null.
    fn math_arg(&mut self, v: V, pos: Pos) -> R<V> {
        if !v.ty.is_number() {
            return self.stop(
                "lua.dynamic_types",
                pos,
                format!("a math function of a {} value", v.ty),
            );
        }
        self.non_null(v, pos, "call a math function with a nil value")
    }

    /// `s:upper()`, `s:lower()`, `s:len()`.
    fn method(&mut self, obj: &ast::Expr, m: &str, args: &[ast::Expr], pos: Pos) -> R<V> {
        if !args.is_empty() || !matches!(m, "upper" | "lower" | "len") {
            return self.stop(
                if matches!(m, "find" | "match" | "gmatch" | "gsub") {
                    "lua.pattern"
                } else {
                    "lua.string"
                },
                pos,
                format!(":{m}() runs in the engine tier"),
            );
        }
        let v = self.operand(obj, Ty::Str)?;
        self.string_fn(v, m, pos)
    }

    fn string_fn(&mut self, v: V, f: &str, pos: Pos) -> R<V> {
        if v.ty != Ty::Str {
            return self.stop(
                "lua.dynamic_types",
                pos,
                format!("string.{f} of a {} value", v.ty),
            );
        }
        let v = self.non_null(v, pos, &format!("call string.{f} with a nil value"))?;
        Ok(match f {
            "len" => V {
                e: Expr::call(Builtin::ByteLen, vec![v.e]),
                ty: Ty::Int,
                n: Nul::Never,
            },
            "upper" => V {
                e: Expr::call(Builtin::AsciiUpper, vec![v.e]),
                ty: Ty::Str,
                n: Nul::Never,
            },
            _ => V {
                e: Expr::call(Builtin::AsciiLower, vec![v.e]),
                ty: Ty::Str,
                n: Nul::Never,
            },
        })
    }

    /// `tostring(v)`.
    fn tostring(&mut self, v: V, pos: Pos) -> R<V> {
        let text = match v.ty {
            Ty::Str => v.e.clone(),
            Ty::Int | Ty::Float | Ty::Bool => Expr::call(Builtin::ToStr, vec![v.e.clone()]),
            Ty::Null if v.n == Nul::Nil => str_lit("nil"),
            _ => {
                return self.stop("lua.tostring", pos, format!("tostring of a {} value", v.ty));
            }
        };
        let e = match v.n {
            Nul::Never => text,
            Nul::Nil => Expr::Cond(
                Box::new(is_null(v.e)),
                Box::new(str_lit("nil")),
                Box::new(text),
            ),
            _ => {
                return self.stop(
                    "lua.tostring",
                    pos,
                    "tostring of sprig.null names an address",
                );
            }
        };
        Ok(V {
            e,
            ty: Ty::Str,
            n: Nul::Never,
        })
    }

    // -- helpers ----------------------------------------------------------------

    /// A call of a helper, inlined in the scope it was declared in (a
    /// closure reads and writes the locals it sees there), with the
    /// arguments evaluated first. A helper whose body is one `return e` is
    /// an expression; any other runs as statements before the statement
    /// that calls it, in a block its `return`s leave.
    pub(super) fn inline(
        &mut self,
        func: &Rc<Func>,
        depth: usize,
        args: &[ast::Expr],
        pos: Pos,
        sole: bool,
    ) -> R<V> {
        if func.vararg {
            return self.stop("lua.vararg", pos, "a helper of ...");
        }
        if let Some(last) = args.last()
            && matches!(last.k, ExprKind::Call(..) | ExprKind::Method(..))
            && args.len() < func.params.len()
        {
            return self.stop(
                "lua.multiple_results",
                last.pos,
                "a call's results filling several parameters",
            );
        }
        let mut vals = Vec::with_capacity(args.len());
        for a in args {
            vals.push(self.val(a)?);
        }
        self.inline_vals(func, depth, &vals, pos, sole)
    }

    /// [`Self::inline`] with the arguments already evaluated.
    fn inline_vals(
        &mut self,
        func: &Rc<Func>,
        depth: usize,
        vals: &[V],
        pos: Pos,
        sole: bool,
    ) -> R<V> {
        let id = Rc::as_ptr(func) as usize;
        if self.inlining.contains(&id) {
            return self.stop(
                "lua.recursion",
                pos,
                "a helper that calls itself runs in the engine tier",
            );
        }
        if self.inlining.len() >= 8 || self.inlines >= 200 {
            return self.stop(
                "lua.helper",
                pos,
                "helpers nested more than 8 deep, or more than 200 helper calls",
            );
        }
        let single = match func.body.as_slice() {
            [
                ast::Stat {
                    k: StatKind::Return(r),
                    ..
                },
            ] if r.len() == 1 => Some(&r[0]),
            _ => None,
        };
        let saved_salt = self.salt;
        self.inlines += 1;
        self.salt = u64::from(self.inlines) << 32;
        let stash = self.scopes.split_off((depth + 1).min(self.scopes.len()));
        self.scopes.push(Vec::new());
        self.inlining.push(id);
        let r = if let Some(e) = single {
            for (i, p) in func.params.iter().enumerate() {
                let v = vals.get(i).cloned().unwrap_or_else(V::nil);
                self.bind(p, Var::Val(v));
            }
            self.val(e)
        } else {
            self.inline_body(func, vals, pos, sole)
        };
        self.inlining.pop();
        self.scopes.pop();
        self.scopes.extend(stash);
        self.salt = saved_salt;
        r
    }

    /// A statement-form helper's body, into the statements before the
    /// one calling it (under the conditions of the operand it is in): the
    /// parameters as locals, then the body in a block its `return`s leave.
    /// One that assigns the caller's locals must be the whole statement
    /// (`f(x)`, `local r = f(x)`, `return f(x)`): run before the statement,
    /// it would change values the statement read before the call.
    fn inline_body(&mut self, func: &Func, args: &[V], pos: Pos, sole: bool) -> R<V> {
        let before = self.facts.clone();
        let first_local = self.f.locals.len();
        let outer = std::mem::take(&mut self.pre);
        let mut stmts = Vec::new();
        let r = (|| {
            for (i, p) in func.params.iter().enumerate() {
                let v = args.get(i).cloned().unwrap_or_else(V::nil);
                let key = self.wkey(func.pos, &format!("param {i}"));
                let l = self.new_local(p, key, v.ty, v.n);
                self.set_local(l, v, func.pos, &mut stmts)?;
                self.bind(p, Var::Local(l));
            }
            Ok(())
        })();
        self.pre = outer;
        r?;
        let label = self.next_label();
        self.jframes.push(JFrame::Helper {
            label,
            result: None,
            all_values: true,
            kind: Nul::Never,
        });
        let body = self.block(&func.body);
        let Some(JFrame::Helper {
            result,
            all_values,
            kind,
            ..
        }) = self.jframes.pop()
        else {
            return Err(Stop);
        };
        let mut changed = BTreeSet::new();
        super::assigned(&body, &mut changed);
        if !sole
            && changed
                .iter()
                .any(|l| (l.0 as usize) < first_local && !self.is_result(*l))
        {
            self.facts = before;
            return self.stop(
                "lua.helper",
                pos,
                "a helper that changes the caller's variables, called inside an expression: \
                 call it in a statement of its own",
            );
        }
        self.facts = before;
        self.facts
            .retain(|k| !matches!(k, Key::L(l) if changed.contains(l)));
        let always = exits(&body);
        match result {
            Some(l) => {
                stmts.push(Stmt::Assign(l, Expr::Lit(Literal::Null)));
                stmts.push(Stmt::Block(label, body));
                self.needs(stmts);
                let n = if all_values && always {
                    kind
                } else {
                    kind.join(Nul::Nil)
                };
                Ok(V {
                    e: Expr::Local(l),
                    ty: self.f.local_def(l).ty,
                    n,
                })
            }
            None => {
                stmts.push(Stmt::Block(label, body));
                self.needs(stmts);
                Ok(V::nil())
            }
        }
    }

    /// Whether `l` is an enclosing helper's result local.
    fn is_result(&self, l: LocalId) -> bool {
        self.jframes
            .iter()
            .any(|f| matches!(f, JFrame::Helper { result: Some(r), .. } if *r == l))
    }

    // -- data access --------------------------------------------------------------

    /// `db.sql | sqlOne | exec(text, args...)`: the data operation, and
    /// what the result is bound as. `None` when `x` isn't one.
    pub(super) fn db_call(&mut self, x: &ast::Expr, name: &str, out: &mut Block) -> R<Option<Var>> {
        let ExprKind::Call(callee, args) = &x.k else {
            return Ok(None);
        };
        let Some(path) = self.lib_path(callee) else {
            return Ok(None);
        };
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        let kind = match path.as_slice() {
            ["db", "sql"] => "sql",
            ["db", "sqlOne"] => "sqlOne",
            ["db", "exec"] => "exec",
            ["db", "call"] => return self.db_invoke(x, args, name, out).map(Some),
            ["db", "publish"] => return self.db_publish(x, args, name, out).map(Some),
            ["db", "commitOffset" | "mq"] => {
                return self.stop(
                    "lua.db",
                    x.pos,
                    "db.commitOffset and db.mq run in the engine tier: the gateway itself \
                     answers them, so MariaDB has nothing to call",
                );
            }
            ["db", f] => {
                return self.stop("lua.db", x.pos, format!("db.{f} runs in the engine tier"));
            }
            _ => return Ok(None),
        };
        let at = Self::at(x.pos);
        let text = match args.first().map(|a| &a.k) {
            Some(ExprKind::Str(s)) => s.clone(),
            _ => return self.stop("db.dynamic_sql", x.pos, "the SQL text must be a literal"),
        };
        let mut binds = Vec::new();
        // Decoded arguments: bound as their column's type, once the
        // statement says which.
        let mut decoded: Vec<(usize, Expr, Pos)> = Vec::new();
        for a in &args[1..] {
            if matches!(a.k, ExprKind::Vararg) {
                return self.stop("lua.vararg", a.pos, "... as arguments");
            }
            if let Some(d) = self.dval(a)? {
                decoded.push((binds.len(), d, a.pos));
                binds.push(Expr::Lit(Literal::Null));
                continue;
            }
            let v = self.val(a)?;
            if v.ty == Ty::Json {
                return self.stop("db.json_bind", a.pos, "JSON values can't be bound yet");
            }
            if v.n == Nul::Both {
                // nil and sprig.null both bind as NULL.
            }
            binds.push(v.e);
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
        for (i, d, p) in decoded {
            // The engine tier binds the value's own kind; read as the
            // column's type, another kind runs the record in the engine
            // tier (nil and cjson.null bind as NULL in both).
            let ty = bound.targets.get(i).copied().flatten();
            let Some(r) = ty.and_then(|t| Self::dread(d, t, true)) else {
                return self.stop(
                    "db.json_bind",
                    p,
                    "a decoded value bound where the statement doesn't name its column \
                     (or the column is JSON): convert it first, with tostring(x) or tonumber(x)",
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
                    .map(|(c, t)| {
                        let l = self.f.local(&format!("{base}_{c}"), *t);
                        self.kinds.insert(l, Nul::SNull);
                        (c.clone(), l)
                    })
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
        if let Var::Row { found, cols } = &var {
            self.facts.remove(&Key::L(*found));
            for (_, l) in cols {
                self.facts.remove(&Key::L(*l));
            }
        }
        Ok(Some(var))
    }

    /// `db.call(name, ...)` into the callee's call entry: the callee must
    /// be a compiled function or procedure of this database the deploy
    /// knows.
    fn db_invoke(
        &mut self,
        x: &ast::Expr,
        args: &[ast::Expr],
        name: &str,
        out: &mut Block,
    ) -> R<Var> {
        let at = Self::at(x.pos);
        let target = match args.first().map(|a| &a.k) {
            Some(ExprKind::Str(s)) => s.clone(),
            _ => return self.stop("db.call", x.pos, "the function's name must be a literal"),
        };
        let callee = match self.cat.callee(&target) {
            None => {
                return self.stop(
                    "db.call",
                    x.pos,
                    format!("{target} isn't a logic function or procedure of this database"),
                );
            }
            Some(Err(why)) => return self.stop("db.call", x.pos, why),
            Some(Ok(k)) => k,
        };
        if callee.module_id == self.module_id || callee.reaches.contains(&self.module_id) {
            return self.stop(
                "lua.recursion",
                x.pos,
                format!("{target} calls back into this code"),
            );
        }
        if callee.depth + 1 > MAX_CALL_DEPTH {
            return self.stop(
                "db.call",
                x.pos,
                format!(
                    "calls through {target} nest deeper than logic.max_call_depth \
                     ({MAX_CALL_DEPTH})"
                ),
            );
        }
        let rest = &args[1..];
        if rest.len() != callee.params.len() {
            return self.stop(
                "db.call",
                x.pos,
                format!(
                    "{target} takes {} arguments, not {}",
                    callee.params.len(),
                    rest.len()
                ),
            );
        }
        let mut binds = Vec::new();
        for a in rest {
            if matches!(a.k, ExprKind::Vararg) {
                return self.stop("lua.vararg", a.pos, "... as arguments");
            }
            let v = self.val(a)?;
            if v.ty == Ty::Json {
                return self.stop("db.json_bind", a.pos, "tables can't be passed yet");
            }
            // nil and sprig.null both pass NULL.
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
                let l = self.f.local(&format!("{base}_{n}"), t);
                self.kinds.insert(l, Nul::SNull);
                (n.clone(), l)
            })
            .collect();
        let mut parts = vec![format!("CALL `{}`(", callee.entry())];
        for _ in 1..binds.len() {
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
        for (_, l) in &cols {
            self.facts.remove(&Key::L(*l));
        }
        self.reaches.insert(callee.module_id);
        self.reaches.extend(callee.reaches.iter().copied());
        self.depth = self.depth.max(callee.depth + 1);
        let dep = crate::link::CallDep::of(&callee);
        if !self.calls.contains(&dep) {
            self.calls.push(dep);
        }
        Ok(Var::Call { event, cols })
    }

    /// A key or value of `db.publish` as the engine tier writes it: text
    /// as it is, numbers and booleans as their text, `nil` and
    /// `sprig.null` as `NULL`.
    fn publish_text(&mut self, a: Option<&ast::Expr>) -> R<Expr> {
        let Some(a) = a else {
            return Ok(Expr::Lit(Literal::Null));
        };
        let v = self.val(a)?;
        Ok(match v.ty {
            Ty::Str => v.e,
            Ty::Null => Expr::Lit(Literal::Null),
            Ty::Int | Ty::Float | Ty::Bool => {
                if v.n == Nul::Never {
                    Expr::call(Builtin::ToStr, vec![v.e])
                } else {
                    Expr::Cond(
                        Box::new(is_null(v.e.clone())),
                        Box::new(Expr::Lit(Literal::Null)),
                        Box::new(Expr::call(Builtin::ToStr, vec![v.e])),
                    )
                }
            }
            Ty::Json => {
                return self.stop(
                    "lua.json",
                    a.pos,
                    "a table published as JSON text: Lua writes its members in no defined \
                     order, so publish text you build",
                );
            }
        })
    }

    /// `db.publish(stream, key, value, headers)` into the stream's outbox,
    /// with the gateway's checks first.
    fn db_publish(
        &mut self,
        x: &ast::Expr,
        args: &[ast::Expr],
        name: &str,
        out: &mut Block,
    ) -> R<Var> {
        let at = Self::at(x.pos);
        let stream = match args.first().map(|a| &a.k) {
            Some(ExprKind::Str(s)) => s.clone(),
            _ => return self.stop("db.publish", x.pos, "the stream's name must be a literal"),
        };
        if args.len() > 4 {
            return self.stop(
                "db.publish",
                x.pos,
                "publishing to a member tenant's stream needs the gateway",
            );
        }
        let target = match self.cat.stream(&stream) {
            None => {
                return self.stop(
                    "db.publish",
                    x.pos,
                    format!("{stream} isn't a stream of this database"),
                );
            }
            Some(Err(why)) => return self.stop("db.publish", x.pos, why),
            Some(Ok(t)) => t,
        };
        let key = self.publish_text(args.get(1))?;
        let value = self.publish_text(args.get(2))?;
        // Headers: a table of at most one text member (Lua orders more
        // members in no defined way), or nothing.
        let headers = match args.get(3).map(|a| (a, &a.k)) {
            None | Some((_, ExprKind::Nil)) => Expr::Lit(Literal::Null),
            Some((a, ExprKind::Table(fs))) if fs.len() <= 1 && Self::is_record(fs) => {
                let v = self.val(a)?;
                Expr::call(Builtin::JsonText, vec![v.e])
            }
            Some((a, _)) => {
                return self.stop(
                    "db.publish",
                    a.pos,
                    "headers must be a table of at most one member (or nil) to compile: Lua \
                     orders more members in no defined way",
                );
            }
        };
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
            binds: vec![key, value, headers],
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

/// What `pcall`'s second result is.
pub(crate) enum Second {
    /// The function's value when it ran, else the error's message.
    Val(V),
    /// Reading it is a reason: why.
    Unreadable(String),
}

impl L<'_, '_> {
    /// `pcall(f, args...)`: the arguments first (outside), then `f`
    /// inlined inside a [`Stmt::Try`], which catches what Lua's `pcall`
    /// does: the code's own errors and errors of data operations, not
    /// `sprig.reject` or budgets. The first result is whether `f` ran
    /// without an error.
    pub(super) fn pcall(&mut self, args: &[ast::Expr], pos: Pos, sole: bool) -> R<(V, Second)> {
        let Some((f, rest)) = args.split_first() else {
            return self.stop("lua.pcall", pos, "pcall() without a function");
        };
        let (func, depth) = match &f.k {
            ExprKind::Function(func) => (Rc::clone(func), self.scopes.len() - 1),
            ExprKind::Name(n) => match self.lookup(n) {
                Some(Var::Helper { func, depth }) => (func, depth),
                _ => {
                    return self.stop(
                        "lua.pcall",
                        f.pos,
                        "pcall of something other than a helper function runs in the engine tier",
                    );
                }
            },
            _ => {
                return self.stop(
                    "lua.pcall",
                    f.pos,
                    "pcall of something other than a helper function runs in the engine tier",
                );
            }
        };
        if func.vararg {
            return self.stop("lua.vararg", pos, "a helper of ...");
        }
        let mut vals = Vec::with_capacity(rest.len());
        for a in rest {
            if matches!(
                a.k,
                ExprKind::Call(..) | ExprKind::Method(..) | ExprKind::Vararg
            ) && std::ptr::eq(a, rest.last().unwrap_or(a))
                && rest.len() < func.params.len()
            {
                return self.stop(
                    "lua.multiple_results",
                    a.pos,
                    "a call's results filling several parameters",
                );
            }
            vals.push(self.val(a)?);
        }
        // The body: everything the inlined call needs, then its value.
        let outer = std::mem::take(&mut self.pre);
        let conds = std::mem::take(&mut self.conds);
        let entry = self.facts.clone();
        let r = self.inline_vals(&func, depth, &vals, pos, sole);
        let mut body = std::mem::replace(&mut self.pre, outer);
        self.conds = conds;
        let v = match r {
            Ok(v) => v,
            Err(Stop) => {
                self.facts = entry;
                return Err(Stop);
            }
        };
        self.tmp += 1;
        let n = self.tmp;
        let result = (v.ty != Ty::Null).then(|| {
            let key = self.wkey(pos, &format!("pcall{n}_value"));
            self.new_local(&format!("pcall{n}_value"), key, v.ty, v.n)
        });
        if let Some(l) = result {
            body.push(Stmt::Assign(l, v.e.clone()));
        }
        let mut l = |what: &str, ty: Ty| self.f.local(&format!("pcall{n}_{what}"), ty);
        let caught = crate::ir::Caught {
            flag: l("caught", Ty::Bool),
            code: l("code", Ty::Int),
            state: l("state", Ty::Str),
            text: l("text", Ty::Str),
            message: l("message", Ty::Str),
            name: l("name", Ty::Str),
        };
        let readable = super::catch_readable(&body);
        if let Some(r) = result {
            // A value from an earlier time round must not show through.
            self.needs(vec![Stmt::Assign(r, Expr::Lit(Literal::Null))]);
        }
        let mut changed = BTreeSet::new();
        super::assigned(&body, &mut changed);
        self.needs(vec![Stmt::Try { body, caught }]);
        // What held before holds after only for what the body didn't
        // assign (an error can come anywhere in it).
        self.facts = entry;
        self.facts
            .retain(|k| !matches!(k, Key::L(l) if changed.contains(l)));
        let ok = V {
            e: not(Expr::Local(caught.flag)),
            ty: Ty::Bool,
            n: Nul::Never,
        };
        let second = if readable {
            let (ty, value, kind) = match result {
                Some(r) => {
                    let t = self.f.local_def(r).ty;
                    let Some(ty) = t.join(Ty::Str) else {
                        return Ok((
                            ok,
                            Second::Unreadable(format!(
                                "the function's value ({t}) when it ran and the error's text \
                                 when it didn't: give the function a text result"
                            )),
                        ));
                    };
                    // Read only when the body ended normally, which set it.
                    (ty, Expr::Local(r), v.n)
                }
                None => (Ty::Str, Expr::Lit(Literal::Null), Nul::Nil),
            };
            Second::Val(V {
                e: Expr::Cond(
                    Box::new(Expr::Local(caught.flag)),
                    Box::new(Expr::Local(caught.message)),
                    Box::new(value),
                ),
                ty,
                n: kind,
            })
        } else {
            Second::Unreadable(
                "the error pcall caught, whose text can differ between MariaDB and the engine \
                 tier (a data operation's error, or Lua's own runtime error)"
                    .to_owned(),
            )
        };
        Ok((ok, second))
    }
}

/// `logic.max_call_depth`'s default: compiled calls nest at most this deep.
const MAX_CALL_DEPTH: u32 = 8;
