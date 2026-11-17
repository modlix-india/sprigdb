//! Lua functions without data access, compiled by the Lua front end and
//! run in the reference interpreter, against the same source on the
//! engine tier's Lua 5.1 runtime (`sprig_logic_js::Program`): both must
//! agree on the event, its fields (doubles to 1e-9) and the error class
//! of every call. Needs no database; the compiled routines themselves
//! are checked against the engine tier on MariaDB by `lua_calls.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    missing_docs
)]

mod lua_cases;

use std::sync::Arc;

use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, SprigError};
use sprig_logic_compile::interp::{self, Value};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, StaticColumns, lua};
use sprig_logic_js::bridge::NoDb;
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

fn close(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            x == y || (x - y).abs() <= 1e-9 * x.abs().max(y.abs())
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| close(v, w)))
        }
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| close(v, w))
        }
        _ => a == b,
    }
}

type Got = std::result::Result<(String, J), u16>;

fn same(a: &Got, b: &Got) -> bool {
    match (a, b) {
        (Ok((e1, r1)), Ok((e2, r2))) => e1 == e2 && close(r1, r2),
        (Err(x), Err(y)) => x == y,
        _ => false,
    }
}

fn ir_ty(t: ParamTy) -> Ty {
    match t {
        ParamTy::Int => Ty::Int,
        ParamTy::Float => Ty::Float,
        ParamTy::Bool => Ty::Bool,
        ParamTy::Json => Ty::Json,
        ParamTy::Str => Ty::Str,
    }
}

fn arg(v: &J, t: ParamTy) -> Value {
    match (v, t) {
        (J::Null, _) => Value::Null,
        (J::Number(n), ParamTy::Int) => Value::Int(n.as_i64().unwrap()),
        (J::Number(n), _) => Value::Float(n.as_f64().unwrap()),
        (J::Bool(b), _) => Value::Bool(*b),
        (J::String(s), _) => Value::Str(s.clone()),
        (other, _) => Value::Json(other.clone()),
    }
}

#[test]
fn compiled_lua_agrees_with_the_engine_tier() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut failures = Vec::new();
    let mut n = 0;
    for c in lua_cases::pure() {
        let sig = ScriptSig {
            params: c
                .params
                .iter()
                .map(|(n, t)| ((*n).to_owned(), *t))
                .collect(),
            scalar: c.scalar.is_some(),
        };
        let lsig = lua::Signature {
            params: c
                .params
                .iter()
                .map(|(n, t)| ((*n).to_owned(), ir_ty(*t)))
                .collect(),
            returns: c.scalar.map(ir_ty),
            row: None,
            on: None,
        };
        let compiled = lua::compile_with(
            c.name,
            c.src,
            &lsig,
            &StaticColumns::new(),
            &Options::default(),
        )
        .unwrap();
        if !compiled.compilable() {
            failures.push(format!(
                "{}: doesn't compile: {:?}",
                c.name, compiled.analysis.reasons
            ));
            continue;
        }
        let f = compiled.ir.as_ref().unwrap();
        let p = Program::new("lua", c.name, c.src, sig).unwrap();
        for args in &c.calls {
            n += 1;
            let iargs: Vec<Value> = args
                .iter()
                .zip(c.params)
                .map(|(v, (_, t))| arg(v, *t))
                .collect();
            let want: Got = {
                let host = Host {
                    db: Arc::new(NoDb),
                    outbound: Arc::new(NoOutbound),
                    default_db: None,
                    info: J::Null,
                };
                let a = p.coerce(c.name, args.clone()).unwrap();
                rt.block_on(p.run(a, host, Limits::default()))
                    .map(|o| (o.event, o.result))
                    .map_err(|e| class(&e))
            };
            let got: Got = interp::run(f, &iargs, &mut interp::NoDb, &interp::Limits::default())
                .map(|o| (o.event, o.result))
                .map_err(|e| e.code().map_or(0, sprig_common::SprigCode::number));
            let label = format!("{}{args:?}", c.name);
            if same(&want, &got) {
                println!("{label}: {want:?}");
            } else {
                failures.push(format!("{label}: lua {want:?} vs compiled {got:?}"));
            }
        }
    }
    println!("{n} calls, {} mismatches", failures.len());
    assert!(failures.is_empty(), "mismatches:\n{}", failures.join("\n"));
}

/// One write a validation or trigger sees: the operation, the row after
/// it and the row before it.
type Write = (&'static str, J, J);

/// Validations and after-commit triggers in Lua, against the table
/// `orders`: the engine tier gets `ctx` as test runs and trigger jobs give
/// it (`trigger {op, before, after}`, `row`, `old`); the compiled function
/// gets the columns it reads by name (`c`, `old_c`, `op`), as the
/// enforcement binds them.
#[test]
fn compiled_lua_validations_agree_with_the_engine_tier() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cols = StaticColumns::new().table(
        "orders",
        &[
            ("id", Ty::Int),
            ("product_id", Ty::Int),
            ("qty", Ty::Int),
            ("note", Ty::Str),
        ],
    );
    let row =
        |id: i64, qty: J, note: J| json!({"id": id, "product_id": 7, "qty": qty, "note": note});
    let writes: Vec<Write> = vec![
        ("insert", row(1, json!(5), json!("a")), J::Null),
        ("insert", row(2, json!(-1), J::Null), J::Null),
        ("insert", row(3, J::Null, json!("b")), J::Null),
        (
            "update",
            row(4, json!(2), json!("c")),
            row(4, json!(9), json!("c")),
        ),
        (
            "update",
            row(5, json!(9), J::Null),
            row(5, json!(2), json!("x")),
        ),
        (
            "update",
            row(6, json!(3), json!("big")),
            row(6, J::Null, json!("big")),
        ),
        (
            "delete",
            row(7, json!(1), json!("d")),
            row(7, json!(1), json!("d")),
        ),
    ];
    let cases: &[(&str, &str)] = &[
        (
            "v_rules",
            "local r = ctx.row
             if r.qty ~= sprig.null and r.qty < 0 then sprig.reject('negative qty') end
             if ctx.old ~= sprig.null and ctx.old.qty ~= sprig.null and ctx.old.qty > r.qty then
               sprig.reject('qty went down from ' .. ctx.old.qty)
             end
             if ctx.trigger.op == 'delete' then return end
             if r.note == sprig.null then return { flagged = true } end
             return { flagged = false, len = #r.note }",
        ),
        (
            "v_function_form",
            "return function (c)
               local t = c.trigger
               if t.after.qty == sprig.null then sprig.reject('qty is required') end
               if t.before == sprig.null then return { kind = 'new', qty = t.after.qty } end
               return { kind = t.op, delta = t.after.qty - t.before.qty }
             end",
        ),
        ("v_unchecked_old", "return { was = ctx.old.qty }"),
        (
            "v_helpers",
            "local function check(q)
               if q > 100 then error('too many: ' .. q) end
               return 'twice ' .. q * 2
             end
             local id = ctx.row.id
             if id == sprig.null then return {} end
             local ok, e = pcall(function () return check(id * 30) end)
             return { ok = ok, e = e }",
        ),
    ];
    let mut failures = Vec::new();
    let mut n = 0;
    let sig = ScriptSig {
        params: vec![("ctx".to_owned(), ParamTy::Json)],
        scalar: false,
    };
    for (name, src) in cases {
        let compiled = lua::compile_with(
            name,
            src,
            &lua::Signature::row_of("orders"),
            &cols,
            &Options::default(),
        )
        .unwrap();
        if !compiled.compilable() {
            failures.push(format!(
                "{name}: doesn't compile: {:?}",
                compiled.analysis.reasons
            ));
            continue;
        }
        let f = compiled.ir.as_ref().unwrap();
        let p = Program::new("lua", name, src, sig.clone()).unwrap();
        for (op, after, before) in &writes {
            n += 1;
            let ctx = json!({"trigger": {"op": op, "before": before, "after": after},
                             "row": after, "old": before});
            let want: Got = {
                let host = Host {
                    db: Arc::new(NoDb),
                    outbound: Arc::new(NoOutbound),
                    default_db: None,
                    info: J::Null,
                };
                rt.block_on(p.run(vec![ctx], host, Limits::default()))
                    .map(|o| (o.event, o.result))
                    .map_err(|e| class(&e))
            };
            let args: Vec<Value> = f
                .params
                .iter()
                .map(|q| {
                    let (img, col) = match q.name.strip_prefix("old_") {
                        Some(c) => (before, c),
                        None => (after, q.name.as_str()),
                    };
                    if q.name == "op" {
                        return Value::Str((*op).to_owned());
                    }
                    let t = match q.ty {
                        Ty::Int => ParamTy::Int,
                        Ty::Float => ParamTy::Float,
                        Ty::Bool => ParamTy::Bool,
                        Ty::Json => ParamTy::Json,
                        _ => ParamTy::Str,
                    };
                    arg(img.get(col).unwrap_or(&J::Null), t)
                })
                .collect();
            let got: Got = interp::run(f, &args, &mut interp::NoDb, &interp::Limits::default())
                .map(|o| (o.event, o.result))
                .map_err(|e| e.code().map_or(0, sprig_common::SprigCode::number));
            let label = format!("{name}[{op} {after} / {before}]");
            if same(&want, &got) {
                println!("{label}: {want:?}");
            } else {
                failures.push(format!("{label}: lua {want:?} vs compiled {got:?}"));
            }
        }
    }
    println!("{n} writes, {} mismatches", failures.len());
    assert!(failures.is_empty(), "mismatches:\n{}", failures.join("\n"));
}
