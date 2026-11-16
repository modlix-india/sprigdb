//! The JavaScript front end: what compiles, what goes to the engine tier
//! and why, and the lowered functions run in the reference interpreter.

use serde_json::json;

use super::*;
use crate::interp::{self, Value};
use crate::sql::StaticColumns;

fn cat() -> StaticColumns {
    StaticColumns::new()
        .table(
            "products",
            &[("id", Ty::Int), ("price", Ty::Float), ("stock", Ty::Int)],
        )
        .table(
            "orders",
            &[
                ("id", Ty::Int),
                ("product_id", Ty::Int),
                ("qty", Ty::Int),
                ("note", Ty::Str),
            ],
        )
}

fn sig(params: &[(&str, Ty)], returns: Option<Ty>) -> Signature {
    Signature {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        returns,
        row: None,
    }
}

fn js(src: &str, s: &Signature) -> Compiled {
    compile_with("f", src, s, &cat(), &Options::default()).unwrap()
}

fn reasons(c: &Compiled) -> Vec<String> {
    c.analysis.reasons.iter().map(|r| r.code.clone()).collect()
}

fn run(c: &Compiled, args: &[Value]) -> std::result::Result<(String, serde_json::Value), u16> {
    let f = c.ir.as_ref().unwrap();
    interp::run(f, args, &mut interp::NoDb, &interp::Limits::default())
        .map(|o| (o.event, o.result))
        .map_err(|e| e.code().map_or(0, sprig_common::SprigCode::number))
}

#[test]
fn branches_compile_and_follow_javascript() {
    let s = sig(&[("score", Ty::Int), ("vip", Ty::Bool)], None);
    let c = js(
        "if (score === null) return sprig.event('error', { message: 'no score' });
         if (score >= 90) return { grade: 'A', bonus: score + 5 };
         return { grade: score >= 50 ? 'pass' : 'fail', bonus: 0 };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(95), Value::Bool(true)]),
        Ok(("output".into(), json!({"grade": "A", "bonus": 100})))
    );
    assert_eq!(
        run(&c, &[Value::Int(40), Value::Bool(true)]),
        Ok(("output".into(), json!({"grade": "fail", "bonus": 0})))
    );
    assert_eq!(
        run(&c, &[Value::Null, Value::Bool(true)]),
        Ok(("error".into(), json!({"message": "no score"})))
    );
    let names: Vec<&str> =
        c.ir.as_ref()
            .unwrap()
            .events
            .iter()
            .map(|e| e.name.as_str())
            .collect();
    assert_eq!(names, ["output", "error"]);
}

#[test]
fn null_reads_as_zero_and_text_concatenates() {
    let s = sig(&[("a", Ty::Int), ("b", Ty::Float), ("t", Ty::Str)], None);
    let c = js(
        "let total = a * 2;
         total += b;
         const label = `n=${a}, t=` + t + '!';
         return { total, label, big: a >= 10, nt: !t };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    // `total` widened from an integer to a double for `+= b`.
    assert_eq!(
        run(&c, &[Value::Null, Value::Float(1.5), Value::Null]),
        Ok((
            "output".into(),
            json!({"total": 1.5, "label": "n=null, t=null!", "big": false, "nt": true})
        ))
    );
    assert_eq!(
        run(
            &c,
            &[Value::Int(12), Value::Null, Value::Str(String::new())]
        ),
        Ok((
            "output".into(),
            json!({"total": 24.0, "label": "n=12, t=!", "big": true, "nt": true})
        ))
    );
}

#[test]
fn loops_break_and_throw() {
    let s = sig(&[("n", Ty::Int)], Some(Ty::Int));
    let c = js(
        "export default function (n) {
           let sum = 0;
           for (let i = 0; i < 10; i++) {
             if (i >= n) break;
             sum = sum + i;
           }
           let k = 0;
           while (k < 3) { k++; }
           if (sum > 20) throw new Error('too big');
           return sum + k;
         }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(4)]),
        Ok(("output".into(), json!({"value": 9})))
    );
    assert_eq!(run(&c, &[Value::Int(9)]), Err(39865));
}

#[test]
fn switch_and_continue_compile() {
    let s = sig(&[("n", Ty::Int), ("kind", Ty::Str)], None);
    let c = js(
        "let sum = 0;
         for (let i = 0; i < n; i++) {
           if (i % 2 === 1) continue;
           sum = sum + i;
         }
         let k = 0;
         let odd = 0;
         while (k < n) {
           k++;
           if (k % 2 === 0) { continue; }
           odd++;
         }
         let label = '';
         switch (kind) {
           case 'a':
           case 'b':
             label = 'early';
             break;
           case 'z':
             return { sum, odd, label: 'last' };
           default:
             label = 'other';
         }
         let size = 'big';
         switch (n) {
           case 0: size = 'none'; break;
           case 1: size = 'one'; break;
         }
         return { sum, odd, label, size };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let text = c.plsql.as_ref().unwrap().statements.join("\n");
    assert!(text.contains("CONTINUE;"), "{text}");
    let r = |n: i64, k: &str| run(&c, &[Value::Int(n), Value::Str(k.into())]);
    // 0 + 2 + 4 = 6 below 6; odd k in 1..=6: 1, 3, 5.
    assert_eq!(
        r(6, "b"),
        Ok((
            "output".into(),
            json!({"sum": 6, "odd": 3, "label": "early", "size": "big"})
        ))
    );
    assert_eq!(
        r(1, "q"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 1, "label": "other", "size": "one"})
        ))
    );
    assert_eq!(
        r(0, "z"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 0, "label": "last"})
        ))
    );
    assert_eq!(
        r(0, "a"),
        Ok((
            "output".into(),
            json!({"sum": 0, "odd": 0, "label": "early", "size": "none"})
        ))
    );
}

#[test]
fn rows_are_guarded_unless_tested() {
    let s = sig(&[("productId", Ty::Int), ("qty", Ty::Int)], None);
    let quote = js(
        "import { db, sprig } from 'sprig:db';
         export default function (productId, qty) {
           const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
           if (p === null || !(p.stock >= qty))
             return sprig.event('error', { message: 'not enough stock' });
           return { total: p.price * qty + 40 };
         }",
        &s,
    );
    assert!(quote.compilable(), "{:?}", quote.analysis.reasons);
    let text = quote.plsql.as_ref().unwrap().statements.join("\n");
    assert!(!text.contains("cannot read properties"), "{text}");
    let unguarded = js(
        "const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);
         return { total: p.price * qty };",
        &s,
    );
    assert!(unguarded.compilable(), "{:?}", unguarded.analysis.reasons);
    let text = unguarded.plsql.as_ref().unwrap().statements.join("\n");
    assert!(
        text.contains("cannot read properties of null (reading ''price'')"),
        "{text}"
    );
    let conditional = js(
        "const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);
         return { total: qty > 1 ? p.price : 0 };",
        &s,
    );
    assert_eq!(reasons(&conditional), ["js.null_row"]);
}

#[test]
fn what_runs_in_the_engine_tier() {
    let s = sig(&[("x", Ty::Int), ("t", Ty::Str)], None);
    for (src, code) in [
        (
            "try { db.exec('DELETE FROM orders WHERE id = ?', [x]); } catch (e) { return {m: e.message}; } return {};",
            "js.catch_value",
        ),
        (
            "try { return {a: 1}; } catch (e) { return {e}; }",
            "js.catch_value",
        ),
        ("return { u: t.toUpperCase() };", "js.nullable_receiver"),
        ("return { r: fetch('http://x') };", "js.call"),
        (
            "outer: for (let i = 0; i < x; i++) { continue outer; } return {};",
            "js.statement",
        ),
        (
            "switch (x) { case 1: x = 2; case 2: return {a: x}; } return {};",
            "js.switch",
        ),
        (
            "switch (x) { case 1: if (t === 'a') break; return {a: 1}; } return {};",
            "js.switch",
        ),
        ("return { s: x == '1' };", "js.loose_equality"),
        ("return { s: t < 'b' };", "js.compare"),
        (
            "function h(a) { if (a > 0) return h(a - 1); return 0; } return { v: h(x) };",
            "js.recursion",
        ),
        (
            "function h(a) { let b = a; return b; } return { v: x > 0 && h(x) > 1 };",
            "js.helper",
        ),
        ("const h = (a) => a; return { v: h };", "js.helper"),
        (
            "import fs from 'fs'; export default function (x, t) { return {}; }",
            "js.import",
        ),
        (
            "const r = db.sql('SELECT * FROM products'); return { r };",
            "db.select_star",
        ),
        (
            "const q = 'SELECT 1'; db.exec(q); return {};",
            "db.dynamic_sql",
        ),
    ] {
        let c = js(src, &s);
        assert!(
            reasons(&c).contains(&code.to_owned()),
            "{src}: {:?}",
            c.analysis.reasons
        );
        assert!(!c.compilable());
    }
    // A wrong number of SQL arguments is an error, as for KIRun.
    let e = compile_with(
        "f",
        "db.exec('DELETE FROM orders WHERE id = ?', [x, x]); return {};",
        &s,
        &cat(),
        &Options::default(),
    )
    .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sprig_common::SprigCode::LogicSignatureMismatch)
    );
}

#[test]
fn for_of_reads_typed_columns() {
    let s = sig(&[("pid", Ty::Int)], None);
    let c = js(
        "let total = 0;
         let notes = '';
         for (const o of db.sql('SELECT qty, note FROM orders WHERE product_id = ? ORDER BY id', [pid])) {
           total += o.qty;
           notes = notes + o.note + ';';
         }
         return { total, notes };",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
}

// ---- validations and after-commit triggers (jsbind) ---------------------------

fn sales() -> StaticColumns {
    cat().table(
        "sales",
        &[
            ("id", Ty::Int),
            ("status", Ty::Str),
            ("total", Ty::Float),
            ("discount_pct", Ty::Float),
            ("qty", Ty::Int),
        ],
    )
}

fn bound(src: &str) -> sprig_common::Result<Compiled> {
    compile_with(
        "v",
        src,
        &Signature::row_of("sales"),
        &sales(),
        &Options::default(),
    )
}

/// Runs `c` with its parameters (the columns it reads) taken from `args`
/// by name; missing ones are `NULL`. An error is its message (the
/// interpreter reports 39863 and 39865 alike).
fn run_named(c: &Compiled, args: &[(&str, Value)]) -> std::result::Result<String, String> {
    let f = c.ir.as_ref().unwrap();
    let vals: Vec<Value> = f
        .params
        .iter()
        .map(|p| {
            args.iter()
                .find(|(n, _)| *n == p.name)
                .map_or(Value::Null, |(_, v)| v.clone())
        })
        .collect();
    interp::run(f, &vals, &mut interp::NoDb, &interp::Limits::default())
        .map(|o| o.event)
        .map_err(|e| e.message)
}

fn rejected(m: &str) -> std::result::Result<String, String> {
    Err(m.to_owned())
}

fn param_names(c: &Compiled) -> Vec<String> {
    let mut v: Vec<String> =
        c.ir.as_ref()
            .unwrap()
            .params
            .iter()
            .map(|p| p.name.clone())
            .collect();
    v.sort();
    v
}

#[test]
fn the_books_javascript_validation_compiles() {
    // A row validation in the `(row, old)` form, as users write it.
    let c = bound(
        r#"export default function (row, old) {
  if (row.discount_pct > 0) {
    const limit = row.total >= 1000 ? 20 : 10;
    if (row.discount_pct > limit)
      sprig.reject(`discount ${row.discount_pct}% is above ${limit}%`);
  }
  if (old !== null && old.status === "shipped" && row.total !== old.total)
    sprig.reject("the total of a shipped order can't change");
}"#,
    )
    .unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    // The parameters are the columns it reads, and op for old's presence.
    assert_eq!(
        param_names(&c),
        ["discount_pct", "old_status", "old_total", "op", "total"]
    );
    let ins = |d: Value, t: f64| {
        run_named(
            &c,
            &[
                ("discount_pct", d),
                ("total", Value::Float(t)),
                ("op", Value::Str("insert".into())),
            ],
        )
    };
    assert_eq!(
        ins(Value::Float(25.0), 600.0),
        rejected("discount 25% is above 10%")
    );
    assert_eq!(
        ins(Value::Float(25.0), 1600.0),
        rejected("discount 25% is above 20%")
    );
    assert_eq!(ins(Value::Float(15.0), 1600.0), Ok("output".into()));
    assert_eq!(ins(Value::Float(5.0), 600.0), Ok("output".into()));
    // null > 0 is false in JavaScript, as NULL > 0 skips the branch.
    assert_eq!(ins(Value::Null, 600.0), Ok("output".into()));
    let upd = |status: &str, before: f64, after: f64| {
        run_named(
            &c,
            &[
                ("discount_pct", Value::Null),
                ("total", Value::Float(after)),
                ("op", Value::Str("update".into())),
                ("old_status", Value::Str(status.into())),
                ("old_total", Value::Float(before)),
            ],
        )
    };
    assert_eq!(
        upd("shipped", 10.0, 12.0),
        rejected("the total of a shipped order can't change")
    );
    assert_eq!(upd("shipped", 10.0, 10.0), Ok("output".into()));
    assert_eq!(upd("new", 10.0, 12.0), Ok("output".into()));
}

#[test]
fn a_one_message_javascript_validation_is_a_check() {
    let c = bound(
        "export default function (row) {
           if (row.qty <= 0) sprig.reject('qty must be positive');
         }",
    )
    .unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(param_names(&c), ["qty"]);
    let (cond, msg) = crate::pure::reject_check(
        c.ir.as_ref().unwrap(),
        &[Some(crate::pure::Sql {
            text: "`qty`".into(),
            ty: Ty::Int,
        })],
    )
    .unwrap();
    assert_eq!(msg, "qty must be positive");
    assert!(cond.contains("`qty`"), "{cond}");
    // Two messages: triggers, not a CHECK.
    let two = bound(
        "if (row.qty <= 0) sprig.reject('none');
         if (row.qty > 100) sprig.reject('too many');",
    )
    .unwrap();
    assert!(two.compilable(), "{:?}", two.analysis.reasons);
    let e = crate::pure::reject_check(
        two.ir.as_ref().unwrap(),
        &[Some(crate::pure::Sql {
            text: "`qty`".into(),
            ty: Ty::Int,
        })],
    )
    .unwrap_err();
    assert!(e.contains("more than one message"), "{e}");
}

#[test]
fn the_three_ways_of_seeing_the_row_agree() {
    let forms = [
        // A test run's ctx.
        "export default function (ctx) {
           if (ctx.trigger.before !== null && ctx.trigger.before.qty > ctx.trigger.after.qty)
             sprig.reject('qty only grows');
         }",
        // KIRun's names.
        "export default function (qty, old_qty, op) {
           if (op === 'update' && old_qty > qty) sprig.reject('qty only grows');
         }",
        // A body alone.
        "if (old && old.qty > row.qty) sprig.reject('qty only grows');",
    ];
    for src in forms {
        let c = bound(src).unwrap();
        assert!(c.compilable(), "{src}: {:?}", c.analysis.reasons);
        assert_eq!(param_names(&c), ["old_qty", "op", "qty"], "{src}");
        let r = |op: &str, before: Value, after: i64| {
            run_named(
                &c,
                &[
                    ("op", Value::Str(op.into())),
                    ("old_qty", before),
                    ("qty", Value::Int(after)),
                ],
            )
        };
        assert_eq!(
            r("update", Value::Int(5), 3),
            rejected("qty only grows"),
            "{src}"
        );
        assert_eq!(r("update", Value::Int(5), 7), Ok("output".into()), "{src}");
        assert_eq!(r("insert", Value::Null, 3), Ok("output".into()), "{src}");
    }
}

#[test]
fn old_is_null_for_an_insert() {
    // Reading a column of old without testing it: JavaScript's TypeError
    // on an insert, as QuickJS would throw.
    let c = bound("if (old.status === 'shipped') sprig.reject('shipped');").unwrap();
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let r = |op: &str| {
        run_named(
            &c,
            &[
                ("op", Value::Str(op.into())),
                ("old_status", Value::Str("shipped".into())),
            ],
        )
    };
    assert_eq!(
        r("insert"),
        rejected("TypeError: cannot read properties of null (reading 'status')")
    );
    assert_eq!(r("update"), rejected("shipped"));
    // A column the table lacks, and a parameter that names nothing.
    let c = bound("if (row.nope > 1) sprig.reject('x');").unwrap();
    assert_eq!(reasons(&c), ["js.member"]);
    let Err(e) = bound("export default function (row, extra) { return; }") else {
        panic!("an unknown parameter compiled");
    };
    assert_eq!(
        e.code(),
        Some(sprig_common::SprigCode::LogicSignatureMismatch)
    );
}

#[test]
fn nondeterministic_calls_are_reasons() {
    let s = sig(&[("price", Ty::Float)], Some(Ty::Float));
    for (src, what) in [
        ("return price * Math.random();", "Math.random()"),
        ("return Date.now() + price;", "Date.now()"),
        ("const d = new Date(); return price;", "new Date()"),
    ] {
        let c = js(src, &s);
        assert_eq!(reasons(&c), ["js.nondeterministic"], "{src}");
        assert!(c.analysis.reasons[0].detail.contains(what), "{src}");
    }
    // A local named Math is the code's own.
    let c = js(
        "const Math = { random: 1 }; return price;",
        &sig(&[("price", Ty::Float)], Some(Ty::Float)),
    );
    assert!(!reasons(&c).contains(&"js.nondeterministic".to_owned()));
}

#[test]
fn a_javascript_function_is_a_computed_expression() {
    let s = sig(&[("price", Ty::Float), ("stock", Ty::Int)], Some(Ty::Float));
    let c = js(
        "export default function (price, stock) {
           if (stock <= 0) return 0;
           return price * stock - 1;
         }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let e = crate::pure::computed(
        c.ir.as_ref().unwrap(),
        &[
            Some(crate::pure::Sql {
                text: "(`price`)".into(),
                ty: Ty::Float,
            }),
            Some(crate::pure::Sql {
                text: "(`stock`)".into(),
                ty: Ty::Int,
            }),
        ],
    )
    .unwrap();
    assert!(e.text.starts_with("(CASE WHEN"), "{}", e.text);
    assert!(
        e.text.contains("`price`") && e.text.contains("`stock`"),
        "{}",
        e.text
    );
}

/// A database for the interpreter: `INSERT` fails with a duplicate key for
/// id 1, `SELECT` finds nothing.
struct FakeDb;

impl interp::Db for FakeDb {
    fn query(&mut self, _: &str, _: &[Value]) -> sprig_common::Result<Vec<Vec<Option<String>>>> {
        Ok(Vec::new())
    }
    fn exec(&mut self, sql: &str, binds: &[Value]) -> sprig_common::Result<u64> {
        if sql.starts_with("INSERT") && binds.first() == Some(&Value::Int(1)) {
            return Err(sprig_common::SprigError::backend(
                1062,
                *b"23000",
                "Duplicate entry '1' for key 'PRIMARY'",
            ));
        }
        Ok(1)
    }
}

fn run_db(c: &Compiled, args: &[Value]) -> std::result::Result<(String, serde_json::Value), u16> {
    let f = c.ir.as_ref().unwrap();
    interp::run(f, args, &mut FakeDb, &interp::Limits::default())
        .map(|o| (o.event, o.result))
        .map_err(|e| match e.kind {
            sprig_common::ErrorKind::Sprig(c) => c.number(),
            sprig_common::ErrorKind::Backend { errno, .. }
            | sprig_common::ErrorKind::Standard { errno, .. } => errno,
        })
}

fn compiles(src: &str, s: &Signature) -> Compiled {
    let c = js(src, s);
    assert!(c.compilable(), "{src}: {:#?}", c.analysis.reasons);
    c
}

#[test]
fn try_catch_sees_the_error_objects_name_and_message() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "let r = 'none';
         try {
           if (x < 0) throw new Error('neg ' + x);
           if (x === 0) throw new RangeError('zero');
           r = 'ok';
         } catch (e) {
           r = e.name + ':' + e.message;
         }
         return { r };",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(-2)]),
        Ok(("output".into(), json!({"r": "Error:neg -2"})))
    );
    assert_eq!(
        run(&c, &[Value::Int(0)]),
        Ok(("output".into(), json!({"r": "RangeError:zero"})))
    );
    assert_eq!(
        run(&c, &[Value::Int(5)]),
        Ok(("output".into(), json!({"r": "ok"})))
    );
    let p = c.plsql.unwrap().procedure;
    assert!(p.contains("GET DIAGNOSTICS CONDITION 1"), "{p}");
    assert!(p.contains("v__src INT := 0;"), "{p}");
}

#[test]
fn finally_runs_on_every_way_out() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "let log = '';
         function step(t) { log = log + t; }
         for (let i = 0; i < 3; i++) {
           try {
             step('t' + i);
             if (i === 1) continue;
             if (i === 2) break;
             step('a');
           } finally {
             step('f');
           }
           step('n');
         }
         try {
           return { log, v: x };
         } finally {
           log = log + 'X';
         }",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(1)]),
        Ok(("output".into(), json!({"log": "t0afnt1ft2f", "v": 1})))
    );
}

#[test]
fn finally_overrides_and_swallows_and_errors_go_on() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "function f1() { try { return 1; } finally { return 2; } }
         function f2(a) {
           try { if (a > 0) throw new Error('x'); return 'n'; }
           finally { if (a > 1) return 'swallowed'; }
         }
         let out = '';
         try { out = f2(x); } catch (e) { out = 'caught ' + e.message; }
         return { a: f1(), b: out };",
        &s,
    );
    for (x, b) in [(0, "n"), (1, "caught x"), (2, "swallowed")] {
        assert_eq!(
            run(&c, &[Value::Int(x)]),
            Ok(("output".into(), json!({"a": 2, "b": b}))),
            "x = {x}"
        );
    }
}

#[test]
fn a_caught_error_thrown_again_reaches_the_outer_catch() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "try {
           try { throw new TypeError('inner'); }
           catch (e) { if (x > 0) throw e; return { w: 'kept ' + e.name }; }
         } catch (e2) { return { w: 'outer ' + e2.name + ' ' + e2.message }; }",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(0)]),
        Ok(("output".into(), json!({"w": "kept TypeError"})))
    );
    assert_eq!(
        run(&c, &[Value::Int(1)]),
        Ok(("output".into(), json!({"w": "outer TypeError inner"})))
    );
    // Uncaught, it is the function's error.
    let c = compiles(
        "try { throw new Error('a'); } catch (e) { if (x > 0) throw e; } return {};",
        &s,
    );
    assert_eq!(run(&c, &[Value::Int(1)]), Err(39865));
    assert_eq!(run(&c, &[Value::Int(0)]), Ok(("output".into(), json!({}))));
}

#[test]
fn reject_and_budgets_are_not_caught() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "try { if (x === 1) sprig.reject('no'); } catch (e) { return { c: 1 }; }
         return { c: 0 };",
        &s,
    );
    assert_eq!(run(&c, &[Value::Int(1)]), Err(39863));
    assert_eq!(
        run(&c, &[Value::Int(0)]),
        Ok(("output".into(), json!({"c": 0})))
    );
    let c = compiles(
        "let n = 0;
         try { while (true) { n++; } } catch (e) { return { c: 1 }; } finally { n = 0; }
         return { n };",
        &s,
    );
    assert_eq!(run(&c, &[Value::Int(0)]), Err(39864));
    // Arithmetic JavaScript doesn't throw on isn't caught either.
    let c = compiles(
        "let q = 0; try { q = 10 % x; } catch (e) { return { c: 1 }; } return { q };",
        &s,
    );
    assert_eq!(run(&c, &[Value::Int(0)]), Err(39866));
}

#[test]
fn a_null_row_and_database_errors_are_caught() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "const p = db.sqlOne('SELECT stock FROM products WHERE id = ?', [x]);
         try { return { s: p.stock }; } catch (e) { return { n: e.name, m: e.message }; }",
        &s,
    );
    assert_eq!(
        run_db(&c, &[Value::Int(7)]),
        Ok((
            "output".into(),
            json!({"n": "TypeError", "m": "cannot read property 'stock' of null"})
        ))
    );
    let c = compiles(
        "let ok = true;
         try { db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [x, 1, 1]); }
         catch (e) { ok = false; }
         return { ok };",
        &s,
    );
    assert_eq!(
        run_db(&c, &[Value::Int(1)]),
        Ok(("output".into(), json!({"ok": false})))
    );
    assert_eq!(
        run_db(&c, &[Value::Int(2)]),
        Ok(("output".into(), json!({"ok": true})))
    );
    // Through a `finally`, a database error keeps its number.
    let c = compiles(
        "let n = 0;
         try { db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [x, 1, 1]); }
         finally { n = 1; }
         return { n };",
        &s,
    );
    assert_eq!(run_db(&c, &[Value::Int(1)]), Err(1062));
    assert_eq!(
        run_db(&c, &[Value::Int(2)]),
        Ok(("output".into(), json!({"n": 1})))
    );
}

#[test]
fn do_while_runs_once_then_tests() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "let i = 0; let s = 0;
         do { i++; if (i === 2) continue; s += i; } while (i < x);
         return { i, s };",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(0)]),
        Ok(("output".into(), json!({"i": 1, "s": 1})))
    );
    assert_eq!(
        run(&c, &[Value::Int(3)]),
        Ok(("output".into(), json!({"i": 3, "s": 4})))
    );
}

#[test]
fn helpers_are_inlined_with_their_closures() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = compiles(
        "const tax = (p) => p * 0.25;
         function net(p, q) { const g = p * q; return g + tax(g); }
         function label(n) { if (n > 100) return 'big'; if (n > 10) return 'mid'; return 'small'; }
         let total = 0;
         for (let i = 1; i <= x; i++) total += net(i, 2);
         if (label(total) === 'mid') total = total + 1000;
         return { total, l: label(total), t: tax(4) };",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(3)]),
        Ok((
            "output".into(),
            json!({"total": 1015.0, "l": "big", "t": 1.0})
        ))
    );
    assert_eq!(
        run(&c, &[Value::Int(1)]),
        Ok((
            "output".into(),
            json!({"total": 2.5, "l": "small", "t": 1.0})
        ))
    );
    // A helper in a loop's test runs before each test.
    let c = compiles(
        "function more(i) { if (i < x) return true; return false; }
         let i = 0; while (more(i)) { i++; } return { i };",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(4)]),
        Ok(("output".into(), json!({"i": 4})))
    );
    // Module form: helpers beside the default export see module scope only.
    let c = compiles(
        "import { db } from 'sprig:db';
         const twice = (n) => n * 2;
         function pick(a, b) { if (a > b) { return a; } return b; }
         export default function (x) { return { v: pick(twice(x), 10) }; }",
        &s,
    );
    assert_eq!(
        run(&c, &[Value::Int(7)]),
        Ok(("output".into(), json!({"v": 14})))
    );
    assert_eq!(
        run(&c, &[Value::Int(2)]),
        Ok(("output".into(), json!({"v": 10})))
    );
}

/// The compiler's view with one callee and two streams.
struct Linked {
    cols: StaticColumns,
}

impl crate::sql::Columns for Linked {
    fn column(&self, table: &str, column: &str) -> Option<Ty> {
        self.cols.column(table, column)
    }
    fn has_table(&self, table: &str) -> bool {
        self.cols.has_table(table)
    }
    fn callee(&self, name: &str) -> Option<std::result::Result<crate::link::Callee, String>> {
        match name {
            "price" => Some(Ok(crate::link::Callee {
                module_id: 31,
                name: "price".into(),
                params: vec![Ty::Int],
                fields: vec![("price".into(), Ty::Float), ("ok".into(), Ty::Int)],
                reads: vec!["products".into()],
                writes: Vec::new(),
                reaches: vec![40],
                depth: 1,
            })),
            "deep" => Some(Ok(crate::link::Callee {
                module_id: 32,
                name: "deep".into(),
                params: Vec::new(),
                fields: Vec::new(),
                reads: Vec::new(),
                writes: Vec::new(),
                reaches: Vec::new(),
                depth: 8,
            })),
            "engine_only" => Some(Err("engine_only runs in the engine tier".into())),
            _ => None,
        }
    }
    fn stream(&self, name: &str) -> Option<std::result::Result<crate::link::StreamTarget, String>> {
        (name == "orders_s").then(|| {
            Ok(crate::link::StreamTarget {
                name: "orders_s".into(),
                outbox: "__sprig_eo_5".into(),
                max_bytes: 10,
                compact: true,
            })
        })
    }
}

fn linked(src: &str, module: u64) -> Compiled {
    let opts = Options {
        module_id: module,
        ..Options::default()
    };
    compile_with(
        "f",
        src,
        &sig(&[("x", Ty::Int)], None),
        &Linked { cols: cat() },
        &opts,
    )
    .unwrap()
}

/// Answers a call of `price` and records statements.
#[derive(Default)]
struct CallDb {
    seen: Vec<String>,
}

impl interp::Db for CallDb {
    fn query(&mut self, _: &str, _: &[Value]) -> sprig_common::Result<Vec<Vec<Option<String>>>> {
        Ok(Vec::new())
    }
    fn exec(&mut self, sql: &str, _: &[Value]) -> sprig_common::Result<u64> {
        self.seen.push(sql.to_owned());
        Ok(1)
    }
    fn call(
        &mut self,
        sql: &str,
        binds: &[Value],
    ) -> sprig_common::Result<(String, Option<String>)> {
        self.seen.push(sql.to_owned());
        Ok(match binds.first() {
            Some(Value::Int(7)) => (
                "output".into(),
                Some(r#"{"price": 2.5, "ok": true}"#.into()),
            ),
            _ => ("missing".into(), Some("{}".into())),
        })
    }
}

#[test]
fn db_call_compiles_into_the_callee_call_entry() {
    let c = linked(
        "const r = db.call('price', x);
         return { e: r.event, p: r.fields.price, ok: r.fields.ok, all: r };",
        9,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let p = c.plsql.as_ref().unwrap();
    assert!(
        p.procedure
            .contains("CALL `__sprig_l31_call`((p_x), v_r_event, v_r_result);"),
        "{}",
        p.procedure
    );
    assert!(
        p.procedure
            .contains("v_r_price := CAST(JSON_VALUE(v_r_result, '$.\"price\"') AS DOUBLE);"),
        "{}",
        p.procedure
    );
    assert_eq!(c.analysis.reads, vec!["products".to_owned()]);
    assert_eq!(c.analysis.calls.len(), 1);
    assert_eq!(c.analysis.calls[0].module, 31);
    assert_eq!(c.analysis.reaches, vec![31, 40]);
    assert_eq!(c.analysis.call_depth, 2);
    let f = c.ir.as_ref().unwrap();
    let mut db = CallDb::default();
    let o = interp::run(f, &[Value::Int(7)], &mut db, &interp::Limits::default()).unwrap();
    assert_eq!(
        o.result,
        json!({"e": "output", "p": 2.5, "ok": 1,
               "all": {"event": "output", "fields": {"price": 2.5, "ok": 1}}})
    );
    assert_eq!(db.seen, vec!["CALL `__sprig_l31_call`(?".to_owned()]);
    let o = interp::run(f, &[Value::Int(8)], &mut db, &interp::Limits::default()).unwrap();
    assert_eq!(o.result["e"], json!("missing"));
    assert_eq!(o.result["p"], json!(null));
}

#[test]
fn db_call_stays_in_the_engine_tier_when_it_must() {
    for (src, module, code) in [
        ("db.call('price', x); return {};", 40, "js.recursion"),
        ("db.call('price', x); return {};", 31, "js.recursion"),
        ("db.call('deep'); return {};", 9, "db.call"),
        ("db.call('engine_only', x); return {};", 9, "db.call"),
        ("db.call('nothing', x); return {};", 9, "db.call"),
        (
            "const r = db.call('price', x); return { n: r.fields.nope };",
            9,
            "js.member",
        ),
        ("db.call('price'); return {};", 9, "db.call"),
        ("const n = 'price'; db.call(n, x); return {};", 9, "db.call"),
        ("db.commitOffset('g', 's', 0, 1); return {};", 9, "js.db"),
        ("db.mq.ack('t'); return {};", 9, "*"),
        (
            "db.publish('elsewhere', 'k', 'v'); return {};",
            9,
            "db.publish",
        ),
        (
            "db.publish('orders_s', 'k', 'v', ['h']); return {};",
            9,
            "db.publish",
        ),
        (
            "db.publish('orders_s', 'k', 'v', null, {tenant: 'a'}); return {};",
            9,
            "db.publish",
        ),
    ] {
        let c = linked(src, module);
        assert!(!c.compilable(), "{src}");
        assert!(
            code == "*" || c.analysis.reasons.iter().any(|r| r.code == code),
            "{src}: {:?}",
            c.analysis.reasons
        );
    }
}

#[test]
fn db_publish_writes_the_outbox_after_the_gateway_checks() {
    let c = linked(
        "const a = db.publish('orders_s', x, { id: x }, { h: 'v' });
         db.publish('orders_s', null, 'gone');
         return { a: a.affected };",
        9,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let p = c.plsql.as_ref().unwrap();
    assert!(
        p.procedure
            .contains("INSERT INTO `__sprig_eo_5` (k, v, headers) VALUES ("),
        "{}",
        p.procedure
    );
    assert!(p.procedure.contains("fail(39944"), "{}", p.procedure);
    assert!(p.procedure.contains("fail(39955"), "{}", p.procedure);
    assert!(p.procedure.contains("JSON_COMPACT("), "{}", p.procedure);
    assert_eq!(c.analysis.writes, vec!["orders_s".to_owned()]);
    let f = c.ir.as_ref().unwrap();
    // The first publish fits (key "7", value {"id":7}, headers {"h":"v"});
    // the second has no key on a compacted stream.
    let mut db = CallDb::default();
    let e = interp::run(f, &[Value::Int(7)], &mut db, &interp::Limits::default()).unwrap_err();
    assert_eq!(e.code().map(sprig_common::SprigCode::number), Some(39955));
    assert_eq!(db.seen.len(), 1);
    // A value of 11 bytes is above the stream's 10.
    let e = interp::run(
        f,
        &[Value::Int(1_234_567_890)],
        &mut CallDb::default(),
        &interp::Limits::default(),
    )
    .unwrap_err();
    assert_eq!(e.code().map(sprig_common::SprigCode::number), Some(39944));
}
