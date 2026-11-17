//! The Lua front end: what compiles, what goes to the engine tier and
//! why, and the lowered functions run in the reference interpreter.

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
        on: None,
    }
}

fn lua(src: &str, s: &Signature) -> Compiled {
    compile_with("f", src, s, &cat(), &Options::default()).unwrap()
}

fn reasons(c: &Compiled) -> Vec<String> {
    c.analysis.reasons.iter().map(|r| r.code.clone()).collect()
}

type Got = std::result::Result<(String, serde_json::Value), u16>;

fn run(c: &Compiled, args: &[Value]) -> Got {
    let f = c.ir.as_ref().unwrap();
    interp::run(f, args, &mut interp::NoDb, &interp::Limits::default())
        .map(|o| (o.event, o.result))
        .map_err(|e| e.code().map_or(0, sprig_common::SprigCode::number))
}

#[allow(clippy::unnecessary_wraps)]
fn ok(v: serde_json::Value) -> Got {
    Ok(("output".into(), v))
}

#[test]
fn branches_follow_lua() {
    let s = sig(&[("score", Ty::Int), ("vip", Ty::Bool)], None);
    let c = lua(
        "if score == sprig.null then return sprig.event('error', { message = 'no score' }) end
         if score >= 90 then return { grade = 'A', bonus = score + 5 } end
         return { grade = score >= 50 and 'pass' or 'fail', bonus = 0 }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(95), Value::Bool(true)]),
        ok(json!({"grade": "A", "bonus": 100}))
    );
    assert_eq!(
        run(&c, &[Value::Int(40), Value::Bool(true)]),
        ok(json!({"grade": "fail", "bonus": 0}))
    );
    assert_eq!(
        run(&c, &[Value::Null, Value::Bool(true)]),
        Ok(("error".into(), json!({"message": "no score"})))
    );
}

#[test]
fn sprig_null_is_true_and_nil_is_false() {
    let s = sig(&[("a", Ty::Int), ("b", Ty::Bool)], None);
    let c = lua(
        "local t = 0
         if a then t = t + 1 end          -- sprig.null is true
         if b then t = t + 10 end         -- false is false, sprig.null true
         local n = nil
         if n then t = t + 100 end
         local x = a == nil               -- never: a NULL argument is sprig.null
         return { t = t, x = x, y = a == sprig.null, z = not b }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Null, Value::Null]),
        ok(json!({"t": 11, "x": false, "y": true, "z": false}))
    );
    assert_eq!(
        run(&c, &[Value::Int(0), Value::Bool(false)]),
        ok(json!({"t": 1, "x": false, "y": false, "z": true}))
    );
}

#[test]
fn arithmetic_on_null_is_lua_s_error() {
    let s = sig(&[("a", Ty::Int), ("b", Ty::Float)], None);
    let c = lua(
        "local s = a + b
         return { s = s, m = a % 3, n = -7 % 3, p = 2 ^ 3, d = a / 2, q = math.floor(a / 2) }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(run(&c, &[Value::Null, Value::Float(1.0)]), Err(39865));
    assert_eq!(
        run(&c, &[Value::Int(-5), Value::Float(0.5)]),
        ok(json!({"s": -4.5, "m": 1, "n": 2, "p": 8.0, "d": -2.5, "q": -3}))
    );
}

#[test]
fn loops_and_text() {
    let s = sig(&[("n", Ty::Int), ("name", Ty::Str)], Some(Ty::Str));
    let c = lua(
        "local out = ''
         for i = 1, n do
           if i > 4 then break end
           out = out .. i .. ','
         end
         for i = 10, 1, -3 do out = out .. i end
         local k = 0
         while k < 3 do k = k + 1 end
         repeat local j = k; k = k + 1 until j >= 5
         return out .. '|' .. k .. '|' .. #name .. '|' .. name:upper()",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(6), Value::Str("Zoë".into())]),
        ok(json!({"value": "1,2,3,4,10741|6|4|ZOë"}))
    );
}

#[test]
fn helpers_records_and_errors() {
    let s = sig(&[("x", Ty::Int)], None);
    let c = lua(
        "local function double(v) return v * 2 end
         local function classify(v)
           if v < 0 then return 'neg' end
           if v == 0 then return 'zero' end
           return 'pos'
         end
         local r = { kind = classify(x), twice = double(x) }
         r.extra = r.twice + 1
         if x > 100 then error('too big') end
         assert(x ~= 42, 'not that one')
         return r",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(5)]),
        ok(json!({"kind": "pos", "twice": 10, "extra": 11}))
    );
    assert_eq!(
        run(&c, &[Value::Int(-1)]),
        ok(json!({"kind": "neg", "twice": -2, "extra": -1}))
    );
    assert_eq!(run(&c, &[Value::Int(101)]), Err(39865));
    assert_eq!(run(&c, &[Value::Int(42)]), Err(39865));
}

#[test]
fn function_form_and_param_assignment() {
    let s = sig(&[("a", Ty::Int), ("b", Ty::Int)], Some(Ty::Int));
    let c = lua(
        "return function (x, y)
           x = x + 1
           return x * y
         end",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Int(2), Value::Int(5)]),
        ok(json!({"value": 15}))
    );
}

#[test]
fn engine_tier_reasons() {
    let s = sig(&[("a", Ty::Int), ("t", Ty::Str)], None);
    for (src, code) in [
        ("return { v = t + 1 }", "lua.dynamic_types"),
        ("for k, v in pairs({}) do end", "lua.pairs"),
        ("return { v = math.random() }", "lua.nondeterministic"),
        ("local x = t:sub(1, 2)", "lua.string"),
        ("local x = string.find(t, 'a')", "lua.pattern"),
        ("g = 1", "lua.global"),
        (
            "local function f(n) return f(n) end return { v = f(a) }",
            "lua.recursion",
        ),
        ("local x = ... return {}", "lua.vararg"),
        ("setmetatable({}, {})", "lua.metatable"),
        ("local c = coroutine.create(print)", "lua.coroutine"),
        (
            "local x = {} if a > 0 then x.v = 1 end return x",
            "lua.nil_field",
        ),
        ("local s = 'x' .. true", "lua.concat"),
        ("return { v = 1 < 'a' }", "lua.compare"),
        ("local x = (", "lua.parse"),
        ("db.mq.ack('t')", "lua.db"),
        ("db.commitOffset('g', 's', 0, 1)", "lua.db"),
        ("local r = db.call('nope', a)", "db.call"),
        ("db.publish('s', a, 'v')", "db.publish"),
        ("local x = fetch('http://x')", "lua.fetch"),
        ("local ok = pcall(print, 1)", "lua.pcall"),
        ("local ok = xpcall(function() end, print)", "lua.pcall"),
        (
            "local ok, e = pcall(function() db.exec('DELETE FROM orders') end) return { e = e }",
            "lua.pcall_value",
        ),
    ] {
        let c = lua(src, &s);
        assert!(
            reasons(&c).iter().any(|r| r == code),
            "{src}: {:?}",
            c.analysis.reasons
        );
    }
}

#[test]
fn checks_in_conditional_operands_run_only_there() {
    // `b` is read only when `a` is false: its check must not fail a call
    // where `a` is true.
    let s = sig(&[("a", Ty::Bool), ("b", Ty::Int)], None);
    let c = lua(
        "if a or b > 3 then return { r = 'yes' } end
         local v = a and 0 or b + 1
         return { r = 'no', v = v }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(
        run(&c, &[Value::Bool(true), Value::Null]),
        ok(json!({"r": "yes"}))
    );
    assert_eq!(run(&c, &[Value::Bool(false), Value::Null]), Err(39865));
    assert_eq!(
        run(&c, &[Value::Bool(false), Value::Int(2)]),
        ok(json!({"r": "no", "v": 3}))
    );
}

#[test]
fn helpers_that_change_the_caller_s_locals_stand_alone() {
    let s = sig(&[("n", Ty::Int)], None);
    let ok_src = "local count = 0
                  local function bump(by) count = count + by return count end
                  bump(n)
                  local r = bump(1)
                  return { count = count, r = r }";
    let c = lua(ok_src, &s);
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    assert_eq!(run(&c, &[Value::Int(4)]), ok(json!({"count": 5, "r": 5})));
    // Inside an expression, the call would run before `count` is read.
    let c = lua(
        "local count = 0
         local function bump(by) count = count + by return count end
         local r = count + bump(1)
         return { r = r }",
        &s,
    );
    assert_eq!(reasons(&c).first().map(String::as_str), Some("lua.helper"));
}

#[test]
fn rows_compare_with_sprig_null() {
    let s = sig(&[("id", Ty::Int)], None);
    let c = lua(
        "local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', id)
         if p == nil then return { r = 'never' } end
         if not p then return { r = 'never' } end
         if p == sprig.null then return { r = 'none' } end
         return { r = 'some', s = p.stock }",
        &s,
    );
    assert!(c.compilable(), "{:?}", c.analysis.reasons);
    let ir = c.ir.unwrap().to_string();
    assert!(ir.contains("db.sql_one"), "{ir}");
}
