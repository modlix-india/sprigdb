//! The Lua differential corpus: functions written in Lua with the calls
//! to run, shared by `lua_engine.rs` (the reference interpreter against
//! the engine tier) and `lua_calls.rs` (compiled routines on MariaDB
//! against the engine tier).

#![allow(dead_code)]

use serde_json::{Value as J, json};
use sprig_logic_js::ParamTy;

pub(crate) struct Case {
    pub(crate) name: &'static str,
    pub(crate) src: &'static str,
    pub(crate) params: &'static [(&'static str, ParamTy)],
    pub(crate) scalar: Option<ParamTy>,
    pub(crate) calls: Vec<Vec<J>>,
}

pub(crate) const I: ParamTy = ParamTy::Int;
pub(crate) const F: ParamTy = ParamTy::Float;
pub(crate) const S: ParamTy = ParamTy::Str;
pub(crate) const B: ParamTy = ParamTy::Bool;

/// Functions without data access.
pub(crate) fn pure() -> Vec<Case> {
    vec![
        Case {
            name: "branches",
            src: "if score == sprig.null then return sprig.event('error', { message = 'no score' }) end
                  if score >= 90 then return { grade = 'A', bonus = score + 5 } end
                  return { grade = score >= 50 and 'pass' or 'fail', bonus = 0 }",
            params: &[("score", I), ("vip", B)],
            scalar: None,
            calls: vec![
                vec![json!(95), json!(true)],
                vec![json!(60), json!(false)],
                vec![J::Null, json!(true)],
                vec![json!(40), J::Null],
            ],
        },
        Case {
            name: "truth",
            src: "local t = 0
                  if a then t = t + 1 end
                  if b then t = t + 10 end
                  if s then t = t + 100 end
                  local n = nil
                  if n then t = t + 1000 end
                  if 0 then t = t + 10000 end
                  if '' then t = t + 100000 end
                  return { t = t, x = a == nil, y = a == sprig.null, z = not b,
                           w = b == false, v = s ~= sprig.null }",
            params: &[("a", I), ("b", B), ("s", S)],
            scalar: None,
            calls: vec![
                vec![J::Null, J::Null, J::Null],
                vec![json!(0), json!(false), json!("")],
                vec![json!(3), json!(true), json!("x")],
            ],
        },
        Case {
            name: "arith",
            src: "return { s = a + b, d = a - b, p = a * b, q = a / b, m = a % b, n = -a % 3,
                           e = 2 ^ a, f = math.floor(a / 3), c = math.ceil(b), ab = math.abs(-a),
                           mx = math.max(a, 2, b), mn = math.min(a, b), fm = math.fmod(a, 3),
                           pw = math.pow(2, 10), fl = math.floor(b), mi = math.max(a, 2) }",
            params: &[("a", I), ("b", F)],
            scalar: None,
            calls: vec![
                vec![json!(7), json!(2.5)],
                vec![json!(-7), json!(-2.5)],
                vec![json!(10), json!(4.0)],
                vec![J::Null, json!(1.5)],
                vec![json!(1), J::Null],
            ],
        },
        Case {
            name: "intmod",
            src: "return { a = x % y, b = x % -y, c = -x % y, d = math.fmod(x, y), e = math.fmod(-x, y) }",
            params: &[("x", I), ("y", I)],
            scalar: None,
            calls: vec![
                vec![json!(7), json!(3)],
                vec![json!(6), json!(3)],
                vec![json!(0), json!(5)],
                vec![json!(13), json!(4)],
            ],
        },
        Case {
            name: "text",
            src: "local s = 'hi ' .. name
                  if #name > 3 then s = s .. '!' end
                  return { s = s, up = name:upper(), lo = string.lower(name), len = #name,
                           l2 = name:len(), n = 'n=' .. n, t = tostring(n), ty = type(n),
                           tn = type(name), tb = tostring(n > 1) }",
            params: &[("name", S), ("n", I)],
            scalar: None,
            calls: vec![
                vec![json!("Abba"), json!(2)],
                vec![json!("zeb"), json!(-3)],
                vec![json!(""), json!(0)],
                vec![J::Null, json!(1)],
                vec![json!("ok"), J::Null],
            ],
        },
        Case {
            name: "utf8_length",
            src: "return { n = #s, u = s:upper() }",
            params: &[("s", S)],
            scalar: None,
            calls: vec![vec![json!("José")], vec![json!("日本")], vec![json!("a")]],
        },
        Case {
            name: "loops",
            src: "local out = ''
                  for i = 1, n do
                    if i > 4 then break end
                    out = out .. i .. ','
                  end
                  for i = 10, 1, -3 do out = out .. i end
                  for i = n, 1, -1 do out = out .. '<' .. i end
                  local k = 0
                  while k < n do k = k + 2 end
                  repeat local j = k; k = k + 1 until j >= 7
                  local sum = 0
                  for i = 1, 3 do
                    for j = i, 3 do sum = sum + i * j end
                  end
                  return { out = out, k = k, sum = sum }",
            params: &[("n", I)],
            scalar: None,
            calls: vec![vec![json!(6)], vec![json!(0)], vec![json!(-2)], vec![J::Null]],
        },
        Case {
            name: "float_for",
            src: "local t = 0
                  for x = 0, 1, 0.25 do t = t + x end
                  for x = a, a + 1 do t = t + x end
                  return t",
            params: &[("a", F)],
            scalar: Some(F),
            calls: vec![vec![json!(0.5)], vec![json!(2.0)]],
        },
        Case {
            name: "helpers",
            src: "local function double(v) return v * 2 end
                  local function classify(v)
                    if v < 0 then return 'neg' end
                    if v == 0 then return 'zero' end
                    return 'pos'
                  end
                  local count = 0
                  local function bump(by) count = count + by end
                  bump(2) bump(x)
                  local r = { kind = classify(x), twice = double(x) }
                  r.extra = r.twice + count
                  return r",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(5)], vec![json!(0)], vec![json!(-4)], vec![J::Null]],
        },
        Case {
            name: "errors",
            src: "if x == sprig.null then error('missing') end
                  if x > 100 then error('too big: ' .. x, 0) end
                  assert(x ~= 42, 'not that one')
                  if x == 13 then sprig.reject('unlucky ' .. x) end
                  assert(x ~= 7)
                  return { half = x / 2 }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![
                vec![json!(4)],
                vec![J::Null],
                vec![json!(101)],
                vec![json!(42)],
                vec![json!(13)],
                vec![json!(7)],
            ],
        },
        Case {
            name: "and_or",
            src: "local a1 = x and 'yes'
                  local o1 = y or 'default'
                  local o2 = nil or 5
                  local o3 = false or nil
                  local c = (x and y) or 'neither'
                  local d = x == 1 and 10 or 20
                  local e = flag and 1 or 2
                  return { a1 = a1, o1 = o1, o2 = o2, c = c, d = d, e = e, o3 = o3 == nil }",
            params: &[("x", I), ("y", S), ("flag", B)],
            scalar: None,
            calls: vec![
                vec![json!(1), json!("why"), json!(true)],
                vec![J::Null, J::Null, J::Null],
                vec![json!(2), json!(""), json!(false)],
            ],
        },
        Case {
            name: "function_form",
            src: "return function (p, q)
                    p = p + 1
                    if q == sprig.null then return p end
                    return p * q
                  end",
            params: &[("a", I), ("b", I)],
            scalar: Some(I),
            calls: vec![
                vec![json!(2), json!(5)],
                vec![json!(2), J::Null],
                vec![J::Null, json!(1)],
            ],
        },
        Case {
            name: "multi_assign",
            src: "local a, b, c = x, x * 2
                  a, b = b, a
                  local t = { n = 0 }
                  t.n, t.m = t.n + 1, 'm'
                  return { a = a, b = b, c = c == nil, n = t.n, m = t.m }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(3)], vec![json!(0)]],
        },
        Case {
            name: "equality",
            src: "return { a = x == y, b = x ~= y, c = s == 'x', d = x == s, e = f == x,
                           g = x == nil, h = nil == nil, i = sprig.null == nil }",
            params: &[("x", I), ("y", I), ("s", S), ("f", F)],
            scalar: None,
            calls: vec![
                vec![json!(1), json!(1), json!("x"), json!(1.0)],
                vec![json!(1), json!(2), json!("y"), json!(1.5)],
                vec![J::Null, J::Null, J::Null, J::Null],
                vec![json!(3), J::Null, json!("x"), json!(3.0)],
            ],
        },
        Case {
            name: "compare_null",
            src: "if a < b then return { r = 'lt' } end
                  return { r = 'ge' }",
            params: &[("a", I), ("b", F)],
            scalar: None,
            calls: vec![
                vec![json!(1), json!(2.0)],
                vec![json!(3), json!(2.0)],
                vec![J::Null, json!(2.0)],
            ],
        },
        Case {
            name: "strings_compare",
            src: "return { lt = a < b, le = a <= b, gt = a > b }",
            params: &[("a", S), ("b", S)],
            scalar: None,
            calls: vec![
                vec![json!("abc"), json!("abd")],
                vec![json!("b"), json!("a")],
                vec![json!("é"), json!("z")],
                vec![json!("a"), json!("a")],
            ],
        },
        Case {
            name: "scalar_returns",
            src: "if n > 2 then return 'big' end
                  if n > 0 then return nil end
                  return 'small'",
            params: &[("n", I)],
            scalar: Some(S),
            calls: vec![vec![json!(3)], vec![json!(1)], vec![json!(0)]],
        },
        Case {
            name: "record_values",
            src: "local r = {}
                  r.a = 1
                  if n > 0 then r.b = 'pos' else r.b = 'neg' end
                  local inner = { x = n, y = { z = 'deep' }, list = { 1, 2, n } }
                  return { r = r, inner = inner, empty = {} }",
            params: &[("n", I)],
            scalar: None,
            calls: vec![vec![json!(2)], vec![json!(-1)]],
        },
        Case {
            name: "repeat_scope",
            src: "local i = 0
                  local seen = ''
                  repeat
                    i = i + 1
                    local done = i >= n
                    seen = seen .. i
                  until done
                  return { seen = seen }",
            params: &[("n", I)],
            scalar: None,
            calls: vec![vec![json!(3)], vec![json!(0)]],
        },
        Case {
            name: "elseif",
            src: "local r
                  if x < 0 then r = 'neg'
                  elseif x == 0 then r = 'zero'
                  elseif x < 10 then r = 'small'
                  else r = 'big' end
                  return { r = r }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(-1)], vec![json!(0)], vec![json!(5)], vec![json!(50)]],
        },
        Case {
            name: "pcall_messages",
            src: "if x == sprig.null then return { r = 'none' } end
                  local function risky(v)
                    if v < 0 then error('negative ' .. v) end
                    if v == 0 then error('zero', 0) end
                    assert(v ~= 7, 'seven')
                    if v == 8 then assert(false) end
                    return 'got ' .. v
                  end
                  local ok, r = pcall(risky, x)
                  local ok2 = pcall(function() local y = x + 1 end)
                  local ok3, e3 = pcall(function() error('inner ' .. x) end)
                  if not pcall(risky, 1) then return { r = 'never' } end
                  return { ok = ok, r = r, ok2 = ok2, ok3 = ok3, e3 = e3 or 'none' }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![
                vec![json!(3)],
                vec![json!(-2)],
                vec![json!(0)],
                vec![json!(7)],
                vec![json!(8)],
                vec![J::Null],
            ],
        },
        Case {
            name: "pcall_rethrow",
            src: "if x == sprig.null then return {} end
                  local function inner(v)
                    if v > 2 then error('big ' .. v) end
                    return 'small'
                  end
                  local ok, e = pcall(function ()
                    local ok2, e2 = pcall(inner, x)
                    if not ok2 then error(e2, 0) end
                    if x == 1 then error('one') end
                    return e2
                  end)
                  if x == 5 then error(e, 0) end
                  return { ok = ok, e = e }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(0)], vec![json!(1)], vec![json!(3)], vec![json!(5)]],
        },
        Case {
            name: "pcall_uncaught",
            src: "local ok = pcall(function ()
                    if x == 0 then sprig.reject('zero') end
                    error('caught')
                  end)
                  local n = 0
                  for i = 1, 3 do
                    local ok2 = pcall(function () if i == x then error('at ' .. i) end end)
                    if not ok2 then n = n + 10 else n = n + 1 end
                  end
                  return { ok = ok, n = n }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(0)], vec![json!(2)], vec![json!(9)]],
        },
        Case {
            name: "tonumber_and_type",
            src: "local a = tonumber(x)
                  return { t = type(a), b = a == nil, n = type(nil), u = type(sprig.null) }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(3)], vec![J::Null]],
        },
    ]
}

/// The tables the data cases use, created afresh before every call.
pub(crate) const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL, label VARCHAR(32))",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))",
    "INSERT INTO products VALUES (7, 400.00, 10, 'lamp'), (8, 19.99, 3, NULL), (9, 0.10, 0, 'pin')",
    "INSERT INTO orders VALUES (100, 7, 1, 'first'), (101, 8, 2, NULL)",
    "CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, what VARCHAR(64) NOT NULL)",
    "CREATE TABLE __sprig_eo_901 (event_id BIGINT AUTO_INCREMENT PRIMARY KEY, partition_no INT UNSIGNED NULL, \
     k VARBINARY(4096) NULL, v LONGBLOB NULL, headers JSON NULL)",
    "CREATE TABLE __sprig_eo_902 (event_id BIGINT AUTO_INCREMENT PRIMARY KEY, partition_no INT UNSIGNED NULL, \
     k VARBINARY(4096) NULL, v LONGBLOB NULL, headers JSON NULL, \
     CONSTRAINT sprig_sc_needs_key CHECK (k IS NOT NULL))",
];

/// The tables [`SEED`] makes, for dropping them first.
pub(crate) const SEEDED: &str = "products, orders, audit, __sprig_eo_901, __sprig_eo_902";

/// Streams: name, outbox, compacted. Their `stream.max_message_bytes` is
/// [`MAX_BYTES`].
pub(crate) const STREAMS: &[(&str, &str, bool)] = &[
    ("orders_s", "__sprig_eo_901", false),
    ("latest_s", "__sprig_eo_902", true),
];

/// The small `stream.max_message_bytes` the streams get here.
pub(crate) const MAX_BYTES: u64 = 64;

/// The rows every data case leaves, compared after each call.
pub(crate) const TABLES: &[&str] = &[
    "SELECT id, price, stock, label FROM products ORDER BY id",
    "SELECT id, product_id, qty, note FROM orders ORDER BY id",
    "SELECT id, what FROM audit ORDER BY id",
    "SELECT event_id, k, v, headers FROM __sprig_eo_901 ORDER BY event_id",
    "SELECT event_id, k, v, headers FROM __sprig_eo_902 ORDER BY event_id",
];

/// Functions that read and write the tables of [`SEED`].
pub(crate) fn data() -> Vec<Case> {
    vec![
        Case {
            name: "quote",
            src: "local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', productId)
                  if p == sprig.null or not (p.stock >= qty) then
                    return sprig.event('error', { message = 'not enough stock' })
                  end
                  return { total = p.price * qty + 40 }",
            params: &[("productId", I), ("qty", I)],
            scalar: None,
            calls: vec![
                vec![json!(7), json!(3)],
                vec![json!(7), json!(1000)],
                vec![json!(999), json!(1)],
                vec![json!(8), json!(2)],
                vec![json!(9), json!(1)],
                vec![json!(7), J::Null],
            ],
        },
        Case {
            name: "orders",
            src: "local t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', qty, productId, qty)
                  if t.affected ~= 1 then return sprig.event('error', { message = 'out of stock for ' .. productId }) end
                  db.exec(\"INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, CONCAT('it''s ', ?))\",
                          orderId, productId, qty, 'order ' .. orderId)
                  local left = db.sqlOne('SELECT stock FROM products WHERE id = ?', productId)
                  local orders = db.sql('SELECT id, qty, note FROM orders WHERE product_id = ? ORDER BY id', productId)
                  return { left = left.stock, orders = orders, changed = t.affected }",
            params: &[("orderId", I), ("productId", I), ("qty", I)],
            scalar: None,
            calls: vec![
                vec![json!(1), json!(7), json!(2)],
                vec![json!(2), json!(8), json!(3)],
                vec![json!(3), json!(8), json!(4)],
                vec![json!(4), json!(999), json!(1)],
                vec![json!(100), json!(7), json!(1)],
            ],
        },
        Case {
            name: "rows",
            src: "for i = 1, 4 do
                    if i > n then break end
                    db.exec('INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, ?)',
                            200 + i, 9, i, i % 2 == 0 and 'even' or nil)
                  end
                  local total, notes, count = 0, '', 0
                  for _, o in ipairs(db.sql('SELECT qty, note FROM orders WHERE product_id = ? ORDER BY id', 9)) do
                    if count >= limit then break end
                    total = total + o.qty
                    if o.note ~= sprig.null then notes = notes .. o.note .. ';' end
                    count = count + 1
                  end
                  local all = db.sql('SELECT id, qty FROM orders WHERE id > ? ORDER BY id', 150)
                  return { total = total, notes = notes, count = count, all = all, n = #all }",
            params: &[("n", I), ("limit", I)],
            scalar: None,
            calls: vec![
                vec![json!(3), json!(10)],
                vec![json!(4), json!(2)],
                vec![json!(0), json!(5)],
                vec![json!(2), json!(0)],
            ],
        },
        Case {
            name: "nulls_from_rows",
            src: "local p = db.sqlOne('SELECT label, stock FROM products WHERE id = ?', id)
                  if p == sprig.null then return { found = false } end
                  local label = p.label
                  if label == sprig.null then label = 'none' end
                  return { found = true, label = label, upper = label:upper(), has = p.label ~= sprig.null,
                           truthy = p.label and 'yes' or 'no', stock = p.stock }",
            params: &[("id", I)],
            scalar: None,
            calls: vec![vec![json!(7)], vec![json!(8)], vec![json!(1)]],
        },
        Case {
            name: "null_row_read",
            src: "local p = db.sqlOne('SELECT price FROM products WHERE id = ?', id)
                  return { price = p.price }",
            params: &[("id", I)],
            scalar: None,
            calls: vec![vec![json!(7)], vec![json!(1)]],
        },
        Case {
            name: "write_errors",
            src: "db.exec('INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, ?)', id, 7, 1, 'x')
                  if id == 102 then error('after the write') end
                  return { ok = true }",
            params: &[("id", I)],
            scalar: None,
            calls: vec![vec![json!(102)], vec![json!(100)], vec![json!(103)]],
        },
        Case {
            name: "pcall_db",
            src: "local ok = pcall(function ()
                    db.exec('INSERT INTO orders (id, product_id, qty, note) VALUES (?, ?, ?, ?)', id, 7, 1, 'p')
                    db.exec('INSERT INTO audit (what) VALUES (?)', 'after ' .. id)
                  end)
                  local ok2, e = pcall(function ()
                    local r = db.sqlOne('SELECT id FROM orders WHERE qty > ?', 0)
                    return r.id
                  end)
                  local ok3 = pcall(function ()
                    local p = db.sqlOne('SELECT price FROM products WHERE id = ?', id)
                    return p.price
                  end)
                  return { ok = ok, ok2 = ok2, ok3 = ok3, n = #db.sql('SELECT id FROM orders') }",
            params: &[("id", I)],
            scalar: None,
            calls: vec![vec![json!(100)], vec![json!(300)], vec![json!(7)]],
        },
        Case {
            name: "empty_rows",
            src: "local rows = db.sql('SELECT id FROM orders WHERE qty > ?', q)
                  return { rows = rows, n = #rows }",
            params: &[("q", I)],
            scalar: None,
            calls: vec![vec![json!(1)], vec![json!(100)]],
        },
    ]
}

/// Functions the `db.call` cases call, compiled in this order.
pub(crate) fn callees() -> Vec<Case> {
    vec![
        Case {
            name: "price",
            src: "local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', id)
                  if p == sprig.null then return sprig.event('missing', { id = id, why = 'no product ' .. id }) end
                  return { price = p.price, stock = p.stock, cheap = p.price < 50 }",
            params: &[("id", I)],
            scalar: None,
            calls: Vec::new(),
        },
        Case {
            name: "take",
            src: "local t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', q, id, q)
                  if t.affected ~= 1 then error('out of stock: ' .. id) end
                  db.exec('INSERT INTO audit (what) VALUES (?)', 'took ' .. q .. ' of ' .. id)
                  local left = db.sqlOne('SELECT stock FROM products WHERE id = ?', id)
                  return { left = left.stock }",
            params: &[("id", I), ("q", I)],
            scalar: None,
            calls: Vec::new(),
        },
        Case {
            name: "check",
            src: "if x == 0 then sprig.reject('zero is not allowed') end
                  if x < 0 then error('negative: ' .. x) end
                  return { v = x * 2, label = 'ok ' .. x }",
            params: &[("x", I)],
            scalar: None,
            calls: Vec::new(),
        },
        Case {
            name: "total",
            src: "local a = db.call('price', id)
                  if a.event ~= 'output' then return sprig.event('missing', { why = a.fields.why }) end
                  return { total = a.fields.price * n, stock = a.fields.stock }",
            params: &[("id", I), ("n", I)],
            scalar: None,
            calls: Vec::new(),
        },
    ]
}

/// Functions calling [`callees`] and publishing to [`STREAMS`].
pub(crate) fn calls() -> Vec<Case> {
    vec![
        Case {
            name: "call_fields",
            src: "local r = db.call('price', x)
                  return { e = r.event, p = r.fields.price, s = r.fields.stock, c = r.fields.cheap,
                           w = r.fields.why, nothing = r.fields.nothing == nil,
                           missing = r.fields.why == sprig.null }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(7)], vec![json!(8)], vec![json!(70)]],
        },
        Case {
            name: "call_whole",
            src: "local r = db.call('price', x)
                  return { r = r, f = r.fields }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(8)], vec![json!(71)]],
        },
        Case {
            name: "call_writes",
            src: "local n, last = 0, -1
                  for k = 1, x do
                    local t = db.call('take', 8, 1)
                    last = t.fields.left
                    n = n + 1
                  end
                  return { n = n, last = last }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(0)], vec![json!(2)], vec![json!(5)]],
        },
        Case {
            name: "call_errors",
            src: "local r = db.call('check', x)
                  return { label = r.fields.label }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(1)], vec![json!(0)], vec![json!(-3)]],
        },
        Case {
            name: "call_nested",
            src: "local t = db.call('total', x, 3)
                  local u = db.call('take', x, 2)
                  return { e = t.event, total = t.fields.total, left = u.fields.left }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(7)], vec![json!(8)], vec![json!(9)]],
        },
        Case {
            name: "call_inline",
            src: "local p = db.call('price', x).fields.price
                  local e = 'e:' .. db.call('check', 2).event
                  local q = x > 7 and db.call('price', x).fields.stock or -1
                  local n = db.publish('orders_s', 'k' .. x, 'v').affected + #db.sql('SELECT id FROM orders')
                  return { p = p, e = e, q = q, n = n, whole = db.call('check', 3) }",
            params: &[("x", I)],
            scalar: None,
            calls: vec![vec![json!(7)], vec![json!(8)], vec![json!(70)]],
        },
        Case {
            name: "publish",
            src: "local a = db.publish('orders_s', key, value)
                  db.publish('orders_s', nil, 2.5, { source = 'lua' })
                  db.publish('orders_s', key, flag)
                  local b = db.publish('latest_s', key, 'last ' .. key)
                  return { a = a.affected, b = b.affected }",
            params: &[("key", I), ("value", S), ("flag", B)],
            scalar: None,
            calls: vec![
                vec![json!(1), json!("one"), json!(true)],
                vec![json!(2), J::Null, J::Null],
                vec![J::Null, json!("x"), json!(false)],
                vec![json!(3), json!("a value much too long for the stream's sixty-four byte limit, by far"), json!(true)],
            ],
        },
    ]
}
