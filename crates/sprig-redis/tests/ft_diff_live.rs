//! `FT.*` against a reference Redis with its query engine (Redis 8's
//! `redis:8-alpine` image has it), through a real client (`redis`,
//! BSD-3-Clause), one connection to each server.
//!
//! - `SPRIG_TEST_REDIS_REF`: the reference, such as `127.0.0.1:26391`.
//! - `SPRIG_TEST_REDIS_SPRIG`: a SprigDB gateway's Redis listener, as a URL
//!   with a tenant login (`redis://acme.app:pw@127.0.0.1:47479`).
//!
//! Skipped when either is unset. The test writes keys under `ftd:` and
//! `ftx:` and the index `ftd_idx` on both servers, and deletes them at the
//! end. Results are compared as sets of keys (scores are BM25 on both
//! sides, with different statistics), in order where the query sorts.
//! `HIGHLIGHT` replies compare field by field; `FT.AGGREGATE ...
//! WITHCURSOR` compares the rows of every `FT.CURSOR READ` together.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::wildcard_enum_match_arm,
    clippy::match_wildcard_for_single_variants,
    clippy::many_single_char_names,
    clippy::type_complexity
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use redis::Value;
use redis::aio::MultiplexedConnection;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

async fn conn(url: &str) -> MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn run(c: &mut MultiplexedConnection, args: &[String]) -> redis::RedisResult<Value> {
    let mut cmd = redis::cmd(&args[0]);
    for a in &args[1..] {
        cmd.arg(a.as_str());
    }
    cmd.query_async(c).await
}

fn words(s: &str) -> Vec<String> {
    // Arguments split at spaces; `'...'` keeps spaces inside.
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in s.chars() {
        match ch {
            '\'' => quoted = !quoted,
            ' ' if !quoted => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn text(v: &Value) -> String {
    match v {
        Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        Value::SimpleString(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Okay => "OK".into(),
        Value::Nil => "(nil)".into(),
        other => format!("{other:?}"),
    }
}

fn pairs(v: &Value) -> BTreeMap<String, String> {
    match v {
        Value::Array(a) => a
            .chunks(2)
            .map(|p| {
                let val = match &p[1] {
                    Value::Array(l) => {
                        let mut s: Vec<String> = l.iter().map(text).collect();
                        s.sort();
                        s.join("|")
                    }
                    other => text(other),
                };
                (text(&p[0]), val)
            })
            .collect(),
        _ => BTreeMap::new(),
    }
}

/// An `FT.SEARCH` reply as (total, hits in order with their fields).
fn search_reply(v: &Value, nocontent: bool) -> (i64, Vec<(String, BTreeMap<String, String>)>) {
    let Value::Array(a) = v else {
        return (-1, Vec::new());
    };
    let total = match a.first() {
        Some(Value::Int(n)) => *n,
        _ => -1,
    };
    let mut out = Vec::new();
    let mut i = 1;
    while i < a.len() {
        let k = text(&a[i]);
        i += 1;
        let f = match a.get(i) {
            Some(v @ Value::Array(_)) if !nocontent => {
                i += 1;
                pairs(v)
            }
            _ => BTreeMap::new(),
        };
        out.push((k, f));
    }
    (total, out)
}

/// Compares one command's replies; a description of the difference.
fn diff_search(cmd: &str, s: &Value, r: &Value) -> Option<String> {
    let up = cmd.to_ascii_uppercase();
    let nocontent = up.contains(" NOCONTENT");
    let sorted = up.contains(" SORTBY ");
    let (ts, mut hs) = search_reply(s, nocontent);
    let (tr, mut hr) = search_reply(r, nocontent);
    if !sorted {
        hs.sort();
        hr.sort();
    }
    (ts != tr || hs != hr).then(|| format!("{cmd}\n  sprig: {ts} {hs:?}\n  ref:   {tr} {hr:?}"))
}

fn diff_agg(cmd: &str, s: &Value, r: &Value) -> Option<String> {
    let up = cmd.to_ascii_uppercase();
    let rows = |v: &Value| -> (i64, Vec<BTreeMap<String, String>>) {
        let Value::Array(a) = v else {
            return (-1, Vec::new());
        };
        let n = match a.first() {
            Some(Value::Int(n)) => *n,
            _ => -1,
        };
        (n, a.iter().skip(1).map(pairs).collect())
    };
    let (ns, mut rs) = rows(s);
    let (nr, mut rr) = rows(r);
    // Rows that tie on the sort keys come in any order: the sort key's
    // sequence must match, and the rows as a set.
    let mut order_differs = false;
    if let Some(pos) = up.find(" SORTBY ") {
        let key = cmd[pos + 8..]
            .split_whitespace()
            .nth(1)
            .unwrap_or("")
            .trim_start_matches('@')
            .to_owned();
        let seq = |v: &[BTreeMap<String, String>]| -> Vec<Option<String>> {
            v.iter().map(|m| m.get(&key).cloned()).collect()
        };
        order_differs = seq(&rs) != seq(&rr);
    }
    rs.sort();
    rr.sort();
    // Redis Search's count after FILTER isn't the number of rows.
    let count_differs = ns != nr && !up.contains(" FILTER ");
    (count_differs || order_differs || rs != rr)
        .then(|| format!("{cmd}\n  sprig: {ns} {rs:?}\n  ref:   {nr} {rr:?}"))
}

const TITLES: &[&str] = &[
    "Hello world",
    "Hello there",
    "Goodbye world",
    "Search engine basics",
    "Running a database",
    "The quick brown fox",
    "Lazy dogs running",
    "Engines and runners",
    "Brown bread",
    "World of search",
];
const BODIES: &[&str] = &[
    "the quick brown fox jumps over the lazy dog",
    "a fox runs into the database",
    "search engines index documents",
    "nothing to see here",
    "hello again, world",
    "dogs and foxes are running",
];
const TAGS: &[&str] = &[
    "red",
    "Blue",
    "red,green",
    "blue, Green",
    "yellow",
    "RED,blue",
];

/// The corpus: 40 hashes under `ftd:`, two outside the prefix, a string
/// key under it.
fn corpus() -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for i in 0..40usize {
        let mut c = vec![
            "HSET".to_owned(),
            format!("ftd:{i}"),
            "title".into(),
            TITLES[i % TITLES.len()].into(),
            "price".into(),
            ((i * 7) % 60).to_string(),
        ];
        if i % 3 != 0 {
            c.extend(["body".into(), BODIES[i % BODIES.len()].into()]);
        }
        if i % 4 != 1 {
            c.extend(["tags".into(), TAGS[i % TAGS.len()].into()]);
        }
        if i % 5 == 0 {
            c.extend(["extra".into(), format!("x{i}")]);
        }
        out.push(c);
    }
    out.push(words("HSET ftx:1 title 'Hello world' price 5"));
    out.push(words("HSET ftx:2 title 'search engine' price 6"));
    out.push(words("SET ftd:str 'Hello world'"));
    out
}

const SEARCHES: &str = "
FT.SEARCH ftd_idx * NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx hello NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'hello world' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'hello|world' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'hello world|fox' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '-hello' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'world -hello' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'world -hello fox' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'world -hello fox' NOCONTENT LIMIT 0 1000 DIALECT 2
FT.SEARCH ftd_idx running NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx run NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx engines NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx @title:hello NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx @body:fox NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@title:hello world' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@title:hello world' NOCONTENT LIMIT 0 1000 DIALECT 2
FT.SEARCH ftd_idx '@title:(hello|search)' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@title|body:fox' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx hel* NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx %helo% NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '\"quick brown\"' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '\"brown quick\"' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@price:[10 30]' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@price:[(10 +inf]' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@price:[-inf (21]' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@tags:{red}' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@tags:{red | blue}' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@tags:{GREEN}' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@tags:{gr*}' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'world @tags:{blue}' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'hello -@tags:{red}' NOCONTENT LIMIT 0 1000 DIALECT 2
FT.SEARCH ftd_idx '~fox hello' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'the hello' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx 'nomatchword' NOCONTENT LIMIT 0 1000
FT.SEARCH ftd_idx '@price:[$lo $hi]' NOCONTENT LIMIT 0 1000 PARAMS 4 lo 5 hi 15 DIALECT 2
FT.SEARCH ftd_idx world NOCONTENT LIMIT 0 1000 INFIELDS 1 body
FT.SEARCH ftd_idx world NOCONTENT LIMIT 0 1000 FILTER price 20 40
FT.SEARCH ftd_idx * NOCONTENT LIMIT 0 1000 INKEYS 3 ftd:1 ftd:2 ftx:1
FT.SEARCH ftd_idx * NOCONTENT SORTBY price ASC LIMIT 0 10
FT.SEARCH ftd_idx * NOCONTENT SORTBY price DESC LIMIT 5 10
FT.SEARCH ftd_idx world SORTBY price ASC LIMIT 0 5
FT.SEARCH ftd_idx world SORTBY price ASC LIMIT 0 5 RETURN 2 title price
FT.SEARCH ftd_idx world SORTBY price ASC LIMIT 0 5 RETURN 3 title AS t price
FT.SEARCH ftd_idx * SORTBY price DESC LIMIT 0 0
FT.SEARCH ftd_idx hello HIGHLIGHT LIMIT 0 1000
FT.SEARCH ftd_idx 'hello world' HIGHLIGHT FIELDS 1 title TAGS '[' ']' LIMIT 0 1000
FT.SEARCH ftd_idx running HIGHLIGHT LIMIT 0 1000
FT.SEARCH ftd_idx '\"quick brown\"' HIGHLIGHT FIELDS 1 body LIMIT 0 1000
FT.SEARCH ftd_idx 'world -hello' HIGHLIGHT LIMIT 0 1000
FT.SEARCH ftd_idx '@title:engines' HIGHLIGHT LIMIT 0 1000
FT.SEARCH ftd_idx %helo% HIGHLIGHT TAGS '<em>' '</em>' LIMIT 0 1000
FT.SEARCH ftd_idx world SORTBY price ASC LIMIT 0 5 RETURN 2 title body HIGHLIGHT
";

const AGGREGATES: &str = "
FT.AGGREGATE ftd_idx * GROUPBY 1 @tags REDUCE COUNT 0 AS n
FT.AGGREGATE ftd_idx * GROUPBY 1 @title REDUCE COUNT 0 AS n REDUCE SUM 1 @price AS total REDUCE AVG 1 @price AS avg REDUCE MIN 1 @price AS lo REDUCE MAX 1 @price AS hi SORTBY 2 @n DESC
FT.AGGREGATE ftd_idx world GROUPBY 0 REDUCE COUNT 0 AS n REDUCE STDDEV 1 @price AS sd REDUCE COUNT_DISTINCT 1 @title AS titles
FT.AGGREGATE ftd_idx * LOAD 2 @title @price APPLY '@price * 2 + 1' AS d SORTBY 4 @price ASC @title ASC LIMIT 0 8
FT.AGGREGATE ftd_idx hello LOAD 1 @title APPLY 'upper(@title)' AS u APPLY 'strlen(@title)' AS n
FT.AGGREGATE ftd_idx * LOAD 1 @price FILTER '@price >= 30 && @price < 50'
FT.AGGREGATE ftd_idx * GROUPBY 1 @title REDUCE TOLIST 1 @price AS prices REDUCE FIRST_VALUE 4 @price BY @price DESC AS top
FT.AGGREGATE ftd_idx @tags:{red} GROUPBY 1 @tags REDUCE QUANTILE 2 @price 0.5 AS med SORTBY 2 @tags ASC
";

const AFTER_WRITES: &str = "
HSET ftd:1 title 'Brand new hello title' price 99
HDEL ftd:2 title
DEL ftd:3
HSET ftd:100 title 'Hello late arrival' price 1 tags red
EXPIRE ftd:4 1000
HSET ftd:5 tags 'purple'
";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_matches_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), var("SPRIG_TEST_REDIS_SPRIG"))
    else {
        eprintln!("SPRIG_TEST_REDIS_REF or SPRIG_TEST_REDIS_SPRIG not set: skipped");
        return;
    };
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&url).await;
    let both = |c: &str| words(c);
    // A clean start on both.
    for c in [&mut r, &mut s] {
        let _ = run(c, &both("FT.DROPINDEX ftd_idx DD")).await;
        let mut del = vec![
            "DEL".to_owned(),
            "ftd:str".into(),
            "ftx:1".into(),
            "ftx:2".into(),
        ];
        del.extend((0..101).map(|i| format!("ftd:{i}")));
        run(c, &del).await.unwrap();
    }
    for cmd in corpus() {
        run(&mut r, &cmd).await.unwrap();
        run(&mut s, &cmd).await.unwrap();
    }
    let create = both(
        "FT.CREATE ftd_idx ON HASH PREFIX 1 ftd: SCHEMA title TEXT WEIGHT 2.0 body TEXT price NUMERIC SORTABLE tags TAG",
    );
    assert_eq!(text(&run(&mut r, &create).await.unwrap()), "OK");
    assert_eq!(text(&run(&mut s, &create).await.unwrap()), "OK");
    // The reference indexes in the background.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut bad = Vec::new();
    let mut n = 0;
    let mut check = |bad: &mut Vec<String>,
                     cmd: &str,
                     sv: redis::RedisResult<Value>,
                     rv: redis::RedisResult<Value>| {
        n += 1;
        match (sv, rv) {
            (Ok(sv), Ok(rv)) => {
                let d = if cmd.to_ascii_uppercase().contains("FT.AGGREGATE") {
                    diff_agg(cmd, &sv, &rv)
                } else {
                    diff_search(cmd, &sv, &rv)
                };
                bad.extend(d);
            }
            (Err(_), Err(_)) => {}
            (sv, rv) => bad.push(format!("{cmd}\n  sprig: {sv:?}\n  ref:   {rv:?}")),
        }
    };
    for round in ["before writes", "after writes"] {
        if round == "after writes" {
            for line in AFTER_WRITES.lines().filter(|l| !l.trim().is_empty()) {
                let c = both(line);
                let a = run(&mut r, &c).await.map(|v| text(&v));
                let b = run(&mut s, &c).await.map(|v| text(&v));
                assert_eq!(a.ok(), b.ok(), "{line}");
            }
        }
        for line in SEARCHES
            .lines()
            .chain(AGGREGATES.lines())
            .filter(|l| !l.trim().is_empty())
        {
            let c = both(line);
            let sv = run(&mut s, &c).await;
            let rv = run(&mut r, &c).await;
            check(&mut bad, &format!("[{round}] {line}"), sv, rv);
        }
    }
    // FT.AGGREGATE ... WITHCURSOR, read to the end with FT.CURSOR READ:
    // the same rows (Redis's first count and the moment its cursor id
    // turns 0 differ, so neither is compared).
    for line in [
        "FT.AGGREGATE ftd_idx * LOAD 2 @title @price SORTBY 4 @price ASC @title ASC MAX 100 WITHCURSOR COUNT 7",
        "FT.AGGREGATE ftd_idx * GROUPBY 1 @tags REDUCE COUNT 0 AS n WITHCURSOR COUNT 2",
        "FT.AGGREGATE ftd_idx hello LOAD 1 @title WITHCURSOR",
    ] {
        let c = both(line);
        let mut rows: [Vec<BTreeMap<String, String>>; 2] = [Vec::new(), Vec::new()];
        for (i, conn) in [&mut s, &mut r].into_iter().enumerate() {
            let mut reply = run(conn, &c).await;
            let mut reads = 0;
            loop {
                let Ok(Value::Array(parts)) = &reply else {
                    bad.push(format!("{line}: {reply:?}"));
                    break;
                };
                if let Some(Value::Array(batch)) = parts.first() {
                    rows[i].extend(batch.iter().skip(1).map(pairs));
                }
                let id = parts.get(1).map(text).unwrap_or_default();
                if id == "0" || reads > 100 {
                    break;
                }
                reads += 1;
                reply = run(conn, &both(&format!("FT.CURSOR READ ftd_idx {id}"))).await;
            }
        }
        let sorted = line.contains("SORTBY");
        let [mut a, mut b] = rows;
        if !sorted {
            a.sort();
            b.sort();
        }
        if a != b {
            bad.push(format!("{line}\n  sprig: {a:?}\n  ref:   {b:?}"));
        }
        n += 1;
    }
    // Cursor errors: an unknown cursor (READ and DEL), a cursor deleted.
    for line in [
        "FT.CURSOR READ ftd_idx 12345",
        "FT.CURSOR DEL ftd_idx 12345",
        "FT.CURSOR READ nosuch_idx 12345",
        "FT.AGGREGATE ftd_idx * WITHCURSOR COUNT 0",
    ] {
        let c = both(line);
        let sv = run(&mut s, &c).await;
        let rv = run(&mut r, &c).await;
        if sv.is_ok() || rv.is_ok() {
            bad.push(format!(
                "{line}: expected errors\n  sprig: {sv:?}\n  ref:   {rv:?}"
            ));
        }
        n += 1;
    }
    for conn in [&mut s, &mut r] {
        let reply = run(
            conn,
            &both("FT.AGGREGATE ftd_idx * LOAD 1 @price WITHCURSOR COUNT 1"),
        )
        .await
        .unwrap();
        let Value::Array(parts) = reply else {
            panic!("WITHCURSOR reply");
        };
        let id = text(&parts[1]);
        assert_ne!(id, "0");
        assert_eq!(
            text(
                &run(conn, &both(&format!("FT.CURSOR DEL ftd_idx {id}")))
                    .await
                    .unwrap()
            ),
            "OK"
        );
        assert!(
            run(conn, &both(&format!("FT.CURSOR READ ftd_idx {id}")))
                .await
                .is_err()
        );
    }
    n += 1;
    // Errors on both sides.
    for line in [
        "FT.SEARCH nosuch_idx hello",
        "FT.SEARCH ftd_idx '(hello'",
        "FT.SEARCH ftd_idx hello LIMIT 0",
        "FT.CREATE ftd_idx SCHEMA a TEXT",
        "FT.AGGREGATE ftd_idx * GROUPBY 1 @tags REDUCE NOSUCH 0",
        "FT.DROPINDEX nosuch_idx",
        "FT.SEARCH ftd_idx hello HIGHLIGHT FIELDS 1 nosuch",
        "FT.SEARCH ftd_idx hello HIGHLIGHT TAGS x",
    ] {
        let c = both(line);
        let sv = run(&mut s, &c).await;
        let rv = run(&mut r, &c).await;
        if sv.is_ok() || rv.is_ok() {
            bad.push(format!(
                "{line}: expected errors\n  sprig: {sv:?}\n  ref:   {rv:?}"
            ));
        }
        n += 1;
    }
    // FT.INFO's document count and schema, FT._LIST and FT.TAGVALS.
    let info_s = pairs(&run(&mut s, &both("FT.INFO ftd_idx")).await.unwrap());
    let info_r = pairs(&run(&mut r, &both("FT.INFO ftd_idx")).await.unwrap());
    for k in ["index_name", "num_docs"] {
        if info_s.get(k) != info_r.get(k) {
            bad.push(format!(
                "FT.INFO {k}: sprig {:?}, ref {:?}",
                info_s.get(k),
                info_r.get(k)
            ));
        }
    }
    let list_s = run(&mut s, &both("FT._LIST")).await.unwrap();
    if !format!("{list_s:?}").contains("ftd_idx") {
        bad.push(format!("FT._LIST: {list_s:?}"));
    }
    let tags = |v: Value| -> BTreeSet<String> {
        match v {
            Value::Array(a) | Value::Set(a) => a.iter().map(text).collect(),
            _ => BTreeSet::new(),
        }
    };
    let ts = tags(run(&mut s, &both("FT.TAGVALS ftd_idx tags")).await.unwrap());
    let tr = tags(run(&mut r, &both("FT.TAGVALS ftd_idx tags")).await.unwrap());
    if ts != tr {
        bad.push(format!("FT.TAGVALS: sprig {ts:?}, ref {tr:?}"));
    }
    n += 3;
    // FT.DROPINDEX DD deletes the indexed hashes, not the others.
    for c in [&mut r, &mut s] {
        assert_eq!(
            text(&run(c, &both("FT.DROPINDEX ftd_idx DD")).await.unwrap()),
            "OK"
        );
    }
    let left = |v: Value| text(&v);
    let es = left(
        run(&mut s, &both("EXISTS ftd:1 ftd:100 ftx:1 ftd:str"))
            .await
            .unwrap(),
    );
    let er = left(
        run(&mut r, &both("EXISTS ftd:1 ftd:100 ftx:1 ftd:str"))
            .await
            .unwrap(),
    );
    if es != er {
        bad.push(format!(
            "after FT.DROPINDEX DD: EXISTS sprig {es}, ref {er}"
        ));
    }
    for c in [&mut r, &mut s] {
        run(c, &both("DEL ftx:1 ftx:2 ftd:str")).await.unwrap();
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("ft corpus: {n} checks, {} mismatches", bad.len());
    assert!(bad.is_empty(), "{} of {n} differ", bad.len());
}
