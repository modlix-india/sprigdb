//! Staged rollout by key through a live gateway: a procedure rolled out
//! `BY KEY` on its argument, triggers on a stream and a queue rolled out
//! by each record's or message's key, and an after-commit trigger rolled
//! out by its row's primary key. Each key must get the version
//! `CRC32(CONCAT(key, ':', module)) % 100 < share` picks, the same every
//! time, whether the version runs compiled into MariaDB or in the engine
//! tier, and whichever way it is reached (a client's `CALL`, a compiled
//! caller's `db.call`, an engine-tier caller's).
//!
//! Needs a gateway with workers (triggers run in a worker) and a tenant
//! user who may create databases and routines; the queue part also needs
//! the gateway's AMQP listener:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! export SPRIG_TEST_LOGIC_AMQP=127.0.0.1:43672      # optional
//! cargo test -p sprig-gateway --test logic_rollout_key_live -- --nocapture
//! ```
//!
//! Works in the database `rollout_key_live`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_logic::rollout::bucket;
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "rollout_key_live";

fn user() -> (String, String) {
    (
        std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into()),
        std::env::var("SPRIG_TEST_LOGIC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
    )
}

async fn connect(test: &str) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let (user, password) = user();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Some(Conn::new(opts).await.unwrap())
}

type Rows = Vec<Vec<Option<String>>>;

async fn sql(c: &mut Conn, q: &str) -> Result<Rows, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) -> Rows {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

async fn notes(c: &mut Conn) -> String {
    let w = must(c, "SHOW WARNINGS").await;
    w.into_iter()
        .filter_map(|r| r.get(2).cloned().flatten())
        .collect::<Vec<_>>()
        .join("; ")
}

/// The module id in a deploy's notes (`module M version V`).
fn module_of(notes: &str) -> u64 {
    let i = notes.find("module ").unwrap() + 7;
    notes[i..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

async fn wait_rows(c: &mut Conn, q: &str, n: usize, secs: u64) -> Rows {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let rows = must(c, q).await;
        if rows.len() >= n || Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

struct Checks {
    passed: usize,
    failed: Vec<String>,
}

impl Checks {
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            self.passed += 1;
            println!("ok   {name}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

/// The version a key gets at `pct`%.
fn want(key: &str, module: u64, pct: u32) -> &'static str {
    if bucket(key, module) < pct {
        "v2"
    } else {
        "v1"
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollouts_pick_by_key() {
    let Some(mut c) = connect("rollouts_pick_by_key").await else {
        return;
    };
    let mut ck = Checks {
        passed: 0,
        failed: Vec::new(),
    };
    let started = Instant::now();
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE seen (src VARCHAR(8) NOT NULL, k VARCHAR(32) NOT NULL, \
         n INT NOT NULL, v VARCHAR(8) NOT NULL, PRIMARY KEY (src, k, n))"
            .into(),
    ] {
        must(&mut c, &q).await;
    }

    // ---- a procedure rolled out by its argument -------------------------------------------
    let pick = |v: &str, with: &str| {
        format!(
            "CREATE OR REPLACE PROCEDURE pick(k BIGINT) LANGUAGE javascript {with} AS $$ \
             return {{ v: '{v}', k }}; $$"
        )
    };
    must(&mut c, &pick("v1", "")).await;
    let r = sql(
        &mut c,
        &pick(
            "v2",
            "WITH (SPRIG_ROLLOUT = 40, SPRIG_ROLLOUT_BY = 'key:k')",
        ),
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a candidate rolls out by key on its argument",
        r.is_ok(),
        format!("{r:?} / {n}"),
    );
    let module = module_of(&n);
    must(
        &mut c,
        "CREATE PROCEDURE via_c(k BIGINT) LANGUAGE javascript AS $$ \
         const r = db.call('pick', k); return { v: r.fields.v }; $$",
    )
    .await;
    let nc = notes(&mut c).await;
    must(
        &mut c,
        "CREATE PROCEDURE via_e(k BIGINT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $$ \
         const r = db.call('pick', k); return { v: r.fields.v }; $$",
    )
    .await;
    let mut wrong = Vec::new();
    let mut v2 = 0;
    for round in 0..2 {
        for k in 0..40u64 {
            let w = want(&k.to_string(), module, 40);
            if round == 0 && w == "v2" {
                v2 += 1;
            }
            for p in ["pick", "via_c", "via_e"] {
                let rows = must(&mut c, &format!("CALL {p}({k})")).await;
                let got = rows[0][1].clone().unwrap_or_default();
                if got != w {
                    wrong.push(format!("{p}({k}) {got} want {w}"));
                }
            }
        }
    }
    ck.check(
        &format!(
            "every key gets its bucket's version, twice, from a client's CALL, a compiled \
             db.call and an engine-tier one ({v2} of 40 keys on the candidate)"
        ),
        wrong.is_empty() && v2 > 0 && v2 < 40,
        format!("{wrong:?} / compiled caller: {nc}"),
    );
    let r = sql(&mut c, "ALTER PROCEDURE pick ROLLOUT TO 80").await;
    let mut wrong = Vec::new();
    for k in 0..40u64 {
        let w = want(&k.to_string(), module, 80);
        let rows = must(&mut c, &format!("CALL via_c({k})")).await;
        if rows[0][1].as_deref() != Some(w) {
            wrong.push(k);
        }
    }
    ck.check(
        "a larger stage keeps the key: ROLLOUT TO 80 keeps BY KEY k",
        r.is_ok() && wrong.is_empty(),
        format!("{r:?} {wrong:?}"),
    );
    let r = sql(&mut c, "ALTER PROCEDURE pick ROLLOUT TO 50 BY USER").await;
    let by_user = r.is_ok();
    let r2 = sql(&mut c, "ALTER PROCEDURE pick ROLLOUT TO 50 BY KEY").await;
    ck.check(
        "BY USER switches the key away; BY KEY then needs the parameter named at deploy",
        by_user && matches!(&r2, Err((39861, _))),
        format!("{r:?} / {r2:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE OR REPLACE PROCEDURE pick(k BIGINT) LANGUAGE javascript \
         WITH (SPRIG_ROLLOUT = 10, SPRIG_ROLLOUT_BY = 'key:nope') AS $$ return { v: 'v3', k }; $$",
    )
    .await;
    ck.check(
        "a key naming no parameter is refused (39870)",
        matches!(&r, Err((39870, _))),
        format!("{r:?}"),
    );

    // ---- a stream trigger rolled out by record key -----------------------------------------
    must(&mut c, &format!("CREATE STREAM {DB}.ev PARTITIONS 2")).await;
    let trig = |v: &str, with: &str| {
        format!(
            "CREATE OR REPLACE TRIGGER tk ON STREAM {DB}.ev LANGUAGE javascript {with} AS $$
export default function (ctx) {{
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO seen (src, k, n, v) VALUES (?, ?, ?, ?)', ['s', ctx.record.key ?? '-', e.n, '{v}']);
}}
$$"
        )
    };
    let r = sql(&mut c, &trig("v1", "")).await;
    let n1 = notes(&mut c).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let r2 = sql(
        &mut c,
        &trig("v2", "WITH (SPRIG_ROLLOUT = 50, SPRIG_TIER = 'engine')"),
    )
    .await;
    let n2 = notes(&mut c).await;
    ck.check(
        "a stream trigger's candidate (engine tier) rolls out by key over its compiled version",
        r.is_ok() && n1.contains("tier mariadb") && r2.is_ok() && n2.contains("50% of keys"),
        format!("{r:?} {n1} / {r2:?} {n2}"),
    );
    let tmod = module_of(&n2);
    for n in 0..2 {
        let mut values: Vec<String> = (0..30)
            .map(|k| format!("('k{k}', '{{\"n\": {n}}}')"))
            .collect();
        values.push(format!("(NULL, '{{\"n\": {n}}}')"));
        must(
            &mut c,
            &format!(
                "INSERT INTO {DB}.ev (key, value) VALUES {}",
                values.join(", ")
            ),
        )
        .await;
    }
    let rows = wait_rows(&mut c, "SELECT k, n, v FROM seen WHERE src = 's'", 62, 90).await;
    let mut wrong = Vec::new();
    for r in &rows {
        let (k, v) = (r[0].clone().unwrap(), r[2].clone().unwrap());
        let w = if k == "-" { "v1" } else { want(&k, tmod, 50) };
        if v != w {
            wrong.push(format!("{k}/{}: {v} want {w}", r[1].clone().unwrap()));
        }
    }
    let on_cand = rows
        .iter()
        .filter(|r| r[2].as_deref() == Some("v2"))
        .count();
    ck.check(
        &format!(
            "each record ran once with its key's version, the same key alike both times, a \
             record without a key on the active version ({on_cand} of 62 on the candidate)"
        ),
        rows.len() == 62 && wrong.is_empty() && on_cand > 0 && on_cand < 62,
        format!("{} rows, {wrong:?}", rows.len()),
    );
    let r = sql(&mut c, "ALTER TRIGGER tk ROLLOUT PROMOTE").await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    let values: Vec<String> = (0..10)
        .map(|k| format!("('k{k}', '{{\"n\": 2}}')"))
        .collect();
    must(
        &mut c,
        &format!(
            "INSERT INTO {DB}.ev (key, value) VALUES {}",
            values.join(", ")
        ),
    )
    .await;
    let rows = wait_rows(
        &mut c,
        "SELECT v FROM seen WHERE src = 's' AND n = 2",
        10,
        90,
    )
    .await;
    ck.check(
        "after ROLLOUT PROMOTE every record runs the candidate",
        r.is_ok() && rows.len() == 10 && rows.iter().all(|r| r[0].as_deref() == Some("v2")),
        format!("{r:?} {rows:?}"),
    );

    // ---- an after-commit trigger rolled out by its row's key -------------------------------
    // The key is the row's primary key as `id=<value>`; the worker picks
    // per event, so an insert and the update after it run the same
    // version.
    must(
        &mut c,
        "CREATE TABLE src (id BIGINT PRIMARY KEY, n INT NOT NULL)",
    )
    .await;
    let at = |v: &str, events: &str, with: &str| {
        format!(
            "CREATE OR REPLACE TRIGGER ak AFTER COMMIT ON src FOR {events} LANGUAGE javascript \
             {with} AS $$
export default function (row) {{
  db.exec('INSERT INTO seen (src, k, n, v) VALUES (?, ?, ?, ?)', ['a', String(row.id), row.n, '{v}']);
}}
$$"
        )
    };
    let r = sql(&mut c, &at("v1", "INSERT, UPDATE", "")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let r2 = sql(
        &mut c,
        &at("v2", "INSERT, UPDATE", "WITH (SPRIG_ROLLOUT = 50)"),
    )
    .await;
    let n2 = notes(&mut c).await;
    ck.check(
        "an after-commit trigger's candidate rolls out by its row's key",
        r.is_ok() && r2.is_ok() && n2.contains("50% of keys"),
        format!("{r:?} / {r2:?} {n2}"),
    );
    let amod = module_of(&n2);
    let r3 = sql(&mut c, &at("v3", "INSERT", "WITH (SPRIG_ROLLOUT = 10)")).await;
    ck.check(
        "a staged rollout that changes the trigger's events is refused (39870)",
        matches!(&r3, Err((39870, _))),
        format!("{r3:?}"),
    );
    // The worker reads the module again within two seconds.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let values: Vec<String> = (0..40).map(|k| format!("({k}, 0)")).collect();
    must(
        &mut c,
        &format!("INSERT INTO src (id, n) VALUES {}", values.join(", ")),
    )
    .await;
    wait_rows(&mut c, "SELECT k FROM seen WHERE src = 'a'", 40, 90).await;
    must(&mut c, "UPDATE src SET n = 1").await;
    let rows = wait_rows(&mut c, "SELECT k, n, v FROM seen WHERE src = 'a'", 80, 90).await;
    let mut wrong = Vec::new();
    for r in &rows {
        let (k, v) = (r[0].clone().unwrap(), r[2].clone().unwrap());
        let w = want(&format!("id={k}"), amod, 50);
        if v != w {
            wrong.push(format!("{k}/{}: {v} want {w}", r[1].clone().unwrap()));
        }
    }
    let on_cand = rows
        .iter()
        .filter(|r| r[2].as_deref() == Some("v2"))
        .count();
    ck.check(
        &format!(
            "each insert and update ran once with its row's version, a row's update as its \
             insert ({on_cand} of 80 on the candidate)"
        ),
        rows.len() == 80 && wrong.is_empty() && on_cand > 0 && on_cand < 80,
        format!("{} rows, {wrong:?}", rows.len()),
    );
    let r = sql(&mut c, "ALTER TRIGGER ak ROLLOUT PROMOTE").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    must(&mut c, "UPDATE src SET n = 2").await;
    let rows = wait_rows(
        &mut c,
        "SELECT v FROM seen WHERE src = 'a' AND n = 2",
        40,
        90,
    )
    .await;
    ck.check(
        "after ROLLOUT PROMOTE every row runs the candidate",
        r.is_ok() && rows.len() == 40 && rows.iter().all(|r| r[0].as_deref() == Some("v2")),
        format!("{r:?} {} rows", rows.len()),
    );

    // ---- a queue trigger rolled out by message id -------------------------------------------
    if let Some(amqp) = skip::var("SPRIG_TEST_LOGIC_AMQP") {
        let (user, password) = user();
        let a = Connection::connect(
            &format!("amqp://{user}:{password}@{amqp}/{DB}"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let ch = a.create_channel().await.unwrap();
        ch.queue_declare(
            "work".into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        let qt = |v: &str, with: &str| {
            format!(
                "CREATE OR REPLACE TRIGGER qk ON QUEUE {DB}.work LANGUAGE javascript {with} AS $$
export default function (ctx) {{
  const m = JSON.parse(ctx.message.body);
  db.exec('INSERT INTO seen (src, k, n, v) VALUES (?, ?, ?, ?)', ['q', m.id, 0, '{v}']);
}}
$$"
            )
        };
        must(&mut c, &qt("v1", "WITH (SPRIG_TIER = 'engine')")).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let r = sql(&mut c, &qt("v2", "WITH (SPRIG_ROLLOUT = 50)")).await;
        let n = notes(&mut c).await;
        let qmod = module_of(&n);
        tokio::time::sleep(Duration::from_secs(1)).await;
        for i in 0..20 {
            ch.basic_publish(
                "".into(),
                "work".into(),
                BasicPublishOptions::default(),
                format!("{{\"id\": \"m{i}\"}}").as_bytes(),
                BasicProperties::default()
                    .with_delivery_mode(2)
                    .with_message_id(format!("m{i}").into()),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        }
        let rows = wait_rows(&mut c, "SELECT k, v FROM seen WHERE src = 'q'", 20, 90).await;
        let wrong: Vec<String> = rows
            .iter()
            .filter(|r| r[1].as_deref() != Some(want(r[0].as_deref().unwrap(), qmod, 50)))
            .map(|r| format!("{r:?}"))
            .collect();
        ck.check(
            "a queue trigger's candidate (compiled) runs the messages whose message-id falls in \
             its share, the engine-tier version the rest",
            r.is_ok() && rows.len() == 20 && wrong.is_empty(),
            format!("{r:?} {n} / {} rows {wrong:?}", rows.len()),
        );
        let r = sql(&mut c, "ALTER TRIGGER qk ROLLOUT ABORT").await;
        ck.check(
            "ALTER TRIGGER ... ROLLOUT ABORT ends it",
            r.is_ok(),
            format!("{r:?}"),
        );
        a.close(200, "done".into()).await.unwrap();
    } else {
        println!("no SPRIG_TEST_LOGIC_AMQP: the queue part is skipped");
    }

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!("took {:.1} s", started.elapsed().as_secs_f64());
    println!(
        "rollout by key: {} of {} checks pass",
        ck.passed,
        ck.passed + ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
