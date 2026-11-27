//! Read load and read-your-writes checks against SprigDB's MySQL and
//! MariaDB ports, for reads served on copies (reader sessions: a database
//! named `<db>@replica`).
//!
//! ```text
//! readload prepare <host:port/db> <rows>
//! readload point <secs> <conns> <rows> <uniform|zipf> <host:port/db>[,<host:port/db>...]
//! readload ryw <secs> <rows> <writers> <host:port/db (writes)> <host:port/db>[,...] (reads)
//! ```
//!
//! Environment: `RL_USER`, `RL_PASS` (the tenant login), `RL_TLS=1` for
//! TLS. `point` runs prepared point selects on `sbtest1` (sysbench's
//! shape: `id`, `k`, `c`, `pad`) over `conns` connections spread over the
//! endpoints in turn, and prints statements per second and latency
//! percentiles. `ryw` runs writers that bump a counter row and pass their
//! session's `@sprig_read_token` to a reader connection on another node,
//! which must then read the value just written (never an older one), and
//! checks every reader connection's values never go back.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::*;
use mysql_async::{Conn, OptsBuilder, SslOpts};

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[derive(Clone, Debug)]
struct Endpoint {
    host: String,
    port: u16,
    db: String,
}

fn endpoint(s: &str) -> Endpoint {
    let (addr, db) = s.split_once('/').unwrap_or((s, ""));
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Endpoint {
        host: host.to_owned(),
        port: port.parse().expect("port"),
        db: db.to_owned(),
    }
}

async fn connect(e: &Endpoint) -> mysql_async::Result<Conn> {
    let mut o = OptsBuilder::default()
        .ip_or_hostname(e.host.clone())
        .tcp_port(e.port)
        .user(Some(env("RL_USER", "nfa.app")))
        .pass(Some(env("RL_PASS", "nf-app-1")))
        .prefer_socket(false)
        .stmt_cache_size(64);
    if !e.db.is_empty() {
        o = o.db_name(Some(e.db.clone()));
    }
    if env("RL_TLS", "0") == "1" {
        o = o.ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        );
    }
    Conn::new(o).await
}

fn code(e: &mysql_async::Error) -> String {
    match e {
        mysql_async::Error::Server(s) => s.code.to_string(),
        mysql_async::Error::Io(_) => "io".into(),
        mysql_async::Error::Driver(_) => "driver".into(),
        _ => "other".into(),
    }
}

/// YCSB's Zipfian generator (theta 0.99) over `0..n`.
struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64) -> Self {
        let theta = 0.99;
        let zeta = |m: u64| (1..=m).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let zetan = zeta(n);
        let zeta2 = zeta(2);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self {
            n,
            theta,
            alpha,
            zetan,
            eta,
        }
    }

    fn next(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let v = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        v.min(self.n - 1)
    }
}

/// A small xorshift generator (no dependency).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

async fn prepare(e: &Endpoint, rows: u64) {
    let mut c = connect(e).await.expect("connect");
    c.query_drop("DROP TABLE IF EXISTS sbtest1").await.unwrap();
    c.query_drop(
        "CREATE TABLE sbtest1 (id INT NOT NULL PRIMARY KEY, k INT NOT NULL DEFAULT 0, \
         c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '')",
    )
    .await
    .unwrap();
    c.query_drop("DROP TABLE IF EXISTS kv").await.unwrap();
    c.query_drop("CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut rng = Rng(42);
    let mut i = 1u64;
    while i <= rows {
        let end = (i + 500).min(rows + 1);
        let values: Vec<String> = (i..end)
            .map(|id| {
                let c: String = (0..11)
                    .map(|_| format!("{:010}", rng.next() % 10_000_000_000))
                    .collect::<Vec<_>>()
                    .join("-");
                format!(
                    "({id},{},'{}','pad-{id}')",
                    rng.next() % rows,
                    &c[..c.len().min(119)]
                )
            })
            .collect();
        c.query_drop(format!("INSERT INTO sbtest1 VALUES {}", values.join(",")))
            .await
            .unwrap();
        i = end;
    }
    let kv: Vec<String> = (0..1000).map(|k| format!("({k},0)")).collect();
    c.query_drop(format!("INSERT INTO kv VALUES {}", kv.join(",")))
        .await
        .unwrap();
    println!("prepared {rows} rows in sbtest1 and 1000 in kv");
}

async fn point(secs: u64, conns: usize, rows: u64, zipf: bool, eps: Vec<Endpoint>) {
    let done = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(std::sync::Mutex::new(HashMap::<String, u64>::new()));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let z = Arc::new(Zipf::new(rows));
    let mut hs = Vec::new();
    for i in 0..conns {
        let e = eps[i % eps.len()].clone();
        let (done, errors, z) = (Arc::clone(&done), Arc::clone(&errors), Arc::clone(&z));
        hs.push(tokio::spawn(async move {
            let mut lat: Vec<u32> = Vec::with_capacity(100_000);
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (i as u64 + 1));
            let mut c = None;
            while Instant::now() < deadline {
                if c.is_none() {
                    match connect(&e).await {
                        Ok(x) => c = Some(x),
                        Err(err) => {
                            *errors
                                .lock()
                                .unwrap()
                                .entry(format!("connect {}", code(&err)))
                                .or_default() += 1;
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                }
                let id = if zipf {
                    z.next(rng.unit()) + 1
                } else {
                    rng.next() % rows + 1
                };
                let t = Instant::now();
                let r: mysql_async::Result<Option<String>> = c
                    .as_mut()
                    .unwrap()
                    .exec_first("SELECT c FROM sbtest1 WHERE id = ?", (id,))
                    .await;
                match r {
                    Ok(Some(_)) => {
                        lat.push(u32::try_from(t.elapsed().as_micros()).unwrap_or(u32::MAX));
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        *errors
                            .lock()
                            .unwrap()
                            .entry("missing row".into())
                            .or_default() += 1;
                    }
                    Err(err) => {
                        *errors.lock().unwrap().entry(code(&err)).or_default() += 1;
                        if matches!(err, mysql_async::Error::Io(_)) {
                            c = None;
                        }
                    }
                }
            }
            if let Some(c) = c {
                let _ = c.disconnect().await;
            }
            lat
        }));
    }
    let started = Instant::now();
    let mut lat = Vec::new();
    for h in hs {
        lat.extend(h.await.unwrap());
    }
    let wall = started.elapsed().as_secs_f64().min(secs as f64);
    lat.sort_unstable();
    let p = |q: f64| {
        lat.get(((lat.len() as f64 * q) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let n = done.load(Ordering::Relaxed);
    println!(
        "point: {conns} conns over {} endpoints, {secs} s: {n} selects, {:.0}/s, p50 {} us, p99 {} us, errors {:?}",
        eps.len(),
        n as f64 / wall,
        p(0.5),
        p(0.99),
        errors.lock().unwrap()
    );
}

async fn ryw(secs: u64, writers: usize, write_ep: Endpoint, read_eps: Vec<Endpoint>) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let checked = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));
    let backwards = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(std::sync::Mutex::new(HashMap::<String, u64>::new()));
    let mut hs = Vec::new();
    for w in 0..writers {
        let (we, reps) = (write_ep.clone(), read_eps.clone());
        let (checked, violations, backwards, errors) = (
            Arc::clone(&checked),
            Arc::clone(&violations),
            Arc::clone(&backwards),
            Arc::clone(&errors),
        );
        hs.push(tokio::spawn(async move {
            let note = |k: String| {
                *errors.lock().unwrap().entry(k).or_default() += 1;
            };
            let mut rng = Rng(0xA5A5_1234 ^ (w as u64 + 7));
            let mut wc = None;
            // One reader connection per read endpoint, each remembering the
            // highest value it saw per key (its reads must never go back).
            let mut readers: Vec<(Option<Conn>, HashMap<u64, i64>)> =
                reps.iter().map(|_| (None, HashMap::new())).collect();
            let mut val: HashMap<u64, i64> = HashMap::new();
            while Instant::now() < deadline {
                if wc.is_none() {
                    match connect(&we).await {
                        Ok(c) => wc = Some(c),
                        Err(e) => {
                            note(format!("writer connect {}", code(&e)));
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                }
                // Keys of this writer only: k = w + writers * j.
                let k = w as u64 + writers as u64 * (rng.next() % (1000 / writers as u64).max(1));
                let v = val.get(&k).copied().unwrap_or(0) + 1;
                let c = wc.as_mut().unwrap();
                if let Err(e) = c
                    .exec_drop("UPDATE kv SET v = ? WHERE k = ?", (v, k))
                    .await
                {
                    note(format!("write {}", code(&e)));
                    wc = None;
                    continue;
                }
                // The write is acknowledged; the value is now v.
                val.insert(k, v);
                let token: Option<String> = match c.query_first("SELECT @sprig_read_token").await {
                    Ok(t) => t,
                    Err(e) => {
                        note(format!("token {}", code(&e)));
                        wc = None;
                        continue;
                    }
                };
                let Some(token) = token else {
                    note("token NULL".into());
                    continue;
                };
                let i = usize::try_from(rng.next() % readers.len() as u64).unwrap();
                let exact = rng.next().is_multiple_of(4);
                let (rc, seen) = &mut readers[i];
                if rc.is_none() {
                    match connect(&reps[i]).await {
                        Ok(c) => *rc = Some(c),
                        Err(e) => {
                            note(format!("reader connect {}", code(&e)));
                            continue;
                        }
                    }
                }
                let r = rc.as_mut().unwrap();
                if let Err(e) = r
                    .query_drop(format!("SET @sprig_read_token = '{token}'"))
                    .await
                {
                    note(format!("set token {}", code(&e)));
                    *rc = None;
                    continue;
                }
                let sql = if exact {
                    "SELECT /*+ READ_FROM(leader) */ v FROM kv WHERE k = ?"
                } else {
                    "SELECT v FROM kv WHERE k = ?"
                };
                let got: mysql_async::Result<Option<i64>> = r.exec_first(sql, (k,)).await;
                match got {
                    Ok(Some(g)) => {
                        checked.fetch_add(1, Ordering::Relaxed);
                        if g < v {
                            violations.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "VIOLATION: endpoint {:?} key {k} read {g} after write {v} (token {token}, exact {exact})",
                                reps[i]
                            );
                        }
                        let last = seen.entry(k).or_insert(g);
                        if g < *last {
                            backwards.fetch_add(1, Ordering::Relaxed);
                            eprintln!("BACKWARDS: endpoint {:?} key {k} read {g} after {last}", reps[i]);
                        }
                        *last = (*last).max(g);
                    }
                    Ok(None) => note("read missing".into()),
                    Err(e) => {
                        note(format!("read {}", code(&e)));
                        if matches!(e, mysql_async::Error::Io(_)) {
                            *rc = None;
                        }
                    }
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    println!(
        "ryw: {writers} writers, {} read endpoints, {secs} s: {} checked reads, {} older than the token, {} went back, errors {:?}",
        read_eps.len(),
        checked.load(Ordering::Relaxed),
        violations.load(Ordering::Relaxed),
        backwards.load(Ordering::Relaxed),
        errors.lock().unwrap()
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let eps = |s: &str| s.split(',').map(endpoint).collect::<Vec<_>>();
    match a.first().map(String::as_str) {
        Some("prepare") => prepare(&endpoint(&a[1]), a[2].parse().unwrap()).await,
        Some("point") => {
            point(
                a[1].parse().unwrap(),
                a[2].parse().unwrap(),
                a[3].parse().unwrap(),
                a[4] == "zipf",
                eps(&a[5]),
            )
            .await;
        }
        Some("ryw") => {
            let _rows: u64 = a[2].parse().unwrap();
            ryw(
                a[1].parse().unwrap(),
                a[3].parse().unwrap(),
                endpoint(&a[4]),
                eps(&a[5]),
            )
            .await;
        }
        _ => {
            eprintln!("usage: see the module docs");
            std::process::exit(2);
        }
    }
}
