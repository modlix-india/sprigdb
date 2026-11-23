//! kill -9 of a process that commits transactions on two tenants and
//! consumes their change logs at the same time.
//!
//! The child commits numbered transactions per tenant (one writer thread
//! each; some transactions write a few rows, some a few thousand), prints
//! an acknowledgement for each once it is durable, and runs one consumer
//! per tenant that reads the change log in small pages and appends every
//! change with its place to a file, resuming after the last whole line of
//! that file when it starts.
//!
//! After every kill the parent opens the engine and the change log and
//! checks, per tenant: the change log holds whole transactions, numbered
//! 1..=N with no gap, each once, in order, with strictly increasing places;
//! every acknowledged transaction is there; the store's rows equal the last
//! change of each key; the resolved timestamp never went back; and what the
//! consumer wrote is exactly a prefix of the change log (nothing lost or
//! repeated across the kills).
//!
//! Rounds: `SPRIG_KILL9_ROUNDS` (default 5). The child is this test
//! binary, run with `SPRIG_CHG_KILL9_DIR`.

#![allow(
    clippy::type_complexity,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use sprig_engine::keys::{KeyPrefix, KeyRange, kind};
use sprig_engine::txn::{Dir, ReadMode, TxnOptions};
use sprig_engine::{Engine, EngineConfig};
use sprig_streams::changes::{ChangeConfig, ChangeLog, Pos};
use sprig_streams::seg::testing::TempDir;

const DIR_VAR: &str = "SPRIG_CHG_KILL9_DIR";
const TENANTS: [u64; 2] = [31, 32];

fn open(dir: &Path) -> (Engine, ChangeLog) {
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.budget.engine_mb = 64;
    cfg.checkpoint_interval = Duration::from_millis(200);
    let until = Instant::now() + Duration::from_secs(10);
    let e = loop {
        match Engine::open(cfg.clone()) {
            Ok(e) => break e,
            Err(e) if e.message.contains("lock") && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("{e}"),
        }
    };
    let l = ChangeLog::open(
        &e,
        dir.join("changes"),
        ChangeConfig {
            segment_bytes: 256 << 10,
            index_interval: 4096,
            flush_every: Duration::from_millis(40),
            ..ChangeConfig::default()
        },
    )
    .unwrap();
    (e, l)
}

fn row(tenant: u64, i: u32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 5,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn unit(tenant: u64) -> u64 {
    Engine::unit_of_tenant(tenant).0
}

/// Rows a transaction writes: mostly a few, every 25th a few thousand.
fn rows_of(n: u64) -> u32 {
    if n.is_multiple_of(25) {
        3000
    } else {
        1 + (n % 3) as u32
    }
}

/// The highest transaction number in the store for `tenant` (its counter
/// row), so a restarted child goes on from there.
fn last_txn(e: &Engine, tenant: u64) -> u64 {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    let v = t.get_snapshot(&row(tenant, u32::MAX)).unwrap();
    t.rollback();
    v.map_or(0, |v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
}

fn hex(b: &[u8]) -> Vec<u8> {
    b.iter()
        .flat_map(|x| format!("{x:02x}").into_bytes())
        .collect()
}

fn consumed_path(dir: &Path, tenant: u64) -> std::path::PathBuf {
    dir.join(format!("consumed-{tenant}.log"))
}

/// The whole lines of the consumer's file (a torn last line is cut).
fn consumed(dir: &Path, tenant: u64) -> Vec<(u64, u32, u64, Vec<u8>)> {
    let p = consumed_path(dir, tenant);
    let Ok(b) = std::fs::read(&p) else {
        return Vec::new();
    };
    let whole = b.iter().rposition(|c| *c == b'\n').map_or(0, |i| i + 1);
    if whole < b.len() {
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(whole as u64).unwrap();
    }
    String::from_utf8_lossy(&b[..whole])
        .lines()
        .map(|l| {
            let f: Vec<&str> = l.split(' ').collect();
            (
                f[0].parse().unwrap(),
                f[1].parse().unwrap(),
                f[2].parse().unwrap(),
                f[3].as_bytes().to_vec(),
            )
        })
        .collect()
}

#[test]
#[ignore = "run by kill9_change_log_and_consumers_resume_exactly"]
fn changes_kill9_child() {
    let Ok(dir) = std::env::var(DIR_VAR) else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (e, l) = open(&dir);
    l.spawn_background();
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for tenant in TENANTS {
        // The writer.
        let e2 = e.clone();
        hs.push(std::thread::spawn(move || {
            let mut n = last_txn(&e2, tenant);
            loop {
                n += 1;
                let mut t = e2.begin(TxnOptions::default()).unwrap();
                for i in 0..rows_of(n) {
                    let v = format!("t{n}r{i}");
                    t.put(row(tenant, (n as u32 % 50) * 4000 + i), v.into_bytes())
                        .unwrap();
                }
                t.put(row(tenant, u32::MAX), n.to_be_bytes().to_vec())
                    .unwrap();
                let c = t.commit().unwrap();
                c.wait().unwrap();
                let mut o = std::io::stdout().lock();
                writeln!(o, "ack {tenant} {n}").unwrap();
                o.flush().unwrap();
            }
        }));
        // The consumer.
        let l2 = l.clone();
        let d2 = dir.clone();
        let stop2 = Arc::clone(&stop);
        hs.push(std::thread::spawn(move || {
            let have = consumed(&d2, tenant);
            let after = have
                .last()
                .map_or(Pos::default(), |(i, s, _, _)| Pos { index: *i, seq: *s });
            let mut r = l2.reader(unit(tenant), after, None).unwrap();
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(consumed_path(&d2, tenant))
                .unwrap();
            while !stop2.load(Ordering::Relaxed) {
                let p = r.read(4096).unwrap();
                let mut buf = Vec::new();
                for c in &p.changes {
                    let v = c.value.clone().unwrap_or_default();
                    buf.extend_from_slice(format!("{} {} {} ", c.index, c.seq, c.cts).as_bytes());
                    buf.extend_from_slice(&hex(&v));
                    buf.push(b'\n');
                }
                // Written in pieces, so a kill can tear a line.
                for chunk in buf.chunks(700) {
                    f.write_all(chunk).unwrap();
                }
                if p.at_end {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }));
    }
    for h in hs {
        let _ = h.join();
    }
}

struct Tenant {
    /// Changes: (index, seq, cts, key, value).
    log: Vec<(u64, u32, u64, Vec<u8>, Vec<u8>)>,
}

fn read_log(l: &ChangeLog, tenant: u64) -> Tenant {
    let mut r = l.reader(unit(tenant), Pos::default(), None).unwrap();
    let mut log = Vec::new();
    loop {
        let p = r.read(64 << 10).unwrap();
        for c in p.changes {
            log.push((c.index, c.seq, c.cts, c.key, c.value.unwrap_or_default()));
        }
        if p.at_end {
            return Tenant { log };
        }
    }
}

fn check(dir: &Path, acked: &BTreeMap<u64, u64>, resolved: &mut BTreeMap<u64, u64>) -> u64 {
    let (e, l) = open(dir);
    let mut total = 0;
    for tenant in TENANTS {
        let t = read_log(&l, tenant);
        // Places strictly increase.
        for w in t.log.windows(2) {
            assert!((w[0].0, w[0].1) < (w[1].0, w[1].1), "places out of order");
        }
        // Whole transactions numbered 1..=N, each once, in order.
        let mut by_entry: BTreeMap<u64, Vec<&(u64, u32, u64, Vec<u8>, Vec<u8>)>> = BTreeMap::new();
        for c in &t.log {
            by_entry.entry(c.0).or_default().push(c);
        }
        let mut want = 1u64;
        let mut last: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for recs in by_entry.values() {
            let counter = recs
                .iter()
                .find(|c| c.3 == row(tenant, u32::MAX))
                .expect("every transaction writes its counter");
            let n = u64::from_be_bytes(counter.4.as_slice().try_into().unwrap());
            assert_eq!(
                n, want,
                "tenant {tenant}: transaction {want} missing or repeated"
            );
            assert_eq!(
                recs.len() as u32,
                rows_of(n) + 1,
                "transaction {n} not whole"
            );
            for c in recs {
                assert_eq!(c.1 as usize, recs.iter().position(|x| x.1 == c.1).unwrap());
                if c.3 != row(tenant, u32::MAX) {
                    assert!(c.4.starts_with(format!("t{n}r").as_bytes()));
                }
                last.insert(c.3.clone(), c.4.clone());
            }
            want += 1;
        }
        let n_log = want - 1;
        let n_acked = acked.get(&tenant).copied().unwrap_or(0);
        assert!(
            n_log >= n_acked,
            "tenant {tenant}: acknowledged {n_acked}, log holds {n_log}"
        );
        // The store agrees with the log.
        let mut tx = e.begin(TxnOptions::default()).unwrap();
        let mut p = KeyPrefix {
            tenant,
            shard: 0,
            object: 5,
            kind: kind::PRIMARY,
            index: 0,
        }
        .to_vec();
        p.truncate(p.len());
        let rows = tx
            .scan(
                &KeyRange::prefix(&p),
                Dir::Forward,
                usize::MAX,
                ReadMode::Snapshot,
            )
            .unwrap();
        tx.rollback();
        assert_eq!(
            rows.len(),
            last.len(),
            "tenant {tenant}: store and log differ"
        );
        for (k, v) in rows {
            assert_eq!(
                last.get(&k).map(Vec::as_slice),
                Some(v.as_ref()),
                "tenant {tenant}"
            );
        }
        // The consumer's file is a prefix of the log.
        let got = consumed(dir, tenant);
        assert!(got.len() <= t.log.len());
        for (i, c) in got.iter().enumerate() {
            let w = &t.log[i];
            assert_eq!(
                (c.0, c.1, c.2, &c.3),
                (w.0, w.1, w.2, &hex(&w.4)),
                "consumer diverged at {i}"
            );
        }
        // Resolved timestamps never go back.
        let r = l.resolved(unit(tenant));
        let before = resolved.insert(tenant, r).unwrap_or(0);
        assert!(r >= before, "resolved timestamp went back: {before} -> {r}");
        total += n_log;
    }
    total
}

#[test]
fn kill9_change_log_and_consumers_resume_exactly() {
    let rounds: u32 = std::env::var("SPRIG_KILL9_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let d = TempDir::new("chg-kill9");
    let exe = std::env::current_exe().unwrap();
    let mut acked: BTreeMap<u64, u64> = BTreeMap::new();
    let mut resolved = BTreeMap::new();
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args([
                "changes_kill9_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DIR_VAR, d.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            let mut got: Vec<(u64, u64)> = Vec::new();
            for line in BufReader::new(out)
                .lines()
                .map_while(std::result::Result::ok)
            {
                let f: Vec<&str> = line.split(' ').collect();
                if f.len() == 3 && f[0] == "ack" {
                    got.push((f[1].parse().unwrap(), f[2].parse().unwrap()));
                }
            }
            got
        });
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let ms = 400 + seed % 1200;
        std::thread::sleep(Duration::from_millis(ms));
        let _ = child.kill();
        let _ = child.wait();
        for (t, n) in reader.join().unwrap() {
            let e = acked.entry(t).or_insert(0);
            *e = (*e).max(n);
        }
        let total = check(d.path(), &acked, &mut resolved);
        eprintln!(
            "round {round}: killed after {ms} ms; {total} transactions in the change logs; acked {acked:?}"
        );
    }
}
