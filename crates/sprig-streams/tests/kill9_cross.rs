//! kill -9 of a process whose transactions span tenants: each moves money
//! between accounts of two tenants (two units), writes a marker row, and
//! publishes a record into a third tenant's stream, all in one engine
//! transaction (a commit across three units). Some transactions publish and
//! then roll back.
//!
//! After every kill the parent recovers the engine and the stream log and
//! checks: the money total is preserved; every record in the stream belongs
//! to a committed transaction (its marker row is there) and every committed
//! transaction's record is there exactly once (the record appears exactly
//! when the transaction commits); no rolled-back record ever appears; every
//! acknowledged transaction is there; nothing is left unresolved.
//!
//! Rounds: `SPRIG_KILL9_ROUNDS` (default 5). `SPRIG_CROSS_WAIT=1` runs the
//! protocol with waits between its steps (as on a replicated log). The
//! child is this test binary, run with `SPRIG_KILL9_DIR`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sprig_engine::keys::{KeyPrefix, KeyRange, kind};
use sprig_engine::txn::{Dir, ReadMode, TxnOptions};
use sprig_engine::{Engine, EngineConfig};
use sprig_streams::seg::testing::{TempDir, batch_with, records_of};
use sprig_streams::seg::{Key, SegConfig, SegLog};

const DIR_VAR: &str = "SPRIG_KILL9_DIR";
const ROUND_VAR: &str = "SPRIG_KILL9_ROUND";
const BANKS: [u64; 4] = [11, 12, 13, 14];
const PER_BANK: u32 = 4;
const START: u64 = 1_000;
const STREAM_TENANT: u64 = 20;
const PARTS: u32 = 2;
const THREADS: u32 = 6;

fn wait_mode() -> bool {
    std::env::var("SPRIG_CROSS_WAIT").is_ok_and(|v| v == "1")
}

fn open(dir: &Path) -> (Engine, SegLog) {
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.budget.engine_mb = 64;
    cfg.checkpoint_interval = Duration::from_millis(300);
    let e = Engine::open(cfg).unwrap();
    e.txn_shared().set_cross_unit_wait(wait_mode());
    let l = SegLog::open(
        &e,
        dir.join("streams"),
        SegConfig {
            segment_bytes: 64 << 10,
            flush_every: Duration::from_millis(50),
            ..SegConfig::default()
        },
    )
    .unwrap();
    (e, l)
}

fn prefix(tenant: u64, object: u32) -> Vec<u8> {
    KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec()
}

fn account(i: u32) -> (u64, Vec<u8>) {
    let tenant = BANKS[(i % BANKS.len() as u32) as usize];
    let mut k = prefix(tenant, 1);
    k.extend_from_slice(&i.to_be_bytes());
    (tenant, k)
}

fn marker(tenant: u64, tag: &str) -> Vec<u8> {
    let mut k = prefix(tenant, 2);
    k.extend_from_slice(tag.as_bytes());
    k
}

fn accounts() -> u32 {
    BANKS.len() as u32 * PER_BANK
}

/// The child: transfer and publish until killed.
#[test]
#[ignore = "run by kill9_keeps_transactions_across_tenants_whole"]
fn kill9_cross_child() {
    let Ok(dir) = std::env::var(DIR_VAR) else {
        return;
    };
    let round: u32 = std::env::var(ROUND_VAR).unwrap().parse().unwrap();
    let (e, l) = open(Path::new(&dir));
    if round == 0 {
        let mut t = e.begin(TxnOptions::mariadb()).unwrap();
        for i in 0..accounts() {
            t.put(account(i).1, START.to_be_bytes().to_vec()).unwrap();
        }
        t.commit().unwrap().wait().unwrap();
    }
    println!();
    println!("READY");
    std::io::stdout().flush().unwrap();
    let l = std::sync::Arc::new(l);
    let hs: Vec<_> = (0..THREADS)
        .map(|task| {
            let (e, l) = (e.clone(), std::sync::Arc::clone(&l));
            std::thread::spawn(move || {
                let mut seed = u64::from(round) * 1_000 + u64::from(task) + 1;
                for n in 0u32.. {
                    seed = seed
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let a = (seed >> 33) as u32 % accounts();
                    let b = (a + 1 + (seed >> 45) as u32 % (accounts() - 1)) % accounts();
                    let (lo, hi) = (a.min(b), a.max(b));
                    let Ok(mut t) = e.begin(TxnOptions::mariadb()) else {
                        continue;
                    };
                    let mut bal = [0u64; 2];
                    let mut ok = true;
                    for (slot, i) in [lo, hi].into_iter().enumerate() {
                        match t
                            .get(&account(i).1, ReadMode::ForUpdate)
                            .and_then(sprig_engine::store::Read::expect_ready)
                        {
                            Ok(Some(v)) => {
                                bal[slot] = u64::from_be_bytes(v.as_ref().try_into().unwrap());
                            }
                            _ => ok = false,
                        }
                    }
                    if !ok {
                        continue;
                    }
                    let (from, to) = if a == lo { (0, 1) } else { (1, 0) };
                    let amount = (seed >> 55) % 9;
                    if bal[from] < amount {
                        continue;
                    }
                    bal[from] -= amount;
                    bal[to] += amount;
                    t.put(account(lo).1, bal[0].to_be_bytes().to_vec()).unwrap();
                    t.put(account(hi).1, bal[1].to_be_bytes().to_vec()).unwrap();
                    let rollback = n % 4 == 3;
                    let tag = format!("r{round}-t{task}-n{n}");
                    t.put(marker(account(lo).0, &tag), b"1".to_vec()).unwrap();
                    let value = format!("{}-{tag}", if rollback { "RB" } else { "OK" });
                    let key = Key {
                        tenant: STREAM_TENANT,
                        stream: 1,
                        partition: task % PARTS,
                    };
                    let batch = batch_with(&[(None, Some(value.into_bytes()))], 1_000, None);
                    if l.append_in(&mut t, key, &batch).is_err() {
                        continue;
                    }
                    if rollback {
                        t.rollback();
                        continue;
                    }
                    let Ok(c) = t.commit() else { continue };
                    if c.wait().is_ok() {
                        println!("ACK {tag} {}", account(lo).0);
                        std::io::stdout().flush().unwrap();
                    }
                }
            })
        })
        .collect();
    for h in hs {
        let _ = h.join();
    }
}

/// Every record of the stream's partitions, as text.
fn published(l: &SegLog) -> Vec<String> {
    let mut out = Vec::new();
    for p in 0..PARTS {
        let k = Key {
            tenant: STREAM_TENANT,
            stream: 1,
            partition: p,
        };
        let b = l.bounds(k);
        let mut at = b.log_start;
        while at < b.high_watermark {
            let (_, bytes) = l
                .read(k, at, 1 << 20)
                .unwrap_or_else(|_| panic!("read {at}"));
            if bytes.is_empty() {
                break;
            }
            let mut last = at;
            for (off, _, v) in records_of(&bytes) {
                if off >= at {
                    last = off;
                    out.push(String::from_utf8(v.unwrap()).unwrap());
                }
            }
            at = last + 1;
        }
    }
    out
}

fn check(dir: &Path, acked: &BTreeMap<String, u64>) -> (usize, usize) {
    let (e, l) = open(dir);
    // Recovery resolved what the kill left; the resolver deletes leftovers.
    let until = Instant::now() + Duration::from_secs(20);
    while e.txn_shared().cross_unit_open() > 0 {
        e.resolve_cross_unit();
        assert!(Instant::now() < until, "commits across units left open");
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut t = e.begin(TxnOptions::mariadb()).unwrap();
    let total: u64 = (0..accounts())
        .map(|i| {
            let v = t.get_snapshot(&account(i).1).unwrap().unwrap();
            u64::from_be_bytes(v.as_ref().try_into().unwrap())
        })
        .sum();
    assert_eq!(
        total,
        START * u64::from(accounts()),
        "money was created or lost"
    );
    let mut markers: BTreeSet<String> = BTreeSet::new();
    for tenant in BANKS {
        let p = prefix(tenant, 2);
        for (k, _) in t
            .scan(
                &KeyRange::prefix(&p),
                Dir::Forward,
                usize::MAX,
                ReadMode::Snapshot,
            )
            .unwrap()
        {
            markers.insert(String::from_utf8(k[p.len()..].to_vec()).unwrap());
        }
    }
    let records = published(&l);
    let mut seen = BTreeSet::new();
    for r in &records {
        assert!(!r.starts_with("RB-"), "a rolled-back publish appeared: {r}");
        let tag = r.strip_prefix("OK-").unwrap().to_owned();
        assert!(seen.insert(tag.clone()), "published twice: {r}");
        assert!(markers.contains(&tag), "published without its commit: {r}");
    }
    for m in &markers {
        assert!(seen.contains(m), "committed without its publish: {m}");
    }
    for tag in acked.keys() {
        assert!(
            markers.contains(tag),
            "acknowledged transaction lost: {tag}"
        );
    }
    (markers.len(), records.len())
}

#[test]
fn kill9_keeps_transactions_across_tenants_whole() {
    let rounds: u32 = std::env::var("SPRIG_KILL9_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let d = TempDir::new("kill9-cross");
    let exe = std::env::current_exe().unwrap();
    let mut acked: BTreeMap<String, u64> = BTreeMap::new();
    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args([
                "kill9_cross_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DIR_VAR, d.path())
            .env(ROUND_VAR, round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let want = 20 + (round as usize * 37) % 150;
        let mut got = 0;
        let started = Instant::now();
        while got < want && started.elapsed() < Duration::from_secs(60) {
            let Ok(line) = rx.recv_timeout(Duration::from_secs(60)) else {
                break;
            };
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.first() == Some(&"ACK") && f.len() == 3 {
                acked.insert(f[1].to_owned(), f[2].parse().unwrap());
                got += 1;
            }
        }
        child.kill().unwrap();
        let _ = child.wait();
        // Acknowledgements printed before the kill landed still count.
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(200)) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.first() == Some(&"ACK") && f.len() == 3 {
                acked.insert(f[1].to_owned(), f[2].parse().unwrap());
            }
        }
        let _ = reader.join();
        assert!(got > 0, "round {round}: the child acknowledged nothing");
        let (committed, records) = check(d.path(), &acked);
        println!(
            "round {round}: acked {} total, {committed} committed, {records} records",
            acked.len()
        );
    }
    println!(
        "kill9 across tenants: {rounds} rounds, {} acknowledged, 0 lost, 0 partial (wait={})",
        acked.len(),
        wait_mode()
    );
}
