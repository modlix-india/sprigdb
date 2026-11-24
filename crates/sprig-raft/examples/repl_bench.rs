//! A load generator for `repl_node` clusters, without the Python client's
//! ceiling: each of `clients` threads keeps one connection to tenant
//! `tenant`'s leader and commits single rows (`PUT`) for `seconds`.
//!
//! ```text
//! repl_bench <client addr>,<client addr>,... <tenant> <clients> <seconds>
//! ```
//!
//! Prints one JSON line: commits per second and commit latency p50/p99 (ms).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn ask(s: &mut TcpStream, r: &mut BufReader<TcpStream>, line: &str) -> std::io::Result<String> {
    s.write_all(line.as_bytes())?;
    s.write_all(b"\n")?;
    let mut out = String::new();
    r.read_line(&mut out)?;
    Ok(out.trim().to_owned())
}

fn connect(a: SocketAddr) -> std::io::Result<(TcpStream, BufReader<TcpStream>)> {
    let s = TcpStream::connect_timeout(&a, Duration::from_secs(3))?;
    s.set_nodelay(true)?;
    let r = BufReader::new(s.try_clone()?);
    Ok((s, r))
}

/// The address whose node leads `tenant` (`LEADER <tenant>` answers YES).
fn leader(addrs: &[SocketAddr], tenant: u64) -> SocketAddr {
    for _ in 0..600 {
        for a in addrs {
            if let Ok((mut s, mut r)) = connect(*a)
                && ask(&mut s, &mut r, &format!("LEADER {tenant}"))
                    .is_ok_and(|x| x.starts_with("YES"))
            {
                return *a;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("no leader for tenant {tenant}");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 5,
        "usage: repl_bench <addr,addr,...> <tenant> <clients> <seconds>"
    );
    let addrs: Vec<SocketAddr> = args[1].split(',').map(|a| a.parse().unwrap()).collect();
    let tenant: u64 = args[2].parse().unwrap();
    let clients: u64 = args[3].parse().unwrap();
    let secs: f64 = args[4].parse().unwrap();
    // The unit starts on its first write.
    'init: for _ in 0..200 {
        for a in &addrs {
            if let Ok((mut s, mut r)) = connect(*a)
                && ask(&mut s, &mut r, &format!("PUT {tenant} 0 init")).is_ok_and(|x| x == "OK")
            {
                break 'init;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let lat = Arc::new(Mutex::new(Vec::<f64>::new()));
    let errors = Arc::new(Mutex::new(0u64));
    let end = Instant::now() + Duration::from_secs_f64(secs);
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let (addrs, lat, errors) = (addrs.clone(), Arc::clone(&lat), Arc::clone(&errors));
            std::thread::spawn(move || {
                let mut mine = Vec::new();
                let (mut s, mut r) = connect(leader(&addrs, tenant)).unwrap();
                let value = "x".repeat(100);
                let mut n = 0u64;
                while Instant::now() < end {
                    n += 1;
                    let t0 = Instant::now();
                    match ask(
                        &mut s,
                        &mut r,
                        &format!("PUT {tenant} {} {value}", c * 100_000_000 + n),
                    ) {
                        Ok(x) if x == "OK" => mine.push(t0.elapsed().as_secs_f64() * 1000.0),
                        _ => {
                            *errors.lock().unwrap() += 1;
                            (s, r) = connect(leader(&addrs, tenant)).unwrap();
                        }
                    }
                }
                lat.lock().unwrap().extend(mine);
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let mut l = lat.lock().unwrap().clone();
    l.sort_by(f64::total_cmp);
    let p = |q: f64| {
        l.get(((l.len() as f64 * q) as usize).min(l.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0.0)
    };
    println!(
        "{{\"clients\": {clients}, \"ops\": {}, \"ops_per_s\": {:.1}, \"p50_ms\": {:.3}, \"p99_ms\": {:.3}, \"errors\": {}}}",
        l.len(),
        l.len() as f64 / secs,
        p(0.5),
        p(0.99),
        errors.lock().unwrap()
    );
}
