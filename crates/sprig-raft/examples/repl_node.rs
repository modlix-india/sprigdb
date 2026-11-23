//! One process of a local three-node cluster of replicated engine units:
//! an engine on RocksDB whose tenant units commit through Raft over TCP,
//! with a small line protocol for test clients.
//!
//! ```text
//! cargo run --release -p sprig-raft --example repl_node -- \
//!     <id> <dir> <raft addr of node 1>,<of node 2>,<of node 3> <client addr>
//! ```
//!
//! Client lines (one reply line each):
//! - `PUT <tenant> <id> <value>`: commits the row, `Strict` (a majority
//!   synced); `OK`, or `ERR <retry|fail> <message>`;
//! - `GET <tenant> <id>`: this replica's stored row: `VAL <value>` or
//!   `NONE`;
//! - `LEADER <tenant>`: `YES <epoch>` if this node leads the tenant's
//!   unit, else `NO`;
//! - `APPEND <tenant> <elem> <key>...`: one transaction appending `elem`
//!   to each key's list (read under its record lock): `OK <key>=[<list
//!   before>]...` or `ERR ...`;
//! - `SUM <tenant>`: this replica's unit checksum: `SUM <rows> <crc>
//!   <applied>`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_common::time::SystemClock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::{Durability, SyncMode};
use sprig_engine::txn::{LockRequest, Locked, TxnOptions};
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_raft::tcp::TcpTransport;
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, RaftConfig};

fn key(tenant: u64, id: u64) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// The Raft log's sync mode: `REPL_NODE_SYNC=off` skips fsync (a stand-in
/// for a disk whose fsync costs little, to measure the rest of the commit
/// path); anything else, or unset, syncs fully.
fn sync_mode() -> SyncMode {
    match std::env::var("REPL_NODE_SYNC").as_deref() {
        Ok("off") => SyncMode::Off,
        _ => SyncMode::Full,
    }
}

fn handle(engine: &Engine, line: &str) -> String {
    let p: Vec<&str> = line.split_whitespace().collect();
    let num = |i: usize| p.get(i).and_then(|s| s.parse::<u64>().ok());
    match (p.first().copied(), num(1)) {
        (Some("PUT"), Some(tenant)) => {
            let (Some(id), Some(v)) = (num(2), p.get(3)) else {
                return "ERR fail usage".into();
            };
            let r = (|| {
                let mut t = engine.begin(TxnOptions {
                    durability: Some(Durability::Strict),
                    ..TxnOptions::default()
                })?;
                t.put(key(tenant, id), Bytes::copy_from_slice(v.as_bytes()))?;
                t.commit()?.wait()
            })();
            match r {
                Ok(()) => "OK".into(),
                Err(e) => format!(
                    "ERR {} {}",
                    if e.retryable { "retry" } else { "fail" },
                    e.message.replace('\n', " ")
                ),
            }
        }
        (Some("APPEND"), Some(tenant)) => {
            // One transaction: append <elem> to each listed key's list
            // (`APPEND <tenant> <elem> <key>...`), reading each list under
            // its record lock; replies with the lists as they were before.
            let Some(elem) = num(2) else {
                return "ERR fail usage".into();
            };
            let keys: Vec<u64> = p[3..].iter().filter_map(|k| k.parse().ok()).collect();
            let r = (|| {
                let mut t = engine.begin(TxnOptions {
                    durability: Some(Durability::Strict),
                    ..TxnOptions::default()
                })?;
                let mut before = Vec::new();
                for k in &keys {
                    let kb = Bytes::from(key(tenant, *k));
                    let cur = match t.lock_get_blocking(&kb, LockRequest::for_update())? {
                        Locked::Row(Some(v)) => String::from_utf8_lossy(&v).into_owned(),
                        _ => String::new(),
                    };
                    let next = if cur.is_empty() {
                        elem.to_string()
                    } else {
                        format!("{cur},{elem}")
                    };
                    t.put(kb.to_vec(), Bytes::from(next))?;
                    before.push(format!("{k}=[{cur}]"));
                }
                t.commit()?.wait()?;
                Ok::<_, sprig_common::SprigError>(before.join(" "))
            })();
            match r {
                Ok(b) => format!("OK {b}"),
                Err(e) => format!(
                    "ERR {} {}",
                    if e.retryable { "retry" } else { "fail" },
                    e.message.replace('\n', " ")
                ),
            }
        }
        (Some("GET"), Some(tenant)) => {
            let Some(id) = num(2) else {
                return "ERR fail usage".into();
            };
            engine.txn_shared().observe(engine.applied_max_ts());
            let got = engine
                .begin(TxnOptions::default())
                .and_then(|mut t| t.get_snapshot(&key(tenant, id)));
            match got {
                Ok(Some(v)) => format!("VAL {}", String::from_utf8_lossy(&v)),
                Ok(None) => "NONE".into(),
                Err(e) => format!("ERR fail {}", e.message),
            }
        }
        (Some("LEADER"), Some(tenant)) => {
            let e = engine.leader_epoch(Engine::unit_of_tenant(tenant));
            if e > 0 {
                format!("YES {e}")
            } else {
                "NO".into()
            }
        }
        (Some("SUM"), Some(tenant)) => {
            let unit = Engine::unit_of_tenant(tenant);
            match engine.unit_checksum(unit) {
                Ok(s) => format!(
                    "SUM {} {} {}",
                    s.entries,
                    s.crc,
                    engine.last_applied_of(unit)
                ),
                Err(e) => format!("ERR fail {}", e.message),
            }
        }
        _ => "ERR fail unknown command".into(),
    }
}

fn serve(engine: &Engine, s: TcpStream) {
    let _ = s.set_nodelay(true);
    let Ok(w) = s.try_clone() else { return };
    let mut w = std::io::BufWriter::new(w);
    for line in BufReader::new(s).lines() {
        let Ok(line) = line else { return };
        let reply = handle(engine, &line);
        if writeln!(w, "{reply}").and_then(|()| w.flush()).is_err() {
            return;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 5,
        "usage: repl_node <id> <dir> <raft addrs> <client addr>"
    );
    let id: u64 = args[1].parse().expect("id");
    let dir = std::path::PathBuf::from(&args[2]);
    let peers: Vec<SocketAddr> = args[3]
        .split(',')
        .map(|a| a.parse().expect("raft addr"))
        .collect();
    let client: SocketAddr = args[4].parse().expect("client addr");
    let voters: Vec<u64> = (1..=peers.len() as u64).collect();

    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.store = StoreKind::Rocks;
    cfg.replicated = true;
    cfg.default_durability = Durability::Strict;
    cfg.checkpoint_interval = Duration::from_secs(5);
    let engine = Engine::open(cfg).expect("engine opens");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let transport = TcpTransport::new(id, rt.handle().clone(), None);
    for (i, a) in peers.iter().enumerate() {
        transport.set_peer(i as u64 + 1, *a);
    }
    let store =
        EngineLogStore::open(dir.join("raft-repl"), sync_mode(), None).expect("raft log opens");
    let node = EngineNode::open(
        &engine,
        EngineNodeOptions {
            id,
            config: RaftConfig {
                static_voters: voters,
                ..RaftConfig::default()
            },
            store,
            transport: Arc::clone(&transport) as Arc<dyn sprig_raft::Transport>,
            clock: Arc::new(SystemClock),
            seed: id ^ u64::from(std::process::id()),
            observer: None,
            inline_roles: false,
        },
    )
    .expect("raft node opens");
    rt.block_on(transport.listen(peers[usize::try_from(id - 1).unwrap()], node.node().clone()))
        .expect("raft listener");
    let _driver = node.spawn().expect("driver");
    let l = TcpListener::bind(client).expect("client listener");
    eprintln!(
        "repl_node {id} ready: raft {} clients {client}",
        peers[usize::try_from(id - 1).unwrap()]
    );
    for s in l.incoming() {
        let Ok(s) = s else { continue };
        let e = engine.clone();
        std::thread::spawn(move || serve(&e, s));
    }
}
