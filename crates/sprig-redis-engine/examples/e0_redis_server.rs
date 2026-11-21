//! A Redis server on the engine alone, for benchmarks and crash runs of the
//! engine's Redis path: the gateway's own connection loop and session,
//! with a fixed password in place of the catalog login, and every database
//! on the engine.
//!
//! Environment: `E0_LISTEN` (default `0.0.0.0:6379`), `E0_DIR` (the
//! engine's directory: RocksDB and its log, default `/data`),
//! `E0_PASSWORD` (default `e0`), `E0_ENGINE_MB` (the engine's memory,
//! default 1024).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Frame, Limits};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

/// The tenant every login gets.
const TENANT: &str = "1";

struct NoSql;

#[async_trait]
impl SqlRunner for NoSql {
    async fn query(&mut self, _sql: &str) -> Result<Vec<Row>, SqlError> {
        Err(SqlError {
            errno: None,
            message: "no SQL on this server".into(),
        })
    }

    async fn exec(&mut self, _sql: &str) -> Result<u64, SqlError> {
        Ok(0)
    }
}

struct FixedAuth(Vec<u8>);

#[async_trait]
impl Auth for FixedAuth {
    async fn login(&self, user: Option<&[u8]>, pass: &[u8]) -> Result<Login, Frame> {
        if pass != self.0.as_slice() {
            return Err(sprig_redis::session::wrongpass());
        }
        Ok(Login {
            sql: Box::new(NoSql),
            scope: TENANT.into(),
            user: user.map_or_else(
                || "default".into(),
                |u| String::from_utf8_lossy(u).into_owned(),
            ),
        })
    }
}

#[tokio::main]
async fn main() {
    let var = |k: &str, d: &str| {
        std::env::var(k)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| d.to_owned())
    };
    let listen = var("E0_LISTEN", "0.0.0.0:6379");
    let dir = PathBuf::from(var("E0_DIR", "/data"));
    let pass = var("E0_PASSWORD", "e0");
    let mut cfg = sprig_engine::EngineConfig::new(&dir);
    if let Ok(mb) = var("E0_ENGINE_MB", "1024").parse() {
        cfg.budget.engine_mb = mb;
    }
    let engine = sprig_engine::Engine::open(cfg).expect("engine");
    let unit = EngineUnit::on_engine(engine.clone());
    let backend = Arc::new(Backend::new(
        Arc::new(unit),
        Tenants::All,
        Config::default().databases,
    ));
    let _sweeper = backend.spawn_sweeper(Duration::from_millis(100));
    let env = Arc::new(Env::new(Config::default(), "e0-redis"));
    env.set_engine(Arc::clone(&backend) as Arc<dyn sprig_redis::engine::EngineBackend>);
    let auth: Arc<dyn Auth> = Arc::new(FixedAuth(pass.into_bytes()));
    // The log's groups and syncs, every 5 s: every acknowledged write was
    // in a synced group.
    std::thread::spawn(move || {
        let mut last = engine.stats().log;
        loop {
            std::thread::sleep(Duration::from_secs(5));
            let now = engine.stats().log;
            if now != last {
                eprintln!(
                    "log: {} groups, {} syncs, {} entries in the last 5 s (totals {} / {} / {})",
                    now.groups - last.groups,
                    now.syncs - last.syncs,
                    now.entries - last.entries,
                    now.groups,
                    now.syncs,
                    now.entries
                );
            }
            last = now;
        }
    });
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .expect("listen");
    eprintln!(
        "e0-redis-server listening on {listen}, log in {}",
        dir.display()
    );
    loop {
        let Ok((sock, peer)) = listener.accept().await else {
            continue;
        };
        let _ = sock.set_nodelay(true);
        let s = Session::new(Arc::clone(&env), Arc::clone(&auth), &peer.to_string());
        tokio::spawn(sprig_redis::conn::serve(
            sock,
            s,
            Limits::default(),
            std::future::pending(),
        ));
    }
}
