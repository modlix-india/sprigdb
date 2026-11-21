//! The node's storage engine: one [`Engine`] per process, shared by every
//! front end whose data lives on it (engine-backed SQL databases and Redis
//! databases).
//!
//! It opens on first use, from the environment:
//! - `SPRIGDB_ENGINE_DIR`: its directory (RocksDB and its commit log),
//!   default `/var/lib/sprigdb/engine`;
//! - `SPRIGDB_ENGINE_STORE`: `rocks` (default) or `mem` (an in-memory store
//!   rebuilt from the log at start; for tests);
//! - `SPRIGDB_ENGINE_MB`: its memory in MiB, default 1,024.
//!
//! If it can't open, every caller gets the same error, naming the
//! directory; nothing falls back to MariaDB.

use std::path::PathBuf;
use std::sync::OnceLock;

use sprig_common::{Result, SprigError};
use sprig_engine::{Engine, EngineConfig, StoreKind};

/// The variable naming the engine's directory.
pub const DIR_VAR: &str = "SPRIGDB_ENGINE_DIR";
/// The variable choosing the engine's store (`rocks` or `mem`).
pub const STORE_VAR: &str = "SPRIGDB_ENGINE_STORE";
/// The variable giving the engine's memory, in MiB.
pub const MB_VAR: &str = "SPRIGDB_ENGINE_MB";

/// The engine's settings from the environment.
///
/// # Errors
/// `SPRIGDB_ENGINE_STORE` or `SPRIGDB_ENGINE_MB` has a value it can't use.
pub fn config_from_env() -> Result<EngineConfig> {
    config_from(|k| std::env::var(k).ok())
}

/// The engine's settings from `lookup` (a variable's value by name).
fn config_from(lookup: impl Fn(&str) -> Option<String>) -> Result<EngineConfig> {
    let var = |k: &str| lookup(k).filter(|v| !v.trim().is_empty());
    let dir = var(DIR_VAR).map_or_else(|| PathBuf::from("/var/lib/sprigdb/engine"), PathBuf::from);
    let mut cfg = EngineConfig::new(dir);
    cfg.store = match var(STORE_VAR).as_deref().map(str::trim) {
        None | Some("rocks") => StoreKind::Rocks,
        Some("mem") => StoreKind::Mem,
        Some(other) => {
            return Err(SprigError::internal(format!(
                "{STORE_VAR} must be 'rocks' or 'mem', not '{other}'"
            )));
        }
    };
    if let Some(mb) = var(MB_VAR) {
        cfg.budget.engine_mb = mb.trim().parse().map_err(|_| {
            SprigError::internal(format!("{MB_VAR} must be a number of MiB, not '{mb}'"))
        })?;
    }
    Ok(cfg)
}

/// The node's engine, opened on the first call; every call returns the
/// same handle, or the same failure.
///
/// # Errors
/// The settings are malformed, or the engine failed to open in its
/// directory.
pub fn engine() -> Result<Engine> {
    static ENGINE: OnceLock<std::result::Result<Engine, String>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            let cfg = config_from_env().map_err(|e| e.message.clone())?;
            let dir = cfg.data_dir.display().to_string();
            match Engine::open(cfg) {
                Ok(e) => {
                    tracing::info!(dir = %dir, "storage engine open");
                    Ok(e)
                }
                Err(e) => {
                    tracing::error!(dir = %dir, error = %e, "storage engine failed to open");
                    Err(format!("storage engine in {dir}: {}", e.message))
                }
            }
        })
        .clone()
        .map_err(SprigError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(vars: &[(&str, &str)]) -> Result<EngineConfig> {
        config_from(|k| {
            vars.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn settings_from_the_environment() {
        let d = cfg(&[]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(d.data_dir, PathBuf::from("/var/lib/sprigdb/engine"));
        assert_eq!(d.store, StoreKind::Rocks);
        assert_eq!(d.budget.engine_mb, 1024);
        let c = cfg(&[(DIR_VAR, "/tmp/e"), (STORE_VAR, "mem"), (MB_VAR, " 256 ")])
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(c.data_dir, PathBuf::from("/tmp/e"));
        assert_eq!(c.store, StoreKind::Mem);
        assert_eq!(c.budget.engine_mb, 256);
        assert!(cfg(&[(STORE_VAR, "disk")]).is_err());
        assert!(cfg(&[(MB_VAR, "lots")]).is_err());
    }
}
