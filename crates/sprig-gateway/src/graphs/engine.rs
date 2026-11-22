//! Native graphs stored in the node's storage engine
//! ([`sprig_graph::engine`]): which graphs live there, and the one
//! [`EngineGraph`] per graph every statement on it shares (its dictionary,
//! id blocks and index definitions in memory).
//!
//! A native graph lives on the engine when its database is one of the
//! tenant's engine databases (every database once the engine owns the
//! catalog). `SPRIGDB_ENGINE_GRAPHS` overrides that for the whole node:
//! `all` puts every native graph there, `off` keeps every graph on MariaDB
//! (the "before" arm of a benchmark). Mapped graphs (`CREATE PROPERTY
//! GRAPH`) read SQL tables and never move.
//!
//! A graph on the engine keeps its catalog row (name, id, grants) where it
//! is; its nodes, relationships, names, ids and indexes are engine keys
//! only, never written to MariaDB. If the engine can't open, statements on
//! those graphs fail with the engine's error: nothing falls back to
//! MariaDB.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use sprig_catalog::TenantCatalog;
use sprig_common::Result;
use sprig_graph::engine::{EngineGraph, drop_graph};

use super::GraphRef;

/// The variable that overrides where native graphs live.
pub(crate) const GRAPHS_VAR: &str = "SPRIGDB_ENGINE_GRAPHS";

/// Data shards of a graph on the engine (one unit: placement inside it
/// buys nothing yet).
const SHARDS: usize = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// By database.
    Database,
    All,
    Off,
}

fn mode() -> Mode {
    static MODE: OnceLock<Mode> = OnceLock::new();
    *MODE.get_or_init(|| {
        match std::env::var(GRAPHS_VAR)
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("all") => Mode::All,
            Some("off") => Mode::Off,
            _ => Mode::Database,
        }
    })
}

/// Whether `g`'s elements live on the storage engine.
#[must_use]
pub(crate) fn on_engine(g: &GraphRef, cat: &TenantCatalog) -> bool {
    if g.def.kind == "mapped" {
        return false;
    }
    match mode() {
        Mode::All => true,
        Mode::Off => false,
        Mode::Database => crate::engine::catalog::of(cat).is_some_and(|e| e.dbs.contains(&g.db_id)),
    }
}

type Graphs = Mutex<HashMap<(u64, u64), Arc<EngineGraph>>>;

fn graphs() -> &'static Graphs {
    static G: OnceLock<Graphs> = OnceLock::new();
    G.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The shared [`EngineGraph`] of `g`, opened on first use.
///
/// # Errors
/// The engine can't open; the graph id is outside the engine's graph
/// range.
pub(crate) fn graph(g: &GraphRef) -> Result<Arc<EngineGraph>> {
    let key = (g.tenant.0, g.graph_id);
    if let Some(eg) = graphs()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
    {
        return Ok(Arc::clone(eg));
    }
    let engine = crate::node_engine::engine()?;
    let eg = EngineGraph::open(Arc::new(engine), g.tenant.0, g.graph_id, SHARDS)?;
    let mut m = graphs().lock().unwrap_or_else(PoisonError::into_inner);
    // Another statement may have opened it meanwhile: keep the first.
    Ok(Arc::clone(m.entry(key).or_insert(eg)))
}

/// `DROP GRAPH` of a graph on the engine: every key of it, after the
/// catalog row is gone.
pub(crate) async fn drop_storage(g: &GraphRef) {
    let key = (g.tenant.0, g.graph_id);
    let eg = match graph(g) {
        Ok(eg) => eg,
        Err(e) => {
            tracing::warn!(error = %e, graph = g.graph_id, "dropping graph storage on the engine failed");
            return;
        }
    };
    match drop_graph(&eg, 10_000).await {
        Ok(n) => tracing::info!(
            tenant = g.tenant.0,
            graph = g.graph_id,
            keys = n,
            "graph storage on the engine dropped"
        ),
        Err(e) => {
            tracing::warn!(error = %e, graph = g.graph_id, "dropping graph storage on the engine failed");
        }
    }
    graphs()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&key);
}
