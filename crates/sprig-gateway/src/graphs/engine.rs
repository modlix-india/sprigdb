//! Native graphs stored in the node's storage engine
//! ([`sprig_graph::engine`]): which graphs live there, and the one
//! [`EngineGraph`] per graph every statement on it shares (its dictionary,
//! id blocks and index definitions in memory).
//!
//! Every native graph lives on the engine; mapped graphs (`CREATE
//! PROPERTY GRAPH`) read SQL tables. A graph keeps its catalog record
//! (name, id, grants); its nodes, relationships, names, ids and indexes
//! are engine keys. If the engine can't open, statements on graphs fail
//! with the engine's error.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use sprig_common::Result;
use sprig_graph::engine::{EngineGraph, drop_graph};

use super::GraphRef;

/// Data shards of a graph on the engine (one unit: placement inside it
/// buys nothing yet).
const SHARDS: usize = 1;

/// Whether `g`'s elements live on the storage engine (a native graph).
#[must_use]
pub(crate) fn on_engine(g: &GraphRef) -> bool {
    g.def.kind != "mapped"
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
