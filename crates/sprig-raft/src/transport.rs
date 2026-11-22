//! Frames between nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::NodeId;
use crate::node::Node;

pub mod tcp;

/// Sends a node's frames. Best effort: a frame may be lost, and Raft
/// retries.
pub trait Transport: Send + Sync {
    /// Sends `frame` to `to` without blocking.
    fn send(&self, to: NodeId, frame: Vec<u8>);
}

/// Nodes in one process, connected directly. Links can be cut for tests.
#[derive(Debug, Default)]
pub struct MemNetwork {
    nodes: RwLock<BTreeMap<NodeId, Node>>,
    cut: Mutex<BTreeSet<(NodeId, NodeId)>>,
}

impl MemNetwork {
    /// An empty network.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Adds (or replaces, after a restart) `node`.
    pub fn register(&self, node: &Node) {
        self.nodes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node.id(), node.clone());
    }

    /// Removes a node: frames to it are lost.
    pub fn unregister(&self, id: NodeId) {
        self.nodes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
    }

    /// Cuts (`true`) or restores frames from `from` to `to`.
    pub fn set_cut(&self, from: NodeId, to: NodeId, cut: bool) {
        let mut c = self.cut.lock().unwrap_or_else(PoisonError::into_inner);
        if cut {
            c.insert((from, to));
        } else {
            c.remove(&(from, to));
        }
    }

    /// Cuts or restores every link to and from `id`.
    pub fn isolate(&self, id: NodeId, cut: bool) {
        let ids: Vec<NodeId> = self
            .nodes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        for o in ids {
            if o != id {
                self.set_cut(id, o, cut);
                self.set_cut(o, id, cut);
            }
        }
    }

    /// A transport for `from`.
    #[must_use]
    pub fn transport(self: &Arc<Self>, from: NodeId) -> Arc<MemTransport> {
        Arc::new(MemTransport {
            net: Arc::clone(self),
            from,
        })
    }
}

/// One node's side of a [`MemNetwork`].
#[derive(Debug)]
pub struct MemTransport {
    net: Arc<MemNetwork>,
    from: NodeId,
}

impl Transport for MemTransport {
    fn send(&self, to: NodeId, frame: Vec<u8>) {
        if self
            .net
            .cut
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&(self.from, to))
        {
            return;
        }
        let n = self
            .net
            .nodes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&to)
            .cloned();
        if let Some(n) = n {
            n.deliver(frame);
        }
    }
}
