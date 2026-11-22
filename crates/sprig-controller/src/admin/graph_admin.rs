//! The Admin API's graph views (`sprigctl graph list|show`), over the
//! controller's [`GraphViews`](crate::graph_views::GraphViews).
//!
//! ```text
//! GET  /admin/v1/tenants/{name}/graphs                  the tenant's graphs
//! GET  /admin/v1/tenants/{name}/graphs/{db.graph}       one graph
//! ```

use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};

use super::AdminService;
use crate::graph_views::{GraphRow, GraphView, GraphViews};

impl AdminService {
    fn graph_views(&self) -> Result<&Arc<GraphViews>> {
        self.graphs.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                "this controller has no graph views",
            )
        })
    }

    /// Serves the graph views from `g`.
    #[must_use]
    pub fn with_graphs(mut self, g: Arc<GraphViews>) -> Self {
        self.graphs = Some(g);
        self
    }

    /// `sprigctl graph list`.
    ///
    /// # Errors
    /// An unknown tenant; the catalog records'.
    pub async fn list_graphs(&self, tenant: &str) -> Result<Vec<GraphRow>> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.graph_views()?.graphs(t).await
    }

    /// `sprigctl graph show`.
    ///
    /// # Errors
    /// An unknown tenant or graph; the catalog records'.
    pub async fn show_graph(&self, tenant: &str, graph: &str) -> Result<GraphView> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        self.graph_views()?.graph_show(t, graph).await
    }
}
