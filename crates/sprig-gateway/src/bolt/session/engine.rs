//! Bolt statements on graphs stored in the storage engine
//! ([`crate::graphs::engine`]).
//!
//! - **Auto-commit:** one engine transaction per statement; a write is
//!   answered once its commit is durable (and after the check that no
//!   deleted node kept a relationship, 39906).
//! - **Explicit transactions:** the first statement on an engine graph
//!   opens an engine transaction inside the Bolt transaction; `COMMIT`
//!   commits it (with the same check), `ROLLBACK` and `RESET` roll it back.
//!   A Bolt transaction works on one graph on the engine.
//! - **Bookmarks:** the engine is the node's own, and a commit is durable
//!   before it is answered, so any later read on this node sees it. The
//!   bookmark a commit returns is the session's usual signed one.

use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::ast::Query;
use sprig_graph::engine::{EngineGraph, GraphTxn};
use sprig_graph::eval::Params;
use tokio::sync::{mpsc, oneshot};

use super::{
    Access, CHANNEL_ROWS, ChanSink, GwSession, Msg, StartOrResult, failure, hard_time, lost_conn,
    run_on, stream_on, summary,
};
use crate::graphs::GraphRef;

/// An explicit transaction's engine part.
pub(super) struct EngineTx {
    g: Arc<EngineGraph>,
    /// Here, or away with a streamed result (`back`).
    tx: Option<GraphTxn>,
    back: Option<oneshot::Receiver<GraphTxn>>,
}

impl EngineTx {
    async fn take(&mut self) -> Result<GraphTxn> {
        if let Some(t) = self.tx.take() {
            return Ok(t);
        }
        let back = self.back.take().ok_or_else(lost_conn)?;
        back.await.map_err(|_| lost_conn())
    }

    /// Rolls back, wherever the transaction is.
    pub(super) async fn abandon(mut self) {
        if let Ok(t) = self.take().await {
            t.rollback();
        }
    }

    /// Commits (after the deleted-node check), durable before it returns.
    pub(super) async fn commit(mut self) -> Result<()> {
        let t = self.take().await?;
        t.commit(&self.g).await
    }
}

/// A Bolt transaction that would use a graph on the engine and one on
/// MariaDB: their commits can't be made one.
pub(super) fn mixed_graphs() -> SprigError {
    SprigError::new(
        SprigCode::GraphUnsupported,
        "a transaction can't use a graph stored in the storage engine together with a graph stored in MariaDB",
    )
}

fn other_graph() -> SprigError {
    SprigError::new(
        SprigCode::GraphUnsupported,
        "a transaction on a graph stored in the storage engine can't use another graph",
    )
}

/// A statement for [`GwSession::execute_engine`].
pub(super) struct EngineRun {
    pub(super) q: Query,
    pub(super) p: Params,
    pub(super) g: GraphRef,
    pub(super) cat: Arc<sprig_catalog::TenantCatalog>,
    pub(super) budgets: sprig_graph::Budgets,
    pub(super) visible: Option<Access>,
    pub(super) writes: bool,
    pub(super) streams: bool,
}

impl GwSession {
    /// Runs a statement on a graph stored in the engine.
    #[allow(clippy::too_many_lines)] // auto-commit and transaction, buffered and streamed
    pub(super) async fn execute_engine(&mut self, r: EngineRun) -> Result<StartOrResult> {
        let EngineRun {
            q,
            p,
            g,
            cat,
            budgets,
            visible,
            writes,
            streams,
        } = r;
        let eg = crate::graphs::engine::graph(&g)?;
        if self.tx.is_none() {
            let mut tx = eg.begin(true);
            if streams {
                let (sender, rx) = mpsc::channel(CHANNEL_ROWS);
                tokio::spawn(crate::offload::run_graph_stream(async move {
                    let r = {
                        let mut store = eg.store(&mut tx);
                        let mut sink = ChanSink { tx: sender.clone() };
                        stream_on(&mut store, visible, &q, &p, &budgets, &mut sink).await
                    };
                    let r = match r {
                        Ok(o) => tx.commit(&eg).await.map(|()| o),
                        Err(e) => {
                            tx.rollback();
                            Err(e)
                        }
                    };
                    let end = r.map(|o| summary(o.stats, None)).map_err(|e| failure(&e));
                    let _ = sender.send(Msg::End(end)).await;
                }));
                return Ok(StartOrResult::Stream(rx, g.name));
            }
            let r = {
                let mut store = eg.store(&mut tx);
                if writes {
                    run_on(&mut store, visible, &q, &p, &budgets).await
                } else {
                    hard_time(
                        budgets.max_time,
                        run_on(&mut store, visible, &q, &p, &budgets),
                    )
                    .await
                }
            };
            let out = match r {
                Ok(o) => {
                    tx.commit(&eg).await?;
                    o
                }
                Err(e) => {
                    tx.rollback();
                    return Err(e);
                }
            };
            self.after_commit().await?;
            return Ok(StartOrResult::Result(self.result(out, &g.name, &cat)?));
        }
        // In the explicit transaction.
        let Some(t) = self.tx.as_mut() else {
            return Err(SprigError::internal("no transaction"));
        };
        t.ran += 1;
        if t.engine.is_none() && t.ran > 1 {
            // Earlier statements ran on a graph stored in MariaDB.
            return Err(mixed_graphs());
        }
        let et = t.engine.get_or_insert_with(|| EngineTx {
            g: Arc::clone(&eg),
            tx: Some(eg.begin(false)),
            back: None,
        });
        if !Arc::ptr_eq(&et.g, &eg) {
            return Err(other_graph());
        }
        let mut tx = et.take().await?;
        if streams {
            let (back_tx, back_rx) = oneshot::channel();
            et.back = Some(back_rx);
            let (sender, rx) = mpsc::channel(CHANNEL_ROWS);
            tokio::spawn(crate::offload::run_graph_stream(async move {
                let r = {
                    let mut store = eg.store(&mut tx);
                    let mut sink = ChanSink { tx: sender.clone() };
                    stream_on(&mut store, visible, &q, &p, &budgets, &mut sink).await
                };
                let _ = back_tx.send(tx);
                let end = r.map(|o| summary(o.stats, None)).map_err(|e| failure(&e));
                let _ = sender.send(Msg::End(end)).await;
            }));
            return Ok(StartOrResult::Stream(rx, g.name));
        }
        let r = {
            let mut store = eg.store(&mut tx);
            run_on(&mut store, visible, &q, &p, &budgets).await
        };
        if let Some(t) = self.tx.as_mut()
            && let Some(et) = t.engine.as_mut()
        {
            et.tx = Some(tx);
        }
        let out = r?;
        Ok(StartOrResult::Result(self.result(out, &g.name, &cat)?))
    }
}
