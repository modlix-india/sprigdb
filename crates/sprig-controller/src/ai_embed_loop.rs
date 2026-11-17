//! The `ai_embed` loop (embedded columns), on the DC leader, every two
//! seconds:
//!
//! ```text
//! each active feed subscription of kind `ai_embed` (one per embedded
//!   column, registered by the gateway that ran its DDL) without a live
//!   `ai_embed` job → submit one (spec: tenant, subscription); a job whose
//!   worker died is requeued by dispatch, and a failed or finished one is
//!   replaced here
//! ```
//!
//! The same for retrieval indexes: each active subscription of kind
//! `ai_retrieval` (one per index) gets an `ai_retrieval` job.
//!
//! A job ends by itself when its column (or index), table or subscription
//! is gone, and removes the subscription, so it isn't submitted again. The
//! names match the gateway's `exec::embed_ddl::KIND` and
//! `sprig_ai::retrieval::KIND`, and the worker's handlers.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The subscription kind the gateways register.
pub const SUB_KIND: &str = "ai_embed";
/// The embedding job's kind.
pub const JOB_KIND: &str = "ai_embed";
/// The retrieval index subscriptions' kind, and their jobs'.
pub const RETRIEVAL_KIND: &str = sprig_ai::retrieval::KIND;

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AiEmbedTick;

impl fmt::Display for AiEmbedTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct AiEmbedLoop;

#[async_trait::async_trait]
impl Reconciler for AiEmbedLoop {
    type Key = AiEmbedTick;

    fn name(&self) -> &'static str {
        "ai_embed"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<AiEmbedTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<AiEmbedTick>> {
        Ok(vec![AiEmbedTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &AiEmbedTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let mut live = HashSet::new();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.state.is_final() || (job.kind != JOB_KIND && job.kind != RETRIEVAL_KIND) {
                continue;
            }
            if let (Some(t), Some(n)) = (job.spec["tenant"].as_u64(), job.spec["name"].as_str()) {
                live.insert((job.kind.clone(), t, n.to_owned()));
            }
        }
        let prefix = cx.dc.tier().key("feed/subs/")?;
        let page = cx
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        for kv in &page.kvs {
            let Ok(r) = Record::<Subscription>::decode(&kv.value) else {
                continue;
            };
            let s = r.body;
            let kind = if s.kind == SUB_KIND {
                JOB_KIND
            } else if s.kind == RETRIEVAL_KIND {
                RETRIEVAL_KIND
            } else {
                continue;
            };
            let k = (kind.to_owned(), s.tenant.0, s.name.clone());
            if s.state != "active" || live.contains(&k) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: kind.into(),
                    // One long-running job per embedded column or index.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: kind.into(),
                    zone: None,
                    spec: json!({"tenant": s.tenant.0, "name": s.name}),
                },
            )
            .await?;
            tracing::info!(tenant = %s.tenant, sub = %s.name, job = %id, kind, "embedding job submitted");
            live.insert(k);
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}
