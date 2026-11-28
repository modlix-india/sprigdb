//! The `ai_system` loop: retention of the administration assistant's
//! conversations, kept in the DC's system catalog records
//! ([`sprig_ai::audit::rec`], module `ai_admin` under tenant id 0).
//!
//! ```text
//! on the DC leader, hourly:
//!   drop payloads older than the global ai.audit_payload_days from the
//!   notes, keeping their hashes (sprig_ai_payloads_purged_total)
//! ```
//!
//! The records need no schema, cluster or grant: they are created by the
//! first conversation.

use std::sync::Arc;
use std::time::Duration;

use sprig_ai::audit::AuditStore as _;
use sprig_ai::audit::rec::RecAudit;
use sprig_catalog::RecordStore;
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// How often payloads are purged.
pub const PASS_INTERVAL: Duration = Duration::from_secs(3600);
/// The loop's one key.
const KEY: &str = "system";

/// The loop (see the module docs).
pub struct AiSystemLoop {
    records: Arc<dyn RecordStore>,
    live: Option<(LiveSettings, DcId)>,
}

impl std::fmt::Debug for AiSystemLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiSystemLoop").finish_non_exhaustive()
    }
}

impl AiSystemLoop {
    /// The loop over the catalog `records`, reading
    /// `ai.audit_payload_days` from `live` (its default without).
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, live: Option<(LiveSettings, DcId)>) -> Self {
        Self { records, live }
    }

    /// The global (or DC) `ai.audit_payload_days`.
    #[must_use]
    pub fn days(&self) -> u64 {
        let key = &sprig_ai::settings::AUDIT_PAYLOAD_DAYS;
        match &self.live {
            Some((l, dc)) => l.get(key, &SettingCtx::process(dc)),
            None => key.default,
        }
    }

    /// One pass: the purge. Returns how many payloads it dropped.
    ///
    /// # Errors
    /// The records' (39070 when the engine node can't be reached).
    pub async fn pass(&self) -> Result<u64> {
        let days = self.days();
        // The purge covers every operator's notes; the login only scopes
        // reads.
        let n = RecAudit::system(Arc::clone(&self.records), "")
            .purge_payloads(days)
            .await?;
        if n > 0 {
            sprig_ai::metrics::payloads_purged(n);
            tracing::debug!(
                n,
                days,
                "administration conversations' payloads past their retention purged"
            );
        }
        Ok(n)
    }
}

#[async_trait::async_trait]
impl Reconciler for AiSystemLoop {
    type Key = String;

    fn name(&self) -> &'static str {
        "ai_system"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        Vec::new()
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<String> {
        vec![KEY.to_owned()]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<String>> {
        Ok(vec![KEY.to_owned()])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &String) -> Result<Outcome> {
        cx.leader()?;
        self.pass().await?;
        Ok(Outcome::RequeueAfter(PASS_INTERVAL))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sprig_ai::audit::MsgKind;
    use sprig_ai::role::Role;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::UserId;

    use super::*;

    #[tokio::test]
    async fn the_pass_drops_old_payloads_and_keeps_hashes() {
        let r: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
        let l = AiSystemLoop::new(Arc::clone(&r), None);
        assert_eq!(l.days(), 30);
        assert_eq!(l.pass().await.unwrap(), 0);
        let a = RecAudit::system(Arc::clone(&r), "acme.alice");
        let c = a.create(UserId(0), Role::Admin, "disk").await.unwrap();
        a.append(
            c,
            MsgKind::SystemNote,
            &json!({"sha256": "ab", "payload": {"messages": []}}),
            Some([1; 32]),
        )
        .await
        .unwrap();
        // Within the retention: kept.
        assert_eq!(l.pass().await.unwrap(), 0);
        assert!(
            a.messages(c).await.unwrap()[0]
                .body
                .get("payload")
                .is_some()
        );
    }
}
