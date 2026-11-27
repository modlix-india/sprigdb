//! The internal CA's operator calls (`sprigctl ca`):
//!
//! ```text
//! GET  /admin/v1/ca                 the root, this DC's issuing CA, the nodes' certificates, revocations
//! POST /admin/v1/ca:init            make the root and this DC's issuing CA where missing
//! POST /admin/v1/ca/nodes/{id}      {"action": "renew" | "revoke" | "readmit", "reason": "..."}
//! ```
//!
//! `renew` issues the node a new certificate now; `revoke` revokes every
//! certificate the node holds (every node then closes its connections)
//! and stops renewals; `readmit` lets a revoked node have certificates
//! again (and issues one). Calls that sign need this controller to hold
//! the CA key (`SPRIGDB_CA_KEY_FILE`).

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};

use super::{AdminService, invalid};
use crate::internal_ca::{self, CaView, InitView, InternalCa, IssueOutcome};

/// `POST /admin/v1/ca/nodes/{id}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaNodeRequest {
    /// `renew`, `revoke` or `readmit`.
    pub action: String,
    /// Why (recorded with a revocation).
    #[serde(default)]
    pub reason: Option<String>,
}

/// Its answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaNodeReply {
    /// The node.
    pub node: u64,
    /// What was done.
    pub action: String,
    /// The certificate issued (renew, readmit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    /// Valid until (Unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_after: Option<u64>,
    /// The serials revoked (revoke).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revoked: Vec<String>,
}

fn no_key() -> SprigError {
    SprigError::new(
        SprigCode::ConfigInvalid,
        format!(
            "this controller holds no CA key: set {} on the controllers",
            internal_ca::KEY_FILE_VAR
        ),
    )
}

fn internal_ca() -> Result<InternalCa> {
    InternalCa::from_env()?.ok_or_else(no_key)
}

impl AdminService {
    /// `GET /admin/v1/ca`.
    ///
    /// # Errors
    /// The store's.
    pub async fn ca_show(&self) -> Result<CaView> {
        let ca = InternalCa::from_env()?;
        internal_ca::view(
            ca.as_ref(),
            self.stores.dc.as_ref(),
            self.stores.global.as_ref(),
            self.clock.wall(),
        )
        .await
    }

    /// `POST /admin/v1/ca:init`.
    ///
    /// # Errors
    /// 39998 without the CA key; the store's.
    pub async fn ca_init(&self) -> Result<InitView> {
        internal_ca()?
            .init(
                self.stores.dc.as_ref(),
                self.stores.global.as_ref(),
                self.clock.wall(),
            )
            .await
    }

    /// `POST /admin/v1/ca/nodes/{id}`.
    ///
    /// # Errors
    /// 39792 for an unknown action or a node that
    /// published no key; 39998 without the CA key; the store's.
    pub async fn ca_node(&self, node: &str, req: CaNodeRequest) -> Result<CaNodeReply> {
        let id: u64 = node
            .trim()
            .trim_start_matches("node-")
            .parse()
            .map_err(|_| invalid(format!("{node:?} isn't a node id")))?;
        let (dc, global, now) = (
            self.stores.dc.as_ref(),
            self.stores.global.as_ref(),
            self.clock.wall(),
        );
        let mut reply = CaNodeReply {
            node: id,
            action: req.action.clone(),
            serial: None,
            not_after: None,
            revoked: Vec::new(),
        };
        let issue = |force: bool| async move {
            match internal_ca()?.issue(dc, global, id, force, now).await? {
                IssueOutcome::Issued(c) | IssueOutcome::Current(c) => Ok(c),
                IssueOutcome::NoRequest => Err(invalid(format!(
                    "node {id} has published no key (it does once it starts with issued certificates)"
                ))),
                IssueOutcome::Revoked => Err(invalid(format!(
                    "node {id} is revoked: `sprigctl ca readmit {id}` first"
                ))),
                IssueOutcome::Raced => Err(invalid(format!(
                    "node {id}'s certificate changed meanwhile; try again"
                ))),
            }
        };
        match req.action.as_str() {
            "renew" => {
                let c = issue(true).await?;
                reply.serial = Some(c.serial);
                reply.not_after = Some(c.not_after);
            }
            "revoke" => {
                let reason = req
                    .reason
                    .unwrap_or_else(|| "revoked by an operator".into());
                reply.revoked = internal_ca::revoke(dc, global, id, &reason, now).await?;
            }
            "readmit" => {
                internal_ca::readmit(dc, id).await?;
                let c = issue(true).await?;
                reply.serial = Some(c.serial);
                reply.not_after = Some(c.not_after);
            }
            other => {
                return Err(invalid(format!(
                    "action {other:?}: expected renew, revoke or readmit"
                )));
            }
        }
        Ok(reply)
    }
}
