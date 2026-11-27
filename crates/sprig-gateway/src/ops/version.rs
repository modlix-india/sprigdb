//! The version gate at start: a node whose binary is older than the DC's
//! cluster version refuses to start (the others may write formats it can't
//! read; a downgrade is possible only until the flip).

use sprig_common::{Result, SprigCode, SprigError};

/// Refuses to start a node older than the cluster version.
///
/// # Errors
/// 39998 (configuration) naming both versions; the store's errors are
/// logged and let the node start (etcd is also needed to serve, and is
/// checked again there).
pub async fn check(dc: &dyn sprig_meta::MetaStore) -> Result<()> {
    let mine = super::node_version();
    match sprig_catalog::cluster_version::read(dc).await {
        Ok(cur) => {
            if sprig_catalog::cluster_version::may_start(&mine, cur.as_ref()) {
                Ok(())
            } else {
                Err(SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!(
                        "this node runs {mine}, older than the cluster version {}: after a rolling upgrade is finalized, nodes can't go back to an older binary",
                        cur.map(|c| c.version).unwrap_or_default()
                    ),
                ))
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "the cluster version couldn't be read; starting");
            Ok(())
        }
    }
}
