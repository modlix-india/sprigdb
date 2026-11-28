//! MongoDB reads from copies.
//!
//! A connection is served on the node leading its tenant's unit
//! ([`crate::forward`]). A read command whose `$readPreference` is
//! `secondary` or `secondaryPreferred` runs on a copy instead, through the
//! connection's shadow connection ([`crate::ship`]): `find`, `aggregate`
//! (without `$out` or `$merge`), `count` and `distinct`, outside
//! transactions, and the `getMore` and `killCursors` of the cursors such a
//! read left on the copy. `nearest` reads where the connection is (the
//! leader is the nearest copy), and `primary`, `primaryPreferred` and no
//! preference read the leader.
//!
//! On the copy, a shipped command reads under the read preference's rules:
//! at most `maxStalenessSeconds` old, or `reads.nearest_max_lag_ms` without
//! it (drivers refuse values under 90 s), and never older than the token:
//! `readConcern.afterClusterTime` (a causally consistent session's last
//! operation time), raised to the connection's own last commit.
//!
//! **Operation times.** A reply's `operationTime` is the command's own
//! commit timestamp when it wrote, else this node's cluster time, which
//! never falls below what the node has made visible. Both are hybrid
//! logical clock timestamps in MongoDB's form (`Timestamp(secs, inc)`, the
//! milliseconds in `inc`'s high bits), so `afterClusterTime` converts back
//! exactly.

use std::sync::Arc;

use bson::{Bson, Document, RawDocumentBuf, doc};
use sprig_engine::reads::{ReadMode, ReadPolicy};

use super::conn::Conn;
use crate::frontend::Protocol;
use crate::reader::{ReadFrom, Reader};

/// A token (HLC timestamp) as MongoDB's `Timestamp`.
#[must_use]
pub(crate) fn to_mongo(ts: u64) -> bson::Timestamp {
    let (time, increment) = sprig_engine::txn::hlc::to_mongo(sprig_engine::Ts(ts));
    bson::Timestamp { time, increment }
}

/// MongoDB's `Timestamp` as a token: the inverse of [`to_mongo`].
#[must_use]
pub(crate) fn from_mongo(t: bson::Timestamp) -> u64 {
    let ms = u64::from(t.time) * 1000 + u64::from(t.increment >> 16);
    (ms << 16) | u64::from(t.increment & 0xFFFF)
}

/// What this node has made visible, packed as the cluster clock packs a
/// `Timestamp` (`time << 32 | increment`); `None` without an engine.
pub(crate) fn visible_packed() -> Option<u64> {
    let e = crate::node_engine::engine().ok()?;
    let t = to_mongo(e.txn_shared().visible_ts().0);
    Some((u64::from(t.time) << 32) | u64::from(t.increment))
}

/// A reply's `operationTime`: the connection's commit timestamp when the
/// command raised it (from `before`), else the cluster time `ct`.
pub(crate) fn operation_time(before: Option<u64>, ct: bson::Timestamp) -> bson::Timestamp {
    match sprig_engine::reads::session_token() {
        Some(after) if after > before.unwrap_or(0) && after > 0 => to_mongo(after),
        _ => ct,
    }
}

/// `readConcern.afterClusterTime` of a command, as a token.
fn after_cluster_time(body: &Document) -> Option<u64> {
    match body
        .get_document("readConcern")
        .ok()?
        .get("afterClusterTime")?
    {
        Bson::Timestamp(t) => Some(from_mongo(*t)),
        _ => None,
    }
}

/// The `$readPreference` mode, lower case.
fn preference(body: &Document) -> Option<String> {
    let p = body.get_document("$readPreference").ok()?;
    Some(p.get_str("mode").ok()?.to_ascii_lowercase())
}

/// Whether the command is part of a transaction.
fn in_transaction(body: &Document) -> bool {
    body.get_bool("autocommit") == Ok(false) || body.get_bool("startTransaction") == Ok(true)
}

/// Whether `aggregate`'s pipeline writes (`$out`, `$merge`).
fn writes(body: &Document) -> bool {
    body.get_array("pipeline").is_ok_and(|p| {
        p.iter().any(|s| {
            s.as_document()
                .is_some_and(|d| d.contains_key("$out") || d.contains_key("$merge"))
        })
    })
}

/// A cursor id in a reply (`cursor.id`).
fn reply_cursor(reply: &Document) -> Option<i64> {
    reply.get_document("cursor").ok()?.get_i64("id").ok()
}

impl Conn {
    /// The rules a command on a shadow connection reads under: `None` on
    /// any other connection.
    pub(crate) fn follow_policy(&self, body: &Document) -> Option<ReadPolicy> {
        let r = self.follow.reader.as_ref()?;
        if let Some(t) = after_cluster_time(body) {
            r.raise(t);
        }
        let mut p = r.policy(None);
        if let Ok(prefs) = body.get_document("$readPreference")
            && let Some(s) = prefs
                .get("maxStalenessSeconds")
                .and_then(sprig_doc::filter::as_i64)
                .filter(|s| *s > 0)
        {
            p.mode = ReadMode::Bounded {
                max_lag: std::time::Duration::from_secs(s.unsigned_abs()),
            };
        }
        if preference(body).as_deref() == Some("primary") {
            p.mode = ReadMode::Exact;
        }
        Some(p)
    }

    /// Runs `body` on a copy when its read preference asks for one: its
    /// reply. `None`: the command runs here.
    #[allow(clippy::too_many_lines)] // the choice, the shadow, the cursors
    pub(crate) async fn ship_read(&mut self, name: &str, body: &Document) -> Option<Document> {
        if self.follow.reader.is_some() || !crate::forward::enabled() {
            return None;
        }
        let ours = |id: Option<i64>| id.is_some_and(|i| self.follow.cursors.contains(&i));
        let shipped_cursor = match name {
            "getMore" => ours(body.get_i64("getMore").ok()),
            "killCursors" => body
                .get_array("cursors")
                .is_ok_and(|a| a.iter().any(|c| ours(c.as_i64()))),
            _ => false,
        };
        if !shipped_cursor {
            let read = match name {
                "find" | "count" | "distinct" => true,
                "aggregate" => !writes(body),
                _ => false,
            };
            if !read
                || in_transaction(body)
                || !matches!(
                    preference(body).as_deref(),
                    Some("secondary" | "secondarypreferred")
                )
                || body
                    .get_document("readConcern")
                    .is_ok_and(|c| c.get_str("level") == Ok("snapshot"))
                || !self.follow.may_try()
                || self
                    .oidc
                    .as_ref()
                    .is_some_and(super::conn::OidcState::has_claims)
            {
                return None;
            }
        }
        let principal = self.principal.clone()?;
        let tenant = principal.tenant?.0;
        if self.follow.shadow.is_none() {
            let o = crate::ship::Open {
                listener: self.listener,
                protocol: Protocol::Mongo,
                peer: self.peer,
                tls: self.tls.clone(),
                tenant,
                resume: super::handoff::shadow_json(self, &principal),
                prelude: bytes::Bytes::new(),
                pick: u64::from(self.peer.port()),
            };
            match crate::ship::MongoShadow::open(o) {
                Ok(s) => self.follow.shadow = Some(s),
                Err(e) => {
                    tracing::debug!(tenant, error = %e, "MongoDB read for a copy runs on the leader");
                    self.follow.failed();
                    crate::ship::count(Protocol::Mongo, "unavailable");
                    return None;
                }
            }
        }
        // The token: the session's `afterClusterTime`, raised to this
        // connection's own last commit.
        let mut sent = body.clone();
        let token = after_cluster_time(body)
            .unwrap_or(0)
            .max(crate::reader::session_token());
        if token > 0 {
            let mut rc = body
                .get_document("readConcern")
                .cloned()
                .unwrap_or_default();
            rc.insert("afterClusterTime", to_mongo(token));
            sent.insert("readConcern", rc);
        }
        sent.remove("$clusterTime");
        let raw = RawDocumentBuf::from_document(&sent).ok()?;
        let shadow = self.follow.shadow.as_mut()?;
        let node = shadow.node();
        match shadow.command(raw).await {
            Ok(reply) => {
                let reply = reply.to_document().ok()?;
                match (name, reply_cursor(&reply)) {
                    ("getMore", Some(0)) => {
                        if let Ok(id) = body.get_i64("getMore") {
                            self.follow.cursors.remove(&id);
                        }
                    }
                    ("killCursors", _) => {
                        if let Ok(a) = body.get_array("cursors") {
                            for c in a.iter().filter_map(Bson::as_i64) {
                                self.follow.cursors.remove(&c);
                            }
                        }
                    }
                    (_, Some(id)) if id != 0 => {
                        self.follow.cursors.insert(id);
                    }
                    _ => {}
                }
                crate::ship::count(Protocol::Mongo, "ok");
                tracing::trace!(tenant, node, name, "MongoDB read ran on a copy");
                Some(reply)
            }
            Err(e) => {
                tracing::debug!(tenant, node, error = %e, "MongoDB shadow connection failed");
                self.follow.failed();
                crate::ship::count(Protocol::Mongo, "fallback");
                // A cursor left on the copy is gone with the connection.
                shipped_cursor.then(|| cursor_gone(body.get_i64("getMore").unwrap_or(0)))
            }
        }
    }
}

/// A shadow connection's reader state, from its resume.
pub(crate) fn shadow_reader(tenant: u64) -> Arc<Reader> {
    Arc::new(Reader::new(ReadFrom::Nearest(None), tenant))
}

/// The reply to a `getMore` of a cursor that is gone.
pub(crate) fn cursor_gone(id: i64) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": format!("cursor id {id} not found"),
        "code": 43,
        "codeName": "CursorNotFound",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mongo_timestamps_convert_back_exactly() {
        let ts = (1_764_000_000_123u64 << 16) | 7;
        let m = to_mongo(ts);
        assert_eq!(m.time, 1_764_000_000);
        assert_eq!(from_mongo(m), ts);
        let later = to_mongo(ts + 1);
        assert!((later.time, later.increment) > (m.time, m.increment));
    }

    #[test]
    fn which_commands_are_reads() {
        assert!(writes(
            &doc! {"aggregate": "c", "pipeline": [{"$match": {}}, {"$out": "x"}]}
        ));
        assert!(!writes(
            &doc! {"aggregate": "c", "pipeline": [{"$match": {}}]}
        ));
        assert!(in_transaction(&doc! {"find": "c", "autocommit": false}));
        assert_eq!(
            preference(&doc! {"$readPreference": {"mode": "secondaryPreferred"}}).as_deref(),
            Some("secondarypreferred")
        );
        let t = bson::Timestamp {
            time: 5,
            increment: 3 << 16,
        };
        assert_eq!(
            after_cluster_time(&doc! {"readConcern": {"afterClusterTime": t}}),
            Some(5_003u64 << 16)
        );
    }
}
