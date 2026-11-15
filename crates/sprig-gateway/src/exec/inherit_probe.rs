//! Whether a change stream's subscriber could see inherited records just
//! before a change (`sprig_inherit::probe`): a `delete` goes
//! only to subscribers who could see the record.
//!
//! The change stream (`Changes.Watch`, in this process) sends one marked
//! statement through a session of the subscriber,
//! `/* sprig inherit probe <nonce> */ <ProbeRequest as JSON>`; the nonce is
//! this process's own, so no client can send it (it would let a client
//! test made-up rows against other spaces' layers). The inheritance layer
//! answers one row per probe, `(i, v)`: `v = 1` when probe `i`'s record
//! was in the subscriber's merged view, as its `SELECT` policies filter
//! it, just before the change, `0` when it wasn't, `2` when the
//! subscriber's view doesn't read the changed row (another event will
//! say). A probe that fails answers 0. A `reads_only` probe asks only
//! whether the view reads the changed row now (`1` or `2`): a
//! change it doesn't read sends nothing, whatever the record is now. A
//! `counted` probe is of a change the stream sends by its copy's handover:
//! when the subscriber's view reaches the parent tenant through
//! that tenant edge, it reads the changed row (`1` for `reads_only`), and
//! its record before is taken through the edge as the copy holds it; when
//! a materialized share inside the tenant cuts the view first, it is
//! answered as without `counted`.

use std::sync::OnceLock;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sprig_common::Result;
use sprig_proto_mysql::ColumnDef;
use sprig_types::Rows;

const MARK: &str = "/* sprig inherit probe ";

fn nonce() -> &'static str {
    static N: OnceLock<String> = OnceLock::new();
    N.get_or_init(|| {
        format!(
            "{}{}",
            uuid::Uuid::now_v7().simple(),
            uuid::Uuid::now_v7().simple()
        )
    })
}

/// One record whose visibility before a change is asked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbeItem {
    /// The inheritable table.
    pub table: String,
    /// The record's key, as the change's images carry it.
    pub key: Map<String, Value>,
    /// The space of the changed layer row, when it is one.
    #[serde(default)]
    pub space: Option<u64>,
    /// The `_sprig_share` of the changed copy row, when it is one.
    #[serde(default)]
    pub share: Option<Value>,
    /// The changed row before the change (`None`: an insert).
    #[serde(default)]
    pub before: Option<Map<String, Value>>,
    /// The changed row's columns, for an insert.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Ask only whether the subscriber's view reads the changed row.
    #[serde(default)]
    pub reads_only: bool,
    /// The stream counts the change by its copy's handover.
    #[serde(default)]
    pub counted: bool,
}

/// What is asked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbeRequest {
    /// The logical database.
    pub db: String,
    /// The records.
    pub probes: Vec<ProbeItem>,
}

/// The marked statement of `req`.
#[must_use]
pub(crate) fn statement(req: &ProbeRequest) -> String {
    format!(
        "{MARK}{} */ {}",
        nonce(),
        serde_json::to_string(req).unwrap_or_default()
    )
}

/// What a statement is to the probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Parsed {
    /// Not a probe.
    No,
    /// Marked, but not by this process (refused).
    Forged,
    /// This process's probe.
    Probe(ProbeRequest),
}

impl Parsed {
    /// Whether the statement carries the probe's mark.
    #[must_use]
    pub(crate) fn marked(&self) -> bool {
        !matches!(self, Self::No)
    }
}

/// What `text` is ([`Parsed`]).
#[must_use]
pub(crate) fn parse(text: &Bytes) -> Parsed {
    if !text.starts_with(MARK.as_bytes()) {
        return Parsed::No;
    }
    let s = String::from_utf8_lossy(&text[MARK.len()..]).into_owned();
    s.split_once(" */ ")
        .filter(|(n, _)| *n == nonce())
        .and_then(|(_, j)| serde_json::from_str::<ProbeRequest>(j).ok())
        .map_or(Parsed::Forged, Parsed::Probe)
}

/// The answer per probe (`None`: the view doesn't read the changed row),
/// from the statement's rows `(i, v)` as text.
#[must_use]
pub(crate) fn answer(req: &ProbeRequest, rows: &[Vec<Option<String>>]) -> Vec<Option<bool>> {
    let mut out = vec![Some(false); req.probes.len()];
    for r in rows {
        let n = |i: usize| {
            r.get(i)
                .cloned()
                .flatten()
                .and_then(|v| v.parse::<usize>().ok())
        };
        if let (Some(i), Some(v)) = (n(0), n(1))
            && let Some(slot) = out.get_mut(i)
        {
            *slot = match v {
                1 => Some(true),
                2 => None,
                _ => Some(false),
            };
        }
    }
    out
}

/// Runs `req`'s probes in the session ([`sprig_inherit::probe`]), as rows
/// `(i, v)`.
pub(super) async fn run(
    ctx: &sprig_inherit::Ctx<'_>,
    r: &mut dyn sprig_inherit::Runner,
    req: &ProbeRequest,
    columns: (ColumnDef, ColumnDef),
) -> Result<Rows> {
    let mut rows = Vec::with_capacity(req.probes.len());
    for (i, p) in req.probes.iter().enumerate() {
        let source = match (p.space, &p.share) {
            (Some(s), _) => sprig_inherit::probe::Source::Layer(sprig_inherit::SpaceId(s)),
            (None, Some(k)) => sprig_inherit::probe::Source::Copy(k.clone()),
            (None, None) => sprig_inherit::probe::Source::Above,
        };
        // A counted change, when no materialized share inside the tenant
        // cuts the view before its tenant edge.
        let counted = p.counted
            && sprig_inherit::probe::through_tenant_edge(ctx, &req.db, &p.table).unwrap_or(false);
        if p.reads_only && counted {
            rows.push(vec![Some(i.to_string().into_bytes()), Some(b"1".to_vec())]);
            continue;
        }
        if p.reads_only {
            let v = match sprig_inherit::probe::reads(ctx, &req.db, &p.table, &source) {
                Ok(true) => b"1",
                Ok(false) => b"2",
                Err(e) => {
                    // Fall back to the record as read now.
                    tracing::warn!(table = %p.table, error = %e, "a change stream's source check failed");
                    b"0"
                }
            };
            rows.push(vec![Some(i.to_string().into_bytes()), Some(v.to_vec())]);
            continue;
        }
        let probe = sprig_inherit::probe::Probe {
            table: p.table.clone(),
            key: p.key.clone(),
            source,
            before: p.before.clone(),
            columns: p.columns.clone(),
            counted,
        };
        let v = match Box::pin(sprig_inherit::probe::visible_before(
            ctx, r, &req.db, &probe,
        ))
        .await
        {
            Ok(Some(true)) => b"1",
            Ok(Some(false)) => b"0",
            Ok(None) => b"2",
            Err(e) => {
                // Not visible: a delete the subscriber may have needed is
                // left out rather than one it mustn't see sent.
                tracing::warn!(table = %p.table, error = %e, "a change stream's visibility probe failed");
                b"0"
            }
        };
        rows.push(vec![Some(i.to_string().into_bytes()), Some(v.to_vec())]);
    }
    Ok(Rows {
        columns: vec![columns.0, columns.1],
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_parse_only_with_this_process_nonce() {
        let req = ProbeRequest {
            db: "shop".into(),
            probes: vec![ProbeItem {
                table: "docs".into(),
                space: Some(7),
                ..ProbeItem::default()
            }],
        };
        let s = Bytes::from(statement(&req));
        assert_eq!(parse(&s), Parsed::Probe(req.clone()));
        let forged = Bytes::from(format!("{MARK}0000 */ {{\"db\":\"x\",\"probes\":[]}}"));
        assert_eq!(parse(&forged), Parsed::Forged);
        assert_eq!(parse(&Bytes::from_static(b"SELECT 1")), Parsed::No);
        let rows = vec![vec![Some("0".into()), Some("1".into())]];
        assert_eq!(answer(&req, &rows), vec![Some(true)]);
        let rows = vec![vec![Some("0".into()), Some("2".into())]];
        assert_eq!(answer(&req, &rows), vec![None]);
    }
}
