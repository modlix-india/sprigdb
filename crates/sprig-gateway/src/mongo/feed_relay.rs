//! A tenant's change feed relayed from its home DC, for MongoDB change
//! streams opened at a gateway of another DC.
//!
//! ```text
//! open at a gateway outside the tenant's home DC (ChangeFeed::open):
//!   GatewayPeer.TailFeed on a ready gateway of the home DC (each in turn
//!   until one answers), with the tenant, the client's resume token and
//!   this DC
//!   the home gateway opens the tenant's tail on its shared reader (the
//!   cluster's binlog is read only in its own DC, once for every stream),
//!   and sends `opened` with the starting token, or the error; then every
//!   item (event, transaction end, progress) as JSON
//! this gateway renders the items for its client as it renders its own
//!   tail's: the session's privileges, row policies, `fullDocument` and
//!   pipeline all apply here, where the session is
//! backpressure: this gateway reads the next message only once its stream's
//!   queue has room, so a slow client stalls the home gateway's sender
//! the home gateway gone: the stream fails with 39788 and the client
//!   resumes from its last token, through any gateway
//! residency: under `deny_remote_access`, a DC outside the tenant's
//!   allowed DCs gets no tail (39302), at either end
//! ```
//!
//! The items keep the binlog's typed values (`Value`), so documents are
//! rebuilt with the types the client wrote, as at the home DC.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value as Json, json};
use sprig_api::proto::internal::{TailFeedMessage, TailFeedRequest};
use sprig_binlog::{Date, DateTime, Gtid, OpHint, Time, Timestamp, Value};
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ShardId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::{
    ChangeEvent, DdlKind, EventId, FeedItem, Image, Op, ResumeToken, ShardRef, StartAt,
    TailOptions, TenantTail,
};
use tokio::sync::mpsc;

use crate::tenant::TenantServices;

/// How long the home gateway may take to open the tail.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// Items buffered between the peer call and the stream.
const BUFFER: usize = 256;

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("relayed feed item: {what}"))
}

fn b64(b: &[u8]) -> Json {
    Json::String(STANDARD.encode(b))
}

fn unb64(v: &Json) -> Result<Vec<u8>> {
    STANDARD
        .decode(v.as_str().ok_or_else(|| bad("bytes"))?)
        .map_err(|_| bad("base64"))
}

fn date_json(d: Date) -> Json {
    json!([d.year, d.month, d.day])
}

fn date_of(v: &Json) -> Result<Date> {
    let a = v.as_array().ok_or_else(|| bad("date"))?;
    let n = |i: usize| a.get(i).and_then(Json::as_u64).ok_or_else(|| bad("date"));
    Ok(Date {
        year: u16::try_from(n(0)?).map_err(|_| bad("year"))?,
        month: u8::try_from(n(1)?).map_err(|_| bad("month"))?,
        day: u8::try_from(n(2)?).map_err(|_| bad("day"))?,
    })
}

/// A binlog value as JSON: `[tag, payload]`.
fn value_json(v: &Value) -> Json {
    match v {
        Value::Null => json!(["n"]),
        Value::Int(i) => json!(["i", i]),
        Value::UInt(u) => json!(["u", u]),
        Value::Float(f) => json!(["f", f]),
        Value::Double(d) => json!(["d", d]),
        Value::Decimal(s) => json!(["m", s]),
        Value::Bit(b) => json!(["bit", b]),
        Value::Year(y) => json!(["y", y]),
        Value::Date(d) => json!(["date", date_json(*d)]),
        Value::Time(t) => json!([
            "time",
            [t.negative, t.hours, t.minutes, t.seconds, t.micros, t.fsp]
        ]),
        Value::DateTime(t) => json!([
            "dt",
            [
                date_json(t.date),
                t.hour,
                t.minute,
                t.second,
                t.micros,
                t.fsp
            ]
        ]),
        Value::Timestamp(t) => json!(["ts", [t.secs, t.micros, t.fsp]]),
        Value::Enum(e) => json!(["e", e]),
        Value::Set(s) => json!(["set", s]),
        Value::String(b) => json!(["s", b64(b)]),
        Value::Bytes(b) => json!(["b", b64(b)]),
        Value::Geometry(b) => json!(["g", b64(b)]),
    }
}

fn int<T: TryFrom<u64>>(v: Option<&Json>, what: &str) -> Result<T> {
    v.and_then(Json::as_u64)
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| bad(what))
}

fn value_of(v: &Json) -> Result<Value> {
    let a = v.as_array().ok_or_else(|| bad("value"))?;
    let tag = a
        .first()
        .and_then(Json::as_str)
        .ok_or_else(|| bad("value tag"))?;
    let p = a.get(1);
    let parts = |what: &str| p.and_then(Json::as_array).ok_or_else(|| bad(what));
    Ok(match tag {
        "n" => Value::Null,
        "i" => Value::Int(p.and_then(Json::as_i64).ok_or_else(|| bad("int"))?),
        "u" => Value::UInt(int(p, "uint")?),
        #[allow(clippy::cast_possible_truncation)] // a FLOAT, sent as its own value
        "f" => Value::Float(p.and_then(Json::as_f64).ok_or_else(|| bad("float"))? as f32),
        "d" => Value::Double(p.and_then(Json::as_f64).ok_or_else(|| bad("double"))?),
        "m" => Value::Decimal(
            p.and_then(Json::as_str)
                .ok_or_else(|| bad("decimal"))?
                .to_owned(),
        ),
        "bit" => Value::Bit(int(p, "bit")?),
        "y" => Value::Year(int(p, "year")?),
        "date" => Value::Date(date_of(p.ok_or_else(|| bad("date"))?)?),
        "time" => {
            let t = parts("time")?;
            Value::Time(Time {
                negative: t
                    .first()
                    .and_then(Json::as_bool)
                    .ok_or_else(|| bad("time"))?,
                hours: int(t.get(1), "hours")?,
                minutes: int(t.get(2), "minutes")?,
                seconds: int(t.get(3), "seconds")?,
                micros: int(t.get(4), "micros")?,
                fsp: int(t.get(5), "fsp")?,
            })
        }
        "dt" => {
            let t = parts("datetime")?;
            Value::DateTime(DateTime {
                date: date_of(t.first().ok_or_else(|| bad("datetime"))?)?,
                hour: int(t.get(1), "hour")?,
                minute: int(t.get(2), "minute")?,
                second: int(t.get(3), "second")?,
                micros: int(t.get(4), "micros")?,
                fsp: int(t.get(5), "fsp")?,
            })
        }
        "ts" => {
            let t = parts("timestamp")?;
            Value::Timestamp(Timestamp {
                secs: int(t.first(), "secs")?,
                micros: int(t.get(1), "micros")?,
                fsp: int(t.get(2), "fsp")?,
            })
        }
        "e" => Value::Enum(int(p, "enum")?),
        "set" => Value::Set(int(p, "set")?),
        "s" => Value::String(unb64(p.ok_or_else(|| bad("string"))?)?),
        "b" => Value::Bytes(unb64(p.ok_or_else(|| bad("bytes"))?)?),
        "g" => Value::Geometry(unb64(p.ok_or_else(|| bad("geometry"))?)?),
        other => return Err(bad(&format!("value tag {other}"))),
    })
}

fn image_json(i: &Image) -> Json {
    Json::Array(i.0.iter().map(|(n, v)| json!([n, value_json(v)])).collect())
}

fn image_of(v: &Json) -> Result<Image> {
    let a = v.as_array().ok_or_else(|| bad("image"))?;
    let mut out = Vec::with_capacity(a.len());
    for c in a {
        let name = c
            .get(0)
            .and_then(Json::as_str)
            .ok_or_else(|| bad("column"))?;
        out.push((
            name.to_owned(),
            value_of(c.get(1).ok_or_else(|| bad("column value"))?)?,
        ));
    }
    Ok(Image(out))
}

fn op_text(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
        Op::Truncate => "truncate",
        Op::Ddl(DdlKind::Create) => "ddl_create",
        Op::Ddl(DdlKind::Alter) => "ddl_alter",
        Op::Ddl(DdlKind::Drop) => "ddl_drop",
        Op::Ddl(DdlKind::Rename) => "ddl_rename",
        Op::Ddl(DdlKind::Other) => "ddl_other",
    }
}

fn op_of(s: &str) -> Result<Op> {
    Ok(match s {
        "insert" => Op::Insert,
        "update" => Op::Update,
        "delete" => Op::Delete,
        "truncate" => Op::Truncate,
        "ddl_create" => Op::Ddl(DdlKind::Create),
        "ddl_alter" => Op::Ddl(DdlKind::Alter),
        "ddl_drop" => Op::Ddl(DdlKind::Drop),
        "ddl_rename" => Op::Ddl(DdlKind::Rename),
        "ddl_other" => Op::Ddl(DdlKind::Other),
        other => return Err(bad(&format!("op {other}"))),
    })
}

fn hint_text(h: OpHint) -> &'static str {
    match h {
        OpHint::Insert => "i",
        OpHint::Update => "u",
        OpHint::Replace => "r",
        OpHint::Delete => "d",
        OpHint::Expire => "x",
        OpHint::Notified => "k",
    }
}

fn hint_of(s: &str) -> Option<OpHint> {
    match s {
        "i" => Some(OpHint::Insert),
        "u" => Some(OpHint::Update),
        "r" => Some(OpHint::Replace),
        "d" => Some(OpHint::Delete),
        "x" => Some(OpHint::Expire),
        "k" => Some(OpHint::Notified),
        _ => None,
    }
}

/// A feed item as the relay sends it.
#[must_use]
pub fn encode(item: &FeedItem) -> Json {
    match item {
        FeedItem::TxnEnd { token } => json!({"k": "txn_end", "token": token.to_hex()}),
        FeedItem::Progress { token } => json!({"k": "progress", "token": token.to_hex()}),
        FeedItem::Event(e) => json!({
            "k": "event",
            "token": e.token.to_hex(),
            "id": [e.id.cluster.as_str(), e.id.gtid.to_string(), e.id.row],
            "tenant": e.tenant.0,
            "db": e.db.0,
            "db_name": e.db_name,
            "object": e.object,
            "table": e.table,
            "shard": [e.shard.group.0, e.shard.shard.0],
            "key": image_json(&e.key),
            "op": op_text(e.op),
            "before": e.before.as_ref().map(image_json),
            "after": e.after.as_ref().map(image_json),
            "delta": e.delta,
            "hlc": e.hlc,
            "txn": e.txn.to_string(),
            "ddl": e.ddl,
            "hint": e.hint.map(hint_text),
        }),
    }
}

fn token_of(v: &Json) -> Result<ResumeToken> {
    ResumeToken::from_hex(v.as_str().ok_or_else(|| bad("token"))?)
}

fn gtid_of(v: Option<&Json>) -> Result<Gtid> {
    v.and_then(Json::as_str)
        .ok_or_else(|| bad("gtid"))?
        .parse()
        .map_err(|_| bad("gtid"))
}

/// A relayed feed item back.
///
/// # Errors
/// Internal for an item this build can't read.
pub fn decode(v: &Json) -> Result<FeedItem> {
    let token = token_of(v.get("token").ok_or_else(|| bad("token"))?)?;
    match v.get("k").and_then(Json::as_str) {
        Some("txn_end") => return Ok(FeedItem::TxnEnd { token }),
        Some("progress") => return Ok(FeedItem::Progress { token }),
        Some("event") => {}
        _ => return Err(bad("kind")),
    }
    let id = v
        .get("id")
        .and_then(Json::as_array)
        .ok_or_else(|| bad("id"))?;
    let shard = v
        .get("shard")
        .and_then(Json::as_array)
        .ok_or_else(|| bad("shard"))?;
    let opt_image = |k: &str| -> Result<Option<Image>> {
        match v.get(k) {
            None | Some(Json::Null) => Ok(None),
            Some(i) => image_of(i).map(Some),
        }
    };
    let text = |k: &str| v.get(k).and_then(Json::as_str).map(str::to_owned);
    Ok(FeedItem::Event(Box::new(ChangeEvent {
        id: EventId {
            cluster: ClusterId::new(
                id.first()
                    .and_then(Json::as_str)
                    .ok_or_else(|| bad("cluster"))?,
            )
            .map_err(|_| bad("cluster"))?,
            gtid: gtid_of(id.get(1))?,
            row: int(id.get(2), "row")?,
        },
        token,
        tenant: TenantId(int(v.get("tenant"), "tenant")?),
        db: DbId(int(v.get("db"), "db")?),
        db_name: text("db_name").unwrap_or_default(),
        object: int(v.get("object"), "object")?,
        table: text("table").unwrap_or_default(),
        shard: ShardRef {
            group: GroupId(int(shard.first(), "group")?),
            shard: ShardId(int(shard.get(1), "shard")?),
        },
        key: image_of(v.get("key").ok_or_else(|| bad("key"))?)?,
        op: op_of(&text("op").unwrap_or_default())?,
        before: opt_image("before")?,
        after: opt_image("after")?,
        delta: v.get("delta").and_then(Json::as_array).map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(str::to_owned))
                .collect()
        }),
        hlc: int(v.get("hlc"), "hlc")?,
        txn: gtid_of(v.get("txn"))?,
        ddl: text("ddl"),
        hint: text("hint").as_deref().and_then(hint_of),
    })))
}

/// Whether residency keeps `tenant`'s tail out of `dc`: the tenant's
/// residency doesn't allow it and says `deny_remote_access`.
async fn denied(svc: &TenantServices, tenant: TenantId, dc: &str) -> Result<bool> {
    let e = crate::inherited::dir_entry(svc, tenant)
        .await?
        .unwrap_or_default();
    Ok(sprig_auth::TenantResidency::of(&e.residency)
        .is_some_and(|r| r.deny_remote_access && !r.allowed_dcs.iter().any(|d| d.as_str() == dc)))
}

fn deny(tenant: TenantId, dc: &str) -> SprigError {
    SprigError::new(
        SprigCode::RemoteAccessDenied,
        format!(
            "ER_SPRIG_REMOTE_ACCESS_DENIED: tenant {tenant}'s residency keeps its change feed out \
             of data center {dc} (deny_remote_access)"
        ),
    )
}

fn error_json(e: &SprigError) -> String {
    let (_, body) = sprig_api::status::to_rest(e);
    serde_json::to_string(&body).unwrap_or_default()
}

fn error_of(json: &str) -> SprigError {
    match serde_json::from_str::<sprig_api::status::ErrorBody>(json) {
        Ok(b) => sprig_api::status::from_rest(&b),
        Err(e) => SprigError::internal(format!("relayed feed error: {e}")),
    }
}

/// Serves `GatewayPeer.TailFeed` (the home DC's side): opens the tenant's
/// tail on this gateway's shared reader, sends `opened` with the starting
/// token or the error, then every item until the caller goes.
#[must_use]
#[allow(clippy::too_many_lines)] // open, then relay: one task
pub fn serve(
    svc: Option<Arc<TenantServices>>,
    req: TailFeedRequest,
) -> mpsc::Receiver<TailFeedMessage> {
    let (tx, rx) = mpsc::channel(BUFFER);
    tokio::spawn(async move {
        let tenant = TenantId(req.tenant_id);
        let opened = async {
            let svc = svc.ok_or_else(|| {
                SprigError::new(
                    SprigCode::ForwardFailed,
                    "this gateway serves no change feeds",
                )
            })?;
            if denied(&svc, tenant, &req.caller_dc).await? {
                return Err(deny(tenant, &req.caller_dc));
            }
            let (clusters, placement, names) = super::changes::feed_parts(
                Arc::clone(&svc.global),
                svc.locator.dc_store(),
                Arc::clone(&svc.locator) as Arc<dyn sprig_catalog::CatalogLocator>,
            )?;
            let start = if req.start_at_hlc != 0 {
                // `startAtOperationTime` from another DC: the time index of
                // the tenant's cluster here.
                crate::http::changes::start_at(
                    &svc,
                    &clusters,
                    &placement,
                    tenant,
                    req.start_at_hlc,
                )
                .await?
            } else if req.start_after.is_empty() {
                StartAt::Now
            } else {
                StartAt::After(ResumeToken::from_hex(&req.start_after)?)
            };
            let tail = TenantTail::start(
                tenant,
                start,
                clusters,
                Arc::clone(&placement),
                names,
                TailOptions {
                    progress_every: Duration::from_millis(500),
                    ..TailOptions::default()
                },
            )
            .await?;
            let first = if req.start_after.is_empty() {
                let segment = placement
                    .history(tenant)
                    .await?
                    .last()
                    .map_or(0, |h| h.map_version);
                ResumeToken {
                    tenant,
                    clamp: 0,
                    cluster: tail.cluster().clone(),
                    segment,
                    pos: tail.position().clone(),
                    until: None,
                    progress: true,
                }
                .to_hex()
            } else {
                req.start_after.clone()
            };
            Ok::<_, SprigError>((tail, first))
        }
        .await;
        let (mut tail, first) = match opened {
            Ok(x) => x,
            Err(e) => {
                let _ = tx
                    .send(TailFeedMessage {
                        error_json: error_json(&e),
                        ..TailFeedMessage::default()
                    })
                    .await;
                return;
            }
        };
        if tx
            .send(TailFeedMessage {
                opened: true,
                item: first.into_bytes(),
                ..TailFeedMessage::default()
            })
            .await
            .is_err()
        {
            return;
        }
        metrics::gauge!("sprig_mongo_cursors_open", "kind" => "relayed_feed").increment(1.0);
        loop {
            let item = tokio::select! {
                () = tx.closed() => break,
                i = tail.next() => i,
            };
            let (m, end) = match item {
                Ok(i) => (
                    TailFeedMessage {
                        item: encode(&i).to_string().into_bytes(),
                        ..TailFeedMessage::default()
                    },
                    false,
                ),
                Err(e) => (
                    TailFeedMessage {
                        error_json: error_json(&e),
                        ..TailFeedMessage::default()
                    },
                    true,
                ),
            };
            if tx.send(m).await.is_err() || end {
                break;
            }
        }
        metrics::gauge!("sprig_mongo_cursors_open", "kind" => "relayed_feed").decrement(1.0);
    });
    rx
}

fn lost(home: &DcId, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::ForwardFailed,
        format!(
            "ER_SPRIG_FORWARD_FAILED: the change feed's gateway in {home} is gone ({e}); resume \
             from the last token"
        ),
    )
}

/// The tenant's home DC when it isn't this gateway's.
///
/// # Errors
/// The directory's.
pub async fn remote_home(svc: &TenantServices, tenant: TenantId) -> Result<Option<DcId>> {
    Ok(sprig_shard::home::read_home(&*svc.global, tenant)
        .await?
        .map(|e| e.home_dc)
        .filter(|h| *h != svc.dc))
}

/// A tail of `tenant`'s feed relayed from a gateway of `home`: its
/// starting token (hex) and its items. A `start_at_hlc` other than 0
/// starts at that time through the home cluster's feed time index
/// (`startAtOperationTime`); the caller leaves out the items stamped
/// before it.
///
/// # Errors
/// 39302 under residency; the home gateway's error (39800, 39802 for a
/// time it can't start at); 39788 when no gateway of `home` answers.
pub async fn open(
    svc: &TenantServices,
    home: &DcId,
    tenant: TenantId,
    start_after: Option<&ResumeToken>,
    start_at_hlc: u64,
) -> Result<(String, mpsc::Receiver<Result<FeedItem>>)> {
    if denied(svc, tenant, svc.dc.as_str()).await? {
        return Err(deny(tenant, svc.dc.as_str()));
    }
    let peers = svc.peers.get().ok_or_else(|| {
        SprigError::new(
            SprigCode::ForwardFailed,
            format!(
                "ER_SPRIG_FORWARD_FAILED: the tenant is homed in {home}, and this gateway has no \
                 peer service to relay its change feed"
            ),
        )
    })?;
    let req = TailFeedRequest {
        tenant_id: tenant.0,
        start_after: start_after.map(ResumeToken::to_hex).unwrap_or_default(),
        caller_dc: svc.dc.to_string(),
        start_at_hlc,
    };
    let mut last = None;
    for rec in &peers.gateways_in(home).await? {
        let mut s = match peers.tail_feed(rec, req.clone()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%home, gateway = %rec.gateway_id, error = %e, "change feed: home gateway unreachable; trying the next");
                last = Some(e);
                continue;
            }
        };
        let first = match tokio::time::timeout(OPEN_TIMEOUT, s.message()).await {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => {
                last = Some(lost(home, "the tail ended before it opened"));
                continue;
            }
            Ok(Err(status)) => {
                last = Some(lost(home, status.message()));
                continue;
            }
            Err(_) => {
                last = Some(lost(home, "no answer within 30 s"));
                continue;
            }
        };
        if !first.error_json.is_empty() {
            metrics::counter!("sprig_mongo_change_streams_relayed_total", "outcome" => "refused")
                .increment(1);
            return Err(error_of(&first.error_json));
        }
        let token = String::from_utf8_lossy(&first.item).into_owned();
        metrics::counter!("sprig_mongo_change_streams_relayed_total", "outcome" => "opened")
            .increment(1);
        tracing::debug!(%home, gateway = %rec.gateway_id, %tenant, "change feed relayed from the tenant's home DC");
        let (tx, rx) = mpsc::channel(BUFFER);
        let home = home.clone();
        tokio::spawn(async move {
            loop {
                let item = match s.message().await {
                    Ok(Some(m)) if !m.error_json.is_empty() => Err(error_of(&m.error_json)),
                    Ok(Some(m)) => serde_json::from_slice::<Json>(&m.item)
                        .map_err(|e| bad(&e.to_string()))
                        .and_then(|v| decode(&v)),
                    Ok(None) => Err(lost(&home, "the tail ended")),
                    Err(status) => Err(lost(&home, status.message())),
                };
                let end = item.is_err();
                // The stream is gone, or the tail ended: dropping the call
                // ends it in the home DC too.
                if tx.send(item).await.is_err() || end {
                    break;
                }
            }
        });
        return Ok((token, rx));
    }
    metrics::counter!("sprig_mongo_change_streams_relayed_total", "outcome" => "unavailable")
        .increment(1);
    Err(last.unwrap_or_else(|| lost(home, "no ready gateway there")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)] // one event with every value type
    fn items_cross_with_their_types() {
        let token = ResumeToken {
            tenant: TenantId(7),
            clamp: 3,
            cluster: ClusterId::new("fra1-c1").unwrap(),
            segment: 2,
            pos: "0-1-5".parse().unwrap(),
            until: None,
            progress: false,
        };
        let g: Gtid = "0-1-5".parse().unwrap();
        let after = Image(vec![
            ("id".into(), Value::Int(-4)),
            ("u".into(), Value::UInt(u64::MAX)),
            ("f".into(), Value::Float(1.5)),
            ("d".into(), Value::Double(2.25)),
            ("m".into(), Value::Decimal("-12.340".into())),
            ("bit".into(), Value::Bit(5)),
            ("y".into(), Value::Year(2026)),
            (
                "date".into(),
                Value::Date(Date {
                    year: 2026,
                    month: 11,
                    day: 16,
                }),
            ),
            (
                "t".into(),
                Value::Time(Time {
                    negative: true,
                    hours: 838,
                    minutes: 59,
                    seconds: 59,
                    micros: 1,
                    fsp: 6,
                }),
            ),
            (
                "dt".into(),
                Value::DateTime(DateTime {
                    date: Date {
                        year: 1,
                        month: 2,
                        day: 3,
                    },
                    hour: 4,
                    minute: 5,
                    second: 6,
                    micros: 7,
                    fsp: 3,
                }),
            ),
            (
                "ts".into(),
                Value::Timestamp(Timestamp {
                    secs: 9,
                    micros: 10,
                    fsp: 2,
                }),
            ),
            ("e".into(), Value::Enum(2)),
            ("set".into(), Value::Set(6)),
            ("doc".into(), Value::String(b"{\"a\": 1}".to_vec())),
            ("bin".into(), Value::Bytes(vec![0, 255, 7])),
            ("geo".into(), Value::Geometry(vec![1, 2, 3])),
            ("nil".into(), Value::Null),
        ]);
        let ev = FeedItem::Event(Box::new(ChangeEvent {
            id: EventId {
                cluster: ClusterId::new("fra1-c1").unwrap(),
                gtid: g,
                row: 3,
            },
            token: token.clone(),
            tenant: TenantId(7),
            db: DbId(11),
            db_name: "shop".into(),
            object: 42,
            table: "orders".into(),
            shard: ShardRef {
                group: GroupId(0),
                shard: ShardId(1),
            },
            key: Image(vec![("id".into(), Value::Int(-4))]),
            op: Op::Update,
            before: None,
            after: Some(after),
            delta: Some(vec!["u".into()]),
            hlc: 99,
            txn: g,
            ddl: None,
            hint: Some(OpHint::Replace),
        }));
        for item in [
            ev,
            FeedItem::TxnEnd {
                token: token.clone(),
            },
            FeedItem::Progress { token },
        ] {
            let wire = encode(&item).to_string();
            let back = decode(&serde_json::from_str(&wire).unwrap()).unwrap();
            assert_eq!(back, item, "{wire}");
        }
        let ddl = |k: DdlKind| op_of(op_text(Op::Ddl(k))).unwrap();
        assert_eq!(ddl(DdlKind::Rename), Op::Ddl(DdlKind::Rename));
        assert!(decode(&json!({"k": "event", "token": "zz"})).is_err());
    }
}
