//! Fan-in queue sinks' reads: `CREATE SINK ... FROM QUEUE q FAN IN` in an
//! ancestor P reads every member's instance of the queue definition `q`.
//! The sink's task (in a worker) lists the instances from the DC's index
//! and asks a gateway, as the sink's owner, through `LogicPeer.SinkSql`:
//!
//! ```text
//! FanInGet {def, member, db, queue, max, lease_ms}
//!   the owner holds SPRIG_FAN_IN, SELECT and DELETE on db.queue in P, the
//!   definition reaches P (P declared it or is an intermediate member),
//!   member is below P and doesn't deny remote access (residency)
//!   → a get on the member's instance (an internal principal of the member,
//!     as the AMQP fan-in consumer's), each message held lease_ms:
//!     mq_get's columns (delivery_token first)
//!   (under the task's lease tag when it gives one)
//! FanInRelease {def, member, db, queue, tag}
//!   the same checks → the member's instance gives back what was held
//!   under tag (a dead task's leases, as mq_release)
//! FanInSettle {tokens, ack}
//!   each token checked (its MAC, its tenant below P) and settled with its
//!   owner: acknowledged, or rejected without requeue (the queue's
//!   dead-lettering applies)
//! FanInStreamRead {def, member, db, stream, from, from_ms, max, wait}
//!   (`CREATE SINK ... FROM STREAM s FAN IN`) the owner holds SPRIG_FAN_IN
//!   and SELECT on db.stream in P, the stream definition reaches P, the
//!   member is below P and doesn't deny remote access
//!   → the member instance's records, per partition from the offset
//!     `from` gives (a partition it doesn't name: from the first record at
//!     or after from_ms), at most max per partition, waiting up to wait
//!     for one: StreamRead's columns
//! ```

use std::sync::Arc;

use sprig_common::ids::{DbId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::model::db_key;
use sprig_inherit::msg::{Chain, DefKey, DefKind, DefState, Definition};
use sprig_proto_amqp::error::MqError;
use sprig_sinks::peer::{PeerError, SqlReply, SqlResult};

use super::mq::internal_principal;
use crate::http::sql::Caller;
use crate::tenant::TenantServices;

fn mq_err(e: &MqError) -> SprigError {
    let mut x = SprigError::standard(
        u16::try_from(e.code.number()).unwrap_or(39250),
        *b"HY000",
        format!("{}: {}", e.code.name(), e.text),
    );
    x.retryable = e.code == sprig_proto_amqp::error::MqCode::OwnerMoved;
    x
}

fn denied(what: impl std::fmt::Display) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!(
            "Access denied; you need (at least one of) the {what} privilege(s) for this operation"
        ),
    )
}

fn engine(svc: &TenantServices) -> Result<Arc<sprig_queues::QueueEngine>> {
    svc.mq.get().cloned().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::LogicEngineUnavailable,
            "the queue engine isn't running on this gateway (AMQP is off)",
        );
        e.retryable = true;
        e
    })
}

/// The reader's tenant and its member `member`'s name, when the reader
/// may fan in from it.
async fn member_of(svc: &TenantServices, reader: TenantId, member: TenantId) -> Result<String> {
    let d = super::dir_entry(svc, member)
        .await?
        .ok_or_else(|| super::target_err(format!("tenant {member} doesn't exist")))?;
    if member == reader || !d.path_ids().contains(&reader.0) {
        return Err(super::target_err(format!(
            "tenant {member} isn't a member below tenant {reader}"
        )));
    }
    if sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access) {
        metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
            .increment(1);
        return Err(SprigError::new(
            SprigCode::InheritResidency,
            format!("tenant {} denies remote access (residency)", d.name),
        ));
    }
    Ok(d.name)
}

/// See the module docs.
///
/// # Errors
/// 1227 without the privileges; 39896 for a definition or member out of
/// reach; 39243 for a member denying remote access; the engine's.
#[allow(clippy::too_many_arguments)]
pub async fn get(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
    max: u32,
    lease_ms: u64,
    tag: Option<&str>,
) -> Result<SqlReply> {
    let v = member_vhost(svc, caller, def, member, db, queue).await?;
    let ds = v
        .get_tagged(queue, max.clamp(1, 1000), lease_ms.max(1), tag)
        .await
        .map_err(|x| mq_err(&x))?;
    let rows = ds
        .iter()
        .map(|d| {
            let token = d
                .annotations
                .get("x-sprig-delivery")
                .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned));
            let body = sprig_queues::sql::body_bytes(d);
            vec![
                SqlResult::cell(token.as_deref().map(str::as_bytes)),
                SqlResult::cell(Some(&body)),
                SqlResult::cell(Some(sprig_queues::sql::properties_json(d).as_bytes())),
                SqlResult::cell(Some(d.message.routing_key.as_bytes())),
                SqlResult::cell(Some(d.message.exchange.as_bytes())),
                SqlResult::cell(Some(if d.redelivered { b"1" } else { b"0" })),
                SqlResult::cell(Some(d.delivery_count.to_string().as_bytes())),
            ]
        })
        .collect();
    metrics::counter!("sprig_sink_fanin_queue_gets_total").increment(1);
    Ok(SqlReply {
        results: vec![SqlResult {
            columns: [
                "delivery_token",
                "body",
                "properties",
                "routing_key",
                "exchange",
                "redelivered",
                "delivery_count",
            ]
            .map(str::to_owned)
            .to_vec(),
            types: Vec::new(),
            rows,
            affected: 0,
            error: None,
        }],
        ..SqlReply::default()
    })
}

/// See the module docs: a dead task's leases in member `member`'s
/// instance go back to the queue.
///
/// # Errors
/// As [`get`].
pub async fn release(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
    tag: &str,
) -> Result<SqlReply> {
    let v = member_vhost(svc, caller, def, member, db, queue).await?;
    v.release_tag(queue, tag).await.map_err(|x| mq_err(&x))?;
    Ok(SqlReply {
        results: vec![SqlResult::default()],
        ..SqlReply::default()
    })
}

/// Member `member`'s instance vhost of definition `def`, for the caller
/// as a fan-in reader (see the module docs for the checks).
async fn member_vhost(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    queue: &str,
) -> Result<sprig_queues::sql::SqlVhost> {
    let reader = caller
        .principal
        .tenant
        .ok_or_else(|| SprigError::internal("a sink owner without a tenant"))?;
    let cat = super::catalog(svc, reader).await?;
    let chain = super::chain(svc, &cat).await?;
    let want = DefKey {
        tenant: TenantId(def.0),
        object: def.1,
    };
    super::fanin_mq::queue_definition(&chain, db, queue)
        .filter(|d| d.key() == want)
        .ok_or_else(|| {
            super::target_err(format!(
                "no FOR EACH TENANT queue {db}.{queue} in reach for FAN IN"
            ))
        })?;
    let p = sprig_proto_amqp::auth::Principal {
        tenant_id: reader.0,
        tenant: caller.principal.tenant_name.clone().unwrap_or_default(),
        user: caller.principal.user.clone(),
        user_id: caller.principal.user_id.map(|u| u64::from(u.0)),
        handle: None,
    };
    if !super::fanin_mq::allowed(svc, &cat, &p, db, queue) {
        return Err(denied("SPRIG_FAN_IN, SELECT and DELETE"));
    }
    let m = TenantId(member);
    let name = member_of(svc, reader, m).await?;
    let e = engine(svc)?;
    e.sql_vhost(Arc::new(internal_principal(m, &name)), db, 0)
        .await
        .map_err(|x| mq_err(&x))
}

/// See the module docs: one result per token, with its error if it
/// couldn't be settled.
///
/// # Errors
/// The reader's catalog's.
pub async fn settle(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    tokens: &[String],
    ack: bool,
) -> Result<SqlReply> {
    let reader = caller
        .principal
        .tenant
        .ok_or_else(|| SprigError::internal("a sink owner without a tenant"))?;
    let e = engine(svc)?;
    let mut results = Vec::with_capacity(tokens.len());
    for t in tokens {
        let r = async {
            let td = e.verify_token(t).map_err(|x| mq_err(&x))?;
            let m = TenantId(td.tenant);
            let name = member_of(svc, reader, m).await?;
            let mcat = super::catalog(svc, m).await?;
            let db = mcat
                .databases
                .get(DbId(td.db))
                .map(|d| d.name.clone())
                .ok_or_else(|| super::target_err("the token's vhost no longer exists"))?;
            let v = e
                .sql_vhost(Arc::new(internal_principal(m, &name)), &db, 0)
                .await
                .map_err(|x| mq_err(&x))?;
            if ack {
                v.ack(&td);
            } else {
                v.nack(&td, false);
            }
            Ok::<_, SprigError>(())
        }
        .await;
        results.push(SqlResult {
            error: r.err().map(|x| PeerError::of(&x)),
            ..SqlResult::default()
        });
    }
    Ok(SqlReply {
        results,
        ..SqlReply::default()
    })
}

/// The stream definition `name` of database `db` a fan-in sink in the
/// chain's member may read: one declared at some level of the chain.
#[must_use]
pub fn stream_definition(chain: &Chain, db: &str, name: &str) -> Option<Arc<Definition>> {
    let k = db_key(db);
    chain
        .levels
        .iter()
        .filter_map(|l| l.state.as_ref())
        .flat_map(|s| s.defs.values())
        .find(|d| {
            d.kind == DefKind::Stream
                && db_key(&d.db_name) == k
                && d.name == name
                && d.state == DefState::Active
        })
        .cloned()
}

/// The partitions of a stream definition's instances: its template's
/// `PARTITIONS n` (1 without).
#[must_use]
pub fn template_partitions(d: &Definition) -> u32 {
    let text = d
        .definition
        .get("create")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    let words: Vec<&str> = text.split_whitespace().collect();
    words
        .iter()
        .position(|w| *w == "PARTITIONS")
        .and_then(|i| words.get(i + 1))
        .and_then(|n| n.trim_matches(|c: char| !c.is_ascii_digit()).parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Bytes read from a partition's log at a time.
const READ_BYTES: usize = 1 << 20;

/// See the module docs (`FanInStreamRead`).
///
/// # Errors
/// 1227 without the privileges; 39896 for a definition or member out of
/// reach; 39243 for a member denying remote access; the log's.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn stream_read(
    svc: &Arc<TenantServices>,
    caller: &Caller,
    def: (u64, u64),
    member: u64,
    db: &str,
    stream: &str,
    from: &[sprig_sinks::peer::StreamFrom],
    from_ms: i64,
    max: u32,
    wait_ms: u64,
) -> Result<SqlReply> {
    let reader = caller
        .principal
        .tenant
        .ok_or_else(|| SprigError::internal("a sink owner without a tenant"))?;
    let cat = super::catalog(svc, reader).await?;
    let chain = super::chain(svc, &cat).await?;
    let want = DefKey {
        tenant: TenantId(def.0),
        object: def.1,
    };
    stream_definition(&chain, db, stream)
        .filter(|d| d.key() == want)
        .ok_or_else(|| {
            super::target_err(format!(
                "no FOR EACH TENANT stream {db}.{stream} in reach for FAN IN"
            ))
        })?;
    let p = sprig_proto_amqp::auth::Principal {
        tenant_id: reader.0,
        tenant: caller.principal.tenant_name.clone().unwrap_or_default(),
        user: caller.principal.user.clone(),
        user_id: caller.principal.user_id.map(|u| u64::from(u.0)),
        handle: None,
    };
    if !super::fanin_mq::allowed_to(svc, &cat, &p, db, stream, false) {
        return Err(denied("SPRIG_FAN_IN and SELECT"));
    }
    let m = TenantId(member);
    member_of(svc, reader, m).await?;
    let eng = crate::kafka::engine::streams().ok_or_else(|| {
        SprigError::internal(
            "streams are stored on the SprigDB engine, which failed to start on this node",
        )
    })?;
    let topic = eng
        .topic(m.0, &format!("{db}.{stream}"))?
        .ok_or_else(|| super::target_err(format!("tenant {m} has no instance of {db}.{stream}")))?;
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(wait_ms.min(30_000));
    let max = usize::try_from(max.clamp(1, 10_000)).unwrap_or(1);
    loop {
        let mut rows = Vec::new();
        for (part, b) in eng.log().partitions(m.0, topic.stream_id) {
            let named = from.iter().find(|f| f.partition == part);
            let (mut at, since) = match named {
                Some(f) => match f.offset {
                    Some(o) => (i64::try_from(o).unwrap_or(i64::MAX).max(b.log_start), 0),
                    None => (b.log_start, f.from_ms),
                },
                None => (b.log_start, from_ms),
            };
            let key = sprig_streams::seg::Key {
                tenant: m.0,
                stream: topic.stream_id,
                partition: part,
            };
            let mut n = 0;
            while at < b.high_watermark && n < max {
                let bytes = match eng.log().read(key, at, READ_BYTES) {
                    Ok((_, bytes)) => bytes,
                    Err(Ok(_)) => break,
                    Err(Err(e)) => return Err(e),
                };
                if bytes.is_empty() {
                    break;
                }
                let mut next = at;
                for r in sprig_streams::seg::kafka::decode_stored(&bytes, at)? {
                    next = next.max(r.offset + 1);
                    if r.offset < at || r.timestamp_ms < since || n >= max {
                        continue;
                    }
                    n += 1;
                    let headers = (!r.headers.is_empty()).then(|| {
                        let m: serde_json::Map<String, serde_json::Value> = r
                            .headers
                            .iter()
                            .map(|h| {
                                (
                                    h.key.clone(),
                                    h.value.as_ref().map_or(serde_json::Value::Null, |v| {
                                        serde_json::Value::String(
                                            String::from_utf8_lossy(v).into_owned(),
                                        )
                                    }),
                                )
                            })
                            .collect();
                        serde_json::Value::Object(m).to_string()
                    });
                    rows.push(vec![
                        SqlResult::cell(Some(part.to_string().as_bytes())),
                        SqlResult::cell(Some(r.offset.to_string().as_bytes())),
                        SqlResult::cell(Some(
                            sprig_sinks::stream_source::utc_text(r.timestamp_ms).as_bytes(),
                        )),
                        SqlResult::cell(r.key.as_deref()),
                        SqlResult::cell(r.value.as_deref()),
                        SqlResult::cell(headers.as_deref().map(str::as_bytes)),
                    ]);
                }
                // Where the batches end (compaction leaves gaps).
                let mut pos = 0;
                while let Some(h) = sprig_streams::seg::batch::Head::parse(&bytes[pos..]) {
                    next = next.max(h.next_offset());
                    pos += h.total;
                    if pos >= bytes.len() {
                        break;
                    }
                }
                if next <= at {
                    break;
                }
                at = next;
            }
        }
        if !rows.is_empty() || std::time::Instant::now() >= deadline {
            metrics::counter!("sprig_sink_fanin_stream_reads_total").increment(1);
            return Ok(SqlReply {
                results: vec![SqlResult {
                    columns: ["partition_no", "offset_no", "ts", "key", "value", "headers"]
                        .iter()
                        .map(|c| (*c).to_owned())
                        .collect(),
                    rows,
                    ..SqlResult::default()
                }],
                ..SqlReply::default()
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
