//! Change exchanges (a table's change feed published to an exchange):
//! the `change_exchange` job, one per table
//! with `queue.change_exchange` set, submitted by the controller's
//! `change_exchange` loop for the gateway's feed subscription `cx:o<object>`.
//!
//! ```text
//! the subscription's owner_ref: {db, table, object, exchange, user}
//! tail the tenant's feed from the subscription's checkpoint, through the
//!   worker's shared reader (one binlog dump per cluster, R9 r9feed3)
//! each round: the first item waited for (up to 1 s), then every item
//!   already read, up to ROUND (R9 r9feed4: batches)
//!   the round's changes of the table (insert, update, delete; DDL and
//!   truncate too): the user who set the exchange (`user`) may see them?
//!     its policies don't bind the user: SELECT on the table decides
//!     they bind: the images checked on a gateway in a session of the user,
//!       the round's in one request (SinkSql Images): an insert goes out
//!       when its new row shows, a delete when its old row does; an
//!       update as an update (both), a delete (only the old row) or an
//!       insert (only the new row)
//!   body: the change envelope (as the change topic's record), its
//!     op and images as the user sees them; routing key <db>.<table>.<op>
//!     (c, u, d, ddl, t); message id: message_id(the change's feed id)
//!   published as the user on a gateway (SinkSql PublishBatch: the queue
//!     engine's path of sprig.mq_publish outside a transaction), so the
//!     user's rights on the exchange apply: up to BATCH messages and
//!     BATCH_BYTES a request, in commit order; a queue's messages of the
//!     request reach its owner in one command (one id lookup, one write);
//!     the gateway stops at the first that fails, and the retry starts
//!     from it
//! each queue takes a message id once (its messages and its forward
//!   ledger, as an outbox forward), so a change published again after a
//!   restart (the checkpoint moves after what was published, at most every
//!   second) or a retried publish that went through is a no-op: exactly
//!   once, in commit order (R9 r9feed4, decision 152)
//! the tail is read by a task of its own (sprig_feed::pump), so the loop's
//!   1 s housekeeping wait never cancels a read half-way
//! the job ends when the subscription is dropped (checked every 5 s)
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::dispatch::JobClass;
use sprig_feed::names::CatalogResolver;
use sprig_feed::pump::Pump;
use sprig_feed::subs::Subscriptions;
use sprig_feed::{
    ChangeEvent, EventId, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions,
    TenantTail,
};
use sprig_meta::MetaStore;
use sprig_sinks::peer::{ImageTable, RunAs, SinkPeer, SqlOp, images_answer};

use crate::logic::LogicEnv;
use crate::runtime::{JobCtx, JobHandler};

/// The job kind (the controller's `change_exchange_loop::JOB_KIND`).
pub const JOB_KIND: &str = "change_exchange";

/// `[before, after]` of feed event `e` visible to user `user` of `tenant`:
/// each image checked on a gateway in a session of the user (`SinkSql`
/// `Images`). An absent image is `false`; for DDL
/// and truncate, `(SELECT on the table, false)`.
///
/// # Errors
/// The gateway's, or the directory's (retry).
pub(crate) async fn visible_to(
    peer: &dyn SinkPeer,
    global: &dyn MetaStore,
    tenant: TenantId,
    user: u32,
    seed: u64,
    e: &ChangeEvent,
) -> Result<(bool, bool)> {
    let run_as = RunAs {
        tenant: tenant.0,
        tenant_name: sprig_sinks::peer::tenant_name(global, tenant.0).await?,
        user_id: user,
        database: String::new(),
    };
    Ok(visible_many(peer, &run_as, seed, &[e])
        .await?
        .first()
        .copied()
        .unwrap_or((false, false)))
}

/// The images of `e` to check, and which of them is the before and the
/// after image (DDL and truncate: one `null`, the privilege alone, as
/// the before image).
type Images = (
    Vec<Option<serde_json::Map<String, Value>>>,
    Option<usize>,
    Option<usize>,
);

fn images_of(e: &ChangeEvent) -> Images {
    let img = |i: Option<&sprig_feed::Image>| match i.map(sprig_streams::envelope::image_json) {
        Some(Value::Object(m)) => Some(m),
        _ => None,
    };
    let mut images = Vec::new();
    if matches!(e.op, Op::Ddl(_) | Op::Truncate) {
        images.push(None);
        return (images, Some(0), None);
    }
    let mut push = |m: Option<serde_json::Map<String, Value>>| {
        m.map(|m| {
            images.push(Some(m));
            images.len() - 1
        })
    };
    let b = push(img(e.before.as_ref()));
    let a = push(img(e.after.as_ref()));
    (images, b, a)
}

/// [`visible_to`] for each of `events`, in one `SinkSql` `Images`
/// request as `run_as` (its database unused).
///
/// # Errors
/// The gateway's (retry).
pub(crate) async fn visible_many(
    peer: &dyn SinkPeer,
    run_as: &RunAs,
    seed: u64,
    events: &[&ChangeEvent],
) -> Result<Vec<(bool, bool)>> {
    let mut tables = Vec::with_capacity(events.len());
    let mut slots = Vec::with_capacity(events.len());
    for e in events {
        let (images, b, a) = images_of(e);
        tables.push(ImageTable {
            db: e.db_name.clone(),
            table: e.table.clone(),
            images,
        });
        slots.push((b, a));
    }
    let reply = peer
        .sql(
            seed,
            &run_as.request(SqlOp::Images {
                tables: tables.clone(),
            }),
        )
        .await?
        .ok()?;
    let answer = images_answer(&tables, &reply);
    let v = |t: usize, i: Option<usize>| {
        i.is_some_and(|i| {
            answer
                .get(t)
                .and_then(|x| x.get(i))
                .copied()
                .unwrap_or(false)
        })
    };
    Ok(slots
        .iter()
        .enumerate()
        .map(|(t, (b, a))| (v(t, *b), v(t, *a)))
        .collect())
}

/// The message id a change exchange publishes change `id` of table
/// `object` to `exchange` as: the same whenever the change is read again
/// (its cluster, transaction and row, the feed's idempotency key), so a
/// queue takes it once. 128-bit FNV-1a (stable across builds), marked as
/// a version-8 UUID (RFC 9562's custom version), so it never equals an
/// outbox's ids (version 7).
#[must_use]
pub fn message_id(tenant: TenantId, object: u64, exchange: &str, id: &EventId) -> [u8; 16] {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut h = OFFSET;
    let mut eat = |b: &[u8]| {
        for x in b {
            h ^= u128::from(*x);
            h = h.wrapping_mul(PRIME);
        }
    };
    eat(b"sprig change exchange 1");
    eat(&tenant.0.to_le_bytes());
    eat(&object.to_le_bytes());
    for s in [exchange, id.cluster.as_str()] {
        eat(&(s.len() as u64).to_le_bytes());
        eat(s.as_bytes());
    }
    eat(&id.gtid.domain.to_le_bytes());
    eat(&id.gtid.server.to_le_bytes());
    eat(&id.gtid.seq.to_le_bytes());
    eat(&id.row.to_le_bytes());
    let mut b = h.to_be_bytes();
    b[6] = (b[6] & 0x0f) | 0x80;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

/// The op letter change event `e` goes out as for a user who sees
/// `[before, after]` as given; `None`: not at all.
#[must_use]
pub fn shown_op(e: &Op, before: bool, after: bool) -> Option<&'static str> {
    Some(match (e, before, after) {
        (Op::Insert, _, true) | (Op::Update, false, true) => "c",
        (Op::Delete, true, _) | (Op::Update, true, false) => "d",
        (Op::Update, true, true) => "u",
        (Op::Ddl(_), true, _) => "ddl",
        (Op::Truncate, true, _) => "t",
        _ => return None,
    })
}

/// The envelope `body` (the change topic's record) as op `op`: an insert
/// without its before image, a delete without its after image.
#[must_use]
pub fn reshaped(mut body: Value, op: &str) -> Value {
    if let Some(m) = body.as_object_mut() {
        m.insert("op".into(), Value::String(op.to_owned()));
        match op {
            "c" => {
                m.insert("before".into(), Value::Null);
            }
            "d" => {
                m.insert("after".into(), Value::Null);
            }
            _ => {}
        }
    }
    body
}

/// The subscription's `owner_ref`.
#[derive(Debug, Clone)]
struct Spec {
    db: String,
    table: String,
    object: u64,
    exchange: String,
    user: u32,
}

impl Spec {
    fn of(owner_ref: Option<&str>) -> std::result::Result<Self, String> {
        let v: Value = serde_json::from_str(owner_ref.unwrap_or(""))
            .map_err(|e| format!("the subscription's owner_ref: {e}"))?;
        let s = |k: &str| {
            v[k].as_str()
                .map(str::to_owned)
                .ok_or(format!("owner_ref.{k} is missing"))
        };
        Ok(Self {
            db: s("db")?,
            table: s("table")?,
            object: v["object"].as_u64().ok_or("owner_ref.object is missing")?,
            exchange: s("exchange")?,
            user: v["user"]
                .as_u64()
                .and_then(|u| u32::try_from(u).ok())
                .ok_or("owner_ref.user is missing")?,
        })
    }
}

/// `change_exchange` (see the module docs).
#[derive(Debug, Clone)]
pub struct ChangeExchange(pub LogicEnv);

#[async_trait::async_trait]
impl JobHandler for ChangeExchange {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(cx.spec["tenant"].as_u64().ok_or("spec.tenant is missing")?);
        let name = cx.spec["name"]
            .as_str()
            .ok_or("spec.name is missing")?
            .to_owned();
        Box::pin(serve(&self.0, cx, tenant, &name))
            .await
            .map_err(|e| e.to_string())
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// What the forwarder's loop reads next.
#[derive(Debug)]
enum Next {
    /// A feed item.
    Item(FeedItem),
    /// Nothing within the wait: the loop's housekeeping runs.
    Quiet,
    /// The job was cancelled.
    Stop,
}

/// The tail as the loop reads it: in a task of its own.
/// `TenantTail::next` isn't cancel-safe, and the loop's 1 s wait would
/// otherwise drop a transaction whose catalog lookup outlasts it (a new
/// table's first row while a move holds the catalog, say).
type Feed = Pump<FeedItem>;

fn feed_of(tail: TenantTail) -> Feed {
    Pump::start(tail, AHEAD, "change exchange forwarder")
}

/// Feed items read ahead: a round's worth. Larger than the pump's
/// default, so a round takes many of the table's changes when the
/// forwarder is behind, and the shared reader's 256-item buffer for it
/// keeps draining while a round publishes (a full one detaches it).
const AHEAD: usize = ROUND;

/// Feed items in one round at most: the first one waited for, the rest
/// already read (R9 r9feed4).
const ROUND: usize = 4096;

/// Publishes in one `SinkSql` request at most.
const BATCH: usize = 256;

/// Statement bytes in one `SinkSql` request at most (a larger change goes
/// alone). The peer service takes requests up to 4 MiB, and a statement
/// grows when its JSON is escaped again.
const BATCH_BYTES: usize = 768 << 10;

/// The first round of `feed`: the next item (after up to `wait`), then
/// every item already read, up to [`ROUND`]. A read error after some items
/// is returned with them, to be raised once they are handled.
async fn round(
    feed: &mut Feed,
    cancel: &sprig_common::CancellationToken,
    wait: Duration,
) -> Result<(Next, Vec<FeedItem>, Option<SprigError>)> {
    let first = match read(feed, cancel, wait).await? {
        Next::Item(i) => i,
        other => return Ok((other, Vec::new(), None)),
    };
    let mut items = vec![first];
    while items.len() < ROUND {
        match feed.try_next() {
            Some(Ok(i)) => items.push(i),
            Some(Err(e)) => return Ok((Next::Quiet, items, Some(e))),
            None => break,
        }
    }
    Ok((Next::Quiet, items, None))
}

/// The wait before retry `attempt` (from 1) of a publish or an image
/// check: 100 ms more each time, at most 1 s. Retries are safe (each
/// change's message id is taken once), and a forwarder that waits long
/// after its gateway came back falls behind the shared reader far enough
/// to need a catch-up dump (R9 r9feed4).
fn retry_wait(attempt: u32) -> Duration {
    Duration::from_millis(100 * u64::from(attempt.clamp(1, 10)))
}

/// One publish: the routing key, the body and the message id in hex.
type Publish = sprig_sinks::peer::PublishMessage;

/// The properties of every change exchange message.
const PROPERTIES: &str = r#"{"content_type":"application/json","delivery_mode":2}"#;

/// The end of the request that starts at `from`: at most [`BATCH`]
/// messages and [`BATCH_BYTES`] of them, at least one.
fn batch_end(pubs: &[Publish], from: usize) -> usize {
    let mut bytes = 0usize;
    let mut end = from;
    while end < pubs.len() && end - from < BATCH {
        bytes += pubs[end].body.len() + pubs[end].routing_key.len();
        if end > from && bytes > BATCH_BYTES {
            break;
        }
        end += 1;
    }
    end
}

/// Publishes `pubs` to `exchange` in `run_as`'s database (the vhost), in
/// order, in requests of [`batch_end`], each retried with backoff from its
/// first message that failed. `false`: cancelled before all were
/// published.
async fn publish_all(
    peer: &dyn SinkPeer,
    run_as: &RunAs,
    seed: u64,
    exchange: &str,
    pubs: &[Publish],
    cancel: &sprig_common::CancellationToken,
    requests: &mut u64,
) -> bool {
    let mut done = 0usize;
    let mut attempt = 0u32;
    // Since when publishing fails (logged once it works again).
    let mut failing: Option<Instant> = None;
    while done < pubs.len() {
        let end = batch_end(pubs, done);
        let part = &pubs[done..end];
        *requests += 1;
        let r = peer
            .sql(
                seed,
                &run_as.request(SqlOp::PublishBatch {
                    vhost: run_as.database.clone(),
                    exchange: exchange.to_owned(),
                    messages: part.to_vec(),
                }),
            )
            .await
            .and_then(sprig_sinks::peer::SqlReply::ok);
        // The calls that went through, and the first error.
        let (ok, err) = match r {
            Ok(reply) => {
                let ok = reply
                    .results
                    .iter()
                    .take_while(|x| x.error.is_none())
                    .count()
                    .min(part.len());
                let err = reply
                    .results
                    .get(ok)
                    .and_then(|x| x.error.as_ref())
                    .map_or_else(
                        || {
                            SprigError::internal(format!(
                                "the gateway answered {} of {} publishes",
                                reply.results.len(),
                                part.len()
                            ))
                        },
                        sprig_sinks::peer::PeerError::error,
                    );
                (ok, (ok < part.len()).then_some(err))
            }
            Err(e) => (0, Some(e)),
        };
        done += ok;
        let Some(err) = err else {
            if let Some(at) = failing.take() {
                tracing::info!(
                    tenant = run_as.tenant,
                    attempts = attempt + 1,
                    after_ms = at.elapsed().as_millis(),
                    "change exchange: publishing again"
                );
            }
            attempt = 0;
            continue;
        };
        failing.get_or_insert_with(Instant::now);
        attempt = if ok > 0 { 1 } else { attempt + 1 };
        if attempt == 1 || attempt.is_multiple_of(50) {
            tracing::warn!(tenant = run_as.tenant, published = done, of = pubs.len(), error = %err, "change exchange: publish failed; retrying");
        }
        let wait = retry_wait(attempt);
        tokio::select! {
            () = cancel.cancelled() => return false,
            () = tokio::time::sleep(wait) => {}
        }
    }
    true
}

/// What the forwarder needs to decide what a user sees.
struct Viewer<'a> {
    peer: &'a dyn SinkPeer,
    rls: &'a crate::sinks::RlsCatalogs,
    tenant: TenantId,
    spec: &'a Spec,
    /// The user, with no database (image checks).
    run_as: RunAs,
    /// `SELECT` on the table, checked at most every 2 s while the
    /// table's policies don't bind the user.
    select_ok: Option<(Instant, bool)>,
}

impl Viewer<'_> {
    /// The op each of `events` goes out as for the user ([`shown_op`]),
    /// `None` for hidden: the image checks in one request per
    /// [`BATCH`] changes.
    async fn ops(&mut self, events: &[&ChangeEvent]) -> Result<Vec<Option<&'static str>>> {
        let bound = self
            .rls
            .binds(self.tenant, self.spec.user, &[self.spec.object])
            .await?;
        let mut out = vec![None; events.len()];
        let mut check = Vec::new();
        for (i, e) in events.iter().enumerate() {
            if bound || matches!(e.op, Op::Ddl(_) | Op::Truncate) {
                check.push(i);
                continue;
            }
            // Unbound: SELECT on the table decides (cached 2 s).
            let p = match self.select_ok {
                Some((at, p)) if at.elapsed() < Duration::from_secs(2) => p,
                _ => {
                    let probe = ChangeEvent {
                        op: Op::Truncate,
                        ..(*e).clone()
                    };
                    let (p, _) = visible_many(self.peer, &self.run_as, self.spec.object, &[&probe])
                        .await?
                        .first()
                        .copied()
                        .unwrap_or((false, false));
                    self.select_ok = Some((Instant::now(), p));
                    p
                }
            };
            out[i] = shown_op(&e.op, p && e.before.is_some(), p && e.after.is_some());
        }
        for part in check.chunks(BATCH) {
            let evs: Vec<&ChangeEvent> = part.iter().map(|i| events[*i]).collect();
            let seen = visible_many(self.peer, &self.run_as, self.spec.object, &evs).await?;
            for (k, i) in part.iter().enumerate() {
                let (b, a) = seen.get(k).copied().unwrap_or((false, false));
                out[*i] = shown_op(&events[*i].op, b, a);
            }
        }
        Ok(out)
    }
}

/// The next feed item, or [`Next::Quiet`] after `wait` without one.
/// Cancel-safe: an item is taken only when it is returned.
async fn read(
    feed: &mut Feed,
    cancel: &sprig_common::CancellationToken,
    wait: Duration,
) -> Result<Next> {
    tokio::select! {
        // Cancel first: unbiased, a ready item could win after a cancel.
        biased;
        () = cancel.cancelled() => Ok(Next::Stop),
        r = tokio::time::timeout(wait, feed.next()) => match r {
            Ok(i) => Ok(Next::Item(i?)),
            Err(_) => Ok(Next::Quiet),
        },
    }
}

#[allow(clippy::too_many_lines)] // the loop of the module docs
async fn serve(env: &LogicEnv, cx: &JobCtx, tenant: TenantId, name: &str) -> Result<Value> {
    let subs = Subscriptions::new(Arc::clone(&env.dc));
    let sub = match subs.get(tenant, name).await {
        Ok(s) => s,
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionNotFound) => {
            return Ok(json!({"stopped": "the subscription is gone"}));
        }
        Err(e) => return Err(e),
    };
    let spec = Spec::of(sub.owner_ref.as_deref()).map_err(SprigError::internal)?;
    let (peer, global) = env
        .gateways
        .clone()
        .ok_or_else(|| SprigError::internal("this worker reaches no gateway"))?;
    let rls = crate::sinks::RlsCatalogs::new(Arc::clone(&env.locator))
        .ok_or_else(|| SprigError::internal("the worker can't load row-level security catalogs"))?;
    let tenant_name = sprig_sinks::peer::tenant_name(global.as_ref(), tenant.0).await?;
    let start = match subs.checkpoint(tenant, name).await? {
        Some(cp) => StartAt::After(cp.resume_token()?),
        None => StartAt::Now,
    };
    let tail = TenantTail::start(
        tenant,
        start,
        // R9 (r9feed3): through the worker's shared reader.
        Arc::clone(&env.feed),
        Arc::new(MetaPlacement::new(Arc::clone(&env.dc))),
        Arc::new(CatalogResolver::new(Arc::new(crate::logic::Catalogs(
            Arc::clone(&env.locator),
        )))),
        TailOptions::default(),
    )
    .await?;
    let mut feed = feed_of(tail);
    tracing::info!(%tenant, table = %spec.table, exchange = %spec.exchange, job = %cx.job_id, "change exchange forwarder started");
    let run_as = RunAs {
        tenant: tenant.0,
        tenant_name: tenant_name.clone(),
        user_id: spec.user,
        database: spec.db.clone(),
    };
    let mut viewer = Viewer {
        peer: peer.as_ref(),
        rls: &rls,
        tenant,
        spec: &spec,
        run_as: RunAs {
            database: String::new(),
            ..run_as.clone()
        },
        select_ok: None,
    };
    let ctx = sprig_streams::envelope::ChangeCtx {
        partitions: 1,
        tenant_name: Some(tenant_name),
    };
    let (mut published, mut filtered, mut pending, mut requests) = (0u64, 0u64, 0u64, 0u64);
    let mut last: Option<ResumeToken> = None;
    let mut acked = Instant::now();
    let mut checked = Instant::now();
    loop {
        if cx.cancel.is_cancelled() {
            break;
        }
        if checked.elapsed() > Duration::from_secs(5) {
            checked = Instant::now();
            if let Err(e) = subs.get(tenant, name).await
                && e.code() == Some(SprigCode::FeedSubscriptionNotFound)
            {
                return Ok(json!({"stopped": "the exchange was reset", "published": published}));
            }
        }
        if acked.elapsed() > Duration::from_secs(1)
            && let Some(t) = last.take()
        {
            subs.ack(tenant, name, &t, pending, now_ms()).await?;
            pending = 0;
            acked = Instant::now();
            let _ = cx
                .progress(
                    json!({"published": published, "filtered": filtered, "requests": requests}),
                )
                .await;
        }
        let (next, items, ended) = round(&mut feed, &cx.cancel, Duration::from_secs(1)).await?;
        let began = Instant::now();
        if matches!(next, Next::Stop) {
            break;
        }
        let Some(end) = items.last().map(|i| i.token().clone()) else {
            continue;
        };
        let events: Vec<&ChangeEvent> = items
            .iter()
            .filter_map(|i| match i {
                FeedItem::Event(e) if e.object == spec.object => Some(&**e),
                _ => None,
            })
            .collect();
        let events_n = events.len();
        if !events.is_empty() {
            // The user's view, retried until it can be read.
            let mut attempt = 0u32;
            let ops = loop {
                match viewer.ops(&events).await {
                    Ok(ops) => break ops,
                    Err(err) => {
                        attempt += 1;
                        tracing::warn!(%tenant, error = %err, "change exchange: the changes can't be checked yet; retrying");
                        let wait = retry_wait(attempt);
                        tokio::select! {
                            () = cx.cancel.cancelled() => return Ok(json!({"stopped": "cancelled"})),
                            () = tokio::time::sleep(wait) => {}
                        }
                    }
                }
            };
            let mut pubs = Vec::new();
            for (e, op) in events.iter().zip(ops) {
                let Some(op) = op else {
                    filtered += 1;
                    metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "amqp")
                        .increment(1);
                    continue;
                };
                let Some(rec) = sprig_streams::envelope::record_of(e, &ctx) else {
                    continue;
                };
                let body: Value = rec
                    .value
                    .as_deref()
                    .and_then(|v| serde_json::from_slice(v).ok())
                    .unwrap_or(Value::Null);
                let body = reshaped(body, op).to_string();
                pubs.push(Publish {
                    routing_key: format!("{}.{}.{op}", spec.db, spec.table),
                    body,
                    properties: Some(PROPERTIES.to_owned()),
                    id: hex(&message_id(tenant, spec.object, &spec.exchange, &e.id)),
                });
            }
            if !publish_all(
                peer.as_ref(),
                &run_as,
                spec.object,
                &spec.exchange,
                &pubs,
                &cx.cancel,
                &mut requests,
            )
            .await
            {
                return Ok(json!({"stopped": "cancelled"}));
            }
            let n = pubs.len() as u64;
            published += n;
            pending += n;
            metrics::counter!("sprig_queue_change_exchange_published_total").increment(n);
        }
        if began.elapsed() > Duration::from_millis(500) {
            tracing::debug!(%tenant, items = items.len(), events = events_n, took_ms = began.elapsed().as_millis(), "change exchange: a slow round");
        }
        last = Some(end);
        if let Some(e) = ended {
            if let Some(t) = last {
                let _ = subs.ack(tenant, name, &t, pending, now_ms()).await;
            }
            return Err(e);
        }
    }
    if let Some(t) = last {
        let _ = subs.ack(tenant, name, &t, pending, now_ms()).await;
    }
    Ok(json!({"stopped": "cancelled", "published": published}))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: the 1 s housekeeping wait fires while the tail
    /// waits on a new table's catalog lookup (1.5 s here). Every change
    /// must still come once, in order, and a restart from the last token
    /// taken must repeat none.
    #[tokio::test(start_paused = true)]
    async fn the_housekeeping_wait_never_drops_a_change() {
        use sprig_feed::testing::{SlowFeed, keys_once};
        let feed = SlowFeed::new(12, Duration::from_millis(1500));
        let cancel = sprig_common::CancellationToken::new();
        let mut items = Vec::new();
        let mut last = None;
        let mut f = feed_of(feed.tail().await);
        // The first half, then a restart from the last token handled.
        let end = tokio::time::Instant::now() + Duration::from_secs(60);
        while tokio::time::Instant::now() < end && keys_once(&items).len() < 6 {
            if let Next::Item(i) = read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap() {
                if !matches!(i, FeedItem::Event(_)) {
                    last = Some(i.token().clone());
                }
                items.push(i);
            }
        }
        drop(f);
        // Events after the last boundary token weren't checkpointed.
        let token = last.unwrap();
        let mut kept: Vec<FeedItem> = Vec::new();
        for i in items {
            let boundary = !matches!(i, FeedItem::Event(_)) && i.token() == &token;
            kept.push(i);
            if boundary {
                break;
            }
        }
        let mut f = feed_of(feed.tail_from(StartAt::After(token)).await);
        let end = tokio::time::Instant::now() + Duration::from_secs(120);
        while tokio::time::Instant::now() < end && keys_once(&kept).len() < 12 {
            match read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap() {
                Next::Item(i) => kept.push(i),
                Next::Quiet => {}
                Next::Stop => unreachable!(),
            }
        }
        assert_eq!(keys_once(&kept), (1..=12).collect::<Vec<_>>());
        cancel.cancel();
        assert!(matches!(
            read(&mut f, &cancel, Duration::from_secs(1)).await.unwrap(),
            Next::Stop
        ));
    }

    fn eid(cluster: &str, seq: u64, row: u32) -> EventId {
        EventId {
            cluster: sprig_common::ids::ClusterId::new(cluster).unwrap(),
            gtid: sprig_binlog::Gtid {
                domain: 1,
                server: 1,
                seq,
            },
            row,
        }
    }

    /// The same change always gets the same id; any part of it, the
    /// table or the exchange changes the id; ids are version-8 UUIDs,
    /// never an outbox's (version 7).
    #[test]
    fn message_ids_are_fixed_per_change() {
        let t = TenantId(46_306_000_001);
        let a = message_id(t, 9, "ord.x", &eid("dc-a-c1", 7, 0));
        assert_eq!(a, message_id(t, 9, "ord.x", &eid("dc-a-c1", 7, 0)));
        let others = [
            message_id(t, 9, "ord.x", &eid("dc-a-c1", 7, 1)),
            message_id(t, 9, "ord.x", &eid("dc-a-c1", 8, 0)),
            message_id(t, 9, "ord.x", &eid("dc-a-c2", 7, 0)),
            message_id(t, 10, "ord.x", &eid("dc-a-c1", 7, 0)),
            message_id(t, 9, "ord.y", &eid("dc-a-c1", 7, 0)),
            message_id(TenantId(1), 9, "ord.x", &eid("dc-a-c1", 7, 0)),
        ];
        for (i, o) in others.iter().enumerate() {
            assert_ne!(&a, o, "{i}");
        }
        for id in std::iter::once(a).chain(others) {
            assert_eq!(id[6] >> 4, 8, "version 8");
            assert_eq!(id[8] >> 6, 2, "RFC 9562 variant");
            assert_eq!(
                sprig_sinks::peer::publish_id(&hex(&id)),
                Some(id),
                "hex round trip"
            );
        }
        // Pinned: the id must not change between builds (a replay after
        // an upgrade would publish again).
        assert_eq!(hex(&a), "55b020fa9f9082309847a2007930b83f");
    }

    fn pubs(n: usize, size: usize) -> Vec<Publish> {
        (0..n)
            .map(|i| Publish {
                routing_key: "db.t.c".into(),
                body: format!("{i}{}", "a".repeat(size)),
                properties: None,
                id: format!("{i:032x}"),
            })
            .collect()
    }

    #[test]
    fn batches_are_bounded_by_count_and_bytes() {
        let p = pubs(600, 0);
        assert_eq!(batch_end(&p, 0), BATCH);
        assert_eq!(batch_end(&p, 512), 600);
        assert_eq!(batch_end(&p, 600), 600);
        // 300 KiB each: two fit in BATCH_BYTES (768 KiB), not three.
        let p = pubs(5, 300 << 10);
        assert_eq!(batch_end(&p, 0), 2);
        // One larger than the budget goes alone.
        let p = pubs(3, 1 << 20);
        assert_eq!(batch_end(&p, 0), 1);
        assert_eq!(batch_end(&p, 1), 2);
    }

    /// A gateway that fails message `fail_at` of the stream once (and
    /// stops there, as the real one does), and a request once before it.
    #[derive(Default)]
    struct Gateway {
        seen: std::sync::Mutex<Vec<(String, String)>>,
        requests: std::sync::Mutex<Vec<usize>>,
        calls: std::sync::atomic::AtomicUsize,
        fail_at: usize,
        failed_call: std::sync::atomic::AtomicBool,
        failed_request: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl SinkPeer for Gateway {
        async fn sql(
            &self,
            _seed: u64,
            req: &sprig_sinks::peer::SqlRequest,
        ) -> Result<sprig_sinks::peer::SqlReply> {
            use std::sync::atomic::Ordering;
            let SqlOp::PublishBatch {
                vhost,
                exchange,
                messages,
            } = &req.op
            else {
                panic!("{req:?}");
            };
            assert_eq!((vhost.as_str(), exchange.as_str()), ("db", "x"));
            self.requests.lock().unwrap().push(messages.len());
            if !self.failed_request.swap(true, Ordering::SeqCst) && messages.len() > 1 {
                return Err(SprigError::new(SprigCode::BinlogDisconnected, "no gateway"));
            }
            let mut results = Vec::new();
            for m in messages {
                let n = self.calls.load(Ordering::SeqCst);
                if n == self.fail_at && !self.failed_call.swap(true, Ordering::SeqCst) {
                    results.push(sprig_sinks::peer::SqlResult {
                        error: Some(sprig_sinks::peer::PeerError::of(&SprigError::internal(
                            "owner moved",
                        ))),
                        ..Default::default()
                    });
                    break;
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.seen
                    .lock()
                    .unwrap()
                    .push((m.body.clone(), m.id.clone()));
                results.push(sprig_sinks::peer::SqlResult::default());
            }
            Ok(sprig_sinks::peer::SqlReply {
                results,
                ..Default::default()
            })
        }
    }

    /// 600 publishes go out in commit order, each once, in requests of at
    /// most BATCH, through a failed request and a call that fails halfway
    /// through a request (the retry starts from it).
    #[tokio::test(start_paused = true)]
    async fn publishes_go_once_in_order_through_failures() {
        let gw = Gateway {
            fail_at: 300,
            ..Gateway::default()
        };
        let p = pubs(600, 0);
        let run_as = RunAs {
            tenant: 1,
            tenant_name: "t".into(),
            user_id: 1,
            database: "db".into(),
        };
        let cancel = sprig_common::CancellationToken::new();
        let mut requests = 0;
        assert!(publish_all(&gw, &run_as, 9, "x", &p, &cancel, &mut requests).await);
        let seen = gw.seen.lock().unwrap().clone();
        let want: Vec<(String, String)> =
            p.iter().map(|x| (x.body.clone(), x.id.clone())).collect();
        assert_eq!(seen, want);
        let sizes = gw.requests.lock().unwrap().clone();
        // Failed request, 256, 256 (fails at 300: 44 through), 300..556, 556..600.
        assert_eq!(sizes, vec![256, 256, 256, 256, 44]);
        assert_eq!(requests, 5);
    }

    #[test]
    fn ops_and_bodies() {
        assert_eq!(shown_op(&Op::Update, false, true), Some("c"));
        assert_eq!(shown_op(&Op::Update, true, false), Some("d"));
        assert_eq!(shown_op(&Op::Update, true, true), Some("u"));
        assert_eq!(shown_op(&Op::Update, false, false), None);
        assert_eq!(shown_op(&Op::Insert, false, false), None);
        let b = reshaped(
            json!({"op": "u", "before": {"a": 1}, "after": {"a": 2}}),
            "c",
        );
        assert_eq!(b, json!({"op": "c", "before": null, "after": {"a": 2}}));
    }
}
