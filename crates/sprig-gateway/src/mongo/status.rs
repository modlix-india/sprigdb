//! Smaller MongoDB 8.1–9.0 reporting items:
//! `connectionStatus.authInfo.UUID` (8.1), `serverStatus` with `none: 1`
//! (8.3) and its change-stream metrics (9.0), and change-stream cursors
//! with their position for `currentOp`/`$currentOp` (9.0's
//! `cursor.changeStreams.optime`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use bson::{Binary, Bson, DateTime, Document, Timestamp, doc};

use super::conn::Conn;

/// One tenant's change-stream counters on this gateway (MongoDB 9.0's
/// `metrics.changeStreams`). Each tenant's `serverStatus` shows its own;
/// a login without a tenant sees the whole gateway's ([`ALL`]).
#[derive(Default)]
struct Counters {
    /// Change-stream cursors opened.
    opened: AtomicU64,
    /// Change-stream cursors closed.
    closed: AtomicU64,
    /// Opened per scope: cluster, database, collection.
    scope: [AtomicU64; 3],
    /// The exponential moving average of closed streams' lifespans, in µs
    /// (MongoDB 9.0's `cursor.lifespan.average`; `None` before the first).
    lifespan_average: Mutex<Option<f64>>,
    /// The option and error counters, by path below
    /// `metrics.changeStreams`.
    paths: Mutex<HashMap<String, u64>>,
    /// Change-stream batches returned (`cursor.batchesReturned`), events
    /// and their BSON bytes returned (`docsReturned`, `bytesReturned`), and
    /// events and bytes the streams read before their pipeline's stages
    /// (`docsExamined`, `bytesRead`). Events a row policy hides never reach
    /// a stream, so they aren't counted.
    batches: AtomicU64,
    docs_returned: AtomicU64,
    bytes_returned: AtomicU64,
    docs_examined: AtomicU64,
    bytes_read: AtomicU64,
    /// Events over the BSON limit that failed their stream
    /// (`largeEventsFailed`); `$changeStreamSplitLargeEvent` isn't offered,
    /// so `largeEventsSplit` stays 0.
    large_failed: AtomicU64,
}

/// The key of the whole gateway's counters (every tenant's events are
/// counted there too).
const ALL: u64 = u64::MAX;

/// The counters of `tenant` (0: a login without a tenant).
fn counters_of(tenant: u64) -> Arc<Counters> {
    static M: std::sync::OnceLock<Mutex<HashMap<u64, Arc<Counters>>>> = std::sync::OnceLock::new();
    let mut m = M
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(m.entry(tenant).or_default())
}

/// Runs `f` on `tenant`'s counters and on the whole gateway's.
fn count(tenant: u64, f: impl Fn(&Counters)) {
    f(&counters_of(tenant));
    if tenant != ALL {
        f(&counters_of(ALL));
    }
}

/// The weight of the newest lifespan in `lifespan_average`.
const LIFESPAN_ALPHA: f64 = 0.2;

fn bump(tenant: u64, path: &str) {
    count(tenant, |c| {
        *c.paths
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(path.to_owned())
            .or_default() += 1;
    });
}

/// `option.cursor.batchSize`'s bucket bounds.
const BATCH_BOUNDS: [i64; 5] = [1, 10, 100, 1000, 10000];
/// `option.cursor.maxTimeMS`'s bucket bounds.
const MAX_TIME_BOUNDS: [i64; 13] = [
    100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100_000, 200_000, 500_000, 1_000_000,
];

/// The bucket labels of a histogram with `bounds`: `(-inf, b0)`,
/// `[b0, b1)`, ..., `[bn, inf)`.
fn buckets(bounds: &[i64]) -> Vec<String> {
    let mut out = vec![format!("(-inf, {})", bounds[0])];
    for w in bounds.windows(2) {
        out.push(format!("[{}, {})", w[0], w[1]));
    }
    out.push(format!("[{}, inf)", bounds[bounds.len() - 1]));
    out
}

fn observe(tenant: u64, histogram: &str, bounds: &[i64], v: i64) {
    let i = bounds.iter().take_while(|b| v >= **b).count();
    bump(
        tenant,
        &format!("option.cursor.{histogram}.{}", buckets(bounds)[i]),
    );
}

fn num_of(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Int32(n) => Some(i64::from(*n)),
        Bson::Int64(n) => Some(*n),
        #[allow(clippy::cast_possible_truncation)]
        Bson::Double(f) => Some(*f as i64),
        _ => None,
    }
}

/// A change stream opened with `$changeStream` options `stage` by the
/// `aggregate` command `body`: its option counters and the cursor
/// option histograms (an `aggregate` without `batchSize` counts the
/// default first batch, 101, as MongoDB backfills one), counted for
/// `tenant`.
pub(crate) fn stream_options(tenant: u64, stage: &Document, body: &Document) {
    for (k, v) in stage {
        match (k.as_str(), v) {
            ("fullDocument", Bson::String(m))
                if matches!(m.as_str(), "required" | "updateLookup" | "whenAvailable") =>
            {
                bump(tenant, &format!("option.fullDocument.{m}"));
            }
            ("fullDocumentBeforeChange", Bson::String(m))
                if matches!(m.as_str(), "required" | "whenAvailable") =>
            {
                bump(tenant, &format!("option.fullDocumentBeforeChange.{m}"));
            }
            ("resumeAfter" | "startAfter" | "startAtOperationTime", _) => {
                bump(tenant, &format!("option.{k}"));
            }
            (
                "showExpandedEvents"
                | "showMigrationEvents"
                | "showSystemEvents"
                | "showRawUpdateDescription"
                | "ignoreRemovedShards"
                | "matchCollectionUUIDForUpdateLookup",
                Bson::Boolean(true),
            ) => bump(tenant, &format!("option.{k}")),
            _ => {}
        }
    }
    let batch = body
        .get_document("cursor")
        .ok()
        .and_then(|c| num_of(c.get("batchSize")))
        .unwrap_or(101);
    observe(tenant, "batchSize", &BATCH_BOUNDS, batch);
    if let Some(ms) = num_of(body.get("maxTimeMS")) {
        observe(tenant, "maxTimeMS", &MAX_TIME_BOUNDS, ms);
    }
}

/// A `getMore` on a change stream of `tenant`: the cursor option
/// histograms, for the options it sets.
pub(crate) fn stream_get_more(tenant: u64, body: &Document) {
    if let Some(n) = num_of(body.get("batchSize")) {
        observe(tenant, "batchSize", &BATCH_BOUNDS, n);
    }
    if let Some(ms) = num_of(body.get("maxTimeMS")) {
        observe(tenant, "maxTimeMS", &MAX_TIME_BOUNDS, ms);
    }
}

/// An error a change stream's cursor met (opening, resuming or reading),
/// counted for `tenant` as MongoDB 9.0 sorts them.
pub(crate) fn stream_error(tenant: u64, e: &sprig_proto_mongo::errors::MongoError) {
    // MongoDB's resumable change-stream errors (network, step-down and
    // shutdown codes), besides the label.
    const RETRIABLE: [i32; 16] = [
        6, 7, 63, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 13388, 13435, 13436,
    ];
    let path = match e.code {
        10334 => "error.nonRetriable.bsonObjectTooLarge",
        280 => "error.nonRetriable.changeStreamFatalError",
        286 => "error.nonRetriable.changeStreamHistoryLost",
        11602 => "error.retriable.interruptedDueToReplStateChange",
        c if RETRIABLE.contains(&c) || e.labels.contains(&"ResumableChangeStreamError") => {
            "error.retriable.other"
        }
        _ => "error.nonRetriable.other",
    };
    bump(tenant, path);
}

/// `c`'s option and error counters as `serverStatus` shows them.
fn option_and_error_metrics(c: &Counters) -> (Document, Document) {
    let m = c
        .paths
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let get = |p: &str| n(m.get(p).copied().unwrap_or(0));
    let histogram = |name: &str, bounds: &[i64]| {
        let mut d = Document::new();
        let mut total = 0;
        for b in buckets(bounds) {
            let c = m
                .get(&format!("option.cursor.{name}.{b}"))
                .copied()
                .unwrap_or(0);
            total += c;
            d.insert(b, doc! {"count": n(c)});
        }
        d.insert("totalCount", n(total));
        d
    };
    let option = doc! {
        "fullDocument": {
            "required": get("option.fullDocument.required"),
            "updateLookup": get("option.fullDocument.updateLookup"),
            "whenAvailable": get("option.fullDocument.whenAvailable"),
        },
        "fullDocumentBeforeChange": {
            "required": get("option.fullDocumentBeforeChange.required"),
            "whenAvailable": get("option.fullDocumentBeforeChange.whenAvailable"),
        },
        "ignoreRemovedShards": get("option.ignoreRemovedShards"),
        "matchCollectionUUIDForUpdateLookup": get("option.matchCollectionUUIDForUpdateLookup"),
        "resumeAfter": get("option.resumeAfter"),
        "showExpandedEvents": get("option.showExpandedEvents"),
        "showMigrationEvents": get("option.showMigrationEvents"),
        "showRawUpdateDescription": get("option.showRawUpdateDescription"),
        "showSystemEvents": get("option.showSystemEvents"),
        "startAfter": get("option.startAfter"),
        "startAtOperationTime": get("option.startAtOperationTime"),
        "cursor": {
            "batchSize": histogram("batchSize", &BATCH_BOUNDS),
            "maxTimeMS": histogram("maxTimeMS", &MAX_TIME_BOUNDS),
        },
    };
    let error = doc! {
        "nonRetriable": {
            "bsonObjectTooLarge": get("error.nonRetriable.bsonObjectTooLarge"),
            "changeStreamFatalError": get("error.nonRetriable.changeStreamFatalError"),
            "changeStreamHistoryLost": get("error.nonRetriable.changeStreamHistoryLost"),
            "other": get("error.nonRetriable.other"),
        },
        "retriable": {
            "interruptedDueToReplStateChange": get("error.retriable.interruptedDueToReplStateChange"),
            "other": get("error.retriable.other"),
        },
    };
    (option, error)
}

/// The largest event a change stream returns: MongoDB's BSON document
/// limit plus its 16 KiB of command headroom.
pub(crate) const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024 + 16 * 1024;

fn bson_len(d: &Document) -> u64 {
    bson::to_vec(d).map_or(0, |v| v.len() as u64)
}

/// A change stream of `tenant` read `events` from its feed, before its
/// pipeline.
pub(crate) fn stream_examined(tenant: u64, events: &[Document]) {
    let bytes: u64 = events.iter().map(bson_len).sum();
    count(tenant, |c| {
        c.docs_examined
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        c.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    });
}

/// A change-stream batch of `tenant` returned to the client.
pub(crate) fn stream_returned(tenant: u64, batch: &[Document]) {
    let bytes: u64 = batch.iter().map(bson_len).sum();
    count(tenant, |c| {
        c.batches.fetch_add(1, Ordering::Relaxed);
        c.docs_returned
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        c.bytes_returned.fetch_add(bytes, Ordering::Relaxed);
    });
}

/// The error for an event over [`MAX_EVENT_BYTES`] (MongoDB's 10334,
/// `BSONObjectTooLarge`), counted in `tenant`'s `largeEventsFailed`;
/// `None` for an event within the limit.
pub(crate) fn too_large(
    tenant: u64,
    event: &Document,
) -> Option<sprig_proto_mongo::errors::MongoError> {
    let size = bson_len(event);
    if size <= MAX_EVENT_BYTES as u64 {
        return None;
    }
    count(tenant, |c| {
        c.large_failed.fetch_add(1, Ordering::Relaxed);
    });
    Some(sprig_proto_mongo::errors::MongoError::new(
        10334,
        format!(
            "Executor error during getMore :: caused by :: BSONObj size: {size} (0x{size:X}) is invalid. Size must be between 0 and 16793600(16MB)"
        ),
    ))
}

/// The open change-stream cursors: each one's tenant and when it opened.
fn open_at() -> std::sync::MutexGuard<'static, HashMap<i64, (u64, Instant)>> {
    static M: std::sync::OnceLock<Mutex<HashMap<i64, (u64, Instant)>>> = std::sync::OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// The scope of a change stream on `ns` (`db.coll`, `db.$cmd...` for a
/// database, `admin.$cmd...` for the cluster): 0 cluster, 1 database, 2
/// collection.
pub(crate) fn stream_scope(ns: &str) -> usize {
    let (db, coll) = ns.split_once('.').unwrap_or((ns, ""));
    if coll.is_empty() || coll.starts_with("$cmd") {
        usize::from(db != "admin")
    } else {
        2
    }
}

/// A change-stream cursor `id` of `tenant` opened, of [`stream_scope`]
/// `scope`.
pub(crate) fn stream_opened(tenant: u64, id: i64, scope: usize) {
    count(tenant, |c| {
        c.opened.fetch_add(1, Ordering::Relaxed);
        c.scope[scope.min(2)].fetch_add(1, Ordering::Relaxed);
    });
    open_at().insert(id, (tenant, Instant::now()));
}

/// Cursor `id` closed (any cursor; only change streams are tracked).
pub(crate) fn cursor_closed(id: i64) {
    if let Some((tenant, t)) = open_at().remove(&id) {
        close(tenant, t);
    }
}

fn close(tenant: u64, t: Instant) {
    let micros = u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX);
    #[allow(clippy::cast_precision_loss)]
    let x = micros as f64;
    count(tenant, |c| {
        c.closed.fetch_add(1, Ordering::Relaxed);
        let mut avg = c
            .lifespan_average
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *avg = Some(avg.map_or(x, |a| a + LIFESPAN_ALPHA * (x - a)));
    });
}

/// Streams the table no longer holds (timed out, their session killed)
/// count as closed now. The ids of `tenant`'s open streams (every
/// tenant's for [`ALL`]).
fn sweep(c: &Conn, tenant: u64) -> Vec<i64> {
    let live: std::collections::HashSet<i64> = c.shared.cursors.ids().into_iter().collect();
    let mut m = open_at();
    let gone: Vec<i64> = m.keys().filter(|id| !live.contains(id)).copied().collect();
    for id in gone {
        if let Some((t, at)) = m.remove(&id) {
            close(t, at);
        }
    }
    m.iter()
        .filter(|(_, (t, _))| tenant == ALL || *t == tenant)
        .map(|(id, _)| *id)
        .collect()
}

/// Whose numbers `c`'s `serverStatus` shows: its tenant's, or the whole
/// gateway's for a login without a tenant.
fn view(c: &Conn) -> u64 {
    match super::commands::owner(c).tenant {
        0 => ALL,
        t => t,
    }
}

fn n(v: u64) -> Bson {
    Bson::Int64(i64::try_from(v).unwrap_or(i64::MAX))
}

/// `metrics.changeStreams` (MongoDB 9.0's names, the ones SprigDB
/// counts) of `tenant` ([`ALL`]: the whole gateway).
fn change_stream_metrics(c: &Conn, tenant: u64) -> Document {
    let open = sweep(c, tenant);
    let entries: HashMap<i64, _> = c.shared.cursors.entries().into_iter().collect();
    // A cursor a command is using (its lock taken) is pinned.
    let pinned = open
        .iter()
        .filter(|id| entries.get(id).is_some_and(|cur| cur.try_lock().is_err()))
        .count();
    let k = counters_of(tenant);
    let closed = k.closed.load(Ordering::Relaxed);
    // MongoDB 9.0's names: `average` is an exponential moving
    // average in µs, `totalCount` the streams closed.
    let average = k
        .lifespan_average
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .unwrap_or(0.0);
    let (option, error) = option_and_error_metrics(&k);
    let load = |a: &AtomicU64| n(a.load(Ordering::Relaxed));
    doc! {
        "cursor": {
            "open": {"total": n(open.len() as u64), "pinned": n(pinned as u64)},
            "totalOpened": load(&k.opened),
            "lifespan": {"average": average, "totalCount": n(closed)},
            "batchesReturned": load(&k.batches),
            "bytesRead": load(&k.bytes_read),
            "bytesReturned": load(&k.bytes_returned),
            "docsExamined": load(&k.docs_examined),
            "docsReturned": load(&k.docs_returned),
        },
        "largeEventsFailed": load(&k.large_failed),
        "largeEventsSplit": n(0),
        "option": option,
        "error": error,
        "scope": {
            "cluster": load(&k.scope[0]),
            "db": load(&k.scope[1]),
            "collection": load(&k.scope[2]),
        },
    }
}

/// `serverStatus`: the top-level fields always; each section unless
/// `{<section>: 0}`, or, with `none: 1` (MongoDB 8.3), only the sections
/// asked for by name. The counts are the caller's tenant's (its
/// connections, cursors, change streams and spills on this gateway); a
/// login without a tenant sees the whole gateway's.
pub(crate) fn server_status(c: &Conn, body: &Document) -> Document {
    let tenant = view(c);
    let none = body.get("none").is_some_and(truthy);
    let want = |s: &str| match body.get(s) {
        Some(v) => truthy(v),
        None => !none,
    };
    let up = c.shared.started.elapsed();
    let mut d = doc! {
        "host": c.shared.d.gateway_id.clone(),
        "version": c.shared.settings().server_version.clone(),
        "process": "mongos",
        "pid": i64::from(std::process::id()),
        "uptime": up.as_secs_f64(),
        "uptimeMillis": i64::try_from(up.as_millis()).unwrap_or(i64::MAX),
        "uptimeEstimate": i64::try_from(up.as_secs()).unwrap_or(i64::MAX),
        "localTime": DateTime::now(),
    };
    if want("connections") {
        let mine = c
            .shared
            .d
            .sessions
            .list()
            .into_iter()
            .filter(|r| tenant == ALL || r.tenant.is_some_and(|t| t.0 == tenant))
            .count();
        let current = i32::try_from(mine).unwrap_or(i32::MAX);
        d.insert("connections", doc! {"current": current});
    }
    if want("metrics") {
        d.insert(
            "metrics",
            doc! {
                "changeStreams": change_stream_metrics(c, tenant),
                "cursor": {"open": {"total": n(c.shared.cursors.len_of((tenant != ALL).then_some(tenant)) as u64)}},
                "query": sprig_doc::spill::metrics_doc((tenant != ALL).then_some(tenant)),
            },
        );
    }
    d.insert("ok", 1.0);
    d
}

/// A `serverStatus` section flag: `1`, `true`, ...
pub(crate) fn truthy(v: &Bson) -> bool {
    sprig_doc::filter::truthy(v)
}

/// The connection's UUID (`connectionStatus.authInfo.UUID`, MongoDB 8.1):
/// stable for the connection, from the process id and connection number.
fn conn_uuid(c: &Conn) -> Binary {
    let mut seed = c.shared.process_id.bytes().to_vec();
    seed.extend_from_slice(c.shared.d.gateway_id.as_bytes());
    seed.extend_from_slice(&c.id.to_le_bytes());
    let hi = sprig_doc::meta::fnv64(&seed);
    seed.reverse();
    let lo = sprig_doc::meta::fnv64(&seed);
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&hi.to_be_bytes());
    b[8..].copy_from_slice(&lo.to_be_bytes());
    // Version 8 (custom), RFC 9562 variant.
    b[6] = (b[6] & 0x0F) | 0x80;
    b[8] = (b[8] & 0x3F) | 0x80;
    Binary {
        subtype: bson::spec::BinarySubtype::Uuid,
        bytes: b.to_vec(),
    }
}

/// `connectionStatus`.
pub(crate) fn connection_status(c: &Conn) -> Document {
    let p = c.principal.as_ref();
    let users: Vec<Document> = p
        .map(|p| {
            doc! {"user": format!("{}.{}", p.tenant_name.clone().unwrap_or_default(), p.user), "db": "admin"}
        })
        .into_iter()
        .collect();
    doc! {
        "authInfo": {"authenticatedUsers": users, "authenticatedUserRoles": [], "UUID": conn_uuid(c)},
        "ok": 1.0,
    }
}

/// The caller's change-stream cursors as `currentOp` entries
/// (`idleCursor`), each with `cursor.changeStreams.optime`: the stream's
/// position, the cluster time of its last resume token. For
/// `$currentOp` with `idleCursors: true`.
pub(crate) fn change_stream_ops(c: &Conn) -> Vec<Document> {
    let who = super::commands::owner(c);
    let mut out = Vec::new();
    let opened = open_at().clone();
    for (id, cur) in c.shared.cursors.entries() {
        let Ok(g) = cur.try_lock() else { continue };
        let Some(cur) = g.as_ref() else { continue };
        if cur.owner != who {
            continue;
        }
        let Some(st) = cur.stream.as_ref() else {
            continue;
        };
        let clamp = sprig_feed::ResumeToken::from_hex(&st.token).map_or(0, |t| t.clamp);
        let ts = Timestamp {
            time: u32::try_from(clamp >> 32).unwrap_or(u32::MAX),
            increment: u32::try_from(clamp & 0xFFFF_FFFF).unwrap_or(0),
        };
        let mut cd = doc! {
            "cursorId": id,
            "tailable": true,
            "awaitData": true,
            "noCursorTimeout": cur.no_timeout,
            // No position before the stream's first event or progress
            // token (its token then has no cluster time).
            "changeStreams": if clamp == 0 { doc! {} } else { doc! {"optime": {"ts": ts, "t": -1i64}} },
        };
        if let Some((_, t)) = opened.get(&id) {
            let ago = i64::try_from(t.elapsed().as_millis()).unwrap_or(0);
            cd.insert(
                "createdDate",
                DateTime::from_millis(DateTime::now().timestamp_millis() - ago),
            );
        }
        let mut d =
            doc! {"type": "idleCursor", "host": "sprigdb", "ns": cur.ns.clone(), "cursor": cd};
        if let Some(l) = &cur.lsid {
            d.insert(
                "lsid",
                doc! {"id": Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: l.clone() }},
            );
        }
        out.push(d);
    }
    out
}

#[cfg(test)]
mod tests {
    use sprig_proto_mongo::errors::MongoError;

    use super::*;

    /// The option counters and histograms, and the error counters, under
    /// MongoDB 9.0's names, counted for their tenant only.
    #[test]
    fn change_stream_option_and_error_counters() {
        const T: u64 = 7_100_001;
        const OTHER: u64 = 7_100_002;
        stream_options(
            T,
            &doc! {"fullDocument": "updateLookup", "fullDocumentBeforeChange": "off", "resumeAfter": {"_data": "00"}, "showExpandedEvents": true},
            &doc! {"aggregate": "c", "cursor": {}, "maxTimeMS": 250},
        );
        stream_options(
            T,
            &doc! {"fullDocument": "default", "showExpandedEvents": false},
            &doc! {"aggregate": "c", "cursor": {"batchSize": 5}},
        );
        stream_get_more(
            T,
            &doc! {"getMore": 1i64, "collection": "c", "maxTimeMS": 1000},
        );
        stream_error(T, &MongoError::new(286, "history lost"));
        stream_error(T, &MongoError::new(280, "pre-image required"));
        stream_error(
            T,
            &MongoError::new(234, "rls changed").label("ResumableChangeStreamError"),
        );
        stream_error(T, &MongoError::new(2, "bad value"));
        let (o, e) = option_and_error_metrics(&counters_of(T));
        let at = |d: &Document, p: &str| {
            let mut cur = d.clone();
            let parts: Vec<&str> = p.split('/').collect();
            for k in &parts[..parts.len() - 1] {
                cur = cur.get_document(k).unwrap().clone();
            }
            cur.get_i64(parts[parts.len() - 1]).unwrap()
        };
        assert_eq!(at(&o, "fullDocument/updateLookup"), 1);
        assert_eq!(at(&o, "fullDocument/required"), 0);
        assert_eq!(at(&o, "fullDocumentBeforeChange/whenAvailable"), 0);
        assert_eq!(at(&o, "resumeAfter"), 1);
        assert_eq!(at(&o, "showExpandedEvents"), 1);
        assert_eq!(at(&o, "startAfter"), 0);
        assert_eq!(at(&o, "cursor/batchSize/[100, 1000)/count"), 1);
        assert_eq!(at(&o, "cursor/batchSize/[1, 10)/count"), 1);
        assert_eq!(at(&o, "cursor/batchSize/totalCount"), 2);
        assert_eq!(at(&o, "cursor/maxTimeMS/[200, 500)/count"), 1);
        assert_eq!(at(&o, "cursor/maxTimeMS/[1000, 2000)/count"), 1);
        assert_eq!(at(&o, "cursor/maxTimeMS/totalCount"), 2);
        assert_eq!(
            o.get_document("cursor")
                .unwrap()
                .get_document("maxTimeMS")
                .unwrap()
                .len(),
            15
        );
        assert_eq!(at(&e, "nonRetriable/changeStreamHistoryLost"), 1);
        assert_eq!(at(&e, "nonRetriable/changeStreamFatalError"), 1);
        assert_eq!(at(&e, "nonRetriable/other"), 1);
        assert_eq!(at(&e, "nonRetriable/bsonObjectTooLarge"), 0);
        assert_eq!(at(&e, "retriable/other"), 1);
        assert_eq!(at(&e, "retriable/interruptedDueToReplStateChange"), 0);
        // Another tenant sees none of it; the whole gateway's view does.
        let (o2, e2) = option_and_error_metrics(&counters_of(OTHER));
        assert_eq!(at(&o2, "fullDocument/updateLookup"), 0);
        assert_eq!(at(&o2, "cursor/batchSize/totalCount"), 0);
        assert_eq!(at(&e2, "nonRetriable/changeStreamHistoryLost"), 0);
        let (oa, _) = option_and_error_metrics(&counters_of(ALL));
        assert!(at(&oa, "fullDocument/updateLookup") >= 1);
        // The lifespan average is exponential: the newest weighs 0.2.
        close(T, Instant::now());
        assert!(counters_of(T).lifespan_average.lock().unwrap().is_some());
        assert!(
            counters_of(OTHER)
                .lifespan_average
                .lock()
                .unwrap()
                .is_none()
        );
    }

    /// Batches, events and bytes returned and examined, per tenant; an
    /// event past the BSON limit fails with 10334 and counts in its
    /// tenant's `largeEventsFailed`.
    #[test]
    fn change_stream_cursor_counters() {
        const T: u64 = 7_100_003;
        const OTHER: u64 = 7_100_004;
        let load =
            |t: u64, f: fn(&Counters) -> &AtomicU64| f(&counters_of(t)).load(Ordering::Relaxed);
        let ev = doc! {"operationType": "insert", "fullDocument": {"_id": 1, "a": "x"}};
        stream_examined(T, &[ev.clone(), ev.clone(), ev.clone()]);
        stream_returned(T, std::slice::from_ref(&ev));
        stream_returned(T, &[]);
        assert_eq!(load(T, |c| &c.batches), 2);
        assert_eq!(load(T, |c| &c.docs_returned), 1);
        assert_eq!(load(T, |c| &c.docs_examined), 3);
        assert_eq!(load(T, |c| &c.bytes_returned), bson_len(&ev));
        assert!(too_large(T, &ev).is_none());
        let big =
            doc! {"operationType": "update", "fullDocument": {"s": "x".repeat(MAX_EVENT_BYTES)}};
        let e = too_large(T, &big).unwrap();
        assert_eq!(e.code, 10334);
        assert!(e.message.contains("BSONObj size"), "{}", e.message);
        assert_eq!(load(T, |c| &c.large_failed), 1);
        for f in [
            (|c: &Counters| &c.batches) as fn(&Counters) -> &AtomicU64,
            |c| &c.docs_returned,
            |c| &c.docs_examined,
            |c| &c.large_failed,
        ] {
            assert_eq!(load(OTHER, f), 0);
        }
        assert!(load(ALL, |c| &c.batches) >= 2);
        // Streams opened and closed count for their tenant.
        stream_opened(T, -42, 2);
        cursor_closed(-42);
        assert_eq!(load(T, |c| &c.opened), 1);
        assert_eq!(load(T, |c| &c.closed), 1);
        assert_eq!(load(T, |c| &c.scope[2]), 1);
        assert_eq!(load(OTHER, |c| &c.opened), 0);
    }
}
