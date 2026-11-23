//! Change streams on collections stored on the engine, read from the
//! node's change log ([`crate::changelog`]).
//!
//! A stream reads its tenant's unit's change log from its start place, in
//! pages bounded by bytes, on a task of its own, and renders each change
//! of a document or of a collection record as MongoDB's event:
//!
//! - a document key's write is `insert`, `update` (with
//!   `updateDescription` from the before- and after-images), `replace` or
//!   `delete`, as the document store's hint says (without a hint: an
//!   insert when the key had no version before);
//! - a collection record's write is `create`, `modify`, `createIndexes`
//!   or `dropIndexes` (with `showExpandedEvents`), its delete `drop`, a
//!   delete and a put of the same collection number in one entry `rename`,
//!   and the database drop marker `dropDatabase`; a drop or rename of the
//!   watched collection, and the drop of the watched database, end the
//!   stream with `invalidate`.
//!
//! Before-images are the key's MVCC version just below the change (the
//! reader holds versions from its place on). `fullDocument: updateLookup`
//! is the after-image. `whenAvailable` and `required` images follow the
//! collection's `changeStreamPreAndPostImages` option, as on MongoDB.
//!
//! Tokens are change-log tokens (version 2): the entry and record, so a
//! resume is everything after them. `startAtOperationTime` starts a
//! segment or two before the first change stamped at or after it and
//! leaves out the earlier ones.

// One shape per match arm reads better here than `if let ... else`, and
// the short names (c, d, m, a, b) are the change, event, collection and the
// two images.
#![allow(
    clippy::single_match_else,
    clippy::many_single_char_names,
    clippy::match_same_arms,
    clippy::comparison_chain
)]

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bson::{Bson, DateTime, Document, Timestamp, doc};
use sprig_common::CancellationToken;
use sprig_doc::docstore::catalog::CollMeta;
use sprig_doc::docstore::layout::{self, CATALOG, CatalogKey, MAX_COLLECTION};
use sprig_engine::keys::{KeyRange, kind};
use sprig_engine::txn::Dir;
use sprig_engine::{Engine, Ts};
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_streams::changes::{Change, ChangeLog, HintOp, Page, Reader, Token};
use tokio::sync::mpsc;

use super::changes::{ChangeStream, Images, Item, StreamSpec, token_doc};

/// Bytes a stream reads from the change log at a time.
const PAGE_BYTES: usize = 1 << 20;
/// Bytes of events one stream may hold for its client
/// (`feed.consumer_buffer_bytes`); a slow client's stream stops reading and
/// later reads on from the change log on disk.
const STREAM_BYTES: usize = 8 << 20;

/// The node's budget for every stream's buffered events
/// (`feed.buffer_budget_mb`; `SPRIGDB_FEED_BUFFER_BUDGET_MB`, default 256).
fn node_budget() -> usize {
    static B: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *B.get_or_init(|| {
        std::env::var("SPRIGDB_FEED_BUFFER_BUDGET_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(256)
            << 20
    })
}

/// Bytes buffered by every change-log stream of the node.
static NODE_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// One stream's buffered bytes and whether its client is gone.
#[derive(Default)]
struct Held {
    state: std::sync::Mutex<(usize, bool)>,
    freed: tokio::sync::Notify,
}

impl Held {
    fn lock(&self) -> std::sync::MutexGuard<'_, (usize, bool)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Takes `n` bytes when the stream and the node have room (a stream
    /// with nothing buffered always may, so every stream progresses);
    /// false when the client is gone.
    async fn take(&self, n: usize, stop: &CancellationToken) -> bool {
        use std::sync::atomic::Ordering;
        loop {
            let notified = self.freed.notified();
            {
                let mut g = self.lock();
                if g.1 {
                    return false;
                }
                let node = NODE_BYTES.load(Ordering::Acquire);
                if g.0 == 0 || (g.0 + n <= STREAM_BYTES && node + n <= node_budget()) {
                    g.0 += n;
                    NODE_BYTES.fetch_add(n, Ordering::AcqRel);
                    return true;
                }
            }
            tokio::select! {
                () = notified => {}
                () = stop.cancelled() => return false,
                () = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    /// Gives back `n` bytes (`None`: the client is gone, all of them).
    fn give(&self, n: Option<usize>) {
        use std::sync::atomic::Ordering;
        let mut g = self.lock();
        let back = match n {
            Some(n) => n.min(g.0),
            None => {
                g.1 = true;
                g.0
            }
        };
        g.0 -= back;
        NODE_BYTES.fetch_sub(back, Ordering::AcqRel);
        drop(g);
        self.freed.notify_waiters();
    }
}
/// Events a stream's queue holds before its reader waits.
const QUEUE: usize = 256;
/// [`Token::flags`]: the event ends a collection or database stream.
const ENDS: u8 = 4;
/// [`Token::flags`]: the `invalidate` event.
const INVALIDATE: u8 = 2;

/// An event's `clusterTime` and `wallTime` from a commit timestamp
/// (milliseconds << 16 | counter): seconds, and the rest of the
/// millisecond with the counter as the increment.
fn times(cts: u64) -> (Timestamp, DateTime) {
    let ms = cts >> 16;
    let logical = cts & 0xFFFF;
    (
        Timestamp {
            time: u32::try_from(ms / 1000).unwrap_or(u32::MAX),
            increment: u32::try_from(((ms % 1000) << 16) | logical).unwrap_or(0),
        },
        DateTime::from_millis(i64::try_from(ms).unwrap_or(i64::MAX)),
    )
}

/// The first commit timestamp at or after cluster time `t` (a gateway's
/// cluster time counts within a second, so the whole second counts).
fn cts_of(t: Timestamp) -> u64 {
    (u64::from(t.time) * 1000) << 16
}

fn bad_token(m: &str) -> MongoError {
    MongoError::new(260, format!("invalid resume token: {m}"))
}

fn lost(e: &sprig_common::SprigError) -> MongoError {
    if sprig_streams::changes::is_history_lost(e) {
        MongoError::new(286, e.message.clone()).label("NonResumableChangeStreamError")
    } else {
        MongoError::new(code::INTERNAL_ERROR, e.message.clone())
    }
}

/// The collections of `tenant` as of `at`, by number.
fn catalog_at(engine: &Engine, tenant: u64, at: Ts) -> HashMap<u32, CollMeta> {
    let p = layout::coll_records(tenant, None);
    let r = KeyRange::new(p.clone(), layout::after_prefix(&p));
    let mut scan =
        sprig_engine::txn::mvcc::VersionScan::new(engine.store().as_ref(), &r, Dir::Forward, at);
    let mut out = HashMap::new();
    while let Ok(Some((_, v))) = scan.next_visible() {
        let Some(v) = v.value else { continue };
        let Ok(rec) = Document::from_reader(&mut &v[..]) else {
            continue;
        };
        let (Ok(db), Ok(name)) = (rec.get_str("db"), rec.get_str("name")) else {
            continue;
        };
        if let Ok(m) = CollMeta::from_record(db, name, &rec) {
            out.insert(m.n, m);
        }
    }
    out
}

fn keeps_images(m: &CollMeta) -> bool {
    m.options
        .get_document("changeStreamPreAndPostImages")
        .ok()
        .and_then(|d| d.get_bool("enabled").ok())
        .unwrap_or(false)
}

/// Opens a stream over engine collections. `start_after`: the resume
/// value came as `startAfter` (an invalidate token may be resumed).
///
/// # Errors
/// 260 for a token that isn't this tenant's change-log token, or an
/// invalidate token with `resumeAfter`; 286 when its history is gone;
/// 115 when the change log isn't open on this node.
pub(crate) async fn open(
    spec: StreamSpec,
    resume: Option<&Bson>,
    start_after: bool,
) -> Result<ChangeStream, MongoError> {
    let (Some(log), Ok(engine)) = (crate::changelog::changes(), crate::node_engine::engine())
    else {
        return Err(MongoError::new(
            code::COMMAND_NOT_SUPPORTED,
            "change streams on engine collections need the node's change log, which is not open (SPRIGDB_ENGINE_CHANGELOG)",
        ));
    };
    if NODE_BYTES.load(std::sync::atomic::Ordering::Acquire) >= node_budget() {
        return Err(MongoError::new(
            i32::from(sprig_common::SprigCode::MemoryLimit.number()),
            "the node's change-stream buffers are full (feed.buffer_budget_mb); retry shortly",
        )
        .label("ResumableChangeStreamError"));
    }
    let tenant = spec.tenant.0;
    let unit = Engine::unit_of_tenant(tenant).0;
    let mut from_cts = 0u64;
    let mut ends_now = false;
    let (pos, cts, first) = match resume {
        Some(Bson::Timestamp(t)) => {
            from_cts = cts_of(*t);
            let p = log.place_at(unit, from_cts);
            (p, from_cts, None)
        }
        Some(v) => {
            let hex = v
                .as_document()
                .and_then(|d| d.get_str("_data").ok())
                .ok_or_else(|| {
                    MongoError::new(code::BAD_VALUE, "resume token must be {_data: <string>}")
                })?;
            let t = Token::from_hex(hex).ok_or_else(|| bad_token("not a change-log token"))?;
            if t.tenant != tenant {
                return Err(bad_token("the token is another tenant's"));
            }
            if t.flags & INVALIDATE != 0 && !start_after {
                return Err(MongoError::new(
                    260,
                    "cannot resume a stream after an invalidate event; use startAfter",
                ));
            }
            ends_now = t.flags & ENDS != 0 && !start_after && spec.coll.is_some();
            (t.pos(), t.cts, Some(hex.to_owned()))
        }
        None => {
            // "Now" is after every commit acknowledged before the stream
            // opened: wait (briefly) until the change log has seen the
            // unit's durable entries (it writes on the apply step).
            let want = engine.consensus().durable_index(sprig_engine::UnitId(unit));
            let mut w = log.watch(unit);
            let until = tokio::time::Instant::now() + Duration::from_secs(2);
            while log.head(unit).0.index < want && tokio::time::Instant::now() < until {
                let _ = tokio::time::timeout_at(until, w.changed()).await;
            }
            let (p, c) = log.head(unit);
            (p, c.max(engine.txn_shared().visible_ts().0), None)
        }
    };
    let reader = log.reader(unit, pos, Some(cts)).map_err(|e| lost(&e))?;
    let at = if cts == 0 {
        engine.txn_shared().visible_ts()
    } else {
        Ts(cts)
    };
    let colls = catalog_at(&engine, tenant, at);
    let token = first.unwrap_or_else(|| Token::at(tenant, cts, pos, true).to_hex());
    let (tx, rx) = mpsc::channel(QUEUE);
    let stop = CancellationToken::new();
    let pipeline = spec.pipeline.clone();
    let policies = crate::tenant::rls::DocViews::fingerprint(spec.visibility.as_deref());
    let held = std::sync::Arc::new(Held::default());
    let run = Run {
        spec,
        log,
        tenant,
        colls,
        from_cts,
        ends_now,
        held: std::sync::Arc::clone(&held),
    };
    tokio::spawn(run.run(reader, tx, stop.clone()));
    metrics::gauge!("sprig_mongo_cursors_open", "kind" => "change_stream").increment(1.0);
    let release = super::changes::Release(std::sync::Arc::new(move |n| held.give(n)));
    Ok(ChangeStream::from_parts(
        rx,
        stop,
        token,
        pipeline,
        policies,
        tenant,
        Some(release),
    ))
}

struct Run {
    spec: StreamSpec,
    log: ChangeLog,
    tenant: u64,
    colls: HashMap<u32, CollMeta>,
    from_cts: u64,
    ends_now: bool,
    held: std::sync::Arc<Held>,
}

/// One rendered change.
enum Out {
    Event(Document),
    /// The event, then `invalidate`.
    Ends(Document),
    Skip,
}

impl Run {
    async fn run(mut self, mut reader: Reader, tx: mpsc::Sender<Item>, stop: CancellationToken) {
        let mut watch = self.log.watch(reader.unit());
        if self.ends_now {
            let t = Token::at(self.tenant, 0, reader.pos(), false);
            let _ = tx
                .send(Item::Invalidate(
                    Box::new(Document::new()),
                    self.invalidate_token(t),
                ))
                .await;
            Self::done();
            return;
        }
        loop {
            watch.borrow_and_update();
            let r = tokio::task::spawn_blocking(move || {
                let p = reader.read(PAGE_BYTES);
                (reader, p)
            })
            .await;
            let Ok((r, page)) = r else { break };
            reader = r;
            let page: Page = match page {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.send(Item::Failed(lost(&e))).await;
                    break;
                }
            };
            let mut skip: HashSet<u32> = HashSet::new();
            let mut ended = false;
            for (i, c) in page.changes.iter().enumerate() {
                if skip.contains(&c.seq) && page.changes[..i].iter().any(|x| x.index == c.index) {
                    continue;
                }
                let out = self.render(c, &page.changes[i + 1..], &mut skip);
                let t = Token::at(self.tenant, c.cts, c.pos(), false);
                let item = match out {
                    Ok(Out::Event(d)) if c.cts >= self.from_cts => {
                        // An event's share of the buffers: its change and
                        // the images it carries, roughly.
                        let n = 256 + 2 * (c.key.len() + c.value.as_ref().map_or(0, Vec::len));
                        if !self.held.take(n, &stop).await {
                            break;
                        }
                        Item::Sized(Box::new(d), t.to_hex(), n)
                    }
                    Ok(Out::Ends(d)) if c.cts >= self.from_cts => {
                        let mut it = t;
                        it.flags = ENDS;
                        let mut d = d;
                        d.insert("_id", token_doc(&it.to_hex()));
                        ended = true;
                        Item::Invalidate(Box::new(d), self.invalidate_token(it))
                    }
                    Ok(_) => Item::Progress(Token::at(self.tenant, c.cts, c.pos(), true).to_hex()),
                    Err(e) => {
                        ended = true;
                        Item::Failed(e)
                    }
                };
                if !Self::send(&tx, item).await {
                    Self::done();
                    return;
                }
                if ended {
                    break;
                }
            }
            if ended {
                break;
            }
            if page.at_end {
                let t = Token::at(self.tenant, page.cts, page.pos, true);
                if !Self::send(&tx, Item::Progress(t.to_hex())).await {
                    break;
                }
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = tx.closed() => break,
                    r = watch.changed() => if r.is_err() { break },
                    () = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            } else if stop.is_cancelled() || tx.is_closed() {
                break;
            }
        }
        Self::done();
    }

    fn done() {
        metrics::gauge!("sprig_mongo_cursors_open", "kind" => "change_stream").decrement(1.0);
    }

    fn invalidate_token(&self, mut t: Token) -> String {
        t.flags = INVALIDATE;
        let _ = self;
        t.to_hex()
    }

    async fn send(tx: &mpsc::Sender<Item>, item: Item) -> bool {
        if matches!(item, Item::Progress(_)) {
            return match tx.try_send(item) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            };
        }
        tx.send(item).await.is_ok()
    }

    fn base(&self, c: &Change, op: &str) -> Document {
        let (ts, wall) = times(c.cts);
        let t = Token::at(self.tenant, c.cts, c.pos(), false);
        doc! {
            "_id": token_doc(&t.to_hex()),
            "operationType": op,
            "clusterTime": ts,
            "wallTime": wall,
        }
    }

    fn watched(&self, db: &str, coll: Option<&str>) -> bool {
        if let Some(d) = &self.spec.db
            && d != db
        {
            return false;
        }
        match (&self.spec.coll, coll) {
            (Some((name, _)), Some(c)) => name == c,
            (Some(_), None) => true,
            (None, _) => true,
        }
    }

    fn uuid(&self, d: &mut Document, m: &CollMeta) {
        if self.spec.expanded {
            d.insert(
                "collectionUUID",
                bson::Binary {
                    subtype: bson::spec::BinarySubtype::Uuid,
                    bytes: m.uuid.to_vec(),
                },
            );
        }
    }

    fn render(
        &mut self,
        c: &Change,
        rest: &[Change],
        skip: &mut HashSet<u32>,
    ) -> Result<Out, MongoError> {
        let Some((p, user)) = c.prefix() else {
            return Ok(Out::Skip);
        };
        if p.tenant != self.tenant || p.shard != 0 {
            return Ok(Out::Skip);
        }
        if p.object == CATALOG && p.kind == kind::META {
            return Ok(self.catalog(c, p.index, user, rest, skip));
        }
        if p.kind != kind::PRIMARY
            || p.index != 0
            || p.object <= CATALOG
            || p.object > CATALOG + MAX_COLLECTION
        {
            return Ok(Out::Skip);
        }
        let n = p.object - CATALOG;
        let Some(m) = self.colls.get(&n) else {
            return Ok(Out::Skip);
        };
        if m.view().is_some() || !self.watched(&m.db, Some(&m.name)) {
            return Ok(Out::Skip);
        }
        let m = m.clone();
        self.document(c, &m)
    }

    fn doc_of(v: &[u8]) -> Option<Document> {
        Document::from_reader(&mut layout::doc_bson(v)).ok()
    }

    #[allow(clippy::too_many_lines)]
    fn document(&self, c: &Change, m: &CollMeta) -> Result<Out, MongoError> {
        let after = c.value.as_deref().and_then(Self::doc_of);
        let op = match (&c.hint, &c.value) {
            (Some((h, _)), _) => *h,
            (None, None) => HintOp::Delete,
            (None, Some(_)) => HintOp::Update,
        };
        let needs_before =
            matches!(op, HintOp::Update | HintOp::Delete | HintOp::Replace) || c.hint.is_none();
        let before = if needs_before {
            self.log
                .before_image(c)
                .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.message.clone()))?
                .and_then(|v| Self::doc_of(&v))
        } else {
            None
        };
        // Without a hint: an insert when the key had no version before.
        let op = match (c.hint.is_none(), op, &before) {
            (true, HintOp::Update, None) => HintOp::Insert,
            (true, HintOp::Delete, None) => return Ok(Out::Skip),
            _ => op,
        };
        let kept = keeps_images(m);
        let (db, coll) = (m.db.as_str(), m.name.as_str());
        // Row-level security: as on the MariaDB path, an update that hides
        // a document arrives as a delete, one that shows it as an insert.
        let see = |d: Option<&Document>| {
            d.is_some_and(|d| {
                self.spec
                    .visibility
                    .as_ref()
                    .is_some_and(|v| v.visible(db, coll, d))
            })
        };
        let (b, a) = (see(before.as_ref()), see(after.as_ref()));
        let op = match op {
            HintOp::Insert if !a => return Ok(Out::Skip),
            HintOp::Delete if !b => return Ok(Out::Skip),
            HintOp::Update | HintOp::Replace => match (b, a) {
                (false, false) => return Ok(Out::Skip),
                (true, false) => HintOp::Delete,
                (false, true) => HintOp::Insert,
                (true, true) => op,
            },
            other => other,
        };
        let image = |mode: Images, img: Option<&Document>| -> Result<Option<Bson>, MongoError> {
            if mode == Images::Off {
                return Ok(None);
            }
            if mode.needs_option() && !kept {
                if mode == Images::Required {
                    return Err(MongoError::new(
                        280,
                        "Change stream was configured to require a pre- or post-image, but the collection doesn't keep them (changeStreamPreAndPostImages)",
                    )
                    .label("NonResumableChangeStreamError"));
                }
                return Ok(Some(Bson::Null));
            }
            match img {
                Some(d) => Ok(Some(Bson::Document(d.clone()))),
                None if mode == Images::Required => Err(MongoError::new(
                    280,
                    "Change stream was configured to require a pre- or post-image, but it is no longer available",
                )
                .label("NonResumableChangeStreamError")),
                None => Ok(Some(Bson::Null)),
            }
        };
        let ns = doc! {"db": db, "coll": coll};
        let id_of =
            |d: Option<&Document>| d.and_then(|d| d.get("_id").cloned()).unwrap_or(Bson::Null);
        let mut d;
        match op {
            HintOp::Insert => {
                d = self.base(c, "insert");
                self.uuid(&mut d, m);
                d.insert(
                    "fullDocument",
                    after.clone().map_or(Bson::Null, Bson::Document),
                );
                d.insert("ns", ns);
                d.insert("documentKey", doc! {"_id": id_of(after.as_ref())});
            }
            HintOp::Replace => {
                d = self.base(c, "replace");
                self.uuid(&mut d, m);
                d.insert(
                    "fullDocument",
                    after.clone().map_or(Bson::Null, Bson::Document),
                );
                d.insert("ns", ns);
                d.insert("documentKey", doc! {"_id": id_of(after.as_ref())});
                if let Some(v) = image(self.spec.before, before.as_ref())? {
                    d.insert("fullDocumentBeforeChange", v);
                }
            }
            HintOp::Update => {
                d = self.base(c, "update");
                self.uuid(&mut d, m);
                let full = if self.spec.full == Images::On {
                    after.clone().map(Bson::Document)
                } else {
                    image(self.spec.full, after.as_ref())?
                };
                if let Some(v) = full {
                    d.insert("fullDocument", v);
                }
                d.insert("ns", ns);
                d.insert("documentKey", doc! {"_id": id_of(after.as_ref())});
                let desc = match (&before, &after) {
                    (Some(b), Some(a)) => describe(b, a, self.spec.expanded),
                    _ => doc! {"updatedFields": {}, "removedFields": [], "truncatedArrays": []},
                };
                d.insert("updateDescription", desc);
                if let Some(v) = image(self.spec.before, before.as_ref())? {
                    d.insert("fullDocumentBeforeChange", v);
                }
            }
            HintOp::Delete => {
                d = self.base(c, "delete");
                self.uuid(&mut d, m);
                d.insert("ns", ns);
                d.insert("documentKey", doc! {"_id": id_of(before.as_ref())});
                if let Some(v) = image(self.spec.before, before.as_ref())? {
                    d.insert("fullDocumentBeforeChange", v);
                }
            }
        }
        Ok(Out::Event(d))
    }

    /// A change of the catalog object's metadata: collection records and
    /// database drop markers.
    #[allow(clippy::too_many_lines)]
    fn catalog(
        &mut self,
        c: &Change,
        index: u16,
        user: &[u8],
        rest: &[Change],
        skip: &mut HashSet<u32>,
    ) -> Out {
        match layout::catalog_key(index, user) {
            CatalogKey::DbDrop(db) => {
                if !self.watched(&db, None) {
                    return Out::Skip;
                }
                let mut d = self.base(c, "dropDatabase");
                d.insert("ns", doc! {"db": db.clone()});
                if self.spec.db.as_deref() == Some(db.as_str()) && self.spec.coll.is_none() {
                    Out::Ends(d)
                } else {
                    Out::Event(d)
                }
            }
            CatalogKey::Other => Out::Skip,
            CatalogKey::Coll { db, coll } => {
                let meta = c.value.as_deref().and_then(|v| {
                    let rec = Document::from_reader(&mut &v[..]).ok()?;
                    CollMeta::from_record(&db, &coll, &rec).ok()
                });
                match meta {
                    Some(m) => {
                        let prev = self.colls.insert(m.n, m.clone());
                        if !self.watched(&db, Some(&coll)) || m.view().is_some() {
                            return Out::Skip;
                        }
                        match prev {
                            None if self.spec.expanded => {
                                let mut d = self.base(c, "create");
                                self.uuid(&mut d, &m);
                                d.insert("ns", doc! {"db": db, "coll": coll});
                                let mut desc =
                                    doc! {"idIndex": {"v": 2, "key": {"_id": 1}, "name": "_id_"}};
                                if !m.options.is_empty() {
                                    for (k, v) in &m.options {
                                        desc.insert(k.clone(), v.clone());
                                    }
                                }
                                d.insert("operationDescription", desc);
                                Out::Event(d)
                            }
                            Some(p) if self.spec.expanded => {
                                let before: HashSet<String> = p
                                    .index_specs()
                                    .iter()
                                    .filter_map(|s| s.get_str("name").ok().map(str::to_owned))
                                    .collect();
                                let after: HashSet<String> = m
                                    .index_specs()
                                    .iter()
                                    .filter_map(|s| s.get_str("name").ok().map(str::to_owned))
                                    .collect();
                                let (kind, specs) = if after.len() > before.len() {
                                    (
                                        "createIndexes",
                                        m.index_specs()
                                            .into_iter()
                                            .filter(|s| {
                                                s.get_str("name").is_ok_and(|n| !before.contains(n))
                                            })
                                            .collect::<Vec<_>>(),
                                    )
                                } else if after.len() < before.len() {
                                    (
                                        "dropIndexes",
                                        p.index_specs()
                                            .into_iter()
                                            .filter(|s| {
                                                s.get_str("name").is_ok_and(|n| !after.contains(n))
                                            })
                                            .collect(),
                                    )
                                } else {
                                    ("modify", Vec::new())
                                };
                                let mut d = self.base(c, kind);
                                self.uuid(&mut d, &m);
                                d.insert("ns", doc! {"db": db, "coll": coll});
                                if kind == "modify" {
                                    d.insert("operationDescription", Document::new());
                                } else {
                                    d.insert("operationDescription", doc! {"indexes": specs});
                                }
                                Out::Event(d)
                            }
                            _ => Out::Skip,
                        }
                    }
                    None => {
                        // The record went: a drop, or a rename when the
                        // same entry puts the number under another name.
                        let Some(n) = self
                            .colls
                            .iter()
                            .find(|(_, m)| m.db == db && m.name == coll)
                            .map(|(n, _)| *n)
                        else {
                            return Out::Skip;
                        };
                        let old = self
                            .colls
                            .remove(&n)
                            .unwrap_or_else(|| CollMeta::new(&db, &coll, n, Document::new()));
                        let renamed = rest.iter().filter(|x| x.index == c.index).find_map(|x| {
                            let (p, u) = x.prefix()?;
                            if p.object != CATALOG || p.kind != kind::META {
                                return None;
                            }
                            let CatalogKey::Coll { db: d2, coll: c2 } =
                                layout::catalog_key(p.index, u)
                            else {
                                return None;
                            };
                            let rec = Document::from_reader(&mut x.value.as_deref()?).ok()?;
                            let m = CollMeta::from_record(&d2, &c2, &rec).ok()?;
                            (m.n == n).then_some((x.seq, m))
                        });
                        if let Some((seq, to)) = renamed {
                            skip.insert(seq);
                            self.colls.insert(to.n, to.clone());
                            let from_seen = self.watched(&db, Some(&coll));
                            let to_seen = self.watched(&to.db, Some(&to.name));
                            if !from_seen && !to_seen {
                                return Out::Skip;
                            }
                            let mut d = self.base(c, "rename");
                            self.uuid(&mut d, &old);
                            d.insert("ns", doc! {"db": db.clone(), "coll": coll.clone()});
                            let to_ns = doc! {"db": to.db.clone(), "coll": to.name.clone()};
                            d.insert("to", to_ns.clone());
                            if self.spec.expanded {
                                d.insert(
                                    "operationDescription",
                                    doc! {"to": to_ns, "dropTarget": false},
                                );
                            }
                            return if self.spec.coll.is_some() && from_seen {
                                Out::Ends(d)
                            } else {
                                Out::Event(d)
                            };
                        }
                        if !self.watched(&db, Some(&coll)) || old.view().is_some() {
                            return Out::Skip;
                        }
                        let mut d = self.base(c, "drop");
                        self.uuid(&mut d, &old);
                        d.insert("ns", doc! {"db": db, "coll": coll});
                        if self.spec.coll.is_some() {
                            Out::Ends(d)
                        } else {
                            Out::Event(d)
                        }
                    }
                }
            }
        }
    }
}

/// MongoDB's `updateDescription` from the before- and after-images:
/// changed and added fields (documents compared field by field, arrays by
/// position), removed fields, arrays cut shorter (`truncatedArrays`, their
/// remaining positions compared), and, with `showExpandedEvents`,
/// `disambiguatedPaths` for each path with a numeric component (array
/// positions as integers, field names as strings).
#[must_use]
pub fn describe(old: &Document, new: &Document, expanded: bool) -> Document {
    struct Acc {
        upd: Document,
        rem: Vec<Bson>,
        trunc: Vec<Bson>,
        amb: Document,
    }
    fn note(acc: &mut Acc, comps: &[Bson]) {
        let numeric = comps.iter().any(|c| match c {
            Bson::Int32(_) => true,
            Bson::String(s) => !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
            _ => false,
        });
        if numeric {
            acc.amb.insert(join(comps), Bson::Array(comps.to_vec()));
        }
    }
    fn join(comps: &[Bson]) -> String {
        comps
            .iter()
            .map(|c| match c {
                Bson::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(".")
    }
    fn eq(a: &Bson, b: &Bson) -> bool {
        let wrap = |v: &Bson| bson::to_vec(&doc! {"v": v.clone()}).unwrap_or_default();
        wrap(a) == wrap(b)
    }
    fn value(acc: &mut Acc, comps: &mut Vec<Bson>, a: &Bson, b: &Bson) {
        match (a, b) {
            _ if eq(a, b) => {}
            (Bson::Document(x), Bson::Document(y)) => doc_(acc, comps, x, y),
            (Bson::Array(x), Bson::Array(y)) => {
                if y.len() < x.len() {
                    acc.trunc.push(Bson::Document(doc! {
                        "field": join(comps),
                        "newSize": i32::try_from(y.len()).unwrap_or(i32::MAX),
                    }));
                }
                for (i, v) in y.iter().enumerate() {
                    comps.push(Bson::Int32(i32::try_from(i).unwrap_or(i32::MAX)));
                    match x.get(i) {
                        Some(o) => value(acc, comps, o, v),
                        None => {
                            acc.upd.insert(join(comps), v.clone());
                            note(acc, comps);
                        }
                    }
                    comps.pop();
                }
            }
            _ => {
                acc.upd.insert(join(comps), b.clone());
                note(acc, comps);
            }
        }
    }
    fn doc_(acc: &mut Acc, comps: &mut Vec<Bson>, a: &Document, b: &Document) {
        for (k, nv) in b {
            comps.push(Bson::String(k.clone()));
            match a.get(k) {
                Some(ov) => value(acc, comps, ov, nv),
                None => {
                    acc.upd.insert(join(comps), nv.clone());
                    note(acc, comps);
                }
            }
            comps.pop();
        }
        for k in a.keys() {
            if !b.contains_key(k) {
                comps.push(Bson::String(k.clone()));
                acc.rem.push(Bson::String(join(comps)));
                note(acc, comps);
                comps.pop();
            }
        }
    }
    let mut acc = Acc {
        upd: Document::new(),
        rem: Vec::new(),
        trunc: Vec::new(),
        amb: Document::new(),
    };
    doc_(&mut acc, &mut Vec::new(), old, new);
    let mut d = doc! {
        "updatedFields": acc.upd,
        "removedFields": acc.rem,
        "truncatedArrays": acc.trunc,
    };
    if expanded {
        d.insert("disambiguatedPaths", acc.amb);
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_times_order_like_commit_timestamps() {
        let a = (1_700_000_000_123u64 << 16) | 5;
        let b = (1_700_000_000_123u64 << 16) | 6;
        let c = 1_700_000_001_000u64 << 16;
        let (ta, wa) = times(a);
        let (tb, _) = times(b);
        let (tc, _) = times(c);
        assert!((ta.time, ta.increment) < (tb.time, tb.increment));
        assert!((tb.time, tb.increment) < (tc.time, tc.increment));
        assert_eq!(wa.timestamp_millis(), 1_700_000_000_123);
        assert!(cts_of(ta) <= a);
    }

    #[tokio::test]
    async fn buffers_are_bounded_per_stream() {
        use std::sync::atomic::Ordering;
        let h = Held::default();
        let stop = CancellationToken::new();
        let base = NODE_BYTES.load(Ordering::Acquire);
        assert!(h.take(STREAM_BYTES - 10, &stop).await);
        // Full: the next event waits until the client takes some.
        let t = tokio::time::timeout(Duration::from_millis(100), h.take(100, &stop)).await;
        assert!(t.is_err());
        h.give(Some(STREAM_BYTES - 10));
        assert!(h.take(100, &stop).await);
        assert_eq!(NODE_BYTES.load(Ordering::Acquire), base + 100);
        // The client went: everything comes back, and nothing more is taken.
        h.give(None);
        assert!(!h.take(1, &stop).await);
        assert_eq!(NODE_BYTES.load(Ordering::Acquire), base);
    }

    #[test]
    fn update_descriptions() {
        let d = describe(
            &doc! {"_id": 1, "a": {"1": 1}, "arr": ["foo", {"a": "bar"}, 1, 2, 3], "gone": 1},
            &doc! {"_id": 1, "a": {"1": 2}, "arr": ["foo", {"a": "bar"}], "x": [1]},
            true,
        );
        assert_eq!(
            d,
            doc! {
                "updatedFields": {"a.1": 2, "x": [1]},
                "removedFields": ["gone"],
                "truncatedArrays": [{"field": "arr", "newSize": 2}],
                "disambiguatedPaths": {"a.1": ["a", "1"]},
            }
        );
        let d = describe(&doc! {"a": [{"1": 1}]}, &doc! {"a": [{"1": 2}]}, true);
        assert_eq!(
            d.get_document("disambiguatedPaths").unwrap(),
            &doc! {"a.0.1": ["a", 0, "1"]}
        );
        assert!(!describe(&doc! {}, &doc! {"a": 1}, false).contains_key("disambiguatedPaths"));
    }
}
