//! Change streams over the API on engine databases, read from the node's
//! change log ([`crate::changelog`]) instead of the MariaDB binlog feed.
//!
//! ```text
//! open: the tenant's unit's change log, from
//!         start_after (a change-log token of the tenant; 39801 otherwise),
//!         start_at_hlc (a segment or two before the first change stamped at
//!           or after it; earlier changes left out), or
//!         now (after every commit acknowledged before the stream opened)
//! loop: one page of the log (at most 1 MiB), on a blocking thread:
//!         rows (key kind P) of the database's tables, current row
//!           generation only, with their before-images (MVCC, held by the
//!           reader from its place on)
//!         schema versions of its tables (catalog records m/core/sv/):
//!           ddl, or truncate for a TRUNCATE
//!         a table's object record turning dropped: ddl (DROP TABLE)
//!       events checked as the subscriber, as on MariaDB databases
//!         (exec::rls_images: SELECT and WATCH, row policies over images)
//!       at the end of the log: wait for the next apply; nothing for
//!         api.stream.heartbeat_seconds: a heartbeat with the token after
//!         everything read
//! ```
//!
//! Tokens are change-log tokens (version 2, hex): an event's names its
//! record, a heartbeat's the place after everything read, so a resume
//! goes on exactly after either. `commit_hlc` is the commit timestamp in
//! the API's HLC form: seconds in the high 32 bits, the millisecond and
//! counter in the low (as MongoDB change streams on engine collections
//! show `clusterTime`).
//!
//! Inheritable tables (data inheritance) show the subscriber's merged view,
//! as on MariaDB databases (`inherit`): their layer rows' and copies'
//! changes (`__sprig_ih_<object>`, a tombstone as a delete) become feed
//! events for the same mapping, and the parent tenants whose records the
//! tenant reads through have their change logs read too, from the stream's
//! start (`parent_line`). A copy's handover is a pair of engine commit
//! timestamps recorded by its seed; all units of a node share one clock,
//! so they compare with any tenant's changes.
//!
//! A client that doesn't take its events stops the stream reading (the
//! log is on disk, its place is a position); one whose buffer stayed full
//! for four heartbeat periods gets 39790 and resumes from its last token.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::kind;
use sprig_streams::changes::{Change, ChangeLog, Page, Reader, Token};
use tokio::sync::mpsc;

use super::super::Core;
use super::super::sql::Caller;
use super::{Limits, Slot, WatchEvent, WatchSpec, check, inherit};
use crate::exec::rls_images::{ImagesRequest, TableImages};
use crate::table_changes::{self, PAGE_BYTES, Raw, Watched};

/// Events checked and sent per batch at most.
const DEPTH: usize = 256;

/// A catalog record's relative key: a schema version row
/// (`m/core/sv/<object u64><version u64>`).
const SCHEMA_VERSIONS: &[u8] = b"m/core/sv/";
/// An object row (`m/core/objects/<object u64>`).
const OBJECTS: &[u8] = b"m/core/objects/";

/// Whether `database` of `tenant` streams from the engine's change log:
/// it is one of the engine's databases, or the tenant has no MariaDB
/// cluster (a database it doesn't have yet would be the engine's).
pub(super) async fn serves(
    svc: &crate::tenant::TenantServices,
    cat: &TenantCatalog,
    tenant: TenantId,
    database: &str,
) -> bool {
    let lctn = lctn(svc, cat);
    let on_engine = crate::engine::catalog::of(cat).is_some_and(|eng| {
        cat.database(&database_key(database, lctn))
            .is_some_and(|d| eng.dbs.contains(&d.db_id))
    });
    on_engine
        || svc
            .locator
            .cluster(tenant)
            .await
            .is_ok_and(|c| c.as_str() == crate::tenant::locator::ENGINE_ONLY_CLUSTER)
}

fn lctn(svc: &crate::tenant::TenantServices, cat: &TenantCatalog) -> bool {
    svc.settings.get(
        &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1
}

/// The API's HLC of commit timestamp `cts` (milliseconds << 16 | counter):
/// seconds << 32 | (millisecond << 16 | counter).
#[must_use]
pub(crate) fn hlc_of(cts: u64) -> u64 {
    let ms = cts >> 16;
    let rest = (ms % 1000) << 16 | (cts & 0xFFFF);
    (ms / 1000) << 32 | rest
}

/// The first commit timestamp at or after the API's HLC `hlc` (the
/// inverse of [`hlc_of`]; a stamp counting within a second, as the
/// MariaDB feed's are, starts at the beginning of its second).
#[must_use]
pub(crate) fn cts_of_hlc(hlc: u64) -> u64 {
    let secs = hlc >> 32;
    let low = hlc & 0xFFFF_FFFF;
    let (ms, counter) = if low >> 16 < 1000 {
        (low >> 16, low & 0xFFFF)
    } else {
        (0, 0)
    };
    (secs.saturating_mul(1000).saturating_add(ms)) << 16 | counter
}

fn bad_token(m: &str) -> SprigError {
    SprigError::new(
        SprigCode::FeedBadResumeToken,
        format!("ER_SPRIG_FEED_BAD_RESUME_TOKEN: {m}"),
    )
}

fn history_lost(m: &str) -> SprigError {
    SprigError::new(
        SprigCode::FeedHistoryLost,
        format!("ER_SPRIG_FEED_HISTORY_LOST: {m}"),
    )
}

/// Opens a watch of an engine database (see the module docs); the
/// privilege checks of the requested tables already ran.
///
/// # Errors
/// 39801 for a token that isn't this tenant's change-log token, or whose
/// place the log no longer holds; 39800 when `start_at_hlc`'s place is
/// gone; 39802 when the node's change log isn't open.
pub(super) async fn open(
    core: &Arc<Core>,
    caller: Caller,
    spec: WatchSpec,
    limits: Limits,
    slot: Slot,
    tenant: TenantId,
) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
    let (Some(log), Ok(raw)) = (crate::changelog::changes(), crate::node_engine::engine()) else {
        return Err(SprigError::new(
            SprigCode::FeedSourceUnsupported,
            "ER_SPRIG_FEED_SOURCE_UNSUPPORTED: change streams on engine databases need the \
             node's change log, which is not open (SPRIGDB_ENGINE_CHANGELOG)",
        ));
    };
    let unit = sprig_engine::Engine::unit_of_tenant(tenant.0).0;
    let mut from_cts = 0u64;
    let (pos, hold, token) = if !spec.start_after.is_empty() {
        let t = Token::from_hex(spec.start_after.trim())
            .ok_or_else(|| bad_token("not a change-log token of an engine database"))?;
        if t.tenant != tenant.0 {
            return Err(bad_token("the resume token is another tenant's"));
        }
        (t.pos(), t.cts.max(1), true)
    } else if spec.start_at_hlc != 0 {
        from_cts = cts_of_hlc(spec.start_at_hlc);
        (log.place_at(unit, from_cts), from_cts.max(1), false)
    } else {
        // Now: after every commit acknowledged before the stream opened
        // (the change log writes on the apply step; wait for it briefly).
        let want = raw.consensus().durable_index(sprig_engine::UnitId(unit));
        let mut w = log.watch(unit);
        let until = tokio::time::Instant::now() + Duration::from_secs(2);
        while log.head(unit).0.index < want && tokio::time::Instant::now() < until {
            let _ = tokio::time::timeout_at(until, w.changed()).await;
        }
        let (p, c) = log.head(unit);
        (p, c.max(raw.txn_shared().visible_ts().0), false)
    };
    let reader = log.reader(unit, pos, Some(hold)).map_err(|e| {
        if !sprig_streams::changes::is_history_lost(&e) {
            e
        } else if token {
            bad_token("the change log no longer holds the token's place; start again")
        } else {
            history_lost("start_at_hlc is older than the change log keeps; start later")
        }
    })?;
    let eng = crate::engine::engine()?;
    let svc = core
        .engine
        .tenants
        .clone()
        .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
    // Parent tenants whose inherited records the tenant's inheritable
    // tables read: their change logs from now.
    let mut sources = vec![Source {
        tenant,
        own: true,
        reader: Some(reader),
        tables: Tables::default(),
        at_end: false,
    }];
    for p in parent_line(&svc, tenant, &spec.database).await {
        let pu = sprig_engine::Engine::unit_of_tenant(p.0).0;
        match log.reader(pu, log.head(pu).0, None) {
            Ok(r) => sources.push(Source {
                tenant: p,
                own: false,
                reader: Some(r),
                tables: Tables::default(),
                at_end: false,
            }),
            Err(e) => {
                tracing::warn!(tenant = %p, error = %e, "a parent tenant's change log can't be read for a change stream");
            }
        }
    }
    let (tx, rx) = mpsc::channel(limits.buffer.max(16) + 1);
    let core = Arc::clone(core);
    let space = spec.space.clone();
    let stream = Stream {
        core,
        caller,
        spec,
        svc,
        log,
        eng,
        tenant,
        from_cts,
        heartbeat: limits.heartbeat,
        last: Token::at(tenant.0, hold, pos, true).to_hex(),
        held: Vec::new(),
        readable: HashSet::new(),
        known: inherit::Seen::default(),
    };
    tokio::spawn(super::super::sql::SPACE.scope(space, async move {
        let _slot = slot;
        metrics::gauge!("sprig_api_change_streams").increment(1.0);
        let r = stream.run(sources, &tx).await;
        if let Err(e) = r {
            let _ = tx.try_send(Err(e));
        }
        metrics::gauge!("sprig_api_change_streams").decrement(1.0);
    }));
    Ok(rx)
}

/// The parent tenants of `tenant` whose records its inheritable tables of
/// `database` read through, nearest first, while an edge continues (as the
/// MariaDB feed's `inherit::ancestor_tails` picks them). Empty when the
/// tenant inherits nothing there.
async fn parent_line(
    svc: &crate::tenant::TenantServices,
    tenant: TenantId,
    database: &str,
) -> Vec<TenantId> {
    let Ok(cat) = svc
        .cache
        .get(tenant, sprig_catalog::Freshness::Latest)
        .await
    else {
        return Vec::new();
    };
    let Some(st) = cat
        .ext
        .get::<sprig_inherit::InheritState>(sprig_inherit::MODULE)
    else {
        return Vec::new();
    };
    if st.objects().next().is_none() || cat.ancestry.is_empty() {
        return Vec::new();
    }
    let mut line: Vec<Arc<TenantCatalog>> = Vec::new();
    for a in cat.ancestry.iter() {
        match svc.cache.get(a.id, sprig_catalog::Freshness::Latest).await {
            Ok(c) => line.push(c),
            Err(_) => return Vec::new(),
        }
    }
    line.push(Arc::clone(&cat));
    let mut out = Vec::new();
    let mut j = line.len() - 1;
    while j >= 1 {
        let (parent, child) = (&line[j - 1], &line[j]);
        let Some(cs) = child
            .ext
            .get::<sprig_inherit::InheritState>(sprig_inherit::MODULE)
        else {
            break;
        };
        let edge = cs.objects().any(|o| {
            child.databases.get(o.db).is_some_and(|d| {
                d.name.eq_ignore_ascii_case(database)
                    && matches!(
                        sprig_inherit::plan::tenant_edge(parent, child, &d.name, &o.table, false),
                        Ok(Some(_))
                    )
            })
        });
        if !edge {
            break;
        }
        out.push(parent.tenant);
        j -= 1;
    }
    out
}

/// The tables of one tenant's database as the stream knows them.
#[derive(Default)]
struct Tables {
    /// Watched tables by object, with how their changes are shown.
    known: HashMap<u32, (Watched, Kind)>,
    /// Objects that aren't the stream's (other databases' tables, tables
    /// left out by `objects`, internal tables, documents' collections).
    ignored: HashSet<u32>,
    /// Watched tables the subscriber could read (`SELECT` and `WATCH`) at
    /// the last reload: a dropped table's drop is shown by this, as the
    /// table can't be checked any more (the subscriber's tenant only).
    readable: HashSet<String>,
    /// The catalog version they were read at.
    version: u64,
}

/// One tenant's change log the stream reads: its own, or a parent
/// tenant's whose inherited records its tables read through (from the
/// stream's start; a parent's changes made while no stream was open are
/// not sent, as on MariaDB databases).
struct Source {
    tenant: TenantId,
    own: bool,
    reader: Option<Reader>,
    tables: Tables,
    at_end: bool,
}

/// One change before the subscriber's check (a batch holds at most a
/// page's worth, briefly).
#[allow(clippy::large_enum_variant)]
enum Entry {
    /// An ordinary table's event, with its row's images.
    Plain(WatchEvent, Option<Value>, Option<Value>),
    /// An inheritable table's or copy's change, for the mapping (`own`:
    /// of the subscriber's tenant), and the token it resumes at.
    Mapped(Box<sprig_feed::ChangeEvent>, bool, String),
}

/// What waits for the subscriber's check.
enum Wait {
    Plain {
        ev: WatchEvent,
        /// The row after, for an update that becomes an insert.
        after: Option<Value>,
    },
    Mapped {
        fe: Box<sprig_feed::ChangeEvent>,
        m: inherit::Mapped,
        token: String,
        /// Sent by its copy's handover, whatever the view reads now.
        counted: bool,
    },
}

/// One event in the subscriber's check: its table and images there.
struct Pending {
    wait: Wait,
    table: usize,
    before: Option<usize>,
    after: Option<usize>,
    probe: Option<usize>,
}

/// What a page holds for the stream.
enum Item {
    /// A row change of a table.
    Row(Raw),
    /// A new schema version of table `object`.
    Ddl {
        object: u32,
        ddl: String,
        change: Change,
    },
    /// Table `object` was dropped (its object row turned `dropped`, or
    /// went).
    Dropped {
        object: u32,
        name: String,
        change: Change,
    },
    /// Table `object` was emptied by `TRUNCATE` (its layout record moved
    /// to a new row generation, marked truncated).
    Truncated { object: u32, change: Change },
}

impl Item {
    fn object(&self) -> u32 {
        match self {
            Self::Row(r) => r.object,
            Self::Ddl { object, .. }
            | Self::Dropped { object, .. }
            | Self::Truncated { object, .. } => *object,
        }
    }
}

/// One page of the log and what in it may be the stream's.
fn page(
    log: &ChangeLog,
    reader: &mut Reader,
    tenant: u64,
    want: &dyn Fn(u32) -> bool,
) -> Result<(Page, Vec<Item>)> {
    let mut p = reader.read(PAGE_BYTES)?;
    let mut out = Vec::new();
    // Each table's row generation as of the change being looked at (see
    // `table_changes`): rows a table rewrite copies aren't changes.
    let mut gens: HashMap<u32, u16> = HashMap::new();
    for c in std::mem::take(&mut p.changes) {
        let Some((pre, user)) = c.prefix() else {
            continue;
        };
        if pre.tenant != tenant || pre.shard != 0 {
            continue;
        }
        if pre.object == 0 {
            if pre.kind == kind::META
                && let Some(it) = catalog_item(log, &c, user, want)?
            {
                out.push(it);
            }
            continue;
        }
        if !want(pre.object) {
            continue;
        }
        if pre.kind == kind::META {
            if user == b"layout"
                && let Some(l) = c
                    .value
                    .as_deref()
                    .and_then(crate::engine::layout::LayoutRecord::decode)
            {
                gens.insert(pre.object, l.row_gen);
                let was = log
                    .before_image(&c)?
                    .as_deref()
                    .and_then(crate::engine::layout::LayoutRecord::decode)
                    .map_or(0, |o| o.truncated);
                if l.truncated != was && l.truncated != 0 {
                    out.push(Item::Truncated {
                        object: pre.object,
                        change: c.clone(),
                    });
                }
            }
            continue;
        }
        if pre.kind != kind::PRIMARY {
            continue;
        }
        let g = if let Some(g) = gens.get(&pre.object) {
            *g
        } else {
            let g = table_changes::gen_at(log, tenant, pre.object, c.cts.saturating_sub(1))?;
            gens.insert(pre.object, g);
            g
        };
        if pre.index != g {
            continue;
        }
        let before = log.before_image(&c)?;
        out.push(Item::Row(Raw {
            object: pre.object,
            change: c,
            before,
        }));
    }
    Ok((p, out))
}

/// The object number in a catalog record's key after `prefix`.
fn object_after(user: &[u8], prefix: &[u8]) -> Option<u32> {
    let b: [u8; 8] = user.get(prefix.len()..prefix.len() + 8)?.try_into().ok()?;
    u32::try_from(u64::from_be_bytes(b)).ok()
}

/// A catalog record's change as the stream's item: a table's new schema
/// version, or its drop.
fn catalog_item(
    log: &ChangeLog,
    c: &Change,
    user: &[u8],
    want: &dyn Fn(u32) -> bool,
) -> Result<Option<Item>> {
    if user.starts_with(SCHEMA_VERSIONS) {
        let Some(object) = object_after(user, SCHEMA_VERSIONS).filter(|o| want(*o)) else {
            return Ok(None);
        };
        let Some(v) = c.value.as_deref() else {
            return Ok(None);
        };
        let ddl = serde_json::from_slice::<Value>(v)
            .ok()
            .and_then(|j| j.get("ddl").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default();
        return Ok(Some(Item::Ddl {
            object,
            ddl,
            change: c.clone(),
        }));
    }
    if user.starts_with(OBJECTS) {
        let Some(object) = object_after(user, OBJECTS).filter(|o| want(*o)) else {
            return Ok(None);
        };
        let row = |b: Option<&[u8]>| b.and_then(|b| serde_json::from_slice::<Value>(b).ok());
        let state = |j: &Option<Value>| {
            j.as_ref()
                .and_then(|j| j.get("state").and_then(Value::as_str).map(str::to_owned))
        };
        let after = row(c.value.as_deref());
        if after.is_some() && state(&after).as_deref() != Some("dropped") {
            return Ok(None);
        }
        let before = row(log.before_image(c)?.as_deref());
        if before.is_none() || state(&before).as_deref() == Some("dropped") {
            return Ok(None);
        }
        let j = before.as_ref().or(after.as_ref());
        if j.and_then(|j| j.get("kind").and_then(Value::as_str)) != Some("table") {
            return Ok(None);
        }
        let name = j
            .and_then(|j| j.get("name").and_then(Value::as_str))
            .unwrap_or_default()
            .to_owned();
        return Ok(Some(Item::Dropped {
            object,
            name,
            change: c.clone(),
        }));
    }
    Ok(None)
}

/// An open stream.
struct Stream {
    core: Arc<Core>,
    caller: Caller,
    spec: WatchSpec,
    svc: Arc<crate::tenant::TenantServices>,
    log: ChangeLog,
    eng: &'static crate::engine::Engine,
    tenant: TenantId,
    /// Changes committed before it are left out (`start_at_hlc`).
    from_cts: u64,
    heartbeat: Duration,
    /// The token after everything of the tenant's own log read so far: a
    /// parent tenant's change resumes there.
    last: String,
    /// Inherited changes waiting for a copy's first seed to end.
    held: Vec<(Box<sprig_feed::ChangeEvent>, inherit::Mapped, String)>,
    /// What the stream last knew of inherited records' visibility.
    known: inherit::Seen,
    /// The watched tables the subscriber could read at the last reload.
    readable: HashSet<String>,
}

/// Parent tenants' pages are read again this often while the own log is
/// quiet (their changes don't wake the stream).
const PARENT_POLL: Duration = Duration::from_millis(100);

impl Stream {
    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn run(
        mut self,
        mut sources: Vec<Source>,
        tx: &mpsc::Sender<Result<WatchEvent>>,
    ) -> Result<()> {
        let unit = sources[0].reader.as_ref().map_or(0, Reader::unit);
        let tenant = self.tenant.0;
        let mut watch = self.log.watch(unit);
        let mut quiet = tokio::time::Instant::now();
        for s in &mut sources {
            self.refresh(s).await?;
        }
        self.readable.clone_from(&sources[0].tables.readable);
        loop {
            watch.borrow_and_update();
            let mut entries = Vec::new();
            let mut own_page: Option<Page> = None;
            for s in &mut sources {
                let Some(mut reader) = s.reader.take() else {
                    continue;
                };
                let lg = self.log.clone();
                let want: HashSet<u32> = s.tables.ignored.clone();
                let t = s.tenant.0;
                let res = tokio::task::spawn_blocking(move || {
                    let p = page(&lg, &mut reader, t, &|o| !want.contains(&o));
                    (reader, p)
                })
                .await
                .map_err(|e| SprigError::internal(format!("change log read: {e}")))?;
                s.reader = Some(res.0);
                let (pg, items) = match res.1 {
                    Ok(x) => x,
                    Err(e) if s.own => {
                        return Err(if sprig_streams::changes::is_history_lost(&e) {
                            history_lost(
                                "the change log no longer holds the stream's place (retention); \
                                 start again",
                            )
                        } else {
                            e
                        });
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %s.tenant, error = %e, "a parent tenant's change log stopped for a change stream");
                        s.reader = None;
                        continue;
                    }
                };
                // Objects first seen: the catalog as of now says what they are.
                if items
                    .iter()
                    .any(|i| !s.tables.known.contains_key(&i.object()))
                    && let Err(e) = self.refresh(s).await
                {
                    tracing::warn!(error = %e, "a change stream couldn't reload its tables");
                }
                if s.own {
                    self.readable.clone_from(&s.tables.readable);
                }
                s.at_end = pg.at_end;
                for it in items {
                    if let Some(e) = self.entry(s, it).await? {
                        entries.push(e);
                    }
                }
                if s.own {
                    own_page = Some(pg);
                }
            }
            if !entries.is_empty() || !self.held.is_empty() {
                let mut rest = entries.as_slice();
                loop {
                    let n = rest.len().min(DEPTH);
                    let (chunk, more) = rest.split_at(n);
                    match self.process(chunk, tx).await? {
                        None => return Ok(()),
                        Some(sent) if sent > 0 => quiet = tokio::time::Instant::now(),
                        Some(_) => {}
                    }
                    rest = more;
                    if rest.is_empty() {
                        break;
                    }
                }
            }
            if let Some(pg) = own_page {
                self.last = Token::at(tenant, pg.cts, pg.pos, true).to_hex();
            }
            if sources.iter().any(|s| s.reader.is_some() && !s.at_end) {
                continue;
            }
            // At the end of the logs: the next apply, or a heartbeat (also
            // when other objects' changes keep the log moving).
            if tokio::time::Instant::now() >= quiet + self.heartbeat {
                let hb = WatchEvent {
                    resume_token: self.last.clone(),
                    op: "heartbeat".into(),
                    ..WatchEvent::default()
                };
                if !self.send(hb, tx).await {
                    return Ok(());
                }
                quiet = tokio::time::Instant::now();
            }
            let mut until = quiet + self.heartbeat;
            if sources.len() > 1 {
                until = until.min(tokio::time::Instant::now() + PARENT_POLL);
            }
            if !self.held.is_empty() {
                until = until.min(tokio::time::Instant::now() + Duration::from_millis(250));
            }
            tokio::select! {
                () = tx.closed() => return Ok(()),
                _ = tokio::time::timeout_at(until, watch.changed()) => {}
            }
        }
    }

    /// Reloads source `s`'s tables of the watched database from its
    /// tenant's catalog as of now.
    #[allow(clippy::too_many_lines)] // the tables by kind, then the subscriber's reads
    async fn refresh(&self, s: &mut Source) -> Result<()> {
        let cat = self
            .svc
            .cache
            .get(s.tenant, sprig_catalog::Freshness::Latest)
            .await?;
        if cat.version == s.tables.version && s.tables.version != 0 {
            return Ok(());
        }
        let lctn = lctn(&self.svc, &cat);
        let mut t = Tables {
            version: cat.version,
            ..Tables::default()
        };
        let db = cat
            .database(&database_key(&self.spec.database, lctn))
            .map(|d| (d.db_id, d.name.clone()));
        let st = cat
            .ext
            .get::<sprig_inherit::InheritState>(sprig_inherit::MODULE);
        let named = |n: &str| {
            self.spec.objects.is_empty()
                || self.spec.objects.iter().any(|o| o.eq_ignore_ascii_case(n))
        };
        if let Some(ecat) = crate::engine::catalog::of(&cat) {
            for (oid, p) in &ecat.tables {
                let Ok(object) = u32::try_from(oid.0) else {
                    continue;
                };
                let def = p.as_ref().as_ref().ok().cloned();
                let kind = match (&def, &db) {
                    (Some(d), Some((id, _))) if d.db == *id => {
                        let copy_of = d
                            .name
                            .strip_prefix("__sprig_ih_")
                            .and_then(|n| n.parse::<u64>().ok());
                        match copy_of {
                            // A copy of an inherited table: its object's
                            // name decides, mapped later.
                            Some(id) => st
                                .and_then(|st| st.object(ObjectId(id)))
                                .map(|_| Kind::Copy),
                            None if d.name.starts_with("__sprig")
                                || d.name.starts_with("_sprig") =>
                            {
                                None
                            }
                            None if st
                                .is_some_and(|st| st.object_named_ci(*id, &d.name).is_some()) =>
                            {
                                (!s.own || named(&d.name)).then_some(Kind::Inherit)
                            }
                            // A parent's ordinary table isn't the stream's.
                            None => (s.own && named(&d.name)).then_some(Kind::Plain),
                        }
                    }
                    _ => None,
                };
                match (kind, def, &db) {
                    (Some(k), Some(def), Some((_, name))) => {
                        t.known.insert(
                            object,
                            (
                                Watched {
                                    def,
                                    db: name.clone(),
                                },
                                k,
                            ),
                        );
                    }
                    _ => {
                        t.ignored.insert(object);
                    }
                }
            }
        }
        // Objects seen before that are neither (collections, other
        // models' objects) stay ignored; a table they turn out to be is
        // known now. A watched table gone from the catalog (dropped) stays
        // watched: its drop and last changes may still be ahead in the log.
        for o in &s.tables.ignored {
            if !t.known.contains_key(o) && !ecat_has(&cat, *o) {
                t.ignored.insert(*o);
            }
        }
        for (o, w) in std::mem::take(&mut s.tables.known) {
            if !ecat_has(&cat, o) {
                t.ignored.remove(&o);
                t.known.insert(o, w);
            }
        }
        // What the subscriber may read now, for drops later.
        if s.own {
            let mut req = ImagesRequest {
                watch: true,
                ..ImagesRequest::default()
            };
            let table_names: Vec<String> = t
                .known
                .values()
                .filter(|(_, k)| *k != Kind::Copy)
                .map(|(w, _)| w.def.name.clone())
                .collect();
            for n in &table_names {
                req.tables.push(TableImages {
                    db: self.spec.database.clone(),
                    table: n.clone(),
                    images: vec![None],
                });
            }
            if !table_names.is_empty() {
                let seen = check(&self.core, &self.caller, &req).await?;
                for (n, v) in table_names.iter().zip(&seen) {
                    if v.first().copied().unwrap_or(false) {
                        t.readable.insert(n.to_lowercase());
                    }
                }
            }
            // A dropped table keeps what it had.
            for n in &s.tables.readable {
                if !table_names.iter().any(|x| x.eq_ignore_ascii_case(n)) {
                    t.readable.insert(n.clone());
                }
            }
        }
        s.tables = t;
        Ok(())
    }

    /// Item `it` of source `s` as an entry of the stream, before the
    /// subscriber's check; `None` when it isn't the stream's.
    async fn entry(&self, s: &mut Source, it: Item) -> Result<Option<Entry>> {
        let object = it.object();
        let Some((w, kind)) = s.tables.known.get(&object).cloned() else {
            // Still unknown after the reload: not a table of the database.
            s.tables.ignored.insert(object);
            return Ok(None);
        };
        let (change, op, ddl) = match it {
            Item::Row(r) => {
                if r.change.cts < self.from_cts {
                    return Ok(None);
                }
                let Some(e) = table_changes::event(self.eng, s.tenant.0, &w, &r).await? else {
                    return Ok(None);
                };
                if kind != Kind::Plain {
                    let token = if s.own {
                        e.token.clone()
                    } else {
                        self.last.clone()
                    };
                    return Ok(feed_event(s.tenant, &w, kind, &e, r.change.cts)
                        .map(|fe| Entry::Mapped(Box::new(fe), s.own, token)));
                }
                let ev = WatchEvent {
                    resume_token: e.token.clone(),
                    commit_hlc: hlc_of(r.change.cts),
                    object: w.def.name.clone(),
                    op: match e.op {
                        "c" => "insert",
                        "u" => "update",
                        _ => "delete",
                    }
                    .into(),
                    key: e.key.to_string(),
                    ..WatchEvent::default()
                };
                return Ok(Some(Entry::Plain(ev, e.before, e.after)));
            }
            // A copy's or a parent's definition changes aren't the
            // subscriber's.
            _ if kind == Kind::Copy || !s.own => return Ok(None),
            Item::Ddl { ddl, change, .. } => {
                let op = if first_word(&ddl).eq_ignore_ascii_case("truncate") {
                    "truncate"
                } else {
                    "ddl"
                };
                (change, op, ddl)
            }
            Item::Truncated { change, .. } => {
                let ddl = format!(
                    "TRUNCATE TABLE `{}`.`{}`",
                    w.db.replace('`', "``"),
                    w.def.name.replace('`', "``")
                );
                (change, "truncate", ddl)
            }
            Item::Dropped { name, change, .. } => {
                let ddl = format!(
                    "DROP TABLE `{}`.`{}`",
                    w.db.replace('`', "``"),
                    name.replace('`', "``")
                );
                (change, "ddl", ddl)
            }
        };
        if change.cts < self.from_cts {
            return Ok(None);
        }
        let ev = WatchEvent {
            resume_token: Token::at(self.tenant.0, change.cts, change.pos(), false).to_hex(),
            commit_hlc: hlc_of(change.cts),
            object: w.def.name.clone(),
            op: op.into(),
            ddl,
            ..WatchEvent::default()
        };
        Ok(Some(Entry::Plain(ev, None, None)))
    }

    /// Checks `entries` (and the changes held for a copy's seed) as the
    /// subscriber and sends what it may see, in order. `None`: the stream
    /// ended (the client is gone or too slow); else the events sent.
    #[allow(clippy::too_many_lines)] // one batch's mapping, check and sends, in order
    async fn process(
        &mut self,
        entries: &[Entry],
        tx: &mpsc::Sender<Result<WatchEvent>>,
    ) -> Result<Option<usize>> {
        let mut req = ImagesRequest {
            watch: true,
            ..ImagesRequest::default()
        };
        let mut tables: HashMap<String, usize> = HashMap::new();
        let mut pending: Vec<Pending> = Vec::with_capacity(entries.len());
        let db = self.spec.database.clone();
        let mut slot = |req: &mut ImagesRequest, table: &str| {
            *tables.entry(table.to_lowercase()).or_insert_with(|| {
                req.tables.push(TableImages {
                    db: db.clone(),
                    table: table.to_owned(),
                    images: Vec::new(),
                });
                req.tables.len() - 1
            })
        };
        let push = |req: &mut ImagesRequest, t: usize, img: Option<Map<String, Value>>| {
            let imgs = &mut req.tables[t].images;
            imgs.push(img);
            imgs.len() - 1
        };
        let obj = |v: &Option<Value>| match v {
            Some(Value::Object(m)) => Some(m.clone()),
            _ => None,
        };
        // Inheritable objects' changes become the subscriber's space's.
        let mapper =
            if !self.held.is_empty() || entries.iter().any(|e| matches!(e, Entry::Mapped(..))) {
                inherit::Mapper::load(&self.core, &self.caller).await
            } else {
                None
            };
        let mut inherited: Vec<(Box<sprig_feed::ChangeEvent>, inherit::Mapped, String, bool)> =
            std::mem::take(&mut self.held)
                .into_iter()
                .map(|(fe, m, token)| (fe, m, token, false))
                .collect();
        for e in entries {
            match e {
                Entry::Plain(ev, before, after) => {
                    let t = slot(&mut req, &ev.object);
                    let mut p = Pending {
                        wait: Wait::Plain {
                            ev: ev.clone(),
                            after: after.clone(),
                        },
                        table: t,
                        before: None,
                        after: None,
                        probe: None,
                    };
                    let Wait::Plain { ev: pev, .. } = &mut p.wait else {
                        continue;
                    };
                    match ev.op.as_str() {
                        "insert" => {
                            p.after = Some(push(&mut req, t, obj(after)));
                            pev.after = after.as_ref().map(Value::to_string).unwrap_or_default();
                        }
                        // A delete whose before-image the version GC dropped:
                        // its key's columns stand for the row.
                        "delete" => {
                            let key = serde_json::from_str::<Value>(&ev.key).ok();
                            p.before = Some(push(&mut req, t, obj(before).or_else(|| obj(&key))));
                        }
                        "update" => {
                            p.before = Some(push(&mut req, t, obj(before)));
                            p.after = Some(push(&mut req, t, obj(after)));
                            pev.update = changed(before.as_ref(), after.as_ref()).to_string();
                            if self.spec.full_document {
                                pev.after =
                                    after.as_ref().map(Value::to_string).unwrap_or_default();
                            }
                        }
                        _ => p.probe = Some(push(&mut req, t, None)),
                    }
                    pending.push(p);
                }
                Entry::Mapped(fe, own, token) => {
                    let Some(m) = &mapper else { continue };
                    let mut fe = fe.clone();
                    match m.classify(&mut fe, *own) {
                        inherit::Class::Inherit(x) => {
                            if super::wanted(&self.spec, &fe) {
                                inherited.push((fe, x, token.clone(), false));
                            }
                        }
                        inherit::Class::Plain | inherit::Class::Drop => {}
                    }
                }
            }
        }
        // Changes of a tenant edge's source or copy: the copies' handovers
        // decide which side sends each, read now that the batch is here.
        if let Some(m) = &mapper {
            let mut edges: HashMap<String, Vec<Option<sprig_inherit::handover::Handover>>> =
                HashMap::new();
            for (fe, mp, token, mut counted) in inherited {
                if mp.origin != inherit::Origin::Inside {
                    let t = fe.table.to_lowercase();
                    if !edges.contains_key(&t) {
                        let h = m
                            .engine_handovers(&self.svc, &self.spec.database, &fe.table)
                            .await;
                        edges.insert(t.clone(), h);
                    }
                    match m.verdict(&mp, &edges[&t]) {
                        Some(sprig_inherit::handover::Verdict::Drop) => continue,
                        Some(sprig_inherit::handover::Verdict::Hold) => {
                            self.held.push((fe, mp, token));
                            continue;
                        }
                        Some(sprig_inherit::handover::Verdict::Count) => counted = true,
                        Some(sprig_inherit::handover::Verdict::Unknown) | None => {}
                    }
                }
                let t = slot(&mut req, &fe.table);
                let probe = Some(push(&mut req, t, None));
                pending.push(Pending {
                    wait: Wait::Mapped {
                        fe,
                        m: mp,
                        token,
                        counted,
                    },
                    table: t,
                    before: None,
                    after: None,
                    probe,
                });
            }
        }
        if pending.is_empty() {
            return Ok(Some(0));
        }
        let seen = check(&self.core, &self.caller, &req).await?;
        let v = |p: &Pending, i: Option<usize>| {
            i.is_some_and(|i| seen[p.table].get(i).copied().unwrap_or(false))
        };
        // The merged records, read now in the subscriber's space.
        let keys: Vec<(String, Map<String, Value>)> = pending
            .iter()
            .filter(|p| v(p, p.probe))
            .filter_map(|p| match &p.wait {
                Wait::Mapped { fe, m, .. } => Some((fe.table.clone(), m.key.clone())),
                Wait::Plain { .. } => None,
            })
            .collect();
        let records = if keys.is_empty() {
            HashMap::new()
        } else {
            inherit::read(&self.core, &self.caller, &self.spec.database, &keys).await?
        };
        if let Some(m) = &mapper {
            self.known.at(m.version());
        }
        let mut probes = crate::exec::inherit_probe::ProbeRequest {
            db: self.spec.database.clone(),
            probes: Vec::new(),
        };
        let mut probe_of: HashMap<usize, (usize, bool)> = HashMap::new();
        // A key's later changes in this batch go by what its earlier one
        // left in `known`: each probe puts back only its own change's row,
        // so a second probe of the key would see the first change undone.
        let mut batch_keys: HashSet<String> = HashSet::new();
        for (j, p) in pending.iter().enumerate() {
            let Wait::Mapped { fe, m, counted, .. } = &p.wait else {
                continue;
            };
            if !v(p, p.probe) {
                continue;
            }
            let id = inherit::key_id(&fe.table, &m.key);
            let again = !batch_keys.insert(id.clone());
            let mut item = inherit::probe_item(fe, m);
            item.reads_only = again || records.contains_key(&id) || self.known.get(&id).is_some();
            item.counted = *counted;
            probe_of.insert(j, (probes.probes.len(), item.reads_only));
            probes.probes.push(item);
        }
        let before = if probes.probes.is_empty() {
            Vec::new()
        } else {
            match inherit::probe(&self.core, &self.caller, &probes).await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error = %e, "a change stream's visibility probes failed");
                    vec![Some(false); probes.probes.len()]
                }
            }
        };
        let mut sent = 0;
        for (j, p) in pending.iter().enumerate() {
            let ev = match &p.wait {
                Wait::Plain { ev, after } => {
                    let mut ev = ev.clone();
                    let show = match ev.op.as_str() {
                        "insert" => v(p, p.after),
                        "delete" => v(p, p.before),
                        "update" => match (v(p, p.before), v(p, p.after)) {
                            (true, true) => true,
                            (true, false) => {
                                ev.op = "delete".into();
                                ev.after.clear();
                                ev.update.clear();
                                true
                            }
                            (false, true) => {
                                ev.op = "insert".into();
                                ev.update.clear();
                                ev.after = after.as_ref().map(Value::to_string).unwrap_or_default();
                                true
                            }
                            (false, false) => false,
                        },
                        // A dropped table can't be checked: what the
                        // subscriber could read at the last reload decides.
                        _ if ev.op == "ddl" && ev.ddl.starts_with("DROP TABLE ") => {
                            v(p, p.probe) || self.readable_now(&ev.object)
                        }
                        _ => v(p, p.probe),
                    };
                    show.then_some(ev)
                }
                Wait::Mapped { fe, m, token, .. } => {
                    let Some(mapper) = &mapper else { continue };
                    if !v(p, p.probe) {
                        continue;
                    }
                    let id = inherit::key_id(&fe.table, &m.key);
                    let now = records.contains_key(&id);
                    // As on MariaDB databases: a change the view doesn't
                    // read sends nothing; else probed, else known, else
                    // (present now) unknown.
                    let asked = probe_of
                        .get(&j)
                        .map(|(i, only)| (before.get(*i).copied().unwrap_or(Some(false)), *only));
                    let was = match asked {
                        Some((None, _)) => None,
                        Some((Some(x), false)) => Some(Some(x)),
                        _ => Some(self.known.get(&id)),
                    };
                    let Some(was) = was else { continue };
                    self.known.set(id.clone(), now);
                    let mut out = WatchEvent {
                        resume_token: token.clone(),
                        commit_hlc: fe.hlc,
                        object: fe.table.clone(),
                        ..WatchEvent::default()
                    };
                    let rec = records.get(&id);
                    inherit::decide(mapper, fe, m, rec, was, self.spec.full_document, &mut out)
                        .then_some(out)
                }
            };
            if let Some(ev) = ev {
                if !self.send(ev, tx).await {
                    return Ok(None);
                }
                sent += 1;
            }
        }
        metrics::counter!("sprig_api_change_events_sent_total").increment(sent as u64);
        Ok(Some(sent))
    }

    /// Whether table `name` was readable at the last reload (kept by the
    /// own source; set by [`Stream::run`] before each batch).
    fn readable_now(&self, name: &str) -> bool {
        self.readable.contains(&name.to_lowercase())
    }

    /// Sends `ev`, waiting while the client's buffer is full; a client
    /// whose buffer stays full for four heartbeat periods gets 39790 in
    /// the slot kept for it. `false`: the stream ends.
    async fn send(&self, ev: WatchEvent, tx: &mpsc::Sender<Result<WatchEvent>>) -> bool {
        let until = tokio::time::Instant::now() + self.heartbeat.saturating_mul(4);
        loop {
            if tx.is_closed() {
                return false;
            }
            if tx.capacity() > 1 {
                return tx.try_send(Ok(ev)).is_ok();
            }
            if tokio::time::Instant::now() >= until {
                return super::deliver(tx, ev);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Whether the engine catalog has table `object`.
fn ecat_has(cat: &TenantCatalog, object: u32) -> bool {
    crate::engine::catalog::of(cat)
        .is_some_and(|e| e.tables.contains_key(&ObjectId(u64::from(object))))
}

/// The columns an update changed, with their new values.
fn changed(before: Option<&Value>, after: Option<&Value>) -> Value {
    let Some(Value::Object(a)) = after else {
        return Value::Object(Map::new());
    };
    let b = match before {
        Some(Value::Object(b)) => Some(b),
        _ => None,
    };
    Value::Object(
        a.iter()
            .filter(|(k, v)| b.is_none_or(|b| b.get(*k) != Some(*v)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )
}

/// The first word of a statement, past spaces and comments.
fn first_word(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(r) = s.strip_prefix("/*") {
            s = r.split_once("*/").map_or("", |(_, r)| r).trim_start();
        } else if let Some(r) = s.strip_prefix("--").or_else(|| s.strip_prefix('#')) {
            s = r.split_once('\n').map_or("", |(_, r)| r).trim_start();
        } else {
            break;
        }
    }
    let end = s
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(s.len());
    &s[..end]
}

// ---------------------------------------------------------------------
// Inheritable tables (data inheritance): their changes as the subscriber's
// merged view, as on MariaDB databases (`inherit`).

/// How a watched table's changes reach the subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// An ordinary table: its rows, checked as the subscriber.
    Plain,
    /// An inheritable table: rows of its spaces' layers, mapped to the
    /// subscriber's merged view.
    Inherit,
    /// A materialized copy of an inherited table (`__sprig_ih_<object>`).
    Copy,
}

/// The JSON value as the feed's value (what `image_json` gives back).
fn feed_value(v: &Value) -> sprig_binlog::Value {
    use sprig_binlog::Value as B;
    match v {
        Value::Null => B::Null,
        Value::Bool(b) => B::Int(i64::from(*b)),
        Value::Number(n) => n
            .as_i64()
            .map(B::Int)
            .or_else(|| n.as_u64().map(B::UInt))
            .unwrap_or_else(|| B::Double(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => B::String(s.clone().into_bytes()),
        other => B::String(other.to_string().into_bytes()),
    }
}

/// A row image as the feed's: every column, the layer and copy columns
/// (`_sprig_space`, `_sprig_state`, `_sprig_live`, ...) too, which the
/// mapping and its probes read.
fn feed_image(v: &Value) -> sprig_feed::Image {
    let mut out = Vec::new();
    if let Value::Object(m) = v {
        for (k, x) in m {
            out.push((k.clone(), feed_value(x)));
        }
    }
    sprig_feed::Image(out)
}

/// Whether a copy row's image is live (`_sprig_live`), `None` without an
/// image.
fn live(v: Option<&Value>) -> Option<bool> {
    let m = v?.as_object()?;
    Some(match m.get("_sprig_live") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64() != Some(0),
        Some(Value::String(s)) => s != "0",
        _ => true,
    })
}

/// The engine's commit timestamp as a feed transaction id: one clock for
/// every unit of the node, so the copies' handovers compare engine changes
/// of any tenant (`0-0-<cts>`).
fn engine_gtid(cts: u64) -> sprig_binlog::Gtid {
    sprig_binlog::Gtid {
        domain: 0,
        server: 0,
        seq: cts,
    }
}

/// Change `e` of table `w` in `tenant` as a feed event, for the
/// inheritance mapping. A copy row turning a tombstone is a delete, a
/// tombstone turning live an insert. `None`: nothing changed for a reader
/// (a tombstone rewritten).
fn feed_event(
    tenant: TenantId,
    w: &Watched,
    kind: Kind,
    e: &table_changes::Event,
    cts: u64,
) -> Option<sprig_feed::ChangeEvent> {
    let (mut before, mut after) = (e.before.as_ref(), e.after.as_ref());
    if kind == Kind::Copy {
        if live(before) == Some(false) {
            before = None;
        }
        if live(after) == Some(false) {
            after = None;
        }
    }
    let op = match (before, after) {
        (None, None) => return None,
        (None, Some(_)) => sprig_feed::Op::Insert,
        (Some(_), Some(_)) => sprig_feed::Op::Update,
        (Some(_), None) => sprig_feed::Op::Delete,
    };
    let delta = match (op, before, after) {
        (sprig_feed::Op::Update, Some(Value::Object(b)), Some(Value::Object(a))) => Some(
            a.iter()
                .filter(|(k, v)| b.get(*k) != Some(*v))
                .map(|(k, _)| k.clone())
                .collect(),
        ),
        _ => None,
    };
    let cluster =
        sprig_common::ids::ClusterId::new(crate::tenant::locator::ENGINE_ONLY_CLUSTER).ok()?;
    let gtid = engine_gtid(cts);
    Some(sprig_feed::ChangeEvent {
        id: sprig_feed::EventId {
            cluster: cluster.clone(),
            gtid,
            row: e.pos.seq,
        },
        token: sprig_feed::ResumeToken {
            tenant,
            clamp: 0,
            cluster,
            segment: 0,
            pos: sprig_binlog::GtidPos::default(),
            until: None,
            progress: true,
        },
        tenant,
        db: w.def.db,
        db_name: w.db.clone(),
        object: w.def.object.0,
        table: w.def.name.clone(),
        shard: sprig_feed::ShardRef {
            group: sprig_common::ids::GroupId(0),
            shard: sprig_common::ids::ShardId(0),
        },
        key: feed_image(&e.key),
        op,
        before: before.map(feed_image),
        after: after.map(feed_image),
        delta,
        hlc: hlc_of(cts),
        txn: gtid,
        ddl: None,
        hint: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_round_trips() {
        let cts = (1_764_000_123_456u64 << 16) | 7;
        let h = hlc_of(cts);
        assert_eq!(h >> 32, 1_764_000_123);
        assert_eq!(cts_of_hlc(h), cts);
        // A MariaDB-style stamp: the start of its second.
        assert_eq!(cts_of_hlc(5u64 << 32 | 3), (5000u64 << 16) | 3);
        assert_eq!(cts_of_hlc(5u64 << 32 | 0xFFFF_0000), 5000u64 << 16);
        assert!(hlc_of(cts + 1) > h);
        assert!(hlc_of((1_764_000_124_000u64) << 16) > h);
    }

    #[test]
    fn first_words() {
        assert_eq!(first_word("  /* x */ TRUNCATE TABLE t"), "TRUNCATE");
        assert_eq!(first_word("-- c\nalter table t"), "alter");
        assert_eq!(first_word(""), "");
    }

    #[test]
    fn copy_rows_and_feed_images() {
        let live_row =
            serde_json::json!({"id": 1, "_sprig_live": 1, "_sprig_share": 0, "name": "a"});
        let dead_row = serde_json::json!({"id": 1, "_sprig_live": 0, "_sprig_share": 0});
        assert_eq!(live(Some(&live_row)), Some(true));
        assert_eq!(live(Some(&dead_row)), Some(false));
        assert_eq!(live(None), None);
        assert_eq!(feed_image(&live_row).0.len(), 4);
        let key = feed_image(&serde_json::json!({"id": 1, "_sprig_space": 7}));
        assert_eq!(
            sprig_streams::envelope::image_json(&key),
            serde_json::json!({"id": 1, "_sprig_space": 7})
        );
        assert_eq!(engine_gtid(42).seq, 42);
    }

    #[test]
    fn updates_name_changed_columns() {
        let b = serde_json::json!({"id": 1, "a": "x", "b": 2});
        let a = serde_json::json!({"id": 1, "a": "y", "b": 2});
        assert_eq!(changed(Some(&b), Some(&a)), serde_json::json!({"a": "y"}));
        assert_eq!(changed(None, Some(&a)), a);
    }
}
