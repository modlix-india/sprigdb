//! Engine tables' changes as change events, read from the node's change
//! log ([`sprig_streams::changes`]): what a `CHANGES OF t` sink reads
//! (through `SinkSql`'s `Changes` operation, [`serve`]).
//!
//! ```text
//! change log page ──► records of the watched tables' rows (key kind P,
//!                     the table's current row generation)
//!                 ──► before-image: the key's version just below the change
//!                 ──► op: c (no before), u (both), d (a delete)
//!                 ──► event {op, before, after, source, ts_ms}, key = the
//!                     primary key's JSON, token = resume token v2
//! ```
//!
//! The event is the change-topic envelope the change feed has always
//! written: `{"op": "c"|"u"|"d", "before": {...}|null, "after": {...}|null,
//! "source": {"tenant", "db", "object", "model": "table", "unit", "index",
//! "seq", "cts"}, "ts_ms"}`. Values are JSON numbers for integers, floats
//! and decimals a double holds exactly (other decimals as text), base64
//! for binary strings, the document for `JSON`, and their text for the
//! rest (temporal values as the server shows them, `TIMESTAMP` in UTC).
//!
//! A consumer's reader stays open on this gateway between its requests
//! (keyed by the consumer), so the MVCC versions its before-images need
//! stay held while it keeps reading; a reader idle for five minutes is
//! closed (a later request opens a new one at its token, and changes older
//! than the version GC's horizon then show no before-image).
//!
//! Rows a table rewrite copies into a new row generation (a definition
//! change that can't read old rows through) are not changes: only records
//! of the table's current generation become events.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use serde_json::{Map, Value as J, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::kind;
use sprig_streams::changes::{Change, ChangeLog, Page, Pos, Reader, Token};

use crate::engine::row::Datum;
use crate::engine::table::{Column, TableDef};

/// A page read from the change log at most.
pub(crate) const PAGE_BYTES: usize = 1 << 20;

/// A reader unused this long is closed.
const IDLE: Duration = Duration::from_secs(300);

/// Open readers per gateway at most (the oldest is closed beyond).
const MAX_READERS: usize = 4096;

/// One watched table.
#[derive(Debug, Clone)]
pub(crate) struct Watched {
    /// Its definition.
    pub def: Arc<TableDef>,
    /// Its database's name.
    pub db: String,
}

/// One change event.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Event {
    /// The resume token after it.
    pub token: String,
    /// The table.
    pub object: u64,
    /// `c`, `u` or `d`.
    pub op: &'static str,
    /// The primary key as a JSON object.
    pub key: J,
    /// The row before.
    pub before: Option<J>,
    /// The row after.
    pub after: Option<J>,
    /// The commit time, ms since the epoch.
    pub ts_ms: i64,
    /// The whole envelope.
    pub envelope: J,
    /// Its place in the change log.
    pub pos: Pos,
}

/// What one read gives.
#[derive(Debug, Default)]
pub(crate) struct Batch {
    /// Events in commit order.
    pub events: Vec<Event>,
    /// The token after everything read (past changes of other objects
    /// too).
    pub next: String,
    /// The place after everything read.
    pub pos: Pos,
    /// It read to the end of what the log holds.
    pub at_end: bool,
}

/// An open reader of one consumer.
struct Open {
    reader: Reader,
    tenant: u64,
    used: Instant,
}

fn readers() -> &'static Mutex<HashMap<String, Open>> {
    static R: OnceLock<Mutex<HashMap<String, Open>>> = OnceLock::new();
    R.get_or_init(Mutex::default)
}

/// The commit time in ms of commit timestamp `cts` (ms << 16 | counter).
fn ms_of(cts: u64) -> i64 {
    i64::try_from(cts >> 16).unwrap_or(i64::MAX)
}

/// The first commit timestamp of millisecond `ms`.
fn cts_of_ms(ms: i64) -> u64 {
    u64::try_from(ms.max(0)).unwrap_or(0) << 16
}

/// Where a read starts: after `token` (a resume token v2 of `tenant`),
/// or, without one, at the first change at or after `from_ms` (0: the
/// oldest change the log holds).
///
/// # Errors
/// A token that isn't one of `tenant`'s (39223).
fn start(log: &ChangeLog, tenant: u64, token: Option<&str>, from_ms: i64) -> Result<(Pos, u64)> {
    let unit = sprig_engine::Engine::unit_of_tenant(tenant).0;
    match token.filter(|t| !t.is_empty()) {
        Some(t) => {
            let tk = Token::from_hex(t).ok_or_else(|| {
                SprigError::new(
                    SprigCode::SinkSourceLost,
                    "the sink's change position isn't a change-log token",
                )
            })?;
            if tk.tenant != tenant {
                return Err(SprigError::new(
                    SprigCode::SinkSourceLost,
                    "the sink's change position is another tenant's",
                ));
            }
            Ok((tk.pos(), tk.cts.max(1)))
        }
        None if from_ms > 0 => {
            let cts = cts_of_ms(from_ms);
            Ok((log.place_at(unit, cts), cts))
        }
        None => Ok((log.oldest(unit), 1)),
    }
}

/// Takes `consumer`'s open reader when it is at `pos`, else opens one.
fn take_reader(
    log: &ChangeLog,
    tenant: u64,
    consumer: &str,
    pos: Pos,
    hold: u64,
) -> Result<(Reader, bool)> {
    let mut m = readers().lock().unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    m.retain(|_, o| now.duration_since(o.used) < IDLE);
    if let Some(o) = m.remove(consumer)
        && o.tenant == tenant
        && o.reader.pos() == pos
    {
        return Ok((o.reader, false));
    }
    drop(m);
    let unit = sprig_engine::Engine::unit_of_tenant(tenant).0;
    let r = log.reader(unit, pos, Some(hold)).map_err(|e| {
        if sprig_streams::changes::is_history_lost(&e) {
            SprigError::new(SprigCode::SinkSourceLost, e.message)
        } else {
            e
        }
    })?;
    Ok((r, true))
}

/// Puts `consumer`'s reader back for its next request.
fn put_reader(tenant: u64, consumer: &str, reader: Reader) {
    if consumer.is_empty() {
        return;
    }
    let mut m = readers().lock().unwrap_or_else(PoisonError::into_inner);
    if m.len() >= MAX_READERS
        && let Some(oldest) = m.iter().min_by_key(|(_, o)| o.used).map(|(k, _)| k.clone())
    {
        m.remove(&oldest);
    }
    m.insert(
        consumer.to_owned(),
        Open {
            reader,
            tenant,
            used: Instant::now(),
        },
    );
}

/// Moves `from`'s open reader to `to` (a Kafka fetch's reader is found by
/// the offset the next fetch asks for).
pub(crate) fn rekey(from: &str, to: &str) {
    let mut m = readers().lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(o) = m.remove(from) {
        m.insert(to.to_owned(), o);
    }
}

/// Whether `consumer` has an open reader at `token` (its privileges were
/// checked when it opened).
pub(crate) fn is_open(consumer: &str, tenant: u64, token: Option<&str>) -> bool {
    let Some(pos) = token.and_then(Token::from_hex).map(|t| t.pos()) else {
        return false;
    };
    readers()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(consumer)
        .is_some_and(|o| o.tenant == tenant && o.reader.pos() == pos)
}

/// A change of one of the watched tables, with its before-image.
pub(crate) struct Raw {
    /// The change.
    pub change: Change,
    /// Its table.
    pub object: u32,
    /// The row just before it.
    pub before: Option<Vec<u8>>,
}

/// Reads the next changes of `tables` (by object number) of `tenant`
/// after `token` (or from `from_ms`, see [`start`]): one page of the log
/// of at most `max_bytes`, waiting up to `wait` for one when the log has
/// nothing new. Changes committed before `from_ms` are left out.
/// `consumer` keys the reader kept open between requests (empty: none
/// kept).
///
/// # Errors
/// 39223 when the token is bad or the log no longer holds its place;
/// store and decoding failures.
#[allow(clippy::too_many_arguments)] // one read's inputs
pub(crate) async fn read(
    eng: &crate::engine::Engine,
    log: &ChangeLog,
    tenant: u64,
    tables: &HashMap<u32, Watched>,
    token: Option<&str>,
    from_ms: i64,
    max_bytes: usize,
    wait: Duration,
    consumer: &str,
) -> Result<Batch> {
    let (pos, hold) = start(log, tenant, token, from_ms)?;
    let from_cts = cts_of_ms(from_ms);
    read_from(
        eng, log, tenant, tables, pos, hold, from_cts, max_bytes, wait, consumer,
    )
    .await
}

/// [`read`] after place `pos`, holding versions from `hold`, leaving out
/// changes committed before `from_cts`.
///
/// # Errors
/// As [`read`].
#[allow(clippy::too_many_arguments)] // one read's inputs
pub(crate) async fn read_from(
    eng: &crate::engine::Engine,
    log: &ChangeLog,
    tenant: u64,
    tables: &HashMap<u32, Watched>,
    pos: Pos,
    hold: u64,
    from_cts: u64,
    max_bytes: usize,
    wait: Duration,
    consumer: &str,
) -> Result<Batch> {
    let (mut reader, _) = take_reader(log, tenant, consumer, pos, hold)?;
    let until = tokio::time::Instant::now() + wait;
    let mut watch = log.watch(reader.unit());
    let objects: HashSet<u32> = tables.keys().copied().collect();
    let max_bytes = max_bytes.clamp(1024, PAGE_BYTES);
    loop {
        watch.borrow_and_update();
        let lg = log.clone();
        let objs = objects.clone();
        let res = tokio::task::spawn_blocking(move || {
            let p = page(&lg, &mut reader, tenant, &objs, max_bytes);
            (reader, p)
        })
        .await
        .map_err(|e| SprigError::internal(format!("change log read: {e}")))?;
        reader = res.0;
        let (page, raws) = match res.1 {
            Ok(x) => x,
            Err(e) => {
                return Err(if sprig_streams::changes::is_history_lost(&e) {
                    SprigError::new(SprigCode::SinkSourceLost, e.message)
                } else {
                    e
                });
            }
        };
        let mut events = Vec::new();
        for r in raws {
            if r.change.cts < from_cts {
                continue;
            }
            let Some(w) = tables.get(&r.object) else {
                continue;
            };
            if let Some(ev) = event(eng, tenant, w, &r).await? {
                events.push(ev);
            }
        }
        let next = Token::at(tenant, page.cts, page.pos, true).to_hex();
        let done = !events.is_empty() || !page.at_end || tokio::time::Instant::now() >= until;
        if done {
            put_reader(tenant, consumer, reader);
            return Ok(Batch {
                events,
                next,
                pos: page.pos,
                at_end: page.at_end,
            });
        }
        // Nothing new: wait for the next apply, or the deadline.
        if tokio::time::timeout_at(until, watch.changed())
            .await
            .is_err()
        {
            put_reader(tenant, consumer, reader);
            return Ok(Batch {
                events,
                next,
                pos: page.pos,
                at_end: true,
            });
        }
    }
}

/// One page of the log and the watched tables' row changes in it, with
/// their before-images (read while the reader holds their versions).
fn page(
    log: &ChangeLog,
    reader: &mut Reader,
    tenant: u64,
    objects: &HashSet<u32>,
    max_bytes: usize,
) -> Result<(Page, Vec<Raw>)> {
    let mut p = reader.read(max_bytes)?;
    let mut out = Vec::new();
    // Each table's row generation as of the change being looked at: read
    // as of the page's first change of the table (the reader holds those
    // versions), then followed through the layout records in the log. A
    // row written under another generation is a table rewrite's copy (or
    // its cleanup), not a change.
    let mut gens: HashMap<u32, u16> = HashMap::new();
    for c in std::mem::take(&mut p.changes) {
        let Some((pre, user)) = c.prefix() else {
            continue;
        };
        if pre.tenant != tenant || pre.shard != 0 || !objects.contains(&pre.object) {
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
            }
            continue;
        }
        if pre.kind != kind::PRIMARY {
            continue;
        }
        let g = if let Some(g) = gens.get(&pre.object) {
            *g
        } else {
            let g = gen_at(log, tenant, pre.object, c.cts.saturating_sub(1))?;
            gens.insert(pre.object, g);
            g
        };
        if pre.index != g {
            continue;
        }
        let before = log.before_image(&c)?;
        out.push(Raw {
            object: pre.object,
            change: c,
            before,
        });
    }
    Ok((p, out))
}

/// The row generation of table `object` as of commit timestamp `at`: its
/// layout record's (generation 0 without one).
pub(crate) fn gen_at(log: &ChangeLog, tenant: u64, object: u32, at: u64) -> Result<u16> {
    let key = crate::engine::keys::layout_key(tenant, object);
    let store = log.engine().store();
    let v = sprig_engine::txn::mvcc::read_at(store.as_ref(), &key, sprig_engine::Ts(at))?;
    Ok(v.and_then(|v| v.value)
        .and_then(|b| crate::engine::layout::LayoutRecord::decode(&b))
        .map_or(0, |l| l.row_gen))
}

/// The event of `r`, `None` when its row doesn't decode with the table's
/// current definition (logged).
pub(crate) async fn event(
    eng: &crate::engine::Engine,
    tenant: u64,
    w: &Watched,
    r: &Raw,
) -> Result<Option<Event>> {
    let def = &w.def;
    let sp = eng.space(tenant, def).await?;
    let key = &r.change.key;
    let row = |raw: &[u8]| -> Result<Vec<Datum>> {
        crate::engine::row::decode(def, &sp, key, &Bytes::copy_from_slice(raw))
    };
    // A row of a generation the table has since rewritten may not decode
    // with today's definition: it is left out, not a failed read.
    let (before, after) = match (
        r.before.as_deref().map(row).transpose(),
        r.change.value.as_deref().map(row).transpose(),
    ) {
        (Ok(b), Ok(a)) => (b, a),
        (Err(e), _) | (_, Err(e)) => {
            tracing::warn!(table = %def.name, index = r.change.index, seq = r.change.seq, error = %e, "change of a row that no longer decodes left out");
            return Ok(None);
        }
    };
    let op = match (&before, &after) {
        (None, Some(_)) => "c",
        (Some(_), Some(_)) => "u",
        // A delete whose before-image the version GC already dropped (the
        // consumer was away longer than the engine keeps old versions):
        // still a delete, keyed from the row's key.
        (_, None) => "d",
    };
    let key_of = match after.as_ref().or(before.as_ref()) {
        Some(v) => key_json(def, v),
        None => key_of_bytes(def, sp.first_format, key),
    };
    let before = before.map(|v| row_json(def, &v));
    let after = after.map(|v| row_json(def, &v));
    let ts_ms = ms_of(r.change.cts);
    let token = Token::at(tenant, r.change.cts, r.change.pos(), false).to_hex();
    let envelope = json!({
        "op": op,
        "before": before.clone().unwrap_or(J::Null),
        "after": after.clone().unwrap_or(J::Null),
        "source": {
            "tenant": tenant,
            "db": w.db,
            "object": def.name,
            "model": "table",
            "unit": sprig_engine::Engine::unit_of_tenant(tenant).0,
            "index": r.change.index,
            "seq": r.change.seq,
            "cts": r.change.cts,
        },
        "ts_ms": ts_ms,
    });
    Ok(Some(Event {
        token,
        object: def.object.0,
        op,
        key: key_of,
        before,
        after,
        ts_ms,
        envelope,
        pos: r.change.pos(),
    }))
}

/// A row as a JSON object of its columns.
fn row_json(def: &TableDef, row: &[Datum]) -> J {
    let mut m = Map::new();
    for (c, v) in def.columns.iter().zip(row) {
        m.insert(c.name.clone(), value_json(c, v));
    }
    J::Object(m)
}

/// The primary key of `row` as a JSON object (`_rowid` for a table keyed
/// by a hidden row number).
fn key_json(def: &TableDef, row: &[Datum]) -> J {
    let mut m = Map::new();
    for &i in &def.pk {
        let Some(v) = row.get(i) else { continue };
        match def.columns.get(i) {
            Some(c) => m.insert(c.name.clone(), value_json(c, v)),
            None => m.insert(
                "_rowid".to_owned(),
                match v {
                    Datum::UInt(n) => json!(n),
                    Datum::Int(n) => json!(n),
                    _ => J::Null,
                },
            ),
        };
    }
    J::Object(m)
}

/// The primary key from a row's key: integer and other non-string parts
/// decoded, else the key's bytes as `{"_key": base64}` (a string's key
/// part is its collation's sort key, not its text).
fn key_of_bytes(def: &TableDef, first_format: bool, key: &[u8]) -> J {
    let raw = || {
        let user = key
            .get(sprig_engine::keys::PREFIX_LEN..)
            .unwrap_or_default();
        json!({"_key": base64::engine::general_purpose::STANDARD.encode(user)})
    };
    if def.hidden {
        return match crate::engine::row::hidden_of(key) {
            Ok(Datum::UInt(n)) => json!({"_rowid": n}),
            Ok(Datum::Int(n)) => json!({"_rowid": n}),
            _ => raw(),
        };
    }
    let mut rest = key
        .get(sprig_engine::keys::PREFIX_LEN..)
        .unwrap_or_default();
    let mut m = Map::new();
    for (kc, &i) in def.pk_key(first_format).iter().zip(&def.pk) {
        let Some(c) = def.columns.get(i) else {
            return raw();
        };
        match sprig_engine::keys::typed::decode_column(rest, kc) {
            Ok((Some(v), r)) if !matches!(v, Datum::Str(_) | Datum::Bytes(_)) => {
                m.insert(c.name.clone(), value_json(c, &v));
                rest = r;
            }
            _ => return raw(),
        }
    }
    J::Object(m)
}

/// One value as JSON (see the module docs).
pub(crate) fn value_json(c: &Column, d: &Datum) -> J {
    match d {
        Datum::Null => J::Null,
        Datum::Int(v) => json!(v),
        Datum::UInt(v) | Datum::Bit(v) => json!(v),
        Datum::Float(v) => json!(f64::from(*v)),
        Datum::Double(v) => json!(v),
        Datum::Year(v) => json!(v),
        Datum::Decimal(v) => decimal(&v.to_string()),
        Datum::Bytes(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Datum::Json(b) => serde_json::from_slice(b)
            .unwrap_or_else(|_| J::String(String::from_utf8_lossy(b).into())),
        _ => crate::engine::meta::text(c, d).map_or(J::Null, |t| {
            J::String(String::from_utf8_lossy(&t).into_owned())
        }),
    }
}

/// A `DECIMAL` as a JSON number when a double holds it exactly, else its
/// text (a rounded number would tell the reader a value the row doesn't
/// have).
fn decimal(s: &str) -> J {
    let trimmed = if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    };
    match s.parse::<f64>() {
        Ok(f) if f.is_finite() && (f.to_string() == trimmed || f.to_string() == s) => json!(f),
        _ => J::String(s.to_owned()),
    }
}

/// `SinkSql`'s `Changes`: the next changes of the sink's tables
/// `objects` for `caller`, one result of rows `(token, object, op, key,
/// before, after, ts_ms, value)` (JSON text in key, before, after and
/// value; value is the whole envelope) and the token after the read as
/// the reply's `next`. Opening a reader checks the caller's `SELECT` on
/// each table (`check`, run as the caller), and refuses tables under
/// row-level security (39366).
///
/// # Errors
/// 39223 for a table that is gone or not on the engine, a bad token or
/// lost history; 39366; the privilege check's error; the read's.
#[allow(clippy::too_many_arguments)] // the request's fields
pub(crate) async fn serve<F, Fut>(
    svc: &crate::tenant::TenantServices,
    tenant: u64,
    objects: &[u64],
    token: Option<&str>,
    from_ms: i64,
    max_bytes: u64,
    wait_ms: u64,
    consumer: &str,
    check: F,
) -> Result<(sprig_sinks::peer::SqlResult, String)>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let lost = |m: String| SprigError::new(SprigCode::SinkSourceLost, m);
    let Some(log) = crate::changelog::changes() else {
        return Err(lost(
            "engine tables' changes need the node's change log, which is not open \
             (SPRIGDB_ENGINE_CHANGELOG)"
                .into(),
        ));
    };
    let eng = crate::engine::engine()?;
    let cat = svc
        .cache
        .get(
            sprig_common::ids::TenantId(tenant),
            sprig_catalog::cache::Freshness::Latest,
        )
        .await?;
    let ecat = crate::engine::catalog::of(&cat);
    let rls = sprig_auth::authz::rls::RlsState::of(&cat);
    let mut tables = HashMap::new();
    for &o in objects {
        let oid = sprig_common::ids::ObjectId(o);
        let def = ecat
            .and_then(|e| e.tables.get(&oid))
            .and_then(|p| p.as_ref().as_ref().ok())
            .cloned()
            .ok_or_else(|| {
                lost(format!(
                    "table object {o} is gone (or isn't an engine table)"
                ))
            })?;
        if rls.and_then(|s| s.security(oid)).is_some_and(|s| s.enabled) {
            return Err(SprigError::new(
                SprigCode::RlsUnsupported,
                format!(
                    "{}: table {} has row-level security, which change sinks on engine tables \
                     don't apply yet",
                    SprigCode::RlsUnsupported.name(),
                    def.name
                ),
            ));
        }
        let db = cat
            .databases
            .get(def.db)
            .map(|d| d.name.clone())
            .unwrap_or_default();
        let object = u32::try_from(o).map_err(|_| lost(format!("table object {o}")))?;
        tables.insert(object, Watched { def, db });
    }
    if !is_open(consumer, tenant, token) {
        for w in tables.values() {
            check(format!(
                "SELECT 1 FROM `{}`.`{}` LIMIT 0",
                w.db.replace('`', "``"),
                w.def.name.replace('`', "``")
            ))
            .await?;
        }
    }
    let wait = Duration::from_millis(wait_ms.min(10_000));
    let max = usize::try_from(max_bytes).unwrap_or(PAGE_BYTES);
    let b = read(
        eng, &log, tenant, &tables, token, from_ms, max, wait, consumer,
    )
    .await?;
    let mut res = sprig_sinks::peer::SqlResult {
        columns: [
            "token", "object", "op", "key", "before", "after", "ts_ms", "value",
        ]
        .map(str::to_owned)
        .to_vec(),
        ..Default::default()
    };
    let text = |v: &J| Some(v.to_string());
    for e in b.events {
        res.rows.push(
            [
                Some(e.token),
                Some(e.object.to_string()),
                Some(e.op.to_owned()),
                text(&e.key),
                e.before.as_ref().and_then(text),
                e.after.as_ref().and_then(text),
                Some(e.ts_ms.to_string()),
                text(&e.envelope),
            ]
            .into_iter()
            .map(|c| sprig_sinks::peer::SqlResult::cell(c.as_deref().map(str::as_bytes)))
            .collect(),
        );
    }
    Ok((res, b.next))
}

#[cfg(test)]
mod tests;
