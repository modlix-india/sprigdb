//! The broadcast log of inherited streams, as engine records of the
//! definition's declarer:
//!
//! ```text
//! INSERT INTO s FOR ALL TENANTS [WITH INSTANCE] [UNDER u] ... by P:
//!   limits (39244): each record <= inherit.broadcast_max_bytes, fewer than
//!     inherit.broadcasts_per_hour of the definition in the last hour,
//!     members <= inherit.broadcast_max_members
//!   one log entry in the declarer A's unit, in P's engine transaction when
//!     one is open (it commits or rolls back with it), else on its own:
//!     A | object IB | M | 1 | def object | seq  -> the records, P, form, time
//!     A | object IB | C | 1 | def object        -> the last seq
//!   the definition listed under inherit-bcast-engine/<A>.<object> (DC tier)
//! fan-out (a task on every node, every 2 s or when woken), per listed
//!   definition, entries from its floor in seq order:
//!   members: in scope below P, inside UNDER, made no later than the
//!     entry; WITH INSTANCE: only those with an instance now
//!   each member m whose mark (the next seq it takes: offset of group
//!     __sprig_ib_<object>, instance m, in A's records) is <= seq:
//!     its instance made if needed; then ONE transaction moves the mark
//!     from what was read to seq + 1 (compare-and-set) and appends the
//!     records to m's instance: once, and in broadcast order, per member
//!   a member whose delivery fails waits (1 s doubling to 5 min) and its
//!     later entries wait behind it; the others go on
//!   an entry every member took moves the floor; entries below the floor
//!     older than inherit.broadcast_retention_hours are removed
//! paced at inherit.broadcast_rate deliveries a second
//! ```
//!
//! Records keep the publisher's timestamps; each member's copy carries
//! `x-sprig-tenant` (the member), `x-sprig-origin-tenant` (P) and
//! `x-sprig-broadcast` (`<object>.<seq>`).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::{KeyPrefix, KeyRange, kind, memcmp};
use sprig_engine::txn::{Dir, Isolation, LockRequest, ReadMode, Txn, TxnOptions};
use sprig_inherit::msg::settings as s;
use sprig_inherit::msg::{DefKey, DefKind, DefState, Definition};
use sprig_meta::{ListOpts, Record, Versioned};
use sprig_proto_kafka::broker::data::ProduceRecord;
use sprig_proto_kafka::records::Header;
use sprig_sql::lex::LexOptions;
use sprig_streams::seg::kafka::{EngineStreams, encode_records};

use crate::inherited::{self, broadcast::Form};
use crate::tenant::TenantServices;

/// The declarer's broadcast-log object (`'I' 'B'`).
const OBJECT: u32 = 0x4942_0000;
/// `M | 1 | def object | seq`: an entry.
const LOG: u16 = 1;
/// `M | 2 | def object`: the floor (every entry below it is delivered).
const FLOOR: u16 = 2;
/// `C | 1 | def object`: the last seq.
const SEQ: u16 = 1;

/// The longest a failed member waits before its next try.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Entries one pass reads.
const PASS_ENTRIES: usize = 256;

/// The broadcast header.
const BROADCAST_HEADER: &str = "x-sprig-broadcast";

fn keyed(tenant: u64, index: u16, k: u8, def: u64) -> Vec<u8> {
    let mut v = KeyPrefix {
        tenant,
        shard: 0,
        object: OBJECT,
        kind: k,
        index,
    }
    .to_vec();
    memcmp::put_u64(&mut v, def);
    v
}

fn entry_key(tenant: u64, def: u64, seq: u64) -> Vec<u8> {
    let mut v = keyed(tenant, LOG, kind::META, def);
    memcmp::put_u64(&mut v, seq);
    v
}

fn group(def: u64) -> String {
    format!("__sprig_ib_{def}")
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok())
        .collect()
}

/// A record as the log keeps it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Rec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    k: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    v: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    h: Vec<(String, Option<String>)>,
    ts: i64,
}

impl Rec {
    fn of(r: &ProduceRecord) -> Self {
        Self {
            k: r.key.as_deref().map(hex),
            v: r.value.as_deref().map(hex),
            h: r.headers
                .iter()
                .map(|h| (h.key.clone(), h.value.as_deref().map(hex)))
                .collect(),
            ts: r.timestamp_ms,
        }
    }

    fn record(&self) -> ProduceRecord {
        ProduceRecord {
            key: self.k.as_deref().map(unhex),
            value: self.v.as_deref().map(unhex),
            headers: self
                .h
                .iter()
                .map(|(k, v)| Header {
                    key: k.clone(),
                    value: v.as_deref().map(unhex),
                })
                .collect(),
            timestamp_ms: self.ts,
        }
    }
}

/// A log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    publisher: u64,
    origin: String,
    form: Form,
    committed_ms: u64,
    records: Vec<Rec>,
}

/// `inherit-bcast-engine/<declarer>.<object>`: a definition with log
/// entries to fan out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Listed {
    def: DefKey,
}

impl Versioned for Listed {
    const V: u32 = 1;
}

fn listed_key(svc: &TenantServices, def: DefKey) -> Result<sprig_meta::Key> {
    svc.locator.dc_store().tier().key(&format!(
        "inherit-bcast-engine/{}.{}",
        def.tenant, def.object
    ))
}

fn limit_err(m: &str) -> SprigError {
    SprigError::new(
        SprigCode::InheritBroadcastLimit,
        format!("ER_SPRIG_INHERIT_BROADCAST_LIMIT: {m}"),
    )
}

fn waker() -> &'static tokio::sync::Notify {
    static W: OnceLock<tokio::sync::Notify> = OnceLock::new();
    W.get_or_init(tokio::sync::Notify::new)
}

async fn commit(t: Txn) -> Result<()> {
    let gate = t.reply_gate();
    t.commit()?.wait_async().await?;
    gate.wait_async().await
}

fn begin(eng: &EngineStreams) -> Result<Txn> {
    eng.log()
        .engine()
        .begin(TxnOptions::mysql().with_isolation(Isolation::ReadCommitted))
}

fn read_u64(t: &mut Txn, k: &[u8]) -> Result<u64> {
    Ok(t.read_current(k)?
        .and_then(|b| <[u8; 8]>::try_from(&b[..]).ok())
        .map_or(0, u64::from_be_bytes))
}

/// Entries of `def` from `from`, at most `limit`, in seq order.
fn entries(
    t: &mut Txn,
    tenant: u64,
    def: u64,
    from: u64,
    limit: usize,
) -> Result<Vec<(u64, Entry)>> {
    let start = entry_key(tenant, def, from);
    let end = keyed(tenant, LOG, kind::META, def.saturating_add(1));
    let rows = t.scan(
        &KeyRange {
            start,
            end: Some(end),
        },
        Dir::Forward,
        limit,
        ReadMode::Snapshot,
    )?;
    let prefix = keyed(tenant, LOG, kind::META, def).len();
    let mut out = Vec::with_capacity(rows.len());
    for (k, v) in rows {
        let Ok((seq, _)) = memcmp::get_uint(&k[prefix..], 8) else {
            continue;
        };
        match serde_json::from_slice::<Entry>(&v) {
            Ok(e) => out.push((seq, e)),
            Err(e) => {
                tracing::warn!(def, seq, error = %e, "broadcast log entry unreadable; skipped");
            }
        }
    }
    Ok(out)
}

/// Checks a broadcast's limits.
///
/// # Errors
/// 39244; the directory's and the log's.
async fn check_limits(
    svc: &Arc<TenantServices>,
    eng: &EngineStreams,
    def: &Definition,
    publisher: TenantId,
    form: Form,
    records: &[ProduceRecord],
) -> Result<()> {
    let dcat = inherited::catalog(svc, def.declarer).await?;
    let mut ctx = dcat.object_ctx(&svc.dc, sprig_common::ids::ObjectId(def.object_id));
    if let Some(d) = dcat
        .databases
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(&def.db_name))
    {
        ctx = ctx.with_db(d.db_id);
    }
    let get =
        |k: &'static sprig_settings::Setting<u64>| svc.settings.get(k, &ctx, Some(&dcat.settings));
    let max_bytes = get(&s::BROADCAST_MAX_BYTES);
    for r in records {
        let (_, size) = sprig_streams::seg::kafka::record_size(r);
        if size > max_bytes {
            return Err(limit_err(&format!(
                "a broadcast record of {size} bytes is over inherit.broadcast_max_bytes \
                 ({max_bytes})"
            )));
        }
    }
    let per_hour = get(&s::BROADCASTS_PER_HOUR);
    let a = def.declarer.0;
    let mut t = begin(eng)?;
    let last = read_u64(&mut t, &keyed(a, SEQ, kind::COUNTERS, def.object_id))?;
    let since = sprig_inherit::msg::store::now_ms().saturating_sub(3_600_000);
    let recent = entries(
        &mut t,
        a,
        def.object_id,
        last.saturating_sub(per_hour),
        usize::try_from(per_hour.saturating_add(1)).unwrap_or(usize::MAX),
    )?
    .into_iter()
    .filter(|(_, e)| e.committed_ms >= since)
    .count() as u64;
    t.rollback();
    if recent >= per_hour {
        return Err(limit_err(&format!(
            "{}.{} took {recent} broadcasts in the last hour (inherit.broadcasts_per_hour = \
             {per_hour})",
            def.db_name, def.name
        )));
    }
    let max_members = get(&s::BROADCAST_MAX_MEMBERS);
    let m = inherited::broadcast::members(
        svc,
        def,
        publisher,
        form,
        sprig_inherit::msg::store::now_ms(),
    )
    .await?
    .len() as u64;
    if m > max_members {
        return Err(limit_err(&format!(
            "a broadcast would reach {m} members (inherit.broadcast_max_members = {max_members}); \
             publish per subtree (UNDER t) or per tenant"
        )));
    }
    Ok(())
}

/// Writes a broadcast's log entry: into `txn` (the publisher's engine
/// transaction) when given, else in a transaction of its own. The entry's
/// seq.
///
/// # Errors
/// 39244 (limits); the log's.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish(
    svc: &Arc<TenantServices>,
    eng: &EngineStreams,
    def: &Definition,
    publisher: TenantId,
    origin: &str,
    form: Form,
    records: &[ProduceRecord],
    txn: Option<&mut Txn>,
) -> Result<u64> {
    check_limits(svc, eng, def, publisher, form, records).await?;
    let a = def.declarer.0;
    let entry = Entry {
        publisher: publisher.0,
        origin: origin.to_owned(),
        form,
        committed_ms: sprig_inherit::msg::store::now_ms(),
        records: records.iter().map(Rec::of).collect(),
    };
    let body = serde_json::to_vec(&entry).map_err(|e| SprigError::internal(e.to_string()))?;
    let counter = Bytes::from(keyed(a, SEQ, kind::COUNTERS, def.object_id));
    let write = |t: &mut Txn, seq: u64| -> Result<()> {
        t.put(counter.to_vec(), seq.to_be_bytes().to_vec())?;
        t.put(entry_key(a, def.object_id, seq), body.clone())
    };
    let seq = if let Some(t) = txn {
        t.lock(&counter, LockRequest::write()).await?;
        let seq = read_u64(t, &counter)? + 1;
        write(t, seq)?;
        seq
    } else {
        let mut t = begin(eng)?;
        t.lock(&counter, LockRequest::write()).await?;
        let seq = read_u64(&mut t, &counter)? + 1;
        write(&mut t, seq)?;
        commit(t).await?;
        seq
    };
    let dc = svc.locator.dc_store();
    let k = listed_key(svc, def.key())?;
    dc.txn(sprig_meta::Txn {
        when: Vec::new(),
        then: vec![sprig_meta::Op::Put {
            key: k,
            value: Record::new(Listed { def: def.key() }).encode()?,
            lease: None,
        }],
        otherwise: Vec::new(),
    })
    .await?;
    metrics::counter!("sprig_inherit_broadcasts_total").increment(1);
    waker().notify_one();
    Ok(seq)
}

/// Starts this node's fan-out task (see the module docs).
pub(crate) fn start(svc: &Arc<TenantServices>) {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        let mut backoff: HashMap<(DefKey, TenantId), (Instant, Duration)> = HashMap::new();
        loop {
            let _ = tokio::time::timeout(Duration::from_secs(2), waker().notified()).await;
            let Some(svc) = weak.upgrade() else { return };
            let Some(eng) = crate::kafka::engine::streams() else {
                continue;
            };
            let Ok(list) = listed(&svc).await else {
                continue;
            };
            for def in list {
                if let Err(err) = Box::pin(fan_out(&svc, &eng, def, &mut backoff)).await {
                    tracing::debug!(def = %def, error = %err, "inherited stream broadcast fan-out");
                }
            }
        }
    });
}

async fn listed(svc: &TenantServices) -> Result<Vec<DefKey>> {
    let dc = svc.locator.dc_store();
    let prefix = dc.tier().key("inherit-bcast-engine/")?;
    let page = dc
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<Listed>::decode(&kv.value).ok().map(|r| r.body.def))
        .collect())
}

async fn unlist(svc: &TenantServices, def: DefKey) -> Result<()> {
    let dc = svc.locator.dc_store();
    dc.txn(sprig_meta::Txn {
        when: Vec::new(),
        then: vec![sprig_meta::Op::Delete(listed_key(svc, def)?)],
        otherwise: Vec::new(),
    })
    .await?;
    Ok(())
}

/// The definition `def` as its declarer's catalog has it now.
async fn definition(svc: &TenantServices, def: DefKey) -> Result<Option<Arc<Definition>>> {
    let cat = inherited::catalog(svc, def.tenant).await?;
    Ok(inherited::state_of(&cat)
        .defs
        .values()
        .find(|d| d.object_id == def.object && d.kind == DefKind::Stream)
        .cloned())
}

/// One fan-out pass over definition `def`'s log.
#[allow(clippy::too_many_lines)]
async fn fan_out(
    svc: &Arc<TenantServices>,
    eng: &Arc<EngineStreams>,
    key: DefKey,
    backoff: &mut HashMap<(DefKey, TenantId), (Instant, Duration)>,
) -> Result<()> {
    let a = key.tenant.0;
    let Some(def) = definition(svc, key).await? else {
        // Dropped: nothing more to deliver; its entries go.
        clear(eng, a, key.object).await?;
        return unlist(svc, key).await;
    };
    if def.state != DefState::Active {
        return Ok(());
    }
    let floor_key = keyed(a, FLOOR, kind::META, key.object);
    let (floor, list) = {
        let mut t = begin(eng)?;
        let floor = read_u64(&mut t, &floor_key)?.max(1);
        let list = entries(&mut t, a, key.object, floor, PASS_ENTRIES)?;
        t.rollback();
        (floor, list)
    };
    if list.is_empty() {
        return unlist(svc, key).await;
    }
    let g = group(key.object);
    let mut marks: HashMap<u64, i64> = eng
        .records()
        .offsets_on(a, &g)?
        .into_iter()
        .map(|(i, o)| (i, o.offset))
        .collect();
    let dcat = inherited::catalog(svc, key.tenant).await?;
    let ctx = dcat.setting_ctx(&svc.dc);
    let rate = svc.settings.get(
        &sprig_inherit::msg::settings::BROADCAST_RATE,
        &ctx,
        Some(&dcat.settings),
    );
    let pause = Duration::from_micros(1_000_000 / rate.max(1));
    let lex = LexOptions::mariadb();
    let full = format!("{}.{}", def.db_name, def.name);
    let mut names: HashMap<TenantId, String> = HashMap::new();
    let mut blocked: std::collections::HashSet<TenantId> = std::collections::HashSet::new();
    let mut new_floor = floor;
    let mut contiguous = true;
    for (seq, entry) in &list {
        let seq = *seq;
        let members = inherited::broadcast::members(
            svc,
            &def,
            TenantId(entry.publisher),
            entry.form,
            entry.committed_ms,
        )
        .await?;
        let mut all = true;
        for m in members {
            let mark = marks.get(&m.0).copied().unwrap_or(1);
            if mark > i64::try_from(seq).unwrap_or(i64::MAX) {
                continue;
            }
            if blocked.contains(&m)
                || backoff
                    .get(&(key, m))
                    .is_some_and(|(at, _)| Instant::now() < *at)
            {
                all = false;
                blocked.insert(m);
                continue;
            }
            if entry.form.with_instance && eng.topic(m.0, &full)?.is_none() {
                continue;
            }
            let name = if let Some(n) = names.get(&m) {
                n.clone()
            } else {
                let n = inherited::tenant_name(svc, m).await?;
                names.insert(m, n.clone());
                n
            };
            match deliver(svc, eng, &def, m, &name, seq, mark, entry, &lex).await {
                Ok(true) => {
                    marks.insert(m.0, i64::try_from(seq + 1).unwrap_or(i64::MAX));
                    backoff.remove(&(key, m));
                    metrics::counter!("sprig_inherit_broadcast_deliveries_total").increment(1);
                }
                Ok(false) => {
                    // Moved by another node's pass: read again next time.
                    all = false;
                    blocked.insert(m);
                }
                Err(e) => {
                    let wait = backoff
                        .get(&(key, m))
                        .map_or(Duration::from_secs(1), |(_, w)| (*w * 2).min(MAX_BACKOFF));
                    backoff.insert((key, m), (Instant::now() + wait, wait));
                    tracing::warn!(def = %key, member = %m, seq, error = %e, retry_in = ?wait, "broadcast delivery failed; the member's later broadcasts wait behind it");
                    all = false;
                    blocked.insert(m);
                }
            }
            tokio::time::sleep(pause).await;
        }
        if all && contiguous {
            new_floor = seq + 1;
        } else {
            contiguous = false;
        }
    }
    if new_floor > floor {
        let retention_ms = svc
            .settings
            .get(
                &sprig_inherit::msg::settings::BROADCAST_RETENTION_HOURS,
                &ctx,
                Some(&dcat.settings),
            )
            .saturating_mul(3_600_000);
        let old = sprig_inherit::msg::store::now_ms().saturating_sub(retention_ms);
        let mut t = begin(eng)?;
        t.put(floor_key.clone(), new_floor.to_be_bytes().to_vec())?;
        for (seq, e) in entries(&mut t, a, key.object, 1, PASS_ENTRIES)? {
            if seq >= new_floor || e.committed_ms >= old {
                break;
            }
            t.delete(entry_key(a, key.object, seq))?;
        }
        commit(t).await?;
    }
    Ok(())
}

/// Removes a dropped definition's log.
async fn clear(eng: &EngineStreams, a: u64, def: u64) -> Result<()> {
    let mut t = begin(eng)?;
    for (seq, _) in entries(&mut t, a, def, 0, 100_000)? {
        t.delete(entry_key(a, def, seq))?;
    }
    t.delete(keyed(a, FLOOR, kind::META, def))?;
    t.delete(keyed(a, SEQ, kind::COUNTERS, def))?;
    commit(t).await
}

/// One member's copy of entry `seq`: the mark moves from `mark` to the
/// next entry and the records go into its instance, in one transaction.
/// `false` when the mark had moved.
#[allow(clippy::too_many_arguments)]
async fn deliver(
    svc: &Arc<TenantServices>,
    eng: &Arc<EngineStreams>,
    def: &Definition,
    m: TenantId,
    name: &str,
    seq: u64,
    mark: i64,
    entry: &Entry,
    lex: &LexOptions,
) -> Result<bool> {
    super::streams::ensure_instance(svc, eng, m, def, lex).await?;
    let full = format!("{}.{}", def.db_name, def.name);
    let topic = eng
        .topic(m.0, &full)?
        .ok_or_else(|| SprigError::internal("an inherited stream instance went missing"))?;
    let parts = u32::try_from(topic.partitions.max(1)).unwrap_or(1);
    let id = format!("{}.{seq}", def.object_id);
    let mut by_part: BTreeMap<u32, Vec<ProduceRecord>> = BTreeMap::new();
    for (n, r) in entry.records.iter().enumerate() {
        let mut r = r.record();
        super::streams::stamp(&mut r, super::streams::TENANT_HEADER, name);
        super::streams::stamp(&mut r, super::streams::ORIGIN_HEADER, &entry.origin);
        super::streams::stamp(&mut r, BROADCAST_HEADER, &id);
        let p = match r.key.as_deref() {
            Some(k) => sprig_streams::partition::for_key(k, parts).0,
            None => u32::try_from(n).unwrap_or(0) % parts,
        };
        by_part.entry(p).or_default().push(r);
    }
    let mut t = begin(eng)?;
    if !sprig_streams::seg::records::move_offset_on(
        &mut t,
        def.declarer.0,
        &group(def.object_id),
        m.0,
        0,
        0,
        if mark <= 1 { 0 } else { mark },
        i64::try_from(seq + 1).unwrap_or(i64::MAX),
        sprig_streams::seg::now_ms(),
    )
    .await?
    {
        t.rollback();
        return Ok(false);
    }
    for (p, recs) in by_part {
        eng.log().append_in(
            &mut t,
            sprig_streams::seg::Key {
                tenant: m.0,
                stream: topic.stream_id,
                partition: p,
            },
            &encode_records(&recs, None)?,
        )?;
    }
    commit(t).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_keep_their_bytes_in_the_log() {
        let r = ProduceRecord {
            key: Some(b"k\x00\xff".to_vec()),
            value: None,
            headers: vec![
                Header {
                    key: "h".into(),
                    value: Some(b"v".to_vec()),
                },
                Header {
                    key: "n".into(),
                    value: None,
                },
            ],
            timestamp_ms: 42,
        };
        let back = Rec::of(&r).record();
        assert_eq!(back, r);
    }

    #[test]
    fn entries_sort_by_definition_then_seq() {
        let a = entry_key(7, 3, 2);
        let b = entry_key(7, 3, 10);
        let c = entry_key(7, 4, 1);
        assert!(a < b && b < c);
        assert!(keyed(7, LOG, kind::META, 4) <= c);
    }
}
