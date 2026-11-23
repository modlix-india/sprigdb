//! Change topics of engine tables: `<database>.<table>.changes`, one
//! read-only partition per table, served from the node's change log
//! ([`crate::table_changes`]) in front of the other topic and data stores.
//!
//! ```text
//! offset   = change-log entry index << 21 | record number in the entry
//! record   key = the primary key's JSON, value = the change envelope
//!          {op, before, after, source, ts_ms}, timestamp = commit time
//! bounds   log start = the oldest change kept; high watermark = after
//!          the unit's last entry
//! ```
//!
//! Offsets grow with the change log, with gaps: other tables' changes
//! and whole entries without one of this table's changes are skipped (a
//! read that finds none ends with an empty filtered record, so the
//! consumer moves past them). A transaction of more than 2^21 records
//! (beyond the engine's per-transaction lock memory) can't be numbered.
//! Producing to a change topic, and creating, deleting or altering one,
//! is refused: it follows its table. Reading needs `SELECT` on the table;
//! a group's committed offsets are engine records, as for the engine's
//! own topics (the stream ids are in the engine's range).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_auth::authz::Action;
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_proto_kafka::broker::data::{
    Access, AppendTicket, Bounds, DataTopic, Limits, ProduceRecord, ProducerBatch, StoredRecord,
    StreamData,
};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::topics::{
    NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, TopicKind, Topics,
};
use sprig_streams::changes::{ChangeLog, Pos};
use tokio::time::Instant;
use uuid::Uuid;

use super::data::GatewayData;
use crate::table_changes::Watched;

/// The flag of change topics' stream ids (inside the engine's stream id
/// range, so groups' offsets are engine records).
pub const STREAM_FLAG: u64 = 1 << 61;

/// Bits of an offset for the record number in its entry.
pub const SEQ_BITS: u32 = 21;

const SEQ_MASK: u64 = (1 << SEQ_BITS) - 1;

/// Whether `stream` is a change topic's.
#[must_use]
pub const fn is_change_topic(stream: u64) -> bool {
    stream & STREAM_FLAG != 0
}

/// The offset of the change at `p`.
#[must_use]
pub fn offset_of(p: Pos) -> i64 {
    let seq = u64::from(p.seq).min(SEQ_MASK);
    i64::try_from((p.index << SEQ_BITS) | seq).unwrap_or(i64::MAX)
}

/// The first offset after place `after`.
#[must_use]
pub fn next_offset(after: Pos) -> i64 {
    if after.seq == u32::MAX || u64::from(after.seq) >= SEQ_MASK {
        i64::try_from((after.index + 1) << SEQ_BITS).unwrap_or(i64::MAX)
    } else {
        offset_of(after) + 1
    }
}

/// The place a reader of offset `offset` starts after.
#[must_use]
pub fn pos_before(offset: i64) -> Pos {
    let o = u64::try_from(offset.max(0)).unwrap_or(0);
    let (index, seq) = (o >> SEQ_BITS, o & SEQ_MASK);
    match (index, seq) {
        (0, 0) => Pos::default(),
        (i, 0) => Pos::after_entry(i - 1),
        (i, s) => Pos {
            index: i,
            seq: u32::try_from(s - 1).unwrap_or(u32::MAX),
        },
    }
}

fn terr(code: i16, m: impl Into<String>) -> TopicError {
    TopicError::new(code, m)
}

fn read_only(name: &str) -> TopicError {
    terr(
        err::POLICY_VIOLATION,
        format!("topic {name} is a table's change topic: it is read-only and follows its table"),
    )
}

/// One change topic as resolved for a principal.
struct Resolved {
    name: String,
    object: u32,
    watched: Watched,
}

/// What both stores share: the catalog lookups.
struct Lookup {
    data: Arc<GatewayData>,
}

impl Lookup {
    /// `name` as a change topic of an engine table (`None`: it isn't one).
    async fn resolve(&self, p: &Principal, name: &str) -> Option<(Arc<TenantCatalog>, Resolved)> {
        let base = name.strip_suffix(".changes")?;
        let (db, table) = base.split_once('.')?;
        let cat = self.data.catalog_of(p).await?;
        if !self.data.engine_database(&cat, db) {
            return None;
        }
        let r = self.table(&cat, db, table)?;
        Some((cat, r))
    }

    fn table(&self, cat: &TenantCatalog, db: &str, table: &str) -> Option<Resolved> {
        let lctn = self.data.lctn_of(cat);
        let row = cat.database(&database_key(db, lctn))?;
        let def = crate::engine::catalog::of(cat)?
            .table(row.db_id, table, lctn)?
            .as_ref()
            .as_ref()
            .ok()
            .cloned()?;
        Some(Resolved {
            name: format!("{}.{}.changes", row.name, def.name),
            object: u32::try_from(def.object.0).ok()?,
            watched: Watched {
                def,
                db: row.name.clone(),
            },
        })
    }

    /// Every engine table of the tenant `p` may read, as a change topic.
    async fn all(&self, p: &Principal) -> Vec<Resolved> {
        let Some(cat) = self.data.catalog_of(p).await else {
            return Vec::new();
        };
        let Some(e) = crate::engine::catalog::of(&cat) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for parsed in e.tables.values() {
            let Ok(def) = parsed.as_ref() else { continue };
            let Some(db) = cat.databases.get(def.db) else {
                continue;
            };
            if !self
                .data
                .allowed_named(p, &cat, &db.name, &def.name, Action::Select)
            {
                continue;
            }
            if let Some(r) = self.table(&cat, &db.name, &def.name) {
                out.push(r);
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn may_read(&self, p: &Principal, cat: &TenantCatalog, r: &Resolved) -> bool {
        self.data
            .allowed_named(p, cat, &r.watched.db, &r.watched.def.name, Action::Select)
    }
}

fn stream_of(r: &Resolved) -> u64 {
    STREAM_FLAG | u64::from(r.object)
}

fn info(p: &Principal, r: &Resolved) -> TopicInfo {
    let stream_id = stream_of(r);
    TopicInfo {
        name: r.name.clone(),
        stream_id,
        topic_id: Uuid::from_u64_pair(p.tenant_id, stream_id),
        partitions: 1,
        kind: TopicKind::Changes,
        configs: TopicConfigs::default(),
        defaults: TopicDefaults::default(),
    }
}

/// Topic administration with engine tables' change topics in front of
/// `inner`.
pub struct ChangeTopics {
    look: Lookup,
    inner: Arc<dyn Topics>,
}

impl std::fmt::Debug for ChangeTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeTopics").finish_non_exhaustive()
    }
}

impl ChangeTopics {
    /// Over `inner`, with `data`'s catalog and grants.
    #[must_use]
    pub fn new(data: Arc<GatewayData>, inner: Arc<dyn Topics>) -> Self {
        Self {
            look: Lookup { data },
            inner,
        }
    }
}

#[async_trait]
impl Topics for ChangeTopics {
    async fn list(&self, p: &Principal) -> Result<Vec<TopicInfo>, TopicError> {
        let mut out = self.inner.list(p).await?;
        let have: std::collections::HashSet<String> = out.iter().map(|t| t.name.clone()).collect();
        for r in self.look.all(p).await {
            if !have.contains(&r.name) {
                out.push(info(p, &r));
            }
        }
        Ok(out)
    }

    async fn get(&self, p: &Principal, name: &str) -> Result<Option<TopicInfo>, TopicError> {
        match self.look.resolve(p, name).await {
            Some((cat, r)) if self.look.may_read(p, &cat, &r) => Ok(Some(info(p, &r))),
            Some(_) => Ok(None),
            None => self.inner.get(p, name).await,
        }
    }

    async fn defaults(&self, p: &Principal, name: &str) -> Result<TopicDefaults, TopicError> {
        self.inner.defaults(p, name).await
    }

    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> Result<TopicInfo, TopicError> {
        if self.look.resolve(p, &t.name).await.is_some() {
            return Err(read_only(&t.name));
        }
        self.inner.create(p, t, validate_only).await
    }

    async fn delete(&self, p: &Principal, name: &str) -> Result<TopicInfo, TopicError> {
        if self.look.resolve(p, name).await.is_some() {
            return Err(read_only(name));
        }
        self.inner.delete(p, name).await
    }

    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        if self.look.resolve(p, name).await.is_some() {
            return Err(read_only(name));
        }
        self.inner.grow(p, name, partitions, validate_only).await
    }

    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> Result<(), TopicError> {
        if self.look.resolve(p, name).await.is_some() {
            return Err(read_only(name));
        }
        self.inner.set_configs(p, name, c, validate_only).await
    }
}

/// The data plane with engine tables' change topics in front of `inner`.
pub struct ChangeData {
    look: Lookup,
    inner: Option<Arc<dyn StreamData>>,
}

impl std::fmt::Debug for ChangeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeData").finish_non_exhaustive()
    }
}

fn unknown(name: &str) -> TopicError {
    terr(
        err::UNKNOWN_TOPIC_OR_PARTITION,
        format!("topic {name} does not exist"),
    )
}

fn storage(e: &sprig_common::SprigError) -> TopicError {
    if sprig_streams::changes::is_history_lost(e)
        || e.code() == Some(sprig_common::SprigCode::SinkSourceLost)
    {
        return terr(err::OFFSET_OUT_OF_RANGE, e.message.clone());
    }
    terr(err::KAFKA_STORAGE_ERROR, e.message.clone())
}

fn log() -> Result<ChangeLog, TopicError> {
    crate::changelog::changes().ok_or_else(|| {
        terr(
            err::KAFKA_STORAGE_ERROR,
            "change topics need the node's change log, which is not open (SPRIGDB_ENGINE_CHANGELOG)",
        )
    })
}

fn unit_of(tenant: u64) -> u64 {
    sprig_engine::Engine::unit_of_tenant(tenant).0
}

/// A change topic's bounds.
fn bounds_of(log: &ChangeLog, tenant: u64) -> Bounds {
    let unit = unit_of(tenant);
    Bounds {
        log_start: next_offset(log.oldest(unit)),
        high_watermark: next_offset(log.head(unit).0),
        leader_epoch: 0,
    }
}

impl ChangeData {
    /// Over `inner` (none: only change topics are served), with `data`'s
    /// catalog and grants.
    #[must_use]
    pub fn new(data: Arc<GatewayData>, inner: Option<Arc<dyn StreamData>>) -> Self {
        Self {
            look: Lookup { data },
            inner,
        }
    }

    fn inner(&self) -> Result<&Arc<dyn StreamData>, TopicError> {
        self.inner.as_ref().ok_or_else(|| {
            terr(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                "only change topics are served here",
            )
        })
    }

    /// The change topic of stream `stream` for `p`.
    async fn by_stream(&self, p: &Principal, stream: u64) -> Result<Resolved, TopicError> {
        let object = u32::try_from(stream & !STREAM_FLAG).unwrap_or(u32::MAX);
        let cat = self
            .look
            .data
            .catalog_of(p)
            .await
            .ok_or_else(|| unknown("(unknown)"))?;
        let e = crate::engine::catalog::of(&cat).ok_or_else(|| unknown("(unknown)"))?;
        let def = e
            .tables
            .get(&sprig_common::ids::ObjectId(u64::from(object)))
            .and_then(|p| p.as_ref().as_ref().ok())
            .ok_or_else(|| unknown("(dropped table)"))?;
        let db = cat
            .databases
            .get(def.db)
            .ok_or_else(|| unknown(&def.name))?;
        self.look
            .table(&cat, &db.name, &def.name)
            .ok_or_else(|| unknown(&def.name))
    }

    fn data_topic(p: &Principal, r: &Resolved) -> DataTopic {
        let stream_id = stream_of(r);
        DataTopic {
            name: r.name.clone(),
            stream_id,
            topic_id: Uuid::from_u64_pair(p.tenant_id, stream_id),
            partitions: 1,
            kind: TopicKind::Changes,
            compact: false,
            max_message_bytes: crate::table_changes::PAGE_BYTES,
        }
    }

    fn check(
        &self,
        p: &Principal,
        cat: &TenantCatalog,
        r: &Resolved,
        access: Access,
    ) -> Result<(), TopicError> {
        if access == Access::Write {
            return Err(read_only(&r.name));
        }
        if !self.look.may_read(p, cat, r) {
            return Err(unknown(&r.name));
        }
        Ok(())
    }

    /// Records of `t` from `offset`, about `max_bytes`.
    async fn read_changes(
        &self,
        p: &Principal,
        t: &DataTopic,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Bounds, Vec<StoredRecord>), TopicError> {
        let log = log()?;
        let bounds = bounds_of(&log, p.tenant_id);
        if offset < bounds.log_start || offset > bounds.high_watermark {
            return Err(terr(
                err::OFFSET_OUT_OF_RANGE,
                format!(
                    "offset {offset} is outside [{}, {}]",
                    bounds.log_start, bounds.high_watermark
                ),
            ));
        }
        let topic = self.by_stream(p, t.stream_id).await?;
        let eng = crate::engine::engine().map_err(|e| storage(&e))?;
        let object = topic.object;
        let tables = HashMap::from([(object, topic.watched)]);
        let key = |o: i64| format!("kafka/{object}/{o}");
        let batch = crate::table_changes::read_from(
            eng,
            &log,
            p.tenant_id,
            &tables,
            pos_before(offset),
            1,
            0,
            max_bytes,
            Duration::ZERO,
            &key(offset),
        )
        .await
        .map_err(|e| storage(&e))?;
        let mut out: Vec<StoredRecord> = Vec::with_capacity(batch.events.len() + 1);
        let mut truncated = false;
        for e in batch.events {
            let o = offset_of(e.pos);
            if out
                .first()
                .is_some_and(|f| o - f.offset >= i64::from(i32::MAX))
            {
                truncated = true;
                break;
            }
            out.push(StoredRecord {
                offset: o,
                timestamp_ms: e.ts_ms,
                key: Some(e.key.to_string().into_bytes()),
                value: Some(e.envelope.to_string().into_bytes()),
                headers: Vec::new(),
                leader_epoch: 0,
                filtered: false,
            });
        }
        if !truncated {
            // Past the last change read (other tables' too): an empty
            // filtered record there moves the consumer on.
            let next = next_offset(batch.pos);
            let last = next - 1;
            if last >= offset
                && out.last().is_none_or(|r| r.offset < last)
                && out
                    .first()
                    .is_none_or(|f| last - f.offset < i64::from(i32::MAX))
            {
                out.push(StoredRecord {
                    offset: last,
                    timestamp_ms: out.last().map_or(0, |r| r.timestamp_ms),
                    key: None,
                    value: None,
                    headers: Vec::new(),
                    leader_epoch: 0,
                    filtered: true,
                });
            }
            crate::table_changes::rekey(&key(offset), &key(next));
        }
        Ok((bounds, out))
    }
}

#[async_trait]
impl StreamData for ChangeData {
    async fn topic(
        &self,
        p: &Principal,
        name: &str,
        access: Access,
    ) -> Result<DataTopic, TopicError> {
        match self.look.resolve(p, name).await {
            Some((cat, r)) => {
                self.check(p, &cat, &r, access)?;
                Ok(Self::data_topic(p, &r))
            }
            None => self.inner()?.topic(p, name, access).await,
        }
    }

    async fn topic_by_id(
        &self,
        p: &Principal,
        id: Uuid,
        access: Access,
    ) -> Result<DataTopic, TopicError> {
        let (tenant, stream) = id.as_u64_pair();
        if !is_change_topic(stream) {
            return self.inner()?.topic_by_id(p, id, access).await;
        }
        if tenant != p.tenant_id {
            return Err(terr(
                err::UNKNOWN_TOPIC_ID,
                format!("no topic with id {id}"),
            ));
        }
        let r = self.by_stream(p, stream).await?;
        let cat = self
            .look
            .data
            .catalog_of(p)
            .await
            .ok_or_else(|| unknown(&r.name))?;
        self.check(p, &cat, &r, access)?;
        Ok(Self::data_topic(p, &r))
    }

    async fn append(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        records: Vec<ProduceRecord>,
        producer: Option<ProducerBatch>,
    ) -> Result<AppendTicket, TopicError> {
        if is_change_topic(t.stream_id) {
            return Err(read_only(&t.name));
        }
        self.inner()?
            .append(p, t, partition, records, producer)
            .await
    }

    async fn sequenced(
        &self,
        p: &Principal,
        t: &DataTopic,
        ticket: &AppendTicket,
        deadline: Instant,
    ) -> Result<i64, TopicError> {
        if is_change_topic(t.stream_id) {
            return Err(read_only(&t.name));
        }
        self.inner()?.sequenced(p, t, ticket, deadline).await
    }

    async fn bounds(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
    ) -> Result<Bounds, TopicError> {
        if !is_change_topic(t.stream_id) {
            return self.inner()?.bounds(p, t, partition).await;
        }
        if partition != 0 {
            return Err(unknown(&t.name));
        }
        Ok(bounds_of(&log()?, p.tenant_id))
    }

    async fn read(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Bounds, Vec<StoredRecord>), TopicError> {
        if !is_change_topic(t.stream_id) {
            return self.inner()?.read(p, t, partition, offset, max_bytes).await;
        }
        if partition != 0 {
            return Err(unknown(&t.name));
        }
        self.read_changes(p, t, offset, max_bytes).await
    }

    async fn wait(&self, p: &Principal, beyond: &[(u64, i32, i64)], deadline: Instant) {
        let (mine, theirs): (Vec<(u64, i32, i64)>, Vec<(u64, i32, i64)>) =
            beyond.iter().partition(|(s, _, _)| is_change_topic(*s));
        let mine_wait = async {
            let Ok(log) = log() else {
                tokio::time::sleep_until(deadline).await;
                return;
            };
            let unit = unit_of(p.tenant_id);
            let mut w = log.watch(unit);
            loop {
                w.borrow_and_update();
                let hw = next_offset(log.head(unit).0);
                if mine.iter().any(|(_, _, o)| hw > *o) {
                    return;
                }
                if tokio::time::timeout_at(deadline, w.changed())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        };
        match (&self.inner, mine.is_empty(), theirs.is_empty()) {
            (_, true, true) | (None, true, false) => tokio::time::sleep_until(deadline).await,
            (_, false, true) | (None, false, false) => mine_wait.await,
            (Some(i), true, false) => i.wait(p, &theirs, deadline).await,
            (Some(i), false, false) => {
                tokio::select! {
                    () = mine_wait => {}
                    () = i.wait(p, &theirs, deadline) => {}
                }
            }
        }
    }

    async fn offset_for_time(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        ts: i64,
    ) -> Result<Option<(i64, i64)>, TopicError> {
        if !is_change_topic(t.stream_id) {
            return self.inner()?.offset_for_time(p, t, partition, ts).await;
        }
        let log = log()?;
        let unit = unit_of(p.tenant_id);
        let b = bounds_of(&log, p.tenant_id);
        if ts < 0 {
            // The largest timestamp: the last change's place, roughly.
            return Ok(Some((b.high_watermark, 0)));
        }
        let r = self.by_stream(p, t.stream_id).await?;
        let eng = crate::engine::engine().map_err(|e| storage(&e))?;
        let tables = HashMap::from([(r.object, r.watched)]);
        let cts = u64::try_from(ts).unwrap_or(0) << 16;
        let mut at = log.place_at(unit, cts);
        loop {
            let batch = crate::table_changes::read_from(
                eng,
                &log,
                p.tenant_id,
                &tables,
                at,
                cts.max(1),
                cts,
                crate::table_changes::PAGE_BYTES,
                Duration::ZERO,
                "",
            )
            .await
            .map_err(|e| storage(&e))?;
            if let Some(e) = batch.events.first() {
                return Ok(Some((offset_of(e.pos), e.ts_ms)));
            }
            if batch.at_end || batch.pos == at {
                return Ok(None);
            }
            at = batch.pos;
        }
    }

    async fn epoch_end(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        epoch: i32,
    ) -> Result<(i32, i64), TopicError> {
        if !is_change_topic(t.stream_id) {
            return self.inner()?.epoch_end(p, t, partition, epoch).await;
        }
        Ok((0, bounds_of(&log()?, p.tenant_id).high_watermark))
    }

    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32 {
        if is_change_topic(stream_id) {
            return 0;
        }
        match &self.inner {
            Some(i) => i.leader_epoch(p, stream_id, partition).await,
            None => 0,
        }
    }

    async fn limits(&self, p: &Principal) -> Limits {
        match &self.inner {
            Some(i) => i.limits(p).await,
            None => self.look.data.limits(p).await,
        }
    }

    fn stores_batches(&self, p: &Principal, t: &DataTopic) -> bool {
        if is_change_topic(t.stream_id) {
            return false;
        }
        self.inner.as_ref().is_some_and(|i| i.stores_batches(p, t))
    }

    async fn append_batch(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        batch: &[u8],
        records: &[ProduceRecord],
        producer: Option<ProducerBatch>,
    ) -> Result<AppendTicket, TopicError> {
        if is_change_topic(t.stream_id) {
            return Err(read_only(&t.name));
        }
        self.inner()?
            .append_batch(p, t, partition, batch, records, producer)
            .await
    }

    async fn read_batches(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Bounds, Vec<u8>, i64), TopicError> {
        self.inner()?
            .read_batches(p, t, partition, offset, max_bytes)
            .await
    }
}

/// Puts engine tables' change topics in front of `d`'s topic and data
/// stores when the node has a change log; `d` unchanged otherwise.
pub fn wrap(d: &mut super::KafkaDeps) {
    let Some(data) = d.engine_gate.clone() else {
        return;
    };
    if crate::changelog::changes().is_none() {
        return;
    }
    d.topics = Arc::new(ChangeTopics::new(Arc::clone(&data), Arc::clone(&d.topics)));
    d.data = Some(Arc::new(ChangeData::new(data, d.data.take())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_map_places() {
        let p = Pos { index: 7, seq: 3 };
        assert_eq!(offset_of(p), (7 << SEQ_BITS) | 3);
        assert_eq!(pos_before(offset_of(p)), Pos { index: 7, seq: 2 });
        assert_eq!(
            pos_before(offset_of(Pos { index: 7, seq: 0 })),
            Pos::after_entry(6)
        );
        assert_eq!(next_offset(p), offset_of(p) + 1);
        assert_eq!(next_offset(Pos::after_entry(7)), 8 << SEQ_BITS);
        assert_eq!(
            pos_before(next_offset(Pos::after_entry(7))),
            Pos::after_entry(7)
        );
        assert_eq!(pos_before(0), Pos::default());
        assert!(is_change_topic(STREAM_FLAG | 0xC));
        assert!(sprig_streams::seg::kafka::is_engine_stream(
            STREAM_FLAG | 0xC
        ));
    }
}
