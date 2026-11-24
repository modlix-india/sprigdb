//! Kafka's topics, consumer groups and data plane for streams stored on
//! the engine: [`EngineStreams`] over the segment log and the engine
//! records, and routers ([`RoutedTopics`], [`RoutedGroups`],
//! [`RoutedData`]) that send each request to it or to the front end's
//! other implementation.
//!
//! Which requests are the engine's: a topic name's database is stored on
//! the engine ([`Gate::on_engine`]); a topic id, an offset or a ticket
//! whose stream id is at least [`FIRST_STREAM_ID`] (engine stream ids
//! start there, so they never meet ids given by the tenant catalog).
//!
//! Produce stores the batch as the producer sent it
//! ([`StreamData::append_batch`]); fetch returns stored batches as they
//! are ([`StreamData::read_batches`]).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::Engine;
use sprig_proto_kafka::broker::data::{
    Access, AppendTicket, Bounds, DataTopic, Limits, ProduceRecord, ProducerBatch, StoredRecord,
    StreamData,
};
use sprig_proto_kafka::group::{CommittedOffset, GroupState, GroupStore, StoreError, StoredGroup};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::{Record, RecordBatch};
use sprig_proto_kafka::topics::{
    Cleanup, NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, TopicKind,
    Topics,
};
use tokio::time::Instant;
use uuid::Uuid;

use super::batch::{self, Head};
use super::part::Outcome;
use super::records::{StreamRecords, TopicRecord};
use super::{Key, OutOfRange, Pending, Policy, SegConfig, SegLog};

/// The first id of an engine-stored stream.
pub const FIRST_STREAM_ID: u64 = 1 << 48;

/// Whether `stream` is an engine-stored stream's id.
#[must_use]
pub const fn is_engine_stream(stream: u64) -> bool {
    stream >= FIRST_STREAM_ID
}

/// What a request needs on a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// Reading: fetch, list offsets (`SELECT`).
    Read,
    /// Producing (`INSERT`).
    Write,
    /// Creating a topic (`CREATE` on the database).
    Create,
    /// Adding partitions, changing configs (`ALTER`).
    Alter,
    /// Deleting a topic (`DROP`).
    Drop,
}

/// What the front end decides for the engine store: where databases are
/// and who may do what.
#[async_trait]
pub trait Gate: Send + Sync + 'static {
    /// Whether database `db` of `p`'s tenant is stored on the engine.
    async fn on_engine(&self, p: &Principal, db: &str) -> bool;
    /// Whether the whole tenant is (its consumer-group rows are then
    /// engine records too).
    fn tenant_on_engine(&self, tenant: u64) -> bool;
    /// Whether `p` may do `need` on `db`.`stream`.
    async fn allowed(&self, p: &Principal, db: &str, stream: &str, need: Need) -> bool;
    /// The produce and fetch limits for `p` when no other data plane gives
    /// them.
    async fn limits(&self, p: &Principal) -> Limits {
        let _ = p;
        Limits::default()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn terr(code: i16, message: impl Into<String>) -> TopicError {
    TopicError::new(code, message)
}

fn internal(e: &SprigError) -> TopicError {
    // On a replica that doesn't lead the tenant's unit, a topic change
    // can't commit here: NOT_CONTROLLER sends the client to Metadata's
    // controller, the unit's leader.
    if e.code() == Some(SprigCode::ShardMoved) {
        return terr(err::NOT_CONTROLLER, e.to_string());
    }
    terr(err::UNKNOWN_SERVER_ERROR, e.to_string())
}

/// A refused append: on a replica that doesn't lead the tenant's unit,
/// `NOT_LEADER_OR_FOLLOWER` (the client refreshes Metadata, which names the
/// leader, and retries there); otherwise a storage error.
fn append_err(e: &SprigError) -> TopicError {
    if e.code() == Some(SprigCode::ShardMoved) {
        return terr(err::NOT_LEADER_OR_FOLLOWER, e.to_string());
    }
    terr(err::KAFKA_STORAGE_ERROR, e.to_string())
}

fn split(name: &str) -> std::result::Result<(&str, &str), TopicError> {
    sprig_proto_kafka::mem::split_topic(name)
}

fn unknown(name: &str) -> TopicError {
    terr(
        err::UNKNOWN_TOPIC_OR_PARTITION,
        format!(
            "{} ER_SPRIG_STREAM_NOT_FOUND: topic {name} does not exist",
            SprigCode::StreamNotFound.number()
        ),
    )
}

/// How long a replica that doesn't lead a tenant's unit reuses the topic
/// records it read: with partition groups it serves produce and fetch for
/// partitions it leads while another replica leads the tenant's unit, and
/// a read through the read barrier on every request would cost a round
/// trip each.
const FOLLOWER_FRESH: std::time::Duration = std::time::Duration::from_millis(1000);

/// Streams stored on the node's engine: the segment log and the records.
pub struct EngineStreams {
    log: SegLog,
    records: StreamRecords,
    engine: Engine,
    /// Topic records by (tenant, name), as last read or written here, with
    /// the generation of that read or write.
    cache: Mutex<HashMap<(u64, String), (TopicRecord, u64)>>,
    /// Tenants whose topic records were read (their policies are set), with
    /// the tenant unit's leader epoch at that read.
    loaded: Mutex<HashMap<u64, u64>>,
    /// When a replica that doesn't lead a tenant's unit last read its topic
    /// records: reads within [`FOLLOWER_FRESH`] reuse them (a name not
    /// found reads again).
    read_at: Mutex<HashMap<u64, std::time::Instant>>,
    /// Generations of cache reads and writes.
    generation: std::sync::atomic::AtomicU64,
    /// Inherited values of new streams' settings.
    defaults: TopicDefaults,
}

impl std::fmt::Debug for EngineStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStreams").finish_non_exhaustive()
    }
}

/// The partition key of a request.
fn key(tenant: u64, stream: u64, partition: i32) -> Key {
    Key {
        tenant,
        stream,
        partition: u32::try_from(partition).unwrap_or(u32::MAX),
    }
}

fn ticket_of(p: Pending, partition: i32, count: usize) -> AppendTicket {
    let mut first_event = [0u8; 16];
    first_event[..8].copy_from_slice(&p.index.to_be_bytes());
    first_event[8..12].copy_from_slice(&p.slot.to_be_bytes());
    AppendTicket {
        partition,
        first_event,
        count,
        base_offset: None,
        duplicate: false,
    }
}

/// The pending append of a ticket from [`ticket_of`]: its entry is in
/// `unit` (the partition's, [`SegLog::unit_of_part`]).
fn pending_of(t: &AppendTicket, unit: u64) -> Pending {
    let mut i = [0u8; 8];
    i.copy_from_slice(&t.first_event[..8]);
    let mut s = [0u8; 4];
    s.copy_from_slice(&t.first_event[8..12]);
    Pending {
        unit,
        index: u64::from_be_bytes(i),
        slot: u32::from_be_bytes(s),
    }
}

/// The offset after the last record of whole stored batches.
fn end_of(bytes: &[u8]) -> Option<i64> {
    let mut pos = 0usize;
    let mut end = None;
    while let Some(h) = bytes
        .get(pos..)
        .and_then(Head::parse)
        .filter(|h| pos + h.total <= bytes.len())
    {
        end = Some(h.next_offset());
        pos += h.total;
    }
    end
}

impl EngineStreams {
    /// Opens the stream log in `dir` on `engine` and the records, and
    /// sets the policies of every stream with data. Background work starts
    /// with [`EngineStreams::spawn_background`].
    ///
    /// # Errors
    /// The log can't be opened, or the records can't be read.
    pub fn open(
        engine: &Engine,
        dir: impl Into<PathBuf>,
        cfg: SegConfig,
        defaults: TopicDefaults,
    ) -> Result<Self> {
        let log = SegLog::open(engine, dir, cfg)?;
        let s = Self {
            records: StreamRecords::with_first_id(engine.clone(), FIRST_STREAM_ID),
            log,
            engine: engine.clone(),
            cache: Mutex::new(HashMap::new()),
            loaded: Mutex::new(HashMap::new()),
            read_at: Mutex::new(HashMap::new()),
            generation: std::sync::atomic::AtomicU64::new(0),
            defaults,
        };
        for t in s.log.tenants() {
            s.load(t)?;
        }
        Ok(s)
    }

    /// Starts the log's flusher and maintenance on the current runtime.
    pub fn spawn_background(&self) {
        self.log.spawn_background();
    }

    /// The segment log.
    #[must_use]
    pub fn log(&self) -> &SegLog {
        &self.log
    }

    /// The engine records.
    #[must_use]
    pub fn records(&self) -> &StreamRecords {
        &self.records
    }

    fn policy_of(&self, r: &TopicRecord) -> Policy {
        Policy {
            retention_ms: r.retention_ms.unwrap_or(self.defaults.retention_ms),
            retention_bytes: r.retention_bytes.unwrap_or(self.defaults.retention_bytes),
            compact: r
                .compact
                .unwrap_or(self.defaults.cleanup == Cleanup::Compact),
            ..Policy::default()
        }
    }

    fn next_gen(&self) -> u64 {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1
    }

    fn remember(&self, tenant: u64, r: &TopicRecord) {
        self.log.set_policy(tenant, r.stream_id, self.policy_of(r));
        let g = self.next_gen();
        lock(&self.cache).insert((tenant, r.name.clone()), (r.clone(), g));
    }

    /// Reads a tenant's topic records, setting their policies: once, where
    /// this node's commits are the only ones (a one-node engine, or while
    /// it leads the tenant's unit: again whenever it takes the unit, since
    /// another leader may have changed them). On a replica that doesn't
    /// lead the unit every call reads them (through the read barrier):
    /// they change through replication, not through this node.
    fn load(&self, tenant: u64) -> Result<()> {
        self.load_with(tenant, false)
    }

    /// [`EngineStreams::load`]; `again` skips a follower's recent read.
    fn load_with(&self, tenant: u64, again: bool) -> Result<()> {
        let unit = Engine::unit_of_tenant(tenant);
        let replicated = self.engine.is_replicated_unit(unit);
        let epoch = if replicated {
            self.engine.leader_epoch(unit)
        } else {
            0
        };
        if lock(&self.loaded)
            .get(&tenant)
            .is_some_and(|e| *e == epoch && (!replicated || epoch > 0))
        {
            return Ok(());
        }
        if replicated
            && epoch == 0
            && !again
            && lock(&self.loaded).get(&tenant) == Some(&0)
            && lock(&self.read_at)
                .get(&tenant)
                .is_some_and(|t| t.elapsed() < FOLLOWER_FRESH)
        {
            return Ok(());
        }
        let read_started = std::time::Instant::now();
        let started = self.next_gen();
        let recs = self.records.topics(tenant)?;
        for r in &recs {
            self.log.set_policy(tenant, r.stream_id, self.policy_of(r));
        }
        let g = self.next_gen();
        {
            let mut c = lock(&self.cache);
            // Gone from the records, unless written here since the read
            // began (a create racing this read).
            c.retain(|(t, n), (_, at)| {
                *t != tenant || *at > started || recs.iter().any(|r| r.name == *n)
            });
            for r in recs {
                match c.get(&(tenant, r.name.clone())) {
                    Some((_, at)) if *at > started => {}
                    _ => {
                        c.insert((tenant, r.name.clone()), (r, g));
                    }
                }
            }
        }
        lock(&self.loaded).insert(tenant, epoch);
        if replicated && epoch == 0 {
            lock(&self.read_at).insert(tenant, read_started);
        }
        Ok(())
    }

    /// Whether a lookup that found nothing should read the records again (a
    /// follower's reuse of a recent read may predate the topic).
    fn reread(&self, tenant: u64) -> Result<bool> {
        let unit = Engine::unit_of_tenant(tenant);
        if !self.engine.is_replicated_unit(unit) || self.engine.leader_epoch(unit) != 0 {
            return Ok(false);
        }
        self.load_with(tenant, true)?;
        Ok(true)
    }

    /// Every topic record of a tenant.
    ///
    /// # Errors
    /// The records can't be read.
    pub fn topics(&self, tenant: u64) -> Result<Vec<TopicRecord>> {
        // A listing (Metadata for every topic) can't tell a topic created
        // elsewhere since a follower's last read: always read again there.
        self.load_with(tenant, true)?;
        let mut v: Vec<TopicRecord> = lock(&self.cache)
            .iter()
            .filter(|((t, _), _)| *t == tenant)
            .map(|(_, (r, _))| r.clone())
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    /// One topic record (`<database>.<stream>`).
    ///
    /// # Errors
    /// The records can't be read.
    pub fn topic(&self, tenant: u64, name: &str) -> Result<Option<TopicRecord>> {
        self.load(tenant)?;
        let find = || {
            lock(&self.cache)
                .get(&(tenant, name.to_owned()))
                .map(|(r, _)| r.clone())
        };
        match find() {
            None if self.reread(tenant)? => Ok(find()),
            r => Ok(r),
        }
    }

    /// One topic record by stream id.
    ///
    /// # Errors
    /// The records can't be read.
    pub fn topic_by_id(&self, tenant: u64, id: u64) -> Result<Option<TopicRecord>> {
        self.load(tenant)?;
        let find = || {
            lock(&self.cache)
                .iter()
                .find(|((t, _), (r, _))| *t == tenant && r.stream_id == id)
                .map(|(_, (r, _))| r.clone())
        };
        match find() {
            None if self.reread(tenant)? => Ok(find()),
            r => Ok(r),
        }
    }

    /// Creates a stream; `Ok(None)` when the name is taken.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn create(&self, tenant: u64, rec: TopicRecord) -> Result<Option<TopicRecord>> {
        self.load(tenant)?;
        let made = self.records.create(tenant, rec).await?;
        if let Some(r) = &made {
            self.remember(tenant, r);
        }
        Ok(made)
    }

    /// Replaces a stream's record; `Ok(false)` when it is gone.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn update(&self, tenant: u64, rec: &TopicRecord) -> Result<bool> {
        let ok = self.records.update(tenant, rec).await?;
        if ok {
            self.remember(tenant, rec);
        }
        Ok(ok)
    }

    /// Drops a stream: its record, then its partitions.
    ///
    /// # Errors
    /// The engine or the log failed.
    pub async fn drop_stream(&self, tenant: u64, name: &str) -> Result<Option<TopicRecord>> {
        let gone = self.records.delete(tenant, name).await?;
        lock(&self.cache).remove(&(tenant, name.to_owned()));
        if let Some(r) = &gone {
            self.log.drop_stream(tenant, r.stream_id)?;
        }
        Ok(gone)
    }

    /// Drops every stream of database `db` (`DROP DATABASE`).
    ///
    /// # Errors
    /// As [`Self::drop_stream`].
    pub async fn drop_database(&self, tenant: u64, db: &str) -> Result<usize> {
        let prefix = format!("{db}.");
        let names: Vec<String> = self
            .topics(tenant)?
            .into_iter()
            .map(|r| r.name)
            .filter(|n| n.starts_with(&prefix))
            .collect();
        for n in &names {
            self.drop_stream(tenant, n).await?;
        }
        Ok(names.len())
    }

    /// The effective `max.message.bytes` of a stream.
    #[must_use]
    pub fn max_message_bytes(&self, r: &TopicRecord) -> usize {
        usize::try_from(
            r.max_message_bytes
                .unwrap_or(self.defaults.max_message_bytes),
        )
        .unwrap_or(usize::MAX)
    }

    fn info(tenant: u64, r: &TopicRecord, defaults: TopicDefaults) -> TopicInfo {
        TopicInfo {
            name: r.name.clone(),
            stream_id: r.stream_id,
            topic_id: Uuid::from_u64_pair(tenant, r.stream_id),
            partitions: r.partitions,
            kind: TopicKind::Events,
            configs: TopicConfigs {
                retention_ms: r.retention_ms,
                retention_bytes: r.retention_bytes,
                cleanup: r
                    .compact
                    .map(|c| if c { Cleanup::Compact } else { Cleanup::Delete }),
                max_message_bytes: r.max_message_bytes,
            },
            defaults,
        }
    }

    fn data_topic(&self, tenant: u64, r: &TopicRecord) -> DataTopic {
        DataTopic {
            name: r.name.clone(),
            stream_id: r.stream_id,
            topic_id: Uuid::from_u64_pair(tenant, r.stream_id),
            partitions: r.partitions,
            kind: TopicKind::Events,
            compact: r
                .compact
                .unwrap_or(self.defaults.cleanup == Cleanup::Compact),
            max_message_bytes: self.max_message_bytes(r),
        }
    }

    /// Proposes one checked batch; the ticket to wait on.
    ///
    /// # Errors
    /// The log refused it.
    pub fn propose(&self, k: Key, batch: &[u8]) -> Result<Pending> {
        self.log.propose(k, batch)
    }

    /// Proposes several checked batches in one log entry (all or none).
    ///
    /// # Errors
    /// The log refused the entry.
    pub fn propose_many(&self, batches: &[(Key, Vec<u8>)]) -> Result<Vec<Pending>> {
        self.log.propose_many(batches)
    }

    /// Waits for a proposed append: its base offset.
    ///
    /// # Errors
    /// The append was refused (a Kafka code), or `deadline` passed
    /// (`REQUEST_TIMED_OUT`, retryable: an idempotent retry finds it).
    pub async fn outcome(
        &self,
        p: Pending,
        deadline: Instant,
    ) -> std::result::Result<i64, TopicError> {
        match self.log.outcome(p, deadline).await {
            None => Err(terr(
                err::REQUEST_TIMED_OUT,
                "39946 ER_SPRIG_STREAM_SEQUENCE_TIMEOUT: the append was not applied in time; \
                 retry",
            )),
            Some(Outcome::Appended { base, .. } | Outcome::Duplicate { base, .. }) => Ok(base),
            Some(Outcome::Refused { code, message }) => Err(terr(code, message)),
        }
    }

    fn bounds_of(b: super::Bounds) -> Bounds {
        Bounds {
            log_start: b.log_start,
            high_watermark: b.high_watermark,
            leader_epoch: b.epoch,
        }
    }

    fn read_raw(
        &self,
        k: Key,
        offset: i64,
        max_bytes: usize,
    ) -> std::result::Result<(Bounds, Vec<u8>), TopicError> {
        match self.log.read(k, offset, max_bytes) {
            Ok((b, bytes)) => Ok((Self::bounds_of(b), bytes)),
            Err(Ok(OutOfRange(b))) => Err(terr(
                err::OFFSET_OUT_OF_RANGE,
                format!(
                    "offset {offset} is outside [{}, {}]",
                    b.log_start, b.high_watermark
                ),
            )),
            Err(Err(e)) => Err(terr(err::KAFKA_STORAGE_ERROR, e.to_string())),
        }
    }
}

/// A record's key bytes and its size as stream limits count it: key,
/// value and the headers as their JSON text (as on every stream).
#[must_use]
pub fn record_size(r: &ProduceRecord) -> (u64, u64) {
    let key = r.key.as_ref().map_or(0, Vec::len) as u64;
    let headers: Vec<(String, Option<Vec<u8>>)> = r
        .headers
        .iter()
        .map(|h| (h.key.clone(), h.value.clone()))
        .collect();
    let h = crate::publish::headers_json(&headers).map_or(0, |j| j.len()) as u64;
    (key, key + r.value.as_ref().map_or(0, Vec::len) as u64 + h)
}

/// Encodes records as one batch for the log (records written through the
/// decoded path: SQL publishes and the like).
///
/// # Errors
/// The batch could not be encoded.
pub fn encode_records(
    records: &[ProduceRecord],
    producer: Option<ProducerBatch>,
) -> Result<Vec<u8>> {
    let base_ts = records.iter().map(|r| r.timestamp_ms).min().unwrap_or(0);
    let max_ts = records.iter().map(|r| r.timestamp_ms).max().unwrap_or(0);
    let n = i32::try_from(records.len()).unwrap_or(i32::MAX);
    RecordBatch {
        base_timestamp: base_ts,
        max_timestamp: max_ts,
        last_offset_delta: n - 1,
        producer_id: producer.map_or(-1, |p| p.producer_id),
        producer_epoch: producer.map_or(-1, |p| p.epoch),
        base_sequence: producer.map_or(-1, |p| p.base_sequence),
        records: records
            .iter()
            .enumerate()
            .map(|(i, r)| Record {
                offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
                timestamp_delta: r.timestamp_ms - base_ts,
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .map_err(|e| SprigError::internal(format!("stream batch: {e}")))
}

/// Decodes stored batches into records at or after `from`.
///
/// # Errors
/// A stored batch is malformed.
pub fn decode_stored(bytes: &[u8], from: i64) -> Result<Vec<StoredRecord>> {
    let batches = RecordBatch::decode_all(bytes, 256 << 20, false)
        .map_err(|e| SprigError::internal(format!("stream log: stored batch: {e}")))?;
    let mut out = Vec::new();
    for b in batches {
        let append_time = b.attributes & batch::ATTR_LOG_APPEND_TIME != 0;
        for r in b.records {
            let offset = b.base_offset + i64::from(r.offset_delta);
            if offset < from {
                continue;
            }
            out.push(StoredRecord {
                offset,
                timestamp_ms: if append_time {
                    b.max_timestamp
                } else {
                    b.base_timestamp + r.timestamp_delta
                },
                key: r.key,
                value: r.value,
                headers: r.headers,
                leader_epoch: b.partition_leader_epoch,
                filtered: false,
            });
        }
    }
    Ok(out)
}

// ----- the routers -------------------------------------------------------

/// Topics: the engine's for engine databases, `inner`'s otherwise.
pub struct RoutedTopics {
    eng: Arc<EngineStreams>,
    gate: Arc<dyn Gate>,
    inner: Arc<dyn Topics>,
}

impl std::fmt::Debug for RoutedTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedTopics").finish_non_exhaustive()
    }
}

impl RoutedTopics {
    /// Over the engine streams, the gate and the other topics.
    #[must_use]
    pub fn new(eng: Arc<EngineStreams>, gate: Arc<dyn Gate>, inner: Arc<dyn Topics>) -> Self {
        Self { eng, gate, inner }
    }

    async fn engine_name(&self, p: &Principal, name: &str) -> bool {
        match split(name) {
            Ok((db, _)) => self.gate.on_engine(p, db).await,
            Err(_) => false,
        }
    }

    async fn visible(&self, p: &Principal, r: &TopicRecord) -> bool {
        match split(&r.name) {
            Ok((db, s)) => {
                self.gate.allowed(p, db, s, Need::Read).await
                    || self.gate.allowed(p, db, s, Need::Write).await
            }
            Err(_) => false,
        }
    }

    async fn defaults(&self, p: &Principal, name: &str) -> TopicDefaults {
        self.inner
            .defaults(p, name)
            .await
            .unwrap_or(self.eng.defaults)
    }

    async fn need(
        &self,
        p: &Principal,
        name: &str,
        need: Need,
    ) -> std::result::Result<(), TopicError> {
        let (db, s) = split(name)?;
        if self.gate.allowed(p, db, s, need).await {
            Ok(())
        } else {
            Err(terr(
                err::TOPIC_AUTHORIZATION_FAILED,
                format!("1142 the user may not change topic {name}"),
            ))
        }
    }

    async fn existing(
        &self,
        p: &Principal,
        name: &str,
    ) -> std::result::Result<TopicRecord, TopicError> {
        match self
            .eng
            .topic(p.tenant_id, name)
            .map_err(|e| internal(&e))?
        {
            Some(r) if self.visible(p, &r).await => Ok(r),
            _ => Err(unknown(name)),
        }
    }
}

#[async_trait]
impl Topics for RoutedTopics {
    async fn list(&self, p: &Principal) -> std::result::Result<Vec<TopicInfo>, TopicError> {
        let mut out = match self.inner.list(p).await {
            Ok(v) => v,
            Err(e) if self.gate.tenant_on_engine(p.tenant_id) => {
                tracing::debug!(error = %e.message, "kafka topics of the catalog not listed");
                Vec::new()
            }
            Err(e) => return Err(e),
        };
        for r in self.eng.topics(p.tenant_id).map_err(|e| internal(&e))? {
            if !out.iter().any(|t| t.name == r.name) && self.visible(p, &r).await {
                let d = self.defaults(p, &r.name).await;
                out.push(EngineStreams::info(p.tenant_id, &r, d));
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn get(
        &self,
        p: &Principal,
        name: &str,
    ) -> std::result::Result<Option<TopicInfo>, TopicError> {
        if !self.engine_name(p, name).await {
            return self.inner.get(p, name).await;
        }
        let r = match self
            .eng
            .topic(p.tenant_id, name)
            .map_err(|e| internal(&e))?
        {
            Some(r) if self.visible(p, &r).await => r,
            _ => return Ok(None),
        };
        let d = self.defaults(p, name).await;
        Ok(Some(EngineStreams::info(p.tenant_id, &r, d)))
    }

    async fn defaults(
        &self,
        p: &Principal,
        name: &str,
    ) -> std::result::Result<TopicDefaults, TopicError> {
        Ok(Self::defaults(self, p, name).await)
    }

    async fn create(
        &self,
        p: &Principal,
        t: NewTopic,
        validate_only: bool,
    ) -> std::result::Result<TopicInfo, TopicError> {
        if !self.engine_name(p, &t.name).await {
            return self.inner.create(p, t, validate_only).await;
        }
        self.need(p, &t.name, Need::Create).await?;
        if !sprig_proto_kafka::topics::legal_topic_name(&t.name) {
            return Err(terr(
                err::INVALID_TOPIC_EXCEPTION,
                format!("topic {}: not a legal topic name", t.name),
            ));
        }
        if t.partitions < 1 || t.partitions > 10_000 {
            return Err(terr(
                err::INVALID_PARTITIONS,
                format!("{} partitions: from 1 to 10000", t.partitions),
            ));
        }
        let defaults = self.defaults(p, &t.name).await;
        let exists = self
            .eng
            .topic(p.tenant_id, &t.name)
            .map_err(|e| internal(&e))?
            .is_some();
        let taken = || {
            terr(
                err::TOPIC_ALREADY_EXISTS,
                format!(
                    "{} ER_SPRIG_STREAM_EXISTS: {} already exists",
                    SprigCode::StreamExists.number(),
                    t.name
                ),
            )
        };
        if exists {
            return Err(taken());
        }
        let rec = TopicRecord {
            name: t.name.clone(),
            stream_id: 0,
            partitions: t.partitions,
            retention_ms: t.configs.retention_ms,
            retention_bytes: t.configs.retention_bytes,
            compact: t.configs.cleanup.map(|c| c == Cleanup::Compact),
            max_message_bytes: t.configs.max_message_bytes,
            created_ms: super::now_ms(),
        };
        if validate_only {
            return Ok(TopicInfo {
                topic_id: Uuid::nil(),
                ..EngineStreams::info(p.tenant_id, &rec, defaults)
            });
        }
        let made = self
            .eng
            .create(p.tenant_id, rec)
            .await
            .map_err(|e| internal(&e))?
            .ok_or_else(taken)?;
        Ok(EngineStreams::info(p.tenant_id, &made, defaults))
    }

    async fn delete(
        &self,
        p: &Principal,
        name: &str,
    ) -> std::result::Result<TopicInfo, TopicError> {
        if !self.engine_name(p, name).await {
            return self.inner.delete(p, name).await;
        }
        let r = self.existing(p, name).await?;
        self.need(p, name, Need::Drop).await?;
        let defaults = self.defaults(p, name).await;
        self.eng
            .drop_stream(p.tenant_id, name)
            .await
            .map_err(|e| internal(&e))?;
        Ok(EngineStreams::info(p.tenant_id, &r, defaults))
    }

    async fn grow(
        &self,
        p: &Principal,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> std::result::Result<(), TopicError> {
        if !self.engine_name(p, name).await {
            return self.inner.grow(p, name, partitions, validate_only).await;
        }
        let mut r = self.existing(p, name).await?;
        self.need(p, name, Need::Alter).await?;
        if partitions <= r.partitions || partitions > 10_000 {
            return Err(terr(
                err::INVALID_PARTITIONS,
                format!(
                    "{} ER_SPRIG_STREAM_PARTITIONS_DECREASE: {name} has {} partitions; it can only \
                     grow (to at most 10000)",
                    SprigCode::StreamPartitionsDecrease.number(),
                    r.partitions
                ),
            ));
        }
        if validate_only {
            return Ok(());
        }
        r.partitions = partitions;
        if self
            .eng
            .update(p.tenant_id, &r)
            .await
            .map_err(|e| internal(&e))?
        {
            Ok(())
        } else {
            Err(unknown(name))
        }
    }

    async fn set_configs(
        &self,
        p: &Principal,
        name: &str,
        c: TopicConfigs,
        validate_only: bool,
    ) -> std::result::Result<(), TopicError> {
        if !self.engine_name(p, name).await {
            return self.inner.set_configs(p, name, c, validate_only).await;
        }
        let mut r = self.existing(p, name).await?;
        self.need(p, name, Need::Alter).await?;
        if validate_only {
            return Ok(());
        }
        r.retention_ms = c.retention_ms;
        r.retention_bytes = c.retention_bytes;
        r.compact = c.cleanup.map(|x| x == Cleanup::Compact);
        r.max_message_bytes = c.max_message_bytes;
        if self
            .eng
            .update(p.tenant_id, &r)
            .await
            .map_err(|e| internal(&e))?
        {
            Ok(())
        } else {
            Err(unknown(name))
        }
    }
}

/// Consumer groups: offsets of engine streams are engine records, the
/// others `inner`'s; group rows are engine records for tenants wholly on
/// the engine.
pub struct RoutedGroups {
    eng: Arc<EngineStreams>,
    gate: Arc<dyn Gate>,
    inner: Arc<dyn GroupStore>,
}

impl std::fmt::Debug for RoutedGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedGroups").finish_non_exhaustive()
    }
}

impl RoutedGroups {
    /// Over the engine streams, the gate and the other group store.
    #[must_use]
    pub fn new(eng: Arc<EngineStreams>, gate: Arc<dyn Gate>, inner: Arc<dyn GroupStore>) -> Self {
        Self { eng, gate, inner }
    }
}

fn store_err(e: &SprigError) -> StoreError {
    StoreError(e.to_string())
}

#[async_trait]
impl GroupStore for RoutedGroups {
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> std::result::Result<(), StoreError> {
        let (mine, theirs): (Vec<CommittedOffset>, Vec<CommittedOffset>) = offsets
            .iter()
            .cloned()
            .partition(|o| is_engine_stream(o.stream_id));
        if !theirs.is_empty() {
            self.inner.commit(tenant, group, &theirs).await?;
        }
        if !mine.is_empty() {
            self.eng
                .records
                .commit_offsets(tenant, group, &mine, super::now_ms())
                .await
                .map_err(|e| store_err(&e))?;
        }
        Ok(())
    }

    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> std::result::Result<Vec<CommittedOffset>, StoreError> {
        let wants_inner = only.is_none_or(|o| o.iter().any(|(s, _)| !is_engine_stream(*s)));
        let wants_eng = only.is_none_or(|o| o.iter().any(|(s, _)| is_engine_stream(*s)));
        let mut out = Vec::new();
        if wants_inner {
            match self.inner.fetch(tenant, group, only).await {
                Ok(v) => out.extend(v),
                Err(e) if self.gate.tenant_on_engine(tenant) => {
                    tracing::debug!(error = %e.0, "kafka offsets of the catalog not read");
                }
                Err(e) => return Err(e),
            }
        }
        if wants_eng {
            out.extend(
                self.eng
                    .records
                    .offsets(tenant, group, only)
                    .map_err(|e| store_err(&e))?,
            );
        }
        Ok(out)
    }

    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> std::result::Result<(), StoreError> {
        if self.gate.tenant_on_engine(tenant) {
            self.eng
                .records
                .save_group(tenant, g, state)
                .await
                .map_err(|e| store_err(&e))
        } else {
            self.inner.save_group(tenant, g, state).await
        }
    }

    async fn load_group(
        &self,
        tenant: u64,
        group: &str,
    ) -> std::result::Result<Option<StoredGroup>, StoreError> {
        if self.gate.tenant_on_engine(tenant) {
            self.eng
                .records
                .load_group(tenant, group)
                .map_err(|e| store_err(&e))
        } else {
            self.inner.load_group(tenant, group).await
        }
    }

    async fn list_groups(&self, tenant: u64) -> std::result::Result<Vec<StoredGroup>, StoreError> {
        if self.gate.tenant_on_engine(tenant) {
            self.eng.records.groups(tenant).map_err(|e| store_err(&e))
        } else {
            self.inner.list_groups(tenant).await
        }
    }

    async fn delete_group(&self, tenant: u64, group: &str) -> std::result::Result<(), StoreError> {
        // Offsets of engine streams are engine records whichever store
        // keeps the group's row.
        self.eng
            .records
            .delete_group(tenant, group)
            .await
            .map_err(|e| store_err(&e))?;
        if self.gate.tenant_on_engine(tenant) {
            Ok(())
        } else {
            self.inner.delete_group(tenant, group).await
        }
    }
}

/// The data plane: the engine's for engine streams, `inner`'s otherwise.
pub struct RoutedData {
    eng: Arc<EngineStreams>,
    gate: Arc<dyn Gate>,
    inner: Option<Arc<dyn StreamData>>,
}

impl std::fmt::Debug for RoutedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedData").finish_non_exhaustive()
    }
}

impl RoutedData {
    /// Over the engine streams, the gate and the other data plane (none:
    /// only engine streams are served).
    #[must_use]
    pub fn new(
        eng: Arc<EngineStreams>,
        gate: Arc<dyn Gate>,
        inner: Option<Arc<dyn StreamData>>,
    ) -> Self {
        Self { eng, gate, inner }
    }

    fn inner(&self) -> std::result::Result<&Arc<dyn StreamData>, TopicError> {
        self.inner.as_ref().ok_or_else(|| {
            terr(
                err::UNKNOWN_TOPIC_OR_PARTITION,
                "only streams stored on the engine are served here",
            )
        })
    }

    async fn resolve(
        &self,
        p: &Principal,
        r: Option<TopicRecord>,
        name: &str,
        access: Access,
    ) -> std::result::Result<DataTopic, TopicError> {
        let Some(r) = r else {
            return Err(unknown(name));
        };
        let (db, s) = split(&r.name)?;
        let need = match access {
            Access::Read => Need::Read,
            Access::Write => Need::Write,
        };
        if !self.gate.allowed(p, db, s, need).await {
            let other = match access {
                Access::Read => Need::Write,
                Access::Write => Need::Read,
            };
            if !self.gate.allowed(p, db, s, other).await {
                return Err(unknown(name));
            }
            return Err(terr(
                err::TOPIC_AUTHORIZATION_FAILED,
                format!(
                    "1142 {} denied to user '{}' on topic {name}",
                    if access == Access::Read {
                        "SELECT"
                    } else {
                        "INSERT"
                    },
                    p.user
                ),
            ));
        }
        Ok(self.eng.data_topic(p.tenant_id, &r))
    }
}

#[async_trait]
impl StreamData for RoutedData {
    async fn topic(
        &self,
        p: &Principal,
        name: &str,
        access: Access,
    ) -> std::result::Result<DataTopic, TopicError> {
        let on = match split(name) {
            Ok((db, _)) => self.gate.on_engine(p, db).await,
            Err(_) => false,
        };
        if !on {
            return self.inner()?.topic(p, name, access).await;
        }
        let r = self
            .eng
            .topic(p.tenant_id, name)
            .map_err(|e| internal(&e))?;
        self.resolve(p, r, name, access).await
    }

    async fn topic_by_id(
        &self,
        p: &Principal,
        id: Uuid,
        access: Access,
    ) -> std::result::Result<DataTopic, TopicError> {
        let (tenant, stream) = id.as_u64_pair();
        if !is_engine_stream(stream) {
            return self.inner()?.topic_by_id(p, id, access).await;
        }
        let unknown_id = || terr(err::UNKNOWN_TOPIC_ID, format!("no topic with id {id}"));
        if tenant != p.tenant_id {
            return Err(unknown_id());
        }
        let r = self
            .eng
            .topic_by_id(tenant, stream)
            .map_err(|e| internal(&e))?
            .ok_or_else(unknown_id)?;
        let name = r.name.clone();
        self.resolve(p, Some(r), &name, access).await
    }

    async fn append(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        records: Vec<ProduceRecord>,
        producer: Option<ProducerBatch>,
    ) -> std::result::Result<AppendTicket, TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self
                .inner()?
                .append(p, t, partition, records, producer)
                .await;
        }
        let bytes = encode_records(&records, producer).map_err(|e| internal(&e))?;
        self.append_batch(p, t, partition, &bytes, &records, producer)
            .await
    }

    async fn sequenced(
        &self,
        p: &Principal,
        t: &DataTopic,
        ticket: &AppendTicket,
        deadline: Instant,
    ) -> std::result::Result<i64, TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self.inner()?.sequenced(p, t, ticket, deadline).await;
        }
        self.eng
            .outcome(
                pending_of(
                    ticket,
                    self.eng
                        .log()
                        .unit_of_part(key(p.tenant_id, t.stream_id, ticket.partition)),
                ),
                deadline,
            )
            .await
    }

    async fn bounds(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
    ) -> std::result::Result<Bounds, TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self.inner()?.bounds(p, t, partition).await;
        }
        Ok(EngineStreams::bounds_of(self.eng.log.bounds(key(
            p.tenant_id,
            t.stream_id,
            partition,
        ))))
    }

    async fn read(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> std::result::Result<(Bounds, Vec<StoredRecord>), TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self.inner()?.read(p, t, partition, offset, max_bytes).await;
        }
        let (b, bytes) =
            self.eng
                .read_raw(key(p.tenant_id, t.stream_id, partition), offset, max_bytes)?;
        let recs = decode_stored(&bytes, offset).map_err(|e| internal(&e))?;
        Ok((b, recs))
    }

    async fn wait(&self, p: &Principal, beyond: &[(u64, i32, i64)], deadline: Instant) {
        let (mine, theirs): (Vec<(u64, i32, i64)>, Vec<(u64, i32, i64)>) =
            beyond.iter().partition(|(s, _, _)| is_engine_stream(*s));
        let keys: Vec<(Key, i64)> = mine
            .iter()
            .map(|(s, part, o)| (key(p.tenant_id, *s, *part), *o))
            .collect();
        match (&self.inner, keys.is_empty(), theirs.is_empty()) {
            (_, true, true) | (None, true, false) => tokio::time::sleep_until(deadline).await,
            (_, false, true) | (None, false, false) => self.eng.log.wait(&keys, deadline).await,
            (Some(i), true, false) => i.wait(p, &theirs, deadline).await,
            (Some(i), false, false) => {
                tokio::select! {
                    () = self.eng.log.wait(&keys, deadline) => {}
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
    ) -> std::result::Result<Option<(i64, i64)>, TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self.inner()?.offset_for_time(p, t, partition, ts).await;
        }
        self.eng
            .log
            .offset_for_time(key(p.tenant_id, t.stream_id, partition), ts)
            .map_err(|e| terr(err::KAFKA_STORAGE_ERROR, e.to_string()))
    }

    async fn epoch_end(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        epoch: i32,
    ) -> std::result::Result<(i32, i64), TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self.inner()?.epoch_end(p, t, partition, epoch).await;
        }
        Ok(self
            .eng
            .log
            .epoch_end(key(p.tenant_id, t.stream_id, partition), epoch))
    }

    async fn leader_epoch(&self, p: &Principal, stream_id: u64, partition: i32) -> i32 {
        if !is_engine_stream(stream_id) {
            return match &self.inner {
                Some(i) => i.leader_epoch(p, stream_id, partition).await,
                None => 0,
            };
        }
        self.eng
            .log
            .bounds(key(p.tenant_id, stream_id, partition))
            .epoch
    }

    async fn limits(&self, p: &Principal) -> Limits {
        match &self.inner {
            Some(i) => i.limits(p).await,
            None => self.gate.limits(p).await,
        }
    }

    fn stores_batches(&self, p: &Principal, t: &DataTopic) -> bool {
        if is_engine_stream(t.stream_id) {
            return true;
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
    ) -> std::result::Result<AppendTicket, TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self
                .inner()?
                .append_batch(p, t, partition, batch, records, producer)
                .await;
        }
        for r in records {
            let (key, size) = record_size(r);
            if let Err(e) = crate::publish::check_size(key, size, t.max_message_bytes as u64) {
                return Err(terr(
                    err::MESSAGE_TOO_LARGE,
                    format!("{} {}", e.protocol().mysql_errno, e.message),
                ));
            }
        }
        let count = Head::parse(batch).map_or(0, |h| usize::try_from(h.count).unwrap_or(0));
        let pending = self
            .eng
            .propose(key(p.tenant_id, t.stream_id, partition), batch)
            .map_err(|e| append_err(&e))?;
        Ok(ticket_of(pending, partition, count))
    }

    async fn read_batches(
        &self,
        p: &Principal,
        t: &DataTopic,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> std::result::Result<(Bounds, Vec<u8>, i64), TopicError> {
        if !is_engine_stream(t.stream_id) {
            return self
                .inner()?
                .read_batches(p, t, partition, offset, max_bytes)
                .await;
        }
        let (b, bytes) =
            self.eng
                .read_raw(key(p.tenant_id, t.stream_id, partition), offset, max_bytes)?;
        let next = end_of(&bytes).unwrap_or(offset);
        Ok((b, bytes, next))
    }
}
