//! Engine records of streams stored on the engine: the topic catalog,
//! consumer-group rows and committed offsets, in engine transactions, so
//! an offset commit can share a SQL statement's transaction.
//!
//! ```text
//! object OBJECT of the tenant (shard 0):
//!   M | 1 | bytes(name)                                  -> topic record (JSON)
//!   M | 2 | bytes(group) | instance u64 | stream u64 | partition u32 -> offset (JSON)
//!   M | 6 | bytes(group)                                 -> group row (JSON)
//!   M | 7 | member u64 | stream u64                      -> fan-in topic id u64
//!   M | 8 | fan-in topic id u64                          -> member u64 | stream u64
//!   C | 0                                                -> last stream id u64
//!   C | 7                                                -> last fan-in topic id u64
//! ```
//!
//! A fan-in topic id (from [`FIRST_FANIN_ID`]) names, in a reader's
//! tenant, a member's instance of a stream the reader declares (Kafka's
//! `__t.<member>.<db>.<stream>`): given once, kept for good, so the
//! topic's id and a group's committed offsets on it stay put.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};
use sprig_engine::Engine;
use sprig_engine::keys::{KeyPrefix, KeyRange, kind, memcmp};
use sprig_engine::txn::LockRequest;
use sprig_engine::txn::{Dir, Isolation, ReadMode, Txn, TxnOptions};
use sprig_proto_kafka::group::{CommittedOffset, GroupState, StoredGroup};

/// The tenant's streams object (`'K'` for Kafka).
pub const OBJECT: u32 = 0x4B00_0000;
const TOPICS: u16 = 1;
const OFFSETS: u16 = 2;
const GROUPS: u16 = 6;
const FANIN_IDS: u16 = 7;
const FANIN_TARGETS: u16 = 8;

/// The first fan-in topic id: above every stream id the records give.
pub const FIRST_FANIN_ID: u64 = 1 << 62;

/// Whether `stream` is a fan-in topic id.
#[must_use]
pub const fn is_fan_in(stream: u64) -> bool {
    stream >= FIRST_FANIN_ID
}

/// A stream stored on the engine, as its catalog record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    /// `<database>.<stream>`.
    pub name: String,
    /// Its id (never reused within the tenant).
    pub stream_id: u64,
    /// Partitions.
    pub partitions: i32,
    /// `retention.ms`, when set on the stream.
    pub retention_ms: Option<i64>,
    /// `retention.bytes`, when set.
    pub retention_bytes: Option<i64>,
    /// `cleanup.policy=compact`, when set.
    pub compact: Option<bool>,
    /// `max.message.bytes`, when set.
    pub max_message_bytes: Option<i32>,
    /// Created (ms).
    pub created_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetRow {
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
    committed_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupRow {
    protocol_type: String,
    generation: i32,
    state: String,
}

fn prefix(tenant: u64, kind: u8, index: u16) -> Vec<u8> {
    KeyPrefix {
        tenant,
        shard: 0,
        object: OBJECT,
        kind,
        index,
    }
    .to_vec()
}

fn topic_key(tenant: u64, name: &str) -> Vec<u8> {
    let mut k = prefix(tenant, kind::META, TOPICS);
    memcmp::put_bytes(&mut k, name.as_bytes());
    k
}

fn counter_key(tenant: u64) -> Vec<u8> {
    prefix(tenant, kind::COUNTERS, 0)
}

fn fanin_id_key(reader: u64, member: u64, stream: u64) -> Vec<u8> {
    let mut k = prefix(reader, kind::META, FANIN_IDS);
    memcmp::put_u64(&mut k, member);
    memcmp::put_u64(&mut k, stream);
    k
}

fn fanin_target_key(reader: u64, id: u64) -> Vec<u8> {
    let mut k = prefix(reader, kind::META, FANIN_TARGETS);
    memcmp::put_u64(&mut k, id);
    k
}

fn group_prefix(tenant: u64, index: u16, group: &str) -> Vec<u8> {
    let mut k = prefix(tenant, kind::META, index);
    memcmp::put_bytes(&mut k, group.as_bytes());
    k
}

/// The key of one committed offset (`instance` is 0 except for fan-in).
#[must_use]
pub fn offset_key(tenant: u64, group: &str, instance: u64, stream: u64, partition: i32) -> Vec<u8> {
    let mut k = group_prefix(tenant, OFFSETS, group);
    memcmp::put_u64(&mut k, instance);
    memcmp::put_u64(&mut k, stream);
    memcmp::put_i32(&mut k, partition);
    k
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("stream records: {what}"))
}

fn json<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| bad(&e.to_string()))
}

fn parse<T: for<'a> Deserialize<'a>>(b: &[u8]) -> Result<T> {
    serde_json::from_slice(b).map_err(|e| bad(&format!("corrupt record: {e}")))
}

/// Puts committed offsets into `txn` (a Kafka `OffsetCommit`, or
/// `sprig.commit_offset` inside a SQL transaction).
///
/// # Errors
/// A lock or write error of the transaction.
pub async fn put_offsets(
    txn: &mut Txn,
    tenant: u64,
    group: &str,
    instance: u64,
    offsets: &[CommittedOffset],
    now_ms: i64,
) -> Result<()> {
    for o in offsets {
        let k = Bytes::from(offset_key(
            tenant,
            group,
            instance,
            o.stream_id,
            o.partition,
        ));
        txn.lock(&k, LockRequest::write()).await?;
        txn.put(
            k.to_vec(),
            json(&OffsetRow {
                offset: o.offset,
                leader_epoch: o.leader_epoch,
                metadata: o.metadata.clone(),
                committed_ms: now_ms,
            })?,
        )?;
    }
    Ok(())
}

/// Moves one committed offset of `group` from `expect` (absent counts as 0)
/// to `offset` in `txn`, the row locked first: `false` when it holds
/// another value (someone else moved it), and nothing is written. A
/// trigger's dead-letter replay marks its run this way, so a repeated run
/// finds the mark and stops.
///
/// # Errors
/// A lock, read or write error of the transaction.
#[allow(clippy::too_many_arguments)] // the key's parts and the move
pub async fn move_offset(
    txn: &mut Txn,
    tenant: u64,
    group: &str,
    stream: u64,
    partition: i32,
    expect: i64,
    offset: i64,
    now_ms: i64,
) -> Result<bool> {
    move_offset_on(
        txn, tenant, group, 0, stream, partition, expect, offset, now_ms,
    )
    .await
}

/// [`move_offset`] of a position on an instance (0 for the tenant's own
/// streams, the member for a fan-in reader's position).
///
/// # Errors
/// A lock, read or write error of the transaction.
#[allow(clippy::too_many_arguments)] // the key's parts and the move
pub async fn move_offset_on(
    txn: &mut Txn,
    tenant: u64,
    group: &str,
    instance: u64,
    stream: u64,
    partition: i32,
    expect: i64,
    offset: i64,
    now_ms: i64,
) -> Result<bool> {
    let k = Bytes::from(offset_key(tenant, group, instance, stream, partition));
    txn.lock(&k, LockRequest::write()).await?;
    let have = match txn.read_current(&k)? {
        Some(b) => parse::<OffsetRow>(&b)?.offset,
        None => 0,
    };
    if have != expect {
        return Ok(false);
    }
    txn.put(
        k.to_vec(),
        json(&OffsetRow {
            offset,
            leader_epoch: None,
            metadata: None,
            committed_ms: now_ms,
        })?,
    )?;
    Ok(true)
}

/// A group's committed offset of `(stream, partition)` as `txn` reads it
/// now (the newest committed value or its own write, 0 when there is
/// none), without a lock.
///
/// # Errors
/// The engine failed or the record is corrupt.
pub fn read_offset(
    txn: &mut Txn,
    tenant: u64,
    group: &str,
    stream: u64,
    partition: i32,
) -> Result<i64> {
    let k = offset_key(tenant, group, 0, stream, partition);
    Ok(match txn.read_current(&k)? {
        Some(b) => parse::<OffsetRow>(&b)?.offset,
        None => 0,
    })
}

/// The engine records of streams.
#[derive(Clone)]
pub struct StreamRecords {
    engine: Engine,
    first: u64,
}

impl std::fmt::Debug for StreamRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRecords").finish_non_exhaustive()
    }
}

impl StreamRecords {
    /// Over `engine`.
    #[must_use]
    pub fn new(engine: Engine) -> Self {
        Self::with_first_id(engine, 1)
    }

    /// Over `engine`, giving new streams ids from `first` on (so they
    /// never meet ids given elsewhere).
    #[must_use]
    pub fn with_first_id(engine: Engine, first: u64) -> Self {
        Self {
            engine,
            first: first.max(1),
        }
    }

    fn begin(&self) -> Result<Txn> {
        self.engine
            .begin(TxnOptions::mysql().with_isolation(Isolation::ReadCommitted))
    }

    async fn commit(t: Txn) -> Result<()> {
        let gate = t.reply_gate();
        t.commit()?.wait_async().await?;
        gate.wait_async().await
    }

    /// Reader `reader`'s fan-in topic id of member `member`'s stream
    /// `stream`: the one given before, else a new one (recorded both
    /// ways, in one transaction).
    ///
    /// # Errors
    /// The engine's.
    pub async fn fan_in_id(&self, reader: u64, member: u64, stream: u64) -> Result<u64> {
        let k = fanin_id_key(reader, member, stream);
        let u64_of = |b: &[u8]| <[u8; 8]>::try_from(b).ok().map(u64::from_be_bytes);
        {
            let mut t = self.begin()?;
            let have = t.read_current(&k)?;
            t.rollback();
            if let Some(id) = have.as_deref().and_then(u64_of) {
                return Ok(id);
            }
        }
        let mut t = self.begin()?;
        let counter = Bytes::from(prefix(reader, kind::COUNTERS, FANIN_IDS));
        t.lock(&counter, LockRequest::write()).await?;
        if let Some(id) = t.read_current(&k)?.as_deref().and_then(u64_of) {
            t.rollback();
            return Ok(id);
        }
        let last = t
            .read_current(&counter)?
            .as_deref()
            .and_then(u64_of)
            .unwrap_or(FIRST_FANIN_ID - 1);
        let id = last.max(FIRST_FANIN_ID - 1) + 1;
        t.put(counter.to_vec(), id.to_be_bytes().to_vec())?;
        t.put(k, id.to_be_bytes().to_vec())?;
        let mut target = member.to_be_bytes().to_vec();
        target.extend_from_slice(&stream.to_be_bytes());
        t.put(fanin_target_key(reader, id), target)?;
        Self::commit(t).await?;
        Ok(id)
    }

    /// The member and stream reader `reader`'s fan-in topic id `id`
    /// names, if it was given.
    ///
    /// # Errors
    /// The engine's.
    pub fn fan_in_target(&self, reader: u64, id: u64) -> Result<Option<(u64, u64)>> {
        let mut t = self.begin()?;
        let v = t.read_current(&fanin_target_key(reader, id))?;
        t.rollback();
        Ok(v.filter(|b| b.len() == 16).map(|b| {
            let mut m = [0u8; 8];
            let mut s = [0u8; 8];
            m.copy_from_slice(&b[..8]);
            s.copy_from_slice(&b[8..]);
            (u64::from_be_bytes(m), u64::from_be_bytes(s))
        }))
    }

    fn scan(&self, r: &KeyRange) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let mut t = self.begin()?;
        let mut out = Vec::new();
        let mut from = r.start.clone();
        loop {
            let page = t.scan(
                &KeyRange {
                    start: from.clone(),
                    end: r.end.clone(),
                },
                Dir::Forward,
                512,
                ReadMode::Snapshot,
            )?;
            let n = page.len();
            if let Some((k, _)) = page.last() {
                from = after(k);
            }
            out.extend(page);
            if n < 512 {
                break;
            }
        }
        t.rollback();
        Ok(out)
    }

    /// Every topic of a tenant, by name.
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn topics(&self, tenant: u64) -> Result<Vec<TopicRecord>> {
        self.scan(&KeyRange::prefix(&prefix(tenant, kind::META, TOPICS)))?
            .iter()
            .map(|(_, v)| parse(v))
            .collect()
    }

    /// One topic.
    ///
    /// # Errors
    /// As [`Self::topics`].
    pub fn topic(&self, tenant: u64, name: &str) -> Result<Option<TopicRecord>> {
        let mut t = self.begin()?;
        let v = t.get_snapshot(&topic_key(tenant, name))?;
        t.rollback();
        v.map(|b| parse(&b)).transpose()
    }

    /// Creates a topic, giving it the next stream id; `Ok(None)` when the
    /// name is taken.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn create(&self, tenant: u64, mut rec: TopicRecord) -> Result<Option<TopicRecord>> {
        let mut t = self.begin()?;
        let k = Bytes::from(topic_key(tenant, &rec.name));
        t.lock(&k, LockRequest::write()).await?;
        if t.read_current(&k)?.is_some() {
            t.rollback();
            return Ok(None);
        }
        let c = Bytes::from(counter_key(tenant));
        t.lock(&c, LockRequest::write()).await?;
        let last = t
            .read_current(&c)?
            .map(|b| <[u8; 8]>::try_from(b.as_ref()).map(u64::from_be_bytes))
            .transpose()
            .map_err(|_| bad("corrupt stream counter"))?
            .unwrap_or(0);
        rec.stream_id = last.max(self.first - 1) + 1;
        t.put(c.to_vec(), rec.stream_id.to_be_bytes().to_vec())?;
        t.put(k.to_vec(), json(&rec)?)?;
        Self::commit(t).await?;
        Ok(Some(rec))
    }

    /// Replaces a topic's record (partitions, configs); `Ok(false)` when it
    /// is gone.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn update(&self, tenant: u64, rec: &TopicRecord) -> Result<bool> {
        let mut t = self.begin()?;
        let k = Bytes::from(topic_key(tenant, &rec.name));
        t.lock(&k, LockRequest::write()).await?;
        if t.read_current(&k)?.is_none() {
            t.rollback();
            return Ok(false);
        }
        t.put(k.to_vec(), json(rec)?)?;
        Self::commit(t).await?;
        Ok(true)
    }

    /// Deletes a topic's record; the removed record, if it existed.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn delete(&self, tenant: u64, name: &str) -> Result<Option<TopicRecord>> {
        let mut t = self.begin()?;
        let k = Bytes::from(topic_key(tenant, name));
        t.lock(&k, LockRequest::write()).await?;
        let Some(old) = t.read_current(&k)? else {
            t.rollback();
            return Ok(None);
        };
        let rec: TopicRecord = parse(&old)?;
        t.delete(k.to_vec())?;
        Self::commit(t).await?;
        Ok(Some(rec))
    }

    /// Commits offsets in their own transaction.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn commit_offsets(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
        now_ms: i64,
    ) -> Result<()> {
        let mut t = self.begin()?;
        put_offsets(&mut t, tenant, group, 0, offsets, now_ms).await?;
        Self::commit(t).await
    }

    /// Commits offsets, each on its instance (0 for the tenant's own
    /// streams, the member for a fan-in reader's position), in one
    /// transaction.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn commit_offsets_on(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[(u64, CommittedOffset)],
        now_ms: i64,
    ) -> Result<()> {
        let mut t = self.begin()?;
        for (instance, o) in offsets {
            put_offsets(
                &mut t,
                tenant,
                group,
                *instance,
                std::slice::from_ref(o),
                now_ms,
            )
            .await?;
        }
        Self::commit(t).await
    }

    /// Every committed offset of a group with its instance (0 for the
    /// tenant's own streams).
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn offsets_on(&self, tenant: u64, group: &str) -> Result<Vec<(u64, CommittedOffset)>> {
        let p = group_prefix(tenant, OFFSETS, group);
        let mut out = Vec::new();
        for (k, v) in self.scan(&KeyRange::prefix(&p))? {
            let rest = &k[p.len()..];
            let (instance, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            let (stream, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            let (partition, _) = memcmp::get_int(rest, 4).map_err(|_| bad("offset key"))?;
            let partition = i32::try_from(partition).map_err(|_| bad("offset key"))?;
            let row: OffsetRow = parse(&v)?;
            out.push((
                instance,
                CommittedOffset {
                    stream_id: stream,
                    partition,
                    offset: row.offset,
                    leader_epoch: row.leader_epoch,
                    metadata: row.metadata,
                },
            ));
        }
        Ok(out)
    }

    /// A group's committed offsets: all, or those of `only`.
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn offsets(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>> {
        let p = group_prefix(tenant, OFFSETS, group);
        let mut out = Vec::new();
        for (k, v) in self.scan(&KeyRange::prefix(&p))? {
            let rest = &k[p.len()..];
            let (instance, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            if instance != 0 {
                continue;
            }
            let (stream, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            let (partition, _) = memcmp::get_int(rest, 4).map_err(|_| bad("offset key"))?;
            let partition = i32::try_from(partition).map_err(|_| bad("offset key"))?;
            if only.is_some_and(|o| !o.contains(&(stream, partition))) {
                continue;
            }
            let row: OffsetRow = parse(&v)?;
            out.push(CommittedOffset {
                stream_id: stream,
                partition,
                offset: row.offset,
                leader_epoch: row.leader_epoch,
                metadata: row.metadata,
            });
        }
        Ok(out)
    }

    /// Writes a group's row.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn save_group(&self, tenant: u64, g: &StoredGroup, state: GroupState) -> Result<()> {
        let mut t = self.begin()?;
        let k = Bytes::from(group_prefix(tenant, GROUPS, &g.group));
        t.lock(&k, LockRequest::write()).await?;
        t.put(
            k.to_vec(),
            json(&GroupRow {
                protocol_type: g.protocol_type.clone(),
                generation: g.generation,
                state: state.stored().to_owned(),
            })?,
        )?;
        Self::commit(t).await
    }

    /// A group's row.
    ///
    /// # Errors
    /// The engine failed or the record is corrupt.
    pub fn load_group(&self, tenant: u64, group: &str) -> Result<Option<StoredGroup>> {
        let mut t = self.begin()?;
        let v = t.get_snapshot(&group_prefix(tenant, GROUPS, group))?;
        t.rollback();
        v.map(|b| {
            let r: GroupRow = parse(&b)?;
            Ok(StoredGroup {
                group: group.to_owned(),
                protocol_type: r.protocol_type,
                generation: r.generation,
            })
        })
        .transpose()
    }

    /// The names of every group of a tenant that holds a committed offset,
    /// in name order (a group that commits from SQL has offsets and no
    /// group row).
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn offset_groups(&self, tenant: u64) -> Result<Vec<String>> {
        let p = prefix(tenant, kind::META, OFFSETS);
        let mut out: Vec<String> = Vec::new();
        for (k, _) in self.scan(&KeyRange::prefix(&p))? {
            let (name, _) = memcmp::get_bytes(&k[p.len()..]).map_err(|_| bad("offset key"))?;
            let name = String::from_utf8_lossy(&name).into_owned();
            if out.last() != Some(&name) {
                out.push(name);
            }
        }
        Ok(out)
    }

    /// As [`offsets_on`](Self::offsets_on), with each offset's commit time
    /// (ms since the epoch).
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn offsets_on_timed(
        &self,
        tenant: u64,
        group: &str,
    ) -> Result<Vec<(u64, CommittedOffset, i64)>> {
        let p = group_prefix(tenant, OFFSETS, group);
        let mut out = Vec::new();
        for (k, v) in self.scan(&KeyRange::prefix(&p))? {
            let rest = &k[p.len()..];
            let (instance, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            let (stream, rest) = memcmp::get_uint(rest, 8).map_err(|_| bad("offset key"))?;
            let (partition, _) = memcmp::get_int(rest, 4).map_err(|_| bad("offset key"))?;
            let partition = i32::try_from(partition).map_err(|_| bad("offset key"))?;
            let row: OffsetRow = parse(&v)?;
            out.push((
                instance,
                CommittedOffset {
                    stream_id: stream,
                    partition,
                    offset: row.offset,
                    leader_epoch: row.leader_epoch,
                    metadata: row.metadata,
                },
                row.committed_ms,
            ));
        }
        Ok(out)
    }

    /// Every group row of a tenant as stored: name, generation and state
    /// (`dead` ones included).
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn group_states(&self, tenant: u64) -> Result<Vec<(String, i32, String)>> {
        let p = prefix(tenant, kind::META, GROUPS);
        let mut out = Vec::new();
        for (k, v) in self.scan(&KeyRange::prefix(&p))? {
            let (name, _) = memcmp::get_bytes(&k[p.len()..]).map_err(|_| bad("group key"))?;
            let r: GroupRow = parse(&v)?;
            out.push((
                String::from_utf8_lossy(&name).into_owned(),
                r.generation,
                r.state,
            ));
        }
        Ok(out)
    }

    /// Every group row of a tenant.
    ///
    /// # Errors
    /// The engine failed or a record is corrupt.
    pub fn groups(&self, tenant: u64) -> Result<Vec<StoredGroup>> {
        let p = prefix(tenant, kind::META, GROUPS);
        let mut out = Vec::new();
        for (k, v) in self.scan(&KeyRange::prefix(&p))? {
            let (name, _) = memcmp::get_bytes(&k[p.len()..]).map_err(|_| bad("group key"))?;
            let r: GroupRow = parse(&v)?;
            if r.state == "dead" {
                continue;
            }
            out.push(StoredGroup {
                group: String::from_utf8_lossy(&name).into_owned(),
                protocol_type: r.protocol_type,
                generation: r.generation,
            });
        }
        Ok(out)
    }

    /// Deletes a group's row and offsets.
    ///
    /// # Errors
    /// The engine failed.
    pub async fn delete_group(&self, tenant: u64, group: &str) -> Result<()> {
        let keys: Vec<Vec<u8>> = self
            .scan(&KeyRange::prefix(&group_prefix(tenant, OFFSETS, group)))?
            .into_iter()
            .map(|(k, _)| k)
            .chain(std::iter::once(group_prefix(tenant, GROUPS, group)))
            .collect();
        let mut t = self.begin()?;
        for k in keys {
            let b = Bytes::from(k);
            t.lock(&b, LockRequest::write()).await?;
            t.delete(b.to_vec())?;
        }
        Self::commit(t).await
    }
}

/// The byte successor of `k`: where the next page starts (no key of this
/// layout is a proper prefix of another, so nothing is skipped).
fn after(k: &[u8]) -> Vec<u8> {
    let mut v = k.to_vec();
    while let Some(last) = v.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return v;
        }
        v.pop();
    }
    v
}
