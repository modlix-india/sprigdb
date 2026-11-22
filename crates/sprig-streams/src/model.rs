//! Stream records and their small value types, shared by the engine's
//! stream log, change-record envelopes and the Kafka front end.

/// A stream's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

/// A partition of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionNo(pub u32);

/// An offset in a partition: the record's position, from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Offset(pub u64);

/// The sequencer's leader epoch: raised by every new sequencer of a log
/// shard, and stored with each record it appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct LeaderEpoch(pub u32);

/// `streams.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// Application events, published through the outbox.
    Events,
    /// A change topic of an object, from the change feed.
    Changes,
}

impl StreamKind {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            StreamKind::Events => "events",
            StreamKind::Changes => "changes",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "events" => Some(StreamKind::Events),
            "changes" => Some(StreamKind::Changes),
            _ => None,
        }
    }
}

/// `streams.cleanup`: Kafka's `cleanup.policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cleanup {
    /// Retention by time and size.
    Delete,
    /// Compaction by key (and retention).
    Compact,
}

impl Cleanup {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Cleanup::Delete => "delete",
            Cleanup::Compact => "compact",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "delete" => Some(Cleanup::Delete),
            "compact" => Some(Cleanup::Compact),
            _ => None,
        }
    }
}

/// `streams.envelope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Envelope {
    /// The change envelope (change topics).
    ChangeJson,
    /// The value as published (event streams).
    Raw,
}

impl Envelope {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Envelope::ChangeJson => "change_json",
            Envelope::Raw => "raw",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "change_json" => Some(Envelope::ChangeJson),
            "raw" => Some(Envelope::Raw),
            _ => None,
        }
    }
}

/// `streams.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamState {
    /// Tables being created.
    Creating,
    /// In use.
    Active,
    /// Gone from clients; tables and offsets being removed.
    Dropping,
}

impl StreamState {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            StreamState::Creating => "creating",
            StreamState::Active => "active",
            StreamState::Dropping => "dropping",
        }
    }

    /// From the catalog's text.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "creating" => Some(StreamState::Creating),
            "active" => Some(StreamState::Active),
            "dropping" => Some(StreamState::Dropping),
            _ => None,
        }
    }
}

/// The change topic of object `object`: `<object>.changes`.
#[must_use]
pub fn change_topic_name(object: &str) -> String {
    format!("{object}.changes")
}

/// Whether `name` is a valid stream name: Kafka's topic rules (1 to 249
/// characters of `[a-zA-Z0-9._-]`, not `.` or `..`).
#[must_use]
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// A record as read from the log (the log's `Record`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Its partition.
    pub partition: PartitionNo,
    /// Its offset.
    pub offset: Offset,
    /// Its timestamp (ms since the Unix epoch).
    pub ts_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value; `None` is a tombstone.
    pub value: Option<Vec<u8>>,
    /// Headers (`None`: a header without a value).
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    /// The leader epoch that appended it.
    pub leader_epoch: LeaderEpoch,
}

/// Where a record came from: its idempotency key in the log
/// (`source_event`, `uk_source`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Source {
    /// An outbox row: its `event_id` (UUIDv7).
    Outbox([u8; 16]),
    /// A change event: its feed `EventId`, encoded.
    Change(Vec<u8>),
}

impl Source {
    /// The bytes stored in `source_event`: `e` + the event id, or `c` +
    /// the encoded feed event id.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Source::Outbox(id) => {
                let mut v = Vec::with_capacity(17);
                v.push(b'e');
                v.extend_from_slice(id);
                v
            }
            Source::Change(b) => {
                let mut v = Vec::with_capacity(b.len() + 1);
                v.push(b'c');
                v.extend_from_slice(b);
                v
            }
        }
    }

    /// The feed's event id as `source_event` bytes (at most 64).
    #[must_use]
    pub fn of_event(id: &sprig_feed::EventId) -> Self {
        let c = id.cluster.to_string();
        let mut v = Vec::with_capacity(c.len() + 21);
        v.push(u8::try_from(c.len().min(40)).unwrap_or(40));
        v.extend_from_slice(&c.as_bytes()[..c.len().min(40)]);
        v.extend_from_slice(&id.gtid.domain.to_be_bytes());
        v.extend_from_slice(&id.gtid.server.to_be_bytes());
        v.extend_from_slice(&id.gtid.seq.to_be_bytes());
        v.extend_from_slice(&id.row.to_be_bytes());
        Source::Change(v)
    }
}

/// A record before sequencing: what the sequencer appends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRecord {
    /// The partition (the outbox's, a producer's explicit one, or the
    /// partitioner's).
    pub partition: PartitionNo,
    /// Timestamp (ms since the Unix epoch).
    pub ts_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value; `None` is a tombstone.
    pub value: Option<Vec<u8>>,
    /// Headers as stored (a JSON object), if any.
    pub headers: Option<String>,
    /// The idempotency key.
    pub source: Source,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(valid_name("order_events"));
        assert!(valid_name("orders.changes"));
        assert!(valid_name("a-b.c_9"));
        assert!(!valid_name(".."));
        assert!(!valid_name("a b"));
        assert!(!valid_name(&"x".repeat(250)));
        assert_eq!(change_topic_name("orders"), "orders.changes");
        let e = Source::Outbox([7; 16]).to_bytes();
        assert_eq!(e.len(), 17);
        assert_eq!(e[0], b'e');
    }
}
