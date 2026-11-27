//! A queue of an engine database read as a table: SQL `SELECT`s (joins,
//! grouping, subqueries, prepared statements) over the messages an AMQP
//! queue holds, through the same source the planner reads streams by
//! ([`super::streamtable::StreamSource`]).
//!
//! ```text
//! columns  lane SMALLINT UNSIGNED, priority_class TINYINT UNSIGNED,
//!          seq BIGINT UNSIGNED, published DATETIME(3), exchange, routing_key,
//!          message_id, priority, headers JSON, body LONGBLOB,
//!          delivery_count INT UNSIGNED, expires DATETIME(3)
//! key      PRIMARY (lane, priority_class, seq): the order the queue
//!          delivers in, lane by lane
//! storage  the queue's stored messages, read as they are now (messages
//!          aren't versioned): delivered but unacknowledged ones included
//! ```
//!
//! - **Read-only.** Publishing, getting and acknowledging stay the queue
//!   calls' (`sprig.mq_*`) and the AMQP port's; a write naming a queue, and
//!   a locking read of one, are refused as for a stream.
//! - **Which queues.** The durable classic and quorum queues of the
//!   database's vhost, by name, as the tenant's catalog holds them now; a
//!   table, view, sequence, collection or stream of the same name comes
//!   first. Stream queues are streams already.
//! - **Values.** `published` is the message id's time (UTC); `headers` the
//!   AMQP 0-9-1 headers (1.0 application properties) as JSON; `body` the
//!   payload as stored (an AMQP 1.0 message's body sections encoded).

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::{DbId, ObjectId};
use sprig_engine::row::{Date, DateTime, Value};
use sprig_exec::SqlError;
use sprig_exec::ops::Row;
use sprig_plan::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use sprig_plan::catalog::{KeyId, TableDef as PlanTable};
use sprig_queues::enginestore::EngineMsgStore;
use sprig_queues::store::StoredMsg;

use super::streamtable::StreamSource;
use super::table;

/// Where queues' table objects are numbered (apart from tables',
/// collections', streams' and temporary tables').
const QUEUE_BASE: u64 = 0xA000_0000;

/// Messages read per store call.
const BATCH: usize = 512;

/// Messages counted for a queue's size; beyond it the queue is "large".
const COUNT_CAP: usize = 10_000;

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn table_text(name: &str) -> String {
    format!(
        "CREATE TABLE {} (\n  `lane` smallint(5) unsigned NOT NULL,\n  `priority_class` tinyint(3) unsigned NOT NULL,\n  `seq` bigint(20) unsigned NOT NULL,\n  `published` datetime(3) DEFAULT NULL,\n  `exchange` varchar(255) DEFAULT NULL,\n  `routing_key` varchar(255) DEFAULT NULL,\n  `message_id` varchar(255) DEFAULT NULL,\n  `priority` tinyint(3) unsigned DEFAULT NULL,\n  `headers` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`headers`)),\n  `body` longblob DEFAULT NULL,\n  `delivery_count` int(10) unsigned NOT NULL,\n  `expires` datetime(3) DEFAULT NULL,\n  PRIMARY KEY (`lane`,`priority_class`,`seq`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        quote(name)
    )
}

/// Queue `name` of engine database `db` of the tenant of `cat`, as the
/// catalog holds it now; `None` when there is none (or it is a stream
/// queue, or messages aren't on the engine).
pub(crate) fn find(
    cat: &TenantCatalog,
    db: DbId,
    name: &str,
    lower_case: bool,
) -> Option<Arc<StreamSource>> {
    let state = cat
        .ext
        .get::<sprig_queues::catalog::MqState>(sprig_queues::catalog::MODULE)?;
    let topo = state.dbs.get(&db.0)?;
    let q = topo.queues.get(name).or_else(|| {
        if !lower_case {
            return None;
        }
        let mut hits = topo
            .queues
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name));
        match (hits.next(), hits.next()) {
            (Some((_, q)), None) => Some(q),
            _ => None,
        }
    })?;
    if matches!(q.qtype, sprig_queues::topology::QType::Stream) {
        return None;
    }
    let tenant = cat.tenant.0;
    if !crate::amqp::queues::messages_on_engine(tenant) {
        return None;
    }
    let store = crate::amqp::queues::engine_queues()?.store(tenant, db.0);
    let object = ObjectId(QUEUE_BASE + (q.id & 0x0FFF_FFFF));
    let def = table::parse(object, 1, db, &table_text(&q.name)).ok()?;
    Some(Arc::new(StreamSource {
        def,
        storage: Arc::new(QueueStorage { store, queue: q.id }),
        redis: None,
    }))
}

/// One queue's messages under the planner.
struct QueueStorage {
    store: Arc<EngineMsgStore>,
    queue: u64,
}

fn failed(e: &sprig_queues::store::StoreError) -> SqlError {
    SqlError::new(1105, "HY000", format!("engine: a queue's messages: {e}"))
}

fn datetime_ms(ms: i64) -> Value {
    let Some(t) = chrono_free(ms) else {
        return Value::Null;
    };
    Value::DateTime(t)
}

/// `ms` since the epoch as a UTC `DATETIME(3)`.
pub(crate) fn chrono_free(ms: i64) -> Option<DateTime> {
    let secs = ms.div_euclid(1000);
    let millis = u32::try_from(ms.rem_euclid(1000)).ok()?;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    Some(DateTime {
        date: Date {
            year: u16::try_from(y).ok()?,
            month: u8::try_from(m).ok()?,
            day: u8::try_from(d).ok()?,
        },
        hour: u8::try_from(rem / 3600).ok()?,
        minute: u8::try_from(rem % 3600 / 60).ok()?,
        second: u8::try_from(rem % 60).ok()?,
        micros: millis * 1000,
    })
}

/// A stored message as a row of the queue's table.
fn row_of(m: &StoredMsg) -> Row {
    let rebuilt = sprig_queues::msg::rebuild(
        m.fmt,
        m.exchange.clone(),
        m.routing_key.clone(),
        m.props.clone(),
        m.body.clone(),
    )
    .ok();
    let info = rebuilt.as_ref().map(|r| &r.info);
    let headers = info.map(|i| sprig_queues::catalog::table_json(&i.headers).to_string());
    let text = |s: &str| Value::Str(Bytes::copy_from_slice(s.as_bytes()));
    vec![
        Value::UInt(u64::from(m.lane)),
        Value::UInt(u64::from(m.pclass)),
        Value::UInt(m.seq),
        datetime_ms(sprig_queues::store::msg_id_ms(&m.msg_id)),
        text(&m.exchange),
        text(&m.routing_key),
        info.and_then(|i| i.message_id.as_deref())
            .map_or(Value::Null, text),
        info.and_then(|i| i.priority)
            .map_or(Value::Null, |p| Value::UInt(u64::from(p))),
        headers.map_or(Value::Null, |h| Value::Json(Bytes::from(h))),
        Value::Bytes(m.body.clone()),
        Value::UInt(u64::from(m.delivery_count)),
        m.expires_at_ms.map_or(Value::Null, datetime_ms),
    ]
}

fn key_of(r: &Row) -> [u64; 3] {
    let n = |i: usize| match r.get(i) {
        Some(Value::UInt(v)) => *v,
        _ => 0,
    };
    [n(0), n(1), n(2)]
}

/// A bound's key parts as numbers (`None`: a part that is no number).
fn parts(v: &[Value]) -> Option<Vec<i128>> {
    v.iter()
        .map(|x| match x {
            Value::UInt(u) => Some(i128::from(*u)),
            Value::Int(i) => Some(i128::from(*i)),
            _ => None,
        })
        .collect()
}

/// Whether key `k` lies in `r` (a bound's leading parts compare with the
/// key's first parts).
fn in_range(k: [u64; 3], r: &KeyRange) -> bool {
    let k: Vec<i128> = k.iter().map(|x| i128::from(*x)).collect();
    let cmp = |b: &[i128]| {
        let n = b.len().min(k.len());
        k[..n].cmp(&b[..n])
    };
    let lo = match &r.start {
        Bound::Unbounded => true,
        Bound::Included(v) => parts(v).is_some_and(|b| cmp(&b).is_ge()),
        Bound::Excluded(v) => parts(v).is_some_and(|b| cmp(&b).is_gt()),
    };
    let hi = match &r.end {
        Bound::Unbounded => true,
        Bound::Included(v) => parts(v).is_some_and(|b| cmp(&b).is_le()),
        Bound::Excluded(v) => parts(v).is_some_and(|b| cmp(&b).is_lt()),
    };
    lo && hi
}

impl QueueStorage {
    /// Every message's row, in key order.
    fn all(&self, counters: &Counters) -> Result<Vec<Row>, SqlError> {
        counters.add_seek();
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let batch = self
                .store
                .browse(self.queue, after, BATCH)
                .map_err(|e| failed(&e))?;
            let full = batch.len() == BATCH;
            if let Some(m) = batch.last() {
                after = Some((m.lane, m.pclass, m.seq));
            }
            out.extend(batch.iter().map(row_of));
            if !full {
                return Ok(out);
            }
        }
    }
}

impl Storage for QueueStorage {
    fn table_rows(&self, _table: &PlanTable) -> u64 {
        self.store
            .browse(self.queue, None, COUNT_CAP)
            .map_or(0, |v| v.len() as u64)
    }

    fn rows_in_range(&self, table: &PlanTable, key: KeyId, range: &KeyRange) -> u64 {
        if key != KeyId::Primary {
            return 0;
        }
        if range.is_point(3) {
            return 1;
        }
        self.table_rows(table)
    }

    fn rows_per_key(&self, _table: &PlanTable, key: KeyId, parts: usize) -> Option<f64> {
        (key == KeyId::Primary && parts == 3).then_some(1.0)
    }

    fn get(
        &self,
        _table: &PlanTable,
        key: &[Value],
        counters: &Counters,
    ) -> Result<Option<Row>, SqlError> {
        let want = parts(key);
        let rows = self.all(counters)?;
        let found = rows.into_iter().find(|r| {
            let k = key_of(r);
            want.as_ref()
                .is_some_and(|w| w.len() == 3 && (0..3).all(|i| i128::from(k[i]) == w[i]))
        });
        if found.is_some() {
            counters.add_row();
        }
        Ok(found)
    }

    fn scan(
        &self,
        _table: &PlanTable,
        key: KeyId,
        ranges: Vec<KeyRange>,
        reverse: bool,
        _fetch: Fetch,
        counters: Arc<Counters>,
    ) -> Result<Box<dyn RowCursor>, SqlError> {
        if key != KeyId::Primary {
            return Err(SqlError::new(
                1105,
                "HY000",
                "engine: a queue has no secondary index",
            ));
        }
        let all = self.all(&counters)?;
        let mut out = std::collections::VecDeque::new();
        for r in &ranges {
            let mut rows: Vec<Row> = all
                .iter()
                .filter(|x| in_range(key_of(x), r))
                .cloned()
                .collect();
            if reverse {
                rows.reverse();
            }
            out.extend(rows);
        }
        for _ in &out {
            counters.add_row();
        }
        Ok(Box::new(Rows(out)))
    }
}

/// Rows already read.
struct Rows(std::collections::VecDeque<Row>);

impl RowCursor for Rows {
    fn next(&mut self) -> Result<Option<Row>, SqlError> {
        Ok(self.0.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_milliseconds_read_as_utc_datetimes() {
        let t = chrono_free(1_700_000_000_123).unwrap();
        assert_eq!(
            (
                t.date.year,
                t.date.month,
                t.date.day,
                t.hour,
                t.minute,
                t.second,
                t.micros
            ),
            (2023, 11, 14, 22, 13, 20, 123_000)
        );
        let t = chrono_free(0).unwrap();
        assert_eq!((t.date.year, t.date.month, t.date.day), (1970, 1, 1));
    }

    #[test]
    fn key_ranges_compare_leading_parts() {
        let r = KeyRange {
            start: Bound::Included(vec![Value::UInt(0), Value::UInt(0)]),
            end: Bound::Excluded(vec![Value::UInt(0), Value::UInt(1)]),
        };
        assert!(in_range([0, 0, 7], &r));
        assert!(!in_range([0, 1, 0], &r));
        assert!(in_range([5, 5, 5], &KeyRange::all()));
    }
}
