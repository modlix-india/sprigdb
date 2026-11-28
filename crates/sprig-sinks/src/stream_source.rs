//! A `FROM STREAM` sink's source on an engine stream: the task's
//! partitions read through a gateway ([`crate::peer::SqlOp::StreamRead`])
//! as the sink's owner, who needs `SELECT` on the stream.
//!
//! ```text
//! next_batch  StreamRead {db, stream, from: per partition the next offset
//!             (or, before the first record, the start time), max, wait}
//!             → records in offset order per partition, positioned by
//!               (stream, partition, offset)
//! settle      per partition, the offset after its delivered prefix, saved
//!             in the task's row ({"source": "stream", "offsets": {p: n}})
//! restart     each partition resumes at its saved offset; without one,
//!             from the sink's start (START AT LATEST: its creation time;
//!             EARLIEST: the partition's start; a TIMESTAMP: that time)
//! ```
//!
//! Delivery is at least once: records after the saved offsets are read
//! again after a restart. A partition whose saved offset fell below the
//! log's start (retention) resumes at the first record the log holds.
//!
//! In a `FROM STREAM s FAN IN` sink's task each member instance is one
//! such source ([`StreamSource::fan_in`]): it reads through
//! [`crate::peer::SqlOp::FanInStreamRead`], learns the instance's
//! partitions from what it reads (a partition not read yet starts at the
//! sink's start), and the task's row keeps every member's offsets
//! (`{"source": "stream", "members": {member: {p: n}}}`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_common::Result;

use crate::change_source::Saver;
use crate::model::{Position, SinkRecord};
use crate::peer::{RunAs, SinkPeer, SqlOp, SqlResult, StreamFrom};
use crate::runtime::SourceReader;

/// The source (see the module docs).
pub struct StreamSource {
    peer: Arc<dyn SinkPeer>,
    run_as: RunAs,
    db: String,
    stream: String,
    stream_id: u64,
    /// The task's partitions, each with the next offset to read (`None`:
    /// from `from_ms`).
    next: BTreeMap<u32, Option<u64>>,
    from_ms: i64,
    seed: u64,
    saver: Option<Saver>,
    /// Per partition, the offset after its settled prefix.
    settled: BTreeMap<u32, u64>,
    saved: BTreeMap<u32, u64>,
    /// In a fan-in task: the definition and member read, and every
    /// member's settled offsets (one task row holds them all).
    fan_in: Option<(FanInAt, Members, Value)>,
}

/// A fan-in source's member instance: `(def_tenant, def_object, member)`.
pub type FanInAt = (u64, u64, u64);

/// A fan-in task's settled offsets, per member, per partition.
pub type Members = Arc<std::sync::Mutex<BTreeMap<u64, BTreeMap<u32, u64>>>>;

/// The offsets per member a fan-in task's source part holds.
#[must_use]
pub fn saved_members(part: &Value) -> BTreeMap<u64, BTreeMap<u32, u64>> {
    part.get("members")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.parse().ok()?, saved_offsets(&json!({"offsets": v})))))
                .collect()
        })
        .unwrap_or_default()
}

/// A fan-in task's source part with every member's `offsets`.
#[must_use]
pub fn members_part(members: &BTreeMap<u64, BTreeMap<u32, u64>>) -> Value {
    let m: serde_json::Map<String, Value> = members
        .iter()
        .map(|(t, o)| (t.to_string(), part_of(o)["offsets"].clone()))
        .collect();
    json!({"source": "stream", "members": m})
}

impl std::fmt::Debug for StreamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSource")
            .field("stream", &self.stream)
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

/// The offsets a task's source part holds, per partition.
#[must_use]
pub fn saved_offsets(part: &Value) -> BTreeMap<u32, u64> {
    part.get("offsets")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.parse().ok()?, v.as_u64()?)))
                .collect()
        })
        .unwrap_or_default()
}

/// A task's source part with `offsets`.
#[must_use]
pub fn part_of(offsets: &BTreeMap<u32, u64>) -> Value {
    let m: serde_json::Map<String, Value> = offsets
        .iter()
        .map(|(p, o)| (p.to_string(), json!(o)))
        .collect();
    json!({"source": "stream", "offsets": m})
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day count since 1970-01-01.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Milliseconds since the epoch as UTC text, `YYYY-MM-DD HH:MM:SS.mmm`
/// (a stream's `ts`).
#[must_use]
pub fn utc_text(ms: i64) -> String {
    let (days, in_day) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    let (y, m, d) = civil_from_days(days);
    let s = in_day / 1000;
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03}",
        s / 3600,
        s / 60 % 60,
        s % 60,
        in_day % 1000
    )
}

/// [`utc_text`]'s inverse (a fraction of any length; 0 when it doesn't
/// read).
#[must_use]
pub fn utc_ms(text: &str) -> i64 {
    let n = |s: Option<&str>| s.and_then(|x| x.parse::<i64>().ok());
    let (date, time) = text.split_once(' ').unwrap_or((text, "00:00:00"));
    let mut dp = date.split('-');
    let (Some(y), Some(m), Some(d)) = (n(dp.next()), n(dp.next()), n(dp.next())) else {
        return 0;
    };
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut tp = hms.split(':');
    let (h, mi, s) = (
        n(tp.next()).unwrap_or(0),
        n(tp.next()).unwrap_or(0),
        n(tp.next()).unwrap_or(0),
    );
    let mut f = frac.chars().take(3).collect::<String>();
    while f.len() < 3 {
        f.push('0');
    }
    let ms = f.parse::<i64>().unwrap_or(0);
    days_from_civil(y, m, d) * 86_400_000 + ((h * 60 + mi) * 60 + s) * 1000 + ms
}

/// A record's headers from the stream's `headers` JSON object.
fn headers_of(raw: Option<Vec<u8>>) -> Vec<(String, Vec<u8>)> {
    let Some(v) = raw.and_then(|b| serde_json::from_slice::<Value>(&b).ok()) else {
        return Vec::new();
    };
    let Some(m) = v.as_object() else {
        return Vec::new();
    };
    m.iter()
        .map(|(k, v)| {
            let b = match v {
                Value::String(s) => s.clone().into_bytes(),
                Value::Null => Vec::new(),
                other => other.to_string().into_bytes(),
            };
            (k.clone(), b)
        })
        .collect()
}

impl StreamSource {
    /// A source over partitions `parts` of stream `db`.`stream` (id
    /// `stream_id`) read as `run_as` through `peer` (the gateway `seed`
    /// picks), from the saved `offsets`, else from `from_ms`.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // the source's parts
    pub fn new(
        peer: Arc<dyn SinkPeer>,
        run_as: RunAs,
        db: String,
        stream: String,
        stream_id: u64,
        parts: &[u32],
        offsets: &BTreeMap<u32, u64>,
        from_ms: i64,
        seed: u64,
        saver: Option<Saver>,
    ) -> Self {
        let next = parts
            .iter()
            .map(|p| (*p, offsets.get(p).copied()))
            .collect();
        let kept: BTreeMap<u32, u64> = parts
            .iter()
            .filter_map(|p| Some((*p, *offsets.get(p)?)))
            .collect();
        Self {
            peer,
            run_as,
            db,
            stream,
            stream_id,
            next,
            from_ms,
            seed,
            saver,
            settled: kept.clone(),
            saved: kept,
            fan_in: None,
        }
    }

    /// This source as a fan-in task's reader of member instance `at`,
    /// saving into the task's shared `members` offsets; `part` is the rest
    /// of the task's row (its `of`, kept with the offsets).
    #[must_use]
    pub fn fan_in(mut self, at: FanInAt, members: Members, part: Value) -> Self {
        self.fan_in = Some((at, members, part));
        self
    }

    /// The next offset to read per partition (`None`: not started).
    #[must_use]
    pub fn next(&self) -> &BTreeMap<u32, Option<u64>> {
        &self.next
    }

    fn records(&mut self, r: &SqlResult) -> Vec<SinkRecord> {
        let (Some(p), Some(o), Some(ts), Some(k), Some(v), Some(h)) = (
            r.col("partition_no"),
            r.col("offset_no"),
            r.col("ts"),
            r.col("key"),
            r.col("value"),
            r.col("headers"),
        ) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(r.rows.len());
        for row in &r.rows {
            let num = |i: usize| SqlResult::text(row, i).and_then(|x| x.parse::<u64>().ok());
            let (Some(part), Some(offset)) = (num(p), num(o)) else {
                continue;
            };
            let Ok(part) = u32::try_from(part) else {
                continue;
            };
            let next = self.next.entry(part).or_default();
            *next = Some(next.map_or(offset + 1, |n| n.max(offset + 1)));
            out.push(SinkRecord {
                key: SqlResult::bytes(row, k),
                value: SqlResult::bytes(row, v),
                headers: headers_of(SqlResult::bytes(row, h)),
                op: None,
                before: None,
                after: None,
                ts_ms: SqlResult::text(row, ts).map_or(0, |t| utc_ms(&t)),
                idem_key: format!("s/{}/{part}/{offset}", self.stream_id),
                pos: Position::Stream {
                    stream: self.stream_id,
                    partition: part,
                    offset,
                    epoch: 0,
                },
            });
        }
        out
    }

    async fn save(&mut self) -> Result<()> {
        if self.saved == self.settled {
            return Ok(());
        }
        if let Some(s) = &self.saver {
            let part = match &self.fan_in {
                Some(((_, _, member), all, base)) => {
                    let mut g = all
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    g.insert(*member, self.settled.clone());
                    let mut part = base.clone();
                    part["members"] = members_part(&g)["members"].clone();
                    part
                }
                None => part_of(&self.settled),
            };
            s.store
                .save_task_part(s.sink, s.task_no, s.epoch, &part)
                .await?;
        }
        self.saved = self.settled.clone();
        Ok(())
    }
}

#[async_trait]
impl SourceReader for StreamSource {
    async fn next_batch(
        &mut self,
        max_records: usize,
        _max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let parts = self.next.len().max(1);
        let per = u32::try_from((max_records.max(1)).div_ceil(parts)).unwrap_or(u32::MAX);
        let from = self
            .next
            .iter()
            .map(|(p, n)| StreamFrom {
                partition: *p,
                offset: *n,
                from_ms: self.from_ms,
            })
            .collect();
        let wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        let op = match &self.fan_in {
            Some(((def_tenant, def_object, member), _, _)) => SqlOp::FanInStreamRead {
                def_tenant: *def_tenant,
                def_object: *def_object,
                member: *member,
                db: self.db.clone(),
                stream: self.stream.clone(),
                from,
                from_ms: self.from_ms,
                max_records: per.max(1),
                wait_ms,
            },
            None => SqlOp::StreamRead {
                db: self.db.clone(),
                stream: self.stream.clone(),
                from,
                max_records: per.max(1),
                wait_ms,
            },
        };
        let reply = self
            .peer
            .sql(self.seed, &self.run_as.request(op))
            .await?
            .ok()?;
        let Some(r) = reply.results.first() else {
            return Ok(Vec::new());
        };
        Ok(self.records(r))
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut moved = false;
        for p in done {
            if let Position::Stream {
                partition, offset, ..
            } = p
            {
                let e = self.settled.entry(*partition).or_default();
                if *e < offset + 1 {
                    *e = offset + 1;
                    moved = true;
                }
            }
        }
        if moved {
            self.save().await?;
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Stream {
            partition, offset, ..
        } = pos
        {
            self.next.insert(*partition, Some(offset + 1));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::peer::{SqlReply, SqlRequest};

    #[test]
    fn times_read_as_utc_text() {
        assert_eq!(utc_text(0), "1970-01-01 00:00:00.000");
        assert_eq!(utc_text(1_764_150_000_123), "2025-11-26 09:40:00.123");
        assert_eq!(utc_ms("2025-11-26 09:40:00.123"), 1_764_150_000_123);
        assert_eq!(utc_ms("2025-11-26 09:40:00.1"), 1_764_150_000_100);
        assert_eq!(utc_ms("2000-02-29 00:00:00"), 951_782_400_000);
        for ms in [0, 86_399_999, 951_782_400_000, 4_102_444_800_000] {
            assert_eq!(utc_ms(&utc_text(ms)), ms);
        }
    }

    /// A gateway answering from per-partition record lists.
    struct Fake {
        parts: Vec<Vec<&'static str>>,
        asked: Mutex<Vec<Vec<StreamFrom>>>,
    }

    #[async_trait]
    impl SinkPeer for Fake {
        async fn sql(&self, _seed: u64, req: &SqlRequest) -> Result<SqlReply> {
            let SqlOp::StreamRead {
                from, max_records, ..
            } = &req.op
            else {
                panic!("{req:?}");
            };
            self.asked.lock().unwrap().push(from.clone());
            let mut r = SqlResult {
                columns: ["partition_no", "offset_no", "ts", "key", "value", "headers"]
                    .map(str::to_owned)
                    .to_vec(),
                ..SqlResult::default()
            };
            for f in from {
                let recs = &self.parts[f.partition as usize];
                let start = usize::try_from(f.offset.unwrap_or(0)).unwrap();
                for (o, v) in recs
                    .iter()
                    .enumerate()
                    .skip(start)
                    .take(*max_records as usize)
                {
                    r.rows.push(vec![
                        SqlResult::cell(Some(f.partition.to_string().as_bytes())),
                        SqlResult::cell(Some(o.to_string().as_bytes())),
                        SqlResult::cell(Some(b"2025-11-26 09:40:00.123")),
                        SqlResult::cell(Some(b"k")),
                        SqlResult::cell(Some(v.as_bytes())),
                        SqlResult::cell(Some(br#"{"h":"x"}"#)),
                    ]);
                }
            }
            Ok(SqlReply {
                results: vec![r],
                ..SqlReply::default()
            })
        }
    }

    #[tokio::test]
    async fn partitions_are_read_on_from_their_offsets() {
        let fake = Arc::new(Fake {
            parts: vec![vec!["a0", "a1", "a2"], vec!["b0"]],
            asked: Mutex::new(Vec::new()),
        });
        let peer: Arc<dyn SinkPeer> = fake.clone();
        let saved = BTreeMap::from([(0, 1)]);
        let mut s = StreamSource::new(
            peer,
            RunAs {
                tenant: 1,
                tenant_name: "t".into(),
                user_id: 1,
                database: "app".into(),
            },
            "app".into(),
            "ev".into(),
            7,
            &[0, 1],
            &saved,
            0,
            1,
            None,
        );
        let got = s.next_batch(10, 1 << 20, Duration::ZERO).await.unwrap();
        let vals: Vec<&[u8]> = got.iter().map(|r| r.value.as_deref().unwrap()).collect();
        assert_eq!(vals, vec![&b"a1"[..], b"a2", b"b0"]);
        assert_eq!(got[0].idem_key, "s/7/0/1");
        assert_eq!(got[0].ts_ms, 1_764_150_000_123);
        assert_eq!(got[0].headers, vec![("h".to_owned(), b"x".to_vec())]);
        assert_eq!(s.next(), &BTreeMap::from([(0, Some(3)), (1, Some(1))]));
        // The first read asked partition 0 from its saved offset and
        // partition 1 from the start time.
        let asked = fake.asked.lock().unwrap().clone();
        assert_eq!(asked[0][0].offset, Some(1));
        assert_eq!(asked[0][1].offset, None);
        s.settle(&[got[2].pos.clone(), got[0].pos.clone()])
            .await
            .unwrap();
        assert_eq!(s.settled, BTreeMap::from([(0, 2), (1, 1)]));
        assert_eq!(
            part_of(&s.settled),
            json!({"source": "stream", "offsets": {"0": 2, "1": 1}})
        );
        assert_eq!(saved_offsets(&part_of(&s.settled)), s.settled);
        // Nothing new: an empty batch.
        assert!(
            s.next_batch(10, 1 << 20, Duration::ZERO)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
