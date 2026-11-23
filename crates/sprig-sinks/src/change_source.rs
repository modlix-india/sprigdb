//! A `CHANGES OF` sink's source on engine tables: their changes read from
//! a gateway's change log ([`crate::peer::SqlOp::Changes`]) as the sink's
//! owner.
//!
//! ```text
//! next_batch  Changes {objects, token, from_ms, max_bytes, wait_ms}
//!             → change records (op, before, after, the envelope as value),
//!               each positioned by its resume token
//! settle      the delivered prefix's last token saved in the task's row
//!             ({"source": "changes", "token": ...}), under its epoch
//! restart     resumes after the saved token; without one, from the
//!             sink's start (START AT LATEST: its creation time)
//! ```
//!
//! Delivery is at least once: changes after the last saved token are read
//! again after a restart. While the tables have no new changes, the token
//! after what was read (past other tables' changes) is saved every few
//! seconds once nothing is in flight, so a restart doesn't read them again.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_common::Result;

use crate::model::{ChangeOp, Position, SinkId, SinkRecord, TaskEpoch, TaskNo};
use crate::peer::{RunAs, SinkPeer, SqlOp, SqlResult};
use crate::runtime::SourceReader;
use crate::store::SinksStore;

/// Change-log bytes one read covers at most.
const READ_BYTES: u64 = 1 << 20;

/// How often a caught-up task saves its position at most.
const SAVE_EVERY: Duration = Duration::from_secs(5);

/// Where a change source saves its position.
#[derive(Debug, Clone)]
pub struct Saver {
    /// The sink's store.
    pub store: SinksStore,
    /// The sink.
    pub sink: SinkId,
    /// The task.
    pub task_no: TaskNo,
    /// Its epoch.
    pub epoch: TaskEpoch,
}

/// The source (see the module docs).
pub struct ChangeSource {
    peer: Arc<dyn SinkPeer>,
    run_as: RunAs,
    objects: Vec<u64>,
    /// The position read up to.
    token: Option<String>,
    from_ms: i64,
    seed: u64,
    consumer: String,
    saver: Option<Saver>,
    /// The order of the records given.
    seq: u64,
    /// The last record given, and whether it is settled.
    given: u64,
    settled: u64,
    /// The position read up to when nothing was given for it, to save once
    /// everything given is settled.
    caught_up: Option<String>,
    saved: Option<String>,
    last_save: Instant,
}

impl std::fmt::Debug for ChangeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeSource")
            .field("objects", &self.objects)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// The token a task's source part holds, if any.
#[must_use]
pub fn saved_token(part: &Value) -> Option<String> {
    part.get("token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

impl ChangeSource {
    /// A source over `objects` read as `run_as` through `peer` (the gateway
    /// `seed` picks), after `token`, or from `from_ms` without one.
    /// `consumer` keys the gateway's open reader.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // the source's parts
    pub fn new(
        peer: Arc<dyn SinkPeer>,
        run_as: RunAs,
        objects: Vec<u64>,
        token: Option<String>,
        from_ms: i64,
        seed: u64,
        consumer: String,
        saver: Option<Saver>,
    ) -> Self {
        Self {
            peer,
            run_as,
            objects,
            saved: token.clone(),
            token,
            from_ms,
            seed,
            consumer,
            saver,
            seq: 0,
            given: 0,
            settled: 0,
            caught_up: None,
            last_save: Instant::now(),
        }
    }

    /// Its position (the token read up to).
    #[must_use]
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    async fn save(&mut self, token: &str) -> Result<()> {
        if self.saved.as_deref() == Some(token) {
            return Ok(());
        }
        if let Some(s) = &self.saver {
            s.store
                .save_task_part(
                    s.sink,
                    s.task_no,
                    s.epoch,
                    &json!({"source": "changes", "token": token}),
                )
                .await?;
        }
        self.saved = Some(token.to_owned());
        self.last_save = Instant::now();
        Ok(())
    }
}

/// The records of a `Changes` result.
fn records(r: &SqlResult, seq: &mut u64) -> Vec<SinkRecord> {
    let col = |n: &str| r.col(n);
    let (Some(t), Some(op), Some(k), Some(b), Some(a), Some(ts), Some(v)) = (
        col("token"),
        col("op"),
        col("key"),
        col("before"),
        col("after"),
        col("ts_ms"),
        col("value"),
    ) else {
        return Vec::new();
    };
    let json = |row: &[Option<String>], i: usize| {
        SqlResult::bytes(row, i).and_then(|b| serde_json::from_slice::<Value>(&b).ok())
    };
    let mut out = Vec::with_capacity(r.rows.len());
    for row in &r.rows {
        let Some(token) = SqlResult::text(row, t) else {
            continue;
        };
        *seq += 1;
        out.push(SinkRecord {
            key: SqlResult::bytes(row, k),
            value: SqlResult::bytes(row, v),
            headers: Vec::new(),
            op: SqlResult::text(row, op)
                .as_deref()
                .and_then(ChangeOp::parse),
            before: json(row, b).filter(|x| !x.is_null()),
            after: json(row, a).filter(|x| !x.is_null()),
            ts_ms: SqlResult::text(row, ts)
                .and_then(|x| x.parse().ok())
                .unwrap_or(0),
            idem_key: token.clone(),
            pos: Position::Change {
                event: token.clone(),
                token,
                shard: 0,
                seq: *seq,
            },
        });
    }
    out
}

#[async_trait]
impl SourceReader for ChangeSource {
    async fn next_batch(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let op = SqlOp::Changes {
            objects: self.objects.clone(),
            token: self.token.clone(),
            from_ms: self.from_ms,
            max_bytes: max_bytes.clamp(64 << 10, READ_BYTES),
            wait_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            consumer: self.consumer.clone(),
        };
        let reply = self
            .peer
            .sql(self.seed, &self.run_as.request(op))
            .await?
            .ok()?;
        let mut recs = reply
            .results
            .first()
            .map(|r| records(r, &mut self.seq))
            .unwrap_or_default();
        if recs.len() > max_records.max(1) {
            recs.truncate(max_records.max(1));
            if let Some(Position::Change { token, seq, .. }) = recs.last().map(|r| &r.pos) {
                self.token = Some(token.clone());
                self.seq = *seq;
            }
        } else if let Some(n) = reply.next {
            self.token = Some(n);
        }
        if let Some(r) = recs.last()
            && let Position::Change { seq, .. } = r.pos
        {
            self.given = seq;
            self.caught_up = None;
        } else if let Some(t) = self.token.clone() {
            // Nothing for the tables: once everything given is settled,
            // the position past what was read is saved now and then.
            if self.settled >= self.given {
                if self.last_save.elapsed() >= SAVE_EVERY {
                    self.save(&t).await?;
                }
            } else {
                self.caught_up = Some(t);
            }
        }
        Ok(recs)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let last = done
            .iter()
            .filter_map(|p| match p {
                Position::Change { token, seq, .. } => Some((*seq, token)),
                _ => None,
            })
            .max_by_key(|(s, _)| *s);
        let Some((seq, token)) = last else {
            return Ok(());
        };
        let token = token.clone();
        self.settled = self.settled.max(seq);
        match self.caught_up.clone() {
            Some(t) if self.settled >= self.given => {
                self.save(&t).await?;
                self.caught_up = None;
            }
            _ => self.save(&token).await?,
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Change { token, .. } = pos {
            self.token = Some(token.clone());
            self.caught_up = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::peer::{SqlReply, SqlRequest};

    /// A gateway answering from a list of events (token = its index).
    struct Fake {
        events: Vec<&'static str>,
        asked: Mutex<Vec<Option<String>>>,
    }

    #[async_trait]
    impl SinkPeer for Fake {
        async fn sql(&self, _seed: u64, req: &SqlRequest) -> Result<SqlReply> {
            let SqlOp::Changes { token, .. } = &req.op else {
                panic!("{req:?}");
            };
            self.asked.lock().unwrap().push(token.clone());
            let from: usize = token.as_deref().map_or(0, |t| t.parse().unwrap());
            let mut r = SqlResult {
                columns: [
                    "token", "object", "op", "key", "before", "after", "ts_ms", "value",
                ]
                .map(str::to_owned)
                .to_vec(),
                ..SqlResult::default()
            };
            for (i, op) in self.events.iter().enumerate().skip(from) {
                let cells = [
                    Some((i + 1).to_string()),
                    Some("10".into()),
                    Some((*op).to_owned()),
                    Some(format!("{{\"id\":{i}}}")),
                    Some("null".into()),
                    Some(format!("{{\"id\":{i}}}")),
                    Some("1000".into()),
                    Some("{}".into()),
                ];
                r.rows.push(
                    cells
                        .iter()
                        .map(|c| SqlResult::cell(c.as_deref().map(str::as_bytes)))
                        .collect(),
                );
            }
            Ok(SqlReply {
                results: vec![r],
                next: Some(self.events.len().to_string()),
                ..SqlReply::default()
            })
        }
    }

    fn source(p: Arc<Fake>, token: Option<String>) -> ChangeSource {
        ChangeSource::new(
            p,
            RunAs {
                tenant: 1,
                tenant_name: "t".into(),
                user_id: 2,
                database: "d".into(),
            },
            vec![10],
            token,
            0,
            1,
            "1/2/0".into(),
            None,
        )
    }

    #[tokio::test]
    async fn reads_in_order_and_resumes_after_a_record() {
        let p = Arc::new(Fake {
            events: vec!["c", "u", "d"],
            asked: Mutex::default(),
        });
        let mut s = source(Arc::clone(&p), None);
        let b = s.next_batch(2, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(b.len(), 2);
        assert_eq!(b[0].op, Some(ChangeOp::Create));
        assert_eq!(b[1].op, Some(ChangeOp::Update));
        assert_eq!(b[0].after, Some(json!({"id": 0})));
        assert_eq!(b[0].before, None);
        // Truncated to two: the next read starts after the second.
        assert_eq!(s.token(), Some("2"));
        let b2 = s.next_batch(10, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(b2.len(), 1);
        assert_eq!(b2[0].op, Some(ChangeOp::Delete));
        assert_eq!(s.token(), Some("3"));
        s.settle(&[b[1].pos.clone()]).await.unwrap();
        assert_eq!(s.saved.as_deref(), Some("2"));
        // An exactly-once target's own position wins.
        s.seek_after(&b[0].pos).await.unwrap();
        let b3 = s.next_batch(10, 1 << 20, Duration::ZERO).await.unwrap();
        assert_eq!(b3.len(), 2);
        assert_eq!(
            *p.asked.lock().unwrap(),
            vec![None, Some("2".to_owned()), Some("1".to_owned())]
        );
    }

    #[test]
    fn saved_tokens() {
        assert_eq!(saved_token(&json!({"token": "ab"})), Some("ab".into()));
        assert_eq!(saved_token(&json!({"token": ""})), None);
        assert_eq!(saved_token(&json!({"source": "queue"})), None);
    }
}
