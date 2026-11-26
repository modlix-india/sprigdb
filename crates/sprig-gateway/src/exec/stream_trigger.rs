//! Triggers on streams of engine databases: `CREATE [OR REPLACE] TRIGGER g
//! ON STREAM [db.]s LANGUAGE javascript | lua | kirun AS ...`, run from the
//! node's stream log on the after-commit triggers' runtime
//! ([`super::change_trigger`]).
//!
//! **Declaring.** The stream must exist; the deploying user needs `SELECT`
//! on it and `CREATE ROUTINE` on the trigger's database. The trigger is a
//! module of kind `trigger` (always the definer's rights, engine tier), a
//! binding on the stream's id, a source row and the index entry that makes
//! the controller start its job (`logic_stream_trigger`). Its positions are
//! engine records of the consumer group `__sprig_logic_<binding>`, one per
//! partition, starting at each partition's high watermark: the trigger
//! fires for records published after it was created. `OR REPLACE` on the
//! same stream keeps the positions.
//!
//! **Running.** The worker runs `logic.trigger_lanes` loops (as for an
//! after-commit trigger); lane `j` of `n` serves the partitions `p` with
//! `p % n == j`, so each partition's records run one at a time in offset
//! order and different partitions' run in parallel. A run, in a session as
//! the definer:
//!
//! ```text
//! the lane's partitions with a record past their position (none: wait up
//!   to 2 s for one, then Empty); the next one in turn
//! r := the record at the partition's position (kept between runs, else
//!   read from the log)
//! USE db; START TRANSACTION
//!   move the partition's position from pos to r.offset + 1 (compare-and-
//!   set: moved by another run → ROLLBACK, Moved)
//!   the version with ctx.record = {stream, partition, offset, key, value,
//!   headers, timestamp}
//! COMMIT → Ok
//! failure → ROLLBACK; after logic.trigger_max_attempts failures that
//!   aren't transient: a dead letter (event key p<partition>:<offset>) and
//!   the position moves past r
//! ```
//!
//! The position commits with the code's writes, so they happen exactly
//! once; outside effects are at least once, with the record's
//! `p<partition>:<offset>` as their idempotency key. A position below the
//! partition's log start (retention removed the records) moves to the log
//! start with a warning. A dropped stream stops the trigger: its binding
//! becomes `orphaned` (the job ends within seconds) and a run-log line says
//! why; `CREATE OR REPLACE TRIGGER` on a stream made since starts it again.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::LogicStore;
use sprig_logic::declare::{Declaration, MessageSource, TierPref};
use sprig_logic::messages::{self, MsgIndex, RecordIn, RunOutcome, RunReply, RunRequest};
use sprig_proto_kafka::broker::data::StoredRecord;
use sprig_streams::seg::kafka::{EngineStreams, decode_stored};

use super::PassSession;
use crate::pipeline::ExecCtx;

/// How long a run waits for a record when the lane's partitions have none.
const WAIT: Duration = Duration::from_secs(2);

/// Bytes of the log one read takes at most.
const PAGE_BYTES: usize = 256 * 1024;

/// How often the definer's `SELECT` on the stream is checked again.
const CHECK_EVERY: Duration = Duration::from_secs(30);

/// The most lanes a trigger runs (`logic.trigger_lanes`' maximum).
const MAX_LANES: u32 = 1024;

/// A partition of a trigger: tenant, binding id and partition.
type Key = (u64, u64, u32);

/// A binding: tenant and binding id.
type Binding = (u64, u64);

/// A lane of a binding: tenant, binding id and lane.
type LaneKey = (u64, u64, u32);

/// Records read past a partition's position and not run yet: the next run
/// starting at `from` takes them instead of reading the log again.
struct Pending {
    from: i64,
    records: VecDeque<StoredRecord>,
    /// Where the read batches end (compaction leaves gaps).
    end: i64,
}

static PENDING: LazyLock<Mutex<HashMap<Key, Pending>>> = LazyLock::new(Mutex::default);
/// Failed attempts of a partition's record at an offset (kept by the
/// gateway the binding's runs go to; another gateway counts anew).
static FAILS: LazyLock<Mutex<HashMap<Key, (i64, u32)>>> = LazyLock::new(Mutex::default);
/// Where each lane's next turn over its partitions starts: tenant,
/// binding, lane.
static TURN: LazyLock<Mutex<HashMap<LaneKey, usize>>> = LazyLock::new(Mutex::default);
/// When the definer's `SELECT` on a binding's stream was last checked.
static CHECKED: LazyLock<Mutex<HashMap<Binding, Instant>>> = LazyLock::new(Mutex::default);
/// Bindings this process found orphaned (every lane finds it at once; one
/// writes the record).
static ORPHANED: LazyLock<Mutex<std::collections::HashSet<Binding>>> =
    LazyLock::new(Mutex::default);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The partitions lane `lane` of `lanes` serves among `partitions`.
fn lane_partitions(partitions: u32, lanes: u32, lane: u32) -> Vec<u32> {
    let lanes = lanes.max(1);
    (0..partitions).filter(|p| p % lanes == lane).collect()
}

/// A record's event key (also its dead letter's).
fn event_key(partition: u32, offset: i64) -> String {
    format!("p{partition}:{offset}")
}

/// `ctx.record` of record `r` of partition `partition` of stream `stream`
/// (`db.s`).
fn record_event(stream: &str, partition: u32, r: &StoredRecord) -> Value {
    messages::record_value(
        stream,
        &RecordIn {
            partition,
            offset: u64::try_from(r.offset).unwrap_or(0),
            ts_ms: r.timestamp_ms,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|h| (h.key.clone(), h.value.clone()))
                .collect(),
        },
    )
}

/// Where stored batches `bytes` read from `from` end.
fn batches_end(bytes: &[u8], from: i64) -> i64 {
    let mut end = from;
    let mut pos = 0;
    while let Some(h) = sprig_streams::seg::batch::Head::parse(&bytes[pos..]) {
        end = end.max(h.next_offset());
        pos += h.total;
        if pos >= bytes.len() {
            break;
        }
    }
    end
}

/// A transient failure: the worker tries again, never counted.
fn transient(e: &SprigError) -> RunReply {
    let mut r = RunReply::failed(e);
    r.transient = true;
    r
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn unavailable(m: &str) -> SprigError {
    SprigError::new(SprigCode::LogicEngineUnavailable, m.to_owned())
}

/// Stream `name` of database `db` among `eng`'s streams of `tenant`: by its
/// name as written, else without case.
fn find_stream(
    eng: &EngineStreams,
    tenant: u64,
    db: &str,
    name: &str,
) -> Result<Option<sprig_streams::seg::records::TopicRecord>> {
    let full = format!("{db}.{name}");
    if let Some(t) = eng.topic(tenant, &full)? {
        return Ok(Some(t));
    }
    Ok(eng
        .topics(tenant)?
        .into_iter()
        .find(|t| t.name.eq_ignore_ascii_case(&full)))
}

impl PassSession {
    /// `CREATE TRIGGER ... ON STREAM` (see the module docs). Returns the
    /// client's notes.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_stream_trigger(
        &mut self,
        decl: Declaration,
    ) -> Result<Vec<String>> {
        let src = decl
            .source
            .clone()
            .ok_or_else(|| SprigError::internal("no source"))?;
        if src.fan_in {
            return Err(SprigError::standard(
                1235,
                *b"42000",
                "This version of SprigDB doesn't yet support 'FAN IN triggers': they read a FOR \
                 EACH TENANT stream, and inherited streams aren't built on the SprigDB engine yet",
            ));
        }
        let Some(streams) = crate::kafka::engine::streams() else {
            return Err(SprigError::new(
                SprigCode::NotOnEngine,
                "a trigger on a stream reads the engine's stream log, which this node doesn't \
                 serve",
            ));
        };
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = decl
            .db
            .clone()
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        if decl.tier == TierPref::Mariadb {
            return Err(SprigError::new(
                SprigCode::LogicTierUnavailable,
                format!(
                    "SPRIG_TIER = 'mariadb': a trigger on a stream runs on the engine tier \
                     (database {db})"
                ),
            ));
        }
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let unknown =
            |d: &str| SprigError::standard(1049, *b"42000", format!("Unknown database '{d}'"));
        let row = cat
            .database(&database_key(&db, lctn))
            .cloned()
            .ok_or_else(|| unknown(&db))?;
        let source_db = src.db.clone().unwrap_or_else(|| db.clone());
        let srow = cat
            .database(&database_key(&source_db, lctn))
            .cloned()
            .ok_or_else(|| unknown(&source_db))?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let tenant_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let tenant = ts.tenant;
        let grants = crate::tenant::authz::privileges(ts, &cat);
        // Code: CREATE ROUTINE on the trigger's database.
        let db_key = database_key(&db, lctn);
        if !grants.unrestricted()
            && !grants.allows(
                Action::CreateRoutine,
                AuthzTarget::Database { db_key: &db_key },
            )
        {
            return Err(SprigError::standard(
                1044,
                *b"42000",
                format!(
                    "Access denied for user '{}' to database '{db}'",
                    ts.principal.user
                ),
            ));
        }
        let topic = find_stream(&streams, tenant.0, &srow.name, &src.name)?.ok_or_else(|| {
            SprigError::new(
                SprigCode::StreamNotFound,
                format!("no stream {}.{}", srow.name, src.name),
            )
        })?;
        let short = topic
            .name
            .split_once('.')
            .map_or(src.name.as_str(), |(_, s)| s)
            .to_owned();
        // The definer reads the stream.
        let src_key = database_key(&srow.name, lctn);
        let name_key = object_key(ObjectKind::Table, &short, lctn);
        if !grants.unrestricted()
            && !grants.allows(
                Action::Select,
                AuthzTarget::Named {
                    db_key: &src_key,
                    name_key: &name_key,
                },
            )
            && !grants.allows(Action::Select, AuthzTarget::Database { db_key: &src_key })
        {
            return Err(SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "SELECT command denied to user '{}'@'{}' for table '{short}'",
                    ts.principal.user, ts.principal.client_ip
                ),
            ));
        }
        let mut d = decl.clone();
        d.tier = TierPref::Engine;
        d.source = Some(MessageSource {
            name: short.clone(),
            db: Some(srow.name.clone()),
            ..src
        });
        let svc = Arc::clone(ts.services());
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = svc.records.begin(tenant).await?;
        let deployed = messages::deploy(
            &d,
            messages::MsgTarget {
                catalog: &mut *conn,
                catalog_schema: ep.store.schema(),
                catalog_version: cat.version,
                db_id: row.db_id.0,
                user_id: user.0,
                tenant_name: &tenant_name,
                source_db: (srow.db_id.0, &srow.name),
                object_id: topic.stream_id,
                fanin_declarer: 0,
                compile: None,
            },
        )
        .await?;
        conn.commit().await?;
        // Each partition's position at its high watermark (a replaced
        // trigger on the same stream keeps its positions).
        let group = messages::sprig_streams_group(deployed.binding_id);
        let partitions = u32::try_from(topic.partitions.max(0)).unwrap_or(0);
        let have = streams
            .records()
            .offsets(tenant.0, &group, None)?
            .into_iter()
            .any(|o| o.stream_id == topic.stream_id);
        if deployed.created || !have {
            let at: Vec<sprig_proto_kafka::group::CommittedOffset> = (0..partitions)
                .map(|p| sprig_proto_kafka::group::CommittedOffset {
                    stream_id: topic.stream_id,
                    partition: i32::try_from(p).unwrap_or(i32::MAX),
                    offset: streams
                        .log()
                        .bounds(sprig_streams::seg::Key {
                            tenant: tenant.0,
                            stream: topic.stream_id,
                            partition: p,
                        })
                        .high_watermark,
                    leader_epoch: None,
                    metadata: None,
                })
                .collect();
            streams
                .records()
                .commit_offsets(tenant.0, &group, &at, sprig_streams::seg::now_ms())
                .await?;
        }
        forget_binding(tenant.0, deployed.binding_id);
        messages::put_index(
            svc.locator.dc_store().as_ref(),
            &MsgIndex {
                tenant,
                binding: deployed.binding_id,
                kind: sprig_logic::declare::SourceKind::Stream,
            },
        )
        .await?;
        let mut notes = deployed.notes();
        notes.push(format!(
            "trigger on stream {}.{short} ({partitions} partitions): runs for records published \
             from now on, each partition's in offset order (logic.trigger_lanes lanes)",
            srow.name
        ));
        tracing::info!(%tenant, binding = deployed.binding_id, stream = %topic.name, name = %decl.name, "trigger on an engine stream deployed");
        Ok(notes)
    }

    /// One run of a trigger on an engine stream (see the module docs).
    pub(super) async fn run_stream_trigger(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
    ) -> RunReply {
        match self.run_stream_inner(cx, req).await {
            Ok(r) => r,
            Err(e) => transient(&e),
        }
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    async fn run_stream_inner(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
    ) -> Result<RunReply> {
        if req.source.fanin_definition().is_some() {
            return Ok(RunReply::failed(&SprigError::internal(
                "FAN IN triggers aren't served: inherited streams aren't built on the engine",
            )));
        }
        let streams = crate::kafka::engine::streams()
            .ok_or_else(|| unavailable("the engine's stream log isn't served on this gateway"))?;
        let tenant = req.tenant;
        let binding = req.source.binding_id;
        let stream = req.source.object_id;
        let Some(topic) = streams.topic_by_id(tenant, stream)? else {
            self.stream_gone(req).await?;
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        let db = req.source.db_name.clone();
        let stream_name = format!("{db}.{}", req.source.name);
        // The definer still reads the stream.
        let due = lock(&CHECKED)
            .get(&(tenant, binding))
            .is_none_or(|at| at.elapsed() >= CHECK_EVERY);
        if due {
            if let Err(e) = self.stream_readable(&db, &req.source.name) {
                return Ok(transient(&e));
            }
            lock(&CHECKED).insert((tenant, binding), Instant::now());
        }
        let partitions = u32::try_from(topic.partitions.max(0)).unwrap_or(0);
        let n = req.lanes.clamp(1, MAX_LANES);
        let lane = req.lane.min(n - 1);
        let mine = lane_partitions(partitions, n, lane);
        if mine.is_empty() {
            // More lanes than partitions: this one has none.
            tokio::time::sleep(WAIT).await;
            return Ok(RunReply::of(RunOutcome::Empty));
        }
        let group = messages::sprig_streams_group(binding);
        let log = streams.log();
        let key = |p: u32| sprig_streams::seg::Key {
            tenant,
            stream,
            partition: p,
        };
        let mut waited = false;
        let (p, cur) = loop {
            let at: HashMap<i32, i64> = streams
                .records()
                .offsets(tenant, &group, None)?
                .into_iter()
                .filter(|o| o.stream_id == stream)
                .map(|o| (o.partition, o.offset))
                .collect();
            let pos = |p: u32| {
                at.get(&i32::try_from(p).unwrap_or(i32::MAX))
                    .copied()
                    .unwrap_or(0)
            };
            let start = {
                let mut t = lock(&TURN);
                let s = t.entry((tenant, binding, lane)).or_insert(0);
                *s = s.wrapping_add(1);
                *s
            };
            let mut found = None;
            for i in 0..mine.len() {
                let p = mine[(start + i) % mine.len()];
                let b = log.bounds(key(p));
                let cur = pos(p);
                if cur < b.log_start {
                    // Retention removed records the trigger hadn't run.
                    lock(&PENDING).remove(&(tenant, binding, p));
                    let moved = self.stream_skip(cx, &db, req, p, cur, b.log_start).await?;
                    let mut rep = RunReply::of(RunOutcome::Moved);
                    if moved {
                        tracing::warn!(
                            tenant,
                            binding,
                            partition = p,
                            from = cur,
                            to = b.log_start,
                            "stream trigger: records expired from the stream before they ran"
                        );
                        rep.message = format!(
                            "{}: records {cur} to {} of partition {p} left stream {stream_name} \
                             before the trigger ran them; it goes on from the oldest record kept",
                            SprigCode::FeedHistoryLost.name(),
                            b.log_start
                        );
                    }
                    return Ok(rep);
                }
                if cur < b.high_watermark {
                    found = Some((p, cur));
                    break;
                }
            }
            if let Some(f) = found {
                break f;
            }
            if waited {
                return Ok(RunReply::of(RunOutcome::Empty));
            }
            waited = true;
            let beyond: Vec<(sprig_streams::seg::Key, i64)> =
                mine.iter().map(|&p| (key(p), pos(p))).collect();
            log.wait(&beyond, tokio::time::Instant::now() + WAIT).await;
        };
        let k: Key = (tenant, binding, p);
        // The records at `cur`: kept from the partition's last run, or read
        // now.
        let kept = lock(&PENDING).remove(&k).filter(|pend| pend.from == cur);
        let mut pend = if let Some(pend) = kept {
            pend
        } else {
            let bytes = match log.read(key(p), cur, PAGE_BYTES) {
                Ok((_, b)) => b,
                // Trimmed meanwhile: the next run moves past it.
                Err(Ok(_)) => return Ok(RunReply::of(RunOutcome::Moved)),
                Err(Err(e)) => return Ok(transient(&e)),
            };
            let records = decode_stored(&bytes, cur)?;
            Pending {
                from: cur,
                end: batches_end(&bytes, cur),
                records: records.into(),
            }
        };
        let r = loop {
            match pend.records.pop_front() {
                Some(r) if r.offset < cur => {}
                other => break other,
            }
        };
        let Some(r) = r else {
            // Only control batches or a compacted stretch: past it.
            if pend.end > cur {
                self.stream_skip(cx, &db, req, p, cur, pend.end).await?;
                return Ok(RunReply::of(RunOutcome::Moved));
            }
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        let at = r.offset;
        let next = at + 1;
        let event = record_event(&stream_name, p, &r);
        let ekey = event_key(p, at);
        let attempts = match lock(&FAILS).get(&k) {
            Some((o, n)) if *o == at => *n,
            _ => 0,
        };
        let with_id = |mut rep: RunReply| {
            rep.message_id = Some(ekey.clone());
            rep.attempts = attempts;
            rep
        };
        // Kept for the partition's next run, which starts after `r` (or at
        // it again).
        let keep = |pend: Pending, r: Option<StoredRecord>, from: i64| {
            let mut pend = pend;
            if let Some(r) = r {
                pend.records.push_front(r);
            }
            pend.from = from;
            lock(&PENDING).insert(k, pend);
        };
        let part = i32::try_from(p).unwrap_or(i32::MAX);
        let failure = 'run: {
            if let Err(e) = self.trigger_stmt(cx, &format!("USE {}", bq(&db))).await {
                break 'run e;
            }
            if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
                break 'run e;
            }
            match self.stream_move(req, part, cur, next).await {
                Ok(true) => {}
                Ok(false) => {
                    self.rollback_quietly(cx).await;
                    return Ok(with_id(RunReply::of(RunOutcome::Moved)));
                }
                Err(err) => {
                    self.rollback_quietly(cx).await;
                    break 'run err;
                }
            }
            // In a rollout, the record's key picks the version.
            let (picked, counted) = self.pick_version(req, &event, None).await;
            let started = Instant::now();
            let (ran, logs) = self.run_version(cx, &picked, "record", event.clone()).await;
            let outcome = RunReply::of(if ran.is_ok() {
                RunOutcome::Ok
            } else {
                RunOutcome::Failed
            });
            self.count_run(cx, counted, &picked, started, &outcome)
                .await;
            match ran {
                Ok(()) => match self.trigger_stmt(cx, "COMMIT").await {
                    Ok(()) => {
                        lock(&FAILS).remove(&k);
                        keep(pend, None, next);
                        return Ok(with_id(RunReply {
                            logs,
                            ..RunReply::of(RunOutcome::Ok)
                        }));
                    }
                    Err(err) => {
                        self.rollback_quietly(cx).await;
                        break 'run err;
                    }
                },
                Err(err) => {
                    self.rollback_quietly(cx).await;
                    let mut rep = with_id(RunReply {
                        logs,
                        ..RunReply::failed(&err)
                    });
                    if rep.transient {
                        keep(pend, Some(r), cur);
                        return Ok(rep);
                    }
                    if attempts.saturating_add(1) < req.max_attempts.max(1) {
                        lock(&FAILS).insert(k, (at, attempts + 1));
                        keep(pend, Some(r), cur);
                        return Ok(rep);
                    }
                    // The last attempt: a dead letter, then past it.
                    if let Err(werr) = self.stream_dead_letter(req, &ekey, at, &event, &rep).await {
                        tracing::warn!(error = %werr, binding, "trigger dead letter not written");
                        keep(pend, Some(r), cur);
                        return Ok(rep);
                    }
                    lock(&FAILS).remove(&k);
                    match self.stream_skip(cx, &db, req, p, cur, next).await {
                        Ok(_) => keep(pend, None, next),
                        Err(werr) => {
                            tracing::warn!(error = %werr, binding, "trigger position not moved past a dead letter");
                        }
                    }
                    rep.outcome = RunOutcome::Dead;
                    return Ok(rep);
                }
            }
        };
        keep(pend, Some(r), cur);
        Ok(with_id(transient(&failure)))
    }

    /// Whether the definer may still read stream `db`.`name` (1142 when
    /// not).
    fn stream_readable(&self, db: &str, name: &str) -> Result<()> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let Some(cat) = ts.catalog() else {
            return Ok(());
        };
        let grants = crate::tenant::authz::privileges(ts, cat);
        if grants.unrestricted() {
            return Ok(());
        }
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(db, lctn);
        let name_key = object_key(ObjectKind::Table, name, lctn);
        if grants.allows(
            Action::Select,
            AuthzTarget::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || grants.allows(Action::Select, AuthzTarget::Database { db_key: &db_key })
        {
            return Ok(());
        }
        Err(SprigError::standard(
            1142,
            *b"42000",
            format!(
                "SELECT command denied to user '{}'@'{}' for table '{name}'",
                ts.principal.user, ts.principal.client_ip
            ),
        ))
    }

    /// Partition `p`'s position compare-and-set in the session's engine
    /// transaction; it locks the row.
    async fn stream_move(&mut self, req: &RunRequest, p: i32, from: i64, to: i64) -> Result<bool> {
        let Some(tx) = self.eng.txn.open_tx() else {
            return Err(SprigError::internal(
                "a stream trigger's run has no engine transaction",
            ));
        };
        sprig_streams::seg::records::move_offset(
            tx.engine_txn(),
            req.tenant,
            &messages::sprig_streams_group(req.source.binding_id),
            req.source.object_id,
            p,
            from,
            to,
            sprig_streams::seg::now_ms(),
        )
        .await
    }

    /// Moves partition `p`'s position from `from` to `to` in a transaction
    /// of its own (`false`: another run moved it).
    async fn stream_skip(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        req: &RunRequest,
        p: u32,
        from: i64,
        to: i64,
    ) -> Result<bool> {
        let part = i32::try_from(p).unwrap_or(i32::MAX);
        self.trigger_stmt(cx, &format!("USE {}", bq(db))).await?;
        self.trigger_stmt(cx, "START TRANSACTION").await?;
        match self.stream_move(req, part, from, to).await {
            Ok(true) => {
                self.trigger_stmt(cx, "COMMIT").await?;
                Ok(true)
            }
            Ok(false) => {
                self.rollback_quietly(cx).await;
                Ok(false)
            }
            Err(e) => {
                self.rollback_quietly(cx).await;
                Err(e)
            }
        }
    }

    /// The trigger's stream is gone: its binding becomes `orphaned`, so the
    /// worker's job ends, with a run-log line saying why.
    async fn stream_gone(&mut self, req: &RunRequest) -> Result<()> {
        forget_binding(req.tenant, req.source.binding_id);
        let key = (req.tenant, req.source.binding_id);
        if !lock(&ORPHANED).insert(key) {
            return Ok(());
        }
        let r = self.stream_orphan(req).await;
        if r.is_err() {
            lock(&ORPHANED).remove(&key);
        }
        r
    }

    async fn stream_orphan(&mut self, req: &RunRequest) -> Result<()> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let mut c = ts.services().records.begin(ts.tenant).await?;
        let store = LogicStore::default();
        if let Some(mut b) = store.binding_by_id(&mut *c, req.source.binding_id).await?
            && b.state == "active"
            && b.object_id == Some(req.source.object_id)
        {
            "orphaned".clone_into(&mut b.state);
            store.put_binding(&mut *c, &b).await?;
            let _ = store
                .log(
                    &mut *c,
                    req.source.module_id,
                    Some(req.version),
                    "warn",
                    "trigger",
                    &format!(
                        "stream {}.{} is gone; the trigger stops (CREATE OR REPLACE TRIGGER to \
                         read a stream made since)",
                        req.source.db_name, req.source.name
                    ),
                )
                .await;
            tracing::info!(
                tenant = req.tenant,
                binding = req.source.binding_id,
                stream = %req.source.name,
                "stream trigger: its stream is gone; binding orphaned"
            );
        }
        c.commit().await
    }

    /// Writes a dead letter of a record and its run log line.
    async fn stream_dead_letter(
        &mut self,
        req: &RunRequest,
        event_key: &str,
        offset: i64,
        event: &Value,
        reply: &RunReply,
    ) -> Result<()> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let mut conn = ts.services().records.begin(ts.tenant).await?;
        let store = LogicStore::default();
        store
            .put_dead_letter(
                &mut *conn,
                req.source.binding_id,
                req.version,
                event_key,
                offset.to_string().as_bytes(),
                event,
                reply.attempts.saturating_add(1),
                reply.code,
                &reply.message,
            )
            .await?;
        let _ = store
            .log(
                &mut *conn,
                req.source.module_id,
                Some(req.version),
                "error",
                "trigger",
                &format!(
                    "{}.{} record {event_key}: dead letter after {} attempts: {} {}",
                    req.source.db_name,
                    req.source.name,
                    reply.attempts.saturating_add(1),
                    reply.code,
                    reply.message
                ),
            )
            .await;
        conn.commit().await
    }
}

/// Forgets what this gateway keeps for binding `binding` of `tenant`.
fn forget_binding(tenant: u64, binding: u64) {
    lock(&PENDING).retain(|k, _| !(k.0 == tenant && k.1 == binding));
    lock(&FAILS).retain(|k, _| !(k.0 == tenant && k.1 == binding));
    lock(&TURN).retain(|k, _| !(k.0 == tenant && k.1 == binding));
    lock(&CHECKED).remove(&(tenant, binding));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lanes_split_the_partitions() {
        assert_eq!(lane_partitions(6, 4, 0), vec![0, 4]);
        assert_eq!(lane_partitions(6, 4, 1), vec![1, 5]);
        assert_eq!(lane_partitions(6, 4, 3), vec![3]);
        assert!(lane_partitions(2, 8, 5).is_empty());
        assert_eq!(lane_partitions(3, 1, 0), vec![0, 1, 2]);
        // Every partition is served exactly once.
        let mut all: Vec<u32> = (0..8).flat_map(|j| lane_partitions(13, 8, j)).collect();
        all.sort_unstable();
        assert_eq!(all, (0..13).collect::<Vec<_>>());
    }

    #[test]
    fn records_read_as_ctx_record() {
        let r = StoredRecord {
            offset: 41,
            timestamp_ms: 1_700_000_000_000,
            key: Some(b"k1".to_vec()),
            value: Some(br#"{"a":1}"#.to_vec()),
            headers: vec![sprig_proto_kafka::records::Header {
                key: "h".into(),
                value: Some(vec![0xff, 0x00]),
            }],
            leader_epoch: 0,
            filtered: false,
        };
        let e = record_event("db1.ev", 3, &r);
        assert_eq!(
            e,
            json!({"stream": "db1.ev", "partition": 3, "offset": 41, "key": "k1",
                   "value": "{\"a\":1}", "headers": {"h": "/wA="}, "timestamp": 1_700_000_000_000_i64})
        );
        assert_eq!(event_key(3, 41), "p3:41");
    }
}
