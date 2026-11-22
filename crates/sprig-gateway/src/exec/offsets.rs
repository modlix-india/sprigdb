//! `CALL sprig.commit_offset(group, stream, partition, offset [, metadata])`
//! in a tenant session (for exactly-once consume and write) on a stream of
//! an engine database: the offset is an engine record, written in the
//! session's engine transaction when one is open (so it commits or rolls
//! back with the transaction's writes), else at once. The same records
//! hold a trigger's dead-letter replay marks ([`PassSession::replay_mark`]).

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_streams::offsets;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// One statement's result, kept (internal statements of this module and
/// of [`super::msg_trigger`]).
pub(super) struct Collect {
    wire: Wire,
    pub(super) columns: Vec<String>,
    /// The columns' MySQL types (`LATERAL`'s gateway apply inlines numbers
    /// as numbers).
    pub(super) types: Vec<u8>,
    /// The columns as sent (the AI functions' gateway step keeps the
    /// other columns' metadata).
    pub(super) defs: Vec<ColumnDef>,
    pub(super) rows: Vec<Vec<Option<Vec<u8>>>>,
    pub(super) affected: Option<u64>,
    pub(super) error: Option<SprigError>,
    done: bool,
}

impl Collect {
    pub(super) fn new(wire: Wire) -> Self {
        Self {
            wire,
            columns: Vec::new(),
            types: Vec::new(),
            defs: Vec::new(),
            rows: Vec::new(),
            affected: None,
            error: None,
            done: false,
        }
    }
}

#[async_trait]
impl ResultSink for Collect {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns
            .iter()
            .map(|c| String::from_utf8_lossy(&c.name).into_owned())
            .collect();
        self.types = columns.iter().map(|c| c.col_type).collect();
        self.defs = columns.to_vec();
        Ok(())
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        match values {
            RowValues::Text(v) => self
                .rows
                .push(v.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
            RowValues::Binary(_) => {
                return Err(SprigError::internal(
                    "a binary row in an internal statement",
                ));
            }
        }
        Ok(())
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.len())?
            .into_iter()
            .map(|c| c.map(<[u8]>::to_vec))
            .collect();
        self.rows.push(r);
        Ok(())
    }
    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.affected = Some(ok.affected_rows);
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }
    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::new(
            SprigCode::TypeUnsupported,
            "LOAD DATA LOCAL INFILE can't run inside an internal statement",
        ))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

fn arg_error() -> SprigError {
    SprigError::standard(
        1318,
        *b"42000",
        "Incorrect arguments to sprig.commit_offset",
    )
}

impl PassSession {
    /// Runs `sql` through this session (logical names, its transaction);
    /// `internal` allows `__sprig_` tables. The statement's own error is in
    /// the result, not returned.
    pub(super) async fn run_internal(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        internal: bool,
    ) -> Result<Collect> {
        let mut sink = Collect::new(Wire {
            persona: cx.proto.persona,
            caps: cx.proto.caps,
        });
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = true;
        }
        let text = Bytes::copy_from_slice(sql.as_bytes());
        let r = Box::pin(self.tenant_text(
            cx,
            &text,
            &QueryAttrs::default(),
            false,
            RowFormat::Text,
            &mut sink,
        ))
        .await;
        if internal && let Some(ts) = self.tenant.as_deref_mut() {
            ts.types.internal = false;
        }
        r?;
        Ok(sink)
    }

    /// As [`PassSession::run_internal`], with the statement's error
    /// returned.
    pub(super) async fn run_checked(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        internal: bool,
    ) -> Result<Collect> {
        let mut c = self.run_internal(cx, sql, internal).await?;
        match c.error.take() {
            Some(e) => Err(e),
            None => Ok(c),
        }
    }

    /// Whether the session is inside a transaction.
    pub(super) fn offsets_in_transaction(&self) -> bool {
        let s = self.status();
        s.contains(StatusFlags::IN_TRANS) || !s.contains(StatusFlags::AUTOCOMMIT)
    }

    /// The offset-commit step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't `CALL sprig.commit_offset(...)`.
    pub(super) async fn tenant_offsets(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal
            || !text
                .windows(13)
                .any(|w| w.eq_ignore_ascii_case(b"commit_offset"))
        {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let Some(args) = offsets::parse_call(sql) else {
            return Ok(None);
        };
        let r = self.commit_offset_call(cx, &args).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        if let Err(e) = r {
            return Ok(Some(self.fail(out, &e).await?));
        }
        let mut ok = OkPacket::new(status);
        ok.affected_rows = 1;
        out.ok(ok).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_offset_call(&mut self, cx: &mut ExecCtx<'_>, args: &[String]) -> Result<()> {
        if !(4..=5).contains(&args.len()) {
            return Err(arg_error());
        }
        let vals = {
            let c = self
                .run_checked(cx, &format!("SELECT {}", args.join(", ")), false)
                .await?;
            c.rows.into_iter().next().ok_or_else(arg_error)?
        };
        let text = |i: usize| -> Option<String> {
            vals.get(i)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        let num = |i: usize| -> Result<u64> {
            text(i)
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or_else(arg_error)
        };
        let group = text(0).filter(|g| !g.is_empty()).ok_or_else(arg_error)?;
        if group.chars().count() > 255 {
            return Err(arg_error());
        }
        let stream = text(1).ok_or_else(arg_error)?;
        let partition = u32::try_from(num(2)?).map_err(|_| arg_error())?;
        let offset = num(3)?;
        let metadata = text(4);
        if metadata.as_ref().is_some_and(|m| m.chars().count() > 1024) {
            return Err(SprigError::standard(
                1406,
                *b"22001",
                "Data too long for column 'metadata': at most 1024 characters",
            ));
        }
        let current = self.current_logical();
        // A stream of an engine database: its offset is an engine record.
        let (db_text, name_text) = match stream.split_once('.') {
            Some((d, n)) => (Some(d.to_owned()), n.to_owned()),
            None => (current.clone(), stream.clone()),
        };
        if let Some(db) = db_text.filter(|d| self.engine_database_named(d)) {
            return self
                .commit_offset_engine(&group, &db, &name_text, partition, offset, metadata)
                .await;
        }
        Err(SprigError::new(
            SprigCode::StreamNotFound,
            format!("no stream {stream}: streams are kept in engine databases"),
        ))
    }

    /// `CALL sprig.commit_offset` on stream `name` of engine database `db`:
    /// the group's offset is an engine record of the node's streams,
    /// written in the session's engine transaction when one is open (so it
    /// commits or rolls back with the transaction's writes), else at once.
    ///
    /// # Errors
    /// 39940-range stream errors when the stream or partition doesn't
    /// exist; 1142 without `SELECT` on it; the engine's.
    async fn commit_offset_engine(
        &mut self,
        group: &str,
        db: &str,
        name: &str,
        partition: u32,
        offset: u64,
        metadata: Option<String>,
    ) -> Result<()> {
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let tenant = ts.tenant;
        let Some(eng) = crate::kafka::engine::streams() else {
            return Err(SprigError::new(
                SprigCode::StreamNotFound,
                format!(
                    "no stream {db}.{name}: streams of engine databases aren't served on this node"
                ),
            ));
        };
        let topic = eng
            .topic(tenant.0, &format!("{db}.{name}"))?
            .ok_or_else(|| {
                SprigError::new(SprigCode::StreamNotFound, format!("no stream {db}.{name}"))
            })?;
        if u32::try_from(topic.partitions).unwrap_or(0) <= partition {
            return Err(SprigError::new(
                SprigCode::StreamNotFound,
                format!("stream {db}.{name} has no partition {partition}"),
            ));
        }
        // Committing a position is reading the stream.
        {
            let cat = match ts.catalog() {
                Some(c) => std::sync::Arc::clone(c),
                None => ts.refresh().await?,
            };
            let lctn = ts.lower_case_table_names();
            let grants = crate::tenant::authz::privileges(ts, &cat);
            let db_key = database_key(db, lctn);
            let name_key = object_key(ObjectKind::Table, name, lctn);
            if !grants.unrestricted()
                && !grants.allows(
                    Action::Select,
                    Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    },
                )
                && !grants.allows(Action::Select, Target::Database { db_key: &db_key })
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "SELECT command denied to user '{}'@'{}' for table `{db}`.`{name}`",
                        ts.principal.user, ts.principal.host
                    ),
                ));
            }
        }
        let o = sprig_proto_kafka::group::CommittedOffset {
            stream_id: topic.stream_id,
            partition: i32::try_from(partition).unwrap_or(i32::MAX),
            offset: i64::try_from(offset).unwrap_or(i64::MAX),
            leader_epoch: None,
            metadata,
        };
        let now = sprig_streams::seg::now_ms();
        if self.offsets_in_transaction() {
            return match self.eng.txn.open_tx() {
                Some(tx) => {
                    sprig_streams::seg::records::put_offsets(
                        tx.engine_txn(),
                        tenant.0,
                        group,
                        0,
                        std::slice::from_ref(&o),
                        now,
                    )
                    .await
                }
                None => Err(SprigError::standard(
                    1235,
                    *b"42000",
                    "This version of SprigDB doesn't yet support 'sprig.commit_offset on a stream \
                     of an engine database inside a transaction that isn't on the engine'",
                )),
            };
        }
        eng.records()
            .commit_offsets(tenant.0, group, std::slice::from_ref(&o), now)
            .await
    }

    /// Marks the replay of dead letter `dl_id` of the trigger whose replay
    /// group is `group`, in the session's engine transaction: `false` when
    /// it is already marked (a repeated run, which must not run again).
    ///
    /// # Errors
    /// 1235 when the session has no engine transaction; the engine's.
    pub(super) async fn replay_mark(&mut self, group: &str, dl_id: u64) -> Result<bool> {
        let tenant = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .tenant;
        let now = sprig_streams::seg::now_ms();
        match self.eng.txn.open_tx() {
            Some(tx) => {
                sprig_streams::seg::records::move_offset(
                    tx.engine_txn(),
                    tenant.0,
                    group,
                    dl_id,
                    0,
                    0,
                    1,
                    now,
                )
                .await
            }
            None => Err(SprigError::standard(
                1235,
                *b"42000",
                "This version of SprigDB doesn't yet support 'a trigger's dead-letter replay \
                 outside a transaction on the engine'",
            )),
        }
    }
}
