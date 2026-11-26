//! Plain indexes a user's statement adds to an engine table, built online:
//! `ALTER TABLE t ADD INDEX k (...)[, ADD KEY ...]` and `CREATE INDEX k ON
//! t (...)`, with no `ALGORITHM = COPY` or `LOCK = SHARED | EXCLUSIVE`
//! (`engine::alter::adds_plain_indexes_only`). The table stays readable and
//! writable while the entries are written:
//!
//! ```text
//! the statement, as usual, with engine::ddl::USER_ONLINE set: each new
//!   plain index is published IGNORED and without entries (the change
//!   waits for the table's users once, briefly); every write from then on
//!   keeps it, and no plan reads it
//! backfill   Engine::backfill_index: a batch of rows per transaction, rows
//!            read with shared locks (a write to one of them waits for its
//!            batch), the table never held
//! public     ALTER INDEX k NOT IGNORED, a metadata-only change; plans may
//!            read it
//! the client gets the statement's own OK then (affected rows, info)
//! ```
//!
//! A backfill or publish that fails drops the new indexes and answers its
//! error. A gateway that dies in between leaves an `IGNORED` index that
//! writes keep: `ALTER TABLE t ALTER INDEX k NOT IGNORED` would show it
//! unfinished, so drop and add it again. A unique, full-text or spatial
//! index, and any other change in the same statement, take the usual path
//! (the change holds the table while it writes the entries).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::Freshness;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_sql::names::{DdlKind, DdlTarget};

use crate::exec::PassSession;
use crate::exec::QueryAttrs;
use crate::exec::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// Rows per backfill transaction.
const BATCH: usize = 1_000;

/// How long the publish waits for the table's users.
const WAIT: Duration = Duration::from_secs(60);

/// A sink that passes everything on but keeps the statement's final OK.
struct HoldOk<'a> {
    inner: &'a mut dyn ResultSink,
    ok: Option<OkPacket>,
}

#[async_trait]
impl ResultSink for HoldOk<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.ok = Some(ok);
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.inner.error(err).await
    }
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.column_list(columns, end).await
    }
    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }
    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
    fn finished(&self) -> bool {
        self.ok.is_some() || self.inner.finished()
    }
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

impl PassSession {
    /// DDL statement `target` (client text `text`) when it only adds plain
    /// indexes to an engine table: built online (see the module docs).
    /// `None` for any other statement.
    pub(in crate::exec) async fn engine_online_index(
        &mut self,
        cx: &mut ExecCtx<'_>,
        target: &DdlTarget,
        text: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Option<Result<Step>> {
        // The statement itself comes back here while it runs.
        if crate::engine::ddl::USER_ONLINE.try_with(|_| ()).is_ok() {
            return None;
        }
        let DdlTarget::AlterObject {
            kind: DdlKind::Table,
            name,
            ..
        } = target
        else {
            return None;
        };
        let sql = std::str::from_utf8(text).ok()?;
        if !crate::engine::alter::adds_plain_indexes_only(sql) {
            return None;
        }
        let db = name.db.clone().or_else(|| self.current_logical())?;
        if !self.engine_database_named(&db) {
            return None;
        }
        // A temporary table of that name (it hides the table) takes its
        // own path.
        if self
            .eng
            .temps
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(&name.name))
        {
            return None;
        }
        let names = Arc::new(Mutex::new(Vec::new()));
        let text = Bytes::copy_from_slice(text);
        let (ran, held) = {
            let mut hold = HoldOk {
                inner: &mut *out,
                ok: None,
            };
            let ran = crate::engine::ddl::USER_ONLINE
                .scope(
                    Arc::clone(&names),
                    Box::pin(self.tenant_text(
                        cx,
                        &text,
                        &QueryAttrs::default(),
                        false,
                        RowFormat::Text,
                        &mut hold,
                    )),
                )
                .await;
            (ran, hold.ok.take())
        };
        let step = match ran {
            Err(e) => return Some(Err(e)),
            Ok(step) => step,
        };
        // An error was answered already.
        let mut ok = held?;
        let built = names.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if let Err(e) = self.online_finish(&db, &name.name, &built).await {
            return Some(self.fail(out, &e).await);
        }
        if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
            ok.status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let st = ok.status;
        if let Err(e) = out.ok(ok).await {
            return Some(Err(e));
        }
        Some(Ok(Step {
            errored: step.errored,
            status: Some(st),
        }))
    }

    /// Writes the entries of the new indexes `built` of `db`.`table`, then
    /// makes them public; on a failure they are dropped.
    async fn online_finish(&mut self, db: &str, table: &str, built: &[String]) -> Result<()> {
        if built.is_empty() {
            return Ok(());
        }
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let r = async {
            let cat = svc.cache.get(tenant, Freshness::Latest).await?;
            let def = crate::autoindex::ddl::table_def(&svc, &cat, db, table)
                .ok_or_else(|| SprigError::internal(format!("table {db}.{table} went")))?;
            let eng = crate::engine::engine()?;
            let cache = svc.cache.clone();
            let (d, t) = (db.to_owned(), table.to_owned());
            let svc2 = Arc::clone(&svc);
            let version_now = move || {
                let cat = cache.cached(tenant)?;
                crate::autoindex::ddl::table_def(&svc2, &cat, &d, &t).map(|x| x.version)
            };
            for n in built {
                let (rows, _) = eng
                    .backfill_index(tenant.0, &def, n, BATCH, Duration::ZERO, &version_now)
                    .await?;
                tracing::info!(%tenant, db, table, index = %n, rows, "online index: entries written");
            }
            let clause = built
                .iter()
                .map(|n| format!("ALTER INDEX {} NOT IGNORED", q(n)))
                .collect::<Vec<_>>()
                .join(", ");
            crate::autoindex::ddl::alter(&svc, tenant, db, table, &clause, WAIT, false).await
        }
        .await;
        if let Err(e) = &r {
            tracing::warn!(%tenant, db, table, error = %e, "online index build failed; its indexes are dropped");
            let clause = built
                .iter()
                .map(|n| format!("DROP INDEX {}", q(n)))
                .collect::<Vec<_>>()
                .join(", ");
            if let Err(e2) =
                crate::autoindex::ddl::alter(&svc, tenant, db, table, &clause, WAIT, false).await
            {
                tracing::warn!(%tenant, db, table, error = %e2, "an unfinished online index wasn't dropped");
            }
        }
        r
    }
}
