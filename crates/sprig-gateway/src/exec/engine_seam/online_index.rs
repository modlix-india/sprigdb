//! Plain and unique indexes a user's statement adds to an engine table,
//! built online: `ALTER TABLE t ADD [UNIQUE] INDEX k (...)[, ADD KEY ...]`,
//! `ADD CONSTRAINT c UNIQUE (...)` and `CREATE [UNIQUE] INDEX k ON t
//! (...)`, with no `ALGORITHM = COPY` or `LOCK = SHARED | EXCLUSIVE`
//! (`engine::alter::adds_indexes_only`). The table stays readable and
//! writable while the entries are written:
//!
//! ```text
//! the statement, as usual, with engine::ddl::USER_ONLINE set: each new
//!   index is published IGNORED and without entries (the change waits for
//!   the table's users once, briefly); every write from then on keeps it,
//!   a unique one refusing duplicates among the rows written since, and no
//!   plan reads it. The table gets an online build mark naming them.
//! backfill   Engine::backfill: a batch of rows per transaction, rows read
//!            with shared locks (a write to one of them waits for its
//!            batch), the table never held; a unique index's entry another
//!            row already holds is a duplicate (1062)
//! public     ALTER INDEX k NOT IGNORED, a metadata-only change; plans may
//!            read it; the mark goes
//! the client gets the statement's own OK then (affected rows, info)
//! ```
//!
//! A backfill or publish that fails drops the new indexes and answers its
//! error. A gateway that dies in between leaves `IGNORED` indexes that
//! writes keep, and the mark: the next process of the same node finishes
//! the build when the tenant's first engine statement arrives
//! ([`resume_once`]), and on any node `ALTER TABLE t ALTER INDEX k NOT
//! IGNORED` (or `VISIBLE`) writes the missing entries before it makes the
//! index public. A full-text or spatial index, and any other change in the
//! same statement, take the usual path (the change holds the table while
//! it writes the entries).

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_sql::names::{DdlKind, DdlTarget};

use crate::exec::PassSession;
use crate::exec::QueryAttrs;
use crate::exec::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::TenantServices;

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
    /// DDL statement `target` (client text `text`) when it only adds
    /// indexes to an engine table: built online (see the module docs).
    /// `None` for any other statement; one that makes indexes of an
    /// unfinished online build public first writes their entries (then
    /// `None`: it runs as usual).
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
        let adds = crate::engine::alter::adds_indexes_only(sql);
        let public = if adds {
            Vec::new()
        } else {
            crate::engine::alter::made_public(sql)?
        };
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
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let ts = self.tenant.as_deref()?;
        let (svc, tenant) = (Arc::clone(ts.services()), ts.tenant);
        if !adds {
            return match resume_named(&svc, tenant, &db, &name.name, &public, mysql).await {
                Ok(()) => None,
                Err(e) => Some(self.fail(out, &e).await),
            };
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
        if let Err(e) = finish(&svc, tenant, &db, &name.name, &built, mysql).await {
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
}

/// Writes the entries of the new indexes `built` of `db`.`table`, then
/// makes them public; on a failure they are dropped. Either way the
/// table's online build mark goes. `mysql`: a duplicate's key is named as
/// MySQL names it.
async fn finish(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    table: &str,
    built: &[String],
    mysql: bool,
) -> Result<()> {
    if built.is_empty() {
        return Ok(());
    }
    let eng = crate::engine::engine()?;
    let mut object = None;
    let r = async {
        let cat = svc.cache.get(tenant, Freshness::Latest).await?;
        let def = crate::autoindex::ddl::table_def(svc, &cat, db, table)
            .ok_or_else(|| SprigError::internal(format!("table {db}.{table} went")))?;
        object = crate::engine::object_of(&def).ok();
        let cache = svc.cache.clone();
        let (d, t) = (db.to_owned(), table.to_owned());
        let svc2 = Arc::clone(svc);
        let version_now = move || {
            let cat = cache.cached(tenant)?;
            crate::autoindex::ddl::table_def(&svc2, &cat, &d, &t).map(|x| x.version)
        };
        for n in built {
            let (rows, _) = eng
                .backfill(
                    tenant.0,
                    &def,
                    n,
                    BATCH,
                    Duration::ZERO,
                    &version_now,
                    mysql,
                )
                .await?;
            tracing::info!(%tenant, db, table, index = %n, rows, "online index: entries written");
        }
        let clause = built
            .iter()
            .map(|n| format!("ALTER INDEX {} NOT IGNORED", q(n)))
            .collect::<Vec<_>>()
            .join(", ");
        crate::autoindex::ddl::alter(svc, tenant, db, table, &clause, WAIT, false).await
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
            crate::autoindex::ddl::alter(svc, tenant, db, table, &clause, WAIT, false).await
        {
            tracing::warn!(%tenant, db, table, error = %e2, "an unfinished online index wasn't dropped");
        }
    }
    if let Some(o) = object
        && let Err(e) = eng.unmark_online(tenant.0, o).await
    {
        tracing::warn!(%tenant, db, table, error = %e, "an online build mark wasn't removed");
    }
    r
}

/// `ALTER TABLE db.table ALTER INDEX k NOT IGNORED` for indexes `names`:
/// those an unfinished online build left get their entries first. The
/// statement itself then makes them public (and a failure here answers
/// the backfill's error, leaving them as they were).
async fn resume_named(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    table: &str,
    names: &[String],
    mysql: bool,
) -> Result<()> {
    let cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let Some(def) = crate::autoindex::ddl::table_def(svc, &cat, db, table) else {
        return Ok(());
    };
    let Ok(object) = crate::engine::object_of(&def) else {
        return Ok(());
    };
    let eng = crate::engine::engine()?;
    let Some((_, _, marked)) = eng
        .online_marks(tenant.0)?
        .into_iter()
        .find(|(o, _, _)| *o == object)
    else {
        return Ok(());
    };
    let unfinished: Vec<&String> = marked
        .iter()
        .filter(|m| names.iter().any(|n| n.eq_ignore_ascii_case(m)))
        .collect();
    if unfinished.is_empty() {
        return Ok(());
    }
    let cache = svc.cache.clone();
    let (d, t) = (db.to_owned(), table.to_owned());
    let svc2 = Arc::clone(svc);
    let version_now = move || {
        let cat = cache.cached(tenant)?;
        crate::autoindex::ddl::table_def(&svc2, &cat, &d, &t).map(|x| x.version)
    };
    for n in &unfinished {
        let (rows, _) = eng
            .backfill(
                tenant.0,
                &def,
                n,
                BATCH,
                Duration::ZERO,
                &version_now,
                mysql,
            )
            .await?;
        tracing::info!(%tenant, db, table, index = %n, rows, "online index: unfinished build's entries written");
    }
    // What the mark names and the statement doesn't stays marked.
    let rest: Vec<&str> = marked
        .iter()
        .filter(|m| !unfinished.contains(m))
        .map(String::as_str)
        .collect();
    if rest.is_empty() {
        eng.unmark_online(tenant.0, object).await
    } else {
        eng.mark_online(tenant.0, object, &rest).await
    }
}

/// Once per tenant and process (the tenant's first engine statement):
/// the online builds an earlier process of this node left unfinished are
/// finished in the background (their indexes' entries written, then made
/// public), and marks naming no unfinished index go.
pub(in crate::exec) fn resume_once(svc: &Arc<TenantServices>, cat: &TenantCatalog) {
    static DONE: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    let tenant = cat.tenant;
    if !DONE
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(tenant.0)
    {
        return;
    }
    let Ok(eng) = crate::engine::engine() else {
        return;
    };
    let marks = match eng.online_marks(tenant.0) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(%tenant, error = %e, "online build marks not read");
            return;
        }
    };
    let mut work = Vec::new();
    for (object, owner, names) in marks {
        if !crate::engine::temp::orphaned(&owner) {
            continue;
        }
        let row = cat.objects.iter().find(|o| {
            u64::from(object) == o.object_id.0
                && o.kind == sprig_catalog::ObjectKind::Table
                && o.state != sprig_catalog::ObjectState::Dropped
        });
        let place =
            row.and_then(|o| Some((cat.databases.get(o.db_id)?.name.clone(), o.name.clone())));
        work.push((object, place, names));
    }
    if work.is_empty() {
        return;
    }
    let svc = Arc::clone(svc);
    tokio::spawn(async move {
        for (object, place, names) in work {
            let unfinished = match &place {
                Some((db, table)) => {
                    let cat = svc.cache.get(tenant, Freshness::Latest).await;
                    cat.ok()
                        .and_then(|cat| crate::autoindex::ddl::table_def(&svc, &cat, db, table))
                        .map(|def| {
                            names
                                .iter()
                                .filter(|n| {
                                    def.indexes
                                        .iter()
                                        .any(|x| x.ignored && x.name.eq_ignore_ascii_case(n))
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                }
                None => Vec::new(),
            };
            match (&place, unfinished.is_empty()) {
                (Some((db, table)), false) => {
                    match finish(&svc, tenant, db, table, &unfinished, false).await {
                        Ok(()) => {
                            tracing::info!(%tenant, db, table, indexes = ?unfinished, "an unfinished online index build was finished");
                        }
                        Err(e) => {
                            tracing::warn!(%tenant, db, table, error = %e, "an unfinished online index build failed; its indexes were dropped");
                        }
                    }
                }
                _ => {
                    if let Err(e) = eng.unmark_online(tenant.0, object).await {
                        tracing::warn!(%tenant, object, error = %e, "an online build mark wasn't removed");
                    }
                }
            }
        }
    });
}
