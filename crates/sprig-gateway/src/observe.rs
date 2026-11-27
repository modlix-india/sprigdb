//! Statement SLIs and the root trace span.
//!
//! [`observed`] wraps the gateway's executor, outermost, so every request
//! of every front end (MySQL, MariaDB, HTTP) passes it:
//!
//! - **SLIs.** `sprig_gateway_statements_total` (`protocol`, `result`),
//!   `sprig_gateway_query_duration_seconds` (`protocol`: admission, the
//!   gateway and MariaDB) and `sprig_gateway_overhead_seconds`
//!   (`protocol`: the same minus the time spent waiting on MariaDB, which
//!   the backend layer adds up through [`backend_time`]). `result` is `ok`,
//!   `client` (errors the application caused), `sprig` (errors SprigDB
//!   caused) or `backend` (MariaDB unreachable or failed mid-statement);
//!   see [`classify`]. Silent requests (`COM_STMT_SEND_LONG_DATA`,
//!   `COM_STMT_CLOSE`) and requests cancelled by `KILL` or a hang-up are not
//!   counted. A tenant whose statements reached
//!   `telemetry.tenant_series_min_rps` over its last full minute gets its
//!   own series: the `tenant` label (its id) on
//!   `sprig_gateway_statements_total` and
//!   `sprig_gateway_query_duration_seconds`; every other statement has
//!   `tenant=""`, which `PromQL` treats as no label, so the series count
//!   stays bounded by the busy tenants.
//! - **Traces.** A request that is head-sampled
//!   (`telemetry.trace_sample_ratio`) or carries a W3C `traceparent` query
//!   attribute runs inside a `gateway.request` span (target [`TARGET`])
//!   carrying the tenant id, protocol and request kind; the backend
//!   layer's `backend.query` spans are its children. The span records
//!   `result` and `error = true` on failure. Building a span for every
//!   statement cost more than a microsecond each, so the other requests
//!   run without one: they are timed and counted the same, and one that
//!   fails or is slower than `telemetry.trace_slow_ms` is still kept as a
//!   trace (tail sampling), of its root alone. Their log lines carry no
//!   request span fields.
//!
//! No SQL text and no values go into spans or labels.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::telemetry::trace::{self, TARGET};
use sprig_common::time::poll;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, Value};
use tracing::Instrument as _;

use crate::pipeline::{
    Abandon, EndOfRows, ExecCtx, ExecSummary, Executor, LoginInfo, Request, ResultSink, RowFormat,
    RowValues, SessionExec, SharedExecutor, Wire,
};

tokio::task_local! {
    /// Nanoseconds the current request spent waiting on MariaDB.
    static BACKEND_NANOS: Arc<AtomicU64>;
}

/// Whether requests get their `gateway.request` span: on unless the
/// gateway's environment has `SPRIGDB_REQUEST_SPANS=off` (for comparisons
/// of the span's cost; without it no request is traced or logged with
/// its span's fields).
fn request_spans() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("SPRIGDB_REQUEST_SPANS").map_or(true, |v| v != "off"))
}

/// Adds `d` to the current request's time waiting on MariaDB (the backend
/// layer calls it around each round trip). Outside a request, nothing.
pub fn backend_time(d: Duration) {
    let _ = BACKEND_NANOS.try_with(|n| {
        n.fetch_add(
            u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    });
}

/// Counts a failed request for the administration agent: its code at
/// `<protocol>.<request>`; no text, no names.
fn count_error(code: u16, protocol: &str, request: &str) {
    sprig_common::incidents::record_code(code, &format!("{protocol}.{request}"));
}

/// The `result` label of an error.
#[must_use]
pub fn classify(e: &SprigError) -> &'static str {
    match e.kind {
        ErrorKind::Backend { errno, .. } => match errno {
            // Server shutting down, too many connections, gone away, lost
            // mid-query, connection killed by the server: MariaDB failed.
            1040 | 1053 | 1077 | 1152 | 1158..=1161 | 1927 | 2006 | 2013 => "backend",
            _ => "client",
        },
        ErrorKind::Standard { .. } => "client",
        ErrorKind::Sprig(code) => match code {
            SprigCode::BackendUnavailable
            | SprigCode::BackendPoolTimeout
            | SprigCode::BackendLost
            | SprigCode::BackendProtocol
            | SprigCode::ClusterUnavailable => "backend",
            _ if e.protocol().http_status < 500 => "client",
            _ => "sprig",
        },
    }
}

const RATE_SHARDS: usize = 16;
const RATE_WINDOW: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct Rate {
    since: Instant,
    count: u64,
    busy: bool,
}

/// Per-tenant statement rates, sharded to keep the lock short.
struct TenantRates {
    shards: [std::sync::Mutex<std::collections::HashMap<u64, Rate>>; RATE_SHARDS],
}

impl TenantRates {
    fn get() -> &'static TenantRates {
        static RATES: std::sync::OnceLock<TenantRates> = std::sync::OnceLock::new();
        RATES.get_or_init(|| TenantRates {
            shards: std::array::from_fn(
                |_| std::sync::Mutex::new(std::collections::HashMap::new()),
            ),
        })
    }

    /// Counts one statement of `tenant`; `true` while the tenant was at
    /// `telemetry.tenant_series_min_rps` over its last full window.
    fn busy(&self, tenant: u64, now: Instant) -> bool {
        let shard = usize::try_from(tenant % RATE_SHARDS as u64).unwrap_or(0);
        let mut m = self.shards[shard]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let r = m.entry(tenant).or_insert(Rate {
            since: now,
            count: 0,
            busy: false,
        });
        r.count += 1;
        let elapsed = now.saturating_duration_since(r.since);
        if elapsed >= RATE_WINDOW {
            let min = sprig_common::telemetry::tenant_series_min_rps();
            r.busy = r.count >= min.saturating_mul(elapsed.as_secs());
            r.since = now;
            r.count = 0;
        }
        r.busy
    }
}

/// `exec` with SLI metrics and the root span in front.
#[must_use]
pub fn observed(exec: SharedExecutor) -> SharedExecutor {
    Arc::new(Observed { inner: exec })
}

struct Observed {
    inner: SharedExecutor,
}

impl Executor for Observed {
    fn open(&self, login: &LoginInfo) -> Box<dyn SessionExec> {
        Box::new(ObservedSession {
            inner: self.inner.open(login),
            tenant: login.principal.tenant.map(|t| t.0),
            sli: Sli::default(),
            internal: false,
        })
    }
}

struct ObservedSession {
    inner: Box<dyn SessionExec>,
    tenant: Option<u64>,
    sli: Sli,
    /// The session runs a protocol engine's own statements
    /// ([`SessionExec::set_internal`]).
    internal: bool,
}

/// Whether the innermost entered span is a traced request already (a
/// MongoDB command's `gateway.request`): an engine's
/// statements inside it record no root span of their own.
fn in_traced_request() -> bool {
    tracing::Span::current()
        .metadata()
        .is_some_and(|m| m.target() == TARGET)
}

/// A session's SLI metric handles, registered on first use (registering
/// them with their labels on every statement cost about 2% of a MongoDB
/// point read's gateway CPU). A session's protocol and tenant
/// don't change; `result` and whether the tenant has its own series do.
#[derive(Default)]
struct Sli {
    /// `sprig_gateway_statements_total` by (result, tenant series).
    statements: Vec<(&'static str, bool, metrics::Counter)>,
    /// `sprig_gateway_query_duration_seconds` without and with the tenant
    /// series.
    duration: [Option<metrics::Histogram>; 2],
    overhead: Option<metrics::Histogram>,
}

impl Sli {
    fn record(
        &mut self,
        protocol: &'static str,
        tenant: Option<u64>,
        busy: bool,
        result: &'static str,
        total: Duration,
        overhead: Duration,
    ) {
        let label = || match tenant {
            Some(t) if busy => t.to_string(),
            _ => String::new(),
        };
        let i = if let Some(i) = self
            .statements
            .iter()
            .position(|(r, b, _)| *r == result && *b == busy)
        {
            i
        } else {
            let c = metrics::counter!("sprig_gateway_statements_total", "protocol" => protocol, "result" => result, "tenant" => label());
            self.statements.push((result, busy, c));
            self.statements.len() - 1
        };
        self.statements[i].2.increment(1);
        self.duration[usize::from(busy)]
            .get_or_insert_with(|| metrics::histogram!("sprig_gateway_query_duration_seconds", "protocol" => protocol, "tenant" => label()))
            .record(total.as_secs_f64());
        self.overhead
            .get_or_insert_with(
                || metrics::histogram!("sprig_gateway_overhead_seconds", "protocol" => protocol),
            )
            .record(overhead.as_secs_f64());
    }
}

fn traceparent(req: &Request) -> Option<String> {
    let Request::Query { attrs, .. } = req else {
        return None;
    };
    match attrs.get(b"traceparent")? {
        Value::Bytes(b) => std::str::from_utf8(b).ok().map(str::to_owned),
        _ => None,
    }
}

#[async_trait]
impl SessionExec for ObservedSession {
    /// A point read is always an engine's statement inside its traced
    /// command: its SLIs, no span of its own.
    async fn point_read(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: crate::pipeline::StmtRef,
        params: &sprig_proto_mysql::command::ExecuteParams,
    ) -> Result<crate::pipeline::PointRead> {
        let protocol = cx.session.protocol.as_str();
        let waited = Arc::new(AtomicU64::new(0));
        let started = poll::start();
        let run = Box::pin(self.inner.point_read(cx, stmt, params));
        let r = BACKEND_NANOS.scope(Arc::clone(&waited), run).await;
        // A fallback is counted by the execute that follows it.
        if matches!(r, Ok(crate::pipeline::PointRead::Fallback)) {
            return r;
        }
        let total = poll::end().saturating_duration_since(started);
        let result = match &r {
            Ok(_) => "ok",
            Err(e) => {
                count_error(sprig_common::incidents::code_of(e), protocol, "point_read");
                classify(e)
            }
        };
        let backend = Duration::from_nanos(waited.load(Ordering::Relaxed));
        let busy = self
            .tenant
            .is_some_and(|t| TenantRates::get().busy(t, poll::now()));
        self.sli.record(
            protocol,
            self.tenant,
            busy,
            result,
            total,
            total.saturating_sub(backend),
        );
        r
    }

    fn holds_idle(&self) -> bool {
        self.inner.holds_idle()
    }

    fn release_idle(&mut self) {
        self.inner.release_idle();
    }

    fn keep_idle(&mut self, on: bool) {
        self.inner.keep_idle(on);
    }

    async fn execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: Request,
        out: &mut dyn ResultSink,
    ) -> Result<ExecSummary> {
        if req.is_silent() {
            return self.inner.execute(cx, req, out).await;
        }
        let protocol = cx.session.protocol.as_str();
        let request = req.name();
        // One span per request that is head-sampled or carries a trace
        // context; the others are timed and counted as well, and kept as a
        // one-span trace if they fail or are slow (`unsampled_root`). An
        // engine statement inside a traced command (MongoDB's) is part of
        // that command's span.
        let traced = !((self.internal && in_traced_request()) || !request_spans());
        let parent = if traced { traceparent(&req) } else { None };
        let sampled_here = traced && parent.is_none() && trace::head_sampled();
        let span = if parent.is_some() || sampled_here {
            Some(tracing::info_span!(
                target: TARGET,
                "gateway.request",
                protocol,
                request = req.name(),
                tenant = self.tenant,
                gateway_session = cx.session.id.0,
                traceparent = parent,
                sampled = sampled_here,
                result = tracing::field::Empty,
                error = tracing::field::Empty,
            ))
        } else {
            None
        };
        let waited = Arc::new(AtomicU64::new(0));
        let started = poll::start();
        let mut sink = Watch {
            inner: out,
            result: None,
            code: None,
        };
        let run = self.inner.execute(cx, req, &mut sink);
        let r = match &span {
            Some(sp) => {
                BACKEND_NANOS
                    .scope(Arc::clone(&waited), run.instrument(sp.clone()))
                    .await
            }
            None => BACKEND_NANOS.scope(Arc::clone(&waited), run).await,
        };
        let total = poll::end().saturating_duration_since(started);
        let result = match &r {
            Ok(_) => sink.result.unwrap_or("ok"),
            Err(e) => classify(e),
        };
        match (&r, sink.code) {
            (Err(e), _) => count_error(sprig_common::incidents::code_of(e), protocol, request),
            (Ok(_), Some(code)) => count_error(code, protocol, request),
            _ => {}
        }
        if let Some(span) = &span {
            // `ok` matters only to a trace that may be kept: recording a
            // field makes the log formatter write the span's fields again.
            if result != "ok" {
                span.record("result", result);
                span.record("error", true);
            } else if trace::may_keep(span, total) {
                span.record("result", result);
            }
        } else if traced {
            let (tenant, session) = (self.tenant, cx.session.id.0);
            trace::unsampled_root(
                "gateway.request",
                || {
                    let mut a = vec![
                        ("protocol".to_owned(), protocol.to_owned()),
                        ("request".to_owned(), request.to_owned()),
                    ];
                    if let Some(t) = tenant {
                        a.push(("tenant".to_owned(), t.to_string()));
                    }
                    a.push(("gateway_session".to_owned(), session.to_string()));
                    a.push(("result".to_owned(), result.to_owned()));
                    if result != "ok" {
                        a.push(("error".to_owned(), "true".to_owned()));
                    }
                    a
                },
                total,
                result != "ok",
            );
        }
        let backend = Duration::from_nanos(waited.load(Ordering::Relaxed));
        let busy = self
            .tenant
            .is_some_and(|t| TenantRates::get().busy(t, poll::now()));
        self.sli.record(
            protocol,
            self.tenant,
            busy,
            result,
            total,
            total.saturating_sub(backend),
        );
        r
    }

    async fn init_db(&mut self, cx: &mut ExecCtx<'_>, schema: &str) -> Result<()> {
        self.inner.init_db(cx, schema).await
    }

    async fn reset(&mut self, cx: &mut ExecCtx<'_>) -> Result<()> {
        self.inner.reset(cx).await
    }

    async fn abandoned(&mut self, cx: &mut ExecCtx<'_>, why: Abandon) {
        self.inner.abandoned(cx, why).await;
    }

    fn take_cut_commit(&mut self) -> Option<Result<()>> {
        self.inner.take_cut_commit()
    }

    async fn close(&mut self) {
        self.inner.close().await;
    }

    fn attach_capture(&mut self, capture: crate::workload::SessionCapture) {
        self.inner.attach_capture(capture);
    }

    fn workload(&self, db: &str, table: &str) -> Option<crate::workload::FrontCapture> {
        self.inner.workload(db, table)
    }

    fn idle_timeout(&self) -> Option<std::time::Duration> {
        self.inner.idle_timeout()
    }

    fn set_internal(&mut self, on: bool) {
        self.internal = on;
        self.inner.set_internal(on);
    }

    fn row_policy_binds(&self, object: u64) -> Option<bool> {
        self.inner.row_policy_binds(object)
    }

    fn engine_database(&self, name: &str) -> bool {
        self.inner.engine_database(name)
    }

    fn row_policy_docs(&self) -> Option<std::sync::Arc<crate::tenant::rls::DocViews>> {
        self.inner.row_policy_docs()
    }

    fn holds_tenant_privilege(&self, action: sprig_auth::authz::Action) -> Option<bool> {
        self.inner.holds_tenant_privilege(action)
    }

    fn holds_privilege_on(
        &self,
        action: sprig_auth::authz::Action,
        db: &str,
        name: Option<&str>,
    ) -> Option<bool> {
        self.inner.holds_privilege_on(action, db, name)
    }

    fn row_guard(&self) -> Option<std::sync::Arc<sprig_doc::docstore::guard::RowGuard>> {
        self.inner.row_guard()
    }

    fn set_claims(&mut self, claims: Option<std::sync::Arc<serde_json::Value>>) {
        self.inner.set_claims(claims);
    }

    fn lend_engine_txn(
        &mut self,
        txn: Option<sprig_engine::txn::Txn>,
    ) -> Option<sprig_engine::txn::Txn> {
        self.inner.lend_engine_txn(txn)
    }
}

/// A sink that notes the class of an error relayed through it (a backend
/// ERR packet reaches the client as `Ok` from the executor).
struct Watch<'a> {
    inner: &'a mut dyn ResultSink,
    result: Option<&'static str>,
    /// The first error's number, for the administration agent's counts.
    code: Option<u16>,
}

#[async_trait]
impl ResultSink for Watch<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }
    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    fn try_raw_row(&mut self, payload: Bytes) -> std::result::Result<Result<()>, Bytes> {
        self.inner.try_raw_row(payload)
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        // The first error of a multi-statement decides.
        self.result.get_or_insert(classify(err));
        self.code
            .get_or_insert(sprig_common::incidents::code_of(err));
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
        self.inner.finished()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod rate_tests {
    use super::*;

    #[test]
    fn a_tenant_gets_series_after_a_busy_minute() {
        sprig_common::telemetry::set_tenant_series_min_rps(10);
        let r = TenantRates::get();
        let t0 = Instant::now();
        let tenant = 9_000_001;
        assert!(!r.busy(tenant, t0));
        for _ in 0..700 {
            r.busy(tenant, t0 + Duration::from_secs(1));
        }
        // The window closes: 701 statements in 60 s is above 10/s.
        assert!(r.busy(tenant, t0 + Duration::from_secs(60)));
        // A quiet minute drops the series again.
        assert!(!r.busy(tenant, t0 + Duration::from_secs(121)));
    }
}
