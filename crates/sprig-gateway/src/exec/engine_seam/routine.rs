//! The runtime of SQL stored programs on engine databases: `CALL` of a
//! procedure, a stored function in an expression, and the bytecode both
//! run (`crate::engine::routine::code`).
//!
//! Each statement of a body runs through the session's own statement path
//! (`tenant_text`), as if the client had sent it, with the variables it
//! reads put in as literals: privileges, row-level security, transactions
//! and warnings work as for any statement. A procedure's result sets go to
//! the client as they come, each flagged "more results"; a final OK ends
//! the call. Handlers, cursors, `SIGNAL` and `GET DIAGNOSTICS` follow both
//! servers' rules.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::column_type as ct;
use sprig_proto_mysql::{Caps, ColumnDef, StatusFlags};

use crate::exec::QueryAttrs;

use crate::engine::assign::{self, Condition, Level};
use crate::engine::routine::code::{HCond, Op, Program, Tgt};
use crate::engine::routine::parse::{self, Kind, Mode, Stmt};
use crate::engine::routine::{Routine, subst};
use crate::engine::row::Datum;
use crate::engine::table::Column;
use crate::engine::{catalog, define, not_on_engine, table};
use crate::exec::PassSession;
use crate::exec::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::session::LocalWarning;

mod event;
mod schedule;
mod trigger;
mod validation;

tokio::task_local! {
    /// The routines running on this task, outermost first (recursion).
    static STACK: RefCell<Vec<String>>;
}

/// How deep procedures may nest (`max_sp_recursion_depth` is about the
/// same routine; this is any).
const MAX_DEPTH: usize = 64;

/// A condition a statement of a body raised.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Raised {
    code: u16,
    state: [u8; 5],
    message: String,
    /// An error (a warning or note otherwise).
    error: bool,
    /// A note (not a warning).
    note: bool,
}

impl Raised {
    fn of(e: &SprigError) -> Self {
        let p = e.protocol();
        Raised {
            code: p.mysql_errno,
            state: p.sqlstate,
            message: e.message.clone(),
            error: true,
            note: false,
        }
    }

    fn warning(w: &LocalWarning) -> Self {
        Raised {
            code: w.code,
            state: warning_state(w.code),
            message: w.message.clone(),
            error: w.level.eq_ignore_ascii_case("error"),
            note: w.level.eq_ignore_ascii_case("note"),
        }
    }

    fn to_error(&self) -> SprigError {
        SprigError::standard(self.code, self.state, self.message.clone())
    }

    fn class(&self) -> &[u8] {
        &self.state[..2]
    }
}

/// The SQLSTATE of a warning a statement raised (the area keeps none).
fn warning_state(code: u16) -> [u8; 5] {
    match code {
        1329 | 1643 => *b"02000",
        1264 | 1690 => *b"22003",
        1292 | 1366 | 1367 => *b"22007",
        1048 | 1062 | 1452 | 1451 => *b"23000",
        1406 => *b"22001",
        1365 => *b"22012",
        1146 => *b"42S02",
        1054 => *b"42S22",
        _ => *b"01000",
    }
}

fn no_data() -> Raised {
    Raised {
        code: 1329,
        state: *b"02000",
        message: "No data - zero rows fetched, selected, or processed".into(),
        error: false,
        note: false,
    }
}

/// How well a handler condition matches: 3 a code, 2 a SQLSTATE, 1 a
/// class; 0 no match.
fn rank(h: &HCond, r: &Raised) -> u8 {
    match h {
        HCond::Code(c) if *c == r.code => 3,
        HCond::State(s) if s.as_bytes() == r.state => 2,
        HCond::Warning if !r.error && (r.class() == b"01" || !r.note) && r.class() != b"02" => 1,
        HCond::Warning if r.class() == b"01" => 1,
        HCond::NotFound if r.class() == b"02" => 1,
        HCond::Exception if r.error && !matches!(r.class(), b"00" | b"01" | b"02") => 1,
        _ => 0,
    }
}

/// What a statement of a body gave back.
#[derive(Debug, Default)]
struct Ran {
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<Option<Bytes>>>,
    affected: u64,
    warnings: Vec<LocalWarning>,
    /// The OK's last insert id.
    last_id: u64,
}

/// The sink a body statement answers into: result sets to the client
/// (`forward`), or kept (`capture`); OKs and errors kept for the runtime.
struct BodySink<'a> {
    inner: Option<&'a mut dyn ResultSink>,
    wire: Wire,
    capture: bool,
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<Option<Bytes>>>,
    error: Option<SprigError>,
    affected: u64,
    last_id: u64,
    finished: bool,
}

impl<'a> BodySink<'a> {
    fn new(inner: Option<&'a mut dyn ResultSink>, wire: Wire, capture: bool) -> Self {
        BodySink {
            inner,
            wire,
            capture,
            columns: Vec::new(),
            rows: Vec::new(),
            error: None,
            affected: 0,
            last_id: 0,
            finished: false,
        }
    }
}

#[async_trait]
impl ResultSink for BodySink<'_> {
    fn wire(&self) -> Wire {
        self.wire
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.finished = false;
        self.columns = columns.to_vec();
        match (&mut self.inner, self.capture) {
            (Some(i), false) => i.begin_rows(columns, format).await,
            _ => Ok(()),
        }
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        if !self.capture {
            if let Some(i) = &mut self.inner {
                return i.row(values).await;
            }
            return Ok(());
        }
        match values {
            RowValues::Text(v) => {
                self.rows
                    .push(v.iter().map(|x| x.map(Bytes::copy_from_slice)).collect());
                Ok(())
            }
            RowValues::Binary(_) => Err(not_on_engine("binary rows inside a stored program")),
        }
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        if self.capture {
            let row = text_row(&payload, self.columns.len())
                .ok_or_else(|| SprigError::internal("an unreadable row inside a stored program"))?;
            self.rows.push(row);
            return Ok(());
        }
        match &mut self.inner {
            Some(i) => i.raw_row(payload).await,
            None => Ok(()),
        }
    }

    async fn end_rows(&mut self, mut end: EndOfRows) -> Result<()> {
        self.finished = true;
        if self.capture {
            return Ok(());
        }
        end.status |= StatusFlags::MORE_RESULTS_EXISTS;
        match &mut self.inner {
            Some(i) => i.end_rows(end).await,
            None => Ok(()),
        }
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.finished = true;
        self.affected = ok.affected_rows;
        self.last_id = ok.last_insert_id;
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.finished = true;
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        Ok(())
    }

    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        self.finished = true;
        Ok(())
    }

    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(not_on_engine("LOAD DATA LOCAL inside a stored program"))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(i) if !self.capture => i.flush().await,
            _ => Ok(()),
        }
    }

    fn finished(&self) -> bool {
        self.finished
    }
}

/// The values of a text protocol row of `n` columns.
fn text_row(p: &Bytes, n: usize) -> Option<Vec<Option<Bytes>>> {
    let mut out = Vec::with_capacity(n);
    let mut i = 0usize;
    for _ in 0..n {
        let first = *p.get(i)?;
        let (len, at) = match first {
            0xfb => {
                out.push(None);
                i += 1;
                continue;
            }
            0xfc => (
                usize::from(u16::from_le_bytes([*p.get(i + 1)?, *p.get(i + 2)?])),
                i + 3,
            ),
            0xfd => (
                usize::from(*p.get(i + 1)?)
                    | usize::from(*p.get(i + 2)?) << 8
                    | usize::from(*p.get(i + 3)?) << 16,
                i + 4,
            ),
            0xfe => {
                let b: [u8; 8] = p.get(i + 1..i + 9)?.try_into().ok()?;
                (usize::try_from(u64::from_le_bytes(b)).ok()?, i + 9)
            }
            x => (usize::from(x), i + 1),
        };
        out.push(Some(
            p.slice(at..at.checked_add(len).filter(|e| *e <= p.len())?),
        ));
        i = at + len;
    }
    Some(out)
}

/// A value from a text result, typed by its column.
fn wire_datum(v: Option<&[u8]>, c: &ColumnDef) -> Datum {
    let Some(v) = v else { return Datum::Null };
    let s = std::str::from_utf8(v).unwrap_or("");
    let unsigned = c.flags & 0x20 != 0;
    match c.col_type {
        ct::TINY | ct::SHORT | ct::LONG | ct::LONGLONG | ct::INT24 => {
            if unsigned {
                s.parse().map_or_else(|_| Datum::bytes(v), Datum::UInt)
            } else {
                s.parse().map_or_else(|_| Datum::bytes(v), Datum::Int)
            }
        }
        ct::NEWDECIMAL | ct::DECIMAL => {
            sprig_engine::row::Decimal::parse(s).map_or_else(|| Datum::bytes(v), Datum::Decimal)
        }
        ct::FLOAT | ct::DOUBLE => s.parse().map_or_else(|_| Datum::bytes(v), Datum::Double),
        ct::YEAR => s.parse().map_or_else(|_| Datum::bytes(v), Datum::Year),
        ct::NULL => Datum::Null,
        _ if c.charset == 63
            && !matches!(
                c.col_type,
                ct::DATE | ct::DATETIME | ct::TIMESTAMP | ct::TIME
            ) =>
        {
            Datum::Bytes(Bytes::copy_from_slice(v))
        }
        _ => Datum::Str(Bytes::copy_from_slice(v)),
    }
}

/// `s` as a SQL string literal that reads the same under any
/// `sql_mode`.
fn str_lit(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) if !s.contains('\\') && !s.contains('\0') => {
            // Plain: the engine's `INSERT` reads no introducer inside a
            // function's arguments.
            format!("'{}'", s.replace('\'', "''"))
        }
        _ => format!("CONVERT(X'{}' USING utf8mb4)", hex(b)),
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

/// A value as a literal a statement reads back as the same value.
fn literal(d: &Datum, col: Option<&Column>) -> String {
    match d {
        Datum::Null => "NULL".into(),
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) | Datum::Bit(v) => v.to_string(),
        Datum::Decimal(v) => v.to_string(),
        Datum::Float(v) => approx(&crate::engine::expr::float_text(*v)),
        Datum::Double(v) => approx(&crate::engine::expr::double_text(*v)),
        Datum::Date(v) => format!("DATE'{v}'"),
        Datum::DateTime(v) => format!("TIMESTAMP'{v}'"),
        Datum::Time(v) => format!("TIME'{v}'"),
        Datum::Timestamp(t) => format!(
            "TIMESTAMP'{}'",
            crate::engine::temporal::from_timestamp(*t, 0)
        ),
        Datum::Year(v) => v.to_string(),
        Datum::Str(b) | Datum::Json(b) => str_lit(b),
        Datum::Bytes(b) => format!("X'{}'", hex(b)),
        Datum::Enum(i) => match col.and_then(|c| c.members.get(usize::from(*i).wrapping_sub(1))) {
            Some(m) => str_lit(m),
            None => "''".into(),
        },
        Datum::Set(mask) => {
            let parts: Vec<&[u8]> = col
                .map(|c| {
                    c.members
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| mask & (1u64 << i) != 0)
                        .map(|(_, m)| m.as_ref())
                        .collect()
                })
                .unwrap_or_default();
            str_lit(&parts.join(&b","[..]))
        }
    }
}

fn approx(t: &str) -> String {
    if t.contains(['e', 'E']) || t.contains("inf") || t.contains("nan") {
        t.to_owned()
    } else {
        format!("{t}e0")
    }
}

/// A variable's column, from its declared type (`None` when the engine
/// can't read the type: the value is kept as computed).
fn column_of(ty: &str, name: &str, mysql: bool, db_cs: &(String, String)) -> Option<Column> {
    param_table(ty, name, mysql, db_cs)?
        .columns
        .first()
        .cloned()
}

/// A one-column table of a variable or parameter `name` of type `ty`.
fn param_table(
    ty: &str,
    name: &str,
    mysql: bool,
    db_cs: &(String, String),
) -> Option<Arc<table::TableDef>> {
    if ty.is_empty() {
        return None;
    }
    let cx = define::Ctx {
        mysql,
        db_cs: db_cs.clone(),
    };
    let sql = format!("CREATE TABLE `r` ({} {ty})", subst::quote(name));
    let define::Created::Def(def) = define::create(&sql, &cx).ok()? else {
        return None;
    };
    let text = define::render(&def);
    table::parse(ObjectId(0), 0, DbId(0), &text).ok()
}

/// The function a prepared `CALL` wraps around each `?` in an `OUT` or
/// `INOUT` position (`engine_call_mark_outs`).
const PS_OUT: &str = "_sprig_ps_out";

/// One running routine.
struct Frame {
    prog: Arc<Program>,
    vals: Vec<Datum>,
    cols: Vec<Option<Column>>,
    cursors: Vec<Option<VecDeque<Vec<Datum>>>>,
    function: bool,
    /// A trigger's body (it ends without `RETURN`).
    trigger: bool,
    db: String,
    name: String,
    /// Rows the statements changed or selected into variables, summed
    /// (MariaDB's count for the `CALL`), and the last statement's (MySQL's).
    affected: u64,
    last_affected: u64,
    /// `SQL SECURITY DEFINER`: the account whose privileges the body runs
    /// with.
    definer: Option<(String, String)>,
}

/// A handler that is running.
struct Active {
    handler: usize,
    resume: usize,
    hstack_len: usize,
    skip: (usize, usize),
    conds: Vec<Raised>,
}

/// How a body ended.
enum Flow {
    Done,
    Returned(Datum),
}

impl PassSession {
    /// The session context a routine statement records (`None` for other
    /// DDL).
    pub(super) fn routine_ddl_session(
        &mut self,
        cx: &ExecCtx<'_>,
        target: &sprig_sql::names::DdlTarget,
    ) -> Option<crate::engine::routine::ddl::Session> {
        use sprig_sql::names::{DdlKind, DdlTarget};
        let routine = matches!(
            target,
            DdlTarget::CreateObject {
                kind: DdlKind::Procedure | DdlKind::Function | DdlKind::Trigger | DdlKind::Event,
                ..
            } | DdlTarget::AlterObject {
                kind: DdlKind::Procedure | DdlKind::Function | DdlKind::Event,
                ..
            }
        );
        if !routine {
            return None;
        }
        let entry = self
            .eng
            .entry
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned());
        let bs = self.bs(cx);
        let sv = bs.vars();
        let get = |k: &str| sv.vars.get(k).cloned().unwrap_or_default();
        let (secs, _) = crate::engine::temporal::now();
        let now = crate::engine::temporal::from_timestamp(
            sprig_engine::row::Timestamp {
                seconds: u32::try_from(secs).unwrap_or(0),
                micros: 0,
            },
            0,
        );
        Some(crate::engine::routine::ddl::Session {
            entry,
            sql_mode: get("sql_mode"),
            charset_client: get("character_set_client"),
            collation_connection: get("collation_connection"),
            now: now.to_string(),
            event: self.eng.event_eval.take(),
        })
    }

    /// The routine `name` of kind `kind` in database `db` (the current one
    /// when `None`), when `db` is an engine database that has it.
    fn sp_find(&self, db: Option<&str>, kind: Kind, name: &str) -> Option<(Arc<Routine>, String)> {
        let db = match db {
            Some(d) => d.to_owned(),
            None => self.current_logical()?,
        };
        let ts = self.tenant.as_deref()?;
        let lctn = ts.lower_case_table_names();
        let cat = self.engine_cat()?;
        let row = cat.database(&database_key(&db, lctn))?;
        let r = catalog::of(&cat)?.routine(row.db_id, kind, name)?;
        Some((Arc::clone(r), row.name.clone()))
    }

    /// Whether `text`, a statement being prepared (`?` for parameters), is
    /// a `CALL` of a procedure of an engine database. Such a statement is
    /// kept as text and run at each execute with the bound values written
    /// in; its result sets go to the client as binary rows.
    pub(in crate::exec) fn engine_call_prepares(&self, text: &[u8]) -> bool {
        let Ok(t) = std::str::from_utf8(text) else {
            return false;
        };
        if !t
            .trim_start()
            .get(..4)
            .is_some_and(|w| w.eq_ignore_ascii_case("CALL"))
        {
            return false;
        }
        let nulls = vec!["NULL".to_owned(); sprig_search::sqlscan::placeholders(t).len()];
        let Some(t) = sprig_search::sqlscan::interpolate(t, &nulls) else {
            return false;
        };
        let Ok(mut p) = parse::P::new(&t) else {
            return false;
        };
        let Ok(Stmt::Call { db, name, .. }) = p.statement(true) else {
            return false;
        };
        self.sp_find(db.as_deref(), Kind::Procedure, &name)
            .is_some()
    }

    /// A prepared `CALL`'s text with each `?` in an `OUT` or `INOUT`
    /// position wrapped in [`PS_OUT`]: at each execute the parameter starts
    /// from the bound value and its end value goes back to the client in
    /// the output-parameters result set, as both servers send it.
    pub(in crate::exec) fn engine_call_mark_outs(&self, text: &Bytes) -> Bytes {
        let Ok(sql) = std::str::from_utf8(text) else {
            return text.clone();
        };
        let n = sprig_search::sqlscan::placeholders(sql).len();
        if n == 0 {
            return text.clone();
        }
        let marks: Vec<String> = (0..n).map(|k| format!("@_sprig_q{k}")).collect();
        let Some(marked) = sprig_search::sqlscan::interpolate(sql, &marks) else {
            return text.clone();
        };
        let Ok(mut parser) = parse::P::new(&marked) else {
            return text.clone();
        };
        let Ok(Stmt::Call { db, name, args }) = parser.statement(true) else {
            return text.clone();
        };
        let Some((r, _)) = self.sp_find(db.as_deref(), Kind::Procedure, &name) else {
            return text.clone();
        };
        let Ok((_, prog)) = &r.def else {
            return text.clone();
        };
        let mut wrap = vec![false; n];
        for (i, a) in args.iter().enumerate() {
            if prog.modes.get(i).is_some_and(|m| *m != Mode::In)
                && let Some(k) = a
                    .trim()
                    .strip_prefix("@_sprig_q")
                    .and_then(|k| k.parse::<usize>().ok())
                && k < n
            {
                wrap[k] = true;
            }
        }
        if !wrap.contains(&true) {
            return text.clone();
        }
        let vals: Vec<String> = wrap
            .iter()
            .map(|w| {
                if *w {
                    format!("{PS_OUT}(?)")
                } else {
                    "?".to_owned()
                }
            })
            .collect();
        sprig_search::sqlscan::interpolate(sql, &vals).map_or_else(|| text.clone(), Bytes::from)
    }

    /// A client's `CALL` of a procedure of an engine database (`None`: the
    /// text names no such procedure, the logic layer's `CALL` goes on).
    pub(in crate::exec) async fn engine_sql_call(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Option<Result<Step>> {
        let text = std::str::from_utf8(original).ok()?;
        let mut p = parse::P::new(text).ok()?;
        let Ok(Stmt::Call { db, name, args }) = p.statement(true) else {
            return None;
        };
        let (r, db_name) = self.sp_find(db.as_deref(), Kind::Procedure, &name)?;
        if format == RowFormat::Binary {
            let e = not_on_engine("a prepared CALL of a stored procedure");
            return Some(self.fail(out, &e).await);
        }
        let fut = self.sp_call_top(cx, r, db_name, args, more, out);
        Some(Box::pin(with_stack(fut)).await)
    }

    /// Runs a client's `CALL`.
    async fn sp_call_top(
        &mut self,
        cx: &mut ExecCtx<'_>,
        r: Arc<Routine>,
        db_name: String,
        args: Vec<String>,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let prog = match &r.def {
            Ok((_, p)) => Arc::clone(p),
            Err(why) => return self.fail(out, &not_on_engine(why)).await,
        };
        if prog.results && !cx.proto.has(Caps::MULTI_RESULTS) {
            let e = SprigError::standard(
                1312,
                *b"0A000",
                format!(
                    "PROCEDURE {db_name}.{} can't return a result set in the given context",
                    r.name
                ),
            );
            return self.fail(out, &e).await;
        }
        // Arguments: values in the caller's context, `OUT` targets; a
        // prepared `CALL`'s `?` in an `OUT`/`INOUT` position goes back in
        // the output-parameters result set.
        let mut args = args;
        let mut back: Vec<usize> = Vec::new();
        let mut outs = Vec::with_capacity(args.len());
        for (i, a) in args.iter_mut().enumerate() {
            if let Some(inner) = a
                .trim()
                .strip_prefix(PS_OUT)
                .map(str::trim_start)
                .and_then(|x| x.strip_prefix('('))
                .and_then(|x| x.strip_suffix(')'))
            {
                *a = inner.to_owned();
                back.push(i);
                outs.push(Some(Tgt::Slot(usize::MAX)));
                continue;
            }
            let a = a.trim();
            outs.push(
                a.strip_prefix('@')
                    .filter(|u| !u.starts_with('@'))
                    .map(|u| Tgt::User(u.trim_matches(['`', '\'', '"']).to_owned())),
            );
        }
        let vals = match self.sp_eval_list(cx, &args).await {
            Ok(v) => v,
            Err(e) => return self.fail(out, &e).await,
        };
        let mut frame = match self.sp_frame(cx, &r, &prog, &db_name, &vals, &outs) {
            Ok(f) => f,
            Err(e) => return self.fail(out, &e).await,
        };
        let r2 = self.sp_run(cx, &mut frame, Some(&mut *out)).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match r2 {
            Ok(_) => {
                if let Err(e) = self.sp_copy_out(cx, &frame, &outs, None).await {
                    return self.fail(out, &e).await;
                }
                if !back.is_empty()
                    && let Err(e) = self
                        .sp_out_params(cx, &r, &db_name, &frame, &back, out)
                        .await
                {
                    return self.fail(out, &e).await;
                }
                let mut ok = OkPacket::new(status);
                ok.affected_rows = if self.persona == sprig_proto_mysql::Persona::MySql84 {
                    frame.last_affected
                } else {
                    frame.affected
                };
                out.ok(ok).await?;
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => self.fail(out, &e).await,
        }
    }

    /// The output-parameters result set of a prepared `CALL`: one row of
    /// the end values of parameters `back`, flagged as output parameters.
    #[allow(clippy::too_many_arguments)]
    async fn sp_out_params(
        &mut self,
        cx: &mut ExecCtx<'_>,
        r: &Routine,
        db_name: &str,
        frame: &Frame,
        back: &[usize],
        out: &mut dyn ResultSink,
    ) -> Result<()> {
        let results = self.engine_vars(cx).results;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let db_cs = self
            .engine_cat()
            .and_then(|c| {
                c.databases
                    .get(r.db)
                    .map(|d| (d.default_charset.clone(), d.default_collation.clone()))
            })
            .unwrap_or_default();
        let dbb = Bytes::from(db_name.to_owned());
        let tb = Bytes::from(r.name.clone());
        let mut defs = Vec::with_capacity(back.len());
        let mut texts: Vec<Option<Bytes>> = Vec::with_capacity(back.len());
        for &i in back {
            let v = &frame.prog.vars[i];
            let def = param_table(&v.ty, &v.name, mysql, &db_cs)
                .ok_or_else(|| not_on_engine("this parameter type as an output parameter"))?;
            let nb = Bytes::from(v.name.clone());
            let mut d = crate::engine::meta::column_def(&dbb, &tb, &def, &tb, 0, &nb, &nb, results);
            // The servers report output parameters with no column flag but
            // BINARY; MySQL with the binary character set.
            d.flags &= sprig_proto_mysql::resultset::ColumnFlags::BINARY.bits();
            if mysql {
                d.charset = 63;
            }
            defs.push(d);
            texts.push(crate::engine::meta::text(&def.columns[0], &frame.vals[i]));
        }
        out.begin_rows(&defs, RowFormat::Text).await?;
        let row: Vec<Option<&[u8]>> = texts.iter().map(|t| t.as_deref()).collect();
        out.row(RowValues::Text(&row)).await?;
        let status = self.status() | StatusFlags::MORE_RESULTS_EXISTS | StatusFlags::PS_OUT_PARAMS;
        out.end_rows(crate::pipeline::EndOfRows {
            status,
            warnings: 0,
        })
        .await
    }

    /// The frame of a call with argument values `vals` (`OUT` parameters
    /// start as NULL).
    fn sp_frame(
        &mut self,
        cx: &ExecCtx<'_>,
        r: &Routine,
        prog: &Arc<Program>,
        db_name: &str,
        vals: &[Datum],
        outs: &[Option<Tgt>],
    ) -> Result<Frame> {
        let nparams = prog.modes.len();
        if vals.len() != nparams {
            return Err(SprigError::standard(
                1318,
                *b"42000",
                format!(
                    "Incorrect number of arguments for {} {db_name}.{}; expected {nparams}, got {}",
                    r.kind.word(),
                    r.name,
                    vals.len()
                ),
            ));
        }
        for (i, m) in prog.modes.iter().enumerate() {
            if *m != Mode::In && outs.get(i).is_none_or(Option::is_none) {
                return Err(SprigError::standard(
                    1414,
                    *b"42000",
                    format!(
                        "OUT or INOUT argument {} for routine {db_name}.{} is not a variable or NEW pseudo-variable in BEFORE trigger",
                        i + 1,
                        r.name
                    ),
                ));
            }
        }
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let db_cs = self
            .engine_cat()
            .and_then(|c| {
                c.databases
                    .get(r.db)
                    .map(|d| (d.default_charset.clone(), d.default_collation.clone()))
            })
            .unwrap_or_default();
        let cols: Vec<Option<Column>> = prog
            .vars
            .iter()
            .map(|v| column_of(&v.ty, &v.name, mysql, &db_cs))
            .collect();
        let env = self.engine_vars(cx).env;
        let mut frame = Frame {
            prog: Arc::clone(prog),
            vals: vec![Datum::Null; prog.vars.len()],
            cols,
            cursors: vec![None; prog.cursors.len()],
            function: r.kind == Kind::Function,
            trigger: false,
            db: db_name.to_owned(),
            name: r.name.clone(),
            affected: 0,
            last_affected: 0,
            definer: match &r.def {
                Ok((c, _)) if c.chars.invoker == Some(true) => None,
                _ => Some(r.cx.definer.clone()),
            },
        };
        let mut conds = Vec::new();
        for (i, m) in prog.modes.iter().enumerate() {
            if *m != Mode::Out {
                let v = store(frame.cols[i].as_ref(), vals[i].clone(), env, &mut conds)?;
                frame.vals[i] = v;
            }
        }
        Ok(frame)
    }

    /// `OUT` and `INOUT` values back to the caller's variables.
    async fn sp_copy_out(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &Frame,
        outs: &[Option<Tgt>],
        mut caller: Option<&mut Frame>,
    ) -> Result<()> {
        let mut sets = Vec::new();
        for (i, m) in frame.prog.modes.iter().enumerate() {
            if *m == Mode::In {
                continue;
            }
            match outs.get(i).cloned().flatten() {
                Some(Tgt::User(u)) => sets.push(format!(
                    "@{} = {}",
                    subst::quote(&u),
                    literal(&frame.vals[i], frame.cols[i].as_ref())
                )),
                Some(Tgt::Slot(s)) => {
                    if let Some(c) = caller.as_deref_mut() {
                        let env = self.engine_vars(cx).env;
                        let mut conds = Vec::new();
                        c.vals[s] =
                            store(c.cols[s].as_ref(), frame.vals[i].clone(), env, &mut conds)?;
                    }
                }
                None => {}
            }
        }
        if !sets.is_empty() {
            let sql = format!("SET {}", sets.join(", "));
            self.sp_statement(cx, &sql, None, true)
                .await
                .map_err(|r| r.to_error())?;
        }
        Ok(())
    }

    /// Values of expressions in the caller's context (`SELECT a, b, ...`).
    async fn sp_eval_list(&mut self, cx: &mut ExecCtx<'_>, exprs: &[String]) -> Result<Vec<Datum>> {
        if exprs.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT {}", exprs.join(", "));
        let ran = self
            .sp_statement(cx, &sql, None, true)
            .await
            .map_err(|r| r.to_error())?;
        let row = ran.rows.into_iter().next().unwrap_or_default();
        Ok(ran
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| wire_datum(row.get(i).and_then(|v| v.as_deref()), c))
            .collect())
    }

    /// Runs one statement of a body through the session's statement path.
    /// `out`: where its result set goes (`capture`: kept instead).
    async fn sp_statement(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
        out: Option<&mut dyn ResultSink>,
        capture: bool,
    ) -> std::result::Result<Ran, Raised> {
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        // The statement's own conditions: the area starts empty.
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.warnings = Some(Vec::new());
        }
        let mut sink = BodySink::new(out, wire, capture);
        let text = Bytes::from(sql.to_owned());
        let attrs = QueryAttrs::default();
        let r =
            Box::pin(self.tenant_text(cx, &text, &attrs, true, RowFormat::Text, &mut sink)).await;
        let warnings = self
            .tenant
            .as_deref()
            .and_then(|ts| ts.warnings.clone())
            .unwrap_or_default();
        // MySQL names the table in a duplicate key's message.
        let named = |e: SprigError| -> SprigError {
            if self.persona != sprig_proto_mysql::Persona::MySql84 {
                return e;
            }
            let lex = self.lex_opts().0;
            match sprig_sql::compat::errmap::dup_key_table(sql.as_bytes(), &lex) {
                Some(t) => sprig_sql::compat::errmap::with_dup_key_table(&e, &t).unwrap_or(e),
                None => e,
            }
        };
        if let Err(e) = r {
            return Err(Raised::of(&named(e)));
        }
        if let Some(e) = sink.error {
            return Err(Raised::of(&named(e)));
        }
        Ok(Ran {
            columns: sink.columns,
            rows: sink.rows,
            affected: sink.affected,
            last_id: sink.last_id,
            warnings,
        })
    }

    /// Runs `sql` as a statement of the client's, its answer kept: the
    /// affected rows, the conditions and the result's columns, or the
    /// error (`LOAD DATA`'s batches).
    pub(super) async fn run_kept(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &str,
    ) -> Result<(u64, Vec<LocalWarning>, Vec<ColumnDef>)> {
        match Box::pin(self.sp_statement(cx, sql, None, true)).await {
            Ok(r) => Ok((r.affected, r.warnings, r.columns)),
            Err(e) => Err(e.to_error()),
        }
    }

    /// The value of an expression of a body.
    async fn sp_value(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &Frame,
        t: &subst::Text,
    ) -> std::result::Result<(Datum, Vec<Raised>), Raised> {
        let sql = format!("SELECT {}", render(t, frame));
        let ran = self.sp_statement(cx, &sql, None, true).await?;
        let row = ran.rows.into_iter().next().unwrap_or_default();
        let v = match ran.columns.first() {
            Some(c) => wire_datum(row.first().and_then(|v| v.as_deref()), c),
            None => Datum::Null,
        };
        Ok((v, ran.warnings.iter().map(Raised::warning).collect()))
    }

    /// Stores `v` in a target.
    fn sp_assign(
        &mut self,
        cx: &ExecCtx<'_>,
        frame: &mut Frame,
        t: &Tgt,
        v: Datum,
        user_sets: &mut Vec<String>,
    ) -> std::result::Result<Vec<Raised>, Raised> {
        match t {
            Tgt::Slot(s) => {
                let env = self.engine_vars(cx).env;
                let mut conds = Vec::new();
                let v = store(frame.cols[*s].as_ref(), v, env, &mut conds)
                    .map_err(|e| Raised::of(&e))?;
                frame.vals[*s] = v;
                Ok(conds.iter().map(raised_of_condition).collect())
            }
            Tgt::User(u) => {
                user_sets.push(format!("@{} = {}", subst::quote(u), literal(&v, None)));
                Ok(Vec::new())
            }
        }
    }

    async fn sp_user_sets(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sets: Vec<String>,
    ) -> std::result::Result<(), Raised> {
        if sets.is_empty() {
            return Ok(());
        }
        let sql = format!("SET {}", sets.join(", "));
        self.sp_statement(cx, &sql, None, true).await.map(|_| ())
    }

    /// Runs a frame's code. `out`: the client (a procedure's result sets).
    #[allow(clippy::too_many_lines)] // one arm per instruction
    async fn sp_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &mut Frame,
        mut out: Option<&mut dyn ResultSink>,
    ) -> Result<Flow> {
        let key = format!("{}.{}", frame.db.to_lowercase(), frame.name.to_lowercase());
        let depth = STACK.try_with(|s| s.borrow().len()).unwrap_or(0);
        let recursive = STACK
            .try_with(|s| s.borrow().contains(&key))
            .unwrap_or(false);
        if recursive {
            return Err(if frame.function {
                SprigError::standard(
                    1424,
                    *b"HY000",
                    "Recursive stored functions and triggers are not allowed",
                )
            } else {
                SprigError::standard(
                    1456,
                    *b"HY000",
                    format!(
                        "Recursive limit 0 (as set by the max_sp_recursion_depth variable) was exceeded for routine {}",
                        frame.name
                    ),
                )
            });
        }
        if depth >= MAX_DEPTH {
            return Err(SprigError::standard(
                1436,
                *b"HY000",
                "Thread stack overrun: stored programs nest too deeply",
            ));
        }
        // `SQL SECURITY DEFINER`: the definer's privileges while it runs.
        let saved_who = match self.sp_become(frame.definer.as_ref()) {
            Ok(w) => w,
            Err(e) => return Err(e),
        };
        let _ = STACK.try_with(|s| s.borrow_mut().push(key.clone()));
        // The routine's database is the current one while it runs.
        let saved_db = self.tenant.as_deref().and_then(|ts| ts.current.clone());
        self.sp_use(Some(frame.db.clone()));
        let r = Box::pin(self.sp_loop(cx, frame, &mut out)).await;
        self.sp_use(saved_db);
        if let (Some(w), Some(ts)) = (saved_who, self.tenant.as_deref_mut()) {
            ts.principal.user_id = w.0;
            ts.principal.user = w.1;
            ts.principal.host = w.2;
            ts.authz = w.3;
        }
        let _ = STACK.try_with(|s| s.borrow_mut().pop());
        r
    }

    /// Switches the session to `definer`'s account (its privileges), when
    /// it is another account than the session's: what to restore.
    #[allow(clippy::type_complexity)]
    fn sp_become(
        &mut self,
        definer: Option<&(String, String)>,
    ) -> Result<
        Option<(
            Option<sprig_common::ids::UserId>,
            String,
            String,
            crate::tenant::authz::SessionAuthz,
        )>,
    > {
        let Some((u, h)) = definer else {
            return Ok(None);
        };
        let Some(cat) = self.engine_cat() else {
            return Ok(None);
        };
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        if ts.principal.user == *u && ts.principal.host.eq_ignore_ascii_case(h) {
            return Ok(None);
        }
        let uid = sprig_auth::TenantAuth::of(&cat).and_then(|auth| {
            auth.accounts(u)
                .iter()
                .find(|a| a.row.host.eq_ignore_ascii_case(h))
                .map(|a| a.row.user_id)
        });
        let Some(uid) = uid else {
            // The tenant's administrator (no user row) defined it: it runs
            // as the session does.
            if ts.principal.user_id.is_none() || *u == ts.principal.user {
                return Ok(None);
            }
            return Err(SprigError::standard(
                1449,
                *b"HY000",
                format!("The user specified as a definer ('{u}'@'{h}') does not exist"),
            ));
        };
        let saved = (
            ts.principal.user_id.replace(uid),
            std::mem::replace(&mut ts.principal.user, u.clone()),
            std::mem::replace(&mut ts.principal.host, h.clone()),
            std::mem::take(&mut ts.authz),
        );
        Ok(Some(saved))
    }

    fn sp_use(&mut self, db: Option<String>) {
        if let Some(bs) = self.bs.as_mut() {
            bs.vars_mut().schema = None;
        }
        if let Some(ts) = self.tenant.as_deref_mut()
            && ts.current != db
        {
            ts.current = db;
            self.eng.stale();
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per instruction
    async fn sp_loop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &mut Frame,
        out: &mut Option<&mut dyn ResultSink>,
    ) -> Result<Flow> {
        let prog = Arc::clone(&frame.prog);
        let mut pc = 0usize;
        let mut hstack: Vec<usize> = Vec::new();
        let mut active: Vec<Active> = Vec::new();
        // The diagnostics area of the last statement: its conditions and
        // row count.
        let mut area: Vec<Raised> = Vec::new();
        let mut row_count: i64 = 0;
        loop {
            let Some(op) = prog.ops.get(pc) else {
                return Ok(Flow::Done);
            };
            let mut next = pc + 1;
            let mut cont = pc + 1;
            // What the instruction raised: an error, or warnings.
            let res: std::result::Result<Vec<Raised>, Raised> = match op {
                Op::End => {
                    if frame.function && !frame.trigger {
                        return Err(SprigError::standard(
                            1321,
                            *b"2F005",
                            format!("FUNCTION {} ended without RETURN", frame.name),
                        ));
                    }
                    return Ok(Flow::Done);
                }
                Op::Set { slot, expr } => match expr {
                    None => {
                        frame.vals[*slot] = Datum::Null;
                        Ok(Vec::new())
                    }
                    Some(t) => match self.sp_value(cx, frame, t).await {
                        Ok((v, mut w)) => {
                            let mut sets = Vec::new();
                            match self.sp_assign(cx, frame, &Tgt::Slot(*slot), v, &mut sets) {
                                Ok(mut w2) => {
                                    w.append(&mut w2);
                                    Ok(w)
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    },
                },
                Op::Sql { text, results } => {
                    let sql = render(text, frame);
                    let sink = if *results { reborrow(out) } else { None };
                    let capture = !*results;
                    match self.sp_statement(cx, &sql, sink, capture).await {
                        Ok(ran) => {
                            row_count = if ran.columns.is_empty() {
                                i64::try_from(ran.affected).unwrap_or(i64::MAX)
                            } else {
                                -1
                            };
                            if ran.columns.is_empty() {
                                frame.affected += ran.affected;
                                frame.last_affected = ran.affected;
                            } else {
                                frame.last_affected = 0;
                            }
                            Ok(ran.warnings.iter().map(Raised::warning).collect())
                        }
                        Err(e) => Err(e),
                    }
                }
                Op::SelectInto { text, into } => {
                    let sql = render(text, frame);
                    match self.sp_statement(cx, &sql, None, true).await {
                        Err(e) => Err(e),
                        Ok(ran) => {
                            let mut w: Vec<Raised> =
                                ran.warnings.iter().map(Raised::warning).collect();
                            if ran.columns.len() != into.len() {
                                Err(Raised::of(&SprigError::standard(
                                    1222,
                                    *b"21000",
                                    "The used SELECT statements have a different number of columns",
                                )))
                            } else if ran.rows.is_empty() {
                                w.push(no_data());
                                Ok(w)
                            } else {
                                let mut sets = Vec::new();
                                let mut failed = None;
                                for (i, t) in into.iter().enumerate() {
                                    let v = wire_datum(
                                        ran.rows[0].get(i).and_then(|v| v.as_deref()),
                                        &ran.columns[i],
                                    );
                                    match self.sp_assign(cx, frame, t, v, &mut sets) {
                                        Ok(mut x) => w.append(&mut x),
                                        Err(e) => {
                                            failed = Some(e);
                                            break;
                                        }
                                    }
                                }
                                if failed.is_none()
                                    && let Err(e) = self.sp_user_sets(cx, sets).await
                                {
                                    failed = Some(e);
                                }
                                match failed {
                                    Some(e) => Err(e),
                                    None if ran.rows.len() > 1 => {
                                        Err(Raised::of(&SprigError::standard(
                                            1172,
                                            *b"42000",
                                            "Result consisted of more than one row",
                                        )))
                                    }
                                    None => {
                                        row_count = 1;
                                        frame.affected += 1;
                                        frame.last_affected = 1;
                                        Ok(w)
                                    }
                                }
                            }
                        }
                    }
                }
                Op::Call {
                    db,
                    name,
                    args,
                    outs,
                } => {
                    let fut = self.sp_call_nested(cx, frame, db.as_deref(), name, args, outs, out);
                    Box::pin(fut).await.map_err(|e| Raised::of(&e))
                }
                Op::Jump(to) => {
                    next = *to;
                    Ok(Vec::new())
                }
                Op::Unless { cond, to, cont: c } => {
                    cont = *c;
                    match self.sp_value(cx, frame, cond).await {
                        Ok((v, w)) => {
                            if crate::engine::row::truth(&v) != Some(true) {
                                next = *to;
                            }
                            Ok(w)
                        }
                        Err(e) => Err(e),
                    }
                }
                Op::NoCase { cont: c } => {
                    cont = *c;
                    Err(Raised::of(&SprigError::standard(
                        1339,
                        *b"20000",
                        "Case not found for CASE statement",
                    )))
                }
                Op::Return(t) => match self.sp_value(cx, frame, t).await {
                    Ok((v, _)) => return Ok(Flow::Returned(v)),
                    Err(e) => Err(e),
                },
                Op::HPush(h) => {
                    hstack.push(*h);
                    Ok(Vec::new())
                }
                Op::HPop(n) => {
                    let keep = hstack.len().saturating_sub(*n);
                    hstack.truncate(keep);
                    Ok(Vec::new())
                }
                Op::Reset(cs) => {
                    for c in cs {
                        frame.cursors[*c] = None;
                    }
                    Ok(Vec::new())
                }
                Op::Open(c) => {
                    if frame.cursors[*c].is_some() {
                        Err(Raised::of(&SprigError::standard(
                            1325,
                            *b"24000",
                            "Cursor is already open",
                        )))
                    } else {
                        let sql = render(&prog.cursors[*c].1, frame);
                        match self.sp_statement(cx, &sql, None, true).await {
                            Ok(ran) => {
                                let rows: VecDeque<Vec<Datum>> = ran
                                    .rows
                                    .iter()
                                    .map(|r| {
                                        ran.columns
                                            .iter()
                                            .enumerate()
                                            .map(|(i, col)| {
                                                wire_datum(r.get(i).and_then(|v| v.as_deref()), col)
                                            })
                                            .collect()
                                    })
                                    .collect();
                                frame.cursors[*c] = Some(rows);
                                Ok(ran.warnings.iter().map(Raised::warning).collect())
                            }
                            Err(e) => Err(e),
                        }
                    }
                }
                Op::Fetch { cursor, into } => match frame.cursors[*cursor].as_mut() {
                    None => Err(Raised::of(&SprigError::standard(
                        1326,
                        *b"24000",
                        "Cursor is not open",
                    ))),
                    Some(rows) => match rows.pop_front() {
                        None => Err(Raised {
                            error: true,
                            ..no_data()
                        }),
                        Some(row) if row.len() != into.len() => {
                            Err(Raised::of(&SprigError::standard(
                                1328,
                                *b"HY000",
                                "Incorrect number of FETCH variables",
                            )))
                        }
                        Some(row) => {
                            let mut sets = Vec::new();
                            let mut w = Vec::new();
                            let mut failed = None;
                            for (t, v) in into.iter().zip(row) {
                                match self.sp_assign(cx, frame, t, v, &mut sets) {
                                    Ok(mut x) => w.append(&mut x),
                                    Err(e) => {
                                        failed = Some(e);
                                        break;
                                    }
                                }
                            }
                            if failed.is_none()
                                && let Err(e) = self.sp_user_sets(cx, sets).await
                            {
                                failed = Some(e);
                            }
                            match failed {
                                Some(e) => Err(e),
                                None => Ok(w),
                            }
                        }
                    },
                },
                Op::Close(c) => {
                    if frame.cursors[*c].take().is_none() {
                        Err(Raised::of(&SprigError::standard(
                            1326,
                            *b"24000",
                            "Cursor is not open",
                        )))
                    } else {
                        Ok(Vec::new())
                    }
                }
                Op::Signal {
                    resignal,
                    cond,
                    items,
                } => {
                    let handled = active.last().and_then(|a| a.conds.last().cloned());
                    if *resignal && handled.is_none() {
                        Err(Raised::of(&SprigError::standard(
                            1645,
                            *b"0K000",
                            "RESIGNAL when handler not active",
                        )))
                    } else {
                        match self
                            .sp_signal(cx, frame, *resignal, cond.as_ref(), items, handled)
                            .await
                        {
                            Ok(r) if r.error => Err(r),
                            Ok(r) => Ok(vec![r]),
                            Err(e) => Err(e),
                        }
                    }
                }
                Op::Diag {
                    stacked,
                    condition,
                    items,
                } => {
                    let src: Vec<Raised> = if *stacked {
                        match active.last() {
                            Some(a) => a.conds.clone(),
                            None => {
                                return Err(SprigError::standard(
                                    1647,
                                    *b"0Z002",
                                    "GET STACKED DIAGNOSTICS when handler not active",
                                ));
                            }
                        }
                    } else {
                        area.clone()
                    };
                    self.sp_diag(cx, frame, &src, row_count, condition.as_ref(), items)
                        .await
                }
                Op::HEnd(h) => {
                    let Some(a) = active.pop() else {
                        return Err(SprigError::internal("handler end without a handler"));
                    };
                    debug_assert_eq!(a.handler, *h);
                    hstack.truncate(a.hstack_len);
                    // The handled conditions leave the area.
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        ts.warnings = Some(Vec::new());
                    }
                    let hd = &prog.handlers[*h];
                    if hd.exit {
                        hstack.truncate(a.skip.0);
                        for c in &hd.close {
                            frame.cursors[*c] = None;
                        }
                        next = hd.exit_to;
                    } else {
                        next = a.resume;
                    }
                    Ok(Vec::new())
                }
            };
            // Conditions: the error, or each warning in turn (the last
            // one with a handler is handled).
            let (raised, is_error) = match res {
                Ok(w) if w.is_empty() => {
                    if !matches!(op, Op::Diag { .. } | Op::HEnd(_) | Op::Jump(_)) {
                        area.clear();
                    }
                    pc = next;
                    continue;
                }
                Ok(w) => (w, false),
                Err(e) => (vec![e], true),
            };
            area.clone_from(&raised);
            let mut chosen: Option<(usize, usize, Raised)> = None;
            for r in &raised {
                if let Some((h, first)) = find_handler(&prog, &hstack, &active, r) {
                    chosen = Some((h, first, r.clone()));
                }
            }
            match chosen {
                Some((h, first, r)) => {
                    active.push(Active {
                        handler: h,
                        resume: cont,
                        hstack_len: hstack.len(),
                        skip: (first, hstack.len()),
                        conds: vec![r],
                    });
                    pc = prog.handlers[h].start;
                }
                None if is_error => {
                    return Err(raised[0].to_error());
                }
                None => {
                    // Unhandled warnings stay in the area for the client.
                    if let Some(ts) = self.tenant.as_deref_mut() {
                        let area = ts.warnings.get_or_insert_with(Vec::new);
                        for r in &raised {
                            if !area
                                .iter()
                                .any(|w| w.code == r.code && w.message == r.message)
                            {
                                area.push(LocalWarning {
                                    level: if r.note { "Note" } else { "Warning" },
                                    code: r.code,
                                    message: r.message.clone(),
                                });
                            }
                        }
                    }
                    pc = next;
                }
            }
        }
    }

    /// A `CALL` inside a body.
    #[allow(clippy::too_many_arguments)]
    async fn sp_call_nested(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &mut Frame,
        db: Option<&str>,
        name: &str,
        args: &[subst::Text],
        outs: &[Option<Tgt>],
        out: &mut Option<&mut dyn ResultSink>,
    ) -> Result<Vec<Raised>> {
        let Some((r, db_name)) =
            self.sp_find(db.or(Some(frame.db.as_str())), Kind::Procedure, name)
        else {
            let d = db.unwrap_or(&frame.db);
            return Err(SprigError::standard(
                1305,
                *b"42000",
                format!("PROCEDURE {d}.{name} does not exist"),
            ));
        };
        let prog = match &r.def {
            Ok((_, p)) => Arc::clone(p),
            Err(why) => return Err(not_on_engine(why)),
        };
        let exprs: Vec<String> = args.iter().map(|t| render(t, frame)).collect();
        let vals = self.sp_eval_list(cx, &exprs).await?;
        let mut inner = self.sp_frame(cx, &r, &prog, &db_name, &vals, outs)?;
        Box::pin(self.sp_run(cx, &mut inner, reborrow(out))).await?;
        self.sp_copy_out(cx, &inner, outs, Some(&mut *frame))
            .await?;
        frame.affected += inner.affected;
        frame.last_affected = inner.last_affected;
        Ok(Vec::new())
    }

    /// The condition a `SIGNAL` or `RESIGNAL` raises.
    async fn sp_signal(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &Frame,
        resignal: bool,
        cond: Option<&(Option<u16>, Option<String>)>,
        items: &[(String, subst::Text)],
        handled: Option<Raised>,
    ) -> std::result::Result<Raised, Raised> {
        let mut r = match (cond, &handled) {
            (Some((code, state)), _) => {
                let state: [u8; 5] = state
                    .as_deref()
                    .and_then(|s| s.as_bytes().try_into().ok())
                    .unwrap_or(match (&handled, code) {
                        (Some(h), Some(_)) => h.state,
                        _ => *b"HY000",
                    });
                let class = &state[..2];
                let (dcode, msg) = match class {
                    b"01" => (1642, "Unhandled user-defined warning condition"),
                    b"02" => (1643, "Unhandled user-defined not found condition"),
                    _ => (1644, "Unhandled user-defined exception condition"),
                };
                Raised {
                    code: code.unwrap_or(dcode),
                    state,
                    message: msg.to_owned(),
                    error: !matches!(class, b"01" | b"02"),
                    note: false,
                }
            }
            (None, Some(h)) if resignal => h.clone(),
            _ => {
                return Err(Raised::of(&SprigError::standard(
                    1645,
                    *b"0K000",
                    "RESIGNAL when handler not active",
                )));
            }
        };
        for (item, t) in items {
            let (v, _) = self.sp_value(cx, frame, t).await?;
            match item.as_str() {
                "MESSAGE_TEXT" => {
                    r.message = String::from_utf8_lossy(&assign::text_of(&v)).into_owned();
                }
                "MYSQL_ERRNO" => {
                    let n = match &v {
                        Datum::Int(n) => u16::try_from(*n).ok(),
                        Datum::UInt(n) => u16::try_from(*n).ok(),
                        _ => String::from_utf8_lossy(&assign::text_of(&v))
                            .trim()
                            .parse()
                            .ok(),
                    };
                    match n.filter(|n| *n > 0) {
                        Some(n) => r.code = n,
                        None => {
                            return Err(Raised::of(&SprigError::standard(
                                1231,
                                *b"42000",
                                format!(
                                    "Variable 'MYSQL_ERRNO' can't be set to the value of '{}'",
                                    String::from_utf8_lossy(&assign::text_of(&v))
                                ),
                            )));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(r)
    }

    /// `GET DIAGNOSTICS`.
    async fn sp_diag(
        &mut self,
        cx: &mut ExecCtx<'_>,
        frame: &mut Frame,
        area: &[Raised],
        row_count: i64,
        condition: Option<&subst::Text>,
        items: &[(Tgt, String)],
    ) -> std::result::Result<Vec<Raised>, Raised> {
        let cond = match condition {
            None => None,
            Some(t) => {
                let (v, _) = self.sp_value(cx, frame, t).await?;
                let n = match v {
                    Datum::Int(n) => usize::try_from(n).unwrap_or(0),
                    Datum::UInt(n) => usize::try_from(n).unwrap_or(0),
                    other => String::from_utf8_lossy(&assign::text_of(&other))
                        .trim()
                        .parse()
                        .unwrap_or(0),
                };
                match n.checked_sub(1).and_then(|i| area.get(i)) {
                    Some(c) => Some(c.clone()),
                    None => {
                        return Err(Raised::of(&SprigError::standard(
                            1758,
                            *b"35000",
                            "Invalid condition number",
                        )));
                    }
                }
            }
        };
        let mut sets = Vec::new();
        let mut w = Vec::new();
        for (t, item) in items {
            let v = match (&cond, item.as_str()) {
                (None, "NUMBER") => Datum::Int(i64::try_from(area.len()).unwrap_or(0)),
                (None, "ROW_COUNT") => Datum::Int(row_count),
                (Some(c), "RETURNED_SQLSTATE") => {
                    Datum::str(std::str::from_utf8(&c.state).unwrap_or(""))
                }
                (Some(c), "MESSAGE_TEXT") => Datum::str(&c.message),
                (Some(c), "MYSQL_ERRNO") => Datum::Int(i64::from(c.code)),
                (Some(_), "CLASS_ORIGIN" | "SUBCLASS_ORIGIN") => Datum::str("ISO 9075"),
                (Some(_), _) => Datum::str(""),
                (None, _) => Datum::Null,
            };
            w.append(&mut self.sp_assign(cx, frame, t, v, &mut sets)?);
        }
        self.sp_user_sets(cx, sets).await?;
        Ok(w)
    }
}

/// The handler for `r`: the innermost block with a match, the best match
/// in it (code, then SQLSTATE, then class; the first declared on a tie).
/// Handlers of the blocks a running handler was declared in (and inside)
/// don't catch what it raises. Returns (handler, index of its block's
/// first handler on the stack).
fn find_handler(
    prog: &Program,
    hstack: &[usize],
    active: &[Active],
    r: &Raised,
) -> Option<(usize, usize)> {
    let skipped = |i: usize| active.iter().any(|a| i >= a.skip.0 && i < a.skip.1);
    let mut i = hstack.len();
    while i > 0 {
        i -= 1;
        if skipped(i) {
            continue;
        }
        let h = hstack[i];
        let block = prog.handlers[h].block;
        if !prog.handlers[h].conds.iter().any(|c| rank(c, r) > 0) {
            continue;
        }
        // The block's handlers on the stack: contiguous.
        let mut first = i;
        while first > 0 && prog.handlers[hstack[first - 1]].block == block && !skipped(first - 1) {
            first -= 1;
        }
        let mut last = i;
        while last + 1 < hstack.len() && prog.handlers[hstack[last + 1]].block == block {
            last += 1;
        }
        let best = (first..=last)
            .filter(|k| !skipped(*k))
            .map(|k| {
                let h = hstack[k];
                let rk = prog.handlers[h]
                    .conds
                    .iter()
                    .map(|c| rank(c, r))
                    .max()
                    .unwrap_or(0);
                (rk, std::cmp::Reverse(k), h)
            })
            .max()?;
        if best.0 == 0 {
            return None;
        }
        return Some((best.2, first));
    }
    None
}

fn raised_of_condition(c: &Condition) -> Raised {
    Raised {
        code: c.code,
        state: warning_state(c.code),
        message: c.message.clone(),
        error: false,
        note: c.level == Level::Note,
    }
}

/// `v` stored in a variable of column `col` (as computed without one).
fn store(
    col: Option<&Column>,
    v: Datum,
    env: assign::Env,
    conds: &mut Vec<Condition>,
) -> Result<Datum> {
    let Some(c) = col else { return Ok(v) };
    if v.is_null() {
        return Ok(Datum::Null);
    }
    let mut c = c.clone();
    c.nullable = true;
    let mut cx = assign::Store {
        env,
        db: b"",
        table: "",
        row: 1,
        conds,
    };
    assign::store(&mut cx, &c, v)
}

/// A fragment with the frame's values put in.
fn render(t: &subst::Text, frame: &Frame) -> String {
    subst::render(t, &|slot| {
        literal(&frame.vals[slot], frame.cols[slot].as_ref())
    })
}

/// The client sink, borrowed again for one statement.
fn reborrow<'s>(o: &'s mut Option<&mut dyn ResultSink>) -> Option<&'s mut dyn ResultSink> {
    match o {
        Some(x) => Some(&mut **x),
        None => None,
    }
}

/// Runs `fut` with a routine stack on the task (a fresh one at the top).
async fn with_stack<F: std::future::Future>(fut: F) -> F::Output {
    if STACK.try_with(|_| ()).is_ok() {
        fut.await
    } else {
        STACK.scope(RefCell::new(Vec::new()), fut).await
    }
}

/// A call of a stored function in a statement's text.
struct FnCall {
    start: usize,
    end: usize,
    db: Option<String>,
    name: String,
    args: Vec<String>,
}

/// The stored-function calls of `sql` (outermost ones), by `is_fn`.
fn fn_calls(sql: &str, is_fn: &dyn Fn(Option<&str>, &str) -> bool) -> Vec<FnCall> {
    use sqlparser::ast::{Expr, ObjectNamePart, visit_expressions};
    let Ok(stmts) = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, sql)
    else {
        return Vec::new();
    };
    let mut starts: Vec<(usize, Option<String>, String)> = Vec::new();
    let _ = visit_expressions(&stmts, |e| {
        if let Expr::Function(f) = e {
            let parts: Vec<&sqlparser::ast::Ident> = f
                .name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i),
                    ObjectNamePart::Function(_) => None,
                })
                .collect();
            let (db, id) = match parts.as_slice() {
                [n] => (None, *n),
                [d, n] => (Some(d.value.clone()), *n),
                _ => return std::ops::ControlFlow::<()>::Continue(()),
            };
            if is_fn(db.as_deref(), &id.value) {
                let first = parts[0];
                if let Some(at) = line_col(sql, first.span.start.line, first.span.start.column) {
                    starts.push((at, db, id.value.clone()));
                }
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    starts.sort_by_key(|s| s.0);
    let mut out: Vec<FnCall> = Vec::new();
    for (at, db, name) in starts {
        if out.iter().any(|c| at >= c.start && at < c.end) {
            continue;
        }
        let Ok(toks) = define::lex::lex(&sql[at..]) else {
            continue;
        };
        let Some(open) = toks.iter().position(|t| t.op("(")) else {
            continue;
        };
        let mut depth = 0usize;
        let mut args = Vec::new();
        let mut arg_start = toks.get(open + 1).map_or(0, |t| t.at);
        let mut end = None;
        for t in &toks[open..] {
            if t.op("(") {
                depth += 1;
            } else if t.op(")") {
                depth -= 1;
                if depth == 0 {
                    let a = sql[at + arg_start..at + t.at].trim();
                    if !a.is_empty() {
                        args.push(a.to_owned());
                    }
                    end = Some(at + t.at + 1);
                    break;
                }
            } else if t.op(",") && depth == 1 {
                args.push(sql[at + arg_start..at + t.at].trim().to_owned());
                arg_start = t.at + 1;
            }
        }
        if let Some(end) = end {
            out.push(FnCall {
                start: at,
                end,
                db,
                name,
                args,
            });
        }
    }
    out
}

fn line_col(text: &str, line: u64, col: u64) -> Option<usize> {
    if line == 0 || col == 0 {
        return None;
    }
    let mut start = 0usize;
    for _ in 1..line {
        start += text[start..].find('\n')? + 1;
    }
    let mut at = start;
    let mut chars = text[start..].chars();
    for _ in 1..col {
        at += chars.next()?.len_utf8();
    }
    Some(at)
}

/// Whether expressions read a column (they then change per row).
fn reads_columns(args: &[String]) -> bool {
    use sqlparser::ast::{Expr, visit_expressions};
    if args.is_empty() {
        return false;
    }
    let sql = format!("SELECT {}", args.join(", "));
    let Ok(stmts) = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &sql)
    else {
        return true;
    };
    let mut found = false;
    let _ = visit_expressions(&stmts, |e| {
        if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            found = true;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    found
}

/// The `CAST` type of a return type, when one fits.
fn cast_type(ty: &str) -> Option<String> {
    let up = ty.to_ascii_uppercase();
    let word: String = up.chars().take_while(char::is_ascii_alphabetic).collect();
    let args = up
        .find('(')
        .and_then(|a| up[a..].find(')').map(|b| up[a..=a + b].to_owned()))
        .unwrap_or_default();
    Some(match word.as_str() {
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT" => {
            if up.contains("UNSIGNED") {
                "UNSIGNED".into()
            } else {
                "SIGNED".into()
            }
        }
        "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => format!("DECIMAL{args}"),
        "FLOAT" | "DOUBLE" | "REAL" => "DOUBLE".into(),
        "CHAR" | "VARCHAR" => format!("CHAR{args}"),
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => "CHAR".into(),
        "DATE" => "DATE".into(),
        "DATETIME" | "TIMESTAMP" => format!("DATETIME{args}"),
        "TIME" => format!("TIME{args}"),
        _ => return None,
    })
}

/// A function whose body only computes (`RETURN e`, or `DECLARE`s, `SET`s
/// and a final `RETURN`) as one expression of its arguments' texts.
fn inline(c: &parse::Create, args: &[String]) -> Option<String> {
    let mut env: Vec<(String, String)> = c
        .params
        .iter()
        .zip(args)
        .map(|(p, a)| (p.name.to_lowercase(), format!("({a})")))
        .collect();
    let sub = |e: &str, env: &[(String, String)]| -> String {
        let names: Vec<String> = env.iter().map(|(n, _)| n.clone()).collect();
        let t = subst::text(e, true, &|n| {
            names.iter().position(|x| x.eq_ignore_ascii_case(n))
        });
        format!("({})", subst::render(&t, &|i| env[i].1.clone()))
    };
    let body: Vec<&Stmt> = match &c.body {
        Stmt::Return(_) => vec![&c.body],
        Stmt::Block { decls, body, .. } => {
            for d in decls {
                let parse::Decl::Var { names, default, .. } = d else {
                    return None;
                };
                for n in names {
                    let v = default
                        .as_ref()
                        .map_or_else(|| "NULL".to_owned(), |e| sub(e, &env));
                    env.push((n.to_lowercase(), v));
                }
            }
            body.iter().collect()
        }
        _ => return None,
    };
    let (last, rest) = body.split_last()?;
    for s in rest {
        let Stmt::Set(items) = s else { return None };
        for a in items {
            let parse::Assign::Local(n, e) = a else {
                return None;
            };
            let v = sub(e, &env);
            env.push((n.to_lowercase(), v));
        }
    }
    let Stmt::Return(e) = last else { return None };
    let e = sub(e, &env);
    Some(match c.returns.as_deref().and_then(cast_type) {
        Some(t) => format!("CAST({e} AS {t})"),
        None => e,
    })
}

impl PassSession {
    /// A statement that calls stored functions of an engine database: each
    /// call with constant arguments is run first and its value put in its
    /// place; a call per row is put in as its expression when the function
    /// only computes. The statement then runs as written otherwise.
    /// `None`: it calls none.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::exec) async fn engine_sql_functions(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<sprig_catalog::TenantCatalog>,
        logical: Option<&[u8]>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(eng) = catalog::of(cat) else {
            return Ok(None);
        };
        if !eng.routines.keys().any(|(_, k, _)| *k == Kind::Function) {
            return Ok(None);
        }
        let text = match logical.or(self.eng.entry.as_deref()) {
            Some(t) => String::from_utf8_lossy(t).into_owned(),
            None => return Ok(None),
        };
        let calls = {
            let is_fn = |db: Option<&str>, n: &str| self.sp_find(db, Kind::Function, n).is_some();
            fn_calls(&text, &is_fn)
        };
        if calls.is_empty() {
            return Ok(None);
        }
        let depth = STACK.try_with(|s| s.borrow().len()).unwrap_or(0);
        if depth >= MAX_DEPTH {
            return Err(SprigError::standard(
                1424,
                *b"HY000",
                "Recursive stored functions and triggers are not allowed",
            ));
        }
        let fut = self.sp_rewrite_run(cx, text, calls, more, format, out);
        Box::pin(with_stack(fut)).await.map(Some)
    }

    async fn sp_rewrite_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: String,
        calls: Vec<FnCall>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let mut values = Vec::with_capacity(calls.len());
        for c in &calls {
            let Some((r, db_name)) = self.sp_find(c.db.as_deref(), Kind::Function, &c.name) else {
                let e =
                    SprigError::internal("a stored function left the catalog during the statement");
                return self.fail(out, &e).await;
            };
            let v = match &r.def {
                Err(why) => Err(not_on_engine(why)),
                Ok((def, _)) if reads_columns(&c.args) => match inline(def, &c.args) {
                    Some(e) if def.params.len() == c.args.len() => Ok(e),
                    _ => Err(not_on_engine(
                        "a stored function with statements called once per row",
                    )),
                },
                Ok(_) => Box::pin(self.sp_call_function(cx, &r, &db_name, &c.args)).await,
            };
            match v {
                Ok(s) => values.push(s),
                Err(e) => return self.fail(out, &e).await,
            }
        }
        let refs: Vec<subst::Ref> = calls
            .iter()
            .enumerate()
            .map(|(i, c)| subst::Ref {
                start: c.start,
                end: c.end,
                slot: i,
            })
            .collect();
        let t = subst::Text {
            aliases: subst::select_aliases(&text, &refs),
            sql: text,
            refs,
        };
        let sql = subst::render(&t, &|i| values[i].clone());
        let _ = STACK.try_with(|s| s.borrow_mut().push(" rewrite".into()));
        let attrs = QueryAttrs::default();
        let r = Box::pin(self.tenant_text(cx, &Bytes::from(sql), &attrs, more, format, out)).await;
        let _ = STACK.try_with(|s| s.borrow_mut().pop());
        r
    }

    /// Runs a stored function on argument texts: its value as a literal.
    async fn sp_call_function(
        &mut self,
        cx: &mut ExecCtx<'_>,
        r: &Arc<Routine>,
        db_name: &str,
        args: &[String],
    ) -> Result<String> {
        let (def, prog) = match &r.def {
            Ok((d, p)) => (d, Arc::clone(p)),
            Err(why) => return Err(not_on_engine(why)),
        };
        let vals = self.sp_eval_list(cx, args).await?;
        let mut frame = self.sp_frame(cx, r, &prog, db_name, &vals, &[])?;
        let v = match Box::pin(self.sp_run(cx, &mut frame, None)).await? {
            Flow::Returned(v) => v,
            Flow::Done => {
                return Err(SprigError::standard(
                    1321,
                    *b"2F005",
                    format!("FUNCTION {} ended without RETURN", r.name),
                ));
            }
        };
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let db_cs = self
            .engine_cat()
            .and_then(|c| {
                c.databases
                    .get(r.db)
                    .map(|d| (d.default_charset.clone(), d.default_collation.clone()))
            })
            .unwrap_or_default();
        let col = def
            .returns
            .as_deref()
            .and_then(|t| column_of(t, &r.name, mysql, &db_cs));
        let env = self.engine_vars(cx).env;
        let mut conds = Vec::new();
        let v = store(col.as_ref(), v, env, &mut conds)?;
        Ok(literal(&v, col.as_ref()))
    }
}

impl PassSession {
    /// `SHOW CREATE PROCEDURE | FUNCTION` and `SHOW PROCEDURE | FUNCTION
    /// STATUS` for engine databases (`sql`: the statement as rewritten).
    #[allow(clippy::too_many_lines)] // two SHOW forms, each server's columns
    pub(in crate::exec) fn engine_show_routine(
        &self,
        info: &sprig_sql::names::show::ShowInfo,
        sql: &[u8],
    ) -> Option<Result<crate::engine::show::Answer>> {
        use crate::engine::show::{Answer, coldef};
        use sprig_proto_mysql::resultset::encode_text_row;
        use sprig_sql::names::show::{ShowKind, like_match};
        let ts = self.tenant.as_deref()?;
        let cat = self.engine_cat()?;
        let eng = catalog::of(&cat)?;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let (cs, dec) = if mysql { (45u16, 31u8) } else { (224, 0x27) };
        let kind = match info.kind {
            ShowKind::CreateProcedure | ShowKind::ProcedureStatus => Kind::Procedure,
            _ => Kind::Function,
        };
        let word = if kind == Kind::Function {
            "Function"
        } else {
            "Procedure"
        };
        if matches!(
            info.kind,
            ShowKind::CreateProcedure | ShowKind::CreateFunction
        ) {
            let name = info.object.clone()?;
            let db = if let Some(s) = info.schema.as_deref() {
                s.parse::<sprig_common::names::SchemaName>().ok()?.db()?
            } else {
                let cur = self.current_logical()?;
                cat.database(&database_key(&cur, ts.lower_case_table_names()))?
                    .db_id
            };
            if !eng.dbs.contains(&db) {
                return None;
            }
            let Some(r) = eng.routine(db, kind, &name) else {
                return Some(Err(SprigError::standard(
                    1305,
                    *b"42000",
                    format!("{} {name} does not exist", kind.word()),
                )));
            };
            let defs = vec![
                coldef("", "", word, "", cs, 256, 0xfd, 0x0001, dec),
                coldef(
                    "",
                    "",
                    "sql_mode",
                    "",
                    cs,
                    if mysql { 468 } else { 356 },
                    0xfd,
                    0x0001,
                    dec,
                ),
                coldef(
                    "",
                    "",
                    &format!("Create {word}"),
                    "",
                    cs,
                    4096,
                    0xfd,
                    0x0001,
                    dec,
                ),
                coldef(
                    "",
                    "",
                    "character_set_client",
                    "",
                    cs,
                    128,
                    0xfd,
                    0x0001,
                    dec,
                ),
                coldef(
                    "",
                    "",
                    "collation_connection",
                    "",
                    cs,
                    if mysql { 128 } else { 256 },
                    0xfd,
                    0x0001,
                    dec,
                ),
                coldef("", "", "Database Collation", "", cs, 128, 0xfd, 0x0001, dec),
            ];
            let row = encode_text_row([
                Some(r.name.as_bytes()),
                Some(r.cx.sql_mode.as_bytes()),
                Some(r.show.as_bytes()),
                Some(r.cx.charset_client.as_bytes()),
                Some(r.cx.collation_connection.as_bytes()),
                Some(r.cx.db_collation.as_bytes()),
            ]);
            return Some(Ok(Answer {
                defs,
                rows: vec![row],
                no_index: false,
            }));
        }
        // STATUS: every routine of the kind in the tenant's databases.
        let map = ts.map();
        let mut defs = vec![
            coldef("", "", "Db", "", cs, 256, 0xfd, 0x0001, 0),
            coldef("", "", "Name", "", cs, 256, 0xfd, 0x0001, 0),
            coldef(
                "",
                "",
                "Type",
                "",
                cs,
                if mysql { 36 } else { 52 },
                if mysql { 0xfe } else { 0xfd },
                0x0001,
                0,
            ),
        ];
        if mysql {
            defs.push(coldef("", "", "Language", "", cs, 256, 0xfd, 0x0001, 0));
        }
        let dt = if mysql { 0x07 } else { 0x0c };
        defs.extend([
            coldef(
                "",
                "",
                "Definer",
                "",
                cs,
                if mysql { 1152 } else { 1536 },
                0xfd,
                0x0001,
                0,
            ),
            coldef("", "", "Modified", "", 63, 19, dt, 0x0081, 0),
            coldef("", "", "Created", "", 63, 19, dt, 0x0081, 0),
            coldef(
                "",
                "",
                "Security_type",
                "",
                cs,
                28,
                if mysql { 0xfe } else { 0xfd },
                0x0001,
                0,
            ),
            coldef(
                "",
                "",
                "Comment",
                "",
                cs,
                if mysql { 262_140 } else { 786_420 },
                0xfc,
                0x0011,
                0,
            ),
            coldef(
                "",
                "",
                "character_set_client",
                "",
                cs,
                if mysql { 256 } else { 128 },
                0xfd,
                0x0001,
                0,
            ),
            coldef("", "", "collation_connection", "", cs, 256, 0xfd, 0x0001, 0),
            coldef("", "", "Database Collation", "", cs, 256, 0xfd, 0x0001, 0),
        ]);
        let mut list: Vec<(String, String, Arc<Routine>)> = eng
            .routines
            .iter()
            .filter(|((_, k, _), _)| *k == kind)
            .filter_map(|((d, _, _), r)| {
                // The database as the name layer maps answers back: its
                // physical schema.
                let logical = cat.databases.get(*d)?.name.clone();
                let db = sprig_sql::names::NameMap::physical(
                    &*map,
                    &logical,
                    sprig_sql::names::Purpose::Metadata,
                )
                .ok()
                .and_then(|p| p.0.first().map(ToString::to_string))
                .unwrap_or(logical);
                Some((db, r.name.clone(), Arc::clone(r)))
            })
            .filter(|(_, n, _)| info.like.as_deref().is_none_or(|p| like_match(p, n, false)))
            .collect();
        list.sort_by(|a, b| (&a.0, a.1.to_lowercase()).cmp(&(&b.0, b.1.to_lowercase())));
        let rows = list
            .iter()
            .map(|(db, n, r)| {
                let definer = format!("{}@{}", r.cx.definer.0, r.cx.definer.1);
                let invoker = r
                    .def
                    .as_ref()
                    .is_ok_and(|(c, _)| c.chars.invoker == Some(true));
                let comment = r
                    .def
                    .as_ref()
                    .ok()
                    .and_then(|(c, _)| c.chars.comment.clone())
                    .unwrap_or_default();
                let mut vals: Vec<Option<Vec<u8>>> = vec![
                    Some(db.clone().into_bytes()),
                    Some(n.clone().into_bytes()),
                    Some(kind.word().as_bytes().to_vec()),
                ];
                if mysql {
                    vals.push(Some(b"SQL".to_vec()));
                }
                vals.extend([
                    Some(definer.into_bytes()),
                    Some(r.cx.modified.clone().into_bytes()),
                    Some(r.cx.created.clone().into_bytes()),
                    Some(if invoker {
                        b"INVOKER".to_vec()
                    } else {
                        b"DEFINER".to_vec()
                    }),
                    Some(comment.into_bytes()),
                    Some(r.cx.charset_client.clone().into_bytes()),
                    Some(r.cx.collation_connection.clone().into_bytes()),
                    Some(r.cx.db_collation.clone().into_bytes()),
                ]);
                encode_text_row(vals.iter().map(|v| v.as_deref()))
            })
            .collect();
        let answer = Answer {
            defs,
            rows,
            no_index: !mysql,
        };
        match info.filter.clone() {
            Some(range) => {
                let cond = std::str::from_utf8(sql.get(range)?).ok()?;
                crate::engine::infoschema::filter_answer(answer, cond).map(Ok)
            }
            None => Some(Ok(answer)),
        }
    }
}
