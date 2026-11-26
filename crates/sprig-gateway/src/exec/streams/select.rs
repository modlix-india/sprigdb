//! `SELECT` from streams of engine databases.
//!
//! A stream reads like a table with the columns `partition_no`,
//! `offset_no`, `ts`, `key`, `value` and `headers` (as streams kept in
//! MariaDB read), keyed by `(partition_no, offset_no)`: a condition on the
//! partition and the offset reads only that part of the stream log.
//!
//! ```text
//! SELECT ... FROM <stream> [JOIN <stream>] ...
//!   names           every table it names is a stream of an engine
//!                   database (a common table expression's name aside);
//!                   a stream joined with tables goes to the engine's
//!                   general path, which reads streams as tables
//!                   (`engine::streamtable`), as a prepared statement does
//!   privileges      SELECT on each stream (1142)
//!   plan and run    sprig_plan over StreamCatalog / StreamStorage,
//!                   sprig_exec for expressions; text results
//! ```
//!
//! Reads see the stream as it is now (the log isn't versioned), inside a
//! transaction too. Every condition is checked on the rows read, also
//! where the key range already guarantees it.

use std::collections::{HashMap, HashSet};
use std::ops::{Bound, ControlFlow};
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::row::{ColumnType, Date, DateTime, LobSize, Value};
use sprig_exec::ops::Row;
use sprig_exec::{Ctx, ExprType, FieldType, Kind, Persona, Session, SqlError};
use sprig_plan::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use sprig_plan::catalog::{Catalog, ColumnDef as PlanColumn, IndexDef, IndexPart, KeyId, TableDef};
use sprig_plan::{PlanOptions, Planner};
use sprig_proto_kafka::broker::data::StoredRecord;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};
use sprig_sql::lex::LexOptions;
use sprig_streams::seg::kafka::{EngineStreams, Need, decode_stored};
use sprig_streams::seg::sql::SqlHost;
use sqlparser::ast::{ObjectName, ObjectNamePart, Query, Statement, Visit, Visitor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// The most bytes one statement's sorts and hash tables may take.
const MEMORY_LIMIT: usize = 256 << 20;

/// Bytes read from the log at a time.
const READ_BYTES: usize = 1 << 20;

/// A result: column definitions and text rows.
pub(crate) type Answer = (Vec<ColumnDef>, Vec<Vec<Option<Vec<u8>>>>);

fn sql_error(e: SqlError) -> SprigError {
    let mut state = *b"HY000";
    if e.sqlstate.len() == 5 {
        state.copy_from_slice(e.sqlstate.as_bytes());
    }
    SprigError::standard(e.code, state, e.message)
}

fn internal(e: impl std::fmt::Display) -> SqlError {
    SqlError::new(1105, "HY000", e.to_string())
}

/// Whether `sql` starts as a query (`SELECT`, `WITH`, `(`), comments and
/// spaces aside.
fn looks_like_query(sql: &str) -> bool {
    let mut s = sql.trim_start();
    loop {
        if let Some(r) = s.strip_prefix("/*") {
            match r.find("*/") {
                Some(i) => s = r[i + 2..].trim_start(),
                None => return false,
            }
        } else if s.starts_with("--") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = s[i + 1..].trim_start(),
                None => return false,
            }
        } else {
            break;
        }
    }
    let head: String = s.chars().take(6).collect::<String>().to_ascii_uppercase();
    head.starts_with("SELECT") || head.starts_with("WITH") || head.starts_with('(')
}

/// The names of the statement's common table expressions.
#[derive(Default)]
struct Ctes(HashSet<String>);

impl Visitor for Ctes {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.0.insert(c.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }
}

fn ident_parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// One stream the statement reads.
#[derive(Debug, Clone)]
struct StreamTable {
    def: Arc<TableDef>,
    stream_id: u64,
    partitions: u32,
}

/// The columns of a stream read as a table.
fn stream_def(db: &str, name: &str) -> TableDef {
    TableDef {
        schema: db.to_owned(),
        name: name.to_owned(),
        columns: vec![
            PlanColumn::new("partition_no", ColumnType::int(true)).not_null(),
            PlanColumn::new("offset_no", ColumnType::bigint(true)).not_null(),
            PlanColumn::new("ts", ColumnType::DateTime { fsp: 3 }).not_null(),
            PlanColumn::new("key", ColumnType::VarBinary { bytes: 4096 }),
            PlanColumn::new(
                "value",
                ColumnType::Blob {
                    size: LobSize::Long,
                },
            ),
            PlanColumn::new("headers", ColumnType::Json),
        ],
        primary: Some(IndexDef::new(
            "PRIMARY",
            true,
            vec![IndexPart::asc(0), IndexPart::asc(1)],
        )),
        indexes: Vec::new(),
    }
}

/// The storage of one stream read as table `db`.`name` by the engine's
/// general path (`engine::streamtable`).
pub(crate) fn one_stream(
    eng: Arc<EngineStreams>,
    tenant: u64,
    db: &str,
    name: &str,
    stream_id: u64,
    partitions: u32,
) -> Arc<dyn Storage> {
    let mut tables = HashMap::new();
    tables.insert(
        lower(db, name),
        StreamTable {
            def: Arc::new(stream_def(db, name)),
            stream_id,
            partitions,
        },
    );
    Arc::new(StreamStorage {
        eng,
        tenant,
        tables,
    })
}

fn lower(db: &str, name: &str) -> (String, String) {
    (db.to_ascii_lowercase(), name.to_ascii_lowercase())
}

/// The streams the statement may read, by name.
struct StreamCatalog {
    current: Option<String>,
    tables: HashMap<(String, String), StreamTable>,
}

impl Catalog for StreamCatalog {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>> {
        self.tables
            .get(&lower(schema, name))
            .map(|t| Arc::clone(&t.def))
    }

    fn default_schema(&self) -> Option<String> {
        self.current.clone()
    }
}

/// The stream log behind the planner.
struct StreamStorage {
    eng: Arc<EngineStreams>,
    tenant: u64,
    tables: HashMap<(String, String), StreamTable>,
}

/// One partition's offsets to read: `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span {
    partition: u32,
    from: i64,
    to: i64,
}

/// An integer key bound, `None` when the value isn't one (the range is
/// then taken wider; the conditions are checked on every row anyway).
fn int_of(v: &Value) -> Option<i128> {
    match v {
        Value::Int(i) => Some(i128::from(*i)),
        Value::UInt(u) => Some(i128::from(*u)),
        _ => None,
    }
}

/// The spans of `range` over a stream of `partitions` partitions, with
/// each partition's `(log_start, high_watermark)` from `bounds`.
fn spans_of(range: &KeyRange, partitions: u32, bounds: &dyn Fn(u32) -> (i64, i64)) -> Vec<Span> {
    if partitions == 0 {
        return Vec::new();
    }
    let last = i128::from(partitions - 1);
    // (partition, offset within it) at each end; offsets: start inclusive,
    // end exclusive.
    let (mut p_lo, mut o_lo): (i128, Option<i128>) = (0, None);
    match &range.start {
        Bound::Included(v) | Bound::Excluded(v) => {
            let excl = matches!(range.start, Bound::Excluded(_));
            match (v.first().and_then(int_of), v.get(1).and_then(int_of)) {
                (Some(p), Some(o)) => {
                    p_lo = p;
                    o_lo = Some(if excl { o + 1 } else { o });
                }
                (Some(p), None) if v.len() == 1 => p_lo = if excl { p + 1 } else { p },
                (Some(p), None) => p_lo = p,
                _ => {}
            }
        }
        Bound::Unbounded => {}
    }
    let (mut p_hi, mut o_hi): (i128, Option<i128>) = (last, None);
    match &range.end {
        Bound::Included(v) | Bound::Excluded(v) => {
            let excl = matches!(range.end, Bound::Excluded(_));
            match (v.first().and_then(int_of), v.get(1).and_then(int_of)) {
                (Some(p), Some(o)) => {
                    p_hi = p;
                    o_hi = Some(if excl { o } else { o + 1 });
                }
                (Some(p), None) if v.len() == 1 => p_hi = if excl { p - 1 } else { p },
                (Some(p), None) => p_hi = p,
                _ => {}
            }
        }
        Bound::Unbounded => {}
    }
    let mut out = Vec::new();
    let mut p = p_lo.max(0);
    while p <= p_hi.min(last) {
        let part = u32::try_from(p).unwrap_or(u32::MAX);
        let (start, hw) = bounds(part);
        let from = if p == p_lo { o_lo.unwrap_or(0) } else { 0 }.max(i128::from(start));
        let to = if p == p_hi {
            o_hi.unwrap_or(i128::from(hw))
        } else {
            i128::from(hw)
        }
        .min(i128::from(hw));
        if from < to {
            out.push(Span {
                partition: part,
                from: i64::try_from(from).unwrap_or(i64::MAX),
                to: i64::try_from(to).unwrap_or(i64::MAX),
            });
        }
        p += 1;
    }
    out
}

/// Milliseconds since the epoch as a `DATETIME(3)` in UTC.
fn datetime_of(ms: i64) -> DateTime {
    let days = ms.div_euclid(86_400_000);
    let in_day = ms.rem_euclid(86_400_000);
    // Days since 1970-01-01 to a civil date (the proleptic Gregorian
    // calendar, eras of 400 years).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let date = Date::new(
        u16::try_from(year.clamp(0, 9999)).unwrap_or(0),
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    );
    let secs = in_day / 1000;
    DateTime::new(
        date,
        u8::try_from(secs / 3600).unwrap_or(0),
        u8::try_from(secs / 60 % 60).unwrap_or(0),
        u8::try_from(secs % 60).unwrap_or(0),
        u32::try_from(in_day % 1000 * 1000).unwrap_or(0),
    )
}

/// A record as a row of the stream's columns.
fn row_of(partition: u32, r: &StoredRecord) -> Row {
    let headers = if r.headers.is_empty() {
        Value::Null
    } else {
        let m: serde_json::Map<String, serde_json::Value> = r
            .headers
            .iter()
            .map(|h| {
                (
                    h.key.clone(),
                    h.value.as_ref().map_or(serde_json::Value::Null, |v| {
                        serde_json::Value::String(String::from_utf8_lossy(v).into_owned())
                    }),
                )
            })
            .collect();
        Value::Json(Bytes::from(serde_json::Value::Object(m).to_string()))
    };
    vec![
        Value::UInt(u64::from(partition)),
        Value::UInt(u64::try_from(r.offset).unwrap_or(0)),
        Value::DateTime(datetime_of(r.timestamp_ms)),
        r.key.as_deref().map_or(Value::Null, Value::bytes),
        r.value.as_deref().map_or(Value::Null, Value::bytes),
        headers,
    ]
}

impl StreamStorage {
    fn table(&self, def: &TableDef) -> std::result::Result<&StreamTable, SqlError> {
        self.tables
            .get(&lower(&def.schema, &def.name))
            .ok_or_else(|| internal("stream select: not a stream of the statement"))
    }

    fn bounds(&self, t: &StreamTable) -> HashMap<u32, (i64, i64)> {
        self.eng
            .log()
            .partitions(self.tenant, t.stream_id)
            .into_iter()
            .map(|(p, b)| (p, (b.log_start, b.high_watermark)))
            .collect()
    }

    fn spans(&self, t: &StreamTable, range: &KeyRange) -> Vec<Span> {
        let b = self.bounds(t);
        spans_of(range, t.partitions, &|p| {
            b.get(&p).copied().unwrap_or((0, 0))
        })
    }
}

impl Storage for StreamStorage {
    fn table_rows(&self, table: &TableDef) -> u64 {
        let Ok(t) = self.table(table) else { return 0 };
        self.bounds(t)
            .values()
            .map(|(s, h)| u64::try_from(h - s).unwrap_or(0))
            .sum()
    }

    fn rows_in_range(&self, table: &TableDef, key: KeyId, range: &KeyRange) -> u64 {
        if key != KeyId::Primary {
            return 0;
        }
        let Ok(t) = self.table(table) else { return 0 };
        self.spans(t, range)
            .iter()
            .map(|s| u64::try_from(s.to - s.from).unwrap_or(0))
            .sum()
    }

    #[allow(clippy::cast_precision_loss)] // an estimate
    fn rows_per_key(&self, table: &TableDef, key: KeyId, parts: usize) -> Option<f64> {
        if key != KeyId::Primary {
            return None;
        }
        let t = self.table(table).ok()?;
        match parts {
            1 => Some((self.table_rows(table) as f64 / f64::from(t.partitions.max(1))).max(1.0)),
            2 => Some(1.0),
            _ => None,
        }
    }

    fn get(
        &self,
        table: &TableDef,
        key: &[Value],
        counters: &Counters,
    ) -> sprig_exec::Result<Option<Row>> {
        let t = self.table(table)?;
        let (Some(p), Some(o)) = (key.first().and_then(int_of), key.get(1).and_then(int_of)) else {
            return Ok(None);
        };
        counters.add_seek();
        let (Ok(p), Ok(o)) = (u32::try_from(p), i64::try_from(o)) else {
            return Ok(None);
        };
        let mut c = StreamCursor::new(
            Arc::clone(&self.eng),
            self.tenant,
            t.stream_id,
            vec![Span {
                partition: p,
                from: o,
                to: o.saturating_add(1),
            }],
            false,
            Arc::new(Counters::default()),
        );
        let row = c.next()?;
        if row.is_some() {
            counters.add_row();
        }
        Ok(row)
    }

    fn scan(
        &self,
        table: &TableDef,
        key: KeyId,
        ranges: Vec<KeyRange>,
        reverse: bool,
        _fetch: Fetch,
        counters: Arc<Counters>,
    ) -> sprig_exec::Result<Box<dyn RowCursor>> {
        if key != KeyId::Primary {
            return Err(internal("a stream has no secondary index"));
        }
        let t = self.table(table)?;
        let mut spans = Vec::new();
        for r in &ranges {
            let mut s = self.spans(t, r);
            if reverse {
                s.reverse();
            }
            spans.extend(s);
        }
        Ok(Box::new(StreamCursor::new(
            Arc::clone(&self.eng),
            self.tenant,
            t.stream_id,
            spans,
            reverse,
            counters,
        )))
    }
}

/// Rows of spans, span after span; backwards inside a span with
/// `reverse` (the span is read whole first, then handed over).
struct StreamCursor {
    eng: Arc<EngineStreams>,
    tenant: u64,
    stream: u64,
    spans: Vec<Span>,
    at: usize,
    /// The next offset to read in the current span.
    next: Option<i64>,
    buf: std::collections::VecDeque<Row>,
    reverse: bool,
    counters: Arc<Counters>,
}

impl StreamCursor {
    fn new(
        eng: Arc<EngineStreams>,
        tenant: u64,
        stream: u64,
        spans: Vec<Span>,
        reverse: bool,
        counters: Arc<Counters>,
    ) -> Self {
        Self {
            eng,
            tenant,
            stream,
            spans,
            at: 0,
            next: None,
            buf: std::collections::VecDeque::new(),
            reverse,
            counters,
        }
    }

    /// Reads the next stretch of the current span into `buf` (the whole
    /// span when reading backwards); `false` when no span is left.
    fn fill(&mut self) -> sprig_exec::Result<bool> {
        let Some(span) = self.spans.get(self.at).copied() else {
            return Ok(false);
        };
        let key = sprig_streams::seg::Key {
            tenant: self.tenant,
            stream: self.stream,
            partition: span.partition,
        };
        let next = self.next;
        let mut at = next.unwrap_or_else(|| {
            self.counters.add_seek();
            span.from
        });
        let mut rows = Vec::new();
        let mut done = true;
        while at < span.to {
            let bytes = match self.eng.log().read(key, at, READ_BYTES) {
                Ok((_, b)) => b,
                // Trimmed meanwhile, or past the end: the span is done.
                Err(Ok(_)) => break,
                Err(Err(e)) => return Err(internal(e.message)),
            };
            if bytes.is_empty() {
                break;
            }
            let recs = decode_stored(&bytes, at).map_err(|e| internal(e.message))?;
            // Where the returned batches end (compaction leaves gaps).
            let mut end = at;
            let mut pos = 0;
            while let Some(h) = sprig_streams::seg::batch::Head::parse(&bytes[pos..]) {
                end = end.max(h.next_offset());
                pos += h.total;
                if pos >= bytes.len() {
                    break;
                }
            }
            for r in recs.iter().filter(|r| r.offset >= at && r.offset < span.to) {
                rows.push(row_of(span.partition, r));
            }
            if end <= at {
                break;
            }
            at = end;
            if !self.reverse {
                done = at >= span.to;
                break;
            }
        }
        if done {
            self.at += 1;
            self.next = None;
        } else {
            self.next = Some(at);
        }
        if self.reverse {
            rows.reverse();
        }
        self.buf.extend(rows);
        Ok(true)
    }
}

impl RowCursor for StreamCursor {
    fn next(&mut self) -> sprig_exec::Result<Option<Row>> {
        loop {
            if let Some(r) = self.buf.pop_front() {
                self.counters.add_row();
                return Ok(Some(r));
            }
            if !self.fill()? {
                return Ok(None);
            }
        }
    }
}

fn wire_type(f: FieldType) -> u8 {
    match f {
        FieldType::Null => column_type::NULL,
        FieldType::Tiny => column_type::TINY,
        FieldType::Short => column_type::SHORT,
        FieldType::Int24 => column_type::INT24,
        FieldType::Long => column_type::LONG,
        FieldType::LongLong => column_type::LONGLONG,
        FieldType::Year => column_type::YEAR,
        FieldType::Bit => column_type::BIT,
        FieldType::NewDecimal => column_type::NEWDECIMAL,
        FieldType::Float => column_type::FLOAT,
        FieldType::Double => column_type::DOUBLE,
        FieldType::VarString => column_type::VAR_STRING,
        FieldType::String => column_type::STRING,
        FieldType::Blob => column_type::BLOB,
        FieldType::MediumBlob => column_type::MEDIUM_BLOB,
        FieldType::LongBlob => column_type::LONG_BLOB,
        FieldType::Date => column_type::DATE,
        FieldType::Time => column_type::TIME,
        FieldType::DateTime => column_type::DATETIME,
        FieldType::Timestamp => column_type::TIMESTAMP,
        FieldType::Json => column_type::JSON,
    }
}

/// A result column's definition from the planner's static type.
fn column_def(name: &str, ty: &ExprType, persona: Persona, charset: u16) -> ColumnDef {
    let m = ty.meta(persona);
    let text = ty.kind == Kind::Str && !m.binary;
    let mut flags = ColumnFlags::empty();
    if m.not_null {
        flags |= ColumnFlags::NOT_NULL;
    }
    if m.unsigned {
        flags |= ColumnFlags::UNSIGNED;
    }
    if m.binary {
        flags |= ColumnFlags::BINARY;
    }
    if matches!(
        m.field,
        FieldType::Blob | FieldType::MediumBlob | FieldType::LongBlob
    ) {
        flags |= ColumnFlags::BLOB;
    }
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: if text { charset } else { 63 },
        length: m.len,
        col_type: wire_type(m.field),
        flags: flags.bits(),
        decimals: m.dec,
        ..ColumnDef::default()
    }
}

/// What the statement reads, when it reads streams of engine databases:
/// the streams by name. `Ok(None)`: it names none (it goes on as usual).
///
/// # Errors
/// `None` for a stream joined with tables; 1142 without `SELECT` on a
/// stream.
fn streams_read(
    eng: &EngineStreams,
    host: &dyn SqlHost,
    stmt: &Statement,
) -> Result<Option<HashMap<(String, String), StreamTable>>> {
    let tenant = host.tenant();
    let mut ctes = Ctes::default();
    let _ = stmt.visit(&mut ctes);
    let current = host.current_db();
    let mut streams = HashMap::new();
    let mut tables = false;
    let mut failed: Option<SprigError> = None;
    let _ = sqlparser::ast::visit_relations(stmt, |name: &ObjectName| {
        let Some(parts) = ident_parts(name) else {
            tables = true;
            return ControlFlow::Continue(());
        };
        let (db, s) = match parts.as_slice() {
            [s] if ctes.0.contains(&s.to_ascii_lowercase()) => {
                return ControlFlow::Continue(());
            }
            [s] => {
                let Some(d) = &current else {
                    tables = true;
                    return ControlFlow::Continue(());
                };
                (d.clone(), s.clone())
            }
            [d, s] => (d.clone(), s.clone()),
            _ => {
                tables = true;
                return ControlFlow::Continue(());
            }
        };
        if !host.on_engine(&db) {
            tables = true;
            return ControlFlow::Continue(());
        }
        match eng.topic(tenant, &format!("{db}.{s}")) {
            Ok(Some(r)) => {
                let def = stream_def(&db, &s);
                streams.insert(
                    lower(&db, &s),
                    StreamTable {
                        def: Arc::new(def),
                        stream_id: r.stream_id,
                        partitions: u32::try_from(r.partitions.max(0)).unwrap_or(0),
                    },
                );
                // The privilege, as the servers check it on a table.
                if !host.allowed(&db, &s, Need::Read) {
                    let (u, h) = host.user();
                    failed = Some(SprigError::standard(
                        1142,
                        *b"42000",
                        format!("SELECT command denied to user '{u}'@'{h}' for table '{s}'"),
                    ));
                    return ControlFlow::Break(());
                }
            }
            Ok(None) => tables = true,
            Err(e) => {
                failed = Some(e);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    if let Some(e) = failed {
        return Err(e);
    }
    if streams.is_empty() {
        return Ok(None);
    }
    // With tables: the engine's general path reads the streams as tables.
    if tables {
        return Ok(None);
    }
    Ok(Some(streams))
}

/// Runs `sql` when it is a query that reads streams of engine databases;
/// `Ok(None)` when it isn't one (the statement goes on as usual).
/// `charset`: the connection's results character set id.
///
/// # Errors
/// The statement's (as the planner and the expression engine raise them),
/// the privileges', or the log's.
pub(crate) fn run(
    eng: &Arc<EngineStreams>,
    host: &dyn SqlHost,
    sql: &str,
    lex: LexOptions,
    persona: Persona,
    charset: u16,
) -> Result<Option<Answer>> {
    if !looks_like_query(sql) {
        return Ok(None);
    }
    let tenant = host.tenant();
    // Cheap first: a stream's name must appear in the text.
    let topics = eng.topics(tenant)?;
    let low = sql.to_ascii_lowercase();
    if !topics.iter().any(|t| {
        t.name
            .rsplit_once('.')
            .is_some_and(|(_, s)| low.contains(&s.to_ascii_lowercase()))
    }) {
        return Ok(None);
    }
    let quoted = sprig_streams::read::quote_columns(sql, &lex);
    let Ok(mut parsed) = Parser::parse_sql(&MySqlDialect {}, &quoted) else {
        return Ok(None);
    };
    if parsed.len() != 1 || !matches!(parsed[0], Statement::Query(_)) {
        return Ok(None);
    }
    let stmt = parsed.remove(0);
    let Some(tables) = streams_read(eng, host, &stmt)? else {
        return Ok(None);
    };
    let catalog = StreamCatalog {
        current: host.current_db(),
        tables: tables.clone(),
    };
    let storage: Arc<dyn Storage> = Arc::new(StreamStorage {
        eng: Arc::clone(eng),
        tenant,
        tables,
    });
    let mut session = Session::new(persona);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    session.now_seconds = i64::try_from(now.as_secs()).unwrap_or(0);
    session.now_micros = now.subsec_micros();
    session.fold_constants = true;
    let ran = (|| -> std::result::Result<Answer, SqlError> {
        let mut planner = Planner::new(&catalog, storage, &session);
        planner.options = PlanOptions {
            keep_filters: true,
            memory_limit: Some(MEMORY_LIMIT),
            ..PlanOptions::for_persona(persona)
        };
        let mut plan = planner.plan(&stmt, &[])?;
        let mut ctx = Ctx::new(&session);
        let defs: Vec<ColumnDef> = plan
            .columns
            .iter()
            .map(|c| column_def(&c.name, &c.ty, persona, charset))
            .collect();
        let tys: Vec<ExprType> = plan.columns.iter().map(|c| c.ty).collect();
        let mut rows = Vec::new();
        let mut each = || -> std::result::Result<(), SqlError> {
            while let Some(r) = plan.source.next(&mut ctx)? {
                rows.push(
                    r.iter()
                        .zip(&tys)
                        .map(|(v, ty)| sprig_exec::value_text(v, ty, persona).map(|b| b.to_vec()))
                        .collect(),
                );
            }
            Ok(())
        };
        if tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
        {
            tokio::task::block_in_place(each)?;
        } else {
            each()?;
        }
        Ok((defs, rows))
    })();
    ran.map(Some).map_err(sql_error)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn queries_are_told_apart() {
        assert!(looks_like_query("  select 1"));
        assert!(looks_like_query(
            "/* x */ WITH a AS (SELECT 1) SELECT * FROM a"
        ));
        assert!(looks_like_query("-- c\n(SELECT 1)"));
        assert!(!looks_like_query("INSERT INTO s VALUES (1)"));
        assert!(!looks_like_query("SHOW STREAMS"));
    }

    #[test]
    fn dates_from_milliseconds() {
        let d = datetime_of(0);
        assert_eq!(
            (d.date.year, d.date.month, d.date.day, d.hour),
            (1970, 1, 1, 0)
        );
        // 2026-11-21 16:45:30.123 UTC.
        let d = datetime_of(1_795_279_530_123);
        assert_eq!(
            (
                d.date.year,
                d.date.month,
                d.date.day,
                d.hour,
                d.minute,
                d.second,
                d.micros
            ),
            (2026, 11, 21, 16, 45, 30, 123_000)
        );
        // 2000-02-29.
        let d = datetime_of(951_782_400_000);
        assert_eq!((d.date.year, d.date.month, d.date.day), (2000, 2, 29));
    }

    #[test]
    fn key_ranges_become_partition_spans() {
        let b = |_p: u32| (10i64, 100i64);
        let u = |v: u64| Value::UInt(v);
        // Everything.
        let all = spans_of(&KeyRange::all(), 3, &b);
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|s| (s.from, s.to) == (10, 100)));
        // partition_no = 1 AND offset_no >= 40 AND offset_no < 50.
        let r = KeyRange {
            start: Bound::Included(vec![u(1), u(40)]),
            end: Bound::Excluded(vec![u(1), u(50)]),
        };
        assert_eq!(
            spans_of(&r, 3, &b),
            vec![Span {
                partition: 1,
                from: 40,
                to: 50
            }]
        );
        // partition_no > 0: partitions 1 and 2.
        let r = KeyRange {
            start: Bound::Excluded(vec![u(0)]),
            end: Bound::Unbounded,
        };
        assert_eq!(
            spans_of(&r, 3, &b)
                .iter()
                .map(|s| s.partition)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        // Below the log start or past the end: clamped; nothing past the end.
        let r = KeyRange::point(vec![u(2), u(5)]);
        assert!(spans_of(&r, 3, &b).is_empty());
        let r = KeyRange::point(vec![u(7)]);
        assert!(spans_of(&r, 3, &b).is_empty());
    }
}
