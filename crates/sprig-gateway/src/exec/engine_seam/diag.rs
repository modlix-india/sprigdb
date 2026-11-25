//! The session's diagnostics area (`SHOW WARNINGS`, `SHOW ERRORS`,
//! `@@warning_count`, `@@error_count`) for statements the gateway answers
//! itself, engine statements first of all.
//!
//! A statement that never reached a backend connection left nothing
//! there, so its conditions are kept here and the next diagnostic
//! statement is answered from them. Sending it to a backend connection
//! instead would show whatever that connection ran last, possibly for
//! another session.
//!
//! The area as a statement leaves it:
//!
//! ```text
//! the statement set the gateway's list (engine conditions, a gateway
//!   error, a DDL note)                          → that list
//! it ran on a backend connection and raised
//!   conditions there                            → the backend's (forwarded)
//! it failed on a backend connection             → its error
//! otherwise, it raised nothing:
//!   SHOW WARNINGS / ERRORS / COUNT(*)           → unchanged (both servers)
//!   MariaDB: SET, USE, SELECT or DO without
//!   tables                                      → unchanged
//!   anything else                               → empty
//! ```
//!
//! MySQL 8.4 clears the area with every other statement, also with
//! `SELECT @@warning_count` (which reads it first); MariaDB 11.8 keeps it
//! through statements that use no table and raise nothing.

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};
use sprig_sql::lex::{LexOptions, Token, TokenKind, lex};
use sprig_sql::names::{LocalAnswer, ShowKind, StmtKind, StmtPlan};

use super::super::PassSession;
use super::super::tenant::{Step, send_local};
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};
use crate::tenant::session::LocalWarning;

/// The most conditions the area lists (`max_error_count`'s default); the
/// counts include the ones beyond it.
pub(in crate::exec) const MAX_KEPT: usize = 64;

/// What a statement that raised nothing does to the area.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::exec) enum Keep {
    /// It empties it.
    #[default]
    No,
    /// MariaDB keeps it (no table used); MySQL empties it.
    IfMaria,
    /// Both keep it (a diagnostic statement).
    Always,
}

/// The session's diagnostics bookkeeping between statements.
#[derive(Debug, Default)]
pub(in crate::exec) struct DiagState {
    /// The session ran a statement since login or a reset.
    started: bool,
    /// Conditions of the area beyond its kept list.
    extra: usize,
    /// The area the running statement found, and its extra count.
    found: Option<Vec<LocalWarning>>,
    found_extra: usize,
    /// What the running statement does to the area when it raises nothing.
    keep: Keep,
    /// The running statement raised conditions on a backend connection:
    /// a diagnostic statement goes there.
    backend: bool,
}

impl DiagState {
    /// A login or reset: the next statement finds an empty area.
    pub(in crate::exec) fn restart(&mut self) {
        *self = Self::default();
    }

    /// A statement starts; `cur` is what the session's list holds
    /// (`Some` when the last statement set it). The area it finds.
    fn begin(&mut self, cur: Option<Vec<LocalWarning>>, maria: bool) -> Option<Vec<LocalWarning>> {
        let mut cur = cur;
        let mut extra = self.extra;
        if !self.started {
            self.started = true;
            if cur.is_none() {
                cur = Some(Vec::new());
                extra = 0;
            }
        } else if cur.is_none() && !self.backend {
            // The last statement raised nothing.
            let kept = match self.keep {
                Keep::Always => true,
                Keep::IfMaria => maria,
                Keep::No => false,
            };
            if kept {
                cur = self.found.take();
                extra = self.found_extra;
            } else {
                cur = Some(Vec::new());
                extra = 0;
            }
        }
        if cur.is_none() {
            extra = 0;
        }
        self.backend = false;
        self.keep = Keep::No;
        self.found.clone_from(&cur);
        self.found_extra = extra;
        self.extra = extra;
        cur
    }

    /// A statement that bypasses the tenant pipeline (the statement
    /// shortcut, a prepared execute) starts.
    pub(in crate::exec) fn bypass(&mut self, backend: bool) {
        self.started = true;
        self.keep = Keep::No;
        self.backend = backend;
        self.extra = 0;
    }

    /// What the running statement does to the area if it raises nothing.
    pub(in crate::exec) fn set_keep(&mut self, keep: Keep) {
        self.keep = keep;
    }

    /// The running statement raised conditions on a backend connection.
    pub(in crate::exec) fn on_backend(&mut self) {
        self.backend = true;
    }
}

/// A condition the area lists.
pub(in crate::exec) fn warning(level: &'static str, code: u16, message: String) -> LocalWarning {
    LocalWarning {
        level,
        code,
        message,
    }
}

/// A failed statement's area: its error, in the port's wording (as the
/// client got it).
pub(in crate::exec) fn error_area(e: &SprigError, persona: Persona) -> Vec<LocalWarning> {
    let mapped = sprig_sql::compat::errmap::for_persona(persona, e);
    let e = mapped.as_ref().unwrap_or(e);
    vec![warning(
        "Error",
        e.protocol().mysql_errno,
        e.message.clone(),
    )]
}

/// `items` as a kept list and the count beyond it.
pub(in crate::exec) fn kept<I: IntoIterator<Item = LocalWarning>>(
    items: I,
) -> (Vec<LocalWarning>, usize) {
    let mut v = Vec::new();
    let mut extra = 0;
    for w in items {
        if v.len() < MAX_KEPT {
            v.push(w);
        } else {
            extra += 1;
        }
    }
    (v, extra)
}

/// A diagnostic `SHOW`, read from the statement's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) struct DiagShow {
    /// `SHOW ERRORS` (errors only).
    pub errors: bool,
    /// `SHOW COUNT(*) WARNINGS | ERRORS`.
    pub count: bool,
    /// `LIMIT [offset,] row_count`.
    pub limit: Option<(u64, u64)>,
}

fn tokens(text: &[u8]) -> Option<Vec<Token>> {
    Some(
        lex(text, &LexOptions::mariadb())
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect(),
    )
}

fn is_word(t: Option<&Token>, text: &[u8], w: &str) -> bool {
    t.is_some_and(|t| t.kind == TokenKind::Word && t.bytes(text).eq_ignore_ascii_case(w.as_bytes()))
}

fn is_punct(t: Option<&Token>, text: &[u8], p: &[u8]) -> bool {
    t.is_some_and(|t| matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(text) == p)
}

fn number(t: Option<&Token>, text: &[u8]) -> Option<u64> {
    let t = t.filter(|t| t.kind == TokenKind::Number)?;
    std::str::from_utf8(t.bytes(text)).ok()?.parse().ok()
}

/// The tokens without a final `;`.
fn statement_tokens(text: &[u8]) -> Option<Vec<Token>> {
    let mut t = tokens(text)?;
    if is_punct(t.last(), text, b";") {
        t.pop();
    }
    Some(t)
}

/// `SHOW [COUNT(*)] WARNINGS | ERRORS [LIMIT [offset,] n]`.
pub(in crate::exec) fn parse_show(text: &[u8]) -> Option<DiagShow> {
    let t = statement_tokens(text)?;
    if !is_word(t.first(), text, "SHOW") {
        return None;
    }
    let mut j = 1;
    let mut count = false;
    if is_word(t.get(1), text, "COUNT") {
        if !(is_punct(t.get(2), text, b"(")
            && is_punct(t.get(3), text, b"*")
            && is_punct(t.get(4), text, b")"))
        {
            return None;
        }
        count = true;
        j = 5;
    }
    let errors = if is_word(t.get(j), text, "WARNINGS") {
        false
    } else if is_word(t.get(j), text, "ERRORS") {
        true
    } else {
        return None;
    };
    j += 1;
    let mut limit = None;
    if !count && is_word(t.get(j), text, "LIMIT") {
        let a = number(t.get(j + 1), text)?;
        if is_punct(t.get(j + 2), text, b",") {
            let b = number(t.get(j + 3), text)?;
            limit = Some((a, b));
            j += 4;
        } else {
            limit = Some((0, a));
            j += 2;
        }
    }
    (j == t.len()).then_some(DiagShow {
        errors,
        count,
        limit,
    })
}

/// Which count a select item reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum Count {
    Warnings,
    Errors,
}

/// `SELECT @@[session.]warning_count | error_count [[AS] alias], ...`:
/// each item's column name and count. `None` for any other statement.
pub(in crate::exec) fn parse_count_select(text: &[u8]) -> Option<Vec<(String, Count)>> {
    let t = statement_tokens(text)?;
    if !is_word(t.first(), text, "SELECT") {
        return None;
    }
    let mut out = Vec::new();
    let mut j = 1;
    loop {
        let v = t.get(j).filter(|v| v.kind == TokenKind::SysVar)?;
        let raw = v.bytes(text);
        let name = std::str::from_utf8(raw).ok()?;
        let bare = name.trim_start_matches('@');
        let bare = bare
            .split_once('.')
            .map_or(bare, |(scope, n)| {
                if scope.eq_ignore_ascii_case("session") || scope.eq_ignore_ascii_case("local") {
                    n
                } else {
                    // GLOBAL: not a session count.
                    ""
                }
            })
            .trim_matches('`');
        let count = if bare.eq_ignore_ascii_case("warning_count") {
            Count::Warnings
        } else if bare.eq_ignore_ascii_case("error_count") {
            Count::Errors
        } else {
            return None;
        };
        j += 1;
        let mut col = name.to_owned();
        if is_word(t.get(j), text, "AS") {
            j += 1;
        }
        if let Some(a) = t.get(j).filter(|a| {
            (a.kind.is_ident_like() && !is_word(Some(a), text, "FROM"))
                || a.kind == TokenKind::String
        }) {
            let b = a.bytes(text);
            col = match a.kind {
                TokenKind::String | TokenKind::QuotedIdent if b.len() >= 2 => {
                    String::from_utf8_lossy(&b[1..b.len() - 1]).into_owned()
                }
                _ => String::from_utf8_lossy(b).into_owned(),
            };
            j += 1;
        }
        out.push((col, count));
        if j == t.len() {
            return Some(out);
        }
        if !is_punct(t.get(j), text, b",") {
            return None;
        }
        j += 1;
    }
}

/// Where an unquoted word starting with `$$` begins, if one does.
fn dollar_quote(text: &[u8]) -> Option<usize> {
    tokens(text)?
        .iter()
        .find(|t| t.kind == TokenKind::Word && t.bytes(text).starts_with(b"$$"))
        .map(|t| t.start as usize)
}

/// Whether a statement uses no table: `SET`, `USE`, and `SELECT` or `DO`
/// with no `FROM` other than `FROM DUAL` (MariaDB keeps the area through
/// these when they raise nothing).
pub(in crate::exec) fn uses_no_table(text: &[u8]) -> bool {
    let Some(t) = tokens(text) else {
        return false;
    };
    if is_word(t.first(), text, "SET") || is_word(t.first(), text, "USE") {
        // `SET` of a variable from a subquery reads a table.
        return !t.iter().any(|x| is_word(Some(x), text, "FROM"));
    }
    if !(is_word(t.first(), text, "SELECT") || is_word(t.first(), text, "DO")) {
        return false;
    }
    !t.iter().enumerate().any(|(i, x)| {
        (is_word(Some(x), text, "FROM") && !is_word(t.get(i + 1), text, "DUAL"))
            || is_word(Some(x), text, "INTO")
    })
}

/// The `SHOW WARNINGS` columns, as each server reports them.
fn warning_columns(persona: Persona, charset: u16, mbmax: u32) -> Vec<ColumnDef> {
    let mysql = persona == Persona::MySql84;
    let text = |name: &str, chars: u32| ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset,
        length: chars * mbmax,
        col_type: column_type::VAR_STRING,
        flags: ColumnFlags::NOT_NULL.bits(),
        decimals: if mysql { 31 } else { 39 },
        ..ColumnDef::default()
    };
    vec![
        text("Level", 7),
        ColumnDef {
            name: Bytes::from_static(b"Code"),
            charset: 63,
            length: if mysql { 5 } else { 4 },
            col_type: column_type::LONG,
            flags: (ColumnFlags::NOT_NULL | ColumnFlags::UNSIGNED | ColumnFlags::BINARY).bits(),
            ..ColumnDef::default()
        },
        text("Message", 512),
    ]
}

/// A count column (`@@warning_count` and the like).
fn count_column(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 63,
        length: 21,
        col_type: column_type::LONGLONG,
        flags: (ColumnFlags::UNSIGNED | ColumnFlags::BINARY).bits(),
        ..ColumnDef::default()
    }
}

/// How many conditions `area` holds (with `extra` beyond its list), or
/// how many errors.
pub(in crate::exec) fn count_of(area: &[LocalWarning], extra: usize, which: Count) -> usize {
    match which {
        Count::Warnings => area.len() + extra,
        Count::Errors => area.iter().filter(|w| w.level == "Error").count(),
    }
}

/// The answer to a diagnostic `SHOW` from the area.
pub(in crate::exec) fn show_answer(
    area: &[LocalWarning],
    extra: usize,
    show: DiagShow,
    persona: Persona,
    charset: u16,
    mbmax: u32,
) -> LocalAnswer {
    let which = if show.errors {
        Count::Errors
    } else {
        Count::Warnings
    };
    if show.count {
        let name = if show.errors {
            "@@session.error_count"
        } else {
            "@@session.warning_count"
        };
        return LocalAnswer {
            columns: vec![count_column(name)],
            rows: vec![vec![Some(Bytes::from(
                count_of(area, extra, which).to_string(),
            ))]],
        };
    }
    let (skip, take) = show.limit.unwrap_or((0, u64::MAX));
    let skip = usize::try_from(skip).unwrap_or(usize::MAX);
    let take = usize::try_from(take).unwrap_or(usize::MAX);
    LocalAnswer {
        columns: warning_columns(persona, charset, mbmax),
        rows: area
            .iter()
            .filter(|w| !show.errors || w.level == "Error")
            .skip(skip)
            .take(take)
            .map(|w| {
                vec![
                    Some(Bytes::from_static(w.level.as_bytes())),
                    Some(Bytes::from(w.code.to_string())),
                    Some(Bytes::from(w.message.clone())),
                ]
            })
            .collect(),
    }
}

/// The answer to a `SELECT` of the counts.
pub(in crate::exec) fn count_answer(
    area: &[LocalWarning],
    extra: usize,
    items: &[(String, Count)],
) -> LocalAnswer {
    LocalAnswer {
        columns: items.iter().map(|(n, _)| count_column(n)).collect(),
        rows: vec![
            items
                .iter()
                .map(|(_, c)| Some(Bytes::from(count_of(area, extra, *c).to_string())))
                .collect(),
        ],
    }
}

impl PassSession {
    /// The area as the running statement finds it, the previous
    /// statement's effect applied. `None`: the area is on the backend
    /// connection (a diagnostic statement is forwarded).
    pub(in crate::exec) fn diag_begin(&mut self) -> Option<Vec<LocalWarning>> {
        let maria = self.persona != Persona::MySql84;
        let ts = self.tenant.as_deref_mut()?;
        Some(self.eng.diag.begin(ts.warnings.take(), maria)).flatten()
    }

    /// A failed statement's area: its error as the client got it (the
    /// port's wording, MySQL's `'table.key'` in a 1062).
    pub(in crate::exec) fn diag_error(&self, e: &SprigError) -> Vec<LocalWarning> {
        let named = self
            .eng
            .dup_table
            .as_deref()
            .and_then(|t| sprig_sql::compat::errmap::with_dup_key_table(e, t));
        error_area(named.as_ref().unwrap_or(e), self.persona)
    }

    /// The running statement left `area` (its own conditions, or the area
    /// it found).
    pub(in crate::exec) fn diag_set(&mut self, area: Vec<LocalWarning>, extra: usize) {
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.warnings = Some(area);
            self.eng.diag.extra = extra;
        }
    }

    /// The area the running statement found (a diagnostic statement
    /// answered here leaves it as it was).
    pub(in crate::exec) fn diag_restore(&mut self, area: Option<Vec<LocalWarning>>) {
        let extra = self.eng.diag.found_extra;
        match area {
            Some(a) => self.diag_set(a, extra),
            None => self.eng.diag.keep = Keep::Always,
        }
    }
}

impl PassSession {
    /// A diagnostic statement answered from the gateway's area
    /// (`Some`), or the running statement's effect on the area noted
    /// (`None`: it runs as usual).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::exec) async fn diag_statement(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sp: &StmtPlan,
        original: &[u8],
        previous: Option<Vec<LocalWarning>>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        // MySQL 8.4 reads an unquoted `$$` as the start of a dollar-quoted
        // string, which these statements can't hold: a syntax error (the
        // MySQL 9 client sends `select $$` at login to find out).
        if self.persona == Persona::MySql84
            && matches!(
                sp.info.kind,
                StmtKind::Select
                    | StmtKind::Insert
                    | StmtKind::Replace
                    | StmtKind::Update
                    | StmtKind::Delete
                    | StmtKind::Set
            )
            && let Some(at) = dollar_quote(original)
        {
            let near = String::from_utf8_lossy(&original[at..]);
            let e = SprigError::standard(
                1064,
                *b"42000",
                format!(
                    "You have an error in your SQL syntax; check the manual that corresponds to \
                     your MySQL server version for the right syntax to use near '{}' at line 1",
                    near.chars().take(80).collect::<String>()
                ),
            );
            return self.fail(out, &e).await.map(Some);
        }
        let extra = self.eng.diag.found_extra;
        let shows = sp
            .show
            .as_ref()
            .is_some_and(|s| matches!(s.kind, ShowKind::Warnings | ShowKind::Errors));
        if shows {
            if let (Some(area), Some(show)) = (&previous, parse_show(original)) {
                let (charset, mbmax) = self.diag_charset(cx);
                let ans = show_answer(area, extra, show, self.persona, charset, mbmax);
                self.diag_restore(previous);
                return self.diag_send(&ans, more, format, out).await.map(Some);
            }
            // The backend connection holds the area: it answers, and the
            // area stays where it is.
            self.eng.diag.set_keep(Keep::Always);
            return Ok(None);
        }
        if let (Some(area), Some(items)) = (&previous, parse_count_select(original)) {
            let ans = count_answer(area, extra, &items);
            // MySQL empties the area once the counts are read; MariaDB
            // keeps it (no table used).
            if self.persona == Persona::MySql84 {
                self.diag_set(Vec::new(), 0);
            } else {
                self.diag_restore(previous);
            }
            return self.diag_send(&ans, more, format, out).await.map(Some);
        }
        if uses_no_table(original) {
            self.eng.diag.set_keep(Keep::IfMaria);
        }
        Ok(None)
    }

    /// The results character set (its id and bytes per character) of
    /// the area's text columns.
    fn diag_charset(&mut self, cx: &ExecCtx<'_>) -> (u16, u32) {
        let sv = self.engine_vars(cx);
        sv.results
            .cs
            .map_or((cx.proto.collation, 4), |c| (c.id, c.mbmax))
    }

    /// Sends a local answer with the session's status.
    async fn diag_send(
        &mut self,
        ans: &LocalAnswer,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let mut s = self.status() | self.engine_txn_flags();
        if more {
            s |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        send_local(out, ans, format, s).await?;
        Ok(Step {
            errored: false,
            status: Some(s),
        })
    }
}

/// What a client statement answered (see [`CountSink`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec) enum Answered {
    /// Nothing seen (an answer sent some other way).
    Nothing,
    /// An OK packet with these affected rows.
    Ok(u64),
    /// A result set of this many rows.
    Rows(u64),
    /// An error.
    Error,
}

/// A sink that notes what a client statement answered (the last answer of
/// it), for the session's `ROW_COUNT()` and `FOUND_ROWS()`, and gives an
/// OK packet the value the statement's `LAST_INSERT_ID(expr)` set.
pub(in crate::exec) struct CountSink<'a> {
    inner: &'a mut dyn ResultSink,
    conn: u64,
    /// What it answered.
    pub got: Answered,
    rows: u64,
    /// The value `LAST_INSERT_ID(expr)` set, once taken.
    pub set_id: Option<u64>,
}

impl<'a> CountSink<'a> {
    /// A sink over `inner` for connection `conn`.
    pub(in crate::exec) fn new(inner: &'a mut dyn ResultSink, conn: u64) -> Self {
        Self {
            inner,
            conn,
            got: Answered::Nothing,
            rows: 0,
            set_id: None,
        }
    }

    /// What the statement answered and the `LAST_INSERT_ID(expr)` value it
    /// set, once it has ended.
    pub(in crate::exec) fn finish(mut self) -> (Answered, Option<u64>) {
        if let Some(v) = sprig_exec::take_set_insert_id(self.conn) {
            self.set_id = Some(v);
        }
        (self.got, self.set_id)
    }
}

#[async_trait]
impl ResultSink for CountSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.rows = 0;
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.rows = 0;
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.got = Answered::Rows(0);
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.rows += 1;
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.rows += 1;
        self.inner.raw_row(payload).await
    }

    fn try_raw_row(&mut self, payload: Bytes) -> std::result::Result<Result<()>, Bytes> {
        let r = self.inner.try_raw_row(payload);
        if r.is_ok() {
            self.rows += 1;
        }
        r
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.got = Answered::Rows(self.rows);
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, mut ok: OkPacket) -> Result<()> {
        self.got = Answered::Ok(ok.affected_rows);
        if let Some(v) = sprig_exec::take_set_insert_id(self.conn) {
            ok.last_insert_id = v;
            self.set_id = Some(v);
        }
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.got = Answered::Error;
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

/// A sink that notes what a statement relayed from a backend connection
/// raised: its warning count and its error.
pub(in crate::exec) struct DiagSink<'a> {
    inner: &'a mut dyn ResultSink,
    /// The largest warning count an OK or end-of-rows packet carried.
    pub warnings: u16,
    /// The error sent, as the client saw it.
    pub error: Option<SprigError>,
}

impl<'a> DiagSink<'a> {
    /// A sink over `inner`.
    pub(in crate::exec) fn new(inner: &'a mut dyn ResultSink) -> Self {
        Self {
            inner,
            warnings: 0,
            error: None,
        }
    }
}

#[async_trait]
impl ResultSink for DiagSink<'_> {
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
        self.warnings = self.warnings.max(end.warnings);
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
        self.warnings = self.warnings.max(end.warnings);
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.warnings = self.warnings.max(ok.warnings);
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
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
mod tests {
    use super::*;

    #[test]
    fn diagnostic_shows() {
        let p = |s: &str| parse_show(s.as_bytes());
        assert_eq!(
            p("SHOW WARNINGS"),
            Some(DiagShow {
                errors: false,
                count: false,
                limit: None
            })
        );
        assert_eq!(p("show errors limit 2;").unwrap().limit, Some((0, 2)));
        assert_eq!(p("SHOW WARNINGS LIMIT 1, 3").unwrap().limit, Some((1, 3)));
        let c = p("SHOW COUNT(*) ERRORS").unwrap();
        assert!(c.count && c.errors);
        assert_eq!(p("SHOW WARNINGS x"), None);
        assert_eq!(p("SHOW TABLES"), None);
    }

    #[test]
    fn count_selects() {
        let p = |s: &str| parse_count_select(s.as_bytes());
        assert_eq!(
            p("SELECT @@warning_count, @@SESSION.error_count AS e, @@WARNING_COUNT"),
            Some(vec![
                ("@@warning_count".to_owned(), Count::Warnings),
                ("e".to_owned(), Count::Errors),
                ("@@WARNING_COUNT".to_owned(), Count::Warnings),
            ])
        );
        assert_eq!(
            p("select @@error_count w"),
            Some(vec![("w".to_owned(), Count::Errors)])
        );
        assert_eq!(p("SELECT @@warning_count + 1"), None);
        assert_eq!(p("SELECT @@warning_count FROM t"), None);
        assert_eq!(p("SELECT @@global.warning_count"), None);
        assert_eq!(p("SELECT 1"), None);
    }

    #[test]
    fn dollar_quotes() {
        assert_eq!(dollar_quote(b"select $$"), Some(7));
        assert_eq!(dollar_quote(b"select $$ab$$"), Some(7));
        assert_eq!(dollar_quote(b"select `$$`, '$$', a$$"), None);
    }

    #[test]
    fn statements_without_tables() {
        for s in [
            "SELECT 1",
            "SET @a = 1",
            "DO 1",
            "SELECT 1 FROM DUAL",
            "USE d",
        ] {
            assert!(uses_no_table(s.as_bytes()), "{s}");
        }
        for s in [
            "SELECT * FROM t",
            "SET @a = (SELECT 1 FROM t)",
            "INSERT INTO t VALUES (1)",
            "SELECT 1 INTO @a",
        ] {
            assert!(!uses_no_table(s.as_bytes()), "{s}");
        }
    }

    #[test]
    fn answers() {
        let area = vec![
            warning("Warning", 1265, "a".into()),
            warning("Error", 1062, "b".into()),
        ];
        let show = |t: &str| parse_show(t.as_bytes()).unwrap();
        let a = show_answer(
            &area,
            0,
            show("SHOW WARNINGS LIMIT 1, 1"),
            Persona::MariaDb,
            45,
            4,
        );
        assert_eq!(a.rows.len(), 1);
        assert_eq!(a.rows[0][1].as_deref(), Some(&b"1062"[..]));
        let a = show_answer(
            &area,
            3,
            show("SHOW COUNT(*) WARNINGS"),
            Persona::MySql84,
            45,
            4,
        );
        assert_eq!(a.rows[0][0].as_deref(), Some(&b"5"[..]));
        assert_eq!(&a.columns[0].name[..], b"@@session.warning_count");
        let a = show_answer(&area, 0, show("SHOW ERRORS"), Persona::MySql84, 45, 4);
        assert_eq!(a.rows.len(), 1);
        assert_eq!(a.columns[1].length, 5);
        let a = count_answer(&area, 0, &[("e".into(), Count::Errors)]);
        assert_eq!(a.rows[0][0].as_deref(), Some(&b"1"[..]));
    }

    /// The area across statements, as each server keeps it.
    #[test]
    fn the_area_between_statements() {
        let w = |c| vec![warning("Warning", c, String::new())];
        for maria in [true, false] {
            let mut d = DiagState::default();
            // A new session finds an empty area, never a backend's.
            assert_eq!(d.begin(None, maria), Some(vec![]));
            // An engine INSERT left two warnings ...
            let found = d.begin(Some(w(1265)), maria);
            assert_eq!(found, Some(w(1265)));
            // ... a SHOW WARNINGS answered here keeps them ...
            d.set_keep(Keep::Always);
            assert_eq!(d.begin(None, maria), Some(w(1265)));
            // ... `SELECT 1` keeps them on MariaDB only ...
            d.set_keep(Keep::IfMaria);
            let after = d.begin(None, maria);
            assert_eq!(after, if maria { Some(w(1265)) } else { Some(vec![]) });
            // ... and a statement on a table empties the area.
            assert_eq!(d.begin(None, maria), Some(vec![]));
            // Conditions raised on a backend connection stay there.
            d.on_backend();
            assert_eq!(d.begin(None, maria), None);
            d.set_keep(Keep::Always);
            assert_eq!(d.begin(None, maria), None);
            // A reset starts again from an empty area.
            d.restart();
            assert_eq!(d.begin(None, maria), Some(vec![]));
        }
    }

    /// `SHOW WARNINGS` and `SHOW COUNT(*) WARNINGS` through a client
    /// connection's wire: the definitions are the ones each server sends
    /// (observed on MariaDB 11.8 and MySQL 8.4 for the same statements).
    #[tokio::test]
    async fn answers_over_the_wire() {
        use futures::StreamExt;
        use sprig_proto_mysql::{Caps, PacketCodec};
        use tokio_util::codec::Framed;

        let caps =
            Caps::PROTOCOL_41 | Caps::SECURE_CONNECTION | Caps::TRANSACTIONS | Caps::DEPRECATE_EOF;
        let area = vec![warning(
            "Warning",
            1292,
            "Truncated incorrect INTEGER value: 'x'".into(),
        )];
        let hex = |b: &[u8]| {
            use std::fmt::Write as _;
            b.iter().fold(String::new(), |mut s, x| {
                let _ = write!(s, "{x:02x}");
                s
            })
        };
        let cases = [
            (
                Persona::MariaDb,
                224,
                "SHOW WARNINGS",
                vec![
                    "03646566000000054c6576656c000ce0001c000000fd0100270000",
                    "0364656600000004436f6465000c3f000400000003a100000000",
                    "03646566000000074d657373616765000ce00000080000fd0100270000",
                ],
            ),
            (
                Persona::MySql84,
                45,
                "SHOW WARNINGS",
                vec![
                    "03646566000000054c6576656c000c2d001c000000fd01001f0000",
                    "0364656600000004436f6465000c3f000500000003a100000000",
                    "03646566000000074d657373616765000c2d0000080000fd01001f0000",
                ],
            ),
            (
                Persona::MySql84,
                45,
                "SHOW COUNT(*) WARNINGS",
                vec![
                    "0364656600000017404073657373696f6e2e7761726e696e675f636f756e74000c3f001500000008a000000000",
                ],
            ),
        ];
        for (persona, charset, sql, defs) in cases {
            let ans = show_answer(
                &area,
                0,
                parse_show(sql.as_bytes()).unwrap(),
                persona,
                charset,
                4,
            );
            let (srv, cli) = tokio::io::duplex(1 << 16);
            let mut framed = Framed::new(srv, PacketCodec::new(1 << 24));
            {
                let mut sink = crate::mysql::sink::WireSink::new(&mut framed, persona, caps);
                send_local(&mut sink, &ans, RowFormat::Text, StatusFlags::AUTOCOMMIT)
                    .await
                    .unwrap();
            }
            crate::mysql::sink::flush(&mut framed).await.unwrap();
            drop(framed);
            let mut rd = Framed::new(cli, PacketCodec::new(1 << 24));
            let mut got = Vec::new();
            while let Some(p) = rd.next().await {
                got.push(p.unwrap().payload);
            }
            assert_eq!(got[0][..], [u8::try_from(defs.len()).unwrap()], "{sql}");
            for (g, want) in got[1..=defs.len()].iter().zip(&defs) {
                assert_eq!(hex(g), *want, "{persona:?} {sql}");
            }
            assert_eq!(got.len(), 3 + defs.len(), "{persona:?} {sql}");
        }
    }
}
