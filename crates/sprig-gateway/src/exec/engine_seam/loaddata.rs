//! `LOAD DATA LOCAL INFILE` into an engine table.
//!
//! The client sends the file (the protocol's `LOCAL INFILE` request); the
//! gateway reads its records as each server does (`FIELDS TERMINATED BY`,
//! `[OPTIONALLY] ENCLOSED BY`, `ESCAPED BY`, `LINES STARTING BY` /
//! `TERMINATED BY`, `IGNORE n LINES`, a column and variable list, `SET`)
//! and inserts them in batches as `INSERT IGNORE` (or `REPLACE`) statements
//! of the client's, through every layer a client statement goes through.
//! `LOCAL` makes data errors and duplicate keys warnings on both servers,
//! which `IGNORE` gives. The batches run in one transaction when the
//! session has none open. The answer is the servers' OK: `Records: n
//! Deleted: n  Skipped: n  Warnings: n`, with 1261 and 1262 for records
//! with too few or too many fields.
//!
//! A file on the gateway's own machine (`LOAD DATA INFILE` without
//! `LOCAL`) is refused with the servers' `--secure-file-priv` error.

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{Persona, StatusFlags};
use sprig_sql::lex::{self, LexOptions, Token, TokenKind};

use super::super::PassSession;
use super::super::tenant::Step;
use super::super::{local_infile_disabled, split};
use super::diag;
use super::sqlprep::{ident, near, string_value};
use crate::pipeline::{ExecCtx, ResultSink};
use crate::tenant::session::LocalWarning;

/// Records per `INSERT`.
const BATCH: usize = 500;

/// What a duplicate key does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dup {
    /// `IGNORE` (also `LOCAL`'s default): skipped, with a warning.
    Ignore,
    /// `REPLACE`: the old row goes.
    Replace,
}

/// One entry of the column list.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    /// A column, as written.
    Column(String),
    /// A user variable (lower case), for `SET`.
    Var(String),
}

/// How the file's records are written.
#[derive(Debug, Clone)]
struct Format {
    field_term: Vec<u8>,
    enclosed: Option<u8>,
    escaped: Option<u8>,
    line_start: Vec<u8>,
    line_term: Vec<u8>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            field_term: b"\t".to_vec(),
            enclosed: None,
            escaped: Some(b'\\'),
            line_start: Vec::new(),
            line_term: b"\n".to_vec(),
        }
    }
}

/// A `LOAD DATA` statement as read.
#[derive(Debug)]
struct Load {
    local: bool,
    dup: Dup,
    /// The table as written (`t` or `db.t`).
    table: String,
    binary: bool,
    format: Format,
    ignore_lines: u64,
    targets: Option<Vec<Target>>,
    /// `SET col = expr`: the column and the expression as written.
    sets: Vec<(String, String)>,
}

/// One field: `None` is `NULL`.
type Field = Option<Vec<u8>>;

fn one_byte(s: &str) -> Option<u8> {
    s.as_bytes().first().copied()
}

/// Reads `LOAD DATA ...`; `Ok(None)` for any other statement.
#[allow(clippy::too_many_lines)] // one pass over the statement's clauses
fn read(sql: &[u8], opts: LexOptions, persona: Persona) -> Result<Option<Load>> {
    let Ok(tokens) = lex::lex(sql, &opts) else {
        return Ok(None);
    };
    let sig: Vec<&Token> = tokens.iter().filter(|t| !t.kind.is_trivia()).collect();
    let is = |k: usize, w: &[u8]| {
        sig.get(k)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    let punct = |k: usize, c: &[u8]| {
        sig.get(k)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == c)
    };
    if !(is(0, b"LOAD") && is(1, b"DATA")) {
        return Ok(None);
    }
    let at = |k: usize| sig.get(k).map_or(sql.len(), |t| t.start as usize);
    let bad = |k: usize| near(persona, sql, at(k));
    let text = |k: usize| -> Result<String> {
        sig.get(k)
            .and_then(|t| string_value(t, sql, opts.no_backslash_escapes))
            .ok_or_else(|| bad(k))
    };
    let mut k = 2;
    if is(k, b"LOW_PRIORITY") || is(k, b"CONCURRENT") {
        k += 1;
    }
    let local = is(k, b"LOCAL");
    if local {
        k += 1;
    }
    if !is(k, b"INFILE") {
        return Err(bad(k));
    }
    text(k + 1)?;
    k += 2;
    // `LOCAL` without `REPLACE` is `IGNORE` (without `LOCAL` the statement
    // is refused).
    let mut dup = Dup::Ignore;
    if is(k, b"REPLACE") {
        dup = Dup::Replace;
        k += 1;
    } else if is(k, b"IGNORE") {
        k += 1;
    }
    if !(is(k, b"INTO") && is(k + 1, b"TABLE")) {
        return Err(bad(k));
    }
    k += 2;
    let first = k;
    if sig.get(k).and_then(|t| ident(t, sql)).is_none() {
        return Err(bad(k));
    }
    k += 1;
    if punct(k, b".") {
        if sig.get(k + 1).and_then(|t| ident(t, sql)).is_none() {
            return Err(bad(k + 1));
        }
        k += 2;
    }
    let table = String::from_utf8_lossy(&sql[at(first)..sig[k - 1].end as usize]).into_owned();
    if is(k, b"PARTITION") {
        return Err(crate::engine::not_on_engine(
            "LOAD DATA into named partitions",
        ));
    }
    let mut binary = false;
    if (is(k, b"CHARACTER") && is(k + 1, b"SET")) || is(k, b"CHARSET") {
        k += if is(k, b"CHARSET") { 1 } else { 2 };
        let cs = sig
            .get(k)
            .map(|t| String::from_utf8_lossy(t.bytes(sql)).to_ascii_lowercase())
            .unwrap_or_default();
        let cs = cs.trim_matches(['\'', '"', '`']);
        match cs {
            "binary" => binary = true,
            "utf8mb4" | "utf8" | "utf8mb3" | "ascii" => {}
            _ => {
                return Err(crate::engine::not_on_engine(
                    "LOAD DATA from a file in this character set",
                ));
            }
        }
        k += 1;
    }
    let mut format = Format::default();
    if is(k, b"FIELDS") || is(k, b"COLUMNS") {
        k += 1;
        let mut any = false;
        loop {
            if is(k, b"TERMINATED") && is(k + 1, b"BY") {
                format.field_term = text(k + 2)?.into_bytes();
                k += 3;
            } else if is(k, b"OPTIONALLY") && is(k + 1, b"ENCLOSED") && is(k + 2, b"BY") {
                format.enclosed = one_byte(&text(k + 3)?);
                k += 4;
            } else if is(k, b"ENCLOSED") && is(k + 1, b"BY") {
                format.enclosed = one_byte(&text(k + 2)?);
                k += 3;
            } else if is(k, b"ESCAPED") && is(k + 1, b"BY") {
                format.escaped = one_byte(&text(k + 2)?);
                k += 3;
            } else {
                break;
            }
            any = true;
        }
        if !any {
            return Err(bad(k));
        }
    }
    if is(k, b"LINES") {
        k += 1;
        let mut any = false;
        loop {
            if is(k, b"STARTING") && is(k + 1, b"BY") {
                format.line_start = text(k + 2)?.into_bytes();
                k += 3;
            } else if is(k, b"TERMINATED") && is(k + 1, b"BY") {
                format.line_term = text(k + 2)?.into_bytes();
                k += 3;
            } else {
                break;
            }
            any = true;
        }
        if !any {
            return Err(bad(k));
        }
    }
    if format.field_term.is_empty() && format.enclosed.is_none() {
        return Err(crate::engine::not_on_engine(
            "LOAD DATA with fixed-width fields (empty FIELDS TERMINATED BY)",
        ));
    }
    if format.line_term.is_empty() {
        format.line_term = b"\n".to_vec();
    }
    let mut ignore_lines = 0;
    if is(k, b"IGNORE") {
        let n = sig
            .get(k + 1)
            .filter(|t| t.kind == TokenKind::Number)
            .and_then(|t| std::str::from_utf8(t.bytes(sql)).ok()?.parse::<u64>().ok())
            .ok_or_else(|| bad(k + 1))?;
        if !(is(k + 2, b"LINES") || is(k + 2, b"ROWS")) {
            return Err(bad(k + 2));
        }
        ignore_lines = n;
        k += 3;
    }
    let mut targets = None;
    if punct(k, b"(") {
        let mut list = Vec::new();
        k += 1;
        if punct(k, b")") {
            k += 1;
        } else {
            loop {
                let t = sig.get(k).ok_or_else(|| bad(k))?;
                if t.kind == TokenKind::UserVar {
                    let name = String::from_utf8_lossy(&t.bytes(sql)[1..])
                        .trim_matches(['`', '\'', '"'])
                        .to_ascii_lowercase();
                    list.push(Target::Var(name));
                    k += 1;
                } else {
                    let a = t.start as usize;
                    ident(t, sql).ok_or_else(|| bad(k))?;
                    k += 1;
                    while punct(k, b".") && sig.get(k + 1).and_then(|t| ident(t, sql)).is_some() {
                        k += 2;
                    }
                    let b = sig[k - 1].end as usize;
                    list.push(Target::Column(
                        String::from_utf8_lossy(&sql[a..b]).into_owned(),
                    ));
                }
                if punct(k, b",") {
                    k += 1;
                } else if punct(k, b")") {
                    k += 1;
                    break;
                } else {
                    return Err(bad(k));
                }
            }
        }
        targets = Some(list);
    }
    let mut sets = Vec::new();
    if is(k, b"SET") {
        k += 1;
        loop {
            let Some(c) = sig.get(k).filter(|t| ident(t, sql).is_some()) else {
                return Err(bad(k));
            };
            let col = String::from_utf8_lossy(c.bytes(sql)).into_owned();
            k += 1;
            if !(sig.get(k).is_some_and(|t| {
                matches!(t.kind, TokenKind::Op) && matches!(t.bytes(sql), b"=" | b":=")
            })) {
                return Err(bad(k));
            }
            k += 1;
            let from = k;
            let mut depth = 0i32;
            while let Some(t) = sig.get(k) {
                if t.kind == TokenKind::Punct {
                    match t.bytes(sql) {
                        b"(" => depth += 1,
                        b")" => depth -= 1,
                        b"," | b";" if depth == 0 => break,
                        _ => {}
                    }
                }
                k += 1;
            }
            if k == from {
                return Err(bad(k));
            }
            let expr =
                String::from_utf8_lossy(&sql[at(from)..sig[k - 1].end as usize]).into_owned();
            sets.push((col, expr));
            if punct(k, b",") {
                k += 1;
            } else {
                break;
            }
        }
    }
    if punct(k, b";") {
        k += 1;
    }
    if k < sig.len() {
        return Err(bad(k));
    }
    Ok(Some(Load {
        local,
        dup,
        table,
        binary,
        format,
        ignore_lines,
        targets,
        sets,
    }))
}

/// The records found in `buf` from its start: complete ones only, unless
/// `done` (the file has ended). Returns them and the bytes they used.
fn records(buf: &[u8], f: &Format, done: bool) -> (Vec<Vec<Field>>, usize) {
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        if pos >= buf.len() {
            break;
        }
        // `LINES STARTING BY`: a line without it is skipped.
        let mut i = pos;
        if !f.line_start.is_empty() {
            let Some(s) = find(&buf[pos..], &f.line_start) else {
                if done {
                    pos = buf.len();
                }
                break;
            };
            // The prefix must come before the line's end.
            if let Some(e) = find(&buf[pos..], &f.line_term)
                && e < s
            {
                pos += e + f.line_term.len();
                continue;
            }
            i = pos + s + f.line_start.len();
        }
        match record(buf, i, f, done) {
            Some((fields, next)) => {
                out.push(fields);
                pos = next;
            }
            None => break,
        }
    }
    (out, pos)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// The escaped character `c` stands for.
fn unescape(c: u8) -> u8 {
    match c {
        b'0' => 0,
        b'b' => 0x08,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'Z' => 0x1a,
        other => other,
    }
}

/// One record from `i`: its fields and where the next starts; `None` when
/// the record isn't complete in `buf` (and the file goes on).
fn record(buf: &[u8], mut i: usize, f: &Format, done: bool) -> Option<(Vec<Field>, usize)> {
    let at = |i: usize, s: &[u8]| !s.is_empty() && buf[i..].starts_with(s);
    let mut fields = Vec::new();
    loop {
        let mut v = Vec::new();
        let mut escaped_n = false;
        if let Some(q) = f.enclosed
            && buf.get(i) == Some(&q)
        {
            i += 1;
            loop {
                let c = *buf.get(i)?;
                if Some(c) == f.escaped && i + 1 < buf.len() {
                    v.push(unescape(buf[i + 1]));
                    i += 2;
                } else if Some(c) == f.escaped {
                    if done {
                        v.push(c);
                        i += 1;
                    } else {
                        return None;
                    }
                } else if c == q {
                    if buf.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                    } else if i + 1 >= buf.len()
                        || at(i + 1, &f.field_term)
                        || at(i + 1, &f.line_term)
                    {
                        if i + 1 >= buf.len() && !done {
                            return None;
                        }
                        i += 1;
                        break;
                    } else {
                        v.push(c);
                        i += 1;
                    }
                } else {
                    v.push(c);
                    i += 1;
                }
            }
        } else {
            let start = i;
            while i < buf.len() && !at(i, &f.field_term) && !at(i, &f.line_term) {
                let c = buf[i];
                if Some(c) == f.escaped && i + 1 < buf.len() {
                    if i == start && buf[i + 1] == b'N' {
                        escaped_n = true;
                    }
                    v.push(unescape(buf[i + 1]));
                    i += 2;
                } else {
                    v.push(c);
                    i += 1;
                }
            }
            if i >= buf.len() && !done {
                return None;
            }
            // `\N` alone, or the word NULL unenclosed when fields can be
            // enclosed, is NULL.
            if escaped_n && v == b"N" || f.enclosed.is_some() && v == b"NULL" {
                fields.push(None);
                if !next_field(buf, &mut i, f, done)? {
                    return Some((fields, i));
                }
                continue;
            }
        }
        fields.push(Some(v));
        if !next_field(buf, &mut i, f, done)? {
            return Some((fields, i));
        }
    }
}

/// After a field at `i`: `Some(true)` when another field follows (past
/// the field terminator), `Some(false)` at the record's end (past the line
/// terminator, or the file's end), `None` when more bytes are needed.
fn next_field(buf: &[u8], i: &mut usize, f: &Format, done: bool) -> Option<bool> {
    let at = |i: usize, s: &[u8]| !s.is_empty() && buf[i..].starts_with(s);
    if *i >= buf.len() {
        return if done { Some(false) } else { None };
    }
    if at(*i, &f.line_term) {
        *i += f.line_term.len();
        return Some(false);
    }
    if at(*i, &f.field_term) {
        *i += f.field_term.len();
        return Some(true);
    }
    // After an enclosed field, text up to the next terminator is skipped.
    while *i < buf.len() && !at(*i, &f.field_term) && !at(*i, &f.line_term) {
        *i += 1;
    }
    next_field(buf, i, f, done)
}

/// A field as a literal of the `INSERT`.
fn literal(v: &Field, binary: bool, nbe: bool) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(b) => match std::str::from_utf8(b) {
            Ok(t) if !binary => sprig_sql::names::rewrite::sql_string(t, nbe),
            _ => {
                use std::fmt::Write as _;
                let mut s = String::with_capacity(3 + 2 * b.len());
                s.push_str("X'");
                for x in b {
                    let _ = write!(s, "{x:02X}");
                }
                s.push('\'');
                s
            }
        },
    }
}

/// `expr` with each user variable the record set (`vars`) written in.
fn with_vars(expr: &str, vars: &[(String, String)], opts: LexOptions) -> String {
    let b = expr.as_bytes();
    let Ok(tokens) = lex::lex(b, &opts) else {
        return expr.to_owned();
    };
    let mut out = String::with_capacity(expr.len());
    let mut at = 0;
    for t in &tokens {
        if t.kind != TokenKind::UserVar {
            continue;
        }
        let name = String::from_utf8_lossy(&t.bytes(b)[1..])
            .trim_matches(['`', '\'', '"'])
            .to_ascii_lowercase();
        if let Some((_, lit)) = vars.iter().find(|(n, _)| *n == name) {
            out.push_str(&expr[at..t.start as usize]);
            out.push('(');
            out.push_str(lit);
            out.push(')');
            at = t.end as usize;
        }
    }
    out.push_str(&expr[at..]);
    out
}

/// A warning's "at row N" counted from the file's first record.
fn renumbered(mut w: LocalWarning, offset: u64) -> LocalWarning {
    if let Some(p) = w.message.rfind("at row ") {
        let digits = &w.message[p + 7..];
        let end = digits
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(digits.len());
        if let Ok(n) = digits[..end].parse::<u64>() {
            let rest = digits[end..].to_owned();
            w.message = format!("{}at row {}{rest}", &w.message[..p], n + offset);
        }
    }
    w
}

/// The work of one `LOAD DATA`, as the records arrive.
struct Run {
    load: Load,
    /// The columns each record fills, by position (`None` for a variable).
    columns: Vec<Option<String>>,
    nbe: bool,
    opts: LexOptions,
    /// Lines still to skip.
    skip: u64,
    /// Records read (after the skipped lines).
    records: u64,
    affected: u64,
    warnings: Vec<LocalWarning>,
    pending: Vec<String>,
    /// The first error; the rest of the file is then only read.
    failed: Option<SprigError>,
    /// Data errors fail the statement (`REPLACE` under a strict mode;
    /// `LOCAL` alone is `IGNORE`).
    strict: bool,
}

impl Run {
    /// One record as a row of the `INSERT`, with 1261 / 1262 for a field
    /// count that doesn't fit.
    fn row(&mut self, fields: &[Field]) {
        self.records += 1;
        let n = self.columns.len();
        let odd = match fields.len().cmp(&n) {
            std::cmp::Ordering::Less => Some((
                1261,
                format!("Row {} doesn't contain data for all columns", self.records),
            )),
            std::cmp::Ordering::Greater => Some((
                1262,
                format!(
                    "Row {} was truncated; it contained more data than there were input columns",
                    self.records
                ),
            )),
            std::cmp::Ordering::Equal => None,
        };
        if let Some((code, message)) = odd {
            // `REPLACE` under a strict mode: an error, as on the servers.
            if self.strict {
                self.failed = Some(SprigError::standard(code, *b"01000", message));
                return;
            }
            self.warnings.push(diag::warning("Warning", code, message));
        }
        let mut row_values = Vec::new();
        let mut assigned = Vec::new();
        for (k, c) in self.columns.iter().enumerate() {
            let lit = fields
                .get(k)
                .map(|f| literal(f, self.load.binary, self.nbe));
            match (c, &self.load.targets) {
                (Some(_), _) => row_values.push(lit.unwrap_or_else(|| "DEFAULT".to_owned())),
                (None, Some(t)) => {
                    if let Some(Target::Var(v)) = t.get(k) {
                        assigned.push((v.clone(), lit.unwrap_or_else(|| "NULL".to_owned())));
                    }
                }
                (None, None) => {}
            }
        }
        for (_, e) in &self.load.sets {
            row_values.push(with_vars(e, &assigned, self.opts));
        }
        self.pending.push(format!("({})", row_values.join(", ")));
    }

    /// The `INSERT` for the pending rows.
    fn statement(&mut self) -> String {
        let verb = match self.load.dup {
            Dup::Ignore => "INSERT IGNORE",
            Dup::Replace => "REPLACE",
        };
        let mut cols: Vec<String> = self.columns.iter().flatten().cloned().collect();
        cols.extend(self.load.sets.iter().map(|(c, _)| c.clone()));
        let list = if self.load.targets.is_none() && self.load.sets.is_empty() {
            String::new()
        } else {
            format!(" ({})", cols.join(", "))
        };
        let rows = std::mem::take(&mut self.pending).join(", ");
        format!("{verb} INTO {}{list} VALUES {rows}", self.load.table)
    }
}

impl PassSession {
    /// `LOAD DATA` naming an engine table: `Some` once answered; `None`
    /// for any other statement, or one MariaDB answers.
    #[allow(clippy::too_many_lines)] // the file's packets, then the batches
    pub(super) async fn engine_load_data(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &TenantCatalog,
        current: Option<DbId>,
        sql: &[u8],
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let head = sql.trim_ascii_start();
        if !(head.len() >= 4 && head[..4].eq_ignore_ascii_case(b"LOAD")) {
            return Ok(None);
        }
        let Some(eng) = super::catalog::of(cat) else {
            return Ok(None);
        };
        // An engine table: the current database is the engine's, or the
        // text names one of its databases.
        let tenant = cat.tenant;
        let mentions = current.is_some_and(|c| eng.dbs.contains(&c))
            || eng.dbs.iter().any(|d| {
                let p = super::schema_prefix(tenant.0, *d);
                sql.windows(p.len()).any(|w| w == p.as_bytes())
            });
        if !mentions {
            return Ok(None);
        }
        // The client's own text: logical names.
        let client = self
            .eng
            .entry
            .clone()
            .unwrap_or_else(|| Bytes::copy_from_slice(sql));
        let opts = self.lex_opts().0;
        let load = match read(&client, opts, self.persona) {
            Ok(Some(l)) => l,
            Ok(None) => return Ok(None),
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        if !load.local {
            let server = if self.persona == Persona::MySql84 {
                "MySQL"
            } else {
                "MariaDB"
            };
            let e = SprigError::standard(
                1290,
                *b"HY000",
                format!(
                    "The {server} server is running with the --secure-file-priv option so it \
                     cannot execute this statement"
                ),
            );
            return self.fail(out, &e).await.map(Some);
        }
        if !(cx.proto.has(sprig_proto_mysql::Caps::LOCAL_FILES) && self.tenant_local_infile()) {
            return self
                .fail(out, &local_infile_disabled(self.persona))
                .await
                .map(Some);
        }
        let Some(name) = split::local_infile_name(&client) else {
            return Ok(None);
        };
        // The columns each record fills: the list, or every column.
        let columns: Vec<Option<String>> = if let Some(t) = &load.targets {
            t.iter()
                .map(|t| {
                    if let Target::Column(c) = t {
                        Some(c.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            let probe = format!("SELECT * FROM {} LIMIT 0", load.table);
            match self.run_kept(cx, &probe).await {
                Ok((_, _, cols)) => cols
                    .iter()
                    .map(|c| Some(String::from_utf8_lossy(&c.name).into_owned()))
                    .collect(),
                Err(e) => return self.fail(out, &e).await.map(Some),
            }
        };
        let sv = self.engine_vars(cx);
        let own_txn = !self.eng.txn.is_open() && !sv.autocommit_off;
        let strict = load.dup == Dup::Replace && sv.env.strict;
        let mut run = Run {
            skip: load.ignore_lines,
            load,
            columns,
            nbe: opts.no_backslash_escapes,
            opts,
            records: 0,
            affected: 0,
            warnings: Vec::new(),
            pending: Vec::new(),
            failed: None,
            strict,
        };
        if own_txn && let Err(e) = self.run_kept(cx, "START TRANSACTION").await {
            run.failed = Some(e);
        }
        out.local_infile_request(&name).await?;
        let mut buf: Vec<u8> = Vec::new();
        loop {
            let chunk = out.local_infile_read().await?;
            let done = chunk.is_empty();
            buf.extend_from_slice(&chunk);
            if run.failed.is_some() {
                buf.clear();
            } else {
                let (recs, used) = records(&buf, &run.load.format, done);
                buf.drain(..used);
                for r in recs {
                    if run.skip > 0 {
                        run.skip -= 1;
                        continue;
                    }
                    run.row(&r);
                    if run.failed.is_some() {
                        break;
                    }
                    if run.pending.len() >= BATCH {
                        self.load_batch(cx, &mut run).await;
                    }
                }
                if done && !run.pending.is_empty() {
                    self.load_batch(cx, &mut run).await;
                }
            }
            if done {
                break;
            }
        }
        if let Some(e) = run.failed.take() {
            if own_txn {
                let _ = self.run_kept(cx, "ROLLBACK").await;
            }
            return self.fail(out, &e).await.map(Some);
        }
        if own_txn && let Err(e) = self.run_kept(cx, "COMMIT").await {
            return self.fail(out, &e).await.map(Some);
        }
        let (deleted, skipped) = match run.load.dup {
            Dup::Replace => (run.affected.saturating_sub(run.records), 0),
            Dup::Ignore => (0, run.records.saturating_sub(run.affected)),
        };
        let count = run.warnings.len();
        let (area, extra) = diag::kept(run.warnings);
        self.diag_set(area, extra);
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let mut ok = OkPacket::new(status);
        ok.affected_rows = run.affected;
        ok.warnings = u16::try_from(count).unwrap_or(u16::MAX);
        ok.info = Bytes::from(format!(
            "Records: {}  Deleted: {deleted}  Skipped: {skipped}  Warnings: {count}",
            run.records
        ));
        out.ok(ok).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// Inserts the pending records; the first error stops the load.
    async fn load_batch(&mut self, cx: &mut ExecCtx<'_>, run: &mut Run) {
        let first = run.records - run.pending.len() as u64;
        let stmt = run.statement();
        match self.run_kept(cx, &stmt).await {
            Ok((affected, warnings, _)) => {
                run.affected += affected;
                run.warnings
                    .extend(warnings.into_iter().map(|w| renumbered(w, first)));
            }
            Err(e) => run.failed = Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(r: &[Field]) -> Vec<String> {
        r.iter()
            .map(|f| {
                f.as_ref().map_or("<NULL>".to_owned(), |b| {
                    String::from_utf8_lossy(b).into_owned()
                })
            })
            .collect()
    }

    #[test]
    fn tab_separated_records_are_read() {
        let f = Format::default();
        let (r, used) = records(b"1\ta\\tb\t\\N\n2\tx", &f, false);
        assert_eq!(r.len(), 1);
        assert_eq!(fields(&r[0]), ["1", "a\tb", "<NULL>"]);
        assert_eq!(used, 10);
        let (r, _) = records(b"2\tx", &f, true);
        assert_eq!(fields(&r[0]), ["2", "x"]);
    }

    #[test]
    fn enclosed_fields_hold_terminators_and_doubled_quotes() {
        let f = Format {
            field_term: b",".to_vec(),
            enclosed: Some(b'"'),
            ..Format::default()
        };
        let (r, _) = records(
            b"1,\"a, b\",\"say \"\"hi\"\"\"\n2,NULL,\"NULL\",\n",
            &f,
            true,
        );
        assert_eq!(fields(&r[0]), ["1", "a, b", "say \"hi\""]);
        assert_eq!(fields(&r[1]), ["2", "<NULL>", "NULL", ""]);
    }

    #[test]
    fn load_statements_are_read() {
        let l = read(
            b"LOAD DATA LOCAL INFILE 'f.csv' REPLACE INTO TABLE db.t FIELDS TERMINATED BY ',' \
              OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES (a, @b, c) \
              SET d = @b * 2",
            LexOptions::mysql84(),
            Persona::MySql84,
        )
        .unwrap()
        .unwrap();
        assert!(l.local && l.dup == Dup::Replace && l.table == "db.t");
        assert_eq!(l.format.field_term, b",");
        assert_eq!(l.format.enclosed, Some(b'"'));
        assert_eq!(l.format.line_term, b"\r\n");
        assert_eq!(l.ignore_lines, 1);
        assert_eq!(
            l.targets.unwrap(),
            [
                Target::Column("a".into()),
                Target::Var("b".into()),
                Target::Column("c".into())
            ]
        );
        assert_eq!(l.sets, [("d".to_owned(), "@b * 2".to_owned())]);
        assert_eq!(
            with_vars(
                "@b * 2",
                &[("b".into(), "'7'".into())],
                LexOptions::mysql84()
            ),
            "('7') * 2"
        );
        assert!(
            read(b"SELECT 1", LexOptions::mysql84(), Persona::MySql84)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn warnings_count_rows_from_the_file_start() {
        let w = diag::warning(
            "Warning",
            1265,
            "Data truncated for column 'a' at row 2".into(),
        );
        assert_eq!(
            renumbered(w, 500).message,
            "Data truncated for column 'a' at row 502"
        );
    }
}
