//! SQL tables written from the MongoDB port: `insert`, `update`, `delete`
//! and `findAndModify` on a name that is no collection but a table of the
//! SQL database with the MongoDB database's name ([`super::tables`]).
//!
//! ```text
//! insert          per document: INSERT INTO db.t (fields) VALUES (values)
//! update, delete  per statement: the table's rows read FOR UPDATE (the
//! findAndModify   filter's implied predicate, super::tables), matched,
//!                 sorted and limited by the store with MongoDB's rules;
//!                 per matched row: the update applied to its document
//!                 (sprig_doc::update, MongoDB's operators, pipelines and
//!                 replacements), then
//!                 UPDATE db.t SET <changed columns> WHERE <primary key>
//!                 or DELETE FROM db.t WHERE <primary key>
//! upsert          no row matched: the filter's equalities, the update
//!                 applied as an insert, then INSERT
//! ```
//!
//! - **SQL's rules hold.** Every write is an SQL statement on the
//!   connection's own SQL session, as the client's statement: privileges,
//!   row-level security, constraints, foreign keys, triggers and the
//!   column types apply as for any SQL write. A field that is no column is
//!   refused (121), except `_id` when the table has no `_id` column (drivers
//!   add one to every inserted document; reads show no `_id` either). A
//!   field removed by an update (or left out of a replacement) sets its
//!   column to `NULL`.
//! - **Values** go to SQL by the type map in reverse: numbers as numbers,
//!   `Decimal128` exactly, strings and `ObjectId`s as text, dates as UTC
//!   `DATETIME` text, a UUID (binary subtype 4) as its text, other binary
//!   as bytes, documents and arrays as JSON text into a `JSON` column. A
//!   value with no SQL form (a regex, a timestamp, ...) is refused (2).
//! - **Errors** become MongoDB's: a duplicate key 11000, a failed check
//!   constraint 121, a deadlock or snapshot conflict 112; other SQL errors
//!   are write errors with code 2 and the SQL error's text.
//! - **Atomicity.** Outside a MongoDB transaction each inserted document is
//!   one SQL statement, and each update or delete statement (all its rows)
//!   one SQL transaction. Inside one, every statement runs in the MongoDB
//!   transaction's own engine transaction (lent to the SQL session), so it
//!   commits or aborts with the transaction's documents, and a write error
//!   ends the transaction. Retryable writes aren't remembered for tables:
//!   a retried insert may answer a duplicate key.
//! - **Needs a primary key** for `update`, `delete` and `findAndModify`
//!   (the rows are written back by it); without one they are refused (2).

// Command code names its parts as MongoDB's own does (c, s, m, q, u, d).
#![allow(clippy::many_single_char_names)]

use std::collections::HashSet;
use std::sync::Arc;

use bson::spec::BinarySubtype;
use bson::{Bson, Document, doc};
use sprig_doc::docstore::DocStore;
use sprig_doc::docstore::cmd::Reply;
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_proto_mysql::ColumnDef;

use super::commands::owner;
use super::conn::Conn;
use super::tables::{
    BINARY, Loan, TableRows, documents, finish, give_back, is_json, json_columns, lend, loan_of, q,
    read_rows, sql_failed,
};

/// The field that tags each row's document with its position while the
/// store matches and sorts them.
const ROW: &str = "__sprig_row";

/// What a write needs of the table.
struct Meta {
    /// `db`.`t`.
    from: String,
    defs: Vec<ColumnDef>,
    names: Vec<String>,
    json: HashSet<String>,
    /// The primary key's columns, by position in `defs`.
    pk: Vec<usize>,
    /// The table has an `_id` column.
    has_id: bool,
}

impl Meta {
    fn column(&self, field: &str) -> Option<usize> {
        self.names.iter().position(|n| n == field).or_else(|| {
            self.names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(field))
        })
    }

    fn is_json(&self, i: usize) -> bool {
        self.json.contains(&self.names[i]) || is_json(&self.defs[i])
    }

    /// A binary string column (`BINARY`, `VARBINARY`, `BLOB`): its values
    /// go as bytes. (Numbers and dates have the binary character set too.)
    fn is_bytes(&self, i: usize) -> bool {
        use sprig_proto_mysql::resultset::column_type as ct;
        let d = &self.defs[i];
        d.charset == BINARY
            && !self.is_json(i)
            && matches!(
                d.col_type,
                ct::VARCHAR
                    | ct::VAR_STRING
                    | ct::STRING
                    | ct::TINY_BLOB
                    | ct::MEDIUM_BLOB
                    | ct::LONG_BLOB
                    | ct::BLOB
            )
    }
}

/// The primary key's column names in `SHOW CREATE TABLE`'s text.
fn primary_key(text: &str) -> Vec<String> {
    let Some(at) = text.find("PRIMARY KEY (") else {
        return Vec::new();
    };
    let rest = &text[at + "PRIMARY KEY (".len()..];
    let mut out = Vec::new();
    let mut name = String::new();
    let mut quoted = false;
    let mut chars = rest.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '`' if quoted && chars.peek() == Some(&'`') => {
                name.push('`');
                chars.next();
            }
            '`' => {
                quoted = !quoted;
                if !quoted {
                    out.push(std::mem::take(&mut name));
                }
            }
            ')' if !quoted => break,
            c if quoted => name.push(c),
            _ => {}
        }
    }
    out
}

async fn meta(c: &mut Conn, db: &str, table: &str) -> Result<Meta, MongoError> {
    let from = format!("{}.{}", q(db), q(table));
    let sql = c.sql()?;
    let (defs, _) = sql
        .query_typed(&format!("SELECT * FROM {from} LIMIT 0"))
        .await
        .map_err(|e| sql_failed(&e))?;
    let json = json_columns(sql, &from).await?;
    // A source without a definition of its own (a stream, a queue, a
    // Redis database) has none to show: no key.
    let rows = sql
        .query_typed(&format!("SHOW CREATE TABLE {from}"))
        .await
        .map(|(_, rows)| rows)
        .unwrap_or_default();
    let text = rows
        .first()
        .and_then(|r| r.get(1))
        .and_then(Option::as_deref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let names: Vec<String> = defs
        .iter()
        .map(|d| String::from_utf8_lossy(&d.name).into_owned())
        .collect();
    let pk = primary_key(&text)
        .iter()
        .filter_map(|p| names.iter().position(|n| n.eq_ignore_ascii_case(p)))
        .collect();
    let has_id = names.iter().any(|n| n == "_id");
    Ok(Meta {
        from,
        defs,
        names,
        json,
        pk,
        has_id,
    })
}

fn bad_value(msg: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, msg.into())
}

/// A string literal (backslashes doubled unless the session has
/// `NO_BACKSLASH_ESCAPES`).
fn text_literal(s: &str, backslash: bool) -> String {
    let s = s.replace('\'', "''");
    if backslash {
        format!("'{}'", s.replace('\\', "\\\\"))
    } else {
        format!("'{s}'")
    }
}

fn hex_literal(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(3 + 2 * b.len());
    out.push_str("X'");
    for x in b {
        let _ = write!(out, "{x:02X}");
    }
    out.push('\'');
    out
}

/// A double as an SQL number: plain digits in the usual range, else with
/// an exponent (a double literal).
fn double_literal(f: f64) -> Result<String, MongoError> {
    if !f.is_finite() {
        return Err(bad_value(format!("{f} has no SQL value")));
    }
    let a = f.abs();
    if a == 0.0 || (1e-6..1e15).contains(&a) {
        Ok(format!("{f}"))
    } else {
        Ok(format!("{f:e}"))
    }
}

/// `ms` since the epoch as UTC `DATETIME` text.
fn datetime_text(d: bson::DateTime) -> Result<String, MongoError> {
    let iso = d
        .try_to_rfc3339_string()
        .map_err(|_| bad_value("a date outside SQL's range"))?;
    // 2024-02-03T04:05:06.123Z
    let t = iso.trim_end_matches('Z').replacen('T', " ", 1);
    Ok(t)
}

fn uuid_text(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let h = b.iter().fold(String::new(), |mut acc, x| {
        let _ = write!(acc, "{x:02x}");
        acc
    });
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// The SQL literal of `v` for column `i` of `m`.
fn literal(m: &Meta, i: usize, v: &Bson, backslash: bool) -> Result<String, MongoError> {
    let json = m.is_json(i);
    if json && !matches!(v, Bson::Null) {
        // A JSON column takes the value's JSON text.
        let j = v.clone().into_relaxed_extjson();
        return Ok(text_literal(&j.to_string(), backslash));
    }
    let binary = m.is_bytes(i);
    Ok(match v {
        Bson::Null => "NULL".to_owned(),
        Bson::Boolean(b) => if *b { "1" } else { "0" }.to_owned(),
        Bson::Int32(x) => x.to_string(),
        Bson::Int64(x) => x.to_string(),
        Bson::Double(f) => double_literal(*f)?,
        Bson::Decimal128(d) => text_literal(&d.to_string(), backslash),
        Bson::String(s) if binary => hex_literal(s.as_bytes()),
        Bson::String(s) => text_literal(s, backslash),
        Bson::ObjectId(o) => text_literal(&o.to_hex(), backslash),
        Bson::DateTime(d) => text_literal(&datetime_text(*d)?, backslash),
        Bson::Binary(b) if b.subtype == BinarySubtype::Uuid && b.bytes.len() == 16 && !binary => {
            text_literal(&uuid_text(&b.bytes), backslash)
        }
        Bson::Binary(b) => hex_literal(&b.bytes),
        Bson::Document(_) | Bson::Array(_) => {
            return Err(bad_value(format!(
                "field '{}' holds a document or array; only a JSON column takes one",
                m.names[i]
            )));
        }
        other => {
            return Err(bad_value(format!(
                "field '{}' holds a {} value, which has no SQL form",
                m.names[i],
                sprig_doc::update::type_name(other)
            )));
        }
    })
}

/// A row's value of column `i` as text protocol bytes, as an SQL literal
/// that equals it (the primary key of a row read).
fn stored_literal(m: &Meta, i: usize, v: Option<&[u8]>, backslash: bool) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(b) if m.is_bytes(i) => hex_literal(b),
        Some(b) => text_literal(&String::from_utf8_lossy(b), backslash),
    }
}

/// Whether string literals need their backslashes doubled: the session's
/// `sql_mode` has no `NO_BACKSLASH_ESCAPES` (asked only when a value holds
/// a backslash).
async fn backslash_escapes(c: &mut Conn) -> Result<bool, MongoError> {
    let (_, rows) = c
        .sql()?
        .query_typed("SELECT @@SESSION.sql_mode")
        .await
        .map_err(|e| sql_failed(&e))?;
    let mode = rows
        .first()
        .and_then(|r| r.first())
        .and_then(Option::as_deref)
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
        .unwrap_or_default();
    Ok(!mode.contains("NO_BACKSLASH_ESCAPES"))
}

fn has_backslash(v: &Bson) -> bool {
    match v {
        Bson::String(s) => s.contains('\\'),
        Bson::Document(d) => d.iter().any(|(k, x)| k.contains('\\') || has_backslash(x)),
        Bson::Array(a) => a.iter().any(has_backslash),
        _ => false,
    }
}

/// A write error of statement `index`, or the command's own error.
enum Failure {
    /// A `writeErrors` entry: the next statements may still run.
    Write(Document),
    /// The command fails.
    Command(MongoError),
}

fn write_error(index: usize, code: i32, msg: impl Into<String>) -> Document {
    doc! {"index": i32::try_from(index).unwrap_or(i32::MAX), "code": code, "errmsg": msg.into()}
}

/// An SQL statement's error as MongoDB's.
fn sql_write_failure(index: usize, e: &sprig_doc::engine::SqlError) -> Failure {
    match e.errno {
        Some(1062) => Failure::Write(write_error(
            index,
            code::DUPLICATE_KEY,
            format!("E11000 duplicate key error: {}", e.message),
        )),
        Some(3819 | 4025) => Failure::Write(write_error(
            index,
            code::DOCUMENT_VALIDATION_FAILURE,
            format!("Document failed validation: {}", e.message),
        )),
        Some(1142 | 1143 | 1044 | 1045 | 1213 | 1020 | 1205) => Failure::Command(sql_failed(e)),
        _ => Failure::Write(write_error(
            index,
            code::BAD_VALUE,
            format!("SQL error {}: {}", e.errno.unwrap_or(0), e.message),
        )),
    }
}

/// The `field = literal` pairs of document `d` for the table; refuses a
/// field that is no column.
fn assignments(
    m: &Meta,
    d: &Document,
    backslash: bool,
) -> Result<Vec<(usize, String)>, MongoError> {
    let mut out = Vec::new();
    for (k, v) in d {
        if k == ROW {
            continue;
        }
        let Some(i) = m.column(k) else {
            if k == "_id" && !m.has_id {
                continue;
            }
            return Err(MongoError::new(
                code::DOCUMENT_VALIDATION_FAILURE,
                format!("Document failed validation: field '{k}' is no column of the table"),
            ));
        };
        out.push((i, literal(m, i, v, backslash)?));
    }
    Ok(out)
}

fn insert_text(m: &Meta, sets: &[(usize, String)]) -> String {
    if sets.is_empty() {
        return format!("INSERT INTO {} () VALUES ()", m.from);
    }
    let cols: Vec<String> = sets.iter().map(|(i, _)| q(&m.names[*i])).collect();
    let vals: Vec<&str> = sets.iter().map(|(_, v)| v.as_str()).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        m.from,
        cols.join(", "),
        vals.join(", ")
    )
}

/// `WHERE` naming row `r` by its primary key.
fn key_condition(m: &Meta, r: &[Option<Vec<u8>>], backslash: bool) -> String {
    m.pk.iter()
        .map(|&i| {
            format!(
                "{} = {}",
                q(&m.names[i]),
                stored_literal(m, i, r.get(i).and_then(Option::as_deref), backslash)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The SQL transaction one statement's writes run in: the MongoDB
/// transaction's (lent), or one of its own.
async fn open(c: &mut Conn, tx: &mut Loan) -> Result<(), MongoError> {
    if tx.is_some() {
        return lend(c, tx);
    }
    c.sql()?
        .exec_client("START TRANSACTION")
        .await
        .map(|_| ())
        .map_err(|e| sql_failed(&e))
}

/// Ends what [`open`] opened: commits its own transaction (`ok`) or rolls
/// it back; gives a lent one back.
async fn close(c: &mut Conn, tx: &mut Loan, ok: bool) -> Result<(), MongoError> {
    if tx.is_some() {
        return give_back(c, tx);
    }
    let stmt = if ok { "COMMIT" } else { "ROLLBACK" };
    c.sql()?
        .exec_client(stmt)
        .await
        .map(|_| ())
        .map_err(|e| sql_failed(&e))
}

/// Runs one SQL write; `Ok(affected)`.
async fn run_sql(c: &mut Conn, text: &str, index: usize) -> Result<u64, Failure> {
    let sql = c.sql().map_err(Failure::Command)?;
    match sql.exec_client(text).await {
        Ok(d) => Ok(d.affected),
        Err(e) => Err(sql_write_failure(index, &e)),
    }
}

/// Answers write command `name` on table `target` of `db`.
pub(super) async fn command(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
    target: &str,
) -> Result<Reply, MongoError> {
    let mut tx = loan_of(c, s, body).await?;
    let r = match meta(c, db, target).await {
        Ok(m) => run(c, s, name, body, db, &m, &mut tx).await,
        Err(e) => Err(e),
    };
    finish(s, tx, r).await
}

async fn run(
    c: &mut Conn,
    s: &DocStore,
    name: &str,
    body: &Document,
    db: &str,
    m: &Meta,
    tx: &mut Loan,
) -> Result<Reply, MongoError> {
    if name != "insert" && m.pk.is_empty() {
        return Err(bad_value(format!(
            "{db}.{} has no primary key: it can't be updated or deleted from the MongoDB port (use SQL)",
            body.get_str(name).unwrap_or_default()
        )));
    }
    let backslash = if body_has_backslash(body) {
        backslash_escapes(c).await?
    } else {
        true
    };
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let in_txn = tx.is_some();
    let mut reply = match name {
        "insert" => insert(c, body, m, tx, ordered, backslash).await?,
        "update" => update(c, s, body, db, m, tx, ordered, backslash).await?,
        "delete" => delete(c, s, body, db, m, tx, ordered, backslash).await?,
        _ => return find_and_modify(c, s, body, db, m, tx, backslash).await,
    };
    if in_txn
        && let Ok(errs) = reply.get_array("writeErrors")
        && let Some(Bson::Document(e)) = errs.first()
    {
        // In a transaction a write error ends it.
        let code = e.get_i32("code").unwrap_or(code::BAD_VALUE);
        let msg = e.get_str("errmsg").unwrap_or_default().to_owned();
        return Err(MongoError::new(code, msg));
    }
    reply.insert("ok", 1.0);
    Ok(Reply::Doc(reply))
}

fn body_has_backslash(body: &Document) -> bool {
    ["documents", "updates", "deletes", "query", "update"]
        .iter()
        .filter_map(|k| body.get(k))
        .any(has_backslash)
}

fn docs_of(body: &Document, k: &str) -> Vec<Document> {
    body.get_array(k)
        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
        .unwrap_or_default()
}

/// Adds `errs` to `reply` as `writeErrors` when there are any.
fn with_errors(mut reply: Document, errs: Vec<Document>) -> Document {
    if !errs.is_empty() {
        reply.insert("writeErrors", errs);
    }
    reply
}

fn count(n: u64) -> Bson {
    i32::try_from(n).map_or_else(
        |_| Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)),
        Bson::Int32,
    )
}

async fn insert(
    c: &mut Conn,
    body: &Document,
    m: &Meta,
    tx: &mut Loan,
    ordered: bool,
    backslash: bool,
) -> Result<Document, MongoError> {
    let mut n = 0u64;
    let mut errs = Vec::new();
    if tx.is_some() {
        lend(c, tx)?;
    }
    let mut failed = None;
    for (i, d) in docs_of(body, "documents").iter().enumerate() {
        let sets = match assignments(m, d, backslash) {
            Ok(s) => s,
            Err(e) => {
                errs.push(write_error(i, e.code, e.message));
                if ordered {
                    break;
                }
                continue;
            }
        };
        match run_sql(c, &insert_text(m, &sets), i).await {
            Ok(a) => n += a.min(1),
            Err(Failure::Write(e)) => {
                errs.push(e);
                if ordered {
                    break;
                }
            }
            Err(Failure::Command(e)) => {
                failed = Some(e);
                break;
            }
        }
    }
    if tx.is_some() {
        give_back(c, tx)?;
    }
    if let Some(e) = failed {
        return Err(e);
    }
    Ok(with_errors(doc! {"n": count(n)}, errs))
}

/// The rows of the table `filter` matches, as documents tagged with their
/// row's position, matched (and sorted and limited) by the store with
/// MongoDB's rules; locked for the write.
async fn matched(
    c: &mut Conn,
    s: &DocStore,
    db: &str,
    m: &Meta,
    filter: &Document,
    sort: Option<&Document>,
    limit: Option<i64>,
) -> Result<(TableRows, Vec<Document>), MongoError> {
    let table = m
        .from
        .rsplit_once('.')
        .map(|(_, t)| t.trim_matches('`').replace("``", "`"))
        .unwrap_or_default();
    let rows = read_rows(c, db, &table, Some(filter), true)
        .await?
        .ok_or_else(|| MongoError::new(code::NAMESPACE_NOT_FOUND, "table not found"))?;
    let mut docs = documents(&rows);
    for (i, d) in docs.iter_mut().enumerate() {
        d.insert(ROW, i64::try_from(i).unwrap_or(i64::MAX));
    }
    let mut p: Vec<Document> = vec![doc! {
        "$documents": docs.into_iter().map(Bson::Document).collect::<Vec<_>>()
    }];
    p.push(doc! {"$match": filter.clone()});
    if let Some(st) = sort.filter(|x| !x.is_empty()) {
        p.push(doc! {"$sort": st.clone()});
    }
    if let Some(l) = limit.filter(|l| *l > 0) {
        p.push(doc! {"$limit": l});
    }
    let who = owner(c);
    let out = s.aggregate(who.tenant, db, "", &p, None).await?;
    Ok((rows, out))
}

/// The row position a matched document carries, and the document without
/// it.
fn untag(mut d: Document) -> (usize, Document) {
    let i = d
        .remove(ROW)
        .and_then(|v| sprig_doc::filter::as_i64(&v))
        .and_then(|i| usize::try_from(i).ok())
        .unwrap_or(usize::MAX);
    (i, d)
}

/// The `SET` list taking a row from `old` to `new` (`None`: nothing
/// changes); a field `new` lacks sets its column to `NULL`.
fn changes(
    m: &Meta,
    old: &Document,
    new: &Document,
    backslash: bool,
) -> Result<Option<String>, MongoError> {
    let sets = assignments(m, new, backslash)?;
    let mut parts = Vec::new();
    for (i, lit) in &sets {
        let name = &m.names[*i];
        if old.get(name) != new.get(name) {
            parts.push(format!("{} = {lit}", q(name)));
        }
    }
    for (i, name) in m.names.iter().enumerate() {
        if new.get(name).is_none() && !matches!(old.get(name), None | Some(Bson::Null)) {
            let _ = i;
            parts.push(format!("{} = NULL", q(name)));
        }
    }
    Ok((!parts.is_empty()).then(|| parts.join(", ")))
}

fn apply_cx<'a>(
    query: &'a sprig_doc::filter::Pred,
    filters: &'a [(String, sprig_doc::filter::Pred)],
    inserting: bool,
    now: bson::DateTime,
) -> sprig_doc::update::ApplyCx<'a> {
    sprig_doc::update::ApplyCx {
        query,
        array_filters: filters,
        inserting,
        now,
    }
}

/// `arrayFilters`, parsed.
fn array_filters(u: &Document) -> Result<Vec<(String, sprig_doc::filter::Pred)>, MongoError> {
    sprig_doc::docstore::write::parse_array_filters(&docs_of(u, "arrayFilters"))
}

/// The primary key of `d` as the `_id` an upsert reports: its one key
/// column's value, or a document of its key columns.
fn key_id(m: &Meta, d: &Document) -> Bson {
    if m.has_id
        && let Some(v) = d.get("_id")
    {
        return v.clone();
    }
    match m.pk.as_slice() {
        [one] => d.get(&m.names[*one]).cloned().unwrap_or(Bson::Null),
        many => {
            let mut k = Document::new();
            for &i in many {
                k.insert(
                    m.names[i].clone(),
                    d.get(&m.names[i]).cloned().unwrap_or(Bson::Null),
                );
            }
            Bson::Document(k)
        }
    }
}

/// One update statement's counts.
#[derive(Default)]
struct Counted {
    n: u64,
    modified: u64,
    upserted: Option<Bson>,
}

/// Applies update statement `u` (number `index`), in its transaction.
#[allow(clippy::too_many_arguments)]
async fn update_one(
    c: &mut Conn,
    s: &DocStore,
    db: &str,
    m: &Meta,
    u: &Document,
    index: usize,
    backslash: bool,
    now: bson::DateTime,
) -> Result<Counted, Failure> {
    let cmd = |e: MongoError| Failure::Write(write_error(index, e.code, e.message));
    let q_doc = u.get_document("q").cloned().unwrap_or_default();
    let spec = sprig_doc::update::parse(u.get("u").unwrap_or(&Bson::Null)).map_err(cmd)?;
    let pred = sprig_doc::filter::parse(&q_doc).map_err(cmd)?;
    let filters = array_filters(u).map_err(cmd)?;
    let multi = u.get_bool("multi").unwrap_or(false);
    let upsert = u.get_bool("upsert").unwrap_or(false);
    let (rows, found) = matched(c, s, db, m, &q_doc, None, (!multi).then_some(1))
        .await
        .map_err(Failure::Command)?;
    let mut out = Counted::default();
    if found.is_empty() {
        if !upsert {
            return Ok(out);
        }
        let mut d = sprig_doc::update::upsert_seed(&pred).map_err(cmd)?;
        if matches!(spec, sprig_doc::update::UpdateSpec::Replace(_)) {
            d = Document::new();
        }
        sprig_doc::update::apply(&mut d, &spec, &apply_cx(&pred, &filters, true, now))
            .map_err(cmd)?;
        let sets = assignments(m, &d, backslash).map_err(cmd)?;
        run_sql(c, &insert_text(m, &sets), index).await?;
        out.n = 1;
        out.upserted = Some(key_id(m, &d));
        return Ok(out);
    }
    for f in found {
        let (i, old) = untag(f);
        let Some(r) = rows.rows.get(i) else { continue };
        let mut new = old.clone();
        sprig_doc::update::apply(&mut new, &spec, &apply_cx(&pred, &filters, false, now))
            .map_err(cmd)?;
        out.n += 1;
        let Some(set) = changes(m, &old, &new, backslash).map_err(cmd)? else {
            continue;
        };
        let text = format!(
            "UPDATE {} SET {set} WHERE {}",
            m.from,
            key_condition(m, r, backslash)
        );
        if run_sql(c, &text, index).await? > 0 {
            out.modified += 1;
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn update(
    c: &mut Conn,
    s: &DocStore,
    body: &Document,
    db: &str,
    m: &Meta,
    tx: &mut Loan,
    ordered: bool,
    backslash: bool,
) -> Result<Document, MongoError> {
    let (mut n, mut modified) = (0u64, 0u64);
    let mut upserted = Vec::new();
    let mut errs = Vec::new();
    let now = s.now();
    for (i, u) in docs_of(body, "updates").iter().enumerate() {
        open(c, tx).await?;
        let r = update_one(c, s, db, m, u, i, backslash, now).await;
        close(c, tx, r.is_ok()).await?;
        match r {
            Ok(k) => {
                n += k.n;
                modified += k.modified;
                if let Some(id) = k.upserted {
                    upserted.push(doc! {"index": i32::try_from(i).unwrap_or(i32::MAX), "_id": id});
                }
            }
            Err(Failure::Write(e)) => {
                errs.push(e);
                if ordered {
                    break;
                }
            }
            Err(Failure::Command(e)) => return Err(e),
        }
    }
    let mut reply = doc! {"n": count(n), "nModified": count(modified)};
    if !upserted.is_empty() {
        reply.insert("upserted", upserted);
    }
    Ok(with_errors(reply, errs))
}

#[allow(clippy::too_many_arguments)]
async fn delete(
    c: &mut Conn,
    s: &DocStore,
    body: &Document,
    db: &str,
    m: &Meta,
    tx: &mut Loan,
    ordered: bool,
    backslash: bool,
) -> Result<Document, MongoError> {
    let mut n = 0u64;
    let mut errs = Vec::new();
    for (i, d) in docs_of(body, "deletes").iter().enumerate() {
        let q_doc = d.get_document("q").cloned().unwrap_or_default();
        let limit = d
            .get("limit")
            .and_then(sprig_doc::filter::as_i64)
            .unwrap_or(0);
        open(c, tx).await?;
        let r: Result<u64, Failure> = async {
            let (rows, found) = matched(c, s, db, m, &q_doc, None, (limit == 1).then_some(1))
                .await
                .map_err(Failure::Command)?;
            let mut k = 0u64;
            for f in found {
                let (at, _) = untag(f);
                let Some(r) = rows.rows.get(at) else { continue };
                let text = format!(
                    "DELETE FROM {} WHERE {}",
                    m.from,
                    key_condition(m, r, backslash)
                );
                k += run_sql(c, &text, i).await?;
            }
            Ok(k)
        }
        .await;
        close(c, tx, r.is_ok()).await?;
        match r {
            Ok(k) => n += k,
            Err(Failure::Write(e)) => {
                errs.push(e);
                if ordered {
                    break;
                }
            }
            Err(Failure::Command(e)) => return Err(e),
        }
    }
    Ok(with_errors(doc! {"n": count(n)}, errs))
}

/// `findAndModify`: the first row the query (and sort) matches, removed or
/// updated (or upserted), returned before or after (`new`), projected by
/// `fields`.
#[allow(clippy::too_many_lines)] // remove, update and upsert, then the answer
async fn find_and_modify(
    c: &mut Conn,
    s: &DocStore,
    body: &Document,
    db: &str,
    m: &Meta,
    tx: &mut Loan,
    backslash: bool,
) -> Result<Reply, MongoError> {
    let q_doc = body.get_document("query").cloned().unwrap_or_default();
    let sort = body.get_document("sort").ok().cloned();
    let remove = body.get_bool("remove").unwrap_or(false);
    let want_new = body.get_bool("new").unwrap_or(false);
    let upsert = body.get_bool("upsert").unwrap_or(false);
    let spec = match body.get("update") {
        Some(u) if !remove => Some(sprig_doc::update::parse(u)?),
        None if !remove => {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "Either an update or remove=true must be specified",
            ));
        }
        _ => None,
    };
    let pred = sprig_doc::filter::parse(&q_doc)?;
    let filters = array_filters(body)?;
    let now = s.now();
    open(c, tx).await?;
    let r: Result<(Option<Document>, Document), Failure> = async {
        let as_cmd = Failure::Command;
        let (rows, found) = matched(c, s, db, m, &q_doc, sort.as_ref(), Some(1))
            .await
            .map_err(as_cmd)?;
        let Some(f) = found.into_iter().next() else {
            let Some(spec) = spec.as_ref().filter(|_| upsert) else {
                return Ok((None, doc! {"n": 0, "updatedExisting": false}));
            };
            let mut d = sprig_doc::update::upsert_seed(&pred).map_err(as_cmd)?;
            if matches!(spec, sprig_doc::update::UpdateSpec::Replace(_)) {
                d = Document::new();
            }
            sprig_doc::update::apply(&mut d, spec, &apply_cx(&pred, &filters, true, now))
                .map_err(as_cmd)?;
            let sets = assignments(m, &d, backslash).map_err(as_cmd)?;
            run_sql(c, &insert_text(m, &sets), 0).await?;
            let id = key_id(m, &d);
            return Ok((
                want_new.then_some(d),
                doc! {"n": 1, "updatedExisting": false, "upserted": id},
            ));
        };
        let (at, old) = untag(f);
        let Some(r) = rows.rows.get(at) else {
            return Ok((None, doc! {"n": 0, "updatedExisting": false}));
        };
        let key = key_condition(m, r, backslash);
        let Some(spec) = spec.as_ref() else {
            run_sql(c, &format!("DELETE FROM {} WHERE {key}", m.from), 0).await?;
            return Ok((Some(old), doc! {"n": 1}));
        };
        let mut new = old.clone();
        sprig_doc::update::apply(&mut new, spec, &apply_cx(&pred, &filters, false, now))
            .map_err(as_cmd)?;
        if let Some(set) = changes(m, &old, &new, backslash).map_err(as_cmd)? {
            run_sql(c, &format!("UPDATE {} SET {set} WHERE {key}", m.from), 0).await?;
        }
        Ok((
            Some(if want_new { new } else { old }),
            doc! {"n": 1, "updatedExisting": true},
        ))
    }
    .await;
    close(c, tx, r.is_ok()).await?;
    let (value, last) = match r {
        Ok(x) => x,
        Err(Failure::Write(e)) => {
            let code = e.get_i32("code").unwrap_or(code::BAD_VALUE);
            return Err(MongoError::new(
                code,
                e.get_str("errmsg").unwrap_or_default().to_owned(),
            ));
        }
        Err(Failure::Command(e)) => return Err(e),
    };
    let value = match (value, body.get_document("fields").ok()) {
        (Some(v), Some(p)) if !p.is_empty() => {
            let who = owner(c);
            let pipe = vec![doc! {"$documents": [v]}, doc! {"$project": p.clone()}];
            s.aggregate(who.tenant, db, "", &pipe, None)
                .await?
                .into_iter()
                .next()
                .map_or(Bson::Null, Bson::Document)
        }
        (Some(v), _) => Bson::Document(v),
        (None, _) => Bson::Null,
    };
    Ok(Reply::Doc(
        doc! {"lastErrorObject": last, "value": value, "ok": 1.0},
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_keys_are_read_off_the_definition() {
        let t = "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b``c` varchar(5),\n  PRIMARY KEY (`a`,`b``c`),\n  KEY `k` (`b``c`)\n)";
        assert_eq!(primary_key(t), vec!["a".to_owned(), "b`c".to_owned()]);
        assert!(primary_key("CREATE TABLE `t` (`a` int)").is_empty());
    }

    #[test]
    fn values_become_sql_literals() {
        let def = |name: &str, charset: u16| ColumnDef {
            name: bytes::Bytes::from(name.to_owned()),
            charset,
            col_type: sprig_proto_mysql::resultset::column_type::VAR_STRING,
            ..ColumnDef::default()
        };
        let m = Meta {
            from: "`d`.`t`".into(),
            defs: vec![def("s", 45), def("b", BINARY), def("j", 45)],
            names: vec!["s".into(), "b".into(), "j".into()],
            json: ["j".to_owned()].into_iter().collect(),
            pk: vec![0],
            has_id: false,
        };
        assert_eq!(
            literal(&m, 0, &Bson::String("o'k\\".into()), true).unwrap(),
            "'o''k\\\\'"
        );
        assert_eq!(
            literal(&m, 0, &Bson::String("a\\".into()), false).unwrap(),
            "'a\\'"
        );
        assert_eq!(
            literal(&m, 1, &Bson::String("ab".into()), true).unwrap(),
            "X'6162'"
        );
        assert_eq!(literal(&m, 0, &Bson::Double(2.5), true).unwrap(), "2.5");
        assert_eq!(literal(&m, 0, &Bson::Double(1e300), true).unwrap(), "1e300");
        assert_eq!(
            literal(&m, 0, &Bson::Decimal128("12.50".parse().unwrap()), true).unwrap(),
            "'12.50'"
        );
        assert_eq!(
            literal(&m, 2, &Bson::Document(doc! {"a": [1, "x"]}), true).unwrap(),
            r#"'{"a":[1,"x"]}'"#
        );
        assert!(literal(&m, 0, &Bson::Document(doc! {}), true).is_err());
        assert_eq!(
            literal(
                &m,
                0,
                &Bson::DateTime(bson::DateTime::from_millis(1_700_000_000_123)),
                true
            )
            .unwrap(),
            "'2023-11-14 22:13:20.123'"
        );
        let d = doc! {"s": "x", "_id": 1};
        let sets = assignments(&m, &d, true).unwrap();
        assert_eq!(sets, vec![(0, "'x'".to_owned())]);
        assert!(assignments(&m, &doc! {"zz": 1}, true).is_err());
        let set = changes(&m, &doc! {"s": "x", "j": {"a": 1}}, &doc! {"s": "y"}, true)
            .unwrap()
            .unwrap();
        assert_eq!(set, "`s` = 'y', `j` = NULL");
        assert_eq!(
            changes(&m, &doc! {"s": "x"}, &doc! {"s": "x"}, true).unwrap(),
            None
        );
    }
}
