//! The SQL stream statements for streams of engine databases:
//! `CREATE | ALTER | DROP STREAM`, `SHOW STREAMS`, publishing with
//! `INSERT INTO <stream> VALUES (...)`, and `DROP DATABASE` dropping the
//! database's streams. The statement is planned without waiting
//! ([`plan`]: names, privileges, values, partitions) and then run
//! ([`run`]) on [`EngineStreams`].
//!
//! Not built here (refused with 1235): `KEY (...)`, `ATTACHED TO GROUP`,
//! stream settings (`SET (...)`), `INSERT ... SELECT` and values that
//! aren't literals, and publishing inside a transaction.

use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_kafka::broker::data::{MAX_KEY_BYTES, ProduceRecord};
use sprig_proto_kafka::records::Header;
use sprig_sql::lex::LexOptions;

use super::kafka::{EngineStreams, Need, encode_records};
use super::records::TopicRecord;
use crate::grammar::{self, QName, Stmt};
use crate::model::Cleanup;

/// What the session knows, asked while planning.
pub trait SqlHost {
    /// The tenant.
    fn tenant(&self) -> u64;
    /// The current database.
    fn current_db(&self) -> Option<String>;
    /// Whether database `db` is stored on the engine.
    fn on_engine(&self, db: &str) -> bool;
    /// Whether the session may do `need` on `db`.`stream` (`stream` empty:
    /// on the database).
    fn allowed(&self, db: &str, stream: &str, need: Need) -> bool;
    /// The user, for messages (`user`, `host`).
    fn user(&self) -> (String, String);
    /// Whether the session has a transaction open.
    fn in_transaction(&self) -> bool;
    /// Whether that transaction is on the engine (a publish then joins it,
    /// [`publish_in`]); one on MariaDB can't carry an engine stream's
    /// append.
    fn engine_transaction(&self) -> bool {
        false
    }
}

/// A planned statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// `CREATE STREAM`.
    Create {
        /// The new record (id given at commit).
        rec: TopicRecord,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `ALTER STREAM`: the replaced record.
    Alter(TopicRecord),
    /// `DROP STREAM`.
    Drop {
        /// `<db>.<stream>`.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `SHOW STREAMS`: the rows.
    Show(Vec<Vec<Option<String>>>),
    /// A publish: one batch per partition.
    Publish {
        /// The stream.
        stream_id: u64,
        /// (partition, records).
        batches: Vec<(i32, Vec<ProduceRecord>)>,
    },
    /// `DROP DATABASE`: drop its streams, then let the statement run.
    DropDatabase(String),
}

/// What a run answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// OK with affected rows.
    Ok(u64),
    /// A result set (text).
    Rows {
        /// Column names.
        columns: Vec<String>,
        /// Rows.
        rows: Vec<Vec<Option<String>>>,
    },
    /// Done; the statement itself still runs (`DROP DATABASE`).
    Continue,
}

fn unsupported(what: &str) -> SprigError {
    SprigError::standard(
        1235,
        *b"42000",
        format!(
            "This version of SprigDB doesn't yet support '{what}' on a stream of an engine database"
        ),
    )
}

fn no_db() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn denied(host: &dyn SqlHost, command: &str, name: &str) -> SprigError {
    let (user, h) = host.user();
    SprigError::standard(
        1142,
        *b"42000",
        format!("{command} command denied to user '{user}'@'{h}' for table '{name}'"),
    )
}

fn not_found(name: &str) -> SprigError {
    SprigError::new(
        SprigCode::StreamNotFound,
        format!("ER_SPRIG_STREAM_NOT_FOUND: stream '{name}' does not exist"),
    )
}

fn db_of(q: &QName, host: &dyn SqlHost) -> Result<String> {
    match &q.db {
        Some(d) => Ok(d.clone()),
        None => host.current_db().ok_or_else(no_db),
    }
}

fn hours_ms(h: u64) -> i64 {
    i64::try_from(h.saturating_mul(3_600_000)).unwrap_or(i64::MAX)
}

/// Plans `sql` when it is an engine database's stream statement;
/// `Ok(None)` when it isn't (the statement goes on as usual).
///
/// # Errors
/// The statement is malformed, refers to a missing stream, isn't allowed,
/// or uses something not built for engine streams.
#[allow(clippy::too_many_lines)] // one branch per statement kind
pub fn plan(
    eng: &EngineStreams,
    sql: &str,
    lex: &LexOptions,
    host: &dyn SqlHost,
) -> Result<Option<Plan>> {
    if !grammar::maybe_stream(sql.as_bytes(), true) {
        return Ok(None);
    }
    let tenant = host.tenant();
    let stmt = grammar::classify(sql.as_bytes(), lex)?;
    match stmt {
        Stmt::Create(c) => {
            let Some(q) = c.name.as_ref() else {
                return Ok(None);
            };
            let db = db_of(q, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            if !c.key.is_empty() {
                return Err(unsupported("CREATE STREAM ... KEY (...)"));
            }
            if c.attached.is_some() {
                return Err(unsupported("CREATE STREAM ... ATTACHED TO GROUP"));
            }
            if !host.allowed(&db, &q.name, Need::Create) {
                return Err(denied(host, "CREATE", &q.name));
            }
            let partitions = i32::try_from(c.partitions.unwrap_or(12)).unwrap_or(i32::MAX);
            if !(1..=10_000).contains(&partitions) {
                return Err(SprigError::standard(
                    1231,
                    *b"42000",
                    "PARTITIONS must be from 1 to 10000",
                ));
            }
            Ok(Some(Plan::Create {
                rec: TopicRecord {
                    name: format!("{db}.{}", q.name),
                    stream_id: 0,
                    partitions,
                    retention_ms: c.retention_hours.map(hours_ms),
                    retention_bytes: None,
                    compact: c.cleanup.map(|x| x == Cleanup::Compact),
                    max_message_bytes: None,
                    created_ms: super::now_ms(),
                },
                if_not_exists: c.if_not_exists,
            }))
        }
        Stmt::Alter(a) => {
            let db = db_of(&a.name, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            if !a.settings.is_empty() {
                return Err(unsupported("ALTER STREAM ... SET"));
            }
            let name = format!("{db}.{}", a.name.name);
            let Some(mut r) = eng.topic(tenant, &name)? else {
                return Err(not_found(&name));
            };
            if !host.allowed(&db, &a.name.name, Need::Alter) {
                return Err(denied(host, "ALTER", &a.name.name));
            }
            if let Some(p) = a.partitions {
                let p = i32::try_from(p).unwrap_or(i32::MAX);
                if p < r.partitions || p > 10_000 {
                    return Err(SprigError::new(
                        SprigCode::StreamPartitionsDecrease,
                        format!(
                            "ER_SPRIG_STREAM_PARTITIONS_DECREASE: '{name}' has {} partitions; \
                             they can only grow (to at most 10000)",
                            r.partitions
                        ),
                    ));
                }
                r.partitions = p;
            }
            if let Some(h) = a.retention_hours {
                r.retention_ms = Some(hours_ms(h));
            }
            if let Some(c) = a.cleanup {
                r.compact = Some(c == Cleanup::Compact);
            }
            Ok(Some(Plan::Alter(r)))
        }
        Stmt::Drop { if_exists, name } => {
            let db = db_of(&name, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            if !host.allowed(&db, &name.name, Need::Drop) {
                return Err(denied(host, "DROP", &name.name));
            }
            Ok(Some(Plan::Drop {
                name: format!("{db}.{}", name.name),
                if_exists,
            }))
        }
        Stmt::Show { db } => {
            let db = match db {
                Some(d) => d,
                None => host.current_db().ok_or_else(no_db)?,
            };
            if !host.on_engine(&db) {
                return Ok(None);
            }
            let prefix = format!("{db}.");
            let rows = eng
                .topics(tenant)?
                .into_iter()
                .filter_map(|r| {
                    let s = r.name.strip_prefix(&prefix)?.to_owned();
                    if !host.allowed(&db, &s, Need::Read) && !host.allowed(&db, &s, Need::Write) {
                        return None;
                    }
                    let compact = r.compact.unwrap_or(false);
                    Some(vec![
                        Some(s),
                        Some("events".to_owned()),
                        Some(r.partitions.to_string()),
                        Some(if compact { "compact" } else { "delete" }.to_owned()),
                        None,
                        Some(String::new()),
                        None,
                    ])
                })
                .collect();
            Ok(Some(Plan::Show(rows)))
        }
        Stmt::Insert(i) => {
            let db = db_of(&i.target, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            let name = format!("{db}.{}", i.target.name);
            let Some(r) = eng.topic(tenant, &name)? else {
                // A table of the engine database.
                return Ok(None);
            };
            if i.on_duplicate {
                return Err(SprigError::new(
                    SprigCode::StreamAppendOnly,
                    "ER_SPRIG_STREAM_APPEND_ONLY: INSERT ... ON DUPLICATE KEY UPDATE on a stream",
                ));
            }
            if i.set_form {
                return Err(unsupported("INSERT ... SET"));
            }
            if !host.allowed(&db, &i.target.name, Need::Write) {
                return Err(denied(host, "INSERT", &i.target.name));
            }
            if host.in_transaction() && !host.engine_transaction() {
                return Err(unsupported(
                    "INSERT INTO <stream> inside a transaction that isn't on the engine",
                ));
            }
            let rows = grammar::insert_values(sql.as_bytes(), lex)
                .ok_or_else(|| unsupported("INSERT ... SELECT or values that aren't literals"))?;
            publish_plan(eng, &r, &i, rows).map(Some)
        }
        Stmt::Write { verb, target } => {
            let db = db_of(&target, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            let name = format!("{db}.{}", target.name);
            if eng.topic(tenant, &name)?.is_none() {
                return Ok(None);
            }
            Err(SprigError::new(
                SprigCode::StreamAppendOnly,
                format!(
                    "ER_SPRIG_STREAM_APPEND_ONLY: {verb} on stream '{name}': a stream is append-only"
                ),
            ))
        }
        Stmt::CreateTable(q) => {
            let db = db_of(&q, host)?;
            if !host.on_engine(&db) {
                return Ok(None);
            }
            let name = format!("{db}.{}", q.name);
            if eng.topic(tenant, &name)?.is_some() {
                return Err(SprigError::new(
                    SprigCode::StreamExists,
                    format!("ER_SPRIG_STREAM_EXISTS: '{name}' is a stream"),
                ));
            }
            Ok(None)
        }
        Stmt::DropDatabase(db) => {
            if !host.on_engine(&db) || !host.allowed(&db, "", Need::Drop) {
                return Ok(None);
            }
            Ok(Some(Plan::DropDatabase(db)))
        }
        Stmt::Other => Ok(None),
    }
}

/// The batches of a publish: records by partition (an explicit
/// `partition_no`, else by key, else round robin over the statement).
#[allow(clippy::too_many_lines)] // one check after another, per record
fn publish_plan(
    eng: &EngineStreams,
    r: &TopicRecord,
    i: &grammar::InsertInto,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Plan> {
    let cols: Vec<String> = match &i.columns {
        Some(c) => c.iter().map(|(n, _)| n.to_ascii_lowercase()).collect(),
        None => match rows.first().map(Vec::len) {
            Some(2) => vec!["key".into(), "value".into()],
            _ => vec!["key".into(), "value".into(), "headers".into()],
        },
    };
    for c in &cols {
        if !matches!(
            c.as_str(),
            "key" | "k" | "value" | "v" | "headers" | "partition_no" | "ts"
        ) {
            return Err(SprigError::standard(
                1054,
                *b"42S22",
                format!("Unknown column '{c}' in 'field list'"),
            ));
        }
    }
    let max = eng.max_message_bytes(r);
    let compact = r.compact.unwrap_or(false);
    let parts = u32::try_from(r.partitions.max(1)).unwrap_or(1);
    let now = super::now_ms();
    let mut by_part: std::collections::BTreeMap<i32, Vec<ProduceRecord>> =
        std::collections::BTreeMap::new();
    for (n, row) in rows.into_iter().enumerate() {
        if row.len() != cols.len() {
            return Err(SprigError::standard(
                1136,
                *b"21S01",
                format!("Column count doesn't match value count at row {}", n + 1),
            ));
        }
        let mut rec = ProduceRecord {
            key: None,
            value: None,
            headers: Vec::new(),
            timestamp_ms: now,
        };
        let mut explicit = None;
        for (c, v) in cols.iter().zip(row) {
            match c.as_str() {
                "key" | "k" => rec.key = v,
                "value" | "v" => rec.value = v,
                "headers" => rec.headers = headers_of(v.as_deref())?,
                "partition_no" => {
                    explicit = v
                        .and_then(|b| String::from_utf8(b).ok())
                        .map(|s| {
                            s.trim().parse::<u32>().map_err(|_| {
                                SprigError::standard(
                                    1366,
                                    *b"HY000",
                                    format!("Incorrect partition_no value: '{s}'"),
                                )
                            })
                        })
                        .transpose()?;
                }
                _ => {
                    if let Some(t) = v
                        .and_then(|b| String::from_utf8(b).ok())
                        .and_then(|s| s.trim().parse::<i64>().ok())
                    {
                        rec.timestamp_ms = t;
                    }
                }
            }
        }
        let (key_len, size) = super::kafka::record_size(&rec);
        if key_len > MAX_KEY_BYTES as u64 || size > max as u64 {
            return Err(SprigError::new(
                SprigCode::StreamRecordTooLarge,
                format!(
                    "ER_SPRIG_STREAM_RECORD_TOO_LARGE: a record of {size} bytes (key {key_len}) is \
                     above the stream's limit ({max}; keys {MAX_KEY_BYTES})"
                ),
            ));
        }
        if compact && rec.key.is_none() {
            return Err(SprigError::new(
                SprigCode::StreamCompactNeedsKey,
                "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted stream",
            ));
        }
        let p = match (explicit, rec.key.as_deref()) {
            (Some(p), _) => p % parts,
            (None, Some(k)) => crate::partition::for_key(k, parts).0,
            (None, None) => u32::try_from(n).unwrap_or(0) % parts,
        };
        by_part
            .entry(i32::try_from(p).unwrap_or(0))
            .or_default()
            .push(rec);
    }
    Ok(Plan::Publish {
        stream_id: r.stream_id,
        batches: by_part.into_iter().collect(),
    })
}

/// A headers value: a JSON object of strings (or nulls).
fn headers_of(v: Option<&[u8]>) -> Result<Vec<Header>> {
    let Some(v) = v else {
        return Ok(Vec::new());
    };
    let bad = || {
        SprigError::standard(
            3140,
            *b"22032",
            "Invalid JSON text: headers must be a JSON object of strings",
        )
    };
    let j: serde_json::Value = serde_json::from_slice(v).map_err(|_| bad())?;
    let serde_json::Value::Object(m) = j else {
        return Err(bad());
    };
    m.into_iter()
        .map(|(k, v)| match v {
            serde_json::Value::Null => Ok(Header {
                key: k,
                value: None,
            }),
            serde_json::Value::String(s) => Ok(Header {
                key: k,
                value: Some(s.into_bytes()),
            }),
            other => Ok(Header {
                key: k,
                value: Some(other.to_string().into_bytes()),
            }),
        })
        .collect()
}

/// Runs a publish inside the session's engine transaction: each batch is
/// a note of `txn`, appended when (and if) it commits, in commit order and
/// atomically with its other writes. Other plans run as [`run`] runs them.
///
/// # Errors
/// The transaction is not active, or the records can't be encoded.
pub async fn publish_in(
    eng: &EngineStreams,
    tenant: u64,
    plan: Plan,
    txn: &mut sprig_engine::txn::Txn,
) -> Result<Answer> {
    let Plan::Publish { stream_id, batches } = plan else {
        return run(eng, tenant, plan).await;
    };
    let mut n = 0u64;
    for (p, recs) in &batches {
        n += recs.len() as u64;
        let key = super::Key {
            tenant,
            stream: stream_id,
            partition: u32::try_from(*p).unwrap_or(0),
        };
        eng.log()
            .append_in(txn, key, &encode_records(recs, None)?)?;
    }
    Ok(Answer::Ok(n))
}

/// Runs a plan.
///
/// # Errors
/// The engine or the log failed, or the stream exists (or doesn't).
pub async fn run(eng: &EngineStreams, tenant: u64, plan: Plan) -> Result<Answer> {
    match plan {
        Plan::Create { rec, if_not_exists } => {
            let name = rec.name.clone();
            match eng.create(tenant, rec).await? {
                Some(_) => Ok(Answer::Ok(0)),
                None if if_not_exists => Ok(Answer::Ok(0)),
                None => Err(SprigError::new(
                    SprigCode::StreamExists,
                    format!("ER_SPRIG_STREAM_EXISTS: stream '{name}' already exists"),
                )),
            }
        }
        Plan::Alter(r) => {
            if eng.update(tenant, &r).await? {
                Ok(Answer::Ok(0))
            } else {
                Err(not_found(&r.name))
            }
        }
        Plan::Drop { name, if_exists } => match eng.drop_stream(tenant, &name).await? {
            Some(_) => Ok(Answer::Ok(0)),
            None if if_exists => Ok(Answer::Ok(0)),
            None => Err(not_found(&name)),
        },
        Plan::Show(rows) => Ok(Answer::Rows {
            columns: [
                "Stream",
                "Kind",
                "Partitions",
                "Cleanup",
                "Source",
                "Key",
                "Lag_ms",
            ]
            .iter()
            .map(|c| (*c).to_owned())
            .collect(),
            rows,
        }),
        Plan::Publish { stream_id, batches } => {
            let mut n = 0u64;
            let mut encoded = Vec::with_capacity(batches.len());
            for (p, recs) in &batches {
                n += recs.len() as u64;
                let key = super::Key {
                    tenant,
                    stream: stream_id,
                    partition: u32::try_from(*p).unwrap_or(0),
                };
                encoded.push((key, encode_records(recs, None)?));
            }
            // One log entry for the whole statement: all of it or none.
            let pending = eng.propose_many(&encoded)?;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            for p in pending {
                eng.outcome(p, deadline).await.map_err(|e| {
                    SprigError::internal(format!("stream publish: {} ({})", e.message, e.code))
                })?;
            }
            Ok(Answer::Ok(n))
        }
        Plan::DropDatabase(db) => {
            eng.drop_database(tenant, &db).await?;
            Ok(Answer::Continue)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::seg::SegConfig;
    use crate::seg::testing::TempDir;

    struct H {
        in_txn: bool,
        reader: bool,
    }

    /// In a transaction on the engine.
    struct EngTxn;

    impl SqlHost for EngTxn {
        fn tenant(&self) -> u64 {
            3
        }
        fn current_db(&self) -> Option<String> {
            Some("eng".into())
        }
        fn on_engine(&self, db: &str) -> bool {
            db == "eng"
        }
        fn allowed(&self, _db: &str, _s: &str, _need: Need) -> bool {
            true
        }
        fn user(&self) -> (String, String) {
            ("u".into(), "%".into())
        }
        fn in_transaction(&self) -> bool {
            true
        }
        fn engine_transaction(&self) -> bool {
            true
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_inside_an_engine_transaction_commits_with_it() {
        let d = TempDir::new("seg-sql-txn");
        let (e, s) = open(d.path());
        let h = H {
            in_txn: false,
            reader: false,
        };
        go(&s, &h, "CREATE STREAM ev PARTITIONS 1").await.unwrap();
        let r = s.topic(3, "eng.ev").unwrap().unwrap();
        let lex = LexOptions::mariadb();
        let hw = |s: &EngineStreams| {
            s.log()
                .partitions(3, r.stream_id)
                .iter()
                .map(|(_, b)| b.high_watermark)
                .sum::<i64>()
        };
        let wait_hw = |want: i64| {
            let s = &s;
            async move {
                for _ in 0..500 {
                    if hw(s) == want {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                panic!("high watermark {} != {want}", hw(s));
            }
        };
        // Rolled back: nothing appended.
        let mut t = e.begin(sprig_engine::txn::TxnOptions::default()).unwrap();
        let p = plan(&s, "INSERT INTO ev VALUES ('k', 'gone')", &lex, &EngTxn)
            .unwrap()
            .unwrap();
        assert_eq!(publish_in(&s, 3, p, &mut t).await.unwrap(), Answer::Ok(1));
        t.rollback();
        // Committed: both rows, in one go.
        let mut t = e.begin(sprig_engine::txn::TxnOptions::default()).unwrap();
        let p = plan(
            &s,
            "INSERT INTO ev VALUES ('k', 'a'), ('k', 'b')",
            &lex,
            &EngTxn,
        )
        .unwrap()
        .unwrap();
        assert_eq!(publish_in(&s, 3, p, &mut t).await.unwrap(), Answer::Ok(2));
        assert_eq!(hw(&s), 0);
        t.commit().unwrap().wait().unwrap();
        wait_hw(2).await;
        // A transaction on MariaDB can't carry it.
        let maria = H {
            in_txn: true,
            reader: false,
        };
        assert!(
            go(&s, &maria, "INSERT INTO ev VALUES ('k', 'x')")
                .await
                .is_err()
        );
    }

    impl SqlHost for H {
        fn tenant(&self) -> u64 {
            3
        }
        fn current_db(&self) -> Option<String> {
            Some("eng".into())
        }
        fn on_engine(&self, db: &str) -> bool {
            db == "eng"
        }
        fn allowed(&self, _db: &str, _s: &str, need: Need) -> bool {
            !self.reader || need == Need::Read
        }
        fn user(&self) -> (String, String) {
            ("u".into(), "%".into())
        }
        fn in_transaction(&self) -> bool {
            self.in_txn
        }
    }

    fn open(dir: &std::path::Path) -> (sprig_engine::Engine, EngineStreams) {
        let mut cfg = sprig_engine::EngineConfig::new(dir.join("engine"));
        cfg.log.sync = sprig_engine::log::SyncMode::Off;
        cfg.budget.engine_mb = 64;
        let e = sprig_engine::Engine::open(cfg).unwrap();
        let s = EngineStreams::open(
            &e,
            dir.join("streams"),
            SegConfig::default(),
            sprig_proto_kafka::topics::TopicDefaults::default(),
        )
        .unwrap();
        (e, s)
    }

    async fn go(s: &EngineStreams, h: &H, sql: &str) -> Result<Option<Answer>> {
        let lex = LexOptions::mariadb();
        match plan(s, sql, &lex, h)? {
            None => Ok(None),
            Some(p) => run(s, 3, p).await.map(Some),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::too_many_lines)]
    async fn statements_on_an_engine_database() {
        let d = TempDir::new("seg-sql");
        let (_e, s) = open(d.path());
        let h = H {
            in_txn: false,
            reader: false,
        };
        assert_eq!(
            go(
                &s,
                &h,
                "CREATE STREAM orders PARTITIONS 3 RETENTION '2 days'"
            )
            .await
            .unwrap(),
            Some(Answer::Ok(0))
        );
        let r = s.topic(3, "eng.orders").unwrap().unwrap();
        assert_eq!((r.partitions, r.retention_ms), (3, Some(48 * 3_600_000)));
        assert!(go(&s, &h, "CREATE STREAM eng.orders").await.is_err());
        assert_eq!(
            go(&s, &h, "CREATE STREAM IF NOT EXISTS eng.orders")
                .await
                .unwrap(),
            Some(Answer::Ok(0))
        );
        // Another database's statements are not this module's.
        assert_eq!(go(&s, &h, "CREATE STREAM shop.x").await.unwrap(), None);
        assert_eq!(
            go(&s, &h, "INSERT INTO orders (key, value) VALUES ('a', 'one'), ('b', 'two'), (NULL, CONCAT('th', 'ree'))")
                .await
                .unwrap(),
            Some(Answer::Ok(3))
        );
        assert_eq!(
            go(
                &s,
                &h,
                "INSERT INTO eng.orders VALUES ('a', X'6869', '{\"h\": \"v\"}')"
            )
            .await
            .unwrap(),
            Some(Answer::Ok(1))
        );
        let total: i64 = s
            .log()
            .partitions(3, r.stream_id)
            .iter()
            .map(|(_, b)| b.high_watermark)
            .sum();
        assert_eq!(total, 4);
        // Key 'a' went to one partition both times.
        let pa = crate::partition::for_key(b"a", 3).0;
        let (_, bytes) = s
            .log()
            .read(
                crate::seg::Key {
                    tenant: 3,
                    stream: r.stream_id,
                    partition: pa,
                },
                0,
                1 << 20,
            )
            .unwrap();
        let recs = crate::seg::kafka::decode_stored(&bytes, 0).unwrap();
        let values: Vec<Vec<u8>> = recs
            .iter()
            .filter(|x| x.key.as_deref() == Some(b"a"))
            .map(|x| x.value.clone().unwrap())
            .collect();
        assert_eq!(values, vec![b"one".to_vec(), b"hi".to_vec()]);
        assert!(recs.iter().any(|x| x.headers.iter().any(|h| h.key == "h")));
        // Refusals.
        assert!(
            go(&s, &h, "INSERT INTO orders (key, value) SELECT a, b FROM t")
                .await
                .is_err()
        );
        assert!(go(&s, &h, "UPDATE orders SET v = 1").await.is_err());
        assert!(go(&s, &h, "CREATE TABLE orders (a INT)").await.is_err());
        let t = H {
            in_txn: true,
            reader: false,
        };
        assert!(
            go(&s, &t, "INSERT INTO orders VALUES ('a', 'b')")
                .await
                .is_err()
        );
        let ro = H {
            in_txn: false,
            reader: true,
        };
        let e = go(&s, &ro, "INSERT INTO orders VALUES ('a', 'b')")
            .await
            .unwrap_err();
        assert!(e.message.contains("denied"), "{}", e.message);
        // ALTER, SHOW, DROP.
        assert!(
            go(&s, &h, "ALTER STREAM orders PARTITIONS 2")
                .await
                .is_err()
        );
        go(&s, &h, "ALTER STREAM orders PARTITIONS 5 CLEANUP compact")
            .await
            .unwrap();
        let r2 = s.topic(3, "eng.orders").unwrap().unwrap();
        assert_eq!((r2.partitions, r2.compact), (5, Some(true)));
        match go(&s, &h, "SHOW STREAMS").await.unwrap() {
            Some(Answer::Rows { rows, .. }) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_deref(), Some("orders"));
                assert_eq!(rows[0][2].as_deref(), Some("5"));
            }
            other => panic!("{other:?}"),
        }
        go(&s, &h, "DROP STREAM orders").await.unwrap();
        // Gone: a missing stream is refused (39940), `IF EXISTS` is quiet.
        assert_eq!(
            go(&s, &h, "DROP STREAM orders").await.unwrap_err().code(),
            Some(SprigCode::StreamNotFound)
        );
        assert_eq!(
            go(&s, &h, "DROP STREAM IF EXISTS orders").await.unwrap(),
            Some(Answer::Ok(0))
        );
        assert_eq!(
            go(&s, &h, "ALTER STREAM orders PARTITIONS 9")
                .await
                .unwrap_err()
                .code(),
            Some(SprigCode::StreamNotFound)
        );
        go(&s, &h, "CREATE STREAM a").await.unwrap();
        go(&s, &h, "CREATE STREAM b").await.unwrap();
        assert_eq!(
            go(&s, &h, "DROP DATABASE eng").await.unwrap(),
            Some(Answer::Continue)
        );
        assert!(s.topics(3).unwrap().is_empty());
    }
}
