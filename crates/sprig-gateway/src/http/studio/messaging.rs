//! The stream, queue and sink views' routes (`Streams.ListGroups`, the
//! `Mq` topology and the `Sinks` dead letters), for the caller's own
//! tenant.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/databases/{db}/streams/{name}/groups` | `Streams.ListGroups`: per partition the log start and high watermark (read as the caller, so `SELECT` on the stream is checked), and every consumer group with its committed offset, lag and generation (the engine's stream records) |
//! | `GET /v1/databases/{db}/mq` | The database's virtual host (its vhost record, else the database's own name once it has topology), exchanges, queues and bindings, with names (the queues module's catalog records) |
//! | `GET /v1/databases/{db}/sinks/{name}/deadLetters` | The sink's dead letters (the sinks module's catalog records) |
//! | `POST /v1/databases/{db}/sinks/{name}/deadLetters:replay`, `:discard` | `{ids}` or `{all: true}`: marked `replaying` (the sink's task feeds them back) or `discarded` |
//! | `POST /v1/databases/{db}/sinks/{name}:test` | `Sinks.Test`: `{sample}` (10 by default): `TEST SINK db.name WITH SAMPLE n` as the caller; `{columns, rows}` |
//! | `POST /v1/databases/{db}/connections/{name}:test` | `Connections.Test`: `{platform, write, to}`: `TEST [PLATFORM] CONNECTION [db.]name [WRITE [TO 'to']]` as the caller; `{columns, rows}` |
//!
//! Everything else the views show comes from the statements a SQL client
//! runs, which the gateway checks as for any client: `SHOW STREAMS FROM`,
//! `SELECT ... FROM <stream>`, `SHOW SINKS FROM`, `SHOW SINK STATUS`,
//! `SHOW CREATE SINK`, `ALTER SINK ... PAUSE | RESUME | RESET TO`,
//! `SHOW INHERITED DEFINITIONS | INSTANCES | OVERRIDES`, `ALTER INHERITED`.
//!
//! Who may see what: the topology and the consumer groups need a grant on
//! the database (or `*.*`, or `SPRIG_TENANT_ADMIN`), as the workload views
//! decide; watermarks need `SELECT` on the stream (the read runs as the
//! caller). Dead letters hold record data, so they need
//! `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`, or being the sink's owner
//! (1227 otherwise).

use std::collections::BTreeMap;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde_json::{Value, json};
use sprig_catalog::{Pk, RecordStore};
use sprig_common::ids::TenantId;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::logic::access_of;
use super::{hdr, invalid, json_response, parse_json, read_body, rows_of, run_sql};

/// Dead letters listed at most.
const MAX_DEAD: u32 = 1000;

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_messaging_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/v1/databases/") else {
        return false;
    };
    let Some((_, tail)) = rest.split_once('/') else {
        return false;
    };
    tail == "mq"
        || (tail.starts_with("streams/") && tail.ends_with("/groups"))
        || (tail.starts_with("sinks/") && tail.contains("/deadLetters"))
        || ((tail.starts_with("sinks/") || tail.starts_with("connections/"))
            && tail.ends_with(":test")
            && tail.matches('/').count() == 1)
}

/// `Sinks.Test` and `Connections.Test`'s statement: the
/// statements Studio would run, names quoted.
fn test_statement(db: &str, kind: &str, name: &str, body: &Value) -> Result<String, SprigError> {
    let q = |s: &str| format!("{}.{}", ident(db), ident(s));
    if kind == "sinks" {
        let n = body.get("sample").and_then(Value::as_u64).unwrap_or(10);
        if !(1..=1000).contains(&n) {
            return Err(invalid("sample must be 1 to 1000"));
        }
        return Ok(format!("TEST SINK {} WITH SAMPLE {n}", q(name)));
    }
    let platform = body
        .get("platform")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut sql = if platform {
        format!("TEST PLATFORM CONNECTION {}", ident(name))
    } else {
        format!("TEST CONNECTION {}", q(name))
    };
    if body.get("write").and_then(Value::as_bool).unwrap_or(false) {
        sql.push_str(" WRITE");
        if let Some(to) = body.get("to").and_then(Value::as_str) {
            sql.push_str(" TO '");
            sql.push_str(&to.replace('\\', "\\\\").replace('\'', "''"));
            sql.push('\'');
        }
    }
    Ok(sql)
}

/// `Sinks.Test` / `Connections.Test` (see the module docs).
async fn test_route(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    body: &Value,
) -> Result<Response<Body>, SprigError> {
    let (columns, rows) = run_test(core, caller, db, kind, name, body).await?;
    Ok(json_response(
        200,
        &json!({"columns": columns, "rows": rows}),
    ))
}

/// `Sinks.Test` (`kind` `"sinks"`) or `Connections.Test`
/// (`"connections"`) as the caller: the columns and rows, for the REST
/// route above and the gRPC services (`sprig.v1.Sinks`,
/// `sprig.v1.Connections`).
pub(crate) async fn run_test(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    kind: &str,
    name: &str,
    body: &Value,
) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), SprigError> {
    let sql = test_statement(db, kind, name, body)?;
    let r = run_sql(core, caller, db, sql).await?;
    let columns: Vec<String> = r
        .results
        .iter()
        .find(|x| !x.columns.is_empty())
        .map(|x| x.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    Ok((columns, rows_of(&r)))
}

fn decode(seg: &str) -> String {
    let b = seg.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    let hex = |c: u8| (c as char).to_digit(16);
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2]))
        {
            out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

async fn grant_lines(core: &Arc<Core>, caller: &Caller) -> Result<Vec<String>, SprigError> {
    let r = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    Ok(rows_of(&r)
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

/// The caller's tenant, its catalog records, the database's id and its
/// name as the catalog spells it.
fn catalog(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
) -> Result<(TenantId, Arc<dyn RecordStore>, u32, String), SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("these views need a tenant login"));
    };
    let cat = svc
        .cache
        .cached(tid)
        .ok_or_else(|| invalid("the tenant's catalog isn't loaded yet; retry"))?;
    let row = cat
        .database(db)
        .or_else(|| cat.database(&db.to_ascii_lowercase()))
        .ok_or_else(|| SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'")))?;
    Ok((tid, Arc::clone(&svc.records), row.db_id.0, row.name.clone()))
}

fn denied_db(caller: &Caller, db: &str) -> SprigError {
    SprigError::standard(
        1044,
        *b"42000",
        format!(
            "Access denied for user '{}' to database '{db}'",
            caller.principal.user
        ),
    )
}

/// Answers one of the routes in the module docs.
///
/// # Errors
/// As the module docs say.
pub async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let rest = path.trim_start_matches("/v1/databases/");
    let (db_enc, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let db = decode(db_enc);
    let segs: Vec<String> = tail.split('/').map(decode).collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    match (&method, segs.as_slice()) {
        (&Method::GET, ["mq"]) => topology(core, &caller, &db).await,
        (&Method::GET, ["streams", name, "groups"]) => groups(core, &caller, &db, name).await,
        (&Method::GET, ["sinks", name, "deadLetters"]) => {
            sink_dead(core, &caller, &db, name, None).await
        }
        (&Method::POST, [kind @ ("sinks" | "connections"), name]) if name.ends_with(":test") => {
            let (kind, name) = (
                (*kind).to_owned(),
                name.trim_end_matches(":test").to_owned(),
            );
            let body = read_body(req).await?;
            let body = if body.is_empty() {
                json!({})
            } else {
                parse_json(&body)?
            };
            test_route(core, &caller, &db, &kind, &name, &body).await
        }
        (&Method::POST, ["sinks", name, verb])
            if *verb == "deadLetters:replay" || *verb == "deadLetters:discard" =>
        {
            let (name, verb) = ((*name).to_owned(), (*verb).to_owned());
            let body = parse_json(&read_body(req).await?)?;
            sink_dead(core, &caller, &db, &name, Some((&verb, &body))).await
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

/// `Streams.ListGroups` for one stream.
async fn groups(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    name: &str,
) -> Result<Response<Body>, SprigError> {
    let lines = grant_lines(core, caller).await?;
    if !access_of(&lines, db).list {
        return Err(denied_db(caller, db));
    }
    let (tid, _, _, db_name) = catalog(core, caller, db)?;
    let eng = crate::kafka::engine::streams().ok_or_else(|| {
        SprigError::new(
            SprigCode::StreamNotFound,
            format!("no stream {db}.{name}: streams aren't served on this node"),
        )
    })?;
    // The stream as its record names it: the catalog's spelling of the
    // database, and the stream's own (the URL's may differ in case).
    let topic = match eng.topic(tid.0, &format!("{db_name}.{name}"))? {
        Some(t) => Some(t),
        None => eng.topics(tid.0)?.into_iter().find(|t| {
            t.name
                .split_once('.')
                .is_some_and(|(d, s)| d == db_name && s.eq_ignore_ascii_case(name))
        }),
    };
    let Some(topic) = topic else {
        return Err(SprigError::new(
            SprigCode::StreamNotFound,
            format!("Unknown stream '{db}.{name}'"),
        ));
    };
    let name = topic
        .name
        .split_once('.')
        .map_or(name, |(_, s)| s)
        .to_owned();
    let name = name.as_str();
    // Watermarks as the caller: SELECT on the stream is checked here.
    let marks = run_sql(
        core,
        caller,
        db,
        format!(
            "SELECT partition_no, MIN(offset_no), MAX(offset_no) + 1, COUNT(*) FROM {} GROUP BY partition_no",
            ident(name)
        ),
    )
    .await?;
    let mut parts: BTreeMap<u64, Value> = BTreeMap::new();
    for row in rows_of(&marks) {
        let n = |i: usize| {
            row.get(i)
                .cloned()
                .flatten()
                .and_then(|s| s.parse::<u64>().ok())
        };
        if let Some(p) = n(0) {
            parts.insert(
                p,
                json!({"partition": p, "log_start": n(1), "high_watermark": n(2), "records": n(3)}),
            );
        }
    }
    let partitions = u64::try_from(topic.partitions).unwrap_or(0);
    for p in 0..partitions {
        parts.entry(p).or_insert_with(
            || json!({"partition": p, "log_start": null, "high_watermark": 0, "records": 0}),
        );
    }
    let groups = stream_groups(&eng, tid, topic.stream_id, &parts)?;
    // As SHOW STREAMS names them: a table's change topic, else events.
    let kind = if name.ends_with(".changes") {
        "changes"
    } else {
        "events"
    };
    let cleanup = if topic.compact == Some(true) {
        "compact"
    } else {
        "delete"
    };
    Ok(json_response(
        200,
        &json!({
            "stream": name,
            "kind": kind,
            "cleanup": cleanup,
            "partitions": parts.into_values().collect::<Vec<_>>(),
            "groups": groups.into_values().collect::<Vec<_>>(),
        }),
    ))
}

/// `ms` since the epoch as UTC `YYYY-MM-DD HH:MM:SS` (what the view showed
/// before the engine: MariaDB's `DATE_FORMAT`).
fn utc_text(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rest) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Days since 1970-01-01 to a civil date (proleptic Gregorian).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// Every group of the tenant with an offset on stream `stream`, with its
/// committed offsets and lag against the partitions' high watermarks.
fn stream_groups(
    eng: &sprig_streams::seg::kafka::EngineStreams,
    tid: TenantId,
    stream: u64,
    parts: &BTreeMap<u64, Value>,
) -> Result<BTreeMap<String, Value>, SprigError> {
    // Every group of the tenant with an offset on this stream (the
    // tenant's own streams are instance 0): those committing from SQL have
    // offsets only, Kafka clients' groups also a group row (generation and
    // state).
    let rows: BTreeMap<String, (i32, String)> = eng
        .records()
        .group_states(tid.0)?
        .into_iter()
        .map(|(g, generation, state)| (g, (generation, state)))
        .collect();
    let mut groups: BTreeMap<String, Value> = BTreeMap::new();
    for group in eng.records().offset_groups(tid.0)? {
        let mut offs: Vec<(i32, i64, i64)> = eng
            .records()
            .offsets_on_timed(tid.0, &group)?
            .into_iter()
            .filter(|(instance, o, _)| *instance == 0 && o.stream_id == stream)
            .map(|(_, o, at)| (o.partition, o.offset, at))
            .collect();
        if offs.is_empty() {
            continue;
        }
        offs.sort_unstable();
        let mut lag_all = 0u64;
        let mut list = Vec::new();
        for (p, off, at) in offs {
            let p = u64::try_from(p).unwrap_or(0);
            let off = u64::try_from(off).unwrap_or(0);
            let hw = parts
                .get(&p)
                .and_then(|v| v["high_watermark"].as_u64())
                .unwrap_or(0);
            let lag = hw.saturating_sub(off);
            lag_all += lag;
            list.push(json!({"partition": p, "committed": off, "lag": lag,
                             "committed_at": utc_text(at)}));
        }
        let row = rows.get(&group);
        groups.insert(
            group.clone(),
            json!({"group": group, "generation": row.map(|r| r.0), "state": row.map(|r| r.1.clone()),
                   "partitions": list, "lag": lag_all}),
        );
    }
    Ok(groups)
}

/// A binding key's parts after its database: source id, destination id,
/// routing key (escaped bytes up to `00 00`, as [`Pk::bytes`] wrote them).
fn binding_parts(k: &[u8]) -> Option<(u64, u64, String)> {
    let src = u64::from_be_bytes(k.get(..8)?.try_into().ok()?);
    let dst = u64::from_be_bytes(k.get(8..16)?.try_into().ok()?);
    let mut key = Vec::new();
    let mut i = 16;
    loop {
        let b = *k.get(i)?;
        if b == 0 {
            match *k.get(i + 1)? {
                0 => break,
                0xFF => {
                    key.push(0);
                    i += 2;
                    continue;
                }
                _ => return None,
            }
        }
        key.push(b);
        i += 1;
    }
    Some((src, dst, String::from_utf8_lossy(&key).into_owned()))
}

/// The database's virtual host and its topology, from the queues
/// module's catalog records.
async fn topology(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
) -> Result<Response<Body>, SprigError> {
    use sprig_queues::catalog::{
        BINDINGS, BindingRow, EXCHANGES, ExchangeRow, QUEUES, QueueRow, VHOSTS, VhostRow,
    };
    let lines = grant_lines(core, caller).await?;
    if !access_of(&lines, db).list {
        return Err(denied_db(caller, db));
    }
    let (tid, records, db_id, _) = catalog(core, caller, db)?;
    let mut tx = records.begin(tid).await?;
    let vhost = VHOSTS
        .rows::<VhostRow>(&mut *tx, &Pk::new())
        .await?
        .into_iter()
        .find(|v| v.db_id == db_id);
    let mut ex: Vec<ExchangeRow> = EXCHANGES
        .rows::<ExchangeRow>(&mut *tx, &Pk::new())
        .await?
        .into_iter()
        .filter(|x| x.db_id == db_id)
        .collect();
    let mut qs: Vec<QueueRow> = QUEUES
        .rows::<QueueRow>(&mut *tx, &Pk::new())
        .await?
        .into_iter()
        .filter(|q| q.db_id == db_id)
        .collect();
    let bs = BINDINGS
        .scan::<BindingRow>(&mut *tx, &Pk::new().u32(db_id))
        .await?;
    drop(tx);
    ex.sort_by(|a, b| a.name.cmp(&b.name));
    qs.sort_by(|a, b| a.name.cmp(&b.name));
    let mut names: BTreeMap<u64, Value> = BTreeMap::new();
    let exchanges: Vec<Value> = ex
        .into_iter()
        .map(|x| {
            names.insert(x.object_id, json!(x.name));
            json!({"id": x.object_id, "name": x.name, "type": x.kind, "durable": x.durable,
                   "auto_delete": x.auto_delete, "internal": x.internal, "arguments": x.arguments})
        })
        .collect();
    let queues: Vec<Value> = qs
        .into_iter()
        .map(|q| {
            names.insert(q.object_id, json!(q.name));
            json!({"id": q.object_id, "name": q.name, "type": q.qtype, "durable": q.durable,
                   "auto_delete": q.auto_delete, "arguments": q.arguments, "lanes": q.lanes,
                   "state": q.state})
        })
        .collect();
    let bindings: Vec<Value> = bs
        .into_iter()
        .filter_map(|(pk, r)| {
            let (src, dst, key) = binding_parts(pk.0.get(4..)?)?;
            Some(
                json!({"source": names.get(&src).cloned().unwrap_or(Value::Null),
                   "destination": names.get(&dst).cloned().unwrap_or(Value::Null),
                   "destination_kind": r.dest_kind, "binding_key": key, "arguments": r.arguments}),
            )
        })
        .collect();
    // A database is its own virtual host (AMQP clients name it in the
    // vhost; `/` maps to `amqp.default_vhost_database`). A vhost record
    // exists only when a vhost got limits or a description, so without one
    // the database's name is the vhost once it has any topology (otherwise
    // the queue pages would show "no virtual host yet").
    let vhost_name = vhost
        .as_ref()
        .map(|v| v.vhost_name.clone())
        .or_else(|| (!exchanges.is_empty() || !queues.is_empty()).then(|| db.to_owned()));
    Ok(json_response(
        200,
        &json!({
            "vhost": vhost_name,
            "default_queue_type": vhost.map(|v| v.default_queue_type),
            "exchanges": exchanges,
            "queues": queues,
            "bindings": bindings,
        }),
    ))
}

/// Whether the grants hold `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`.
#[must_use]
pub fn sink_admin(lines: &[String]) -> bool {
    super::grants::Privileges::of(lines).tenant_admin
        || lines.iter().any(|l| {
            let u = l.to_ascii_uppercase();
            u.starts_with("GRANT ") && u.contains("SPRIG_SINK_ADMIN") && u.contains(" ON *.* ")
        })
}

/// A sink's dead letters: listed, or replayed or discarded.
async fn sink_dead(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    name: &str,
    act: Option<(&str, &Value)>,
) -> Result<Response<Body>, SprigError> {
    let lines = grant_lines(core, caller).await?;
    // Sink rows are engine catalog records.
    let (tid, records, db_id, _) = catalog(core, caller, db)?;
    // Replay and discard write the dead letters' states: on the tenant
    // unit's leader, whichever node took the request.
    let records = match (act, &core.studio.tenants) {
        (Some(_), Some(svc)) => Arc::clone(&svc.writers),
        _ => records,
    };
    let store = sprig_sinks::store::SinksStore::new(records, tid);
    let mut sink = None;
    for id in store.ids_named(name).await? {
        if let Some(d) = store.sink(id).await?
            && d.db_id == db_id
        {
            sink = Some((d.id.0, d.owner_user_id));
            break;
        }
    }
    let Some((sink_id, owner)) = sink else {
        return Err(SprigError::new(
            SprigCode::SinkNotFound,
            format!("Unknown sink '{db}.{name}'"),
        ));
    };
    let is_owner = caller.principal.user_id.is_some_and(|u| u.0 == owner);
    if !(is_owner || sink_admin(&lines)) {
        return Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the SPRIG_SINK_ADMIN privilege(s) for this operation, or to own the sink",
        ));
    }
    let id = sprig_sinks::model::SinkId(sink_id);
    if let Some((verb, body)) = act {
        let ids: Vec<u64> = body
            .get("ids")
            .and_then(Value::as_array)
            .map(|xs| {
                xs.iter()
                    .filter_map(|x| {
                        x.as_u64()
                            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let all = body.get("all").and_then(Value::as_bool).unwrap_or(false);
        if ids.is_empty() && !all {
            return Err(invalid(
                "name the dead letters (ids) or ask for all of them",
            ));
        }
        let state = if verb.ends_with(":replay") {
            "replaying"
        } else {
            "discarded"
        };
        let n = store.mark_dead(id, &ids, state).await?;
        return Ok(json_response(200, &json!({"marked": n, "state": state})));
    }
    let rows = store.dead_letters(id, MAX_DEAD).await?;
    let list: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({"id": r.id, "key": r.idem_key, "reason": r.reason, "class": r.class,
                   "attempts": r.attempts, "state": r.state, "at": r.created_at})
        })
        .collect();
    Ok(json_response(
        200,
        &json!({"sink": name, "dead_letters": list}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messaging_paths() {
        assert!(is_messaging_path("/v1/databases/shop/mq"));
        assert!(is_messaging_path(
            "/v1/databases/shop/streams/orders/groups"
        ));
        assert!(is_messaging_path(
            "/v1/databases/shop/sinks/out/deadLetters"
        ));
        assert!(is_messaging_path(
            "/v1/databases/shop/sinks/out/deadLetters:replay"
        ));
        assert!(!is_messaging_path("/v1/databases/shop/streams/orders"));
        // Sinks.Test and Connections.Test.
        assert!(is_messaging_path("/v1/databases/shop/sinks/out:test"));
        assert!(is_messaging_path(
            "/v1/databases/shop/connections/hook:test"
        ));
        assert!(!is_messaging_path("/v1/databases/shop/sinks/out/x:test"));
        assert_eq!(
            test_statement("shop", "sinks", "out", &json!({})).unwrap(),
            "TEST SINK `shop`.`out` WITH SAMPLE 10"
        );
        assert!(test_statement("shop", "sinks", "out", &json!({"sample": 0})).is_err());
        assert_eq!(
            test_statement(
                "shop",
                "connections",
                "h`k",
                &json!({"write": true, "to": "t'1"})
            )
            .unwrap(),
            "TEST CONNECTION `shop`.`h``k` WRITE TO 't''1'"
        );
        assert_eq!(
            test_statement("-", "connections", "kafka_main", &json!({"platform": true})).unwrap(),
            "TEST PLATFORM CONNECTION `kafka_main`"
        );
        assert!(!is_messaging_path("/v1/databases/shop/sql:query"));
        assert_eq!(decode("a%20b%2Fc"), "a b/c");
        assert_eq!(decode("%zz%"), "%zz%");
    }

    #[test]
    fn commit_times_read_as_utc() {
        assert_eq!(utc_text(0), "1970-01-01 00:00:00");
        assert_eq!(utc_text(1_795_801_857_535), "2026-11-27 17:50:57");
        assert_eq!(utc_text(951_782_400_000), "2000-02-29 00:00:00");
        assert_eq!(utc_text(-1000), "1969-12-31 23:59:59");
    }

    #[test]
    fn sink_admins() {
        let l = |xs: &[&str]| xs.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert!(sink_admin(&l(&[
            "GRANT SPRIG_SINK_ADMIN ON *.* TO `a`@`%`"
        ])));
        assert!(sink_admin(&l(&[
            "GRANT SPRIG_TENANT_ADMIN ON *.* TO `a`@`%`"
        ])));
        assert!(!sink_admin(&l(&["GRANT SELECT ON `shop`.* TO `a`@`%`"])));
    }
}
