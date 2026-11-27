//! The gateway's side of `LogicPeer.SinkSql`: a
//! sink task's statements in a session of the sink's owner, opened as the
//! trigger runs are ([`crate::triggers::principal_of`]), through the
//! gateway's SQL engine, so everything a session of the owner checks
//! applies (privileges, column privileges, row-level security, name
//! virtualization).
//!
//! ```text
//! Begin                 SqlEngine::begin(read_only): a transaction handle
//! Read {handle, sql}    one SELECT or WITH (39235 otherwise) in it, capped
//! End {handle}          ROLLBACK
//! Queue {statements}    each CALL sprig.mq_get | mq_ack | mq_nack (39235
//!                       otherwise), autocommit, in order; each statement's
//!                       error is its own result's
//! FanInGet / FanInSettle  a fan-in queue sink's reads of member instances
//! FanInRelease            a dead fan-in task's leases back
//!                       (crate::inherited::fanin_sink)
//! Images {tables}       which row images the user may see: rows
//!                       (t, i, v) of crate::exec::rls_images
//! Publish {statements}  a change exchange's CALL sprig.mq_publish
//!                       calls, autocommit, in order (older workers)
//! PublishBatch {vhost, exchange, messages}
//!                       a change exchange's messages, each as its message
//!                       id, through the queue engine as the user, in order,
//!                       up to the first that fails; to an
//!                       exchange inherited from a declarer, into the
//!                       tenant's own instance, stamped
//! Changes {objects, token, ...}
//!                       a CHANGES OF sink's engine tables' changes from the
//!                       change log (crate::table_changes)
//! StreamRead {db, stream, from, ...}
//!                       a FROM STREAM sink's records: per partition one
//!                       SELECT of the stream as the user (its SELECT
//!                       privilege applies), waiting a little when none
//!                       is there
//! StreamPublish {db, stream, key, value, headers}
//!                       a dead letter: INSERT INTO the stream as the user
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_sinks::peer::{PeerError, SqlOp, SqlReply, SqlRequest, SqlResult};
use tokio::sync::mpsc;

use crate::http::sql::{Caller, Caps, SqlCall, SqlEngine, collect};

/// Replies above this are refused.
const MAX_REPLY: usize = 64 << 20;

/// Serves one request (see the module docs).
pub async fn serve(
    login: &Arc<sprig_auth::LoginService>,
    engine: &Arc<SqlEngine>,
    req: SqlRequest,
) -> SqlReply {
    match Box::pin(serve_inner(login, engine, req)).await {
        Ok(r) => r,
        Err(e) => SqlReply {
            error: Some(PeerError::of(&e)),
            ..SqlReply::default()
        },
    }
}

#[allow(clippy::too_many_lines)] // one arm per operation
async fn serve_inner(
    login: &Arc<sprig_auth::LoginService>,
    engine: &Arc<SqlEngine>,
    req: SqlRequest,
) -> Result<SqlReply> {
    // The feed-driven outbox forwarder's step, internal (no
    // session, no principal).
    if let SqlOp::OutboxForward { schema, table, ids } = &req.op {
        let svc = engine
            .tenants
            .clone()
            .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
        return Box::pin(crate::outbox_feed::serve(
            &svc, req.tenant, schema, table, ids,
        ))
        .await;
    }
    let principal =
        crate::triggers::principal_of(login, req.tenant, &req.tenant_name, req.user_id).await?;
    let caller = Caller {
        principal,
        peer: SocketAddr::from(([127, 0, 0, 1], 0)),
        tls: None,
        listener: "sink",
        generation: 0,
        authorization: None,
        client_dc: None,
    };
    let caps = Caps {
        max_rows: req.max_rows,
        max_bytes: 0,
        timeout: (req.timeout_ms > 0).then(|| std::time::Duration::from_millis(req.timeout_ms)),
    };
    match req.op {
        SqlOp::Begin => {
            let t = engine.begin(&caller, &req.database, true).await?;
            Ok(SqlReply {
                handle: Some(t.name),
                ..SqlReply::default()
            })
        }
        SqlOp::Read { handle, sql } => {
            sprig_sinks::target::script::check_read_only(&sql)?;
            let results = run(engine, &caller, &req.database, sql, Some(&handle), caps).await?;
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::End { handle } => {
            engine.end(&caller, &handle, false).await?;
            Ok(SqlReply::default())
        }
        SqlOp::Queue { statements } => {
            sprig_sinks::peer::check_queue_calls(&statements)?;
            let mut results = Vec::with_capacity(statements.len());
            // Every queue call names its vhost (an argument, or the
            // delivery token's), so the session selects no database: a
            // user granted only the queue (an inherited queue sink's
            // sprig_inherited) may not `USE` the database.
            for sql in statements {
                match run(engine, &caller, "", sql, None, caps).await {
                    Ok(mut r) => results.push(r.pop().unwrap_or_default()),
                    Err(e) => results.push(SqlResult {
                        error: Some(PeerError::of(&e)),
                        ..SqlResult::default()
                    }),
                }
            }
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::FanInGet {
            def_tenant,
            def_object,
            member,
            db,
            queue,
            max,
            lease_ms,
            tag,
        } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::get(
                &svc,
                &caller,
                (def_tenant, def_object),
                member,
                &db,
                &queue,
                max,
                lease_ms,
                tag.as_deref(),
            )
            .await
        }
        // A dead fan-in task's leases in one member.
        SqlOp::FanInRelease {
            def_tenant,
            def_object,
            member,
            db,
            queue,
            tag,
        } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::release(
                &svc,
                &caller,
                (def_tenant, def_object),
                member,
                &db,
                &queue,
                &tag,
            )
            .await
        }
        SqlOp::OutboxForward { .. } => Err(SprigError::internal("served above")),
        // An after-commit trigger's images
        // as its definer may see them, through the marked image check
        // (crate::exec::rls_images), whose nonce only this process knows.
        SqlOp::Images { tables } => {
            let req = crate::exec::rls_images::ImagesRequest {
                tables: tables
                    .into_iter()
                    .map(|t| crate::exec::rls_images::TableImages {
                        db: t.db,
                        table: t.table,
                        images: t.images,
                    })
                    .collect(),
                plain_binary: false,
                watch: false,
            };
            let sql = crate::exec::rls_images::statement(&req);
            let results = run(engine, &caller, "", sql, None, caps).await?;
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        // A change exchange's publishes as the user who
        // set it (its privileges on the exchange apply).
        SqlOp::Publish { statements } => {
            sprig_sinks::peer::check_publish_calls(&statements)?;
            let mut results = Vec::with_capacity(statements.len());
            for sql in statements {
                match run(engine, &caller, "", sql, None, caps).await {
                    Ok(mut r) => results.push(r.pop().unwrap_or_default()),
                    Err(e) => results.push(SqlResult {
                        error: Some(PeerError::of(&e)),
                        ..SqlResult::default()
                    }),
                }
            }
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
        SqlOp::PublishBatch {
            vhost,
            exchange,
            messages,
        } => {
            Box::pin(publish_batch(
                engine, &caller, req.tenant, &vhost, &exchange, messages,
            ))
            .await
        }
        SqlOp::FanInSettle { tokens, ack } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            crate::inherited::fanin_sink::settle(&svc, &caller, &tokens, ack).await
        }
        // A `CHANGES OF` sink's read of its engine tables' changes
        // (crate::table_changes); the owner's SELECT on each table is
        // checked when its reader opens.
        SqlOp::Changes {
            objects,
            token,
            from_ms,
            max_bytes,
            wait_ms,
            consumer,
        } => {
            let svc = engine
                .tenants
                .clone()
                .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
            let consumer = format!("{}/{consumer}", req.tenant);
            let check = |sql: String| {
                let caller = caller.clone();
                async move { run(engine, &caller, "", sql, None, caps).await.map(|_| ()) }
            };
            // Tables under row-level security: each change's images as the
            // owner's policies show them.
            let images = |ireq: crate::exec::rls_images::ImagesRequest| {
                let caller = caller.clone();
                async move {
                    let sql = crate::exec::rls_images::statement(&ireq);
                    let r = run(engine, &caller, "", sql, None, caps).await?;
                    let rows: Vec<Vec<Option<String>>> = r
                        .first()
                        .map(|x| {
                            x.rows
                                .iter()
                                .map(|row| {
                                    (0..row.len()).map(|i| SqlResult::text(row, i)).collect()
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    Ok(crate::exec::rls_images::answer(&ireq, &rows))
                }
            };
            let (result, next) = crate::table_changes::serve(
                &svc,
                req.tenant,
                &objects,
                token.as_deref(),
                from_ms,
                max_bytes,
                wait_ms,
                &consumer,
                check,
                images,
            )
            .await?;
            Ok(SqlReply {
                results: vec![result],
                next: Some(next),
                ..SqlReply::default()
            })
        }
        SqlOp::StreamRead {
            db,
            stream,
            from,
            max_records,
            wait_ms,
        } => {
            Box::pin(stream_read(
                engine,
                &caller,
                caps,
                &db,
                &stream,
                &from,
                max_records,
                wait_ms,
            ))
            .await
        }
        SqlOp::StreamPublish {
            db,
            stream,
            key,
            value,
            headers,
        } => {
            let sql = publish_text(&db, &stream, key.as_deref(), value.as_deref(), &headers)?;
            let results = run(engine, &caller, "", sql, None, caps).await?;
            Ok(SqlReply {
                results,
                ..SqlReply::default()
            })
        }
    }
}

fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `X'..'` of base64 `b64` (`NULL` for none).
fn hex_of(b64: Option<&str>) -> Result<String> {
    use base64::Engine as _;
    use std::fmt::Write as _;
    let Some(b) = b64 else {
        return Ok("NULL".into());
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b)
        .map_err(|_| SprigError::internal("a dead letter's bytes aren't base64"))?;
    let mut h = String::with_capacity(bytes.len() * 2 + 3);
    h.push_str("X'");
    for x in bytes {
        let _ = write!(h, "{x:02X}");
    }
    h.push('\'');
    Ok(h)
}

/// The `INSERT INTO` of a dead letter ([`SqlOp::StreamPublish`]).
fn publish_text(
    db: &str,
    stream: &str,
    key: Option<&str>,
    value: Option<&str>,
    headers: &[(String, String)],
) -> Result<String> {
    use base64::Engine as _;
    let h: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    let h = serde_json::Value::Object(h).to_string();
    Ok(format!(
        "INSERT INTO {}.{} (`key`, `value`, `headers`) VALUES ({}, {}, {})",
        ident(db),
        ident(stream),
        hex_of(key)?,
        hex_of(value)?,
        hex_of(Some(
            &base64::engine::general_purpose::STANDARD.encode(h.as_bytes())
        ))?
    ))
}

/// A stream sink's read ([`SqlOp::StreamRead`]): each partition's records
/// from its offset (or its first record at or after the start time), up
/// to `max` each, as one result; with none there, read again every 100 ms
/// until `wait_ms` has passed.
#[allow(clippy::too_many_arguments)]
async fn stream_read(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    caps: Caps,
    db: &str,
    stream: &str,
    from: &[sprig_sinks::peer::StreamFrom],
    max: u32,
    wait_ms: u64,
) -> Result<SqlReply> {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(wait_ms.min(30_000));
    let table = format!("{}.{}", ident(db), ident(stream));
    loop {
        let mut out = SqlResult::default();
        for f in from {
            let start = match f.offset {
                Some(o) => format!("offset_no >= {o}"),
                None if f.from_ms > 0 => format!(
                    "ts >= '{}'",
                    sprig_sinks::stream_source::utc_text(f.from_ms)
                ),
                None => "TRUE".into(),
            };
            let sql = format!(
                "SELECT partition_no, offset_no, ts, `key`, `value`, headers FROM {table} \
                 WHERE partition_no = {} AND {start} ORDER BY offset_no LIMIT {}",
                f.partition,
                max.max(1)
            );
            let mut r = run(engine, caller, "", sql, None, caps).await?;
            let Some(r) = r.pop() else {
                continue;
            };
            if out.columns.is_empty() {
                out.columns = r.columns;
                out.types = r.types;
            }
            out.rows.extend(r.rows);
        }
        if !out.rows.is_empty() || std::time::Instant::now() >= deadline {
            return Ok(SqlReply {
                results: vec![out],
                ..SqlReply::default()
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A change exchange's batch ([`SqlOp::PublishBatch`]): the messages
/// through the queue engine as the caller (`sprig.mq_publish`'s path
/// outside a transaction, without the statement), each as its message
/// id. One result per message up to the first that failed; `affected`:
/// the queues it reached.
async fn publish_batch(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    tenant: u64,
    vhost: &str,
    exchange: &str,
    messages: Vec<sprig_sinks::peer::PublishMessage>,
) -> Result<SqlReply> {
    let svc = engine
        .tenants
        .clone()
        .ok_or_else(|| SprigError::internal("this gateway serves no tenant catalogs"))?;
    let mq_err = |e: &sprig_proto_amqp::error::MqError| {
        let mut s = SprigError::standard(
            u16::try_from(e.code.number()).unwrap_or(39250),
            *b"HY000",
            format!("{}: {}", e.code.name(), e.text),
        );
        s.retryable = true;
        s
    };
    // An exchange inherited from a declarer: the messages go into this
    // tenant's own instance, stamped as `sprig.mq_publish` does there,
    // each as its id. A batch with a message that names another tenant, or that `sprig.mq_publish` would refuse, takes
    // the statement path as before: at least once.
    let my_name = caller.principal.tenant_name.clone().unwrap_or_default();
    let own = match inherited_exchange(&svc, (tenant, &my_name), vhost, exchange, &messages).await?
    {
        Inherited::No => None,
        Inherited::Own(own) => Some(own),
        Inherited::Statements => {
            return publish_statements(engine, caller, vhost, exchange, messages).await;
        }
    };
    let mq = svc.mq.get().cloned().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::ForwardFailed,
            "the queue engine isn't running on this gateway (AMQP is off)",
        );
        e.retryable = true;
        e
    })?;
    let p = &caller.principal;
    let principal = Arc::new(sprig_proto_amqp::auth::Principal {
        tenant_id: tenant,
        tenant: p.tenant_name.clone().unwrap_or_default(),
        user: p.user.clone(),
        user_id: p.user_id.map(|u| u64::from(u.0)),
        handle: Some(Arc::new(p.clone())),
    });
    let v = mq
        .sql_vhost(principal, vhost, 0)
        .await
        .map_err(|e| mq_err(&e))?;
    let mut msgs = Vec::with_capacity(messages.len());
    for m in messages {
        let id = sprig_sinks::peer::publish_id(&m.id).ok_or_else(|| {
            SprigError::internal(format!("message id {:?} isn't 32 hex digits", m.id))
        })?;
        let mut msg = sprig_queues::sql::message(
            exchange,
            &m.routing_key,
            m.body.into_bytes(),
            m.properties.as_deref(),
        )
        .map_err(|e| mq_err(&e))?;
        if let Some(own) = &own
            && own.names(&msg)
        {
            msg = own.stamp(&msg)?;
        }
        msgs.push((msg, id));
    }
    if let Some(own) = &own {
        // The member's instance of the inherited definitions, as a
        // publish there makes it.
        Box::pin(crate::inherited::mq::ensure_vhost(
            &svc,
            &own.chain,
            &own.db,
            sprig_inherit::msg::Cause::Publish,
        ))
        .await?;
    }
    let results = v
        .publish_batch_once(msgs)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(n) => SqlResult {
                affected: u64::from(n),
                ..SqlResult::default()
            },
            Err(e) => SqlResult {
                error: Some(PeerError::of(&mq_err(&e))),
                ..SqlResult::default()
            },
        })
        .collect();
    Ok(SqlReply {
        results,
        ..SqlReply::default()
    })
}

/// How a batch's messages meet the inherited definitions
/// `sprig.mq_publish` routes itself (`exec::inherit_msg`).
enum Inherited {
    /// None of them names one.
    No,
    /// Some name one that reaches this tenant, without naming another
    /// tenant: they go into this tenant's own instance, stamped.
    Own(OwnInstance),
    /// One names another tenant (or every member), or a definition that
    /// doesn't reach this tenant: the statement path decides.
    Statements,
}

/// A member's own instance of inherited definitions, for a batch.
struct OwnInstance {
    chain: sprig_inherit::msg::Chain,
    /// The vhost's database.
    db: String,
    /// This tenant's name, stamped on its messages.
    me: String,
}

impl OwnInstance {
    /// Whether `m` names an inherited definition.
    fn names(&self, m: &sprig_proto_amqp::message::Message) -> bool {
        crate::inherited::mq::named_def(&self.chain, &self.db, &m.exchange, &m.routing_key)
            .is_some()
    }

    /// `m` with the tenant headers set to this tenant, its routing key
    /// without an `@me:` prefix, as `exec::inherit_msg` stamps a publish
    /// into a tenant's own instance.
    fn stamp(
        &self,
        m: &sprig_proto_amqp::message::Message,
    ) -> Result<sprig_proto_amqp::message::Message> {
        let key =
            crate::inherited::mq::target_of(m).map_or_else(|| m.routing_key.clone(), |(_, k)| k);
        crate::inherited::mq::stamp(m, &key, &self.me, &self.me)
            .map_err(|e| SprigError::standard(3140, *b"22032", e.text))
    }
}

/// How `messages` to `exchange` of `vhost` meet the inherited
/// definitions (see [`Inherited`]): the cases of
/// `PassSession::inherit_mq_publish`, decided for the whole batch.
async fn inherited_exchange(
    svc: &Arc<crate::tenant::TenantServices>,
    (tenant, my_name): (u64, &str),
    vhost: &str,
    exchange: &str,
    messages: &[sprig_sinks::peer::PublishMessage],
) -> Result<Inherited> {
    let cat = svc
        .cache
        .get(
            sprig_common::ids::TenantId(tenant),
            sprig_catalog::Freshness::Latest,
        )
        .await?;
    if crate::inherited::state_of(&cat).is_empty() && cat.ancestry.is_empty() {
        return Ok(Inherited::No);
    }
    let db = sprig_proto_amqp::mem::default_database(
        vhost,
        &svc.settings.get(
            &crate::amqp::DEFAULT_VHOST_DATABASE,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        ),
    );
    let chain = crate::inherited::chain(svc, &cat).await?;
    let me = cat.tenant;
    let reaching = crate::inherited::mq::defs_in(&chain, &db);
    let mut any = false;
    for m in messages {
        let msg = sprig_queues::sql::message(
            exchange,
            &m.routing_key,
            m.body.as_bytes().to_vec(),
            m.properties.as_deref(),
        )
        .map_err(|e| SprigError::internal(e.text))?;
        let Some((kind, name)) =
            crate::inherited::mq::named_def(&chain, &db, &msg.exchange, &msg.routing_key)
        else {
            continue;
        };
        let mine = reaching.iter().any(|d| d.kind == kind && d.name == name);
        let other = crate::inherited::mq::target_of(&msg)
            .is_some_and(|(t, _)| t != my_name && t != me.0.to_string());
        if !mine || other {
            return Ok(Inherited::Statements);
        }
        any = true;
    }
    if !any {
        return Ok(Inherited::No);
    }
    Ok(Inherited::Own(OwnInstance {
        chain,
        db,
        me: my_name.to_owned(),
    }))
}

/// [`publish_batch`] as `CALL sprig.mq_publish` statements, one by one, up
/// to the first that fails.
async fn publish_statements(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    vhost: &str,
    exchange: &str,
    messages: Vec<sprig_sinks::peer::PublishMessage>,
) -> Result<SqlReply> {
    let lit = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
    let caps = Caps {
        max_rows: 0,
        max_bytes: 0,
        timeout: None,
    };
    let mut results = Vec::with_capacity(messages.len());
    for m in messages {
        let sql = format!(
            "CALL sprig.mq_publish({}, {}, {}, {}, 0, {})",
            lit(exchange),
            lit(&m.routing_key),
            lit(&m.body),
            m.properties
                .as_deref()
                .map_or_else(|| "NULL".to_owned(), lit),
            lit(vhost),
        );
        let r = match run(engine, caller, "", sql, None, caps).await {
            Ok(mut r) => r.pop().unwrap_or_default(),
            Err(e) => SqlResult {
                error: Some(PeerError::of(&e)),
                ..SqlResult::default()
            },
        };
        let failed = r.error.is_some();
        results.push(r);
        if failed {
            break;
        }
    }
    Ok(SqlReply {
        results,
        ..SqlReply::default()
    })
}

/// Runs one statement: its results, cells as bytes.
async fn run(
    engine: &Arc<SqlEngine>,
    caller: &Caller,
    database: &str,
    sql: String,
    txn: Option<&str>,
    caps: Caps,
) -> Result<Vec<SqlResult>> {
    let (tx, rx) = mpsc::channel(16);
    let call = SqlCall {
        database: if database.is_empty() {
            "-".to_owned()
        } else {
            database.to_owned()
        },
        sql,
        multi: false,
    };
    let resp = {
        let engine = Arc::clone(engine);
        let caller = caller.clone();
        let txn = txn.map(str::to_owned);
        let task = tokio::spawn(async move {
            engine
                .run_capped(&caller, call, txn.as_deref(), caps, tx)
                .await;
        });
        let resp = collect(rx, MAX_REPLY, None).await;
        let _ = task.await;
        resp?
    };
    Ok(resp.results.iter().map(result_of).collect())
}

/// An API result as a peer result: each cell's bytes (binary columns
/// decoded from base64).
fn result_of(r: &sprig_api::proto::v1::Result) -> SqlResult {
    let binary: Vec<bool> = r
        .columns
        .iter()
        .map(|c| sprig_api::results::is_binary(c.mysql_type, c.charset))
        .collect();
    SqlResult {
        columns: r.columns.iter().map(|c| c.name.clone()).collect(),
        types: r.columns.iter().map(|c| c.mysql_type).collect(),
        rows: r
            .rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let b = sprig_api::results::cell_bytes(
                            v,
                            binary.get(i).copied().unwrap_or(false),
                        );
                        SqlResult::cell(b.as_deref())
                    })
                    .collect()
            })
            .collect(),
        affected: r.affected_rows,
        error: None,
    }
}
