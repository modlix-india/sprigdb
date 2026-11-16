//! Triggers on streams and queues (events go on streams, work goes on
//! queues):
//!
//! ```sql
//! CREATE TRIGGER sync_users ON STREAM security.user_events LANGUAGE javascript AS $$
//!   export default function (ctx) { const e = JSON.parse(ctx.record.value); ... }
//! $$;
//! CREATE TRIGGER bill ON QUEUE billing LANGUAGE lua AS $$ ... $$;
//! ```
//!
//! **Deploy** ([`deploy`]): the module (kind `trigger`, always the
//! definer's rights), an engine-tier version, a `logic_bindings` row with
//! timing `on_message` and the stream or queue as its object, a
//! [`logic_message_sources`](DDL_MESSAGE_SOURCES) row naming it, and an
//! index entry `logic/msg/<tenant>/<binding>` in the DC tier, which the
//! controller's `logic` loop turns into one long-running job:
//!
//! ```text
//! logic_stream_trigger (worker): per partition, in offset order, from the
//!   committed offsets of the consumer group __sprig_logic_<binding>:
//!     a gateway runs, in one transaction of a session as the definer:
//!       commit_offset(group, stream, p, o + 1, expect o)   -- locks the row
//!       the version, with ctx.record
//!     COMMIT; a second run of the same record finds the offset moved
//!     (39877) and rolls back: exactly once for the code's writes
//!   failure: retry with growing waits; after logic.trigger_max_attempts
//!     → logic_dead_letters (event key p<partition>:<offset>), and the
//!     offset moves past the record
//! logic_queue_trigger (worker): logic.trigger_lanes loops, each:
//!     a gateway, in a session as the definer: mq_get(queue, 1); in one
//!     transaction: mq_ack(token) (the row delete), the version with
//!     ctx.message; COMMIT
//!   failure: ROLLBACK, the delivery requeued as a failed one (its
//!     x-delivery-count rises); once it has failed
//!     logic.trigger_max_attempts times: dead-lettered through the queue's
//!     dead-letter exchange, or into logic_dead_letters when it has none
//! ```
//!
//! **Where a version runs.** JavaScript compiles into MariaDB when the
//! compiler can keep its meaning ([`place`]): the version's entry
//! procedure in the trigger database's shard schema takes the fields of
//! `ctx.record` or `ctx.message` the code reads, and the gateway calls it
//! in the definer's session, in the run's transaction, after a savepoint.
//! A record whose parsed JSON the compiled code can't treat as JavaScript
//! does (a member of a kind it wasn't compiled for, text `JSON.parse`
//! refuses) raises 39876; the gateway rolls back to the savepoint and runs
//! that record in the engine tier. Anything else runs in the engine tier
//! (QuickJS, the native KIRun runtime, Lua), in a gateway, through the
//! definer's own session: name virtualization, privilege checks,
//! `db.publish` and `db.mq.*` all work as for a client, with the reason
//! recorded at deploy. JavaScript and Lua code gets one argument,
//! `ctx` (also the global `ctx`): `ctx.record {stream, partition, offset,
//! key, value, headers, timestamp}` or `ctx.message {queue, exchange,
//! routingKey, body, properties, redelivered, deliveryCount}`. A KIRun
//! definition's parameters bind by name to those fields, or to `record`,
//! `message` and `ctx` for the whole object.

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{Migrator, Store};
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, Key, ListOpts, MetaStore, Record, Versioned};

use crate::catalog::{BindingRow, LOGIC_MODULE, LogicStore, ModuleRow, NewVersion};
use crate::declare::{Declaration, Kind, SourceKind};

/// Migration 3: `logic_bindings.timing` gains `on_message`.
pub const DDL_TIMING_ON_MESSAGE: &str = "ALTER TABLE logic_bindings MODIFY timing \
     ENUM('before_write','after_commit','on_read','on_write','on_message') NOT NULL";

/// Migration 3: what each `on_message` binding reads.
pub const DDL_MESSAGE_SOURCES: &str = "CREATE TABLE IF NOT EXISTS logic_message_sources (
  binding_id        BIGINT UNSIGNED NOT NULL,
  module_id         BIGINT UNSIGNED NOT NULL,
  kind              ENUM('stream','queue') NOT NULL,
  db_id             INT UNSIGNED    NOT NULL,
  db_name           VARCHAR(64)     NOT NULL,
  name              VARCHAR(255)    NOT NULL,
  object_id         BIGINT UNSIGNED NOT NULL,
  definer_user_id   INT UNSIGNED    NOT NULL,
  tenant_name       VARCHAR(255)    NOT NULL,
  PRIMARY KEY (binding_id),
  KEY by_module (module_id)
) ENGINE=InnoDB";

/// Migration 4: a fan-in trigger's definition is named by
/// `fanin_declarer` and `object_id` = [`FANIN_SOURCE_FLAG`] | the
/// definition's object id, so any 64-bit declarer id works. Earlier rows
/// packed the declarer into `object_id` (declarers below 2^38); the two
/// updates rewrite them, the binding's `object_id` first (it is read
/// through the source row's old value), each once (`fanin_declarer = 0`).
pub const DDL_SOURCES_FANIN_DECLARER: &str = "ALTER TABLE logic_message_sources \
     ADD COLUMN IF NOT EXISTS fanin_declarer BIGINT UNSIGNED NOT NULL DEFAULT 0";
/// Migration 4: the bindings of packed fan-in sources.
pub const DML_BINDINGS_FANIN_UNPACK: &str = "UPDATE logic_bindings b \
     JOIN logic_message_sources s ON s.binding_id = b.binding_id \
     SET b.object_id = (1 << 62) | (s.object_id & 16777215) \
     WHERE s.fanin_declarer = 0 AND (s.object_id & (1 << 62)) <> 0 AND (s.object_id >> 63) = 0";
/// Migration 4: packed fan-in sources (MariaDB assigns left to right, so
/// `fanin_declarer` reads the old `object_id`).
pub const DML_SOURCES_FANIN_UNPACK: &str = "UPDATE logic_message_sources \
     SET fanin_declarer = (object_id & ~(1 << 62)) >> 24, \
         object_id = (1 << 62) | (object_id & 16777215) \
     WHERE fanin_declarer = 0 AND (object_id & (1 << 62)) <> 0 AND (object_id >> 63) = 0";

/// The job kind of a trigger on a stream.
pub const STREAM_JOB_KIND: &str = "logic_stream_trigger";
/// The job kind of a trigger on a queue.
pub const QUEUE_JOB_KIND: &str = "logic_queue_trigger";

/// A `logic_message_sources` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRow {
    /// The binding.
    pub binding_id: u64,
    /// Its module.
    pub module_id: u64,
    /// Stream or queue.
    pub kind: SourceKind,
    /// The stream's database, or the queue's vhost database.
    pub db_id: u32,
    /// Its name.
    pub db_name: String,
    /// The stream or queue.
    pub name: String,
    /// The stream's or queue's id.
    pub object_id: u64,
    /// Who the trigger runs as.
    pub definer_user_id: u32,
    /// The tenant's name (a definer session is opened by name).
    pub tenant_name: String,
    /// A fan-in trigger's definition's declaring tenant; 0 otherwise
    /// (migration 4).
    #[serde(default)]
    pub fanin_declarer: u64,
}

impl SourceRow {
    /// `(declarer, object)` of a fan-in trigger's definition; `None` for
    /// an ordinary trigger. Rows written before migration 4 carry both in
    /// `object_id`.
    #[must_use]
    pub fn fanin_definition(&self) -> Option<(u64, u64)> {
        if self.object_id & FANIN_SOURCE_FLAG == 0 || self.object_id >> 63 != 0 {
            return None;
        }
        if self.fanin_declarer != 0 {
            return Some((self.fanin_declarer, self.object_id & !FANIN_SOURCE_FLAG));
        }
        legacy_fanin_parts(self.object_id)
    }
}

impl<'de> Deserialize<'de> for SourceKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "stream" => Ok(SourceKind::Stream),
            "queue" => Ok(SourceKind::Queue),
            _ => Err(serde::de::Error::custom(format!("unknown source kind {s}"))),
        }
    }
}

fn map_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the logic catalog is unreachable",
    )
    .with_source(e)
}

/// Writes (or replaces) a binding's source.
///
/// # Errors
/// 39070 or a backend error.
pub async fn put_source(c: &mut Conn, schema: &str, s: &SourceRow) -> Result<()> {
    c.exec_drop(
        format!(
            "REPLACE INTO {}.logic_message_sources (binding_id, module_id, kind, db_id, db_name, \
             name, object_id, definer_user_id, tenant_name, fanin_declarer) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            quote(schema)
        ),
        mysql_async::Params::Positional(vec![
            s.binding_id.into(),
            s.module_id.into(),
            s.kind.as_str().into(),
            s.db_id.into(),
            s.db_name.as_str().into(),
            s.name.as_str().into(),
            s.object_id.into(),
            s.definer_user_id.into(),
            s.tenant_name.as_str().into(),
            s.fanin_declarer.into(),
        ]),
    )
    .await
    .map_err(map_err)
}

/// A binding's source; `None` for a binding that has none (or a catalog
/// without the table).
///
/// # Errors
/// 39070 or a backend error other than a missing table.
pub async fn source(c: &mut Conn, schema: &str, binding_id: u64) -> Result<Option<SourceRow>> {
    type T = (u64, u64, String, u32, String, String, u64, u32, String, u64);
    let q = |declarer: &str| {
        format!(
            "SELECT binding_id, module_id, kind, db_id, db_name, name, object_id, \
             definer_user_id, tenant_name, {declarer} FROM {}.logic_message_sources \
             WHERE binding_id = ?",
            quote(schema)
        )
    };
    let mut r: std::result::Result<Option<T>, _> =
        c.exec_first(q("fanin_declarer"), (binding_id,)).await;
    if matches!(&r, Err(mysql_async::Error::Server(s)) if s.code == 1054) {
        // A catalog before migration 4 (its rows decode the old way).
        r = c.exec_first(q("0"), (binding_id,)).await;
    }
    let row = match r {
        Ok(r) => r,
        Err(mysql_async::Error::Server(s)) if s.code == 1146 => return Ok(None),
        Err(e) => return Err(map_err(e)),
    };
    Ok(row.map(
        |(
            binding_id,
            module_id,
            kind,
            db_id,
            db_name,
            name,
            object_id,
            definer,
            tenant_name,
            fanin_declarer,
        )| {
            SourceRow {
                binding_id,
                module_id,
                kind: if kind == "queue" {
                    SourceKind::Queue
                } else {
                    SourceKind::Stream
                },
                db_id,
                db_name,
                name,
                object_id,
                definer_user_id: definer,
                tenant_name,
                fanin_declarer,
            }
        },
    ))
}

// ---- the index the controller reads ----------------------------------------

/// `logic/msg/<tenant>/<binding>` in the DC tier: a trigger on a stream or
/// queue that should have a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsgIndex {
    /// The tenant.
    pub tenant: TenantId,
    /// The binding.
    pub binding: u64,
    /// `stream` or `queue`.
    pub kind: SourceKind,
}

impl Versioned for MsgIndex {
    const V: u32 = 1;
}

impl MsgIndex {
    /// The job kind this entry runs as.
    #[must_use]
    pub const fn job_kind(&self) -> &'static str {
        match self.kind {
            SourceKind::Stream => STREAM_JOB_KIND,
            SourceKind::Queue => QUEUE_JOB_KIND,
        }
    }
}

// ---------------------------------------------------------------------------
// Fan-in triggers:
// `CREATE TRIGGER g ON STREAM s FAN IN` in tenant P runs once, in P, as P's
// definer, over every member instance of the stream definition `s` below
// P. Its source row's `object_id` carries the definition
// ([`fanin_source_id`]); `logic-fanin/<P>/<binding>/<member>` in the DC
// tier lists the instances it reads (written by gateways when the trigger
// is declared and when an instance below P is made), and
// `inherit-fanin-triggers/<def>/<P>.<binding>` lists the triggers of a
// definition for that. Positions: the trigger's group in P's catalog, with
// `instance_tenant_id` = the member.
// ---------------------------------------------------------------------------

/// Marks a fan-in trigger's source `object_id`.
pub const FANIN_SOURCE_FLAG: u64 = 1 << 62;
const FANIN_OBJECT_BITS: u32 = 24;

/// A fan-in trigger's source `object_id` for its definition's object id
/// `object` (the declarer goes into `fanin_declarer`); `None`
/// for an id that doesn't fit below the flag.
#[must_use]
pub fn fanin_source_id(object: u64) -> Option<u64> {
    (object < FANIN_SOURCE_FLAG).then_some(FANIN_SOURCE_FLAG | object)
}

/// `(declarer, object)` of a source `object_id` written before migration
/// 4, which packed the declarer (below 2^38) above a 24-bit object id.
#[must_use]
fn legacy_fanin_parts(id: u64) -> Option<(u64, u64)> {
    (id & FANIN_SOURCE_FLAG != 0 && id & (1 << 63) == 0).then_some((
        (id & !FANIN_SOURCE_FLAG) >> FANIN_OBJECT_BITS,
        id & ((1 << FANIN_OBJECT_BITS) - 1),
    ))
}

/// `logic-fanin/<P>/<binding>/<member>`: one instance a fan-in trigger reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanInMember {
    /// The trigger's tenant.
    pub tenant: TenantId,
    /// The binding.
    pub binding: u64,
    /// The member.
    pub member: TenantId,
    /// The instance's stream id in the member.
    pub stream: u64,
}

impl Versioned for FanInMember {
    const V: u32 = 1;
}

/// `inherit-fanin-triggers/<def tenant>.<def object>/<P>.<binding>`: a
/// fan-in trigger of a definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanInTrigger {
    /// The trigger's tenant.
    pub tenant: TenantId,
    /// The binding.
    pub binding: u64,
}

impl Versioned for FanInTrigger {
    const V: u32 = 1;
}

async fn put_rec<T: Versioned + Serialize + Clone>(
    store: &dyn MetaStore,
    key: Key,
    v: &T,
) -> Result<()> {
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key,
                value: Record::new(v.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Lists a fan-in trigger under its definition.
///
/// # Errors
/// The store's.
pub async fn put_fanin_trigger(
    store: &dyn MetaStore,
    def: (u64, u64),
    t: &FanInTrigger,
) -> Result<()> {
    let key = store.tier().key(&format!(
        "inherit-fanin-triggers/{}.{}/{}.{}",
        def.0, def.1, t.tenant, t.binding
    ))?;
    put_rec(store, key, t).await
}

/// The fan-in triggers of definition `def`.
///
/// # Errors
/// The store's.
pub async fn fanin_triggers(store: &dyn MetaStore, def: (u64, u64)) -> Result<Vec<FanInTrigger>> {
    let prefix = store
        .tier()
        .key(&format!("inherit-fanin-triggers/{}.{}/", def.0, def.1))?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| {
            Record::<FanInTrigger>::decode(&kv.value)
                .ok()
                .map(|r| r.body)
        })
        .collect())
}

/// Lists an instance a fan-in trigger reads.
///
/// # Errors
/// The store's.
pub async fn put_fanin_member(store: &dyn MetaStore, m: &FanInMember) -> Result<()> {
    let key = store.tier().key(&format!(
        "logic-fanin/{}/{}/{}",
        m.tenant, m.binding, m.member
    ))?;
    put_rec(store, key, m).await
}

/// The instances fan-in trigger `binding` of `tenant` reads.
///
/// # Errors
/// The store's.
pub async fn fanin_members(
    store: &dyn MetaStore,
    tenant: TenantId,
    binding: u64,
) -> Result<Vec<FanInMember>> {
    let prefix = store
        .tier()
        .key(&format!("logic-fanin/{tenant}/{binding}/"))?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 1_000_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| {
            Record::<FanInMember>::decode(&kv.value)
                .ok()
                .map(|r| r.body)
        })
        .collect())
}

/// Removes a fan-in trigger's keys (its instance list and its entry under
/// its definition), when it is gone.
///
/// # Errors
/// The store's.
pub async fn delete_fanin(
    store: &dyn MetaStore,
    tenant: TenantId,
    binding: u64,
    def: (u64, u64),
) -> Result<()> {
    let mut ops = Vec::new();
    for m in fanin_members(store, tenant, binding).await? {
        ops.push(sprig_meta::Op::Delete(
            store
                .tier()
                .key(&format!("logic-fanin/{tenant}/{binding}/{}", m.member))?,
        ));
    }
    ops.push(sprig_meta::Op::Delete(store.tier().key(&format!(
        "inherit-fanin-triggers/{}.{}/{tenant}.{binding}",
        def.0, def.1
    ))?));
    for chunk in ops.chunks(64) {
        store
            .txn(sprig_meta::Txn {
                when: Vec::new(),
                then: chunk.to_vec(),
                otherwise: Vec::new(),
            })
            .await?;
    }
    Ok(())
}

fn index_key(store: &dyn MetaStore, tenant: TenantId, binding: u64) -> Result<Key> {
    store.tier().key(&format!("logic/msg/{tenant}/{binding}"))
}

/// Writes a trigger's index entry.
///
/// # Errors
/// The store's.
pub async fn put_index(store: &dyn MetaStore, e: &MsgIndex) -> Result<()> {
    let key = index_key(store, e.tenant, e.binding)?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key,
                value: Record::new(e.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Removes a trigger's index entry (its job ends at its next check).
///
/// # Errors
/// The store's.
pub async fn delete_index(store: &dyn MetaStore, tenant: TenantId, binding: u64) -> Result<()> {
    let key = index_key(store, tenant, binding)?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Whether a trigger's index entry exists.
///
/// # Errors
/// The store's.
pub async fn indexed(store: &dyn MetaStore, tenant: TenantId, binding: u64) -> Result<bool> {
    let key = index_key(store, tenant, binding)?;
    Ok(store.get(&key, Consistency::Linearizable).await?.is_some())
}

/// Every index entry.
///
/// # Errors
/// The store's.
pub async fn list_index(store: &dyn MetaStore) -> Result<Vec<MsgIndex>> {
    let prefix = store.tier().key("logic/msg/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<MsgIndex>::decode(&kv.value).ok().map(|r| r.body))
        .collect())
}

// ---- deploy ----------------------------------------------------------------

/// Where a trigger on a stream or queue deploys, and what it reads.
pub struct MsgTarget<'a> {
    /// The tenant's catalog writer.
    pub catalog: &'a mut Conn,
    /// `t<tenant>_sprig`.
    pub catalog_schema: &'a str,
    /// The catalog version the session saw.
    pub catalog_version: u64,
    /// The trigger's database.
    pub db_id: u32,
    /// The deploying user (the definer).
    pub user_id: u32,
    /// The tenant's name.
    pub tenant_name: &'a str,
    /// The source's database id and name.
    pub source_db: (u32, &'a str),
    /// The stream's or queue's id.
    pub object_id: u64,
    /// A fan-in trigger's definition's declaring tenant; 0 otherwise.
    pub fanin_declarer: u64,
    /// What the compiled tier needs; `None` keeps the version in the
    /// engine tier (an inherited trigger's instance).
    pub compile: Option<MsgCompile<'a>>,
}

/// Where a trigger's compiled routines go, and what the compiler reads.
pub struct MsgCompile<'a> {
    /// The trigger database's shards, each through `sprig_ctl`.
    pub shards: &'a mut dyn crate::deploy::ShardSessions,
    /// Whether that database's group is always one shard.
    pub single_shard: bool,
    /// The definer's backend account, `` `t1_u7`@`%` ``: the routines'
    /// `DEFINER`.
    pub definer: String,
    /// The trigger database's name.
    pub db_name: &'a str,
    /// Its streams, for a compiled `db.publish`.
    pub streams: crate::service::StreamList,
    /// `logic.max_loop_iterations`.
    pub max_loop_iterations: u64,
    /// `logic.compile_max_cost`.
    pub compile_max_cost: u64,
}

/// The error number a compiled trigger raises for a record it hands to
/// the engine tier.
pub const ENGINE_RERUN: u16 = sprig_logic_compile::ir::ENGINE_RERUN;

/// A trigger version's compiled entry, from its signature
/// (`logic_versions.signature["compiled"]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledEntry {
    /// The fields of `ctx.record` or `ctx.message` it takes, in order:
    /// the parameter's name (the field's, in lower case) and IR type name.
    pub params: Vec<(String, String)>,
    /// It returns its log lines (`p__log`).
    pub log: bool,
}

impl CompiledEntry {
    /// The entry of a version whose signature `sig` records one.
    #[must_use]
    pub fn of(sig: &Value) -> Option<Self> {
        let c = sig.get("compiled")?;
        Some(Self {
            params: c["params"]
                .as_array()?
                .iter()
                .map(|p| {
                    (
                        p["name"].as_str().unwrap_or_default().to_owned(),
                        p["type"].as_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect(),
            log: c["log"].as_bool().unwrap_or(false),
        })
    }

    /// The `CALL` of `module`'s version `version` in database `db`, with
    /// `event`'s fields: the session variables `@__sprig_e`, `@__sprig_r`
    /// (and `@__sprig_l`) get the event, the result and the log lines.
    #[must_use]
    pub fn call(&self, db: &str, module: u64, version: u64, event: &Value) -> String {
        let field = |name: &str| {
            event
                .as_object()
                .and_then(|o| o.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)))
                .map_or(Value::Null, |(_, v)| v.clone())
        };
        let mut args: Vec<String> = self
            .params
            .iter()
            .map(|(n, t)| match (t.as_str(), field(n)) {
                (_, Value::Null) => "NULL".to_owned(),
                ("json", v) => sprig_logic_js::bridge::literal(&Value::String(v.to_string())),
                (_, v) => sprig_logic_js::bridge::literal(&v),
            })
            .collect();
        args.push("@__sprig_e".to_owned());
        args.push("@__sprig_r".to_owned());
        if self.log {
            args.push("@__sprig_l".to_owned());
        }
        format!(
            "CALL {}.{}({})",
            quote(db),
            quote(&sprig_logic_compile::plsql::names(module, version).1),
            args.join(", ")
        )
    }
}

/// What [`deploy`] did.
#[derive(Debug, Clone, Serialize)]
pub struct Deployed {
    /// The module.
    pub module_id: u64,
    /// The new version.
    pub version: u64,
    /// The binding.
    pub binding_id: u64,
    /// The source row.
    pub source: SourceRow,
    /// A new binding (not `OR REPLACE` of an existing one).
    pub created: bool,
    /// `mariadb` or `engine`.
    pub tier: String,
    /// Why the engine tier, when it is.
    pub reasons: Vec<String>,
}

impl Deployed {
    /// Notes for the client.
    #[must_use]
    pub fn notes(&self) -> Vec<String> {
        let what = match self.source.kind {
            SourceKind::Stream => format!(
                "trigger on stream {}.{}: binding {}, consumer group {}",
                self.source.db_name,
                self.source.name,
                self.binding_id,
                sprig_streams_group(self.binding_id)
            ),
            SourceKind::Queue => format!(
                "trigger on queue {} of vhost {}: binding {}, a competing consumer",
                self.source.name, self.source.db_name, self.binding_id
            ),
        };
        let mut v = vec![if self.tier == "mariadb" {
            format!(
                "trigger module {} version {}: tier mariadb (compiled; a record whose JSON it \
                 can't read as JavaScript does runs in the engine tier)",
                self.module_id, self.version
            )
        } else {
            format!(
                "trigger module {} version {}: tier engine",
                self.module_id, self.version
            )
        }];
        v.extend(self.reasons.iter().cloned());
        v.push(what);
        v
    }
}

/// `__sprig_logic_<binding>` (a trigger's consumer group on a stream).
#[must_use]
pub fn sprig_streams_group(binding: u64) -> String {
    format!("__sprig_logic_{binding}")
}

/// The fields of a record a KIRun trigger may name as parameters.
pub const STREAM_FIELDS: &[&str] = &[
    "stream",
    "partition",
    "offset",
    "key",
    "value",
    "headers",
    "timestamp",
    "record",
    "ctx",
];

/// The fields of a message a KIRun trigger may name as parameters.
pub const QUEUE_FIELDS: &[&str] = &[
    "queue",
    "exchange",
    "routingKey",
    "body",
    "properties",
    "redelivered",
    "deliveryCount",
    "message",
    "ctx",
];

/// The signature JavaScript and Lua triggers are called with: one JSON
/// argument, `ctx`.
#[must_use]
pub fn script_signature() -> Value {
    json!({"params": [{"name": "ctx", "type": "json"}], "events": [], "scalar": false})
}

/// Checks a trigger's code before it is recorded: it parses in its
/// runtime, calls only functions the engine tier has, and (KIRun) names
/// only parameters the source has.
///
/// # Errors
/// 39861 for code that doesn't parse; 39862 for unknown functions; 39874
/// for a KIRun parameter the record or message lacks.
pub async fn check(d: &Declaration, kind: SourceKind) -> Result<Value> {
    use sprig_logic_compile::Runtime;
    match d.runtime {
        Runtime::Kirun => {
            let p = sprig_logic_js::kirun::Prepared::new(&d.body)?;
            let missing = p.missing_functions().await;
            if !missing.is_empty() {
                return Err(SprigError::new(
                    SprigCode::LogicUnsupported,
                    format!(
                        "{} calls functions no tier can run: {}",
                        d.name,
                        missing.join(", ")
                    ),
                ));
            }
            let fields = match kind {
                SourceKind::Stream => STREAM_FIELDS,
                SourceKind::Queue => QUEUE_FIELDS,
            };
            for n in p.params() {
                if !fields.contains(&n.as_str()) {
                    return Err(SprigError::new(
                        SprigCode::LogicSignatureMismatch,
                        format!(
                            "{}: parameter {n} isn't a field of the {} (one of {})",
                            d.name,
                            if kind == SourceKind::Stream {
                                "record"
                            } else {
                                "message"
                            },
                            fields.join(", ")
                        ),
                    ));
                }
            }
            Ok(json!({
                "params": p.params().iter().map(|n| json!({"name": n, "type": "json"})).collect::<Vec<_>>(),
                "events": [],
            }))
        }
        Runtime::Javascript | Runtime::Lua => {
            let sig = sprig_logic_js::ScriptSig {
                params: vec![("ctx".to_owned(), sprig_logic_js::ParamTy::Json)],
                scalar: false,
            };
            sprig_logic_js::Program::new(d.runtime.as_str(), &d.name, &d.body, sig)?;
            Ok(script_signature())
        }
        Runtime::Sql => Err(SprigError::new(
            SprigCode::LogicSyntax,
            format!(
                "{}: a trigger on a stream or queue is written in javascript, lua or kirun",
                d.name
            ),
        )),
    }
}

/// The arguments of a run: `[ctx]` for JavaScript and Lua; for KIRun, each
/// parameter's field of `event` (the record or message), `record`,
/// `message` or `ctx` for the whole object.
#[must_use]
pub fn args_for(runtime: &str, kirun_params: &[String], event: &Value, ctx: &Value) -> Vec<Value> {
    if runtime != "kirun" {
        return vec![ctx.clone()];
    }
    kirun_params
        .iter()
        .map(|n| match n.as_str() {
            "record" | "message" => event.clone(),
            "ctx" => ctx.clone(),
            other => event.get(other).cloned().unwrap_or(Value::Null),
        })
        .collect()
}

/// Deploys a trigger `ON STREAM` or `ON QUEUE` (see the module docs), up
/// to the catalog rows; the caller then writes the index entry and, for a
/// stream, the group's first positions.
///
/// # Errors
/// As [`check`]; 1304 for an existing trigger without `OR REPLACE`;
/// catalog errors.
pub async fn deploy(d: &Declaration, t: MsgTarget<'_>) -> Result<Deployed> {
    let src = d
        .source
        .as_ref()
        .ok_or_else(|| SprigError::internal("a message trigger without its source"))?;
    if d.kind != Kind::Trigger {
        return Err(SprigError::internal("a message source on a non-trigger"));
    }
    let signature = check(d, src.kind).await?;
    Migrator::new(&[LOGIC_MODULE])?
        .run(t.catalog, t.catalog_schema, &MigrateOpts::default())
        .await?;
    let store = LogicStore::new(t.catalog_schema);
    let kind = d.kind.as_str();
    let existing = store.module(t.catalog, t.db_id, kind, &d.name).await?;
    if existing.is_some() && !d.or_replace {
        return Err(SprigError::standard(
            1304,
            *b"42000",
            format!("TRIGGER {} already exists", d.name),
        ));
    }
    let (module, exists) = match existing {
        Some(mut m) => {
            m.runtime = d.runtime.as_str().to_owned();
            m.security = "definer".to_owned();
            m.tier_pref = d.tier.as_str().to_owned();
            (m, true)
        }
        None => (
            ModuleRow {
                module_id: Store::new(t.catalog_schema)
                    .next_object_id(t.catalog)
                    .await?
                    .0,
                db_id: t.db_id,
                name: d.name.clone(),
                kind: kind.to_owned(),
                runtime: d.runtime.as_str().to_owned(),
                security: "definer".to_owned(),
                tier_pref: d.tier.as_str().to_owned(),
                active_version: None,
                state: "active".to_owned(),
                created_by: t.user_id,
            },
            false,
        ),
    };
    // Placed (and compiled routines created) before anything is
    // recorded: a refusal leaves no module behind.
    let version = store.next_version(t.catalog, module.module_id).await?;
    let mut t = t;
    let placed = place(
        d,
        src.kind,
        module.module_id,
        version,
        t.catalog,
        t.catalog_schema,
        t.db_id,
        t.compile.as_mut(),
    )
    .await?;
    if exists {
        store
            .update_options(t.catalog, &module, t.catalog_version)
            .await?;
    } else {
        store
            .create_module(t.catalog, &module, t.catalog_version)
            .await?;
    }
    let prior = store.binding(t.catalog, module.module_id).await?;
    if let Some(b) = &prior
        && b.timing != "on_message"
    {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "TRIGGER {} is an after-commit trigger; drop it before declaring it ON {}",
                d.name,
                src.kind.as_str().to_ascii_uppercase()
            ),
        ));
    }
    let mut binding = prior.clone().unwrap_or(BindingRow {
        binding_id: 0,
        module_id: module.module_id,
        object_id: Some(t.object_id),
        column_name: None,
        timing: "on_message".to_owned(),
        events: String::new(),
        when_fields: Vec::new(),
        enforcement: None,
        state: "active".to_owned(),
    });
    let created = binding.binding_id == 0;
    let old_source = if created {
        None
    } else {
        source(t.catalog, t.catalog_schema, binding.binding_id).await?
    };
    // A replaced trigger that reads another stream or queue starts afresh:
    // a new binding, so a new consumer group.
    let moved = old_source
        .as_ref()
        .is_some_and(|o| o.kind != src.kind || o.object_id != t.object_id);
    if moved {
        binding.binding_id = 0;
    }
    binding.object_id = Some(t.object_id);
    binding.state = "active".to_owned();
    binding.binding_id = store.put_binding(t.catalog, &binding).await?;
    store
        .add_version(
            t.catalog,
            module.module_id,
            version,
            &NewVersion {
                source: &d.body,
                signature: {
                    let mut sig = signature;
                    if let Some(c) = &placed.compiled {
                        sig["compiled"] = c.clone();
                    }
                    sig
                },
                catalog_version: t.catalog_version,
                tier: placed.tier,
                placement: placed.placement.clone(),
                plsql: placed.plsql.clone(),
                created_by: t.user_id,
            },
        )
        .await?;
    store
        .activate(t.catalog, module.module_id, version, t.catalog_version)
        .await?;
    let row = SourceRow {
        binding_id: binding.binding_id,
        module_id: module.module_id,
        kind: src.kind,
        db_id: t.source_db.0,
        db_name: t.source_db.1.to_owned(),
        name: src.name.clone(),
        object_id: t.object_id,
        definer_user_id: t.user_id,
        tenant_name: t.tenant_name.to_owned(),
        fanin_declarer: t.fanin_declarer,
    };
    put_source(t.catalog, t.catalog_schema, &row).await?;
    if moved && let Some(o) = &old_source {
        // The old binding stops: no source row, no job.
        let mut old = binding.clone();
        old.binding_id = o.binding_id;
        old.state = "paused".to_owned();
        let _ = store.put_binding(t.catalog, &old).await;
    }
    let _ = store
        .log(
            t.catalog,
            module.module_id,
            Some(version),
            "info",
            "deploy",
            &format!(
                "trigger on {} {}.{} deployed (binding {})",
                src.kind.as_str(),
                row.db_name,
                row.name,
                row.binding_id
            ),
        )
        .await;
    Ok(Deployed {
        module_id: module.module_id,
        version,
        binding_id: binding.binding_id,
        source: row,
        created: created || moved,
        tier: placed.tier.to_owned(),
        reasons: placed.reasons,
    })
}

/// Where a trigger version runs.
struct Placed {
    tier: &'static str,
    placement: Value,
    plsql: Option<String>,
    /// `signature["compiled"]` ([`CompiledEntry`]).
    compiled: Option<Value>,
    reasons: Vec<String>,
}

impl Placed {
    fn engine(code: &str, detail: &str) -> Self {
        Self {
            tier: "engine",
            placement: json!({
                "tier": "engine",
                "reasons": [{"code": code, "at": "source", "detail": detail}],
                "route": null,
                "objects": [],
                "cost": 0,
            }),
            plsql: None,
            compiled: None,
            reasons: vec![format!("{code} at source: {detail}")],
        }
    }
}

/// Places a trigger version and, compiled, creates its routines on every
/// shard of the trigger's database ([`MsgCompile`]): JavaScript compiles
/// with `ctx.record` or `ctx.message` as its parameters; KIRun and Lua
/// triggers, and code the compiler can't keep the meaning of, run in the
/// engine tier, with the reasons.
///
/// # Errors
/// 39868 for `SPRIG_TIER = 'mariadb'` with reasons; the compiler's
/// errors; a shard's error creating the routines.
#[allow(clippy::too_many_arguments)]
async fn place(
    d: &Declaration,
    kind: SourceKind,
    module_id: u64,
    version: u64,
    catalog: &mut Conn,
    catalog_schema: &str,
    db_id: u32,
    c: Option<&mut MsgCompile<'_>>,
) -> Result<Placed> {
    use sprig_logic_compile::Runtime;
    use sprig_logic_compile::js::{MsgSource, Signature};
    let refuse_mariadb = |why: &str| -> Result<()> {
        if d.tier == crate::declare::TierPref::Mariadb {
            return Err(SprigError::new(
                SprigCode::LogicTierUnavailable,
                format!("SPRIG_TIER = 'mariadb' but the code can't compile: {why}"),
            ));
        }
        Ok(())
    };
    match d.runtime {
        Runtime::Javascript => {}
        Runtime::Kirun => {
            let why = "KIRun triggers on streams and queues run in the engine tier";
            refuse_mariadb(why)?;
            return Ok(Placed::engine("kirun.trigger", why));
        }
        _ => {
            let why = "no compiler front end for this language yet";
            refuse_mariadb(why)?;
            return Ok(Placed::engine("lua.front_end", why));
        }
    }
    let Some(c) = c else {
        let why = "an inherited trigger's instance runs in the engine tier";
        refuse_mariadb(why)?;
        return Ok(Placed::engine("trigger.instance", why));
    };
    if d.tier == crate::declare::TierPref::Engine {
        return Ok(Placed::engine("tier.declared", "SPRIG_TIER = 'engine'"));
    }
    let schemas = c.shards.schemas();
    let first = schemas
        .first()
        .cloned()
        .ok_or_else(|| SprigError::internal("a trigger deploy with no shard"))?;
    let cols = {
        let s = c.shards.session(&first).await?;
        crate::service::columns(s, &first).await?
    };
    let env = crate::calls::Env::new(cols, c.db_name)
        .with_callees(crate::calls::callees(catalog, catalog_schema, db_id).await?)
        .with_streams(&c.streams);
    let opts = sprig_logic_compile::Options {
        module_id,
        version,
        max_loop_iterations: c.max_loop_iterations,
        compile_max_cost: c.compile_max_cost,
        definer_security: true,
    };
    let on = match kind {
        SourceKind::Stream => MsgSource::Stream,
        SourceKind::Queue => MsgSource::Queue,
    };
    let compiled = sprig_logic_compile::js::compile_with(
        &d.name,
        &d.body,
        &Signature::message(on),
        &env,
        &opts,
    )?;
    let placed = crate::placement::place(&compiled, d.tier, c.single_shard)?;
    let placement = serde_json::to_value(&placed).unwrap_or_default();
    let (Some(f), Some(p), crate::placement::Tier::MariaDb) =
        (&compiled.ir, &compiled.plsql, placed.tier)
    else {
        return Ok(Placed {
            tier: "engine",
            placement,
            plsql: None,
            compiled: None,
            reasons: placed.reasons.iter().map(ToString::to_string).collect(),
        });
    };
    let mut plan = crate::deploy::plan_entry(&first, p);
    plan.definer = Some(c.definer.clone());
    crate::deploy::deploy_shards(c.shards, &plan).await?;
    Ok(Placed {
        tier: "mariadb",
        placement,
        plsql: Some(p.statements.join(";\n\n")),
        compiled: Some(json!({
            "params": f.params.iter().map(|p| json!({"name": p.name, "type": p.ty.name()})).collect::<Vec<_>>(),
            "log": f.logs(),
            "reruns": f.may_rerun(),
        })),
        reasons: Vec::new(),
    })
}

// ---- runs: the worker asks a gateway --------------------------------------

/// A record of a stream, as the worker read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordIn {
    /// The partition.
    pub partition: u32,
    /// Its offset.
    pub offset: u64,
    /// The timestamp, ms since the epoch.
    pub ts_ms: i64,
    /// The key.
    pub key: Option<Vec<u8>>,
    /// The value (`None`: a tombstone).
    pub value: Option<Vec<u8>>,
    /// Headers.
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// One run a worker asks a gateway for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRequest {
    /// The tenant.
    pub tenant: u64,
    /// The binding's source (definer, tenant name, stream or queue).
    pub source: SourceRow,
    /// The version to run (the module's active one, as the worker read it).
    pub version: u64,
    /// Stream triggers: the record.
    pub record: Option<RecordIn>,
    /// `logic.trigger_max_attempts` (queue triggers decide dead letters
    /// from the message's delivery count).
    pub max_attempts: u32,
    /// Queue triggers: how long a fetched message stays leased, ms.
    pub lease_ms: u64,
    /// `logic.trigger_budget_ms`.
    pub budget_ms: u64,
    /// A fan-in trigger's run: the member whose instance `record` is
    /// from; its position is the trigger's, with `instance_tenant_id`. 0
    /// for the tenant's own stream.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub instance_tenant: u64,
    /// With `instance_tenant`: the instance's stream id in the member (the
    /// source row's id names the definition).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub instance_stream: u64,
    /// A dead letter's replay: the event to run, outside the
    /// consumer group's position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<ReplayRun>,
}

/// A dead letter's replay run: the stored event, run once with
/// the active version in a transaction that moves the dead letter's own
/// marker (group [`replay_group`], "stream" = the dead letter's id) from 0
/// to 1, so a repeat finds it moved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayRun {
    /// The dead letter.
    pub dl_id: u64,
    /// Its event (`ctx.record` or `ctx.message`).
    pub event: Value,
}

/// The marker group of binding `binding`'s dead-letter replays.
#[must_use]
pub fn replay_group(binding: u64) -> String {
    format!("__sprig_logic_replay_{binding}")
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// What a run came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    /// Committed.
    Ok,
    /// The record was already done (the offset had moved): nothing
    /// committed.
    Moved,
    /// Queue triggers: no message was ready.
    Empty,
    /// The code or the transaction failed; rolled back.
    Failed,
    /// Queue triggers: the message failed for the last time and was
    /// dead-lettered.
    Dead,
}

/// A gateway's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReply {
    /// What happened.
    pub outcome: RunOutcome,
    /// The error's number, for `Failed` and `Dead`.
    pub code: u32,
    /// Its text.
    pub message: String,
    /// Whether the failure is transient (lost connections, lock waits, a
    /// fenced shard, a moved owner): it doesn't count as an attempt.
    pub transient: bool,
    /// Queue triggers: the message's id, its delivery count before this
    /// run.
    pub message_id: Option<String>,
    /// Queue triggers: failed deliveries before this one.
    pub attempts: u32,
    /// Log lines the code wrote.
    pub logs: Vec<String>,
}

impl RunReply {
    /// An outcome with no error.
    #[must_use]
    pub fn of(outcome: RunOutcome) -> Self {
        Self {
            outcome,
            code: 0,
            message: String::new(),
            transient: false,
            message_id: None,
            attempts: 0,
            logs: Vec::new(),
        }
    }

    /// A failure.
    #[must_use]
    pub fn failed(e: &SprigError) -> Self {
        Self {
            code: u32::from(error_number(e)),
            message: e.message.clone(),
            transient: transient(e),
            ..Self::of(RunOutcome::Failed)
        }
    }
}

/// An error's number.
#[must_use]
pub fn error_number(e: &SprigError) -> u16 {
    match e.kind {
        sprig_common::ErrorKind::Sprig(c) => c.number(),
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => errno,
    }
}

/// Whether a run's failure is the infrastructure's, not the code's: it is
/// retried for as long as it lasts and never counts toward
/// `logic.trigger_max_attempts`.
#[must_use]
pub fn transient(e: &SprigError) -> bool {
    if e.retryable && e.code() != Some(SprigCode::LogicTriggerPositionMoved) {
        return true;
    }
    matches!(
        error_number(e),
        // Lock waits and deadlocks, lost or refused connections, read-only
        // and fenced servers, a moved queue owner, unreachable catalogs and
        // gateways.
        1205 | 1213 | 2006 | 2013 | 1290 | 1836 | 1927 | 39260 | 39070 | 39788 | 39301
    )
}

/// `bytes` as text: UTF-8 as it is, other bytes as base64.
fn text(b: &[u8]) -> Value {
    if let Ok(s) = std::str::from_utf8(b) {
        Value::String(s.to_owned())
    } else {
        use base64::Engine as _;
        Value::String(base64::engine::general_purpose::STANDARD.encode(b))
    }
}

/// `ctx.record` of a stream record (the object KIRun's `record` gets).
#[must_use]
pub fn record_value(stream: &str, r: &RecordIn) -> Value {
    let headers: serde_json::Map<String, Value> = r
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), v.as_deref().map_or(Value::Null, text)))
        .collect();
    json!({
        "stream": stream,
        "partition": r.partition,
        "offset": r.offset,
        "key": r.key.as_deref().map(text),
        "value": r.value.as_deref().map(text),
        "headers": headers,
        "timestamp": r.ts_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fan_in_source_ids() {
        // Rows written before migration 4 packed the declarer.
        let packed = FANIN_SOURCE_FLAG | (0x6_1FF5_B482 << FANIN_OBJECT_BITS) | 0x4d;
        assert_eq!(legacy_fanin_parts(packed), Some((26_306_000_002, 77)));
        assert_eq!(legacy_fanin_parts(77), None);
        let mut row = SourceRow {
            binding_id: 1,
            module_id: 2,
            kind: SourceKind::Stream,
            db_id: 1,
            db_name: "security".into(),
            name: "user_events".into(),
            object_id: packed,
            definer_user_id: 1,
            tenant_name: "system".into(),
            fanin_declarer: 0,
        };
        assert_eq!(row.fanin_definition(), Some((26_306_000_002, 77)));
        // Now: any 64-bit declarer.
        row.object_id = fanin_source_id(77).unwrap();
        row.fanin_declarer = (1 << 40) + 5;
        assert_eq!(row.fanin_definition(), Some(((1 << 40) + 5, 77)));
        assert_eq!(fanin_source_id(1 << 62), None);
        row.object_id = 77;
        assert_eq!(row.fanin_definition(), None, "an ordinary trigger");
    }

    #[test]
    fn records_and_args() {
        let r = RecordIn {
            partition: 3,
            offset: 1205,
            ts_ms: 7,
            key: Some(b"901".to_vec()),
            value: Some(br#"{"type":"user.added"}"#.to_vec()),
            headers: vec![("x-sprig-tenant".into(), Some(b"acme".to_vec()))],
        };
        let v = record_value("security.user_events", &r);
        assert_eq!(v["key"], "901");
        assert_eq!(v["headers"]["x-sprig-tenant"], "acme");
        let ctx = json!({"record": v});
        let a = args_for(
            "kirun",
            &["value".to_owned(), "offset".to_owned(), "record".to_owned()],
            &v,
            &ctx,
        );
        assert_eq!(a[0], json!(r#"{"type":"user.added"}"#));
        assert_eq!(a[1], json!(1205));
        assert_eq!(a[2]["partition"], 3);
        assert_eq!(args_for("javascript", &[], &v, &ctx), vec![ctx.clone()]);
    }

    #[test]
    fn transient_errors() {
        assert!(transient(&SprigError::backend(1213, *b"40001", "deadlock")));
        assert!(!transient(&SprigError::backend(1062, *b"23000", "dup")));
        assert!(!transient(&SprigError::new(
            SprigCode::LogicUserError,
            "boom"
        )));
        let r = RunReply::failed(&SprigError::new(SprigCode::LogicUserError, "boom"));
        assert_eq!(r.code, 39865);
        assert_eq!(r.outcome, RunOutcome::Failed);
        let j = serde_json::to_string(&r).unwrap();
        assert!(j.contains("\"failed\""));
    }

    #[test]
    fn index_kinds() {
        let e = MsgIndex {
            tenant: TenantId(4),
            binding: 9,
            kind: SourceKind::Queue,
        };
        assert_eq!(e.job_kind(), QUEUE_JOB_KIND);
        let j = serde_json::to_value(&e).unwrap();
        let back: MsgIndex = serde_json::from_value(j).unwrap();
        assert_eq!(back, e);
    }
}
