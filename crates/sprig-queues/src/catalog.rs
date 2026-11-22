//! The `mq` catalog module: vhosts, exchanges, queues, bindings, policies,
//! permissions and the delivery-token key, kept as the engine's catalog
//! records (`m/mq/<table>/<key>`), the snapshot loader ([`MqExtension`])
//! and the writes declares make.
//!
//! ```text
//! m/mq/vhosts/<db u32>                                         VhostRow
//! m/mq/exchanges/<object_id u64>                               ExchangeRow
//! m/mq/xnames/<db u32><name>                                   object id
//! m/mq/queues/<object_id u64>                                  QueueRow
//! m/mq/qnames/<db u32><name>                                   object id
//! m/mq/bindings/<db u32><source u64><dest u64><key><hash 8>    BindingRow
//! m/mq/policies/<db u32><kind u8><name>                        PolicyRow
//! m/mq/permissions/<db u32><user u32>                          PermissionRow
//! m/mq/keys/<kid u8>                                           KeyRow (not versioned)
//! ```
//!
//! Every declare, delete, bind and unbind of a durable object is one
//! versioned catalog change (its rows written through the change's
//! records), so every gateway's routing follows it. Temporary queues are
//! not here: see [`crate::topology::TempSet`]. The name rows stand for the
//! unique keys on (database, name) the MariaDB tables had.
//!
//! **As built:**
//! - Exchanges and queues have no `objects` row (the `objects.kind` enum
//!   has no `exchange` or `queue` yet, as streams have none): their ids
//!   come from the object counter.
//! - `arguments` are JSON with RabbitMQ's value types folded (integers to
//!   numbers, strings, booleans, nested tables and arrays; byte strings as
//!   `{"$bytes": base64}`).
//! - The delivery-token key is one record, made on first use in its own
//!   short engine transaction.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_proto_amqp::backend::DestKind;
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};

use crate::engine::TopoChange;
use crate::topology::{
    ApplyTo, BindingDef, ExKind, ExchangeDef, PermissionDef, PolicyDef, QType, QueueDef, Topology,
};

/// The module name.
pub const MODULE: &str = "mq";

/// Vhost limits by database.
pub const VHOSTS: RowTable = RowTable::new(MODULE, "vhosts");
/// Exchanges by object id.
pub const EXCHANGES: RowTable = RowTable::new(MODULE, "exchanges");
/// Exchange ids by database and name.
pub const XNAMES: RowTable = RowTable::new(MODULE, "xnames");
/// Queues by object id.
pub const QUEUES: RowTable = RowTable::new(MODULE, "queues");
/// Queue ids by database and name.
pub const QNAMES: RowTable = RowTable::new(MODULE, "qnames");
/// Bindings.
pub const BINDINGS: RowTable = RowTable::new(MODULE, "bindings");
/// Policies.
pub const POLICIES: RowTable = RowTable::new(MODULE, "policies");
/// Users' permissions.
pub const PERMISSIONS: RowTable = RowTable::new(MODULE, "permissions");
/// The delivery-token key.
pub const KEYS: RowTable = RowTable::new(MODULE, "keys");

/// The module's MariaDB migrations: none, its rows are engine records.
pub static MQ_MIGRATIONS: &[Migration] = &[];

/// The `mq` module, for [`Migrator::new`].
pub const MQ_MODULE: Module = Module {
    name: MODULE,
    migrations: MQ_MIGRATIONS,
};

/// Makes sure catalog `schema` can take the module's records (its core
/// migrations, which create the record log).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

/// A vhost's settings: what `mq_vhosts` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VhostRow {
    /// The vhost.
    pub vhost_name: String,
    /// Its database.
    pub db_id: u32,
    /// `classic`, `quorum` or `stream`.
    pub default_queue_type: String,
    /// `{"max-queues": n, ...}`.
    pub limits: Option<Value>,
    /// Free text.
    pub description: Option<String>,
}

/// An exchange: what `mq_exchanges` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeRow {
    /// Its id.
    pub object_id: u64,
    /// Its database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// `direct`, `fanout`, `topic`, `headers` or `x-delayed-message`.
    pub kind: String,
    /// A delayed exchange's routing type.
    pub delayed_type: Option<String>,
    /// Durable.
    pub durable: bool,
    /// Auto-delete.
    pub auto_delete: bool,
    /// Internal.
    pub internal: bool,
    /// Arguments (see the module docs).
    pub arguments: Value,
}

/// A queue: what `mq_queues` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueRow {
    /// Its id.
    pub object_id: u64,
    /// Its database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// `classic`, `quorum` or `stream`.
    pub qtype: String,
    /// Durable.
    pub durable: bool,
    /// Auto-delete.
    pub auto_delete: bool,
    /// Arguments (see the module docs).
    pub arguments: Value,
    /// Lanes.
    pub lanes: u16,
    /// `active` or `deleting`.
    pub state: String,
    /// Milliseconds since the epoch.
    pub last_used_at_ms: u64,
}

/// A binding: what `mq_bindings` held besides its key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindingRow {
    /// `queue` or `exchange`.
    pub dest_kind: String,
    /// Arguments (see the module docs).
    pub arguments: Value,
}

/// A policy: what `mq_policies` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyRow {
    /// Its database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// `user` or `operator`.
    pub kind: String,
    /// The name pattern (a regular expression).
    pub pattern: String,
    /// What it applies to.
    pub apply_to: String,
    /// Higher wins.
    pub priority: i32,
    /// The definition.
    pub definition: Value,
}

/// A user's permissions on a vhost: what `mq_permissions` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionRow {
    /// Its database.
    pub db_id: u32,
    /// The user.
    pub user_id: u32,
    /// Configure pattern.
    pub configure_re: String,
    /// Write pattern.
    pub write_re: String,
    /// Read pattern.
    pub read_re: String,
}

/// The delivery-token key: what `mq_keys` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyRow {
    /// The secret.
    pub secret: Vec<u8>,
}

fn name_pk(db: u32, name: &str) -> Pk {
    Pk::new().u32(db).bytes(name.as_bytes())
}

fn binding_pk(db: u32, src: u64, dst: u64, key: &str, hash: [u8; 8]) -> Pk {
    Pk::new()
        .u32(db)
        .u64(src)
        .u64(dst)
        .bytes(key.as_bytes())
        .raw(&hash)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("mq catalog: {what}"))
}

// ----------------------------------------------------------------------
// Arguments as JSON.
// ----------------------------------------------------------------------

/// A field value as JSON.
#[must_use]
pub fn value_json(v: &FieldValue) -> Value {
    match v {
        FieldValue::Bool(b) => json!(b),
        FieldValue::I8(_)
        | FieldValue::U8(_)
        | FieldValue::I16(_)
        | FieldValue::U16(_)
        | FieldValue::I32(_)
        | FieldValue::U32(_)
        | FieldValue::I64(_)
        | FieldValue::Timestamp(_) => v.as_i64().map_or(Value::Null, |n| json!(n)),
        FieldValue::F32(f) => json!(f),
        FieldValue::F64(f) => json!(f),
        FieldValue::Decimal(s, u) => json!({"$decimal": [s, u]}),
        FieldValue::LongStr(b) => match std::str::from_utf8(b) {
            Ok(s) => json!(s),
            Err(_) => json!({"$bytes": STANDARD.encode(b)}),
        },
        FieldValue::Bytes(b) => json!({"$bytes": STANDARD.encode(b)}),
        FieldValue::Array(a) => Value::Array(a.iter().map(value_json).collect()),
        FieldValue::Table(t) => table_json(t),
        FieldValue::Void => Value::Null,
    }
}

/// A field table as a JSON object.
#[must_use]
pub fn table_json(t: &FieldTable) -> Value {
    Value::Object(
        t.0.iter()
            .map(|(k, v)| (k.clone(), value_json(v)))
            .collect::<Map<_, _>>(),
    )
}

/// JSON back to a field value.
#[must_use]
pub fn json_value(v: &Value) -> FieldValue {
    match v {
        Value::Null => FieldValue::Void,
        Value::Bool(b) => FieldValue::Bool(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => FieldValue::I64(i),
            None => FieldValue::F64(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => FieldValue::str(s),
        Value::Array(a) => FieldValue::Array(a.iter().map(json_value).collect()),
        Value::Object(o) => {
            if let Some(Value::String(b)) = o.get("$bytes")
                && o.len() == 1
            {
                return FieldValue::Bytes(STANDARD.decode(b).unwrap_or_default().into());
            }
            FieldValue::Table(json_table(v))
        }
    }
}

/// A JSON object as a field table.
#[must_use]
pub fn json_table(v: &Value) -> FieldTable {
    match v {
        Value::Object(o) => FieldTable(o.iter().map(|(k, v)| (k.clone(), json_value(v))).collect()),
        _ => FieldTable::new(),
    }
}

fn args_hash(t: &FieldTable) -> [u8; 8] {
    let text = table_json(t).to_string();
    let d = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&d.as_ref()[..8]);
    out
}

// ----------------------------------------------------------------------
// The snapshot state.
// ----------------------------------------------------------------------

/// The `mq` module in a snapshot: each database's durable model.
#[derive(Debug, Clone, Default)]
pub struct MqState {
    /// The module's tables exist.
    pub migrated: bool,
    /// By database id.
    pub dbs: HashMap<u32, Arc<Topology>>,
}

impl ExtState for MqState {
    fn bytes(&self) -> usize {
        self.dbs
            .values()
            .map(|t| 256 * (t.exchanges.len() + t.queues.len() + t.bindings.len()))
            .sum()
    }
}

impl MqState {
    /// A database's model (the built-ins only when it has none).
    #[must_use]
    pub fn topology(&self, db: u32, version: u64) -> Arc<Topology> {
        self.dbs.get(&db).cloned().unwrap_or_else(|| {
            let mut t = Topology::new();
            t.version = version;
            Arc::new(t)
        })
    }
}

/// Reads and writes the module's records.
#[derive(Debug, Clone, Copy, Default)]
pub struct MqStore;

impl MqStore {
    /// Every database's durable model, at `version`.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx, version: u64) -> Result<MqState> {
        let mut topo: HashMap<u32, Topology> = HashMap::new();
        let fresh = || {
            let mut t = Topology::new();
            t.version = version;
            t
        };
        let mut names: HashMap<u64, (String, DestKind)> = HashMap::new();
        for r in EXCHANGES.rows::<ExchangeRow>(rec, &Pk::new()).await? {
            let x = ExchangeDef {
                id: r.object_id,
                name: r.name.clone(),
                kind: ExKind::parse(&r.kind).ok_or_else(|| bad("exchange type"))?,
                durable: r.durable,
                auto_delete: r.auto_delete,
                internal: r.internal,
                arguments: json_table(&r.arguments),
                builtin: false,
            };
            names.insert(r.object_id, (r.name.clone(), DestKind::Exchange));
            topo.entry(r.db_id)
                .or_insert_with(fresh)
                .exchanges
                .insert(r.name, Arc::new(x));
        }
        for r in QUEUES.rows::<QueueRow>(rec, &Pk::new()).await? {
            if r.state != "active" {
                continue;
            }
            let qd = QueueDef {
                id: r.object_id,
                name: r.name.clone(),
                qtype: QType::parse(&r.qtype).ok_or_else(|| bad("qtype"))?,
                durable: r.durable,
                exclusive: None,
                auto_delete: r.auto_delete,
                arguments: json_table(&r.arguments),
                lanes: r.lanes,
                temp_on: None,
            };
            names.insert(r.object_id, (r.name.clone(), DestKind::Queue));
            topo.entry(r.db_id)
                .or_insert_with(fresh)
                .queues
                .insert(r.name, Arc::new(qd));
        }
        for (pk, r) in BINDINGS.scan::<BindingRow>(rec, &Pk::new()).await? {
            let Some((db, from_id, to_id, key)) = split_binding(&pk.0) else {
                return Err(bad("binding key"));
            };
            let (Some((source, _)), Some((dest, kind))) = (names.get(&from_id), names.get(&to_id))
            else {
                continue;
            };
            let b = BindingDef {
                source: source.clone(),
                dest: dest.clone(),
                dest_kind: *kind,
                key,
                arguments: json_table(&r.arguments),
            };
            if let Some(t) = topo.get_mut(&db) {
                t.bindings.push(b);
            }
        }
        let mut policies = POLICIES.rows::<PolicyRow>(rec, &Pk::new()).await?;
        policies.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.name.cmp(&b.name))
        });
        for r in policies {
            let Ok(re) = regex::Regex::new(&r.pattern) else {
                continue;
            };
            let p = PolicyDef {
                name: r.name,
                operator: r.kind == "operator",
                pattern: re,
                apply_to: ApplyTo::parse(&r.apply_to).unwrap_or(ApplyTo::All),
                priority: r.priority,
                definition: r.definition.as_object().cloned().unwrap_or_default(),
            };
            topo.entry(r.db_id).or_insert_with(fresh).policies.push(p);
        }
        for r in PERMISSIONS.rows::<PermissionRow>(rec, &Pk::new()).await? {
            let (Ok(c), Ok(w), Ok(rd)) = (
                regex::Regex::new(&r.configure_re),
                regex::Regex::new(&r.write_re),
                regex::Regex::new(&r.read_re),
            ) else {
                continue;
            };
            topo.entry(r.db_id)
                .or_insert_with(fresh)
                .permissions
                .push(PermissionDef {
                    user_id: u64::from(r.user_id),
                    configure: c,
                    write: w,
                    read: rd,
                });
        }
        for r in VHOSTS.rows::<VhostRow>(rec, &Pk::new()).await? {
            if let Some(n) = r
                .limits
                .as_ref()
                .and_then(|l| l.get("max-queues"))
                .and_then(Value::as_u64)
            {
                topo.entry(r.db_id).or_insert_with(fresh).max_queues = Some(n);
            }
        }
        Ok(MqState {
            migrated: true,
            dbs: topo.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
        })
    }

    /// The id of exchange or queue `name` in database `db`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn id_of(
        rec: &mut dyn RecordTx,
        db: u32,
        name: &str,
        kind: DestKind,
    ) -> Result<Option<u64>> {
        let t = match kind {
            DestKind::Exchange => XNAMES,
            DestKind::Queue => QNAMES,
        };
        t.get::<u64>(rec, &name_pk(db, name)).await
    }

    /// Queue `id`, in any state.
    ///
    /// # Errors
    /// The record store's.
    pub async fn queue(rec: &mut dyn RecordTx, id: u64) -> Result<Option<QueueRow>> {
        QUEUES.get(rec, &Pk::new().u64(id)).await
    }

    /// Replaces queue `id`'s arguments (inherited queues follow their
    /// definition).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_queue_arguments(rec: &mut dyn RecordTx, id: u64, args: &Value) -> Result<()> {
        let pk = Pk::new().u64(id);
        if let Some(mut q) = QUEUES.get::<QueueRow>(rec, &pk).await? {
            q.arguments = args.clone();
            QUEUES.put(rec, &pk, &q).await?;
        }
        Ok(())
    }

    async fn delete_bindings(
        rec: &mut dyn RecordTx,
        db: u32,
        hit: impl Fn(u64, u64, &BindingRow) -> bool,
    ) -> Result<()> {
        for (pk, r) in BINDINGS.scan::<BindingRow>(rec, &Pk::new().u32(db)).await? {
            let Some((_, src, dst, _)) = split_binding(&pk.0) else {
                continue;
            };
            if hit(src, dst, &r) {
                BINDINGS.delete(rec, &pk).await?;
            }
        }
        Ok(())
    }

    /// Applies a change to the records of the caller's catalog change.
    /// `new_id` is used for a new exchange or queue. Declaring a name that
    /// exists (raced by another gateway) is a no-op.
    ///
    /// # Errors
    /// The record store's.
    pub async fn apply(
        rec: &mut dyn RecordTx,
        db: u32,
        change: &TopoChange,
        new_id: u64,
    ) -> Result<()> {
        match change {
            TopoChange::AddExchange(x) => {
                if Self::id_of(rec, db, &x.name, DestKind::Exchange)
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
                let delayed =
                    (x.kind == ExKind::Delayed).then(|| x.route_kind().as_str().to_owned());
                let row = ExchangeRow {
                    object_id: new_id,
                    db_id: db,
                    name: x.name.clone(),
                    kind: x.kind.as_str().to_owned(),
                    delayed_type: delayed,
                    durable: x.durable,
                    auto_delete: x.auto_delete,
                    internal: x.internal,
                    arguments: table_json(&x.arguments),
                };
                EXCHANGES.put(rec, &Pk::new().u64(new_id), &row).await?;
                XNAMES.put(rec, &name_pk(db, &x.name), &new_id).await
            }
            TopoChange::DelExchange(name) => {
                let Some(id) = Self::id_of(rec, db, name, DestKind::Exchange).await? else {
                    return Ok(());
                };
                Self::delete_bindings(rec, db, |src, dst, r| {
                    src == id || (dst == id && r.dest_kind == "exchange")
                })
                .await?;
                EXCHANGES.delete(rec, &Pk::new().u64(id)).await?;
                XNAMES.delete(rec, &name_pk(db, name)).await
            }
            TopoChange::AddQueue(qd) => {
                if Self::id_of(rec, db, &qd.name, DestKind::Queue)
                    .await?
                    .is_some()
                {
                    return Ok(());
                }
                let row = QueueRow {
                    object_id: new_id,
                    db_id: db,
                    name: qd.name.clone(),
                    qtype: qd.qtype.as_str().to_owned(),
                    durable: qd.durable,
                    auto_delete: qd.auto_delete,
                    arguments: table_json(&qd.arguments),
                    lanes: qd.lanes,
                    state: "active".to_owned(),
                    last_used_at_ms: now_ms(),
                };
                QUEUES.put(rec, &Pk::new().u64(new_id), &row).await?;
                QNAMES.put(rec, &name_pk(db, &qd.name), &new_id).await
            }
            TopoChange::DelQueue(name) => {
                let Some(id) = Self::id_of(rec, db, name, DestKind::Queue).await? else {
                    return Ok(());
                };
                Self::delete_bindings(rec, db, |_, dst, r| dst == id && r.dest_kind == "queue")
                    .await?;
                QUEUES.delete(rec, &Pk::new().u64(id)).await?;
                QNAMES.delete(rec, &name_pk(db, name)).await
            }
            TopoChange::Bind(b) | TopoChange::Unbind(b) => {
                let (Some(src), Some(dst)) = (
                    Self::id_of(rec, db, &b.source, DestKind::Exchange).await?,
                    Self::id_of(rec, db, &b.dest, b.dest_kind).await?,
                ) else {
                    return Ok(());
                };
                let kind = match b.dest_kind {
                    DestKind::Queue => "queue",
                    DestKind::Exchange => "exchange",
                };
                let pk = binding_pk(db, src, dst, &b.key, args_hash(&b.arguments));
                if matches!(change, TopoChange::Bind(_)) {
                    if BINDINGS.get::<BindingRow>(rec, &pk).await?.is_some() {
                        return Ok(());
                    }
                    BINDINGS
                        .put(
                            rec,
                            &pk,
                            &BindingRow {
                                dest_kind: kind.to_owned(),
                                arguments: table_json(&b.arguments),
                            },
                        )
                        .await
                } else {
                    BINDINGS.delete(rec, &pk).await
                }
            }
        }
    }

    /// The tenant's token key, made on first use (one short transaction
    /// of its own: the key isn't versioned).
    ///
    /// # Errors
    /// The record store's.
    pub async fn token_secret(store: &dyn RecordStore, tenant: TenantId) -> Result<Vec<u8>> {
        let pk = Pk::new().u8(1);
        let mut tx = store.begin(tenant).await?;
        if let Some(k) = KEYS.get::<KeyRow>(&mut *tx, &pk).await? {
            let _ = tx.rollback().await;
            return Ok(k.secret);
        }
        let got = KEYS.get_for_update::<KeyRow>(&mut *tx, &pk).await?;
        if let Some(k) = got {
            let _ = tx.rollback().await;
            return Ok(k.secret);
        }
        let mut secret = [0u8; 32];
        let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut secret);
        KEYS.put(
            &mut *tx,
            &pk,
            &KeyRow {
                secret: secret.to_vec(),
            },
        )
        .await?;
        tx.commit().await?;
        Ok(secret.to_vec())
    }
}

/// A binding key's parts: database, source id, destination id, routing
/// key.
fn split_binding(k: &[u8]) -> Option<(u32, u64, u64, String)> {
    let db = u32::from_be_bytes(k.get(..4)?.try_into().ok()?);
    let src = u64::from_be_bytes(k.get(4..12)?.try_into().ok()?);
    let dst = u64::from_be_bytes(k.get(12..20)?.try_into().ok()?);
    // The routing key: escaped bytes up to the `00 00` terminator.
    let mut key = Vec::new();
    let mut i = 20;
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
    Some((db, src, dst, String::from_utf8_lossy(&key).into_owned()))
}

/// The module in the catalog cache: [`MqState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct MqExtension;

#[async_trait::async_trait]
impl CatalogExtension for MqExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        MQ_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        if !tx.catalog_records() {
            return Ok(Arc::new(MqState::default()));
        }
        Ok(Arc::new(MqStore::load(tx.records()?, 0).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[MQ_MODULE]).unwrap_or_else(|_| unreachable!());
        assert_eq!(MQ_MODULE.newest(), 0);
    }

    #[test]
    fn arguments_round_trip() {
        let t = FieldTable::from([
            ("x-match", FieldValue::str("any")),
            ("n", FieldValue::I32(7)),
            ("b", FieldValue::Bool(true)),
            (
                "raw",
                FieldValue::Bytes(bytes::Bytes::from_static(&[0, 1, 255])),
            ),
            (
                "nested",
                FieldValue::Table(FieldTable::from([("k", FieldValue::I64(1))])),
            ),
        ]);
        let back = json_table(&table_json(&t));
        assert_eq!(back.get("x-match"), Some(&FieldValue::str("any")));
        assert_eq!(back.get("n").and_then(FieldValue::as_i64), Some(7));
        assert_eq!(back.get("raw"), t.get("raw"));
        assert_eq!(args_hash(&t), args_hash(&t.clone()));
    }

    use sprig_catalog::records::mem::MemRecords;

    const T: TenantId = TenantId(57_306_000_002);

    fn ex(name: &str) -> ExchangeDef {
        ExchangeDef {
            id: 0,
            name: name.into(),
            kind: ExKind::Topic,
            durable: true,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::from([("alternate-exchange", FieldValue::str("ae"))]),
            builtin: false,
        }
    }

    fn q(name: &str) -> QueueDef {
        QueueDef {
            id: 0,
            name: name.into(),
            qtype: QType::Classic,
            durable: true,
            exclusive: None,
            auto_delete: false,
            arguments: FieldTable::from([("x-max-length", FieldValue::I32(10))]),
            lanes: 1,
            temp_on: None,
        }
    }

    fn bind(src: &str, dst: &str, kind: DestKind, key: &str) -> BindingDef {
        BindingDef {
            source: src.into(),
            dest: dst.into(),
            dest_kind: kind,
            key: key.into(),
            arguments: FieldTable::default(),
        }
    }

    async fn run(store: &MemRecords, db: u32, changes: &[(TopoChange, u64)]) {
        let mut tx = store.begin(T).await.unwrap();
        for (c, id) in changes {
            MqStore::apply(&mut *tx, db, c, *id).await.unwrap();
        }
        tx.commit().await.unwrap();
    }

    async fn load(store: &MemRecords) -> MqState {
        let mut tx = store.begin(T).await.unwrap();
        MqStore::load(&mut *tx, 3).await.unwrap()
    }

    #[tokio::test]
    async fn declares_binds_and_deletes_follow_the_sql_rules() {
        let store = MemRecords::new();
        run(
            &store,
            1,
            &[
                (TopoChange::AddExchange(ex("orders")), 10),
                (TopoChange::AddExchange(ex("audit")), 11),
                (TopoChange::AddQueue(q("billing")), 12),
                (TopoChange::AddQueue(q("ship")), 13),
                // A declare of an existing name is a no-op (keeps id 10).
                (TopoChange::AddExchange(ex("orders")), 99),
                (
                    TopoChange::Bind(bind("orders", "billing", DestKind::Queue, "o.*")),
                    0,
                ),
                (
                    TopoChange::Bind(bind("orders", "billing", DestKind::Queue, "o.*")),
                    0,
                ),
                (
                    TopoChange::Bind(bind("orders", "ship", DestKind::Queue, "a\0b")),
                    0,
                ),
                (
                    TopoChange::Bind(bind("orders", "audit", DestKind::Exchange, "#")),
                    0,
                ),
                (
                    TopoChange::Bind(bind("audit", "ship", DestKind::Queue, "x")),
                    0,
                ),
                // A binding naming nothing is ignored.
                (
                    TopoChange::Bind(bind("nope", "ship", DestKind::Queue, "x")),
                    0,
                ),
            ],
        )
        .await;
        // Another database keeps the same names apart.
        run(&store, 2, &[(TopoChange::AddQueue(q("billing")), 20)]).await;
        let st = load(&store).await;
        let t = st.topology(1, 3);
        assert_eq!(t.version, 3);
        assert_eq!(t.exchanges["orders"].id, 10);
        assert_eq!(
            t.exchanges["orders"].arguments.get("alternate-exchange"),
            Some(&FieldValue::str("ae"))
        );
        assert_eq!(t.queues["billing"].id, 12);
        assert_eq!(t.bindings.len(), 4);
        assert!(
            t.bindings
                .iter()
                .any(|b| b.key == "a\0b" && b.dest == "ship")
        );
        assert_eq!(st.topology(2, 3).queues["billing"].id, 20);
        // Deleting a queue takes its bindings; deleting an exchange takes
        // the ones from it and to it.
        run(
            &store,
            1,
            &[
                (TopoChange::DelQueue("billing".into()), 0),
                (
                    TopoChange::Unbind(bind("audit", "ship", DestKind::Queue, "x")),
                    0,
                ),
            ],
        )
        .await;
        let t = load(&store).await.topology(1, 3);
        assert!(!t.queues.contains_key("billing"));
        assert_eq!(t.bindings.len(), 2);
        run(&store, 1, &[(TopoChange::DelExchange("audit".into()), 0)]).await;
        let t = load(&store).await.topology(1, 3);
        assert_eq!(t.bindings.len(), 1);
        assert_eq!(t.bindings[0].dest, "ship");
        // The name rows went with the objects: the names can be declared again.
        run(&store, 1, &[(TopoChange::AddQueue(q("billing")), 30)]).await;
        assert_eq!(load(&store).await.topology(1, 3).queues["billing"].id, 30);
        let mut tx = store.begin(T).await.unwrap();
        assert_eq!(
            MqStore::id_of(&mut *tx, 1, "audit", DestKind::Exchange)
                .await
                .unwrap(),
            None
        );
        let mut args = MqStore::queue(&mut *tx, 13).await.unwrap().unwrap();
        assert_eq!(args.state, "active");
        args.arguments = json!({"x-max-length": 5});
        MqStore::set_queue_arguments(&mut *tx, 13, &args.arguments)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let t = load(&store).await.topology(1, 3);
        assert_eq!(
            t.queues["ship"]
                .arguments
                .get("x-max-length")
                .and_then(FieldValue::as_i64),
            Some(5)
        );
    }

    #[tokio::test]
    async fn the_token_key_is_made_once() {
        let store = MemRecords::new();
        let a = MqStore::token_secret(&store, T).await.unwrap();
        let b = MqStore::token_secret(&store, T).await.unwrap();
        assert_eq!(a.len(), 32);
        assert_eq!(a, b);
    }
}
