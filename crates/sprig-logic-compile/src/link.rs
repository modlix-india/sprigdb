//! What compiled code links to outside its own version: other logic
//! modules it calls (`db.call`) and streams it publishes to
//! (`db.publish`).
//!
//! ```text
//! db.call("quote", a, b)       CALL __sprig_l<m>_call(a, b, event, result)
//!                              the callee module's call entry: no result set,
//!                              the active version or, mid-rollout, the
//!                              version the caller's wrapper would pick
//! db.publish("s", k, v, h)     INSERT INTO __sprig_eo_<id> (k, v, headers)
//!                              the stream's outbox, in the code's own shard
//!                              schema, after the gateway's checks
//! ```
//!
//! The deploy fills these in from the catalog ([`crate::sql::Columns::callee`],
//! [`crate::sql::Columns::stream`]); anything it can't link stays in the engine tier
//! with the reason it gives.

use serde::Serialize;

use crate::ir::Ty;

/// A logic function or procedure that compiled code can call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Callee {
    /// The module.
    pub module_id: u64,
    /// Its name, as declared.
    pub name: String,
    /// Its parameters' types, in call order.
    pub params: Vec<Ty>,
    /// The fields a call returns, as the engine tier's `db.call` sees
    /// them: the wrapper's columns after `event` (every event's fields
    /// once, in event order), booleans as integers (the wrapper casts them).
    pub fields: Vec<(String, Ty)>,
    /// Tables it reads, through its own calls too.
    pub reads: Vec<String>,
    /// Tables and streams it writes, through its own calls too.
    pub writes: Vec<String>,
    /// Modules its compiled calls reach (for cycles).
    pub reaches: Vec<u64>,
    /// Its longest chain of compiled calls (0 when it calls none).
    pub depth: u32,
}

impl Callee {
    /// The routine compiled callers call: `__sprig_l<module>_call`.
    #[must_use]
    pub fn entry(&self) -> String {
        call_entry(self.module_id)
    }
}

/// A module's call entry: `__sprig_l<module>_call`.
#[must_use]
pub fn call_entry(module_id: u64) -> String {
    format!("__sprig_l{module_id}_call")
}

/// What a compiled caller was compiled against, kept in its placement so
/// a later deploy of the callee can tell whether it still fits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CallDep {
    /// The callee module.
    pub module: u64,
    /// Its name.
    pub name: String,
    /// Parameter types (IR names).
    pub params: Vec<String>,
    /// Fields and their types (IR names).
    pub fields: Vec<(String, String)>,
    /// Whether it wrote anything.
    pub writes: bool,
}

impl CallDep {
    /// The dependency on `c`.
    #[must_use]
    pub fn of(c: &Callee) -> Self {
        Self {
            module: c.module_id,
            name: c.name.clone(),
            params: c.params.iter().map(|t| t.name().to_owned()).collect(),
            fields: c
                .fields
                .iter()
                .map(|(n, t)| (n.clone(), t.name().to_owned()))
                .collect(),
            writes: !c.writes.is_empty(),
        }
    }
}

/// A stream compiled code can publish to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTarget {
    /// Its name.
    pub name: String,
    /// Its outbox table in the code's shard schema, `__sprig_eo_<id>`.
    pub outbox: String,
    /// `stream.max_message_bytes` for its database, read at deploy.
    pub max_bytes: u64,
    /// A compacted stream: a record needs a key.
    pub compact: bool,
}
