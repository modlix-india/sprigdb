//! A stream of an engine database read as a table by the general path, so
//! a query may join it with tables and a prepared statement may read it.
//!
//! ```text
//! columns  partition_no INT UNSIGNED, offset_no BIGINT UNSIGNED, ts DATETIME(3),
//!          key VARBINARY(4096), value LONGBLOB, headers JSON
//! key      PRIMARY (partition_no, offset_no): ranges read only those offsets
//! storage  the stream log (`exec::streams::select`), read as it is now
//! ```

use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId};

use super::table::{self, TableDef};

/// Where streams' table objects are numbered (below temporary tables',
/// apart from tables' and collections').
const STREAM_BASE: u64 = 0xB000_0000;

/// A stream as a statement reads it.
pub(crate) struct StreamSource {
    /// The table it reads as.
    pub def: Arc<TableDef>,
    /// Its rows.
    pub storage: Arc<dyn sprig_plan::access::Storage>,
    /// A Redis database's table (`engine::redistable`): read in the
    /// statement's transaction instead of through `storage`.
    pub redis: Option<super::redistable::RedisTable>,
}

impl std::fmt::Debug for StreamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSource")
            .field("table", &self.def.name)
            .finish_non_exhaustive()
    }
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Stream `name` of engine database `db` (named `db_name`) of `tenant`, as
/// the stream log holds it now; `None` when there is none.
pub(crate) fn find(tenant: u64, db: DbId, db_name: &str, name: &str) -> Option<Arc<StreamSource>> {
    let eng = crate::kafka::engine::streams()?;
    let t = eng
        .topic(tenant, &format!("{db_name}.{name}"))
        .ok()
        .flatten()?;
    let text = format!(
        "CREATE TABLE {} (\n  `partition_no` int(10) unsigned NOT NULL,\n  `offset_no` bigint(20) unsigned NOT NULL,\n  `ts` datetime(3) NOT NULL,\n  `key` varbinary(4096) DEFAULT NULL,\n  `value` longblob DEFAULT NULL,\n  `headers` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`headers`)),\n  PRIMARY KEY (`partition_no`,`offset_no`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        quote(name)
    );
    let object = ObjectId(STREAM_BASE + (t.stream_id & 0x0FFF_FFFF));
    let def = table::parse(object, 1, db, &text).ok()?;
    let partitions = u32::try_from(t.partitions.max(0)).unwrap_or(0);
    let storage = crate::exec::streams::select::one_stream(
        eng,
        tenant,
        db_name,
        &def.name,
        t.stream_id,
        partitions,
    );
    Some(Arc::new(StreamSource {
        def,
        storage,
        redis: None,
    }))
}
