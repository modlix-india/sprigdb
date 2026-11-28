//! `information_schema` reads about tables, answered from the engine's
//! definitions.
//!
//! The name layer turns a client's `information_schema` query into one over
//! derived tables that read MariaDB's own views restricted to the tenant's
//! physical schemas. No table exists on MariaDB, so the engine answers that
//! query instead: it builds MariaDB 11.8's `TABLES`, `COLUMNS`,
//! `STATISTICS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`,
//! `CHECK_CONSTRAINTS` and `REFERENTIAL_CONSTRAINTS` in memory (the rows of
//! the physical schemas the query names, with MariaDB's values), and runs
//! the whole query on the planner over them. The answer has MariaDB's
//! column definitions for columns read straight from those tables; a query
//! with any other result column, or one the planner can't run, is left to
//! the backend as before.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::DbId;
use sprig_plan::catalog::{ColumnDef, TableDef};
use sprig_plan::mem::MemDb;

use super::catalog::EngineCatalog;
use super::define::{self, Def, Default, KeyKind};
use super::row::Datum;
use super::show::{Answer, coldef};
use super::table::{ColType, collation_named};

/// MariaDB 11.8's columns of the tables built here: (table, column,
/// character set, length, wire type, flags) as its result metadata shows
/// them.
const COLS: &[(&str, &str, u16, u32, u8, u16)] = &[
    ("TABLES", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("TABLES", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TABLES", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("TABLES", "TABLE_TYPE", 224, 256, 0xfd, 0x1001),
    ("TABLES", "ENGINE", 224, 256, 0xfd, 0x1000),
    ("TABLES", "VERSION", 63, 21, 0x08, 0x1020),
    ("TABLES", "ROW_FORMAT", 224, 40, 0xfd, 0x1000),
    ("TABLES", "TABLE_ROWS", 63, 21, 0x08, 0x1020),
    ("TABLES", "AVG_ROW_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "DATA_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "MAX_DATA_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "INDEX_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "DATA_FREE", 63, 21, 0x08, 0x1020),
    ("TABLES", "AUTO_INCREMENT", 63, 21, 0x08, 0x1020),
    ("TABLES", "CREATE_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "UPDATE_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "CHECK_TIME", 63, 19, 0x0c, 0x1080),
    ("TABLES", "TABLE_COLLATION", 224, 256, 0xfd, 0x1000),
    ("TABLES", "CHECKSUM", 63, 21, 0x08, 0x1020),
    ("TABLES", "CREATE_OPTIONS", 224, 8192, 0xfd, 0x1000),
    ("TABLES", "TABLE_COMMENT", 224, 8192, 0xfd, 0x1001),
    ("TABLES", "MAX_INDEX_LENGTH", 63, 21, 0x08, 0x1020),
    ("TABLES", "TEMPORARY", 224, 4, 0xfd, 0x1000),
    ("COLUMNS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("COLUMNS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "ORDINAL_POSITION", 63, 21, 0x08, 0x1021),
    ("COLUMNS", "COLUMN_DEFAULT", 224, 786_384, 0xfc, 0x1010),
    ("COLUMNS", "IS_NULLABLE", 224, 12, 0xfd, 0x1001),
    ("COLUMNS", "DATA_TYPE", 224, 256, 0xfd, 0x1001),
    ("COLUMNS", "CHARACTER_MAXIMUM_LENGTH", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "CHARACTER_OCTET_LENGTH", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "NUMERIC_PRECISION", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "NUMERIC_SCALE", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "DATETIME_PRECISION", 63, 21, 0x08, 0x1020),
    ("COLUMNS", "CHARACTER_SET_NAME", 224, 128, 0xfd, 0x1000),
    ("COLUMNS", "COLLATION_NAME", 224, 256, 0xfd, 0x1000),
    ("COLUMNS", "COLUMN_TYPE", 224, 786_420, 0xfc, 0x1011),
    ("COLUMNS", "COLUMN_KEY", 224, 12, 0xfd, 0x1001),
    ("COLUMNS", "EXTRA", 224, 320, 0xfd, 0x1001),
    ("COLUMNS", "PRIVILEGES", 224, 320, 0xfd, 0x1001),
    ("COLUMNS", "COLUMN_COMMENT", 224, 4096, 0xfd, 0x1001),
    ("COLUMNS", "IS_GENERATED", 224, 24, 0xfd, 0x1001),
    (
        "COLUMNS",
        "GENERATION_EXPRESSION",
        224,
        786_384,
        0xfc,
        0x1010,
    ),
    (
        "COLUMNS",
        "IS_SYSTEM_TIME_PERIOD_START",
        224,
        12,
        0xfd,
        0x1001,
    ),
    (
        "COLUMNS",
        "IS_SYSTEM_TIME_PERIOD_END",
        224,
        12,
        0xfd,
        0x1001,
    ),
    // The MySQL port's forms of three columns, computed here instead of by
    // the name layer's regular expressions (see `simplify`); invisible, and
    // last, so `SELECT *` reads the visible ones in order.
    ("COLUMNS", "_sprig_my_type", 224, 786_420, 0xfc, 0x1011),
    ("COLUMNS", "_sprig_my_default", 224, 786_384, 0xfc, 0x1010),
    ("COLUMNS", "_sprig_my_extra", 224, 320, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "NON_UNIQUE", 63, 1, 0x08, 0x1001),
    ("STATISTICS", "INDEX_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "SEQ_IN_INDEX", 63, 2, 0x03, 0x1021),
    ("STATISTICS", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("STATISTICS", "COLLATION", 224, 4, 0xfd, 0x1000),
    ("STATISTICS", "CARDINALITY", 63, 21, 0x08, 0x1000),
    ("STATISTICS", "SUB_PART", 63, 3, 0x08, 0x1000),
    ("STATISTICS", "PACKED", 224, 40, 0xfd, 0x1000),
    ("STATISTICS", "NULLABLE", 224, 12, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_TYPE", 224, 64, 0xfd, 0x1001),
    ("STATISTICS", "COMMENT", 224, 64, 0xfd, 0x1001),
    ("STATISTICS", "INDEX_COMMENT", 224, 4096, 0xfd, 0x1001),
    ("STATISTICS", "IGNORED", 224, 12, 0xfd, 0x1001),
    (
        "TABLE_CONSTRAINTS",
        "CONSTRAINT_CATALOG",
        224,
        2048,
        0xfd,
        0x1001,
    ),
    (
        "TABLE_CONSTRAINTS",
        "CONSTRAINT_SCHEMA",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "TABLE_CONSTRAINTS",
        "CONSTRAINT_NAME",
        224,
        256,
        0xfd,
        0x1001,
    ),
    ("TABLE_CONSTRAINTS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TABLE_CONSTRAINTS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    (
        "TABLE_CONSTRAINTS",
        "CONSTRAINT_TYPE",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "KEY_COLUMN_USAGE",
        "CONSTRAINT_CATALOG",
        224,
        2048,
        0xfd,
        0x1001,
    ),
    (
        "KEY_COLUMN_USAGE",
        "CONSTRAINT_SCHEMA",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "KEY_COLUMN_USAGE",
        "CONSTRAINT_NAME",
        224,
        256,
        0xfd,
        0x1001,
    ),
    ("KEY_COLUMN_USAGE", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "COLUMN_NAME", 224, 256, 0xfd, 0x1001),
    ("KEY_COLUMN_USAGE", "ORDINAL_POSITION", 63, 10, 0x08, 0x1001),
    (
        "KEY_COLUMN_USAGE",
        "POSITION_IN_UNIQUE_CONSTRAINT",
        63,
        10,
        0x08,
        0x1000,
    ),
    (
        "KEY_COLUMN_USAGE",
        "REFERENCED_TABLE_SCHEMA",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "KEY_COLUMN_USAGE",
        "REFERENCED_TABLE_NAME",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "KEY_COLUMN_USAGE",
        "REFERENCED_COLUMN_NAME",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "CHECK_CONSTRAINTS",
        "CONSTRAINT_CATALOG",
        224,
        2048,
        0xfd,
        0x1001,
    ),
    (
        "CHECK_CONSTRAINTS",
        "CONSTRAINT_SCHEMA",
        224,
        256,
        0xfd,
        0x1001,
    ),
    ("CHECK_CONSTRAINTS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    (
        "CHECK_CONSTRAINTS",
        "CONSTRAINT_NAME",
        224,
        256,
        0xfd,
        0x1001,
    ),
    ("CHECK_CONSTRAINTS", "LEVEL", 224, 24, 0xfd, 0x1001),
    (
        "CHECK_CONSTRAINTS",
        "CHECK_CLAUSE",
        224,
        786_384,
        0xfc,
        0x1011,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "CONSTRAINT_CATALOG",
        224,
        2048,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "CONSTRAINT_SCHEMA",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "CONSTRAINT_NAME",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "UNIQUE_CONSTRAINT_CATALOG",
        224,
        2048,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "UNIQUE_CONSTRAINT_SCHEMA",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "UNIQUE_CONSTRAINT_NAME",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "MATCH_OPTION",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "UPDATE_RULE",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "DELETE_RULE",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "TABLE_NAME",
        224,
        256,
        0xfd,
        0x1001,
    ),
    (
        "REFERENTIAL_CONSTRAINTS",
        "REFERENCED_TABLE_NAME",
        224,
        256,
        0xfd,
        0x1000,
    ),
    ("SCHEMATA", "CATALOG_NAME", 224, 2048, 0xfd, 0x1001),
    ("SCHEMATA", "SCHEMA_NAME", 224, 256, 0xfd, 0x1001),
    (
        "SCHEMATA",
        "DEFAULT_CHARACTER_SET_NAME",
        224,
        128,
        0xfd,
        0x1001,
    ),
    ("SCHEMATA", "DEFAULT_COLLATION_NAME", 224, 256, 0xfd, 0x1001),
    ("SCHEMATA", "SQL_PATH", 224, 2048, 0xfd, 0x1000),
    ("SCHEMATA", "SCHEMA_COMMENT", 224, 4096, 0xfd, 0x1001),
    ("VIEWS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("VIEWS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("VIEWS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("VIEWS", "VIEW_DEFINITION", 224, 786_420, 0xfc, 0x1011),
    ("VIEWS", "CHECK_OPTION", 224, 32, 0xfd, 0x1001),
    ("VIEWS", "IS_UPDATABLE", 224, 12, 0xfd, 0x1001),
    ("VIEWS", "DEFINER", 224, 1536, 0xfd, 0x1001),
    ("VIEWS", "SECURITY_TYPE", 224, 28, 0xfd, 0x1001),
    ("VIEWS", "CHARACTER_SET_CLIENT", 224, 128, 0xfd, 0x1001),
    ("VIEWS", "COLLATION_CONNECTION", 224, 256, 0xfd, 0x1001),
    ("VIEWS", "ALGORITHM", 224, 40, 0xfd, 0x1001),
    ("ROUTINES", "SPECIFIC_NAME", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_NAME", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_TYPE", 224, 52, 0xfd, 0x1001),
    ("ROUTINES", "DATA_TYPE", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "CHARACTER_MAXIMUM_LENGTH", 63, 21, 0x03, 0x1000),
    ("ROUTINES", "CHARACTER_OCTET_LENGTH", 63, 21, 0x03, 0x1000),
    ("ROUTINES", "NUMERIC_PRECISION", 63, 21, 0x03, 0x1000),
    ("ROUTINES", "NUMERIC_SCALE", 63, 21, 0x03, 0x1000),
    ("ROUTINES", "DATETIME_PRECISION", 63, 21, 0x08, 0x1020),
    ("ROUTINES", "CHARACTER_SET_NAME", 224, 256, 0xfd, 0x1000),
    ("ROUTINES", "COLLATION_NAME", 224, 256, 0xfd, 0x1000),
    ("ROUTINES", "DTD_IDENTIFIER", 224, 786_420, 0xfc, 0x1010),
    ("ROUTINES", "ROUTINE_BODY", 224, 32, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_DEFINITION", 224, 786_420, 0xfc, 0x1010),
    ("ROUTINES", "EXTERNAL_NAME", 224, 256, 0xfd, 0x1000),
    ("ROUTINES", "EXTERNAL_LANGUAGE", 224, 256, 0xfd, 0x1000),
    ("ROUTINES", "PARAMETER_STYLE", 224, 32, 0xfd, 0x1001),
    ("ROUTINES", "IS_DETERMINISTIC", 224, 12, 0xfd, 0x1001),
    ("ROUTINES", "SQL_DATA_ACCESS", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "SQL_PATH", 224, 256, 0xfd, 0x1000),
    ("ROUTINES", "SECURITY_TYPE", 224, 28, 0xfd, 0x1001),
    ("ROUTINES", "CREATED", 63, 19, 0x0c, 0x1081),
    ("ROUTINES", "LAST_ALTERED", 63, 19, 0x0c, 0x1081),
    ("ROUTINES", "SQL_MODE", 224, 32768, 0xfd, 0x1001),
    ("ROUTINES", "ROUTINE_COMMENT", 224, 786_420, 0xfc, 0x1011),
    ("ROUTINES", "DEFINER", 224, 1536, 0xfd, 0x1001),
    ("ROUTINES", "CHARACTER_SET_CLIENT", 224, 128, 0xfd, 0x1001),
    ("ROUTINES", "COLLATION_CONNECTION", 224, 256, 0xfd, 0x1001),
    ("ROUTINES", "DATABASE_COLLATION", 224, 256, 0xfd, 0x1001),
    ("PARAMETERS", "SPECIFIC_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("PARAMETERS", "SPECIFIC_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("PARAMETERS", "SPECIFIC_NAME", 224, 256, 0xfd, 0x1001),
    ("PARAMETERS", "ORDINAL_POSITION", 63, 21, 0x03, 0x1001),
    ("PARAMETERS", "PARAMETER_MODE", 224, 20, 0xfd, 0x1000),
    ("PARAMETERS", "PARAMETER_NAME", 224, 256, 0xfd, 0x1000),
    ("PARAMETERS", "DATA_TYPE", 224, 256, 0xfd, 0x1001),
    (
        "PARAMETERS",
        "CHARACTER_MAXIMUM_LENGTH",
        63,
        21,
        0x03,
        0x1000,
    ),
    ("PARAMETERS", "CHARACTER_OCTET_LENGTH", 63, 21, 0x03, 0x1000),
    ("PARAMETERS", "NUMERIC_PRECISION", 63, 21, 0x03, 0x1000),
    ("PARAMETERS", "NUMERIC_SCALE", 63, 21, 0x03, 0x1000),
    ("PARAMETERS", "DATETIME_PRECISION", 63, 21, 0x08, 0x1020),
    ("PARAMETERS", "CHARACTER_SET_NAME", 224, 256, 0xfd, 0x1000),
    ("PARAMETERS", "COLLATION_NAME", 224, 256, 0xfd, 0x1000),
    ("PARAMETERS", "DTD_IDENTIFIER", 224, 786_420, 0xfc, 0x1011),
    ("PARAMETERS", "ROUTINE_TYPE", 224, 36, 0xfd, 0x1001),
    ("TRIGGERS", "TRIGGER_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("TRIGGERS", "TRIGGER_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TRIGGERS", "TRIGGER_NAME", 224, 256, 0xfd, 0x1001),
    ("TRIGGERS", "EVENT_MANIPULATION", 224, 24, 0xfd, 0x1001),
    ("TRIGGERS", "EVENT_OBJECT_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("TRIGGERS", "EVENT_OBJECT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("TRIGGERS", "EVENT_OBJECT_TABLE", 224, 256, 0xfd, 0x1001),
    ("TRIGGERS", "ACTION_ORDER", 63, 4, 0x08, 0x1001),
    ("TRIGGERS", "ACTION_CONDITION", 224, 786_420, 0xfc, 0x1010),
    ("TRIGGERS", "ACTION_STATEMENT", 224, 786_420, 0xfc, 0x1011),
    ("TRIGGERS", "ACTION_ORIENTATION", 224, 36, 0xfd, 0x1001),
    ("TRIGGERS", "ACTION_TIMING", 224, 24, 0xfd, 0x1001),
    (
        "TRIGGERS",
        "ACTION_REFERENCE_OLD_TABLE",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "TRIGGERS",
        "ACTION_REFERENCE_NEW_TABLE",
        224,
        256,
        0xfd,
        0x1000,
    ),
    (
        "TRIGGERS",
        "ACTION_REFERENCE_OLD_ROW",
        224,
        12,
        0xfd,
        0x1001,
    ),
    (
        "TRIGGERS",
        "ACTION_REFERENCE_NEW_ROW",
        224,
        12,
        0xfd,
        0x1001,
    ),
    ("TRIGGERS", "CREATED", 63, 22, 0x0c, 0x1080),
    ("TRIGGERS", "SQL_MODE", 224, 32768, 0xfd, 0x1001),
    ("TRIGGERS", "DEFINER", 224, 1536, 0xfd, 0x1001),
    ("TRIGGERS", "CHARACTER_SET_CLIENT", 224, 128, 0xfd, 0x1001),
    ("TRIGGERS", "COLLATION_CONNECTION", 224, 256, 0xfd, 0x1001),
    ("TRIGGERS", "DATABASE_COLLATION", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "EVENT_CATALOG", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "EVENT_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "EVENT_NAME", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "DEFINER", 224, 1536, 0xfd, 0x1001),
    ("EVENTS", "TIME_ZONE", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "EVENT_BODY", 224, 32, 0xfd, 0x1001),
    ("EVENTS", "EVENT_DEFINITION", 224, 786_420, 0xfc, 0x1011),
    ("EVENTS", "EVENT_TYPE", 224, 36, 0xfd, 0x1001),
    ("EVENTS", "EXECUTE_AT", 63, 19, 0xc, 0x1080),
    ("EVENTS", "INTERVAL_VALUE", 224, 1024, 0xfd, 0x1000),
    ("EVENTS", "INTERVAL_FIELD", 224, 72, 0xfd, 0x1000),
    ("EVENTS", "SQL_MODE", 224, 32_768, 0xfd, 0x1001),
    ("EVENTS", "STARTS", 63, 19, 0xc, 0x1080),
    ("EVENTS", "ENDS", 63, 19, 0xc, 0x1080),
    ("EVENTS", "STATUS", 224, 72, 0xfd, 0x1001),
    ("EVENTS", "ON_COMPLETION", 224, 48, 0xfd, 0x1001),
    ("EVENTS", "CREATED", 63, 19, 0xc, 0x1081),
    ("EVENTS", "LAST_ALTERED", 63, 19, 0xc, 0x1081),
    ("EVENTS", "LAST_EXECUTED", 63, 19, 0xc, 0x1080),
    ("EVENTS", "EVENT_COMMENT", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "ORIGINATOR", 63, 10, 0x8, 0x1001),
    ("EVENTS", "CHARACTER_SET_CLIENT", 224, 128, 0xfd, 0x1001),
    ("EVENTS", "COLLATION_CONNECTION", 224, 256, 0xfd, 0x1001),
    ("EVENTS", "DATABASE_COLLATION", 224, 256, 0xfd, 0x1001),
    ("PARTITIONS", "TABLE_CATALOG", 224, 2048, 0xfd, 0x1001),
    ("PARTITIONS", "TABLE_SCHEMA", 224, 256, 0xfd, 0x1001),
    ("PARTITIONS", "TABLE_NAME", 224, 256, 0xfd, 0x1001),
    ("PARTITIONS", "PARTITION_NAME", 224, 256, 0xfd, 0x1000),
    ("PARTITIONS", "SUBPARTITION_NAME", 224, 256, 0xfd, 0x1000),
    (
        "PARTITIONS",
        "PARTITION_ORDINAL_POSITION",
        63,
        21,
        0x08,
        0x1020,
    ),
    (
        "PARTITIONS",
        "SUBPARTITION_ORDINAL_POSITION",
        63,
        21,
        0x08,
        0x1020,
    ),
    ("PARTITIONS", "PARTITION_METHOD", 224, 72, 0xfd, 0x1000),
    ("PARTITIONS", "SUBPARTITION_METHOD", 224, 48, 0xfd, 0x1000),
    (
        "PARTITIONS",
        "PARTITION_EXPRESSION",
        224,
        786_420,
        0xfc,
        0x1010,
    ),
    (
        "PARTITIONS",
        "SUBPARTITION_EXPRESSION",
        224,
        786_420,
        0xfc,
        0x1010,
    ),
    (
        "PARTITIONS",
        "PARTITION_DESCRIPTION",
        224,
        786_420,
        0xfc,
        0x1010,
    ),
    ("PARTITIONS", "TABLE_ROWS", 63, 21, 0x08, 0x1021),
    ("PARTITIONS", "AVG_ROW_LENGTH", 63, 21, 0x08, 0x1021),
    ("PARTITIONS", "DATA_LENGTH", 63, 21, 0x08, 0x1021),
    ("PARTITIONS", "MAX_DATA_LENGTH", 63, 21, 0x08, 0x1020),
    ("PARTITIONS", "INDEX_LENGTH", 63, 21, 0x08, 0x1021),
    ("PARTITIONS", "DATA_FREE", 63, 21, 0x08, 0x1021),
    ("PARTITIONS", "CREATE_TIME", 63, 19, 0x0c, 0x1080),
    ("PARTITIONS", "UPDATE_TIME", 63, 19, 0x0c, 0x1080),
    ("PARTITIONS", "CHECK_TIME", 63, 19, 0x0c, 0x1080),
    ("PARTITIONS", "CHECKSUM", 63, 21, 0x08, 0x1020),
    ("PARTITIONS", "PARTITION_COMMENT", 224, 320, 0xfd, 0x1001),
    ("PARTITIONS", "NODEGROUP", 224, 48, 0xfd, 0x1001),
    ("PARTITIONS", "TABLESPACE_NAME", 224, 256, 0xfd, 0x1000),
];

/// The tables built here.
pub(crate) const TABLES: [&str; 14] = [
    "SCHEMATA",
    "PARTITIONS",
    "TRIGGERS",
    "EVENTS",
    "ROUTINES",
    "PARAMETERS",
    "TABLES",
    "VIEWS",
    "COLUMNS",
    "STATISTICS",
    "TABLE_CONSTRAINTS",
    "KEY_COLUMN_USAGE",
    "CHECK_CONSTRAINTS",
    "REFERENTIAL_CONSTRAINTS",
];

fn cols_of(
    t: &str,
) -> impl Iterator<Item = &'static (&'static str, &'static str, u16, u32, u8, u16)> + '_ {
    COLS.iter().filter(move |c| c.0 == t)
}

/// [`coldef`] with `table` (as the query writes it) apart from
/// `org_table`.
#[allow(clippy::too_many_arguments)] // a column definition's fields
fn coldef_in(
    schema: &str,
    table: &str,
    org_table: &str,
    name: &str,
    org: &str,
    charset: u16,
    len: u32,
    ty: u8,
    flags: u16,
    decimals: u8,
) -> Bytes {
    let d = coldef(
        schema, org_table, name, org, charset, len, ty, flags, decimals,
    );
    if table == org_table {
        return d;
    }
    // Replace the first table field (after the catalog and the schema).
    let mut out = bytes::BytesMut::with_capacity(d.len() + table.len());
    let skip = 1 + 3 + 1 + schema.len();
    out.extend_from_slice(&d[..skip]);
    out.extend_from_slice(&[u8::try_from(table.len()).unwrap_or(0)]);
    out.extend_from_slice(table.as_bytes());
    out.extend_from_slice(&d[skip + 1 + org_table.len()..]);
    out.freeze()
}

/// The planner's definition of table `t`.
fn plan_def(t: &str) -> TableDef {
    let ci = collation_named("utf8mb4_general_ci");
    let columns = cols_of(t)
        .map(|&(_, name, _, len, ty, flags)| {
            let cty = match ty {
                0x08 => ColType::Int {
                    bytes: 8,
                    unsigned: flags & 0x20 != 0,
                },
                0x03 => ColType::Int {
                    bytes: 4,
                    unsigned: flags & 0x20 != 0,
                },
                0x0c => ColType::DateTime { fsp: 0 },
                _ => match ci {
                    Some(collation) => ColType::VarChar {
                        chars: (len / 4).max(1),
                        collation,
                    },
                    None => ColType::VarBinary {
                        bytes: len.min(65_535),
                    },
                },
            };
            let mut c = ColumnDef::new(name, cty);
            c.invisible = name.starts_with("_sprig_my_");
            if flags & 1 != 0 { c.not_null() } else { c }
        })
        .collect();
    TableDef {
        schema: "information_schema".into(),
        name: t.into(),
        columns,
        primary: None,
        indexes: Vec::new(),
    }
}

/// A value of a row being built.
#[derive(Clone)]
enum V {
    S(String),
    U(u64),
    N,
}

fn s(x: impl Into<String>) -> V {
    V::S(x.into())
}

fn row_of(t: &str, vals: &HashMap<&str, V>) -> Vec<Datum> {
    cols_of(t)
        .map(|c| match vals.get(c.1) {
            // A time (a routine's `CREATED`): its value, not its text.
            Some(V::S(x)) if c.4 == 0x0c => super::temporal::datetime_from_str(x.as_bytes())
                .and_then(|r| super::temporal::settle(&r, 0, false))
                .map_or(Datum::Null, Datum::DateTime),
            Some(V::S(x)) => Datum::Str(Bytes::from(x.clone().into_bytes())),
            // A signed column (no UNSIGNED flag) takes a signed value.
            Some(V::U(n)) if c.5 & 0x20 == 0 => Datum::Int(i64::try_from(*n).unwrap_or(i64::MAX)),
            Some(V::U(n)) => Datum::UInt(*n),
            Some(V::N) | None => Datum::Null,
        })
        .collect()
}

/// The `COLUMN_DEFAULT` of `information_schema.COLUMNS`: string constants
/// quoted, a nullable column without a default `NULL` (the word), a `NOT
/// NULL` one SQL NULL.
fn is_default(c: &define::Col) -> V {
    if c.generated.is_some() {
        return if c.not_null { V::N } else { s("NULL") };
    }
    match &c.default {
        None | Some(Default::Null) if !c.not_null && !c.auto_inc => s("NULL"),
        None | Some(Default::Null) => V::N,
        Some(Default::Lit(l)) => s(mariadb_quoted(l)),
        Some(Default::Raw(e) | Default::Expr(e)) => s(define::expr::print(e)),
        Some(Default::Now(p)) => s(if *p == 0 {
            "current_timestamp()".to_owned()
        } else {
            format!("current_timestamp({p})")
        }),
    }
}

/// A string constant as MariaDB's `COLUMN_DEFAULT` quotes it: a quote
/// doubled, a backslash, newline, carriage return and NUL escaped with a
/// backslash. Anything else (a number, a bit or hex literal) as it is.
fn mariadb_quoted(l: &str) -> String {
    if !(l.len() >= 2 && l.starts_with('\'') && l.ends_with('\'')) {
        return l.to_owned();
    }
    let v = super::show::unquote(l);
    let mut o = String::with_capacity(v.len() + 2);
    o.push('\'');
    for ch in v.chars() {
        match ch {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// Precision, scale and datetime precision of a column type, as `COLUMNS`
/// shows them.
fn numeric(c: &define::Col) -> (V, V, V) {
    let b = c.ty.base.as_str();
    let args: Vec<u64> =
        c.ty.args
            .trim_matches(|ch| ch == '(' || ch == ')')
            .split(',')
            .filter_map(|x| x.trim().parse().ok())
            .collect();
    let u = c.ty.unsigned;
    match b {
        "tinyint" => (V::U(3), V::U(0), V::N),
        "smallint" => (V::U(5), V::U(0), V::N),
        "mediumint" => (V::U(if u { 8 } else { 7 }), V::U(0), V::N),
        "int" => (V::U(10), V::U(0), V::N),
        "bigint" => (V::U(if u { 20 } else { 19 }), V::U(0), V::N),
        "decimal" => (
            V::U(args.first().copied().unwrap_or(10)),
            V::U(args.get(1).copied().unwrap_or(0)),
            V::N,
        ),
        "float" => match args.as_slice() {
            [m, d] => (V::U(*m), V::U(*d), V::N),
            _ => (V::U(12), V::N, V::N),
        },
        "double" => match args.as_slice() {
            [m, d] => (V::U(*m), V::U(*d), V::N),
            _ => (V::U(22), V::N, V::N),
        },
        "bit" => (V::U(args.first().copied().unwrap_or(1)), V::N, V::N),
        "datetime" | "timestamp" | "time" => (V::N, V::N, V::U(args.first().copied().unwrap_or(0))),
        _ => (V::N, V::N, V::N),
    }
}

/// Character and octet lengths.
fn lengths(c: &define::Col) -> (V, V) {
    let mb = u64::from(c.ty.mbmax());
    let n = u64::from(c.ty.len);
    match c.ty.base.as_str() {
        "char" | "varchar" => (V::U(n), V::U(n * mb)),
        "binary" | "varbinary" => (V::U(n), V::U(n)),
        "tinytext" => (V::U(255 / mb), V::U(255)),
        "text" => (V::U(65_535 / mb), V::U(65_535)),
        "mediumtext" => (V::U(16_777_215 / mb), V::U(16_777_215)),
        "longtext" => (V::U(4_294_967_295 / mb), V::U(4_294_967_295)),
        "tinyblob" => (V::U(255), V::U(255)),
        "blob" => (V::U(65_535), V::U(65_535)),
        "mediumblob" => (V::U(16_777_215), V::U(16_777_215)),
        "longblob" => (V::U(4_294_967_295), V::U(4_294_967_295)),
        "enum" | "set" => {
            let longest =
                c.ty.members
                    .iter()
                    .map(|m| m.chars().count())
                    .max()
                    .unwrap_or(0) as u64;
            let n = if c.ty.base == "set" {
                c.ty.members
                    .iter()
                    .map(|m| m.chars().count() as u64)
                    .sum::<u64>()
                    + c.ty.members.len().saturating_sub(1) as u64
            } else {
                longest
            };
            (V::U(n), V::U(n * mb))
        }
        _ => (V::N, V::N),
    }
}

fn key_of(def: &Def, col: &str) -> &'static str {
    let first = |k: &define::Key| {
        k.parts
            .first()
            .is_some_and(|p| p.col.eq_ignore_ascii_case(col))
    };
    if def
        .primary()
        .is_some_and(|k| k.parts.iter().any(|p| p.col.eq_ignore_ascii_case(col)))
    {
        "PRI"
    } else if def
        .keys
        .iter()
        .any(|k| k.kind == KeyKind::Unique && k.parts.len() == 1 && first(k))
    {
        "UNI"
    } else if def.keys.iter().any(first) {
        "MUL"
    } else {
        ""
    }
}

fn extra_of(c: &define::Col) -> String {
    let mut parts: Vec<String> = Vec::new();
    if c.auto_inc {
        parts.push("auto_increment".into());
    }
    if let Some(p) = c.on_update {
        parts.push(if p == 0 {
            "on update current_timestamp()".into()
        } else {
            format!("on update current_timestamp({p})")
        });
    }
    if let Some((_, stored)) = &c.generated {
        parts.push(if *stored {
            "STORED GENERATED".into()
        } else {
            "VIRTUAL GENERATED".into()
        });
    }
    if c.invisible {
        parts.push("INVISIBLE".into());
    }
    parts.join(" ")
}

/// `COLUMN_TYPE` as MySQL 8.4 shows it: integer types without their
/// display width, except `tinyint(1)` and `ZEROFILL` columns.
fn mysql_type(ty: &str) -> String {
    if ty.starts_with("tinyint(1)") || ty.ends_with("zerofill") {
        return ty.to_owned();
    }
    for base in ["tinyint", "smallint", "mediumint", "int", "bigint", "year"] {
        if let Some(rest) = ty.strip_prefix(base).and_then(|r| r.strip_prefix('('))
            && let Some(close) = rest.find(')')
            && close > 0
            && rest[..close].bytes().all(|b| b.is_ascii_digit())
        {
            return format!("{base}{}", &rest[close + 1..]);
        }
    }
    ty.to_owned()
}

/// `COLUMN_DEFAULT` as MySQL 8.4 shows it, from the MariaDB-form value
/// `d`: no default and `NULL` both `NULL`, a literal unquoted with its
/// escapes undone, `CURRENT_TIMESTAMP` in capitals, `localtimestamp(n)`
/// (what a `(now(n))` default becomes) as `now(n)`.
fn mysql_default(d: &str) -> Option<String> {
    if d == "NULL" {
        return None;
    }
    if d.len() >= 2 && d.starts_with('\'') && d.ends_with('\'') {
        let body = &d[1..d.len() - 1];
        return Some(
            body.replace("\\\\", "\0")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("''", "'")
                .replace('\0', "\\"),
        );
    }
    if d == "current_timestamp()" {
        return Some("CURRENT_TIMESTAMP".to_owned());
    }
    let digit_call = |rest: &str, optional: bool| match rest.as_bytes() {
        [b'(', b')'] => optional,
        [b'(', n, b')'] => (b'0'..=b'6').contains(n),
        _ => false,
    };
    if let Some(rest) = d.strip_prefix("current_timestamp")
        && digit_call(rest, false)
    {
        return Some(d.to_ascii_uppercase());
    }
    if let Some(rest) = d.strip_prefix("localtimestamp")
        && digit_call(rest, true)
    {
        return Some(format!("now{rest}"));
    }
    Some(d.to_owned())
}

/// Whether `d` is a number as a default shows it: `[-+]digits[.digits][e[-+]digits]`.
fn plain_number(d: &str) -> bool {
    let b = d.as_bytes();
    let mut i = usize::from(matches!(b.first(), Some(b'-' | b'+')));
    let digits = |i: &mut usize| {
        let s = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > s
    };
    if !digits(&mut i) {
        return false;
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'-' | b'+')) {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == b.len()
}

/// `EXTRA` as MySQL 8.4 shows it: `DEFAULT_GENERATED` for an expression
/// default (`d`, the MariaDB-form `COLUMN_DEFAULT`), `CURRENT_TIMESTAMP`
/// in capitals.
fn mysql_extra(d: Option<&str>, extra: &str) -> String {
    let bits = |d: &str| matches!(d.as_bytes(), [b'b' | b'B' | b'x' | b'X', b'\'', ..]);
    let generated =
        d.is_some_and(|d| d != "NULL" && !d.starts_with('\'') && !plain_number(d) && !bits(d));
    let mut e = String::with_capacity(extra.len());
    let lower = extra.to_ascii_lowercase();
    let mut i = 0;
    while let Some(k) = lower[i..].find("current_timestamp(") {
        let at = i + k;
        let after = at + "current_timestamp(".len();
        let rest = &extra[after..];
        let n = match rest.as_bytes() {
            [d, b')', ..] if (b'0'..=b'6').contains(d) => Some(&rest[..1]),
            [b')', ..] => Some(""),
            _ => None,
        };
        e.push_str(&extra[i..at]);
        if let Some(n) = n {
            e.push_str("CURRENT_TIMESTAMP(");
            e.push_str(n);
            e.push(')');
            i = after + n.len() + 1;
        } else {
            e.push_str(&extra[at..after]);
            i = after;
        }
    }
    e.push_str(&extra[i..]);
    let e = e.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP");
    let mut out = String::new();
    if generated {
        out.push_str("DEFAULT_GENERATED ");
    }
    out.push_str(&e);
    out.trim_matches(' ').to_owned()
}

/// `secs` since the Unix epoch as `YYYY-MM-DD hh:mm:ss` (UTC).
pub(crate) fn utc_datetime(secs: u64) -> String {
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rest = secs % 86_400;
    // Days to a civil date (proleptic Gregorian).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rest / 3600,
        rest / 60 % 60,
        rest % 60
    )
}

/// Adds every row one table contributes to the in-memory tables.
#[allow(clippy::too_many_lines)] // one block per table built
fn add_table(
    db: &mut MemDb,
    schema: &str,
    def: &Def,
    next: Option<u64>,
    created: Option<String>,
    rows: Option<u64>,
    part_rows: Option<&Vec<u64>>,
) {
    let mut put = |t: &str, vals: HashMap<&str, V>| {
        if let Err(e) = db.insert(t, row_of(t, &vals)) {
            tracing::warn!(table = t, error = %e.message, "information_schema: a row doesn't fit its table");
        }
    };
    let base = |vals: &mut HashMap<&str, V>| {
        vals.insert("TABLE_CATALOG", s("def"));
        vals.insert("TABLE_SCHEMA", s(schema));
        vals.insert("TABLE_NAME", s(def.name.clone()));
    };
    let mut v = HashMap::new();
    base(&mut v);
    v.insert("TABLE_TYPE", s("BASE TABLE"));
    v.insert("ENGINE", s("InnoDB"));
    v.insert("VERSION", V::U(10));
    v.insert("ROW_FORMAT", s("Dynamic"));
    // InnoDB's figures for a table in one page: the average row is the
    // page shared by the rows.
    let rows = rows.unwrap_or(0);
    v.insert("TABLE_ROWS", V::U(rows));
    v.insert(
        "AVG_ROW_LENGTH",
        V::U(16384_u64.checked_div(rows).unwrap_or(0)),
    );
    v.insert("DATA_LENGTH", V::U(16384));
    v.insert("MAX_DATA_LENGTH", V::U(0));
    v.insert(
        "INDEX_LENGTH",
        V::U(
            16384
                * def
                    .keys
                    .iter()
                    .filter(|k| k.kind != KeyKind::Primary)
                    .count() as u64,
        ),
    );
    v.insert("DATA_FREE", V::U(0));
    v.insert("CREATE_TIME", created.map_or(V::N, s));
    v.insert("AUTO_INCREMENT", next.map_or(V::N, V::U));
    v.insert("TABLE_COLLATION", s(def.cs.1.clone()));
    v.insert("CREATE_OPTIONS", s(""));
    v.insert("TABLE_COMMENT", s(def.comment.clone().unwrap_or_default()));
    v.insert("MAX_INDEX_LENGTH", V::U(0));
    v.insert("TEMPORARY", s("N"));
    // An unpartitioned table: one row with no partition, as the servers
    // list it; a partitioned one: a row per partition.
    let mut p = HashMap::new();
    base(&mut p);
    for k in [
        "TABLE_ROWS",
        "AVG_ROW_LENGTH",
        "DATA_LENGTH",
        "INDEX_LENGTH",
        "DATA_FREE",
    ] {
        if let Some(V::U(n)) = v.get(k) {
            p.insert(k, V::U(*n));
        }
    }
    p.insert("PARTITION_COMMENT", s(""));
    p.insert("NODEGROUP", s(""));
    if let Some(spec) = &def.partition {
        v.insert("CREATE_OPTIONS", s("partitioned"));
        for (i, part) in spec.parts.iter().enumerate() {
            let mut q = p.clone();
            q.insert("PARTITION_NAME", s(part.name.clone()));
            q.insert("PARTITION_ORDINAL_POSITION", V::U(i as u64 + 1));
            q.insert("PARTITION_METHOD", s(spec.method_name()));
            q.insert("PARTITION_EXPRESSION", s(spec.expression()));
            q.insert("PARTITION_DESCRIPTION", spec.description(i).map_or(V::N, s));
            let n = part_rows.and_then(|c| c.get(i)).copied().unwrap_or(0);
            q.insert("TABLE_ROWS", V::U(n));
            q.insert(
                "AVG_ROW_LENGTH",
                V::U(16384_u64.checked_div(n).unwrap_or(0)),
            );
            q.insert("DATA_LENGTH", V::U(16384));
            q.insert(
                "PARTITION_COMMENT",
                s(part.comment.clone().unwrap_or_default()),
            );
            q.insert("NODEGROUP", s("default"));
            put("PARTITIONS", q);
        }
        put("TABLES", v);
    } else {
        put("TABLES", v);
        put("PARTITIONS", p);
    }
    for (i, c) in def.cols.iter().enumerate() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("COLUMN_NAME", s(c.name.clone()));
        v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
        v.insert("IS_NULLABLE", s(if c.not_null { "NO" } else { "YES" }));
        v.insert("DATA_TYPE", s(c.ty.base.clone()));
        let (cl, ol) = lengths(c);
        v.insert("CHARACTER_MAXIMUM_LENGTH", cl);
        v.insert("CHARACTER_OCTET_LENGTH", ol);
        let (p, sc, dp) = numeric(c);
        v.insert("NUMERIC_PRECISION", p);
        v.insert("NUMERIC_SCALE", sc);
        v.insert("DATETIME_PRECISION", dp);
        if let Some((cs, co)) = &c.ty.cs {
            v.insert("CHARACTER_SET_NAME", s(cs.clone()));
            v.insert("COLLATION_NAME", s(co.clone()));
        }
        v.insert("COLUMN_KEY", s(key_of(def, &c.name)));
        let default = is_default(c);
        let extra = extra_of(c);
        let ty = c.ty.print();
        v.insert("_sprig_my_type", s(mysql_type(&ty)));
        v.insert(
            "_sprig_my_default",
            match &default {
                V::S(d) => mysql_default(d).map_or(V::N, s),
                _ => V::N,
            },
        );
        v.insert(
            "_sprig_my_extra",
            s(mysql_extra(
                match &default {
                    V::S(d) => Some(d.as_str()),
                    _ => None,
                },
                &extra,
            )),
        );
        v.insert("EXTRA", s(extra));
        v.insert("COLUMN_TYPE", s(ty));
        v.insert("COLUMN_DEFAULT", default);
        v.insert("PRIVILEGES", s("select,insert,update,references"));
        v.insert("COLUMN_COMMENT", s(c.comment.clone().unwrap_or_default()));
        v.insert(
            "IS_GENERATED",
            s(if c.generated.is_some() {
                "ALWAYS"
            } else {
                "NEVER"
            }),
        );
        v.insert(
            "GENERATION_EXPRESSION",
            c.generated
                .as_ref()
                .map_or(V::N, |(g, _)| s(define::expr::print(g))),
        );
        v.insert("IS_SYSTEM_TIME_PERIOD_START", s("NO"));
        v.insert("IS_SYSTEM_TIME_PERIOD_END", s("NO"));
        put("COLUMNS", v);
    }
    for k in &def.keys {
        for (i, p) in k.parts.iter().enumerate() {
            let mut v = HashMap::new();
            base(&mut v);
            v.insert(
                "NON_UNIQUE",
                V::U(u64::from(!matches!(
                    k.kind,
                    KeyKind::Primary | KeyKind::Unique
                ))),
            );
            v.insert("INDEX_SCHEMA", s(schema));
            v.insert("INDEX_NAME", s(k.name.clone()));
            v.insert("SEQ_IN_INDEX", V::U(i as u64 + 1));
            v.insert("COLUMN_NAME", s(p.col.clone()));
            v.insert("COLLATION", s(if p.desc { "D" } else { "A" }));
            v.insert("CARDINALITY", V::U(0));
            v.insert("SUB_PART", p.len.map_or(V::N, |l| V::U(u64::from(l))));
            let nullable = def.col(&p.col).is_some_and(|ci| !def.cols[ci].not_null);
            v.insert("NULLABLE", s(if nullable { "YES" } else { "" }));
            v.insert(
                "INDEX_TYPE",
                s(k.using.clone().unwrap_or_else(|| "BTREE".into())),
            );
            v.insert("COMMENT", s(""));
            v.insert("INDEX_COMMENT", s(k.comment.clone().unwrap_or_default()));
            v.insert("IGNORED", s(if k.ignored { "YES" } else { "NO" }));
            put("STATISTICS", v);
        }
    }
    let constraint = |name: &str, ty: &str| {
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("CONSTRAINT_NAME", s(name));
        v.insert("TABLE_SCHEMA", s(schema));
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("CONSTRAINT_TYPE", s(ty));
        v
    };
    for k in def
        .keys
        .iter()
        .filter(|k| matches!(k.kind, KeyKind::Primary | KeyKind::Unique))
    {
        put(
            "TABLE_CONSTRAINTS",
            constraint(
                &k.name,
                if k.kind == KeyKind::Primary {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                },
            ),
        );
        for (i, p) in k.parts.iter().enumerate() {
            let mut v = HashMap::new();
            v.insert("CONSTRAINT_CATALOG", s("def"));
            v.insert("CONSTRAINT_SCHEMA", s(schema));
            v.insert("CONSTRAINT_NAME", s(k.name.clone()));
            base(&mut v);
            v.insert("COLUMN_NAME", s(p.col.clone()));
            v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
            put("KEY_COLUMN_USAGE", v);
        }
    }
    for f in &def.fks {
        put("TABLE_CONSTRAINTS", constraint(&f.name, "FOREIGN KEY"));
        for (i, c) in f.cols.iter().enumerate() {
            let mut v = HashMap::new();
            v.insert("CONSTRAINT_CATALOG", s("def"));
            v.insert("CONSTRAINT_SCHEMA", s(schema));
            v.insert("CONSTRAINT_NAME", s(f.name.clone()));
            base(&mut v);
            v.insert("COLUMN_NAME", s(c.clone()));
            v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
            v.insert("POSITION_IN_UNIQUE_CONSTRAINT", V::U(i as u64 + 1));
            v.insert(
                "REFERENCED_TABLE_SCHEMA",
                s(f.ref_db.clone().unwrap_or_else(|| schema.to_owned())),
            );
            v.insert("REFERENCED_TABLE_NAME", s(f.ref_table.clone()));
            v.insert(
                "REFERENCED_COLUMN_NAME",
                s(f.ref_cols.get(i).cloned().unwrap_or_default()),
            );
            put("KEY_COLUMN_USAGE", v);
        }
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("CONSTRAINT_NAME", s(f.name.clone()));
        v.insert("UNIQUE_CONSTRAINT_CATALOG", s("def"));
        v.insert(
            "UNIQUE_CONSTRAINT_SCHEMA",
            s(f.ref_db.clone().unwrap_or_else(|| schema.to_owned())),
        );
        v.insert("UNIQUE_CONSTRAINT_NAME", s("PRIMARY"));
        v.insert("MATCH_OPTION", s("NONE"));
        v.insert(
            "UPDATE_RULE",
            s(f.on_update.clone().unwrap_or_else(|| "RESTRICT".into())),
        );
        v.insert(
            "DELETE_RULE",
            s(f.on_delete.clone().unwrap_or_else(|| "RESTRICT".into())),
        );
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("REFERENCED_TABLE_NAME", s(f.ref_table.clone()));
        put("REFERENTIAL_CONSTRAINTS", v);
    }
    // Checks: a column's own check is named after its column, at level
    // `Column`; table checks at level `Table`.
    let mut checks: Vec<(String, &'static str, String)> = def
        .cols
        .iter()
        .filter_map(|c| {
            c.check
                .as_ref()
                .map(|e| (c.name.clone(), "Column", define::expr::print(e)))
        })
        .collect();
    checks.extend(
        def.checks
            .iter()
            .map(|c| (c.name.clone(), "Table", define::expr::print(&c.expr))),
    );
    for (name, level, clause) in checks {
        put("TABLE_CONSTRAINTS", constraint(&name, "CHECK"));
        let mut v = HashMap::new();
        v.insert("CONSTRAINT_CATALOG", s("def"));
        v.insert("CONSTRAINT_SCHEMA", s(schema));
        v.insert("TABLE_NAME", s(def.name.clone()));
        v.insert("CONSTRAINT_NAME", s(name));
        v.insert("LEVEL", s(level));
        v.insert("CHECK_CLAUSE", s(clause));
        put("CHECK_CONSTRAINTS", v);
    }
}

/// A database's row in `SCHEMATA`, under its physical name (the name
/// layer's query reads it so, naming it with its logical name).
fn add_schema(db: &mut MemDb, schema: &str, cs: &str, coll: &str) {
    let mut t = HashMap::new();
    t.insert("CATALOG_NAME", s("def"));
    t.insert("SCHEMA_NAME", s(schema));
    t.insert("DEFAULT_CHARACTER_SET_NAME", s(cs));
    t.insert("DEFAULT_COLLATION_NAME", s(coll));
    t.insert("SQL_PATH", V::N);
    t.insert("SCHEMA_COMMENT", s(""));
    if let Err(e) = db.insert("SCHEMATA", row_of("SCHEMATA", &t)) {
        tracing::warn!(table = "SCHEMATA", error = %e.message, "information_schema: a row doesn't fit its table");
    }
}

/// A sequence's row in `TABLES` (type `SEQUENCE`, one row, as MariaDB
/// lists the table it keeps a sequence in).
fn add_sequence(db: &mut MemDb, schema: &str, name: &str) {
    let mut t = HashMap::new();
    t.insert("TABLE_CATALOG", s("def"));
    t.insert("TABLE_SCHEMA", s(schema));
    t.insert("TABLE_NAME", s(name.to_owned()));
    t.insert("TABLE_TYPE", s("SEQUENCE"));
    t.insert("ENGINE", s("InnoDB"));
    t.insert("TABLE_ROWS", V::U(1));
    if let Err(e) = db.insert("TABLES", row_of("TABLES", &t)) {
        tracing::warn!(table = "TABLES", error = %e.message, "information_schema: a row doesn't fit its table");
    }
    let mut p = HashMap::new();
    p.insert("TABLE_CATALOG", s("def"));
    p.insert("TABLE_SCHEMA", s(schema));
    p.insert("TABLE_NAME", s(name.to_owned()));
    p.insert("TABLE_ROWS", V::U(1));
    p.insert("AVG_ROW_LENGTH", V::U(16384));
    p.insert("DATA_LENGTH", V::U(16384));
    p.insert("INDEX_LENGTH", V::U(0));
    p.insert("DATA_FREE", V::U(0));
    p.insert("PARTITION_COMMENT", s(""));
    p.insert("NODEGROUP", s(""));
    if let Err(e) = db.insert("PARTITIONS", row_of("PARTITIONS", &p)) {
        tracing::warn!(table = "PARTITIONS", error = %e.message, "information_schema: a row doesn't fit its table");
    }
}

/// A view's rows: `TABLES` (type `VIEW`) and `VIEWS`.
fn add_view(db: &mut MemDb, schema: &str, v: &super::view::ViewDef, mysql: bool) {
    let mut put = |t: &str, vals: HashMap<&str, V>| {
        if let Err(e) = db.insert(t, row_of(t, &vals)) {
            tracing::warn!(table = t, error = %e.message, "information_schema: a row doesn't fit its table");
        }
    };
    let mut base = HashMap::new();
    base.insert("TABLE_CATALOG", s("def"));
    base.insert("TABLE_SCHEMA", s(schema));
    base.insert("TABLE_NAME", s(v.name.clone()));
    let mut t = HashMap::new();
    for (k, x) in &base {
        if let V::S(x) = x {
            t.insert(*k, s(x.clone()));
        }
    }
    t.insert("TABLE_TYPE", s("VIEW"));
    t.insert("TABLE_COMMENT", s("VIEW"));
    put("TABLES", t);
    // The select with every table and column under its database's name
    // (the stored select names every column with its table already).
    let cols = |_: &str, _: &str| -> Option<Vec<String>> { None };
    let definition = super::view::Printer::new(mysql, "", &v.body, &cols).query(&v.query, None);
    let mut w = base;
    w.insert("VIEW_DEFINITION", s(definition));
    w.insert(
        "CHECK_OPTION",
        s(match v.check {
            None => "NONE",
            Some(super::view::CheckOption::Cascaded) => "CASCADED",
            Some(super::view::CheckOption::Local) => "LOCAL",
        }),
    );
    w.insert(
        "IS_UPDATABLE",
        s(if super::view::updatable(&v.query, v.algorithm, mysql) {
            "YES"
        } else {
            "NO"
        }),
    );
    w.insert("DEFINER", s(v.definer_text()));
    w.insert(
        "SECURITY_TYPE",
        s(if v.invoker { "INVOKER" } else { "DEFINER" }),
    );
    w.insert("CHARACTER_SET_CLIENT", s("utf8mb4"));
    w.insert(
        "COLLATION_CONNECTION",
        s(if mysql {
            "utf8mb4_0900_ai_ci"
        } else {
            "utf8mb4_uca1400_ai_ci"
        }),
    );
    w.insert("ALGORITHM", s(v.algorithm.word()));
    put("VIEWS", w);
}

/// The type facts `information_schema` shows for a routine's parameter
/// or return type `ty`: (`DATA_TYPE`, char length, octet length,
/// precision, scale, datetime precision, character set, collation,
/// `DTD_IDENTIFIER`).
fn type_facts(ty: &str, db_cs: &(String, String)) -> Option<[V; 9]> {
    let cx = define::Ctx {
        mysql: false,
        db_cs: db_cs.clone(),
    };
    let define::Created::Def(def) =
        define::create(&format!("CREATE TABLE `r` (`v` {ty})"), &cx).ok()?
    else {
        return None;
    };
    let c = def.cols.first()?;
    let (cl, ol) = lengths(c);
    let (p, sc, dp) = numeric(c);
    let (cs, co) = match &c.ty.cs {
        Some((cs, co)) => (s(cs.clone()), s(co.clone())),
        None if matches!(cl, V::U(_))
            && !c.ty.base.contains("binary")
            && !c.ty.base.contains("blob") =>
        {
            (s(db_cs.0.clone()), s(db_cs.1.clone()))
        }
        None => (V::N, V::N),
    };
    Some([
        s(c.ty.base.clone()),
        cl,
        ol,
        p,
        sc,
        dp,
        cs,
        co,
        s(c.ty.print()),
    ])
}

/// A stored procedure or function: its `ROUTINES` row and its
/// `PARAMETERS` rows.
/// The `TRIGGERS` rows of database `dbid`, in table, event, timing and
/// action order.
fn add_triggers(db: &mut MemDb, schema: &str, eng: &super::catalog::EngineCatalog, dbid: DbId) {
    let mut ts: Vec<_> = eng
        .triggers
        .values()
        .filter(|t| t.db == dbid && t.def.is_ok())
        .collect();
    let table = |t: &super::routine::trigger::Trigger| {
        eng.tables
            .get(&t.table)
            .and_then(|p| p.as_ref().as_ref().ok().map(|d| d.name.clone()))
            .unwrap_or_default()
    };
    let rank = |e: super::routine::parse::Event| e as u8;
    ts.sort_by_key(|t| (table(t), t.when().map(|(b, e)| (rank(e), !b)), t.cx.order));
    let mut last: Option<(String, Option<(bool, super::routine::parse::Event)>)> = None;
    let mut order = 0u64;
    for t in ts {
        let Ok(d) = &t.def else { continue };
        let key = (table(t), t.when());
        order = if last.as_ref() == Some(&key) {
            order + 1
        } else {
            1
        };
        last = Some(key.clone());
        let mut v = HashMap::new();
        v.insert("TRIGGER_CATALOG", s("def"));
        v.insert("TRIGGER_SCHEMA", s(schema));
        v.insert("TRIGGER_NAME", s(t.name.clone()));
        v.insert("EVENT_MANIPULATION", s(d.event.word()));
        v.insert("EVENT_OBJECT_CATALOG", s("def"));
        v.insert("EVENT_OBJECT_SCHEMA", s(schema));
        v.insert("EVENT_OBJECT_TABLE", s(key.0));
        v.insert("ACTION_ORDER", V::U(order));
        v.insert("ACTION_CONDITION", V::N);
        v.insert("ACTION_STATEMENT", s(d.body_text.clone()));
        v.insert("ACTION_ORIENTATION", s("ROW"));
        v.insert(
            "ACTION_TIMING",
            s(if d.before { "BEFORE" } else { "AFTER" }),
        );
        v.insert("ACTION_REFERENCE_OLD_TABLE", V::N);
        v.insert("ACTION_REFERENCE_NEW_TABLE", V::N);
        v.insert("ACTION_REFERENCE_OLD_ROW", s("OLD"));
        v.insert("ACTION_REFERENCE_NEW_ROW", s("NEW"));
        v.insert("CREATED", s(t.cx.created.clone()));
        v.insert("SQL_MODE", s(t.cx.sql_mode.clone()));
        v.insert(
            "DEFINER",
            s(format!("{}@{}", t.cx.definer.0, t.cx.definer.1)),
        );
        v.insert("CHARACTER_SET_CLIENT", s(t.cx.charset_client.clone()));
        v.insert("COLLATION_CONNECTION", s(t.cx.collation_connection.clone()));
        v.insert("DATABASE_COLLATION", s(t.cx.db_collation.clone()));
        if let Err(e) = db.insert("TRIGGERS", row_of("TRIGGERS", &v)) {
            tracing::warn!(error = %e.message, "information_schema: a TRIGGERS row doesn't fit its table");
        }
    }
}

/// The `EVENTS` rows of database `dbid`, by name.
fn add_events(db: &mut MemDb, schema: &str, src: &Source<'_>, dbid: DbId, mysql: bool) {
    use super::routine::event;
    let mut es: Vec<_> = src.eng.events.values().filter(|e| e.db == dbid).collect();
    es.sort_by(|a, b| a.name.cmp(&b.name));
    for e in es {
        let Ok(d) = &e.def else { continue };
        let t = |x: &Option<(String, i64)>| x.as_ref().map_or(V::N, |v| s(v.0.clone()));
        let mut v = HashMap::new();
        v.insert("EVENT_CATALOG", s("def"));
        v.insert("EVENT_SCHEMA", s(schema));
        v.insert("EVENT_NAME", s(e.name.clone()));
        v.insert(
            "DEFINER",
            s(format!("{}@{}", e.cx.definer.0, e.cx.definer.1)),
        );
        v.insert("TIME_ZONE", s(d.time_zone.clone()));
        v.insert("EVENT_BODY", s("SQL"));
        v.insert("EVENT_DEFINITION", s(e.body.clone()));
        v.insert(
            "EVENT_TYPE",
            s(if d.at.is_some() {
                "ONE TIME"
            } else {
                "RECURRING"
            }),
        );
        v.insert("EXECUTE_AT", t(&d.at));
        v.insert(
            "INTERVAL_VALUE",
            d.every.as_ref().map_or(V::N, |x| s(x.0.clone())),
        );
        v.insert(
            "INTERVAL_FIELD",
            d.every.as_ref().map_or(V::N, |x| s(x.1.clone())),
        );
        v.insert("SQL_MODE", s(e.cx.sql_mode.clone()));
        v.insert("STARTS", t(&d.starts));
        v.insert("ENDS", t(&d.ends));
        v.insert("STATUS", s(event::status_word(d.status, mysql)));
        v.insert(
            "ON_COMPLETION",
            s(if d.preserve {
                "PRESERVE"
            } else {
                "NOT PRESERVE"
            }),
        );
        v.insert("CREATED", s(e.cx.created.clone()));
        v.insert("LAST_ALTERED", s(e.cx.modified.clone()));
        v.insert(
            "LAST_EXECUTED",
            event::last_run(src.tenant, dbid, &e.name).map_or(V::N, s),
        );
        v.insert("EVENT_COMMENT", s(d.comment.clone()));
        v.insert("ORIGINATOR", V::U(1));
        v.insert("CHARACTER_SET_CLIENT", s(e.cx.charset_client.clone()));
        v.insert("COLLATION_CONNECTION", s(e.cx.collation_connection.clone()));
        v.insert("DATABASE_COLLATION", s(e.cx.db_collation.clone()));
        if let Err(err) = db.insert("EVENTS", row_of("EVENTS", &v)) {
            tracing::warn!(error = %err.message, "information_schema: an EVENTS row doesn't fit its table");
        }
    }
}

#[allow(clippy::too_many_lines)] // one row of each table
fn add_routine(
    db: &mut MemDb,
    schema: &str,
    r: &super::routine::Routine,
    db_cs: &(String, String),
) {
    use super::routine::Kind;
    let mut put = |t: &str, vals: HashMap<&str, V>| {
        if let Err(e) = db.insert(t, row_of(t, &vals)) {
            tracing::warn!(table = t, error = %e.message, "information_schema: a row doesn't fit its table");
        }
    };
    let Ok((c, _)) = &r.def else { return };
    let function = r.kind == Kind::Function;
    let kind = if function { "FUNCTION" } else { "PROCEDURE" };
    let mut v = HashMap::new();
    v.insert("SPECIFIC_NAME", s(r.name.clone()));
    v.insert("ROUTINE_CATALOG", s("def"));
    v.insert("ROUTINE_SCHEMA", s(schema));
    v.insert("ROUTINE_NAME", s(r.name.clone()));
    v.insert("ROUTINE_TYPE", s(kind));
    v.insert("DATA_TYPE", s(""));
    if let Some(ty) = c.returns.as_deref()
        && let Some(f) = type_facts(ty, db_cs)
    {
        let names = [
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "DTD_IDENTIFIER",
        ];
        for (n, x) in names.into_iter().zip(f) {
            v.insert(n, x);
        }
    }
    v.insert("ROUTINE_BODY", s("SQL"));
    v.insert("ROUTINE_DEFINITION", s(c.body_text.clone()));
    v.insert("PARAMETER_STYLE", s("SQL"));
    v.insert(
        "IS_DETERMINISTIC",
        s(if c.chars.deterministic == Some(true) {
            "YES"
        } else {
            "NO"
        }),
    );
    v.insert(
        "SQL_DATA_ACCESS",
        s(c.chars.access.unwrap_or_default().words()),
    );
    v.insert(
        "SECURITY_TYPE",
        s(if c.chars.invoker == Some(true) {
            "INVOKER"
        } else {
            "DEFINER"
        }),
    );
    v.insert("CREATED", s(r.cx.created.clone()));
    v.insert("LAST_ALTERED", s(r.cx.modified.clone()));
    v.insert("SQL_MODE", s(r.cx.sql_mode.clone()));
    v.insert(
        "ROUTINE_COMMENT",
        s(c.chars.comment.clone().unwrap_or_default()),
    );
    v.insert(
        "DEFINER",
        s(format!("{}@{}", r.cx.definer.0, r.cx.definer.1)),
    );
    v.insert("CHARACTER_SET_CLIENT", s(r.cx.charset_client.clone()));
    v.insert("COLLATION_CONNECTION", s(r.cx.collation_connection.clone()));
    v.insert("DATABASE_COLLATION", s(r.cx.db_collation.clone()));
    put("ROUTINES", v);
    let names = [
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "CHARACTER_OCTET_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
        "CHARACTER_SET_NAME",
        "COLLATION_NAME",
        "DTD_IDENTIFIER",
    ];
    let base = |v: &mut HashMap<&str, V>| {
        v.insert("SPECIFIC_CATALOG", s("def"));
        v.insert("SPECIFIC_SCHEMA", s(schema));
        v.insert("SPECIFIC_NAME", s(r.name.clone()));
        v.insert("ROUTINE_TYPE", s(kind));
    };
    if let Some(ty) = c.returns.as_deref() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("ORDINAL_POSITION", V::U(0));
        if let Some(f) = type_facts(ty, db_cs) {
            for (n, x) in names.into_iter().zip(f) {
                v.insert(n, x);
            }
        }
        put("PARAMETERS", v);
    }
    for (i, p) in c.params.iter().enumerate() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
        v.insert("PARAMETER_MODE", s(p.mode.word()));
        v.insert("PARAMETER_NAME", s(p.name.clone()));
        if let Some(f) = type_facts(&p.ty, db_cs) {
            for (n, x) in names.into_iter().zip(f) {
                v.insert(n, x);
            }
        }
        put("PARAMETERS", v);
    }
}

/// The physical schemas of tenant `tenant` that `sql` names, with their
/// databases.
fn schemas_named(sql: &str, tenant: u64) -> Vec<(String, DbId)> {
    let prefix = format!("t{tenant}_d");
    let mut out: Vec<(String, DbId)> = Vec::new();
    let b = sql.as_bytes();
    let mut i = 0;
    while let Some(k) = sql[i..].find(&prefix) {
        let start = i + k;
        let mut j = start;
        while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
            j += 1;
        }
        let name = &sql[start..j];
        if let Ok(sn) = name.parse::<sprig_common::names::SchemaName>()
            && let Some(db) = sn.db()
            && !out.iter().any(|(n, _)| n == name)
        {
            out.push((name.to_owned(), db));
        }
        i = j.max(start + 1);
    }
    out
}

/// The name layer's statement with two of its forms put the planner's way:
/// `CONVERT('x' USING cs) COLLATE c` (a logical name as the view's
/// character set) is the string itself, and the conjuncts that hide keys
/// on hidden columns, `AND (a, b) NOT IN (SELECT ...)`, go (the engine's
/// tables have no hidden columns).
#[allow(clippy::too_many_lines)] // one pass per form the name layer writes
fn simplify(sql: &str) -> String {
    // Functional key parts (the MySQL port's hidden `_sprig_fx_` columns)
    // are engine columns too: their join stays and finds each one's
    // expression. Automatic indexes (`sprig_auto_`) are MariaDB-side.
    let mut owned = sql.replace(" AND LEFT(`INDEX_NAME`, 11) <> 'sprig_auto_'", "");
    // A pair compared with a subquery's pairs (the MySQL port's hidden
    // `json_valid` checks), which the planner reads as one value each.
    owned = owned.replace(
        "(`TABLE_NAME`, `CONSTRAINT_NAME`) IN (SELECT `TABLE_NAME`, `CONSTRAINT_NAME` FROM",
        "CONCAT(`TABLE_NAME`, '`.`', `CONSTRAINT_NAME`) IN (SELECT CONCAT(`TABLE_NAME`, '`.`', `CONSTRAINT_NAME`) FROM",
    );
    // The MySQL port's `COLUMN_TYPE`, `COLUMN_DEFAULT` and `EXTRA` forms
    // are computed with the rows (the `_sprig_my_` columns): no regular
    // expression runs for `SHOW COLUMNS` or the MySQL-shaped `COLUMNS`.
    if owned.contains("REGEXP") {
        for nbe in [false, true] {
            let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, nbe);
            let [ty, def, extra] = sprig_sql::compat::schema::mysql_column_exprs(&lit);
            owned = owned
                .replace(&ty, "`_sprig_my_type`")
                .replace(&def, "`_sprig_my_default`")
                .replace(&extra, "`_sprig_my_extra`");
        }
    }
    // A definer column read through the tenant's account names: the
    // engine's rows already hold the tenant's names.
    let head = "CONVERT(IFNULL((SELECT `account` FROM `__sprig_sys`.`__sprig_names_v` WHERE `name` = SUBSTRING_INDEX(`";
    while let Some(a) = owned.find(head) {
        let rest = &owned[a + head.len()..];
        let Some(col_end) = rest.find('`') else { break };
        let col = rest[..col_end].to_owned();
        let tail = format!("`, '@', 1)), `{col}`) USING utf8mb3)");
        let Some(b) = rest.find(&tail) else { break };
        let end = a + head.len() + b + tail.len();
        owned.replace_range(a..end, &format!("`{col}`"));
    }
    let sql = owned.as_str();
    let mut out = String::with_capacity(sql.len());
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let rest = &sql[i..];
        if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("CONVERT('") {
            // The string literal, then ` USING cs)`, then ` COLLATE c`.
            let mut j = i + 9;
            while j < b.len() {
                if b[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if b[j] == b'\'' {
                    if b.get(j + 1) == Some(&b'\'') {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let lit = &sql[i + 8..=j.min(b.len() - 1)];
            let after = &sql[(j + 1).min(b.len())..];
            if let Some(using) = after.strip_prefix(" USING ")
                && let Some(close) = using.find(')')
            {
                let mut k = j + 1 + 7 + close + 1;
                let tail = &sql[k..];
                if tail.len() > 9 && tail[..9].eq_ignore_ascii_case(" COLLATE ") {
                    let name_len = tail[9..]
                        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .unwrap_or(tail.len() - 9);
                    k += 9 + name_len;
                }
                out.push_str(lit);
                i = k;
                continue;
            }
        }
        if rest.starts_with(" AND (`")
            && let Some(p) = rest.find(") NOT IN (SELECT ")
        {
            let head = &rest[6..p];
            if head.starts_with('`') && !head.contains('(') {
                // Skip to the parenthesis that closes the subquery.
                let start = i + p + ") NOT IN (".len();
                let mut depth = 1;
                let mut k = start;
                let mut in_str = false;
                while k < b.len() && depth > 0 {
                    match b[k] {
                        b'\'' => in_str = !in_str,
                        b'(' if !in_str => depth += 1,
                        b')' if !in_str => depth -= 1,
                        _ => {}
                    }
                    k += 1;
                }
                i = k;
                continue;
            }
        }
        let ch = rest.chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Whether `sql` reads one of the tables built here.
pub(crate) fn reads_tables(sql: &str) -> bool {
    let l = sql.to_ascii_lowercase();
    l.contains("information_schema")
        && TABLES.iter().any(|t| {
            let t = t.to_ascii_lowercase();
            l.contains(&format!("information_schema.{t}"))
                || l.contains(&format!("information_schema`.`{t}`"))
        })
}

/// The name of a column definition payload made by [`coldef`].
fn coldef_name(d: &[u8]) -> Option<String> {
    let mut i = 0;
    let mut field = || -> Option<&[u8]> {
        let n = usize::from(*d.get(i)?);
        if n >= 0xfb {
            return None;
        }
        let v = d.get(i + 1..i + 1 + n)?;
        i += 1 + n;
        Some(v)
    };
    for _ in 0..4 {
        field()?;
    }
    field().map(|n| String::from_utf8_lossy(n).into_owned())
}

/// The rows of a `SHOW` answer that `filter` (`WHERE ...` over the
/// answer's column names, as the server takes it) keeps, run on the
/// planner over the answer as a table of strings. `None`: the planner
/// can't run the condition (the backend's answer stands, as before).
pub(crate) fn filter_answer(answer: Answer, filter: &str) -> Option<Answer> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let names: Vec<String> = answer
        .defs
        .iter()
        .map(|d| coldef_name(d))
        .collect::<Option<_>>()?;
    let ci = collation_named("utf8mb4_general_ci")?;
    let table = "_sprig_show";
    let columns = names
        .iter()
        .map(|n| {
            ColumnDef::new(
                n,
                ColType::VarChar {
                    chars: 16_383,
                    collation: ci,
                },
            )
        })
        .collect();
    let mut db = MemDb::new("information_schema");
    db.create_table(TableDef {
        schema: "information_schema".into(),
        name: table.into(),
        columns,
        primary: None,
        indexes: Vec::new(),
    });
    for row in &answer.rows {
        let vals = sprig_proto_mysql::resultset::decode_text_row(row, names.len()).ok()?;
        let datums = vals
            .iter()
            .map(|v| match v {
                Some(b) => Datum::Str(Bytes::copy_from_slice(b)),
                None => Datum::Null,
            })
            .collect();
        db.insert(table, datums).ok()?;
    }
    let sql = format!("SELECT * FROM information_schema.`{table}` {filter}");
    let stmts = Parser::parse_sql(&MySqlDialect {}, &sql).ok()?;
    let [stmt] = stmts.as_slice() else {
        return None;
    };
    let session = sprig_exec::Session::new(sprig_exec::Persona::MariaDb);
    let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new(db.clone());
    let planner = sprig_plan::Planner::new(&db, storage, &session);
    let mut plan = planner.plan(stmt, &[]).ok()?;
    let mut ctx = sprig_exec::Ctx::with_params(&session, &[]);
    let mut rows = Vec::new();
    while let Some(r) = plan.source.next(&mut ctx).ok()? {
        let vals: Vec<Option<Bytes>> = r
            .iter()
            .map(|v| match v {
                Datum::Str(b) | Datum::Bytes(b) => Some(b.clone()),
                _ => None,
            })
            .collect();
        rows.push(sprig_proto_mysql::resultset::encode_text_row(
            vals.iter().map(|v| v.as_deref()),
        ));
    }
    Some(Answer {
        defs: answer.defs,
        rows,
        no_index: answer.no_index,
    })
}

/// What the engine needs to answer: the tables' definitions per database
/// and their counters.
pub(crate) struct Source<'a> {
    pub eng: &'a EngineCatalog,
    pub tenant: u64,
    /// The next `AUTO_INCREMENT` value per table object.
    pub counters: &'a HashMap<u64, u64>,
    /// Each table's row count (`TABLE_ROWS`), when the query asks for it.
    pub rows: &'a HashMap<u64, u64>,
    /// A partitioned table's rows per partition (`PARTITIONS.TABLE_ROWS`),
    /// when the query asks for them.
    pub part_rows: &'a HashMap<u64, Vec<u64>>,
    /// The tenant's logic functions and procedures (engine-tier code).
    pub logic: &'a [LogicRoutine],
    /// `@sprig_show_auto_indexes = 1`: automatic indexes (`sprig_auto_*`)
    /// are listed in `STATISTICS` and count in `COLUMN_KEY`; otherwise
    /// they are left out, as schema tools expect.
    pub show_auto_indexes: bool,
}

/// A function or procedure of a logic module (`CREATE FUNCTION ...
/// LANGUAGE kirun | javascript | lua`): engine-tier code, listed in
/// `ROUTINES` and `PARAMETERS` beside the SQL routines, with
/// `ROUTINE_BODY` `EXTERNAL` and its language in `EXTERNAL_LANGUAGE`.
#[derive(Debug, Clone)]
pub(crate) struct LogicRoutine {
    /// The database.
    pub db: DbId,
    /// The name.
    pub name: String,
    /// A function (else a procedure).
    pub function: bool,
    /// `kirun`, `javascript`, `lua`.
    pub runtime: String,
    /// `SQL SECURITY INVOKER`.
    pub invoker: bool,
    /// Declared `DETERMINISTIC`.
    pub deterministic: bool,
    /// The parameters: name and SQL type.
    pub params: Vec<(String, String)>,
    /// A function's SQL return type, when it declares one.
    pub returns: Option<String>,
    /// `user@host`.
    pub definer: String,
    /// When it was created (`YYYY-MM-DD hh:mm:ss`, UTC).
    pub created: String,
}

/// The SQL type a logic parameter of type `ty` (the runtime's name) is
/// declared with, when the module records none.
pub(crate) fn logic_sql_type(ty: &str) -> &'static str {
    match ty {
        "i8" | "i16" | "i32" | "i64" | "int" | "integer" | "long" => "bigint(20)",
        "u8" | "u16" | "u32" | "u64" => "bigint(20) unsigned",
        "f32" | "f64" | "float" | "double" | "number" => "double",
        "bool" | "boolean" => "tinyint(1)",
        _ => "longtext",
    }
}

/// A logic routine's `ROUTINES` and `PARAMETERS` rows.
fn add_logic_routine(db: &mut MemDb, schema: &str, r: &LogicRoutine, db_cs: &(String, String)) {
    let mut put = |t: &str, vals: HashMap<&str, V>| {
        if let Err(e) = db.insert(t, row_of(t, &vals)) {
            tracing::warn!(table = t, error = %e.message, "information_schema: a row doesn't fit its table");
        }
    };
    let kind = if r.function { "FUNCTION" } else { "PROCEDURE" };
    let names = [
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "CHARACTER_OCTET_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
        "CHARACTER_SET_NAME",
        "COLLATION_NAME",
        "DTD_IDENTIFIER",
    ];
    let mut v = HashMap::new();
    v.insert("SPECIFIC_NAME", s(r.name.clone()));
    v.insert("ROUTINE_CATALOG", s("def"));
    v.insert("ROUTINE_SCHEMA", s(schema));
    v.insert("ROUTINE_NAME", s(r.name.clone()));
    v.insert("ROUTINE_TYPE", s(kind));
    v.insert("DATA_TYPE", s(""));
    if let Some(ty) = r.returns.as_deref()
        && let Some(f) = type_facts(ty, db_cs)
    {
        for (n, x) in names.into_iter().zip(f) {
            v.insert(n, x);
        }
    }
    v.insert("ROUTINE_BODY", s("EXTERNAL"));
    v.insert("EXTERNAL_NAME", s(r.name.clone()));
    v.insert("EXTERNAL_LANGUAGE", s(r.runtime.to_ascii_uppercase()));
    v.insert("PARAMETER_STYLE", s("SQL"));
    v.insert(
        "IS_DETERMINISTIC",
        s(if r.deterministic { "YES" } else { "NO" }),
    );
    v.insert("SQL_DATA_ACCESS", s("CONTAINS SQL"));
    v.insert(
        "SECURITY_TYPE",
        s(if r.invoker { "INVOKER" } else { "DEFINER" }),
    );
    v.insert("CREATED", s(r.created.clone()));
    v.insert("LAST_ALTERED", s(r.created.clone()));
    v.insert("SQL_MODE", s(""));
    v.insert("ROUTINE_COMMENT", s(""));
    v.insert("DEFINER", s(r.definer.clone()));
    v.insert("CHARACTER_SET_CLIENT", s(db_cs.0.clone()));
    v.insert("COLLATION_CONNECTION", s(db_cs.1.clone()));
    v.insert("DATABASE_COLLATION", s(db_cs.1.clone()));
    put("ROUTINES", v);
    let base = |v: &mut HashMap<&str, V>| {
        v.insert("SPECIFIC_CATALOG", s("def"));
        v.insert("SPECIFIC_SCHEMA", s(schema));
        v.insert("SPECIFIC_NAME", s(r.name.clone()));
        v.insert("ROUTINE_TYPE", s(kind));
    };
    if let Some(ty) = r.returns.as_deref() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("ORDINAL_POSITION", V::U(0));
        if let Some(f) = type_facts(ty, db_cs) {
            for (n, x) in names.into_iter().zip(f) {
                v.insert(n, x);
            }
        }
        put("PARAMETERS", v);
    }
    for (i, (name, ty)) in r.params.iter().enumerate() {
        let mut v = HashMap::new();
        base(&mut v);
        v.insert("ORDINAL_POSITION", V::U(i as u64 + 1));
        v.insert("PARAMETER_MODE", s("IN"));
        v.insert("PARAMETER_NAME", s(name.clone()));
        if let Some(f) = type_facts(ty, db_cs) {
            for (n, x) in names.into_iter().zip(f) {
                v.insert(n, x);
            }
        }
        put("PARAMETERS", v);
    }
}

/// The answer to `sql`, or `None` when the engine leaves it to the
/// backend.
/// What the result metadata of a query over these tables depends on,
/// from its outer `SELECT`.
#[derive(Debug, Default, PartialEq, Eq)]
struct OuterShape {
    /// MySQL 8.4 reads it through a temporary table: `DISTINCT`,
    /// `GROUP BY`, or an `ORDER BY` on a column the `WHERE` doesn't fix
    /// to one value.
    temp: bool,
    /// It has a `GROUP BY`.
    grouped: bool,
    /// Result names given by an alias, with the column each reads.
    aliases: HashMap<String, String>,
    /// Columns the select list reads as themselves (upper case).
    plain: HashSet<String>,
}

fn outer_shape(stmt: &sqlparser::ast::Statement) -> OuterShape {
    use sqlparser::ast::{
        BinaryOperator, Distinct, Expr, GroupByExpr, OrderByKind, SelectItem, SetExpr, Statement,
        Value,
    };
    let mut out = OuterShape::default();
    let Statement::Query(q) = stmt else {
        return out;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return out;
    };
    let column_of = |e: &Expr| match e {
        Expr::Identifier(i) => Some(i.value.to_ascii_uppercase()),
        Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_ascii_uppercase()),
        _ => None,
    };
    for item in &sel.projection {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(col) = column_of(expr) {
                    out.aliases.insert(alias.value.clone(), col);
                }
            }
            SelectItem::UnnamedExpr(e) => {
                if let Some(col) = column_of(e) {
                    out.plain.insert(col);
                }
            }
            // `*` names every column as itself.
            _ => {}
        }
    }
    // Columns the WHERE fixes: `col = constant` conjuncts.
    let mut fixed = Vec::new();
    let mut stack: Vec<&Expr> = sel.selection.iter().collect();
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                stack.push(left.as_ref());
                stack.push(right.as_ref());
            }
            Expr::Nested(x) => stack.push(x.as_ref()),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                for (a, b) in [(left, right), (right, left)] {
                    if let (Some(c), Expr::Value(_)) = (column_of(a), b.as_ref()) {
                        fixed.push(c);
                    }
                }
            }
            _ => {}
        }
    }
    let grouped = matches!(&sel.group_by, GroupByExpr::Expressions(l, _) if !l.is_empty())
        || matches!(&sel.group_by, GroupByExpr::All(_));
    out.grouped = grouped;
    let distinct = matches!(sel.distinct, Some(Distinct::Distinct));
    let ordered = q.order_by.as_ref().is_some_and(|ob| match &ob.kind {
        OrderByKind::Expressions(list) => list.iter().any(|o| match &o.expr {
            Expr::Value(v) if matches!(v.value, Value::Null) => false,
            x => column_of(x).is_none_or(|c| !fixed.contains(&c)),
        }),
        OrderByKind::All(_) => true,
    });
    out.temp = grouped || distinct || ordered;
    out
}

pub(crate) fn answer(sql: &str, src: &Source<'_>, mysql: bool) -> Option<Answer> {
    match try_answer(sql, src, mysql) {
        Ok(a) => Some(a),
        Err(why) => {
            tracing::info!(why = %why, sql, "information_schema: left to the backend");
            None
        }
    }
}

/// [`answer`] for a tenant with no MariaDB, which has no backend to leave
/// the query to: a query the planner refuses with one of the servers'
/// errors (an unknown column, say) gets that error, worded as the port's
/// server words it. `None` as for [`answer`].
pub(crate) fn answer_or_error(
    sql: &str,
    src: &Source<'_>,
    mysql: bool,
) -> Option<sprig_common::Result<Answer>> {
    match try_answer(sql, src, mysql) {
        Ok(a) => Some(Ok(a)),
        Err(Unanswered {
            error: Some(mut e), ..
        }) => {
            if mysql && e.code == 1054 {
                e.message = mysql_clause_words(&e.message);
            }
            Some(Err(super::general::sql_error(e)))
        }
        Err(why) => {
            tracing::info!(why = %why, sql, "information_schema: not answered");
            None
        }
    }
}

/// MariaDB's clause names in a 1054 message as MySQL writes them (the
/// name layer's statement is planned under MariaDB's rules on both ports).
fn mysql_clause_words(message: &str) -> String {
    const WORDS: [(&str, &str); 6] = [
        ("in 'SELECT'", "in 'field list'"),
        ("in 'WHERE'", "in 'where clause'"),
        ("in 'ON'", "in 'on clause'"),
        ("in 'GROUP BY'", "in 'group statement'"),
        ("in 'HAVING'", "in 'having clause'"),
        ("in 'ORDER BY'", "in 'order clause'"),
    ];
    for (maria, my) in WORDS {
        if let Some(head) = message.strip_suffix(maria) {
            return format!("{head}{my}");
        }
    }
    message.to_owned()
}

/// Why the engine didn't answer a query: a note for the log, and the
/// planner's or executor's error when it gave one.
#[derive(Debug)]
struct Unanswered {
    why: String,
    error: Option<sprig_exec::SqlError>,
}

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.why)
    }
}

impl From<String> for Unanswered {
    fn from(why: String) -> Self {
        Self { why, error: None }
    }
}

impl From<&str> for Unanswered {
    fn from(why: &str) -> Self {
        Self {
            why: why.to_owned(),
            error: None,
        }
    }
}

#[allow(clippy::too_many_lines)] // build, plan, run, describe: one pass
fn try_answer(sql: &str, src: &Source<'_>, mysql: bool) -> Result<Answer, Unanswered> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let simplified = simplify(sql);
    let sql = simplified.as_str();
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    let [stmt] = stmts.as_slice() else {
        return Err("not one statement".into());
    };
    let mut db = MemDb::new("information_schema");
    for t in TABLES {
        db.create_table(plan_def(t));
    }
    for (schema, dbid) in schemas_named(sql, src.tenant) {
        if !src.eng.dbs.contains(&dbid) {
            continue;
        }
        let mut objs: Vec<(&String, &sprig_common::ids::ObjectId)> = src
            .eng
            .by_name
            .iter()
            .filter(|((d, _), _)| *d == dbid)
            .map(|((_, n), o)| (n, o))
            .collect();
        objs.sort();
        for (name, o) in objs {
            if name.starts_with("__sprig_") {
                continue;
            }
            let Some(text) = src.eng.texts.get(o) else {
                continue;
            };
            let Ok(def) = define::parse_stored(text) else {
                continue;
            };
            let def = super::show::visible(def, src.show_auto_indexes);
            let created = src.eng.created.get(o).map(|t| {
                let secs = t
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                utc_datetime(secs)
            });
            add_table(
                &mut db,
                &schema,
                &def,
                src.counters.get(&o.0).copied(),
                created,
                src.rows.get(&o.0).copied(),
                src.part_rows.get(&o.0),
            );
        }
        let mut views: Vec<_> = src
            .eng
            .views
            .iter()
            .filter(|((d, _), _)| *d == dbid)
            .filter_map(|(_, v)| v.as_ref().as_ref().ok())
            .collect();
        views.sort_by(|a, b| a.name.cmp(&b.name));
        for v in views {
            add_view(&mut db, &schema, v, mysql);
        }
        let (cs, coll) = src
            .eng
            .charsets
            .get(&dbid)
            .filter(|(cs, co)| !cs.is_empty() && !co.is_empty())
            .map_or_else(
                || {
                    (
                        "utf8mb4".to_owned(),
                        define::default_collation("utf8mb4").to_owned(),
                    )
                },
                Clone::clone,
            );
        add_schema(&mut db, &schema, &cs, &coll);
        let mut routines: Vec<_> = src
            .eng
            .routines
            .iter()
            .filter(|((d, _, _), _)| *d == dbid)
            .map(|(_, r)| r)
            .collect();
        routines.sort_by(|a, b| a.name.cmp(&b.name));
        for r in routines {
            add_routine(&mut db, &schema, r, &(cs.clone(), coll.clone()));
        }
        for r in src.logic.iter().filter(|r| r.db == dbid) {
            // A SQL routine of the same name is the one the database runs.
            if src.eng.routines.keys().any(|(d, k, n)| {
                *d == dbid
                    && n.eq_ignore_ascii_case(&r.name)
                    && (*k == super::routine::Kind::Function) == r.function
            }) {
                continue;
            }
            add_logic_routine(&mut db, &schema, r, &(cs.clone(), coll.clone()));
        }
        add_triggers(&mut db, &schema, src.eng, dbid);
        add_events(&mut db, &schema, src, dbid, mysql);
        let mut seqs: Vec<&String> = src
            .eng
            .seq_names
            .keys()
            .filter(|(d, _)| *d == dbid)
            .map(|(_, n)| n)
            .collect();
        seqs.sort();
        for n in seqs {
            add_sequence(&mut db, &schema, n);
        }
    }
    // The name layer wrote the statement for MariaDB, whichever port the
    // client is on: it runs under MariaDB's rules.
    let session = sprig_exec::Session::new(sprig_exec::Persona::MariaDb);
    let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new(db.clone());
    let planner = sprig_plan::Planner::new(&db, storage, &session);
    let mut plan = planner.plan(stmt, &[]).map_err(|e| Unanswered {
        why: format!("plan: {}", e.message),
        error: Some(e),
    })?;
    let mut ctx = sprig_exec::Ctx::with_params(&session, &[]);
    let mut rows = Vec::new();
    while let Some(r) = plan.source.next(&mut ctx).map_err(|e| Unanswered {
        why: format!("run: {}", e.message),
        error: Some(e),
    })? {
        rows.push(r);
    }
    // Every result column must be one of the tables' own columns.
    let l = sql.to_ascii_lowercase();
    // The table the outer query reads: the name layer names its derived
    // table after it.
    let table = TABLES
        .iter()
        .find(|t| l.contains(&format!(") as `{}`", t.to_ascii_lowercase())))
        .or_else(|| {
            TABLES
                .iter()
                .find(|t| l.contains(&format!("information_schema.{}", t.to_ascii_lowercase())))
        })
        .ok_or("no table")?;
    // The table as the query writes it: the servers name it so.
    let written = [
        format!(") as `{}`", table.to_ascii_lowercase()),
        format!("information_schema.{}", table.to_ascii_lowercase()),
    ]
    .iter()
    .find_map(|pat| {
        let at = l.find(pat.as_str())?;
        let from = at + pat.len() - table.len() - usize::from(pat.starts_with(')'));
        sql.get(from..from + table.len()).map(str::to_owned)
    })
    .unwrap_or_else(|| (*table).to_owned());
    let shape = outer_shape(stmt);
    let mut defs = Vec::new();
    for c in &plan.columns {
        // A column under an alias is described as the column it reads.
        let column = shape
            .aliases
            .get(&c.name)
            .map_or(c.name.as_str(), String::as_str);
        // The MySQL port: MySQL 8.4's view columns (`infoschema_mysql`).
        if mysql {
            // A column read as itself is named as the view names it.
            let alias =
                (!shape.plain.contains(&c.name.to_ascii_uppercase())).then_some(c.name.as_str());
            if let Some(d) =
                super::infoschema_mysql::coldef(table, &written, column, alias, shape.temp)
            {
                defs.push(d);
                continue;
            }
        }
        match cols_of(table).find(|x| x.1.eq_ignore_ascii_case(column)) {
            Some(&(_, org, cs, len, ty, flags)) => {
                defs.push(coldef_in(
                    "information_schema",
                    &written,
                    table,
                    &c.name,
                    org,
                    cs,
                    len,
                    ty,
                    flags,
                    0,
                ));
            }
            // A number the query computes (`COUNT(*)`, an `ORDINAL_POSITION`
            // sum): described by its type, as the servers do.
            None if c.ty.kind != sprig_exec::types::Kind::Str
                && c.ty.kind != sprig_exec::types::Kind::Null =>
            {
                let results = crate::engine::meta::Results { cs: None, mysql };
                let d = crate::engine::general::results::value_def(&c.name, &c.ty, results);
                defs.push(coldef(
                    "",
                    "",
                    &c.name,
                    "",
                    d.charset,
                    d.length,
                    d.col_type,
                    // A grouped count is read back from the grouping's
                    // temporary table, which drops `BINARY`.
                    if shape.grouped {
                        d.flags & !0x0080
                    } else {
                        d.flags
                    },
                    d.decimals,
                ));
            }
            // A column the name layer computes (the MySQL port's
            // `IS_VISIBLE`, `EXPRESSION`): a string of the derived table.
            None => defs.push(coldef("", table, &c.name, "", 224, 256, 0xfd, 0x0000, 0)),
        }
    }
    let mut out = Vec::new();
    for r in rows {
        let vals: Vec<Option<Bytes>> = r
            .iter()
            .map(|v| match v {
                Datum::Null => None,
                // A time (`CREATED`, an event's `STARTS`): its text.
                Datum::DateTime(d) => {
                    let fsp = if d.micros == 0 { 0 } else { 6 };
                    Some(Bytes::from(super::temporal::datetime_text(*d, fsp)))
                }
                Datum::Str(b) | Datum::Bytes(b) => Some(b.clone()),
                Datum::UInt(n) => Some(Bytes::from(n.to_string())),
                Datum::Int(n) => Some(Bytes::from(n.to_string())),
                // A number merged from signed and unsigned branches (an
                // inherited table's `CASE ... END AS ORDINAL_POSITION`).
                Datum::Decimal(d) => Some(Bytes::from(d.to_string())),
                other => Some(Bytes::from(format!("{other:?}"))),
            })
            .collect();
        out.push(sprig_proto_mysql::resultset::encode_text_row(
            vals.iter().map(|v| v.as_deref()),
        ));
    }
    Ok(Answer {
        defs,
        rows: out,
        no_index: true,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use sprig_common::ids::{DbId, ObjectId};

    use super::{Source, answer};
    use crate::engine::catalog::EngineCatalog;

    #[test]
    fn a_rewritten_query_runs_over_the_tables() {
        let text = "CREATE TABLE `k2` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `ua` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(3));
        eng.by_name.insert((DbId(3), "k2".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        let counters = HashMap::new();
        let src = Source {
            eng: &eng,
            tenant: 7,
            counters: &counters,
            rows: &counters,
            part_rows: &HashMap::new(),
            logic: &[],
            show_auto_indexes: false,
        };
        let sql = "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM (SELECT `CONSTRAINT_CATALOG`, 'cc_ddl' AS `CONSTRAINT_SCHEMA`, `CONSTRAINT_NAME`, 'cc_ddl' AS `TABLE_SCHEMA`, `TABLE_NAME`, `CONSTRAINT_TYPE` FROM information_schema.TABLE_CONSTRAINTS WHERE `TABLE_SCHEMA` = 't7_d3_g1_s1' AND `TABLE_NAME` = 'k2' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_') AS `TABLE_CONSTRAINTS` WHERE table_schema = 'cc_ddl' AND table_name = 'k2' ORDER BY CONSTRAINT_NAME";
        let a = answer(sql, &src, false).expect("answered");
        assert_eq!(a.rows.len(), 2);
    }

    #[test]
    fn ordinal_positions_are_numbers() {
        let text = "CREATE TABLE `k2` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(3));
        eng.by_name.insert((DbId(3), "k2".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        eng.dbs.insert(DbId(4));
        eng.by_name.insert((DbId(4), "k2".into()), ObjectId(21));
        eng.texts.insert(ObjectId(21), Arc::from(text));
        let counters = HashMap::new();
        let src = Source {
            eng: &eng,
            tenant: 7,
            counters: &counters,
            rows: &counters,
            part_rows: &HashMap::new(),
            logic: &[],
            show_auto_indexes: false,
        };
        let branch = |s: &str, d: &str| {
            format!(
                "SELECT `TABLE_CATALOG`, '{s}' AS `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `ORDINAL_POSITION` FROM information_schema.COLUMNS WHERE `TABLE_SCHEMA` = '{d}' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_' AND NOT (LEFT(`COLUMN_NAME`, 7) = '_sprig_' OR `COLUMN_NAME` = 'my_row_id')"
            )
        };
        let sql = format!(
            "SELECT COLUMN_NAME, ORDINAL_POSITION FROM (SELECT * FROM ({} UNION ALL {}) AS `COLUMNS`) AS `COLUMNS` WHERE table_schema = 'cc_ddl' AND table_name = 'k2'",
            branch("cc_ddl", "t7_d3_g1_s1"),
            branch("other", "t7_d4_g1_s1")
        );
        let sql = sql.as_str();
        let a = answer(sql, &src, false).expect("answered");
        let rows: Vec<String> = a
            .rows
            .iter()
            .map(|r| String::from_utf8_lossy(r).into_owned())
            .collect();
        assert!(rows[0].ends_with("\u{1}1"), "{rows:?}");
    }

    #[test]
    fn the_statistics_view_of_both_ports_runs() {
        let text = "CREATE TABLE `v1` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(37));
        eng.by_name.insert((DbId(37), "v1".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        let counters = HashMap::new();
        let src = Source {
            eng: &eng,
            tenant: 61_306_000_000,
            counters: &counters,
            rows: &counters,
            part_rows: &HashMap::new(),
            logic: &[],
            show_auto_indexes: false,
        };
        if let Ok(sql) = std::fs::read_to_string("../../target/agent-c-catalog/obs/stat.sql") {
            let a = super::try_answer(&sql, &src, true).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(a.rows.len(), 2);
        }
        let maria = "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE FROM (SELECT `TABLE_CATALOG`, CONVERT('cc_ddl' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_NAME`, `SEQ_IN_INDEX`, `COLUMN_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't61306000000_d37_g37_s37' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_' AND (`TABLE_NAME`, `INDEX_NAME`) NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't61306000000_d37_g37_s37' AND LEFT(`COLUMN_NAME`, 7) = '_sprig_') AND `TABLE_NAME` = 'v1') AS `STATISTICS` WHERE table_schema = 'cc_ddl' AND table_name = 'v1' ORDER BY INDEX_NAME, SEQ_IN_INDEX";
        let a = super::try_answer(maria, &src, false).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(a.rows.len(), 2);
    }

    #[test]
    fn automatic_indexes_are_listed_only_when_the_session_shows_them() {
        let text = "CREATE TABLE `v1` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`),\n  KEY `sprig_auto_00c0ffee` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let mut eng = EngineCatalog::default();
        eng.dbs.insert(DbId(37));
        eng.by_name.insert((DbId(37), "v1".into()), ObjectId(20));
        eng.texts.insert(ObjectId(20), Arc::from(text));
        let counters = HashMap::new();
        let stats = "SELECT INDEX_NAME FROM (SELECT `TABLE_CATALOG`, 'cc_ddl' AS `TABLE_SCHEMA`, `TABLE_NAME`, `INDEX_NAME` FROM information_schema.STATISTICS WHERE `TABLE_SCHEMA` = 't7_d37_g37_s37' AND LEFT(`TABLE_NAME`, 8) <> '__sprig_') AS `STATISTICS` WHERE table_schema = 'cc_ddl' AND table_name = 'v1' ORDER BY INDEX_NAME";
        let keys = "SELECT COLUMN_KEY FROM (SELECT `TABLE_CATALOG`, 'cc_ddl' AS `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`, `COLUMN_KEY` FROM information_schema.COLUMNS WHERE `TABLE_SCHEMA` = 't7_d37_g37_s37') AS `COLUMNS` WHERE table_schema = 'cc_ddl' AND table_name = 'v1' AND column_name = 'b'";
        for (shown, names, key) in [
            (false, vec!["PRIMARY", "ka"], ""),
            (true, vec!["PRIMARY", "ka", "sprig_auto_00c0ffee"], "MUL"),
        ] {
            let src = Source {
                eng: &eng,
                tenant: 7,
                counters: &counters,
                rows: &counters,
                part_rows: &HashMap::new(),
                logic: &[],
                show_auto_indexes: shown,
            };
            let a = super::try_answer(stats, &src, false).unwrap_or_else(|e| panic!("{e}"));
            let mut got: Vec<String> = a
                .rows
                .iter()
                .map(|r| {
                    let v = sprig_proto_mysql::resultset::decode_text_row(r, 1).expect("row");
                    String::from_utf8_lossy(v[0].unwrap_or_default()).into_owned()
                })
                .collect();
            got.sort();
            assert_eq!(got, names, "shown = {shown}");
            let a = super::try_answer(keys, &src, false).unwrap_or_else(|e| panic!("{e}"));
            let v = sprig_proto_mysql::resultset::decode_text_row(&a.rows[0], 1).expect("row");
            assert_eq!(v[0].unwrap_or_default(), key.as_bytes(), "shown = {shown}");
        }
    }

    #[test]
    fn the_name_layers_forms_are_simplified() {
        assert_eq!(
            super::simplify(
                "SELECT CONVERT('cc_ddl' USING utf8mb3) COLLATE utf8mb3_general_ci AS `TABLE_SCHEMA` FROM x WHERE a = 1 AND (`TABLE_NAME`, `INDEX_NAME`) NOT IN (SELECT `TABLE_NAME`, `INDEX_NAME` FROM y WHERE c IN ('a)', 'b')) ORDER BY 1"
            ),
            "SELECT 'cc_ddl' AS `TABLE_SCHEMA` FROM x WHERE a = 1 ORDER BY 1"
        );
    }

    /// The MySQL port's `SHOW COLUMNS` query runs without a regular
    /// expression: the name layer's three expressions become the computed
    /// columns, for either escape mode.
    #[test]
    fn show_columns_needs_no_regular_expression() {
        for nbe in [false, true] {
            let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, nbe);
            for full in [false, true] {
                let sql = sprig_sql::compat::schema::show_columns_sql(
                    "t7_d3_g1_s1",
                    "k2",
                    full,
                    None,
                    None,
                    &lit,
                );
                let out = super::simplify(std::str::from_utf8(&sql).unwrap());
                assert!(!out.contains("REGEXP"), "{out}");
                assert!(out.contains("`_sprig_my_type`"), "{out}");
            }
        }
    }

    #[test]
    fn mysql_forms_of_three_columns() {
        use super::{mysql_default, mysql_extra, mysql_type};
        assert_eq!(mysql_type("int(11) unsigned"), "int unsigned");
        assert_eq!(mysql_type("tinyint(1)"), "tinyint(1)");
        assert_eq!(mysql_type("tinyint(4)"), "tinyint");
        assert_eq!(mysql_type("year(4)"), "year");
        assert_eq!(
            mysql_type("smallint(5) unsigned zerofill"),
            "smallint(5) unsigned zerofill"
        );
        assert_eq!(mysql_type("decimal(10,2)"), "decimal(10,2)");
        assert_eq!(mysql_type("varchar(20)"), "varchar(20)");
        assert_eq!(mysql_default("NULL"), None);
        assert_eq!(mysql_default("'it''s\\\\x'").as_deref(), Some("it's\\x"));
        assert_eq!(mysql_default("'a\\nb'").as_deref(), Some("a\nb"));
        assert_eq!(
            mysql_default("current_timestamp()").as_deref(),
            Some("CURRENT_TIMESTAMP")
        );
        assert_eq!(
            mysql_default("current_timestamp(3)").as_deref(),
            Some("CURRENT_TIMESTAMP(3)")
        );
        assert_eq!(
            mysql_default("localtimestamp(2)").as_deref(),
            Some("now(2)")
        );
        assert_eq!(mysql_default("-5").as_deref(), Some("-5"));
        assert_eq!(mysql_extra(Some("-5"), ""), "");
        assert_eq!(mysql_extra(Some("1.5e3"), ""), "");
        assert_eq!(mysql_extra(Some("b'101'"), ""), "");
        assert_eq!(mysql_extra(Some("'x'"), ""), "");
        assert_eq!(mysql_extra(None, "auto_increment"), "auto_increment");
        assert_eq!(
            mysql_extra(
                Some("current_timestamp(3)"),
                "on update current_timestamp(3)"
            ),
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)"
        );
        assert_eq!(
            mysql_extra(Some("current_timestamp()"), "on update current_timestamp()"),
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
        );
        assert_eq!(mysql_extra(Some("(`a` + 1)"), ""), "DEFAULT_GENERATED");
    }

    /// `SHOW ... WHERE`: the condition runs over the answer's columns.
    #[test]
    fn a_show_answer_is_filtered_by_its_where() {
        use super::super::show::{Answer, coldef};
        use sprig_proto_mysql::resultset::{decode_text_row, encode_text_row};
        let answer = Answer {
            defs: vec![
                coldef(
                    "information_schema",
                    "COLUMNS",
                    "Field",
                    "COLUMN_NAME",
                    224,
                    256,
                    0xfd,
                    1,
                    0,
                ),
                coldef(
                    "information_schema",
                    "COLUMNS",
                    "Key",
                    "COLUMN_KEY",
                    224,
                    12,
                    0xfd,
                    1,
                    0,
                ),
                coldef(
                    "information_schema",
                    "COLUMNS",
                    "Default",
                    "COLUMN_DEFAULT",
                    224,
                    12,
                    0xfd,
                    0,
                    0,
                ),
            ],
            rows: vec![
                encode_text_row([Some(&b"id"[..]), Some(&b"PRI"[..]), None]),
                encode_text_row([Some(&b"a"[..]), Some(&b""[..]), Some(&b"7"[..])]),
                encode_text_row([Some(&b"b"[..]), Some(&b"MUL"[..]), None]),
            ],
            no_index: true,
        };
        let out = super::filter_answer(answer, "WHERE `Key` <> '' AND `default` IS NULL").unwrap();
        let names: Vec<String> = out
            .rows
            .iter()
            .map(|r| {
                String::from_utf8_lossy(decode_text_row(r, 3).unwrap()[0].unwrap()).into_owned()
            })
            .collect();
        assert_eq!(names, ["id", "b"]);
        assert_eq!(out.defs.len(), 3);
    }

    #[test]
    fn defaults_are_quoted_as_mariadb_quotes_them() {
        assert_eq!(super::mariadb_quoted("'it\\'s\\\\x'"), "'it''s\\\\x'");
        assert_eq!(super::mariadb_quoted("'it''s'"), "'it''s'");
        assert_eq!(super::mariadb_quoted("'a\\nb'"), "'a\\nb'");
        assert_eq!(super::mariadb_quoted("-5"), "-5");
    }
}
