//! MySQL 8.4's result metadata for the `information_schema` tables the
//! engine builds (`super::infoschema`), as the MySQL port reports it.
//!
//! MySQL 8.4's `information_schema` tables are views over its data
//! dictionary, so a column's definition depends on how the view is read.
//! Read straight (the plain form), a column names the view as its table
//! and keeps the view column's own flags; a column the view computes
//! names no schema. Read through a temporary table (`DISTINCT`,
//! `GROUP BY`, or an `ORDER BY` that isn't already satisfied), each
//! column names its schema and the dictionary table behind it, with that
//! table's flags. Both forms were observed on MySQL 8.4 for every column.
//!
//! A text column's character set is the session's results character set,
//! which this module doesn't know: its definition carries [`TEXT`] and the
//! length in characters, and the answer's sender fills both in
//! ([`fill_text`]).

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::{BufMut, Bytes, BytesMut};

/// The character set id of a text column whose character set and byte
/// length the sender fills in (no collation has id 0).
pub(crate) const TEXT: u16 = 0;

/// One form of a column's definition.
#[derive(Debug, Clone, Copy)]
struct Form {
    /// `schema`/`table` named (`false` for a column the view computes in
    /// the plain form).
    schema: bool,
    /// Neither table nor original name (a constant, in the temporary form).
    constant: bool,
    /// The `org_table` of the temporary form (`""`: none).
    org_table: &'static str,
    /// Text (`length` in characters) or binary (`length` in bytes).
    text: bool,
    length: u32,
    col_type: u8,
    flags: u16,
    decimals: u8,
}

/// Each column: `table column` then the plain form, `|`, and the
/// temporary form. A form is `k [org_table] length type flags decimals`:
/// `k` is `i` (schema named), `-` (no schema) or `0` (constant); length is
/// `t<chars>`, `tmax` (the most) or `b<bytes>`; flags in hex.
const COLUMNS: &str = "\
SCHEMATA CATALOG_NAME i t64 253 5085 0 | i catalogs t64 253 1081 0
SCHEMATA SCHEMA_NAME i t64 253 5081 0 | i schemata t64 253 1081 0
SCHEMATA DEFAULT_CHARACTER_SET_NAME i t64 253 5005 0 | i character_sets t64 253 1001 0
SCHEMATA DEFAULT_COLLATION_NAME i t64 253 5005 0 | i collations t64 253 1001 0
SCHEMATA SQL_PATH - b0 6 0080 0 | 0 - b0 6 0080 0
SCHEMATA DEFAULT_ENCRYPTION i t3 254 1181 0 | i schemata t3 254 1181 0
PARTITIONS TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
PARTITIONS TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
PARTITIONS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
PARTITIONS PARTITION_NAME i t64 253 5000 0 | i table_partitions t64 253 1000 0
PARTITIONS SUBPARTITION_NAME i t64 253 5000 0 | i table_partitions t64 253 1000 0
PARTITIONS PARTITION_ORDINAL_POSITION - b6 8 00a0 0 | i - b6 3 0020 0
PARTITIONS SUBPARTITION_ORDINAL_POSITION - b6 8 00a0 0 | i - b6 3 0020 0
PARTITIONS PARTITION_METHOD - t13 253 0000 31 | i - t13 253 0000 0
PARTITIONS SUBPARTITION_METHOD - t13 253 0000 31 | i - t13 253 0000 0
PARTITIONS PARTITION_EXPRESSION i t2048 253 0080 0 | i tables t2048 253 0080 0
PARTITIONS SUBPARTITION_EXPRESSION i t2048 253 0080 0 | i tables t2048 253 0080 0
PARTITIONS PARTITION_DESCRIPTION i t65535 252 0090 0 | i table_partitions t65535 252 0090 0
PARTITIONS TABLE_ROWS - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS AVG_ROW_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS DATA_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS MAX_DATA_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS INDEX_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS DATA_FREE - b21 8 00a0 0 | i - b21 8 0020 0
PARTITIONS CREATE_TIME i t19 7 1081 0 | i tables b19 7 1081 0
PARTITIONS UPDATE_TIME - t19 12 0080 0 | i - b19 12 0080 0
PARTITIONS CHECK_TIME - t19 12 0080 0 | i - b19 12 0080 0
PARTITIONS CHECKSUM - b21 8 0080 0 | i - b21 8 0000 0
PARTITIONS PARTITION_COMMENT - t2048 253 0081 31 | i - t6144 252 0091 0
PARTITIONS NODEGROUP - t256 253 0000 31 | i - t256 253 0000 0
PARTITIONS TABLESPACE_NAME - t268 253 0080 31 | i - t268 253 0080 0
TRIGGERS TRIGGER_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
TRIGGERS TRIGGER_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
TRIGGERS TRIGGER_NAME i t64 253 5001 0 | i triggers t64 253 1001 0
TRIGGERS EVENT_MANIPULATION i t6 254 5181 0 | i triggers t6 254 1181 0
TRIGGERS EVENT_OBJECT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
TRIGGERS EVENT_OBJECT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
TRIGGERS EVENT_OBJECT_TABLE i t64 253 5081 0 | i tables t64 253 1081 0
TRIGGERS ACTION_ORDER i b10 3 5021 0 | i triggers b10 3 1021 0
TRIGGERS ACTION_CONDITION - b0 6 0080 0 | 0 - b0 6 0080 0
TRIGGERS ACTION_STATEMENT i tmax 252 1091 0 | i triggers tmax 252 1091 0
TRIGGERS ACTION_ORIENTATION - t3 253 0001 31 | 0 - t3 253 0001 31
TRIGGERS ACTION_TIMING i t6 254 5181 0 | i triggers t6 254 1181 0
TRIGGERS ACTION_REFERENCE_OLD_TABLE - b0 6 0080 0 | 0 - b0 6 0080 0
TRIGGERS ACTION_REFERENCE_NEW_TABLE - b0 6 0080 0 | 0 - b0 6 0080 0
TRIGGERS ACTION_REFERENCE_OLD_ROW - t3 253 0001 31 | 0 - t3 253 0001 31
TRIGGERS ACTION_REFERENCE_NEW_ROW - t3 253 0001 31 | 0 - t3 253 0001 31
TRIGGERS CREATED i t22 7 1081 2 | i triggers b22 7 1081 2
TRIGGERS SQL_MODE i t520 254 1881 0 | i triggers t520 254 1881 0
TRIGGERS DEFINER i t288 253 5089 0 | i triggers t288 253 1081 0
TRIGGERS CHARACTER_SET_CLIENT i t64 253 5005 0 | i character_sets t64 253 1001 0
TRIGGERS COLLATION_CONNECTION i t64 253 5005 0 | i collations t64 253 1001 0
TRIGGERS DATABASE_COLLATION i t64 253 5005 0 | i collations t64 253 1001 0
EVENTS EVENT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
EVENTS EVENT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
EVENTS EVENT_NAME i t64 253 5001 0 | i events t64 253 1001 0
EVENTS DEFINER i t288 253 5089 0 | i events t288 253 1081 0
EVENTS TIME_ZONE i t64 253 1081 0 | i events t64 253 1081 0
EVENTS EVENT_BODY - t3 253 0001 31 | 0 - t3 253 0001 31
EVENTS EVENT_DEFINITION i tmax 252 1091 0 | i events tmax 252 1091 0
EVENTS EVENT_TYPE - t9 253 0001 31 | i - t9 253 0001 0
EVENTS EXECUTE_AT - t19 12 0080 0 | i - b19 12 0080 0
EVENTS INTERVAL_VALUE - t256 253 0000 31 | i - t256 253 0000 0
EVENTS INTERVAL_FIELD i t18 254 0180 0 | i events t18 254 0180 0
EVENTS SQL_MODE i t520 254 1881 0 | i events t520 254 1881 0
EVENTS STARTS - t19 12 0080 0 | i - b19 12 0080 0
EVENTS ENDS - t19 12 0080 0 | i - b19 12 0080 0
EVENTS STATUS - t21 253 0081 31 | i - t21 253 0081 0
EVENTS ON_COMPLETION - t12 253 0001 31 | i - t12 253 0001 0
EVENTS CREATED i t19 7 1081 0 | i events b19 7 1081 0
EVENTS LAST_ALTERED i t19 7 1081 0 | i events b19 7 1081 0
EVENTS LAST_EXECUTED - t19 12 0080 0 | i - b19 12 0080 0
EVENTS EVENT_COMMENT i t2048 253 1081 0 | i events t2048 253 1081 0
EVENTS ORIGINATOR i b10 3 1021 0 | i events b10 3 1021 0
EVENTS CHARACTER_SET_CLIENT i t64 253 5005 0 | i character_sets t64 253 1001 0
EVENTS COLLATION_CONNECTION i t64 253 5005 0 | i collations t64 253 1001 0
EVENTS DATABASE_COLLATION i t64 253 5005 0 | i collations t64 253 1001 0
ROUTINES SPECIFIC_NAME i t64 253 5001 0 | i routines t64 253 1001 0
ROUTINES ROUTINE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
ROUTINES ROUTINE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
ROUTINES ROUTINE_NAME i t64 253 5001 0 | i routines t64 253 1001 0
ROUTINES ROUTINE_TYPE i t9 254 5181 0 | i routines t9 254 1181 0
ROUTINES DATA_TYPE - t50331645 251 0080 31 | i - t50331645 252 0090 0
ROUTINES CHARACTER_MAXIMUM_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
ROUTINES CHARACTER_OCTET_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
ROUTINES NUMERIC_PRECISION i b10 3 0020 0 | i routines b10 3 0020 0
ROUTINES NUMERIC_SCALE i b10 3 0020 0 | i routines b10 3 0020 0
ROUTINES DATETIME_PRECISION i b10 3 0020 0 | i routines b10 3 0020 0
ROUTINES CHARACTER_SET_NAME - t64 253 0000 31 | i - t64 253 0000 0
ROUTINES COLLATION_NAME - t64 253 0000 31 | i - t64 253 0000 0
ROUTINES DTD_IDENTIFIER - t50331645 252 0080 31 | i - t50331645 252 0090 0
ROUTINES ROUTINE_BODY - t8 253 0001 31 | i - t8 253 0001 0
ROUTINES ROUTINE_DEFINITION - tmax 252 0080 31 | i - tmax 252 0090 0
ROUTINES EXTERNAL_NAME - b0 6 0080 0 | 0 - b0 6 0080 0
ROUTINES EXTERNAL_LANGUAGE i t64 253 0081 0 | i routines t64 253 0081 0
ROUTINES PARAMETER_STYLE - t3 253 0001 31 | 0 - t3 253 0001 31
ROUTINES IS_DETERMINISTIC - t3 253 0001 31 | i - t3 253 0001 0
ROUTINES SQL_DATA_ACCESS i t17 254 1181 0 | i routines t17 254 1181 0
ROUTINES SQL_PATH - b0 6 0080 0 | 0 - b0 6 0080 0
ROUTINES SECURITY_TYPE i t7 254 1181 0 | i routines t7 254 1181 0
ROUTINES CREATED i t19 7 1081 0 | i routines b19 7 1081 0
ROUTINES LAST_ALTERED i t19 7 1081 0 | i routines b19 7 1081 0
ROUTINES SQL_MODE i t520 254 1881 0 | i routines t520 254 1881 0
ROUTINES ROUTINE_COMMENT i t65535 252 1091 0 | i routines t65535 252 1091 0
ROUTINES DEFINER i t288 253 5089 0 | i routines t288 253 1081 0
ROUTINES CHARACTER_SET_CLIENT i t64 253 5005 0 | i character_sets t64 253 1001 0
ROUTINES COLLATION_CONNECTION i t64 253 5005 0 | i collations t64 253 1001 0
ROUTINES DATABASE_COLLATION i t64 253 5005 0 | i collations t64 253 1001 0
PARAMETERS SPECIFIC_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
PARAMETERS SPECIFIC_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
PARAMETERS SPECIFIC_NAME i t64 253 5001 0 | i routines t64 253 1001 0
PARAMETERS ORDINAL_POSITION - b11 8 00a1 0 | i - b11 8 0021 0
PARAMETERS PARAMETER_MODE - t5 247 0080 31 | i - t5 253 0080 0
PARAMETERS PARAMETER_NAME - t64 253 0000 31 | i - t64 253 0000 0
PARAMETERS DATA_TYPE - t50331645 251 0080 31 | i - t50331645 252 0090 0
PARAMETERS CHARACTER_MAXIMUM_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
PARAMETERS CHARACTER_OCTET_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
PARAMETERS NUMERIC_PRECISION i b10 3 0020 0 | i parameters b10 3 0020 0
PARAMETERS NUMERIC_SCALE - b10 8 0080 0 | i - b10 8 0000 0
PARAMETERS DATETIME_PRECISION i b10 3 0020 0 | i parameters b10 3 0020 0
PARAMETERS CHARACTER_SET_NAME - t64 253 0000 31 | i - t64 253 0000 0
PARAMETERS COLLATION_NAME - t64 253 0000 31 | i - t64 253 0000 0
PARAMETERS DTD_IDENTIFIER i t16777215 252 1091 0 | i parameters t16777215 252 1091 0
PARAMETERS ROUTINE_TYPE i t9 254 5181 0 | i routines t9 254 1181 0
TABLES TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
TABLES TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
TABLES TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
TABLES TABLE_TYPE i t11 254 5189 0 | i tables t11 254 1181 0
TABLES ENGINE - t64 253 0000 31 | i - t64 253 0000 0
TABLES VERSION - b3 8 0080 0 | i - b3 3 0000 0
TABLES ROW_FORMAT i t10 254 0180 0 | i tables t10 254 0180 0
TABLES TABLE_ROWS - b21 8 00a0 0 | i - b21 8 0020 0
TABLES AVG_ROW_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
TABLES DATA_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
TABLES MAX_DATA_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
TABLES INDEX_LENGTH - b21 8 00a0 0 | i - b21 8 0020 0
TABLES DATA_FREE - b21 8 00a0 0 | i - b21 8 0020 0
TABLES AUTO_INCREMENT - b21 8 00a0 0 | i - b21 8 0020 0
TABLES CREATE_TIME i t19 7 1081 0 | i tables b19 7 1081 0
TABLES UPDATE_TIME - t19 12 0000 0 | i - b19 12 0080 0
TABLES CHECK_TIME - t19 12 0000 0 | i - b19 12 0080 0
TABLES TABLE_COLLATION i t64 253 5004 0 | i collations t64 253 1000 0
TABLES CHECKSUM - b21 8 0080 0 | i - b21 8 0000 0
TABLES CREATE_OPTIONS - t256 253 0000 31 | i - t256 253 0000 0
TABLES TABLE_COMMENT - t2048 253 0000 31 | i - t6144 252 0010 0
VIEWS TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
VIEWS TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
VIEWS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
VIEWS VIEW_DEFINITION - tmax 252 0080 31 | i - tmax 252 0090 0
VIEWS CHECK_OPTION i t8 254 0180 0 | i tables t8 254 0180 0
VIEWS IS_UPDATABLE i t3 254 0180 0 | i tables t3 254 0180 0
VIEWS DEFINER i t288 253 4080 0 | i tables t288 253 0080 0
VIEWS SECURITY_TYPE - t7 253 0080 31 | i - t7 253 0080 0
VIEWS CHARACTER_SET_CLIENT i t64 253 5005 0 | i character_sets t64 253 1001 0
VIEWS COLLATION_CONNECTION i t64 253 5005 0 | i collations t64 253 1001 0
COLUMNS TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
COLUMNS TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
COLUMNS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
COLUMNS COLUMN_NAME - t64 253 0000 31 | i - t64 253 0000 0
COLUMNS ORDINAL_POSITION i b10 3 5021 0 | i columns b10 3 1021 0
COLUMNS COLUMN_DEFAULT i t65535 252 0090 0 | i columns t65535 252 0090 0
COLUMNS IS_NULLABLE - t3 253 0001 31 | i - t3 253 0001 0
COLUMNS DATA_TYPE - t50331645 251 0080 31 | i - t50331645 252 0090 0
COLUMNS CHARACTER_MAXIMUM_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
COLUMNS CHARACTER_OCTET_LENGTH - b21 8 0080 0 | i - b21 8 0000 0
COLUMNS NUMERIC_PRECISION - b10 3 00a0 0 | i - b10 8 0020 0
COLUMNS NUMERIC_SCALE - b10 3 00a0 0 | i - b10 8 0020 0
COLUMNS DATETIME_PRECISION i b10 3 0020 0 | i columns b10 3 0020 0
COLUMNS CHARACTER_SET_NAME - t64 253 0000 31 | i - t64 253 0000 0
COLUMNS COLLATION_NAME - t64 253 0000 31 | i - t64 253 0000 0
COLUMNS COLUMN_TYPE i t16777215 252 1091 0 | i columns t16777215 252 1091 0
COLUMNS COLUMN_KEY i t3 254 1181 0 | i columns t3 254 1181 0
COLUMNS EXTRA - t256 253 0000 31 | i - t256 253 0000 0
COLUMNS PRIVILEGES - t154 253 0000 31 | i - t154 253 0000 0
COLUMNS COLUMN_COMMENT - t2048 253 0081 31 | i - t6144 252 0091 0
COLUMNS GENERATION_EXPRESSION - tmax 252 0081 31 | i - tmax 252 0091 0
COLUMNS SRS_ID i b10 3 4028 0 | i columns b10 3 0020 0
STATISTICS TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
STATISTICS TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
STATISTICS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
STATISTICS NON_UNIQUE - b2 8 0081 0 | i - b2 3 0001 0
STATISTICS INDEX_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
STATISTICS INDEX_NAME - t64 253 0000 31 | i - t64 253 0000 0
STATISTICS SEQ_IN_INDEX i b10 3 5023 0 | i index_column_usage b10 3 1021 0
STATISTICS COLUMN_NAME - t64 253 0000 31 | i - t64 253 0000 0
STATISTICS COLLATION - t1 253 0000 31 | i - t1 253 0000 0
STATISTICS CARDINALITY - b21 8 0080 0 | i - b21 8 0000 0
STATISTICS SUB_PART - b21 8 0080 0 | i - b21 8 0000 0
STATISTICS PACKED - b0 6 0080 0 | 0 - b0 6 0080 0
STATISTICS NULLABLE - t3 253 0001 31 | i - t3 253 0001 0
STATISTICS INDEX_TYPE - t11 253 0081 31 | i - t11 253 0081 0
STATISTICS COMMENT - t8 253 0001 31 | i - t8 253 0001 0
STATISTICS INDEX_COMMENT i t2048 253 1081 0 | i indexes t2048 253 1081 0
STATISTICS IS_VISIBLE - t3 253 0001 31 | i - t3 253 0001 0
STATISTICS EXPRESSION - tmax 252 0080 31 | i - tmax 252 0090 0
TABLE_CONSTRAINTS CONSTRAINT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
TABLE_CONSTRAINTS CONSTRAINT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
TABLE_CONSTRAINTS CONSTRAINT_NAME i t64 253 0000 0 | i - t64 253 0000 0
TABLE_CONSTRAINTS TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
TABLE_CONSTRAINTS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
TABLE_CONSTRAINTS CONSTRAINT_TYPE i t11 253 0081 0 | i - t11 253 0081 0
TABLE_CONSTRAINTS ENFORCED i t3 253 0081 0 | i - t3 253 0081 0
KEY_COLUMN_USAGE CONSTRAINT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
KEY_COLUMN_USAGE CONSTRAINT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
KEY_COLUMN_USAGE CONSTRAINT_NAME i t64 253 0000 0 | i - t64 253 0000 0
KEY_COLUMN_USAGE TABLE_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
KEY_COLUMN_USAGE TABLE_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
KEY_COLUMN_USAGE TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
KEY_COLUMN_USAGE COLUMN_NAME - t64 253 0000 31 | i - t64 253 0000 0
KEY_COLUMN_USAGE ORDINAL_POSITION i b10 3 0021 0 | i - b10 3 0021 0
KEY_COLUMN_USAGE POSITION_IN_UNIQUE_CONSTRAINT i b10 3 0020 0 | i - b10 3 0020 0
KEY_COLUMN_USAGE REFERENCED_TABLE_SCHEMA i t64 253 0080 0 | i - t64 253 0080 0
KEY_COLUMN_USAGE REFERENCED_TABLE_NAME i t64 253 0080 0 | i - t64 253 0080 0
KEY_COLUMN_USAGE REFERENCED_COLUMN_NAME i t64 253 0000 0 | i - t64 253 0000 0
CHECK_CONSTRAINTS CONSTRAINT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
CHECK_CONSTRAINTS CONSTRAINT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
CHECK_CONSTRAINTS CONSTRAINT_NAME i t64 253 5001 0 | i check_constraints t64 253 1001 0
CHECK_CONSTRAINTS CHECK_CLAUSE i tmax 252 1091 0 | i check_constraints tmax 252 1091 0
REFERENTIAL_CONSTRAINTS CONSTRAINT_CATALOG i t64 253 5085 0 | i catalogs t64 253 1081 0
REFERENTIAL_CONSTRAINTS CONSTRAINT_SCHEMA i t64 253 5081 0 | i schemata t64 253 1081 0
REFERENTIAL_CONSTRAINTS CONSTRAINT_NAME - t64 253 0000 31 | i - t64 253 0000 0
REFERENTIAL_CONSTRAINTS UNIQUE_CONSTRAINT_CATALOG i t64 253 5089 0 | i foreign_keys t64 253 1081 0
REFERENTIAL_CONSTRAINTS UNIQUE_CONSTRAINT_SCHEMA i t64 253 5081 0 | i foreign_keys t64 253 1081 0
REFERENTIAL_CONSTRAINTS UNIQUE_CONSTRAINT_NAME i t64 253 0000 0 | i foreign_keys t64 253 0000 0
REFERENTIAL_CONSTRAINTS MATCH_OPTION i t7 254 1181 0 | i foreign_keys t7 254 1181 0
REFERENTIAL_CONSTRAINTS UPDATE_RULE i t11 254 1181 0 | i foreign_keys t11 254 1181 0
REFERENTIAL_CONSTRAINTS DELETE_RULE i t11 254 1181 0 | i foreign_keys t11 254 1181 0
REFERENTIAL_CONSTRAINTS TABLE_NAME i t64 253 5081 0 | i tables t64 253 1081 0
REFERENTIAL_CONSTRAINTS REFERENCED_TABLE_NAME i t64 253 5081 0 | i foreign_keys t64 253 1081 0
";

fn form(f: &[&'static str], with_table: bool) -> Option<Form> {
    let (k, rest) = f.split_first()?;
    let (org_table, rest) = if with_table {
        let (t, r) = rest.split_first()?;
        (if *t == "-" { "" } else { *t }, r)
    } else {
        ("", rest)
    };
    let [len, ty, flags, dec] = rest else {
        return None;
    };
    let (text, length) = match (len.strip_prefix('t'), len.strip_prefix('b')) {
        (Some("max"), _) => (true, u32::MAX),
        (Some(n), _) => (true, n.parse().ok()?),
        (_, Some(n)) => (false, n.parse().ok()?),
        _ => return None,
    };
    Some(Form {
        schema: *k == "i",
        constant: *k == "0",
        org_table,
        text,
        length,
        col_type: ty.parse().ok()?,
        flags: u16::from_str_radix(flags, 16).ok()?,
        decimals: dec.parse().ok()?,
    })
}

type Table = HashMap<(&'static str, String), (&'static str, Form, Form)>;

fn table() -> &'static Table {
    static T: OnceLock<Table> = OnceLock::new();
    T.get_or_init(|| {
        let mut m = HashMap::new();
        for line in COLUMNS.lines() {
            let Some((plain, temp)) = line.split_once(" | ") else {
                continue;
            };
            let p: Vec<&'static str> = plain.split(' ').collect();
            let t: Vec<&'static str> = temp.split(' ').collect();
            if let ([table, column, rest @ ..], Some(tf)) = (p.as_slice(), form(&t, true))
                && let Some(pf) = form(rest, false)
            {
                m.insert((*table, column.to_ascii_uppercase()), (*column, pf, tf));
            }
        }
        m
    })
}

fn lenenc_str(b: &mut BytesMut, s: &[u8]) {
    // Every name here is shorter than 251 bytes.
    b.put_u8(u8::try_from(s.len()).unwrap_or(250));
    b.put_slice(&s[..s.len().min(250)]);
}

/// MySQL 8.4's definition of column `name` (in any letter case) of view
/// `view` (`None`: not a column of it), read in the plain or the
/// temporary form, the view written `written` in the query. A column read
/// as itself is named as the view names it; one read under an alias, by
/// `alias`.
pub(crate) fn coldef(
    view: &str,
    written: &str,
    name: &str,
    alias: Option<&str>,
    temp: bool,
) -> Option<Bytes> {
    let view = view_key(view)?;
    let &(column, plain, tform) = table().get(&(view, name.to_ascii_uppercase()))?;
    let f = if temp { tform } else { plain };
    let mut b = BytesMut::with_capacity(96);
    lenenc_str(&mut b, b"def");
    let (schema, table, org_table, org_name) = if f.constant {
        ("", "", "", "")
    } else if temp {
        ("information_schema", written, f.org_table, column)
    } else {
        (
            if f.schema { "information_schema" } else { "" },
            written,
            view,
            column,
        )
    };
    lenenc_str(&mut b, schema.as_bytes());
    lenenc_str(&mut b, table.as_bytes());
    lenenc_str(&mut b, org_table.as_bytes());
    lenenc_str(&mut b, alias.unwrap_or(column).as_bytes());
    lenenc_str(&mut b, org_name.as_bytes());
    b.put_u8(0x0c);
    b.put_u16_le(if f.text { TEXT } else { 63 });
    b.put_u32_le(f.length);
    b.put_u8(f.col_type);
    b.put_u16_le(f.flags);
    b.put_u8(f.decimals);
    b.put_u16_le(0);
    Some(b.freeze())
}

fn view_key(view: &str) -> Option<&'static str> {
    super::infoschema::TABLES
        .iter()
        .copied()
        .find(|t| t.eq_ignore_ascii_case(view))
}

/// A column definition's text character set and byte length, for
/// definitions made by [`coldef`] (charset [`TEXT`], length in
/// characters): the results character set's id and its bytes per
/// character, or, when the session's `character_set_results` is NULL,
/// the dictionary's own `utf8mb3_general_ci`.
pub(crate) fn fill_text(
    c: &mut sprig_proto_mysql::ColumnDef,
    results: Option<super::meta::ResultsCs>,
) {
    if c.charset != TEXT {
        return;
    }
    let (id, mb) = results.map_or((33, 3), |r| (r.id, r.mbmax));
    c.charset = id;
    if c.length != u32::MAX {
        c.length = c.length.saturating_mul(mb);
    }
}

#[cfg(test)]
mod tests {
    use super::{TEXT, coldef, table};

    #[test]
    fn every_column_reads_in_both_forms() {
        assert_eq!(table().len(), 229);
        let d = coldef("columns", "columns", "table_name", None, false).expect("a column");
        let c = sprig_proto_mysql::ColumnDef::decode(&d, sprig_proto_mysql::Caps::PROTOCOL_41)
            .expect("decodes");
        assert_eq!(&c.name[..], b"TABLE_NAME");
        assert_eq!(&c.org_table[..], b"COLUMNS");
        assert_eq!((c.charset, c.length, c.flags), (TEXT, 64, 0x5081));
        let d = coldef("COLUMNS", "COLUMNS", "TABLE_NAME", None, true).expect("a column");
        let c = sprig_proto_mysql::ColumnDef::decode(&d, sprig_proto_mysql::Caps::PROTOCOL_41)
            .expect("decodes");
        assert_eq!(&c.org_table[..], b"tables");
        assert_eq!(c.flags, 0x1081);
        assert!(coldef("COLUMNS", "COLUMNS", "IS_GENERATED", None, false).is_none());
    }
}
