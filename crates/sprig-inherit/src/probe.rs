//! Whether a change stream's subscriber could see a record just before a
//! change: a change stream sends a `delete` for a record
//! that left the subscriber's view only when the record was in it before.
//!
//! A change touches one row: a layer row of a space on the subscriber's
//! path, a row of a materialized copy, or a parent tenant's root row read
//! through its share view. Every other row the subscriber's merged view
//! reads is as it was, so the record before the change is the merged view
//! with that one row put back as the event's before image says (or taken
//! out, for an insert), filtered by the subscriber's `SELECT` policies:
//!
//! ```text
//! layer row of space L          → the layer, with L's row of the key replaced
//! copy row of share k           → the copy, with share k's row of the key replaced
//! a parent tenant's root row    → the share view's row of the key: the parent's
//!                                  record before, when it was one (not a hide
//!                                  marker) and passed the share's filter
//! view(subscriber's space, key) over these, filtered by its SELECT policies
//!   → one row: it was visible
//! ```
//!
//! A parent tenant's record is taken from its root row alone: overrides
//! the parent itself holds over a grandparent's record, and a
//! grandparent's change seen through the parent, are read with the
//! parent's current values. The rows the probe reads are restricted to
//! the key at every level, as the merged view's own are.

use serde_json::{Map, Value};
use sprig_common::Result;

use crate::grammar::Name;
use crate::plan::{Ctx, Cut};
use crate::view::{C as LAYER, Chain, Top, ViewSql, q};
use crate::{RowCmd, Runner, SpaceId};

/// Where the changed row is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A layer row of this space of the subscriber's tenant.
    Layer(SpaceId),
    /// A row of the materialized copy, of the share with this
    /// `_sprig_share` value.
    Copy(Value),
    /// A parent tenant's root row (read through the share view).
    Above,
}

/// One record to probe.
#[derive(Debug, Clone)]
pub struct Probe {
    /// The inheritable table, as the subscriber names it.
    pub table: String,
    /// The record's key, column by column, as the change's images carry it.
    pub key: Map<String, Value>,
    /// The changed row.
    pub source: Source,
    /// The changed row before the change, every column (`None`: it didn't
    /// exist).
    pub before: Option<Map<String, Value>>,
    /// The changed row's columns, for a change without a before image.
    pub columns: Vec<String>,
    /// The stream counts this change by its copy's handover
    /// ([`crate::handover`]), whatever the view reads now: the
    /// record before is taken through the tenant edge as its copy holds
    /// or its share view reads it, the copy taken as serving.
    pub counted: bool,
}

/// A column's MariaDB type, as `information_schema.COLUMNS` gives it.
#[derive(Debug, Clone, Default)]
struct ColumnType {
    data_type: String,
    column_type: String,
}

/// The members of an `enum('a','b')` or `set(...)` column type.
fn members(column_type: &str) -> Vec<String> {
    let Some(open) = column_type.find('(') else {
        return Vec::new();
    };
    let body = column_type[open + 1..].trim_end_matches(')');
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if quoted && chars.peek() == Some(&'\'') => {
                cur.push('\'');
                chars.next();
            }
            '\'' => {
                if quoted {
                    out.push(std::mem::take(&mut cur));
                }
                quoted = !quoted;
            }
            _ if quoted => cur.push(ch),
            _ => {}
        }
    }
    out
}

/// A string literal under the session's escaping.
fn string_lit(s: &str, nbe: bool) -> String {
    let body = if nbe {
        s.replace('\'', "''")
    } else {
        s.replace('\\', "\\\\").replace('\'', "''")
    };
    format!("'{body}'")
}

/// The SQL value of an image's value of a column of type `ty` (a change
/// record's encoding: `ENUM` and `SET` by number, binary strings in
/// base64, a `TIMESTAMP` in UTC).
fn sql_value(v: &Value, ty: Option<&ColumnType>, nbe: bool) -> String {
    let dt = ty
        .map(|t| t.data_type.to_ascii_lowercase())
        .unwrap_or_default();
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
        Value::Number(n) => match (dt.as_str(), n.as_u64()) {
            ("enum", Some(i)) => {
                let m = members(&ty.map(|t| t.column_type.clone()).unwrap_or_default());
                let i = usize::try_from(i).unwrap_or(0);
                string_lit(
                    if i == 0 {
                        ""
                    } else {
                        m.get(i - 1).map_or("", String::as_str)
                    },
                    nbe,
                )
            }
            ("set", Some(mask)) => {
                let m = members(&ty.map(|t| t.column_type.clone()).unwrap_or_default());
                let on: Vec<&str> = m
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i < 64 && mask & (1u64 << i) != 0)
                    .map(|(_, s)| s.as_str())
                    .collect();
                string_lit(&on.join(","), nbe)
            }
            _ => n.to_string(),
        },
        Value::String(s) => match dt.as_str() {
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                format!("FROM_BASE64({})", string_lit(s, nbe))
            }
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" => "NULL".to_owned(),
            // Records carry a TIMESTAMP in UTC: the session's time zone, as
            // the table shows it.
            "timestamp" => {
                let t = string_lit(s, nbe);
                format!(
                    "COALESCE(FROM_UNIXTIME(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', {t})) \
                     + INTERVAL MICROSECOND({t}) MICROSECOND, {t})"
                )
            }
            _ => string_lit(s, nbe),
        },
        other => string_lit(&other.to_string(), nbe),
    }
}

/// `SELECT <e> AS <c>, ...` of `row` over `columns` (absent ones `NULL`).
fn row_select(
    columns: &[String],
    row: &Map<String, Value>,
    types: &[(String, ColumnType)],
    nbe: bool,
) -> String {
    let ty = |c: &str| {
        types
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(c))
            .map(|(_, t)| t)
    };
    let list: Vec<String> = columns
        .iter()
        .map(|c| {
            let v = row
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(c))
                .map_or(Value::Null, |(_, v)| v.clone());
            format!("{} AS {}", sql_value(&v, ty(c), nbe), q(c))
        })
        .collect();
    format!("SELECT {}", list.join(", "))
}

/// Column types of `tables` in the database's schema, as the
/// provisioning account reads them; empty when they can't be read (values
/// then go in as the images carry them).
async fn column_types(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db_name: &str,
    table: &str,
) -> Vec<(String, ColumnType)> {
    let Ok(schema) = crate::plan::physical_schema(ctx.cat, db_name, ctx.lctn) else {
        return Vec::new();
    };
    let sql = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE \
         TABLE_SCHEMA = {} AND TABLE_NAME = {}",
        string_lit(&schema, false),
        string_lit(table, false)
    );
    let Ok(rows) = r.ctl_query(ctx.cat.tenant, &sql).await else {
        return Vec::new();
    };
    let text = |v: Option<&Option<Vec<u8>>>| {
        v.and_then(Option::as_ref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    };
    rows.iter()
        .map(|row| {
            (
                text(row.first()),
                ColumnType {
                    data_type: text(row.get(1)),
                    column_type: text(row.get(2)),
                },
            )
        })
        .collect()
}

/// Whether `chain` reads `source`: a layer row of a space on it, a row of
/// the copy (of the share) it starts from, a parent tenant's row through
/// its share view.
fn reads_in(chain: &Chain, source: &Source) -> bool {
    let copy = |from: &str| from.contains("__sprig_ih_");
    match (source, &chain.top) {
        (Source::Layer(space), _) => chain.levels.iter().any(|l| l.space == *space),
        (Source::Copy(share), Top::Upstream { from, filter, .. }) if copy(from) => {
            // `copy::read_sql`'s condition names the share it reads.
            share.as_u64().is_none_or(|k| {
                filter
                    .as_deref()
                    .is_none_or(|f| f.starts_with(&format!("`_sprig_share` = {k} ")))
            })
        }
        (Source::Above, Top::Upstream { from, .. }) => !copy(from),
        _ => false,
    }
}

/// Whether the session's merged view of `table` reaches the parent tenant
/// through its tenant edge (the edge's copy, serving or not, or its share
/// view), with no materialized share inside the tenant cutting it first:
/// the changes a copy's handover decides. `db`: the logical
/// database.
///
/// # Errors
/// The chain's.
pub fn through_tenant_edge(ctx: &Ctx<'_>, db: &str, table: &str) -> Result<bool> {
    let name = Name {
        db: Some(db.to_owned()),
        name: table.to_owned(),
    };
    let Some(res) = ctx.resolve(&name)? else {
        return Ok(false);
    };
    let chain = ctx.chain_with(&res.obj, ctx.space, &res.prefix, Cut::Declared)?;
    Ok(match &chain.top {
        Top::Upstream { from, filter, .. } if from.contains("__sprig_ih_") => {
            filter.as_deref().is_some_and(|f| {
                f.starts_with(&format!("`_sprig_share` = {} ", crate::copy::UPSTREAM))
            })
        }
        Top::Upstream { .. } => chain
            .levels
            .first()
            .is_some_and(|l| l.space == crate::ROOT_SPACE),
        Top::Base => false,
    })
}

/// Whether the session's merged view of `table` reads `source` now (the
/// cases [`visible_before`] answers `None` for, without its query): a
/// change stream sends nothing for a change its subscriber's view doesn't
/// read, such as a parent tenant's change while a copy serves it, or a
/// copy's while it is seeded. `db`: the logical database.
///
/// # Errors
/// The chain's.
pub fn reads(ctx: &Ctx<'_>, db: &str, table: &str, source: &Source) -> Result<bool> {
    let name = Name {
        db: Some(db.to_owned()),
        name: table.to_owned(),
    };
    let Some(res) = ctx.resolve(&name)? else {
        return Ok(false);
    };
    let chain = ctx.chain(&res.obj, ctx.space, &res.prefix)?;
    Ok(reads_in(&chain, source))
}

/// Whether the record of `p` was in the session's merged view, as its
/// `SELECT` policies filter it, just before the change (module docs).
/// `None` when the session's view doesn't read the changed row (a space
/// above a serving copy, a copy that isn't serving, a parent's row
/// reaching the session through a copy): another event, the one that
/// changes what it reads, decides then. `db`: the logical database.
///
/// # Errors
/// The chain's, the policies' and the backend's.
pub async fn visible_before(
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    db: &str,
    p: &Probe,
) -> Result<Option<bool>> {
    let name = Name {
        db: Some(db.to_owned()),
        name: p.table.clone(),
    };
    let Some(res) = ctx.resolve(&name)? else {
        return Ok(Some(false));
    };
    let o = &res.obj;
    let nbe = ctx.lex.no_backslash_escapes;
    let mut chain = match (p.counted, &p.source) {
        // A source change counted by the handover happened while the view
        // read through the edge (or no copy existed): the edge's top is
        // replaced by the row below, and a copy, perhaps not readable yet,
        // is never read.
        (true, Source::Above) => ctx.chain_with(o, ctx.space, &res.prefix, Cut::Never)?,
        // The copy, serving or not (a reseed keeps it the source).
        (true, _) => ctx.chain_with(o, ctx.space, &res.prefix, Cut::Declared)?,
        _ => ctx.chain(o, ctx.space, &res.prefix)?,
    };
    // A space the chain doesn't read (above a serving copy), a copy that
    // isn't serving, a parent's row reaching the session through a copy:
    // the change didn't reach the subscriber's view this way. A counted
    // parent's row reaches it through the edge whatever the edge's top.
    let counted_above = p.counted && p.source == Source::Above;
    if !counted_above && !reads_in(&chain, &p.source) {
        return Ok(None);
    }
    let mut layer = res.layer.clone();
    // The key, in the object's key order, and as a condition on `alias`.
    let key_types = column_types(ctx, r, &res.db_name, &o.table).await;
    let key_val = |c: &str| {
        let v = p
            .key
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(c))
            .map_or(Value::Null, |(_, v)| v.clone());
        let ty = key_types
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(c))
            .map(|(_, t)| t);
        sql_value(&v, ty, nbe)
    };
    let vals: Vec<String> = o.key.iter().map(|k| key_val(k)).collect();
    let tuple = if vals.len() == 1 {
        vals[0].clone()
    } else {
        format!("({})", vals.join(", "))
    };
    let key_match = |alias: &str| {
        o.key
            .iter()
            .zip(&vals)
            .map(|(k, v)| format!("{alias}.{} <=> {v}", q(k)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let image_columns = |b: Option<&Map<String, Value>>| -> Vec<String> {
        b.map_or_else(|| p.columns.clone(), |m| m.keys().cloned().collect())
    };
    match &p.source {
        Source::Layer(space) => {
            let mut cols: Vec<String> = o.columns.clone();
            for c in ["_sprig_space", "_sprig_state", "_sprig_ovr"] {
                cols.push(c.to_owned());
            }
            let keep = format!(
                "SELECT {} FROM {} AS {LAYER} WHERE {} AND NOT ({LAYER}.`_sprig_space` <=> {})",
                cols.iter()
                    .map(|c| format!("{LAYER}.{}", q(c)))
                    .collect::<Vec<_>>()
                    .join(", "),
                res.layer,
                key_match(LAYER),
                space.0
            );
            let add = p
                .before
                .as_ref()
                .map(|b| format!(" UNION ALL {}", row_select(&cols, b, &key_types, nbe)))
                .unwrap_or_default();
            layer = format!("({keep}{add})");
        }
        Source::Copy(share) => {
            let Top::Upstream { from, columns, .. } = &mut chain.top else {
                return Ok(None);
            };
            {
                let copy = from
                    .rsplit('.')
                    .next()
                    .unwrap_or_default()
                    .trim_matches('`')
                    .to_owned();
                let types = column_types(ctx, r, &res.db_name, &copy).await;
                // Columns the image lacks (a hierarchy's paths) read as
                // NULL in the row put back.
                let mut cols = image_columns(p.before.as_ref());
                for c in columns.iter() {
                    if !cols.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                        cols.push(c.clone());
                    }
                }
                // The copy's share and liveness follow, as the row put back
                // has them (a copy row's image carries them too).
                cols.retain(|c| {
                    !c.eq_ignore_ascii_case("_sprig_share")
                        && !c.eq_ignore_ascii_case("_sprig_live")
                });
                let src = "`__sprig_src`";
                let share_lit = sql_value(share, None, nbe);
                // The copy's own columns too: the view's condition reads
                // them (`copy::read_sql`), and change images leave them
                // out. A before image is a live row (a tombstone has none).
                let keep = format!(
                    "SELECT {}, {src}.`_sprig_share`, {src}.`_sprig_live` FROM {from} AS {src} \
                     WHERE {} AND NOT ({src}.`_sprig_share` <=> {share_lit})",
                    cols.iter()
                        .map(|c| format!("{src}.{}", q(c)))
                        .collect::<Vec<_>>()
                        .join(", "),
                    key_match(src),
                );
                let add = p
                    .before
                    .as_ref()
                    .map(|b| {
                        format!(
                            " UNION ALL {}, {share_lit} AS `_sprig_share`, TRUE AS `_sprig_live`",
                            row_select(&cols, b, &types, nbe)
                        )
                    })
                    .unwrap_or_default();
                *from = format!("({keep}{add}) AS {src}");
            }
        }
        Source::Above => {
            let filter = ctx.up_edge(o)?.and_then(|e| e.share.filter.clone());
            let Top::Upstream { from, columns, .. } = &mut chain.top else {
                return Ok(None);
            };
            // Through the edge's copy (a counted change): the row put
            // back is a live copy row of the edge, as its condition reads.
            let copy_cols = if from.contains("__sprig_ih_") {
                format!(
                    ", {} AS `_sprig_share`, TRUE AS `_sprig_live`",
                    crate::copy::UPSTREAM
                )
            } else {
                String::new()
            };
            {
                let src = "`__sprig_src`";
                let shared = columns
                    .iter()
                    .map(|c| format!("{src}.{}", q(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                // The parent's record before: its root row, unless that was
                // a hide marker, if the share's filter took it.
                let record = p.before.as_ref().filter(|b| {
                    b.iter()
                        .find(|(n, _)| n.eq_ignore_ascii_case("_sprig_state"))
                        .is_none_or(|(_, v)| {
                            !matches!(v, Value::String(s) if s == "hide") && v.as_u64() != Some(3)
                        })
                });
                let body = match record {
                    Some(b) => {
                        // Columns the image lacks (a hierarchy's paths)
                        // read as NULL.
                        let mut cols = image_columns(Some(b));
                        for c in columns.iter() {
                            if !cols.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                                cols.push(c.clone());
                            }
                        }
                        format!(
                            "SELECT {shared}{copy_cols} FROM ({}) AS {src} WHERE TRUE{}",
                            row_select(&cols, b, &key_types, nbe),
                            filter.map(|f| format!(" AND ({f})")).unwrap_or_default()
                        )
                    }
                    // No record before: no row (and no relation read).
                    None => format!(
                        "SELECT {}{copy_cols} WHERE FALSE",
                        columns
                            .iter()
                            .map(|c| format!("NULL AS {}", q(c)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };
                *from = format!("({body}) AS {src}");
            }
        }
    }
    let vs = ViewSql {
        o,
        layer,
        keys: Some(vec![tuple]),
        origin: false,
        derived: res.derived.clone(),
    };
    let view = vs.view(&chain);
    let filtered =
        crate::rewrite::filtered(ctx, r, &res, view, &vs.output_columns(), RowCmd::Select).await?;
    let sql = format!("SELECT COUNT(*) FROM ({filtered}) AS `__sprig_pb`");
    let rows = match r.query(&sql).await {
        Ok(rows) => rows,
        Err(e) => {
            // The statement carries the record's values: debug level only.
            tracing::debug!(table = %p.table, %sql, error = %e, "a visibility probe's statement failed");
            return Err(e);
        }
    };
    Ok(Some(
        rows.rows
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .is_some_and(|v| v.as_slice() != b"0"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::Level;

    #[test]
    fn a_chain_reads_only_its_own_sources() {
        let level = |s: u64| Level {
            space: SpaceId(s),
            name: String::new(),
            edge: None,
        };
        let top = |from: &str, filter: Option<&str>| Top::Upstream {
            from: from.into(),
            columns: Vec::new(),
            take: None,
            origin: String::new(),
            filter: filter.map(Into::into),
        };
        let copy = Chain {
            top: top(
                "`__sprig_ih_9`",
                Some("`_sprig_share` = 0 AND `_sprig_live` AND (x > 1)"),
            ),
            levels: vec![level(1), level(4)],
        };
        assert!(reads_in(&copy, &Source::Copy(Value::from(0))));
        assert!(!reads_in(&copy, &Source::Copy(Value::from(3))));
        assert!(
            !reads_in(&copy, &Source::Above),
            "a parent's row reaches it as a copy row"
        );
        assert!(reads_in(&copy, &Source::Layer(SpaceId(4))));
        assert!(!reads_in(&copy, &Source::Layer(SpaceId(2))));
        let through = Chain {
            top: top("`__sprig_share_3`", None),
            levels: vec![level(1)],
        };
        assert!(reads_in(&through, &Source::Above));
        assert!(
            !reads_in(&through, &Source::Copy(Value::from(0))),
            "a copy being seeded"
        );
        let base = Chain {
            top: Top::Base,
            levels: vec![level(1)],
        };
        assert!(!reads_in(&base, &Source::Above));
    }

    #[test]
    fn values_as_change_records_carry_them() {
        let e = ColumnType {
            data_type: "enum".into(),
            column_type: "enum('own','ovr','hide','it''s')".into(),
        };
        assert_eq!(members(&e.column_type), ["own", "ovr", "hide", "it's"]);
        assert_eq!(sql_value(&Value::from(3), Some(&e), false), "'hide'");
        assert_eq!(sql_value(&Value::from(4), Some(&e), false), "'it''s'");
        assert_eq!(sql_value(&Value::from(0), Some(&e), false), "''");
        let s = ColumnType {
            data_type: "set".into(),
            column_type: "set('a','b','c')".into(),
        };
        assert_eq!(sql_value(&Value::from(5), Some(&s), false), "'a,c'");
        let b = ColumnType {
            data_type: "varbinary".into(),
            column_type: "varbinary(16)".into(),
        };
        assert_eq!(
            sql_value(&Value::from("AAE="), Some(&b), false),
            "FROM_BASE64('AAE=')"
        );
        assert_eq!(sql_value(&Value::from("a\\'b"), None, false), "'a\\\\''b'");
        assert_eq!(sql_value(&Value::from("a\\'b"), None, true), "'a\\''b'");
        assert_eq!(sql_value(&Value::from(7.5), None, false), "7.5");
        assert_eq!(sql_value(&Value::Null, None, false), "NULL");
        // A TIMESTAMP comes in UTC: read in the session's time zone.
        let ts = ColumnType {
            data_type: "timestamp".into(),
            column_type: "timestamp".into(),
        };
        assert_eq!(
            sql_value(&Value::from("2026-11-16 10:00:00"), Some(&ts), false),
            "COALESCE(FROM_UNIXTIME(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', \
             '2026-11-16 10:00:00')) + INTERVAL MICROSECOND('2026-11-16 10:00:00') MICROSECOND, \
             '2026-11-16 10:00:00')"
        );
        let mut row = Map::new();
        row.insert("ID".into(), Value::from(2));
        let t = [("id".to_owned(), ColumnType::default())];
        assert_eq!(
            row_select(&["id".into(), "x".into()], &row, &t, false),
            "SELECT 2 AS `id`, NULL AS `x`"
        );
    }
}
