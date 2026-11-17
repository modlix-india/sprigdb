//! The facts a digest reads from a tenant's catalog (`t<id>_sprig`): schema
//! versions, the workload store, automatic indexes and the approvals
//! inbox. Only names, counts and times are read; a module the tenant
//! doesn't have yet (no workload recorded, no recommendations) is noted
//! in [`Facts::missing`] instead of failing the digest.

use std::collections::BTreeMap;

use mysql_async::prelude::{FromRow, Queryable as _};
use serde_json::Value;
use sprig_common::Result;
use sprig_common::names::quote;

use super::{
    AutoIndex, Facts, MAX_CHANGES, MAX_STATEMENT, MAX_WAITING, SLOWEST, SchemaChange, Statement,
    Waiting, Workload,
};
use crate::audit::unavailable;

/// A schema change's row: database, object, kind, the statement's start,
/// when (ms).
type ChangeRow = (String, String, String, String, i64);
/// An automatic index's row: name, state, definition, table, database.
type IndexRow = (String, String, String, String, String);

/// MariaDB's "table doesn't exist".
const NO_TABLE: u16 = 1146;

/// `ms` as a `DATETIME` literal value (UTC).
fn at(ms: i64) -> String {
    let rest = ms.rem_euclid(86_400_000);
    format!(
        "{} {:02}:{:02}:{:02}",
        super::date(ms),
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60
    )
}

/// Rows of `sql`, or `None` when its table doesn't exist.
async fn rows<T: FromRow + Send + 'static>(
    c: &mut mysql_async::Conn,
    sql: String,
    params: impl Into<mysql_async::Params> + Send,
) -> Result<Option<Vec<T>>> {
    match c.exec(sql, params).await {
        Ok(r) => Ok(Some(r)),
        Err(mysql_async::Error::Server(e)) if e.code == NO_TABLE => Ok(None),
        Err(e) => Err(unavailable(e)),
    }
}

/// The tenant's databases by id.
///
/// # Errors
/// 39021 when the catalog is unreachable.
pub async fn databases(c: &mut mysql_async::Conn, schema: &str) -> Result<BTreeMap<u32, String>> {
    let r: Vec<(u32, String)> = rows(
        c,
        format!(
            "SELECT db_id, name FROM {}.`databases` WHERE state = 'active'",
            quote(schema)
        ),
        (),
    )
    .await?
    .unwrap_or_default();
    Ok(r.into_iter().collect())
}

/// Fills `facts` from the catalog in `schema`; `now_ms` dates the waiting
/// recommendations' ages.
///
/// # Errors
/// 39021 when the catalog is unreachable.
#[allow(clippy::too_many_lines)] // one query per part
pub async fn catalog(
    c: &mut mysql_async::Conn,
    schema: &str,
    facts: &mut Facts,
    now_ms: i64,
) -> Result<()> {
    let s = quote(schema);
    let (from, to) = (at(facts.from_ms), at(facts.to_ms));
    let len = facts.to_ms - facts.from_ms;
    let before = at(facts.from_ms - len);
    let dbs = databases(c, schema).await?;

    // Schema changes: the object and the statement's verb, never its text.
    let changes: Option<Vec<ChangeRow>> = rows(
        c,
        format!(
            "SELECT d.name, o.name, o.kind, LEFT(TRIM(v.ddl), 24), \
             CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', v.applied_at) DIV 1000 AS SIGNED) \
             FROM {s}.schema_versions v JOIN {s}.objects o ON o.object_id = v.object_id \
             JOIN {s}.`databases` d ON d.db_id = o.db_id \
             WHERE v.applied_at >= ? AND v.applied_at < ? ORDER BY v.applied_at, v.object_id"
        ),
        (&from, &to),
    )
    .await?;
    if let Some(changes) = changes {
        facts.schema_change_count = changes.len() as u64;
        facts.schema_changes = changes
            .into_iter()
            .take(MAX_CHANGES)
            .map(|(db, object, kind, ddl, at_ms)| SchemaChange {
                db,
                object,
                kind,
                verb: ddl
                    .split_whitespace()
                    .next()
                    .unwrap_or("CHANGE")
                    .to_ascii_uppercase(),
                at_ms,
            })
            .collect();
    }

    // The workload store: totals for the period and the one before, and
    // the statements that took the most time.
    let totals = format!(
        "SELECT CAST(COALESCE(SUM(exec_count), 0) AS UNSIGNED), \
         CAST(COALESCE(SUM(error_count), 0) AS UNSIGNED), \
         CAST(COALESCE(SUM(total_us), 0) AS UNSIGNED) \
         FROM {s}.workload_stats WHERE bucket_start >= ? AND bucket_start < ?"
    );
    let now: Option<Vec<(u64, u64, u64)>> = rows(c, totals.clone(), (&from, &to)).await?;
    #[allow(clippy::cast_precision_loss)]
    let workload = |r: Option<&(u64, u64, u64)>| {
        r.map(|(n, e, us)| Workload {
            statements: *n,
            errors: *e,
            total_ms: *us as f64 / 1000.0,
        })
    };
    match now {
        None => facts
            .missing
            .push("the workload (no statements recorded yet)".to_owned()),
        Some(now) => {
            facts.workload = workload(now.first()).unwrap_or_default();
            let prev: Option<Vec<(u64, u64, u64)>> = rows(c, totals, (&before, &from)).await?;
            facts.workload_before = prev
                .as_ref()
                .and_then(|p| workload(p.first()))
                .filter(|w| w.statements > 0);
            let top: Vec<(u64, u64, u64, u64, u64)> = rows(
                c,
                format!(
                    "SELECT fingerprint_id, CAST(SUM(exec_count) AS UNSIGNED), \
                     CAST(SUM(error_count) AS UNSIGNED), CAST(SUM(total_us) AS UNSIGNED), \
                     CAST(MAX(max_us) AS UNSIGNED) FROM {s}.workload_stats \
                     WHERE bucket_start >= ? AND bucket_start < ? \
                     GROUP BY fingerprint_id ORDER BY SUM(total_us) DESC LIMIT {SLOWEST}"
                ),
                (&from, &to),
            )
            .await?
            .unwrap_or_default();
            for (fp, calls, errors, total_us, max_us) in top {
                let info: Option<(u32, String)> = c
                    .exec_first(
                        format!(
                            "SELECT db_id, LEFT(normalized_text, {MAX_STATEMENT}) \
                             FROM {s}.workload_fingerprints WHERE fingerprint_id = ?"
                        ),
                        (fp,),
                    )
                    .await
                    .map_err(unavailable)?;
                let (db, text) = info.map_or((None, String::new()), |(d, t)| {
                    (
                        dbs.get(&d).cloned(),
                        t.split_whitespace().collect::<Vec<_>>().join(" "),
                    )
                });
                #[allow(clippy::cast_precision_loss)]
                facts.slowest.push(Statement {
                    fingerprint: format!("{fp:016x}"),
                    db,
                    text,
                    calls,
                    errors,
                    total_ms: total_us as f64 / 1000.0,
                    max_ms: max_us as f64 / 1000.0,
                });
            }
        }
    }

    // Automatic indexes that changed state in the period.
    let auto: Option<Vec<IndexRow>> = rows(
        c,
        format!(
            "SELECT a.name, a.state, CAST(a.definition AS CHAR), o.name, d.name \
             FROM {s}.auto_indexes a JOIN {s}.objects o ON o.object_id = a.object_id \
             JOIN {s}.`databases` d ON d.db_id = o.db_id \
             WHERE a.state_changed_at >= ? AND a.state_changed_at < ? \
             AND a.state IN ('active', 'observing', 'rolling_out', 'retiring', 'dropped') \
             ORDER BY a.state_changed_at LIMIT 20"
        ),
        (&from, &to),
    )
    .await?;
    for (name, state, def, table, db) in auto.unwrap_or_default() {
        let columns = serde_json::from_str::<Value>(&def)
            .ok()
            .and_then(|v| {
                v["parts"].as_array().map(|p| {
                    p.iter()
                        .filter_map(|x| x["column"].as_str().map(str::to_owned))
                        .collect()
                })
            })
            .unwrap_or_default();
        facts.auto_indexes.push(AutoIndex {
            db,
            table,
            name,
            columns,
            state,
        });
    }

    // Recommendations waiting for review: kind, target and age only.
    let waiting: Option<Vec<(String, String, String, i64)>> = rows(
        c,
        format!(
            "SELECT kind, target, state, created_at_us FROM {s}.recommendations \
             WHERE state IN ('open', 'snoozed', 'approved') ORDER BY created_at_us LIMIT {MAX_WAITING}"
        ),
        (),
    )
    .await?;
    if let Some(w) = waiting {
        #[allow(clippy::cast_precision_loss)]
        {
            facts.waiting = w
                .into_iter()
                .map(|(kind, target, state, created)| Waiting {
                    kind,
                    target,
                    state,
                    age_hours: ((now_ms * 1000 - created).max(0) as f64) / 3.6e9,
                })
                .collect();
        }
        let n: Option<u64> = c
            .exec_first(
                format!(
                    "SELECT COUNT(*) FROM {s}.recommendations \
                     WHERE state IN ('open', 'snoozed', 'approved')"
                ),
                (),
            )
            .await
            .map_err(unavailable)?;
        facts.waiting_count = n.unwrap_or(0);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn datetimes_are_utc() {
        assert_eq!(super::at(1_794_319_391_000), "2026-11-10 14:03:11");
    }
}
