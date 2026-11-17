//! Statement digests on port 3306, from the workload capture:
//! `events_statements_summary_by_digest` and the `sys` views over it,
//! `statement_analysis` and `statements_with_errors_or_warnings` (with their
//! `x$` forms).
//!
//! The rows reach MariaDB as one `JSON_TABLE` over a single JSON literal,
//! at most MySQL's 10,000 (the most recently seen) with texts of at most
//! 1,024 bytes, so the statement stays a few megabytes at most and quick
//! to parse.
//!
//! The gateway passes the tenant's digests ([`PsDigest`]): the workload
//! recorder keeps, beside each fingerprint's hourly statistics, its totals
//! since this gateway first counted it or since the tenant's last `TRUNCATE` of
//! the summary, MySQL's lifetime for the table. Each digest becomes one row
//! with MySQL 8.4's columns:
//!
//! - `DIGEST` is the fingerprint's id in 16 hex digits (MySQL's is a
//!   SHA-256 in 64), `DIGEST_TEXT` its normalized text (literals as `?`,
//!   `IN` lists folded, names qualified with the database).
//! - Times are in picoseconds, from the gateway's measured latency (send
//!   to end of result), so they include the network to MariaDB.
//! - `SUM_ROWS_EXAMINED` adds up only the executions the recorder probed
//!   (watched fingerprints, a sample); `QUANTILE_*` are the upper bounds
//!   of the recorder's histogram buckets.
//! - Counters MariaDB can't give per statement (lock time, temporary
//!   tables, sorts, index use, memory, warnings) are 0;
//!   `QUERY_SAMPLE_TEXT` is `NULL`: the recorder keeps no plain statement
//!   text (parameter samples are sealed).

use super::sql_string;

/// One digest row, as the gateway hands it over.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PsDigest {
    /// The logical database (`SCHEMA_NAME`), if any.
    pub schema: Option<String>,
    /// The fingerprint id, 16 hex digits.
    pub digest: String,
    /// The normalized text.
    pub text: String,
    /// Executions.
    pub count: u64,
    /// Executions that failed.
    pub errors: u64,
    /// Sum, shortest and longest latency, in microseconds.
    pub sum_us: u64,
    /// Shortest latency.
    pub min_us: u64,
    /// Longest latency.
    pub max_us: u64,
    /// Rows changed.
    pub rows_affected: u64,
    /// Rows sent.
    pub rows_sent: u64,
    /// Rows examined by the probed executions.
    pub rows_examined: u64,
    /// First and last execution, microseconds since the epoch.
    pub first_seen_us: u64,
    /// Last execution.
    pub last_seen_us: u64,
    /// Latency quantiles 0.95, 0.99 and 0.999, in microseconds.
    pub p95_us: u64,
    /// 0.99.
    pub p99_us: u64,
    /// 0.999.
    pub p999_us: u64,
}

fn ps(us: u64) -> u64 {
    us.saturating_mul(1_000_000)
}

/// Seconds since the epoch with six places, for `FROM_UNIXTIME`.
fn secs(us: u64) -> String {
    format!("{}.{:06}", us / 1_000_000, us % 1_000_000)
}

/// MySQL 8.4's `performance_schema_digests_size`: rows the summary holds.
pub(super) const MAX_DIGEST_ROWS: usize = 10_000;

/// MySQL 8.4's `performance_schema_max_digest_length`: bytes of
/// `DIGEST_TEXT`. MySQL stops at a token boundary, so a longer text is cut
/// at its last space within the limit.
pub(super) const MAX_DIGEST_TEXT: usize = 1024;

fn capped_text(t: &str) -> &str {
    if t.len() <= MAX_DIGEST_TEXT {
        return t;
    }
    let mut end = MAX_DIGEST_TEXT;
    while !t.is_char_boundary(end) {
        end -= 1;
    }
    let cut = &t[..end];
    cut.rfind(' ').map_or(cut, |i| &cut[..i])
}

/// One row: each column of `cols` (`NAME:CAST;...`) from `values` by name,
/// else 0 for a number and `NULL` for anything else.
fn row(cols: &str, values: &[(&str, String)]) -> String {
    let items: Vec<String> = cols
        .split(';')
        .filter_map(|c| c.split_once(':'))
        .map(|(n, ty)| {
            let v = values
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(n))
                .map_or_else(
                    || {
                        if ty == "UNSIGNED" || ty == "SIGNED" || ty.starts_with("DECIMAL") {
                            "0".to_owned()
                        } else {
                            "NULL".to_owned()
                        }
                    },
                    |(_, v)| v.clone(),
                );
            format!("CAST({v} AS {ty}) AS `{n}`")
        })
        .collect();
    format!("SELECT {}", items.join(", "))
}

/// The alias of the digests' `JSON_TABLE`.
const DG: &str = "`_sprig_dg`";

/// Bytes of JSON the digests' literal may hold. Quoting it as an SQL
/// string at most doubles it, so the statement stays under MariaDB's
/// default `max_allowed_packet` of 16 MiB. Ten thousand digests with
/// texts near [`MAX_DIGEST_TEXT`] (the document engine's statements are
/// that long) passed it, and the read failed with 1153.
pub(super) const MAX_DIGEST_JSON: usize = 6 << 20;

/// The digests as one `JSON_TABLE` over a single string literal: the
/// statement stays small and quick to parse whatever the number of
/// digests (a `UNION ALL` of one `SELECT` per digest took the gateway and
/// MariaDB seconds at a few thousand, and passed `max_allowed_packet` at
/// about ten thousand). At most [`MAX_DIGEST_ROWS`] and
/// [`MAX_DIGEST_JSON`] bytes, the most recently seen first; texts cut at
/// [`MAX_DIGEST_TEXT`] bytes.
fn digest_table(digests: &[&PsDigest], nbe: bool) -> String {
    let mut ds: Vec<&PsDigest> = digests.to_vec();
    ds.sort_by(|a, b| b.last_seen_us.cmp(&a.last_seen_us));
    ds.truncate(MAX_DIGEST_ROWS);
    let rows = ds.iter().map(|d| {
        serde_json::json!({
                "s": d.schema,
                "d": d.digest,
                "t": capped_text(&d.text),
                "n": d.count,
                "e": d.errors,
                "su": ps(d.sum_us),
                "mi": ps(d.min_us),
                "av": ps(d.sum_us.checked_div(d.count).unwrap_or(0)),
                "mx": ps(d.max_us),
                "ra": d.rows_affected,
                "rs": d.rows_sent,
                "rx": d.rows_examined,
                "fs": secs(d.first_seen_us),
                "ls": secs(d.last_seen_us),
                "q95": ps(d.p95_us),
                "q99": ps(d.p99_us),
                "q999": ps(d.p999_us),
        })
        .to_string()
    });
    let mut json = String::from("[");
    for r in rows {
        if json.len() + r.len() + 2 > MAX_DIGEST_JSON {
            break;
        }
        if json.len() > 1 {
            json.push(',');
        }
        json.push_str(&r);
    }
    json.push(']');
    let num = |k: &str| format!("{k} DECIMAL(20,0) PATH '$.{k}'");
    let mut cols = vec![
        "s VARCHAR(64) PATH '$.s'".to_owned(),
        "d VARCHAR(64) PATH '$.d'".to_owned(),
        "t LONGTEXT PATH '$.t'".to_owned(),
    ];
    cols.extend(
        [
            "n", "e", "su", "mi", "av", "mx", "ra", "rs", "rx", "q95", "q99", "q999",
        ]
        .iter()
        .map(|k| num(k)),
    );
    cols.push("fs DECIMAL(20,6) PATH '$.fs'".to_owned());
    cols.push("ls DECIMAL(20,6) PATH '$.ls'".to_owned());
    format!(
        " FROM JSON_TABLE({}, '$[*]' COLUMNS ({})) AS {DG}",
        sql_string(&json, nbe),
        cols.join(", ")
    )
}

/// A column of the digests' table.
fn c(k: &str) -> String {
    format!("{DG}.`{k}`")
}

fn time(k: &str) -> String {
    format!("FROM_UNIXTIME({})", c(k))
}

/// The computed rows of `obj` (lower case) in `schema` (`performance_schema`
/// or `sys`), whose columns are `cols`; `None` when it isn't a digest
/// table or there are no digests (the caller then gives the empty table).
#[allow(clippy::too_many_lines)] // one arm per table
pub(super) fn object(
    schema: &str,
    obj: &str,
    cols: &str,
    digests: &[PsDigest],
    nbe: bool,
) -> Option<String> {
    let all: Vec<&PsDigest> = digests.iter().collect();
    let per_exec = |k: &str| format!("ROUND({} / GREATEST({}, 1))", c(k), c("n"));
    let sql = match (schema, obj) {
        ("performance_schema", "events_statements_summary_by_digest") => {
            if all.is_empty() {
                return None;
            }
            let select = row(
                cols,
                &[
                    ("SCHEMA_NAME", c("s")),
                    ("DIGEST", c("d")),
                    ("DIGEST_TEXT", c("t")),
                    ("COUNT_STAR", c("n")),
                    ("SUM_TIMER_WAIT", c("su")),
                    ("MIN_TIMER_WAIT", c("mi")),
                    ("AVG_TIMER_WAIT", c("av")),
                    ("MAX_TIMER_WAIT", c("mx")),
                    ("SUM_ERRORS", c("e")),
                    ("SUM_ROWS_AFFECTED", c("ra")),
                    ("SUM_ROWS_SENT", c("rs")),
                    ("SUM_ROWS_EXAMINED", c("rx")),
                    ("FIRST_SEEN", time("fs")),
                    ("LAST_SEEN", time("ls")),
                    ("QUANTILE_95", c("q95")),
                    ("QUANTILE_99", c("q99")),
                    ("QUANTILE_999", c("q999")),
                ],
            );
            select + &digest_table(&all, nbe)
        }
        ("sys", "statement_analysis" | "x$statement_analysis") => {
            if all.is_empty() {
                return None;
            }
            let x = obj.starts_with("x$");
            let lat = |k: &str| {
                if x {
                    c(k)
                } else {
                    super::format_pico_time(&c(k))
                }
            };
            let query = if x {
                c("t")
            } else {
                let v = c("t");
                format!(
                    "(CASE WHEN CHAR_LENGTH({v}) > 64 THEN CONCAT(LEFT({v}, 30), ' ... ', RIGHT({v}, 30)) ELSE {v} END)"
                )
            };
            let zero_lat = if x {
                "0".to_owned()
            } else {
                super::format_pico_time("0")
            };
            let mem = if x {
                "0".to_owned()
            } else {
                "'   0 bytes'".to_owned()
            };
            let select = row(
                cols,
                &[
                    ("query", query),
                    ("db", c("s")),
                    ("full_scan", "''".to_owned()),
                    ("exec_count", c("n")),
                    ("err_count", c("e")),
                    ("total_latency", lat("su")),
                    ("max_latency", lat("mx")),
                    ("avg_latency", lat("av")),
                    ("lock_latency", zero_lat.clone()),
                    ("cpu_latency", zero_lat),
                    ("rows_sent", c("rs")),
                    ("rows_sent_avg", per_exec("rs")),
                    ("rows_examined", c("rx")),
                    ("rows_examined_avg", per_exec("rx")),
                    ("rows_affected", c("ra")),
                    ("rows_affected_avg", per_exec("ra")),
                    ("max_controlled_memory", mem.clone()),
                    ("max_total_memory", mem),
                    ("digest", c("d")),
                    ("first_seen", time("fs")),
                    ("last_seen", time("ls")),
                ],
            );
            select + &digest_table(&all, nbe)
        }
        ("sys", "statements_with_errors_or_warnings" | "x$statements_with_errors_or_warnings") => {
            let failed: Vec<&PsDigest> = all.into_iter().filter(|d| d.errors > 0).collect();
            if failed.is_empty() {
                return None;
            }
            let x = obj.starts_with("x$");
            let query = if x {
                c("t")
            } else {
                let v = c("t");
                format!(
                    "(CASE WHEN CHAR_LENGTH({v}) > 64 THEN CONCAT(LEFT({v}, 30), ' ... ', RIGHT({v}, 30)) ELSE {v} END)"
                )
            };
            let select = row(
                cols,
                &[
                    ("query", query),
                    ("db", c("s")),
                    ("exec_count", c("n")),
                    ("errors", c("e")),
                    (
                        "error_pct",
                        format!(
                            "IF({n} = 0, 0, ROUND(100 * {e} / {n}, 4))",
                            n = c("n"),
                            e = c("e")
                        ),
                    ),
                    ("digest", c("d")),
                    ("first_seen", time("fs")),
                    ("last_seen", time("ls")),
                ],
            );
            select + &digest_table(&failed, nbe)
        }
        _ => return None,
    };
    Some(sql)
}

/// Whether `obj` (lower case) in `schema` is computed from digests.
#[must_use]
pub(super) fn is_digest_object(schema: &str, obj: &str) -> bool {
    matches!(
        (schema, obj),
        ("performance_schema", "events_statements_summary_by_digest")
            | (
                "sys",
                "statement_analysis"
                    | "x$statement_analysis"
                    | "statements_with_errors_or_warnings"
                    | "x$statements_with_errors_or_warnings"
            )
    )
}

/// The text of a statement that empties the digest summary: `TRUNCATE
/// [TABLE] performance_schema.events_statements_summary_by_digest` (or
/// unqualified with `performance_schema` current).
#[must_use]
pub fn is_truncate_digests(sql: &[u8], current_db: Option<&str>) -> bool {
    let text = String::from_utf8_lossy(sql);
    let words: Vec<String> = text
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|w| w.replace('`', "").to_ascii_lowercase())
        .collect();
    let rest = match words.as_slice() {
        [t, tb, r] if t == "truncate" && tb == "table" => r,
        [t, r] if t == "truncate" => r,
        _ => return false,
    };
    match rest.split_once('.') {
        Some((db, t)) => db == "performance_schema" && t == "events_statements_summary_by_digest",
        None => {
            rest == "events_statements_summary_by_digest"
                && current_db.is_some_and(|d| d.eq_ignore_ascii_case("performance_schema"))
        }
    }
}
