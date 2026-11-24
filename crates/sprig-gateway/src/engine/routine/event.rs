//! SQL events on engine databases: a catalog object of kind `event`. Its
//! text is the routine context line, whose `event` part holds the
//! evaluated schedule (times as the session's zone printed them and as
//! Unix seconds), then the body. The body compiles like a procedure with
//! no parameters, so it runs on the routines' bytecode; the gateway's
//! scheduler (`crate::sql_events`) runs each due event in a session of its
//! own, as its definer, on the tenant's leader.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};

use super::code::{self, Program};
use super::ddl::{Session, default_sql_mode};
use super::parse::{self, EvStatus, EvStmt, Schedule};
use super::{Context, ident, sql_string};

/// A schedule's expressions as the creating session evaluated them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Eval {
    /// `@@time_zone`.
    pub time_zone: String,
    /// Now: as printed, and Unix seconds.
    pub now: (String, i64),
    /// `AT`'s value.
    pub at: Option<(String, i64)>,
    /// `EVERY`'s value, as text.
    pub value: Option<String>,
    /// `STARTS`'s value.
    pub starts: Option<(String, i64)>,
    /// `ENDS`'s value.
    pub ends: Option<(String, i64)>,
}

/// An event's schedule and options, as stored.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct EvDef {
    /// The creating session's time zone.
    pub time_zone: String,
    /// A one-time event's time: as printed, and Unix seconds.
    pub at: Option<(String, i64)>,
    /// A repeating event's interval: the value as printed and the unit.
    pub every: Option<(String, String)>,
    /// The interval in calendar months (`MONTH`, `QUARTER`, `YEAR`,
    /// `YEAR_MONTH`).
    pub months: i64,
    /// The interval in seconds (the other units).
    pub secs: i64,
    /// `STARTS`.
    pub starts: Option<(String, i64)>,
    /// `ENDS`.
    pub ends: Option<(String, i64)>,
    /// `ON COMPLETION PRESERVE`.
    pub preserve: bool,
    /// Its status.
    pub status: EvStatus,
    /// `COMMENT`.
    pub comment: String,
}

/// One event, as the catalog has it.
#[derive(Debug)]
pub(crate) struct Event {
    /// Its database.
    pub db: DbId,
    /// Its name as created.
    pub name: String,
    /// The creation context.
    pub cx: Context,
    /// The schedule, or why it can't be read.
    pub def: std::result::Result<EvDef, String>,
    /// The body's text.
    pub body: String,
    /// The body as a procedure with no parameters, compiled.
    pub code: std::result::Result<(parse::Create, Arc<Program>), String>,
}

/// The body of an event as a procedure with no parameters.
fn as_procedure(name: &str, body: &str, db: &str) -> Result<(parse::Create, Arc<Program>)> {
    let c = parse::parse_create(&format!("CREATE PROCEDURE {}() {body}", ident(name)))?;
    let p = code::compile(&c, db)?;
    Ok((c, Arc::new(p)))
}

/// Reads an event's catalog text.
pub(crate) fn load(db: DbId, db_name: &str, name: &str, text: &str) -> Event {
    let (cx, body) = super::split_stored(text);
    let def = cx
        .event
        .clone()
        .ok_or_else(|| "an event without its schedule".to_owned());
    Event {
        db,
        name: name.to_owned(),
        code: as_procedure(name, body, db_name).map_err(|e| e.message),
        cx,
        def,
        body: body.to_owned(),
    }
}

/// The interval of `EVERY value unit`: (months, seconds).
///
/// # Errors
/// 1235 for a unit with microseconds; 1542 for a value that isn't
/// positive or is too big.
pub(crate) fn interval(value: &str, unit: &str, mysql: bool) -> Result<(i64, i64)> {
    if unit.contains("MICROSECOND") {
        let server = if mysql { "MySQL" } else { "MariaDB" };
        return Err(SprigError::standard(
            1235,
            *b"42000",
            format!("This version of {server} doesn't yet support 'MICROSECOND'"),
        ));
    }
    let bad = || {
        SprigError::standard(
            1542,
            *b"HY000",
            "INTERVAL is either not positive or too big",
        )
    };
    let v = value.trim();
    if v.starts_with('-') {
        return Err(bad());
    }
    let nums: Vec<i64> = v
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<i64>().unwrap_or(i64::MAX / 4))
        .collect();
    // Simple units take the leading number; compound units take their
    // parts from the right.
    let parts = |n: usize| -> Vec<i64> {
        let mut p = vec![0; n];
        let take = nums.len().min(n);
        p[n - take..].copy_from_slice(&nums[..take]);
        p
    };
    let first = nums.first().copied().unwrap_or(0);
    // (months, seconds, the count in the unit's smallest part)
    let (months, secs, smallest) = match unit {
        "YEAR" => (first.saturating_mul(12), 0, first),
        "QUARTER" => (first.saturating_mul(3), 0, first),
        "MONTH" => (first, 0, first),
        "WEEK" => (0, first.saturating_mul(7 * 86_400), first),
        "DAY" => (0, first.saturating_mul(86_400), first),
        "HOUR" => (0, first.saturating_mul(3_600), first),
        "MINUTE" => (0, first.saturating_mul(60), first),
        "SECOND" => (0, first, first),
        "YEAR_MONTH" => {
            let p = parts(2);
            let m = p[0].saturating_mul(12).saturating_add(p[1]);
            (m, 0, m)
        }
        _ => {
            // Day-time units: [days, hours, minutes, seconds] from the
            // unit's first part to its last.
            let (from, to) = match unit {
                "DAY_HOUR" => (0, 1),
                "DAY_MINUTE" => (0, 2),
                "DAY_SECOND" => (0, 3),
                "HOUR_MINUTE" => (1, 2),
                "HOUR_SECOND" => (1, 3),
                "MINUTE_SECOND" => (2, 3),
                _ => return Err(bad()),
            };
            let p = parts(to - from + 1);
            let mut full = [0i64; 4];
            full[from..=to].copy_from_slice(&p);
            let secs = full[0]
                .saturating_mul(86_400)
                .saturating_add(full[1].saturating_mul(3_600))
                .saturating_add(full[2].saturating_mul(60))
                .saturating_add(full[3]);
            let per = [86_400, 3_600, 60, 1][to];
            (0, secs, secs / per)
        }
    };
    if (months <= 0 && secs <= 0) || smallest > 1_000_000_000 {
        return Err(bad());
    }
    Ok((months, secs))
}

/// The value of `EVERY` as the servers print it: a whole number bare,
/// anything else quoted.
fn printed_value(v: &str) -> String {
    let v = v.trim();
    if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) {
        v.to_owned()
    } else {
        sql_string(v)
    }
}

/// The definition `st` gives (`old`: the event's before an `ALTER`), with
/// the schedule's values `eval`.
///
/// # Errors
/// 1235, 1542, 1543, 1525.
pub(crate) fn build(
    st: &EvStmt,
    eval: Option<&Eval>,
    old: Option<&EvDef>,
    mysql: bool,
) -> Result<EvDef> {
    let missing = || SprigError::internal("an event's schedule wasn't evaluated");
    let mut def = old.cloned().unwrap_or(EvDef {
        time_zone: "SYSTEM".into(),
        at: None,
        every: None,
        months: 0,
        secs: 0,
        starts: None,
        ends: None,
        preserve: false,
        status: EvStatus::Enabled,
        comment: String::new(),
    });
    if let Some(s) = &st.schedule {
        let e = eval.ok_or_else(missing)?;
        def.time_zone.clone_from(&e.time_zone);
        let wrong = |what: &str, v: &str| {
            SprigError::standard(1525, *b"HY000", format!("Incorrect {what} value: '{v}'"))
        };
        match s {
            Schedule::At(x) => {
                def.at = Some(e.at.clone().ok_or_else(|| wrong("AT", x))?);
                def.every = None;
                def.months = 0;
                def.secs = 0;
                def.starts = None;
                def.ends = None;
            }
            Schedule::Every {
                unit, starts, ends, ..
            } => {
                let v = e.value.clone().ok_or_else(|| wrong("INTERVAL", "NULL"))?;
                let (m, sec) = interval(&v, unit, mysql)?;
                def.at = None;
                def.every = Some((printed_value(&v), unit.clone()));
                def.months = m;
                def.secs = sec;
                def.starts = Some(match starts {
                    Some(x) => e.starts.clone().ok_or_else(|| wrong("STARTS", x))?,
                    None => e.now.clone(),
                });
                def.ends = match ends {
                    Some(_) => Some(e.ends.clone().ok_or_else(|| {
                        SprigError::standard(
                            1543,
                            *b"HY000",
                            "ENDS is either invalid or before STARTS",
                        )
                    })?),
                    None => None,
                };
                if let (Some(a), Some(b)) = (&def.starts, &def.ends)
                    && b.1 <= a.1
                {
                    return Err(SprigError::standard(
                        1543,
                        *b"HY000",
                        "ENDS is either invalid or before STARTS",
                    ));
                }
            }
        }
    }
    if let Some(p) = st.preserve {
        def.preserve = p;
    }
    if let Some(s) = st.status {
        def.status = s;
    }
    if let Some(c) = &st.comment {
        def.comment.clone_from(c);
    }
    Ok(def)
}

/// The event's last run time has passed at `now`: a one-time event's
/// time, or a repeating one's `ENDS`.
pub(crate) fn past(def: &EvDef, now: i64) -> bool {
    def.at.as_ref().is_some_and(|a| a.1 < now) || def.ends.as_ref().is_some_and(|e| e.1 < now)
}

/// The catalog text of a `CREATE` or `ALTER EVENT` (`old`: the event's
/// stored text, for `ALTER`).
///
/// # Errors
/// The statement can't be read; the schedule's errors; the body's
/// creation errors.
pub(crate) fn create(
    sql: &str,
    sess: &Session,
    account: &(String, String),
    mysql: bool,
    db_cs: &(String, String),
    db_name: &str,
    old: Option<&str>,
) -> Result<String> {
    let text = sess
        .entry
        .as_deref()
        .filter(|e| parse::parse_event(e).is_ok())
        .unwrap_or(sql);
    let st = parse::parse_event(text)?;
    let old = old.map(super::split_stored);
    let mut def = build(
        &st,
        sess.event.as_ref(),
        old.as_ref().and_then(|o| o.0.event.as_ref()),
        mysql,
    )?;
    // A time already past, kept (`PRESERVE`): stored disabled.
    if let Some(e) = &sess.event
        && st.schedule.is_some()
        && past(&def, e.now.1)
        && def.status == EvStatus::Enabled
    {
        def.status = EvStatus::Disabled;
    }
    let body = match (&st.body_text, &old) {
        (Some(b), _) => b.clone(),
        (None, Some((_, b))) => (*b).to_owned(),
        (None, None) => return Err(SprigError::internal("an event without a body")),
    };
    as_procedure(&st.name, &body, db_name)?;
    let definer = match &st.definer {
        Some((u, h)) if !u.is_empty() || !h.is_empty() => (u.clone(), h.clone()),
        _ => account.clone(),
    };
    let dflt = if mysql {
        "utf8mb4_0900_ai_ci"
    } else {
        "utf8mb4_uca1400_ai_ci"
    };
    let or = |s: &str, d: &str| {
        if s.is_empty() {
            d.to_owned()
        } else {
            s.to_owned()
        }
    };
    let now = sess
        .event
        .as_ref()
        .map_or_else(|| sess.now.clone(), |e| e.now.0.clone());
    let cx = match old {
        Some((mut cx, _)) => {
            cx.modified = now;
            cx.definer = definer;
            cx.event = Some(def);
            cx
        }
        None => Context {
            sql_mode: or(&sess.sql_mode, default_sql_mode(mysql)),
            charset_client: or(&sess.charset_client, "utf8mb4"),
            collation_connection: or(&sess.collation_connection, dflt),
            db_collation: or(&db_cs.1, dflt),
            created: now.clone(),
            modified: now,
            definer,
            order: 0,
            event: Some(def),
        },
    };
    Ok(super::stored(&cx, &body))
}

/// The status as `SHOW EVENTS` prints it.
pub(crate) fn status_word(s: EvStatus, mysql: bool) -> &'static str {
    match s {
        EvStatus::Enabled => "ENABLED",
        EvStatus::Disabled => "DISABLED",
        EvStatus::SlaveDisabled if mysql => "REPLICA_SIDE_DISABLED",
        EvStatus::SlaveDisabled => "SLAVESIDE_DISABLED",
    }
}

/// `SHOW CREATE EVENT`'s statement.
pub(crate) fn show_create(e: &Event, def: &EvDef, mysql: bool) -> String {
    let mut s = format!(
        "CREATE DEFINER={}@{} EVENT {} ON SCHEDULE ",
        ident(&e.cx.definer.0),
        ident(&e.cx.definer.1),
        ident(&e.name)
    );
    if let Some((at, _)) = &def.at {
        let _ = write!(s, "AT {}", sql_string(at));
    } else if let Some((v, u)) = &def.every {
        let _ = write!(s, "EVERY {v} {u}");
        if let Some((st, _)) = &def.starts {
            let _ = write!(s, " STARTS {}", sql_string(st));
        }
        if let Some((en, _)) = &def.ends {
            let _ = write!(s, " ENDS {}", sql_string(en));
        }
    }
    s.push_str(if def.preserve {
        " ON COMPLETION PRESERVE "
    } else {
        " ON COMPLETION NOT PRESERVE "
    });
    s.push_str(match def.status {
        EvStatus::Enabled => "ENABLE",
        EvStatus::Disabled => "DISABLE",
        EvStatus::SlaveDisabled if mysql => "DISABLE ON REPLICA",
        EvStatus::SlaveDisabled => "DISABLE ON SLAVE",
    });
    if !def.comment.is_empty() {
        s.push_str(" COMMENT ");
        s.push_str(&sql_string(&def.comment));
    }
    s.push_str(" DO ");
    s.push_str(&e.body);
    s
}

// ---- when an event runs ----

/// Days from 1970-01-01 to a civil date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// `YYYY-MM-DD hh:mm:ss` as its parts.
fn parts(s: &str) -> Option<[i64; 6]> {
    let n: Vec<i64> = s
        .split(|c: char| !c.is_ascii_digit())
        .filter(|x| !x.is_empty())
        .take(6)
        .map(|x| x.parse().ok())
        .collect::<Option<_>>()?;
    let mut p = [0i64; 6];
    for (i, v) in n.iter().enumerate() {
        p[i] = *v;
    }
    (n.len() >= 3).then_some(p)
}

/// Seconds of a civil time counted as if it were UTC.
fn naive(p: &[i64; 6]) -> i64 {
    days_from_civil(p[0], p[1], p[2]) * 86_400 + p[3] * 3_600 + p[4] * 60 + p[5]
}

/// The `k`-th run of a repeating event, in Unix seconds: `STARTS + k ×
/// interval`, months added on the calendar of `STARTS` (the day clamped
/// to shorter months), at `STARTS`'s offset from UTC.
fn run_k(def: &EvDef, k: i64) -> Option<i64> {
    let (st_text, st) = def.starts.as_ref()?;
    if def.months == 0 {
        return Some(st + k.checked_mul(def.secs)?);
    }
    let p = parts(st_text)?;
    let total = p[0] * 12 + (p[1] - 1) + k.checked_mul(def.months)?;
    let (y, m) = (total.div_euclid(12), total.rem_euclid(12) + 1);
    let d = p[2].min(days_in_month(y, m));
    let q = [y, m, d, p[3], p[4], p[5]];
    Some(st + (naive(&q) - naive(&p)))
}

/// The latest run time at or before `now` (`None`: none yet, or past
/// `ENDS`).
fn latest(def: &EvDef, now: i64) -> Option<i64> {
    if let Some((_, at)) = def.at {
        return (at <= now).then_some(at);
    }
    let st = def.starts.as_ref()?.1;
    if now < st {
        return None;
    }
    let step = if def.months > 0 {
        def.months * 2_629_746
    } else {
        def.secs.max(1)
    };
    let mut k = (now - st) / step;
    while run_k(def, k + 1).is_some_and(|t| t <= now) {
        k += 1;
    }
    while k > 0 && run_k(def, k).is_none_or(|t| t > now) {
        k -= 1;
    }
    let mut t = run_k(def, k)?;
    if let Some((_, end)) = def.ends {
        while t > end && k > 0 {
            k -= 1;
            t = run_k(def, k)?;
        }
        if t > end {
            return None;
        }
    }
    Some(t)
}

/// The run due at `now`, if any: the latest run time after `last` (the
/// time of the last run, or the earliest time the scheduler may still
/// catch up from). Missed runs aren't caught up: only the latest counts.
pub(crate) fn due(def: &EvDef, last: i64, now: i64) -> Option<i64> {
    if def.status != EvStatus::Enabled {
        return None;
    }
    latest(def, now).filter(|t| *t > last)
}

/// The event has no runs left after `now`: on completion it's dropped
/// (or disabled with `PRESERVE`).
pub(crate) fn finished(def: &EvDef, now: i64) -> bool {
    match (&def.at, &def.ends) {
        (Some((_, at)), _) => *at <= now,
        (None, Some((_, end))) => {
            let next = latest(def, now).map_or(def.starts.as_ref().map_or(0, |s| s.1), |t| t);
            *end < now && next <= *end
        }
        _ => false,
    }
}

/// When each event last ran in this process, by (tenant, database,
/// lower-case name): `LAST_EXECUTED`, as printed.
static LAST_RUN: Mutex<Option<LastRuns>> = Mutex::new(None);

/// Last run times by (tenant, database, lower-case name).
type LastRuns = HashMap<(u64, DbId, String), String>;

/// Records a run of an event.
pub(crate) fn ran(tenant: u64, db: DbId, name: &str, at: String) {
    let mut g = LAST_RUN.lock().unwrap_or_else(PoisonError::into_inner);
    g.get_or_insert_with(HashMap::new)
        .insert((tenant, db, name.to_lowercase()), at);
}

/// When an event last ran in this process.
pub(crate) fn last_run(tenant: u64, db: DbId, name: &str) -> Option<String> {
    let g = LAST_RUN.lock().unwrap_or_else(PoisonError::into_inner);
    g.as_ref()?.get(&(tenant, db, name.to_lowercase())).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every(starts: (&str, i64), months: i64, secs: i64) -> EvDef {
        EvDef {
            time_zone: "SYSTEM".into(),
            at: None,
            every: Some(("1".into(), "MONTH".into())),
            months,
            secs,
            starts: Some((starts.0.into(), starts.1)),
            ends: None,
            preserve: false,
            status: EvStatus::Enabled,
            comment: String::new(),
        }
    }

    #[test]
    fn intervals_read_as_the_servers_read_them() {
        assert_eq!(interval("2", "HOUR", false).ok(), Some((0, 7_200)));
        assert_eq!(interval("1:30", "HOUR_MINUTE", false).ok(), Some((0, 5_400)));
        assert_eq!(interval("1-2", "YEAR_MONTH", false).ok(), Some((14, 0)));
        assert_eq!(interval("3", "QUARTER", false).ok(), Some((9, 0)));
        assert_eq!(interval("1 2:03:04", "DAY_SECOND", false).ok(), Some((0, 93_784)));
        assert!(interval("0", "DAY", false).is_err_and(|e| e.message.contains("not positive")));
        assert!(interval("1", "MICROSECOND", true).is_err_and(|e| e.message.contains("MySQL")));
        assert_eq!(printed_value("2"), "2");
        assert_eq!(printed_value("1:30"), "'1:30'");
    }

    #[test]
    fn runs_follow_starts_and_skip_missed_ones() {
        // 2030-01-31 00:00:00 UTC, monthly: Feb clamps to the 28th, March
        // is the 31st again.
        let st = days_from_civil(2030, 1, 31) * 86_400;
        let d = every(("2030-01-31 00:00:00", st), 1, 0);
        let feb = days_from_civil(2030, 2, 28) * 86_400;
        let mar = days_from_civil(2030, 3, 31) * 86_400;
        assert_eq!(latest(&d, feb + 5), Some(feb));
        assert_eq!(latest(&d, mar + 5), Some(mar));
        assert_eq!(due(&d, feb, mar - 1), None);
        assert_eq!(due(&d, feb, mar), Some(mar));
        // Every hour: after a long gap only the latest run is due.
        let h = every(("x", 1_000), 0, 3_600);
        assert_eq!(due(&h, 1_000, 1_000 + 5 * 3_600 + 7), Some(1_000 + 5 * 3_600));
        assert_eq!(latest(&h, 999), None);
        let mut e = h.clone();
        e.ends = Some(("y".into(), 1_000 + 2 * 3_600));
        assert_eq!(latest(&e, 1_000 + 9 * 3_600), Some(1_000 + 2 * 3_600));
        assert!(finished(&e, 1_000 + 9 * 3_600));
        assert!(!finished(&h, 1_000 + 9 * 3_600));
    }
}
