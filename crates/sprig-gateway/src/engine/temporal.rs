//! Dates and times as statements give them: read from text and from
//! numbers, checked against the calendar and the session's modes, moved
//! between the session's time zone and UTC, and printed (rules observed
//! from MariaDB 11.8 and MySQL 8.4 as black boxes).
//!
//! **Text as a date and time.** After white space, either delimited
//! fields (`2024-2-9 1:2:3.5`, any punctuation between fields, a space or
//! `T` before the time, a two-digit year meaning 1970 to 2069) or digits
//! alone (`YYMMDD`, `YYYYMMDD`, `YYMMDDhhmmss`, `YYYYMMDDhhmmss`, the time
//! optionally after a `T`, a fraction after a point). A date needs its
//! year, month and day; the time's missing fields are zero.
//!
//! **A number as a date and time.** 0 is the zero date; 101 to 991231 is
//! `YYMMDD`; 10000101 to 99991231 `YYYYMMDD`; 101000000 to 991231235959
//! `YYMMDDhhmmss`; from there to 99991231235959 `YYYYMMDDhhmmss`.
//!
//! **Text as a time.** `[-][D ]hh:mm[:ss[.f]]`, or digits alone read from
//! the right (`1234` is 00:12:34), or a date and time, whose time of day
//! it is. A number is `hhmmss[.f]`.
//!
//! **Fractions.** Up to six digits are kept; a column of fewer cuts the
//! rest (MariaDB) or rounds half up, carrying as far as it must (MySQL,
//! and MariaDB's `TIME_ROUND_FRACTIONAL`).

use sprig_engine::row::{Date, DateTime, Time, Timestamp};

use super::assign::{Env, NumPrefix};

/// A date and time read from text or a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Read {
    /// The fields, with up to six fraction digits.
    pub dt: DateTime,
    /// A time of day was given.
    pub has_time: bool,
    /// The seventh fraction digit (what rounding to six looks at).
    pub seventh: u8,
    /// More than six fraction digits were given.
    pub long_fraction: bool,
    /// Other text followed the value.
    pub garbage: bool,
}

/// A time read from text or a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadTime {
    /// The signed duration in microseconds; it may be beyond a `TIME`'s
    /// range.
    pub micros: i64,
    /// The seventh fraction digit.
    pub seventh: u8,
    /// More than six fraction digits were given.
    pub long_fraction: bool,
    /// Other text followed the value.
    pub garbage: bool,
    /// The value was a date and time, of which this is the time of day.
    pub from_datetime: bool,
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// Reads up to `max` digits at `*i`; `None` when there is none.
fn digits(s: &[u8], i: &mut usize, max: usize) -> Option<(u32, usize)> {
    let start = *i;
    let mut v = 0u32;
    while *i < s.len() && *i - start < max && s[*i].is_ascii_digit() {
        v = v * 10 + u32::from(s[*i] - b'0');
        *i += 1;
    }
    (*i > start).then_some((v, *i - start))
}

/// Reads a fraction's digits at `*i` (after its point): microseconds, the
/// seventh digit, and whether there were more than six.
fn fraction(s: &[u8], i: &mut usize) -> (u32, u8, bool) {
    let mut micros = 0u32;
    let mut n = 0usize;
    let mut seventh = 0u8;
    while *i < s.len() && s[*i].is_ascii_digit() {
        let d = s[*i] - b'0';
        if n < 6 {
            micros = micros * 10 + u32::from(d);
        } else if n == 6 {
            seventh = d;
        }
        n += 1;
        *i += 1;
    }
    for _ in n..6 {
        micros *= 10;
    }
    (micros, seventh, n > 6)
}

/// A two-digit year: 70 to 99 are 1970 to 1999, 0 to 69 are 2000 to 2069.
fn two_digit_year(y: u32) -> u32 {
    if y >= 70 { 1900 + y } else { 2000 + y }
}

fn build(
    fields: [u32; 6],
    micros: u32,
    has_time: bool,
    seventh: u8,
    long_fraction: bool,
    garbage: bool,
) -> Option<Read> {
    let [y, mo, d, h, mi, sec] = fields;
    if y > 9999 || mo > 12 || d > 31 || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    // The checks above keep every field inside its type.
    #[allow(clippy::cast_possible_truncation)]
    let dt = DateTime::new(
        Date::new(y as u16, mo as u8, d as u8),
        h as u8,
        mi as u8,
        sec as u8,
        micros,
    );
    Some(Read {
        dt,
        has_time,
        seventh,
        long_fraction,
        garbage,
    })
}

/// Reads `s` as a date with an optional time; `None` when it is neither.
pub(crate) fn datetime_from_str(s: &[u8]) -> Option<Read> {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let run = s[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    let after = s.get(i + run).copied();
    let delimited = matches!(run, 1 | 2 | 4)
        && after.is_some_and(|b| b.is_ascii_punctuation())
        && s.get(i + run + 1).is_some_and(u8::is_ascii_digit);
    let mut f = [0u32; 6];
    let mut has_time = false;
    let (mut micros, mut seventh, mut long) = (0, 0, false);
    if delimited {
        // Year, month, day.
        for (k, slot) in f.iter_mut().enumerate().take(3) {
            let (v, n) = digits(s, &mut i, if k == 0 { 4 } else { 2 })?;
            *slot = if k == 0 && n <= 2 {
                two_digit_year(v)
            } else {
                v
            };
            if k < 2 {
                if !s.get(i).is_some_and(u8::is_ascii_punctuation) {
                    return None;
                }
                i += 1;
            }
        }
        // The time, after spaces or a `T`.
        let mut j = i;
        if s.get(j) == Some(&b'T') {
            j += 1;
        } else {
            while j < s.len() && is_space(s[j]) {
                j += 1;
            }
        }
        if j > i && s.get(j).is_some_and(u8::is_ascii_digit) {
            i = j;
            has_time = true;
            // `k` is the field, and says which one a fraction may follow.
            #[allow(clippy::needless_range_loop)]
            for k in 3..6 {
                let Some((v, _)) = digits(s, &mut i, 2) else {
                    break;
                };
                f[k] = v;
                if k == 5 {
                    if s.get(i) == Some(&b'.') && s.get(i + 1).is_some_and(u8::is_ascii_digit) {
                        i += 1;
                        (micros, seventh, long) = fraction(s, &mut i);
                    }
                } else if s.get(i).is_some_and(u8::is_ascii_punctuation)
                    && s.get(i + 1).is_some_and(u8::is_ascii_digit)
                {
                    i += 1;
                } else {
                    break;
                }
            }
        }
    } else {
        // Digits alone: the date's length says where the year ends.
        let (date_len, time_len) = match run {
            6 | 8 => (run, 0),
            12 => (6, 6),
            14 => (8, 6),
            _ => return None,
        };
        let ylen = date_len - 4;
        let (y, _) = digits(s, &mut i, ylen)?;
        f[0] = if ylen == 2 { two_digit_year(y) } else { y };
        f[1] = digits(s, &mut i, 2)?.0;
        f[2] = digits(s, &mut i, 2)?.0;
        let mut tl = time_len;
        if tl == 0
            && s.get(i) == Some(&b'T')
            && s[i + 1..].iter().take_while(|b| b.is_ascii_digit()).count() == 6
        {
            i += 1;
            tl = 6;
        }
        if tl == 6 {
            has_time = true;
            f[3] = digits(s, &mut i, 2)?.0;
            f[4] = digits(s, &mut i, 2)?.0;
            f[5] = digits(s, &mut i, 2)?.0;
            if s.get(i) == Some(&b'.') {
                i += 1;
                (micros, seventh, long) = fraction(s, &mut i);
            }
        }
    }
    let garbage = !s[i..].iter().all(|b| is_space(*b));
    build(f, micros, has_time, seventh, long, garbage)
}

/// Reads a number as a date with an optional time; `None` when it isn't
/// one. A fraction is the seconds' (and is left on a date alone, for the
/// caller to judge: `fraction_on_date`).
pub(crate) fn datetime_from_number(p: &NumPrefix) -> Option<Read> {
    if p.negative && !p.is_zero() {
        return None;
    }
    let n = i64::try_from(p.digits.len()).ok()?;
    if p.point < 0 || p.point > 14 {
        return (p.is_zero()).then(|| Read {
            dt: DateTime::default(),
            has_time: false,
            seventh: 0,
            long_fraction: false,
            garbage: false,
        });
    }
    let at = |k: i64| -> u32 {
        if k >= 0 && k < n {
            // k is inside the digits.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let d = p.digits[k as usize];
            u32::from(d)
        } else {
            0
        }
    };
    let mut whole: u64 = 0;
    for k in 0..p.point {
        whole = whole * 10 + u64::from(at(k));
    }
    let mut micros = 0u32;
    for k in 0..6 {
        micros = micros * 10 + at(p.point + k);
    }
    // One digit: it fits.
    #[allow(clippy::cast_possible_truncation)]
    let seventh = at(p.point + 6) as u8;
    let long_fraction = (p.point + 6..n).any(|k| at(k) != 0);
    let (date, time, has_time) = match whole {
        0 => (0, 0, false),
        101..=691_231 => (whole + 20_000_000, 0, false),
        700_101..=991_231 => (whole + 19_000_000, 0, false),
        10_000_101..=99_991_231 => (whole, 0, false),
        101_000_000..=691_231_235_959 => (whole / 1_000_000 + 20_000_000, whole % 1_000_000, true),
        700_101_000_000..=991_231_235_959 => {
            (whole / 1_000_000 + 19_000_000, whole % 1_000_000, true)
        }
        1_000_101_000_000..=99_991_231_235_959 => (whole / 1_000_000, whole % 1_000_000, true),
        _ => return None,
    };
    // Every part is below 10^4.
    #[allow(clippy::cast_possible_truncation)]
    let f = [
        (date / 10_000) as u32,
        (date / 100 % 100) as u32,
        (date % 100) as u32,
        (time / 10_000) as u32,
        (time / 100 % 100) as u32,
        (time % 100) as u32,
    ];
    build(f, micros, has_time, seventh, long_fraction, false)
}

/// The days of month `m` of year `y`.
fn days_in_month(y: u16, m: u8) -> u8 {
    match m {
        4 | 6 | 9 | 11 => 30,
        2 if (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400) => 29,
        2 => 28,
        _ => 31,
    }
}

/// Whether the session's modes let `d` be stored: a real calendar day
/// (unless `ALLOW_INVALID_DATES`), the zero date unless `NO_ZERO_DATE`, a
/// zero month or day unless `NO_ZERO_IN_DATE`.
pub(crate) fn date_ok(d: Date, env: &Env) -> bool {
    if d.year == 0 && d.month == 0 && d.day == 0 {
        return !(env.no_zero_date && env.strict);
    }
    if d.month == 0 || d.day == 0 {
        return !(env.no_zero_in_date && env.strict);
    }
    env.allow_invalid_dates || d.day <= days_in_month(d.year, d.month)
}

/// Days from 1970-01-01 to the civil date (proleptic Gregorian).
pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day count from 1970-01-01.
pub(crate) fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// The seconds from 1970-01-01 00:00:00 of a real calendar date and time,
/// taken as UTC.
fn epoch_seconds(dt: DateTime) -> i64 {
    days_from_civil(
        i64::from(dt.date.year),
        i64::from(dt.date.month),
        i64::from(dt.date.day),
    ) * 86_400
        + i64::from(dt.hour) * 3600
        + i64::from(dt.minute) * 60
        + i64::from(dt.second)
}

/// The date and time `secs` seconds from 1970-01-01 00:00:00.
fn from_epoch(secs: i64, micros: u32) -> Option<DateTime> {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    if !(0..=9999).contains(&y) {
        return None;
    }
    // The ranges above and the calendar keep each field inside its type.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let dt = DateTime::new(
        Date::new(y as u16, m as u8, d as u8),
        (sod / 3600) as u8,
        (sod / 60 % 60) as u8,
        (sod % 60) as u8,
        micros,
    );
    Some(dt)
}

/// A date and time at a column's precision `fsp`: the fraction cut, or
/// rounded half up when `round` (carrying into the seconds and as far as
/// the year). `None` when rounding leaves the calendar (past year 9999, or
/// out of a date with a zero month or day).
pub(crate) fn settle(r: &Read, fsp: u8, round: bool) -> Option<DateTime> {
    let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
    let mut dt = r.dt;
    let rest = dt.micros % unit;
    dt.micros -= rest;
    let up = round
        && if fsp >= 6 {
            r.seventh >= 5
        } else {
            rest * 2 >= unit
        };
    if !up {
        return Some(dt);
    }
    dt.micros += unit;
    if dt.micros < 1_000_000 {
        return Some(dt);
    }
    dt.micros -= 1_000_000;
    if dt.date.month == 0 || dt.date.day == 0 {
        // No calendar to carry through.
        if dt.second < 59 {
            dt.second += 1;
            return Some(dt);
        }
        return None;
    }
    from_epoch(epoch_seconds(dt) + 1, dt.micros)
}

/// A session-local date and time as a UTC timestamp, at the session's
/// zone offset `tz` (seconds east). `None` outside the type's range
/// (1970-01-01 00:00:01 UTC to 2106 on MariaDB, to 2038 on MySQL); the
/// zero date and time is the zero timestamp.
pub(crate) fn to_timestamp(dt: DateTime, tz: i32, mysql: bool) -> Option<Timestamp> {
    if dt == DateTime::default() {
        return Some(Timestamp::default());
    }
    if dt.date.month == 0 || dt.date.day == 0 {
        return None;
    }
    let secs = epoch_seconds(dt) - i64::from(tz);
    let max = if mysql {
        i64::from(i32::MAX)
    } else {
        i64::from(u32::MAX)
    };
    if secs < 1 || secs > max {
        return None;
    }
    Some(Timestamp::new(u32::try_from(secs).ok()?, dt.micros))
}

/// A UTC timestamp as the session-local date and time.
pub(crate) fn from_timestamp(t: Timestamp, tz: i32) -> DateTime {
    if t.is_zero() {
        return DateTime::default();
    }
    from_epoch(i64::from(t.seconds) + i64::from(tz), t.micros).unwrap_or_default()
}

/// Reads `s` as a time; `None` when it isn't one.
pub(crate) fn time_from_str(s: &[u8]) -> Option<ReadTime> {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    // A date and time: its time of day. A date alone is no time.
    let run = s[i..].iter().take_while(|b| b.is_ascii_digit()).count();
    let dated =
        (matches!(run, 1 | 2 | 4) && s.get(i + run) == Some(&b'-')) || matches!(run, 12 | 14);
    if dated {
        let r = datetime_from_str(s)?;
        if !r.has_time {
            return None;
        }
        let secs =
            i64::from(r.dt.hour) * 3600 + i64::from(r.dt.minute) * 60 + i64::from(r.dt.second);
        return Some(ReadTime {
            micros: secs * 1_000_000 + i64::from(r.dt.micros),
            seventh: r.seventh,
            long_fraction: r.long_fraction,
            garbage: r.garbage,
            from_datetime: true,
        });
    }
    let negative = s.get(i) == Some(&b'-');
    if negative {
        i += 1;
    }
    let (first, n) = digits(s, &mut i, 9)?;
    let (mut days, mut h, mut m, mut sec) = (0u32, 0u32, 0u32, 0u32);
    let mut colons = false;
    // `D hh:mm...`: days, when a clock with a colon follows the space.
    let mut j = i;
    while j < s.len() && s[j] == b' ' {
        j += 1;
    }
    let clock_after_space = j > i && {
        let d2 = s[j..].iter().take_while(|b| b.is_ascii_digit()).count();
        d2 > 0 && s.get(j + d2) == Some(&b':')
    };
    let mut lead = first;
    if clock_after_space {
        days = first;
        i = j;
        lead = digits(s, &mut i, 9)?.0;
    }
    if s.get(i) == Some(&b':') && s.get(i + 1).is_some_and(u8::is_ascii_digit) {
        colons = true;
        h = lead;
        i += 1;
        m = digits(s, &mut i, 2)?.0;
        if s.get(i) == Some(&b':') && s.get(i + 1).is_some_and(u8::is_ascii_digit) {
            i += 1;
            sec = digits(s, &mut i, 2)?.0;
        }
    }
    if !colons {
        // Digits alone, read from the right: hhmmss.
        if clock_after_space || n > 7 {
            return None;
        }
        h = lead / 10_000;
        m = lead / 100 % 100;
        sec = lead % 100;
    }
    let (mut micros, mut seventh, mut long) = (0, 0, false);
    if s.get(i) == Some(&b'.') {
        i += 1;
        (micros, seventh, long) = fraction(s, &mut i);
    }
    if m > 59 || sec > 59 {
        return None;
    }
    let garbage = !s[i..].iter().all(|b| is_space(*b));
    let secs = (i64::from(days) * 24 + i64::from(h)) * 3600 + i64::from(m) * 60 + i64::from(sec);
    let total = secs * 1_000_000 + i64::from(micros);
    Some(ReadTime {
        micros: if negative { -total } else { total },
        seventh,
        long_fraction: long,
        garbage,
        from_datetime: false,
    })
}

/// Reads a number as a time (`hhmmss[.f]`); `None` when its minutes or
/// seconds aren't any.
pub(crate) fn time_from_number(p: &NumPrefix) -> Option<ReadTime> {
    let n = i64::try_from(p.digits.len()).ok()?;
    let at = |k: i64| -> u32 {
        if k >= 0 && k < n {
            // k is inside the digits.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let d = p.digits[k as usize];
            u32::from(d)
        } else {
            0
        }
    };
    if p.point > 10 {
        return None;
    }
    let mut whole: u64 = 0;
    for k in 0..p.point.max(0) {
        whole = whole * 10 + u64::from(at(k));
    }
    let mut micros = 0u32;
    for k in 0..6 {
        micros = micros * 10 + at(p.point + k);
    }
    // One digit: it fits.
    #[allow(clippy::cast_possible_truncation)]
    let seventh = at(p.point + 6) as u8;
    let (h, m, sec) = (whole / 10_000, whole / 100 % 100, whole % 100);
    if m > 59 || sec > 59 {
        return None;
    }
    let total = i64::try_from(h * 3600 + m * 60 + sec).ok()? * 1_000_000 + i64::from(micros);
    Some(ReadTime {
        micros: if p.negative { -total } else { total },
        seventh,
        long_fraction: (p.point + 6..n).any(|k| at(k) != 0),
        garbage: false,
        from_datetime: false,
    })
}

/// The largest `TIME` at precision `fsp`: `838:59:59` and its fraction's
/// nines on MariaDB, without a fraction on MySQL.
pub(crate) fn time_max(fsp: u8, mysql: bool) -> i64 {
    let whole = (838 * 3600 + 59 * 60 + 59) * 1_000_000;
    if mysql {
        return whole;
    }
    let unit = 10i64.pow(6 - u32::from(fsp.min(6)));
    whole + 1_000_000 - unit
}

/// A read time at a column's precision: cut or rounded half up. The
/// result may still be beyond the type's range.
pub(crate) fn settle_time(r: &ReadTime, fsp: u8, round: bool) -> i64 {
    let unit = 10i64.pow(6 - u32::from(fsp.min(6)));
    let abs = r.micros.abs();
    let rest = abs % unit;
    let mut v = abs - rest;
    let up = round
        && if fsp >= 6 {
            r.seventh >= 5
        } else {
            rest * 2 >= unit
        };
    if up {
        v += unit;
    }
    if r.micros < 0 { -v } else { v }
}

/// A `YEAR` from a whole number as the servers map it: 0 is the zero
/// year, 1 to 69 are 2001 to 2069, 70 to 99 are 1970 to 1999, 1901 to
/// 2155 themselves. `None` outside those.
pub(crate) fn year_from_int(n: i128) -> Option<u16> {
    Some(match n {
        0 => 0,
        1..=69 => 2000 + u16::try_from(n).ok()?,
        70..=99 => 1900 + u16::try_from(n).ok()?,
        1901..=2155 => u16::try_from(n).ok()?,
        _ => return None,
    })
}

/// The fraction's text at precision `fsp`: nothing, or a point and `fsp`
/// digits.
fn fraction_text(out: &mut String, micros: u32, fsp: u8) {
    use std::fmt::Write as _;
    if fsp > 0 {
        let digits = format!("{micros:06}");
        let _ = write!(out, ".{}", &digits[..usize::from(fsp.min(6))]);
    }
}

/// A `DATETIME(fsp)` or `TIMESTAMP(fsp)` value's text.
pub(crate) fn datetime_text(d: DateTime, fsp: u8) -> String {
    let mut s = format!("{} {:02}:{:02}:{:02}", d.date, d.hour, d.minute, d.second);
    fraction_text(&mut s, d.micros, fsp);
    s
}

/// A `TIME(fsp)` value's text: `[-]hh:mm:ss[.f]`, three hour digits past
/// 99.
pub(crate) fn time_text(t: Time, fsp: u8) -> String {
    let abs = t.micros().unsigned_abs();
    let (secs, frac) = (abs / 1_000_000, abs % 1_000_000);
    let mut s = format!(
        "{}{:02}:{:02}:{:02}",
        if t.micros() < 0 { "-" } else { "" },
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    );
    fraction_text(&mut s, u32::try_from(frac).unwrap_or(0), fsp);
    s
}

/// The current time: seconds since the epoch (UTC) and microseconds.
pub(crate) fn now() -> (i64, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (
        i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        d.subsec_micros(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::assign::num_prefix;

    fn dt(s: &str) -> Option<String> {
        datetime_from_str(s.as_bytes()).map(|r| {
            let mut t = datetime_text(r.dt, 6);
            if !r.has_time {
                t.push_str(" (date)");
            }
            if r.garbage {
                t.push_str(" +text");
            }
            t
        })
    }

    #[test]
    fn text_reads_as_dates_the_way_the_servers_read_it() {
        let ok = |s: &str, want: &str| assert_eq!(dt(s).as_deref(), Some(want), "{s}");
        ok("2024-02-29", "2024-02-29 00:00:00.000000 (date)");
        ok("2024-2-9 1:2:3", "2024-02-09 01:02:03.000000");
        ok("2024-02-29T13:14:15.1234567", "2024-02-29 13:14:15.123456");
        ok("20240229", "2024-02-29 00:00:00.000000 (date)");
        ok("20240229131415", "2024-02-29 13:14:15.000000");
        ok("240229131415", "2024-02-29 13:14:15.000000");
        ok("20240229T131415", "2024-02-29 13:14:15.000000");
        ok("20240229131415.5", "2024-02-29 13:14:15.500000");
        ok("99-12-31", "1999-12-31 00:00:00.000000 (date)");
        ok("69-1-1 1:1", "2069-01-01 01:01:00.000000");
        ok("2024-02-29  13:14:15", "2024-02-29 13:14:15.000000");
        ok(" 2024-02-29 13:14:15 ", "2024-02-29 13:14:15.000000");
        ok("2024-02-29 13", "2024-02-29 13:00:00.000000");
        ok("2024.02.29", "2024-02-29 00:00:00.000000 (date)");
        ok("2024/02/29 13.14.15", "2024-02-29 13:14:15.000000");
        ok("2024@02@29 13*14*15", "2024-02-29 13:14:15.000000");
        ok("10:11:12", "2010-11-12 00:00:00.000000 (date)");
        ok("0000-00-00", "0000-00-00 00:00:00.000000 (date)");
        ok("2024-00-10", "2024-00-10 00:00:00.000000 (date)");
        ok("2024-02-29abc", "2024-02-29 00:00:00.000000 (date) +text");
        ok(
            "2024-02-29 13:14:15+05:30",
            "2024-02-29 13:14:15.000000 +text",
        );
        ok("2024-02-29 13:14:15Z", "2024-02-29 13:14:15.000000 +text");
        for bad in [
            "abc",
            "",
            "2024-13-01",
            "2024-02",
            "2024",
            "202402",
            "2024022",
            "10000-01-01",
            "13:14:15",
            "2024-02-29 24:00:00",
        ] {
            assert_eq!(dt(bad), None, "{bad}");
        }
        let r = datetime_from_str(b"2024-02-29 13:14:15.1234567").unwrap();
        assert!(r.long_fraction);
        assert_eq!(r.seventh, 7);
    }

    #[test]
    fn numbers_read_as_dates_in_their_ranges() {
        let n = |s: &str| {
            datetime_from_number(&num_prefix(s.as_bytes())).map(|r| datetime_text(r.dt, 0))
        };
        assert_eq!(n("0").as_deref(), Some("0000-00-00 00:00:00"));
        assert_eq!(n("100"), None);
        assert_eq!(n("101").as_deref(), Some("2000-01-01 00:00:00"));
        assert_eq!(n("691231").as_deref(), Some("2069-12-31 00:00:00"));
        assert_eq!(n("700101").as_deref(), Some("1970-01-01 00:00:00"));
        assert_eq!(n("991231").as_deref(), Some("1999-12-31 00:00:00"));
        assert_eq!(n("1000101"), None);
        assert_eq!(n("10000101").as_deref(), Some("1000-01-01 00:00:00"));
        assert_eq!(n("99991231").as_deref(), Some("9999-12-31 00:00:00"));
        assert_eq!(n("100000000"), None);
        assert_eq!(n("101000000").as_deref(), Some("2000-01-01 00:00:00"));
        assert_eq!(n("691231235959").as_deref(), Some("2069-12-31 23:59:59"));
        assert_eq!(n("700101000000").as_deref(), Some("1970-01-01 00:00:00"));
        assert_eq!(n("1000101000000").as_deref(), Some("0100-01-01 00:00:00"));
        assert_eq!(n("99991231235959").as_deref(), Some("9999-12-31 23:59:59"));
        assert_eq!(n("100000101000000"), None);
        assert_eq!(n("-20240229"), None);
        assert_eq!(
            n("20240229131415.123456").as_deref(),
            Some("2024-02-29 13:14:15")
        );
    }

    #[test]
    fn times_read_from_text_and_numbers() {
        let t = |s: &str| {
            time_from_str(s.as_bytes()).map(|r| {
                let mut o = time_text(Time::from_micros(r.micros).unwrap_or_default(), 6);
                if r.garbage {
                    o.push_str(" +text");
                }
                o
            })
        };
        assert_eq!(t("12:34:56").as_deref(), Some("12:34:56.000000"));
        assert_eq!(t("1:2:3").as_deref(), Some("01:02:03.000000"));
        assert_eq!(t("-838:59:59.999").as_deref(), Some("-838:59:59.999000"));
        assert_eq!(t("123456").as_deref(), Some("12:34:56.000000"));
        assert_eq!(t("1234").as_deref(), Some("00:12:34.000000"));
        assert_eq!(t("12").as_deref(), Some("00:00:12.000000"));
        assert_eq!(t("12.5").as_deref(), Some("00:00:12.500000"));
        assert_eq!(t("12:34").as_deref(), Some("12:34:00.000000"));
        assert_eq!(t("1:2").as_deref(), Some("01:02:00.000000"));
        assert_eq!(t("34 22:59:59").as_deref(), Some("838:59:59.000000"));
        assert_eq!(t("1 2").as_deref(), Some("00:00:01.000000 +text"));
        assert_eq!(t("12:00:00 x").as_deref(), Some("12:00:00.000000 +text"));
        assert_eq!(t("12:34:56abc").as_deref(), Some("12:34:56.000000 +text"));
        assert_eq!(t(" 12:00:00 ").as_deref(), Some("12:00:00.000000"));
        assert_eq!(t("-0:0:0.5").as_deref(), Some("-00:00:00.500000"));
        assert_eq!(t("2024-02-29 13:14:15").as_deref(), Some("13:14:15.000000"));
        assert_eq!(t("20240229131415").as_deref(), Some("13:14:15.000000"));
        for bad in ["abc", "", "12:60:00", "1234567", "2024-02-29"] {
            assert_eq!(t(bad), None, "{bad}");
        }
        // Beyond the range: read, for the caller to clip.
        assert!(time_from_str(b"839:00:00").unwrap().micros > time_max(6, false));
        let n = |s: &str| time_from_number(&num_prefix(s.as_bytes())).map(|r| r.micros / 1_000_000);
        assert_eq!(n("123456"), Some(12 * 3600 + 34 * 60 + 56));
        assert_eq!(n("100"), Some(60));
        assert_eq!(n("12"), Some(12));
        assert_eq!(n("1260"), None);
        assert_eq!(n("-123456"), Some(-(12 * 3600 + 34 * 60 + 56)));
        assert_eq!(n("8385959"), Some(838 * 3600 + 59 * 60 + 59));
    }

    #[test]
    fn fractions_cut_or_round_and_carry() {
        let r = datetime_from_str(b"2024-12-31 23:59:59.9999999").unwrap();
        assert_eq!(
            datetime_text(settle(&r, 6, false).unwrap(), 6),
            "2024-12-31 23:59:59.999999"
        );
        assert_eq!(
            datetime_text(settle(&r, 0, false).unwrap(), 0),
            "2024-12-31 23:59:59"
        );
        assert_eq!(
            datetime_text(settle(&r, 6, true).unwrap(), 6),
            "2025-01-01 00:00:00.000000"
        );
        assert_eq!(
            datetime_text(settle(&r, 0, true).unwrap(), 0),
            "2025-01-01 00:00:00"
        );
        let r = datetime_from_str(b"2024-02-29 13:14:15.5").unwrap();
        assert_eq!(
            datetime_text(settle(&r, 0, true).unwrap(), 0),
            "2024-02-29 13:14:16"
        );
        assert_eq!(
            datetime_text(settle(&r, 3, true).unwrap(), 3),
            "2024-02-29 13:14:15.500"
        );
        let r = datetime_from_str(b"9999-12-31 23:59:59.9").unwrap();
        assert_eq!(settle(&r, 0, true), None);
        let t = time_from_str(b"1:2:3.4567").unwrap();
        assert_eq!(settle_time(&t, 3, false), (3723 * 1_000_000) + 456_000);
        assert_eq!(settle_time(&t, 3, true), (3723 * 1_000_000) + 457_000);
        assert_eq!(
            time_max(3, false),
            (838 * 3600 + 59 * 60 + 59) * 1_000_000 + 999_000
        );
        assert_eq!(time_max(3, true), (838 * 3600 + 59 * 60 + 59) * 1_000_000);
    }

    #[test]
    fn the_calendar_and_the_time_zone() {
        let e = Env::STRICT;
        assert!(date_ok(Date::new(2024, 2, 29), &e));
        assert!(!date_ok(Date::new(2023, 2, 29), &e));
        assert!(!date_ok(Date::new(1900, 2, 29), &e));
        assert!(date_ok(Date::new(2000, 2, 29), &e));
        assert!(!date_ok(Date::new(2024, 2, 31), &e));
        assert!(date_ok(Date::new(0, 0, 0), &e));
        assert!(date_ok(Date::new(2024, 0, 10), &e));
        let my = Env {
            no_zero_date: true,
            no_zero_in_date: true,
            ..Env::STRICT
        };
        assert!(!date_ok(Date::new(0, 0, 0), &my));
        assert!(!date_ok(Date::new(2024, 0, 10), &my));
        let allow = Env {
            allow_invalid_dates: true,
            ..Env::STRICT
        };
        assert!(date_ok(Date::new(2023, 2, 31), &allow));
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(days_from_civil(2024, 2, 29)), (2024, 2, 29));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        let d = datetime_from_str(b"2024-02-29 13:14:15").unwrap().dt;
        let utc = to_timestamp(d, 0, false).unwrap();
        assert_eq!(utc.seconds, 1_709_212_455);
        // The same wall clock at +05:30 is five and a half hours earlier.
        let ist = to_timestamp(d, 19_800, false).unwrap();
        assert_eq!(ist.seconds, 1_709_212_455 - 19_800);
        assert_eq!(
            datetime_text(from_timestamp(ist, 0), 0),
            "2024-02-29 07:44:15"
        );
        assert_eq!(
            datetime_text(from_timestamp(utc, 19_800), 0),
            "2024-02-29 18:44:15"
        );
        // The range: one second after the epoch to 2106 (MariaDB) or 2038
        // (MySQL); the zero value is its own.
        let at =
            |s: &str, mysql| to_timestamp(datetime_from_str(s.as_bytes()).unwrap().dt, 0, mysql);
        assert!(at("1970-01-01 00:00:01", false).is_some());
        assert!(at("1970-01-01 00:00:00", false).is_none());
        assert!(at("1969-12-31 23:59:59", false).is_none());
        assert!(at("2038-01-19 03:14:08", false).is_some());
        assert!(at("2038-01-19 03:14:08", true).is_none());
        assert!(at("2038-01-19 03:14:07", true).is_some());
        assert!(at("2106-02-07 06:28:15", false).is_some());
        assert!(at("2106-02-07 06:28:16", false).is_none());
        assert_eq!(at("0000-00-00 00:00:00", false), Some(Timestamp::default()));
        assert_eq!(year_from_int(24), Some(2024));
        assert_eq!(year_from_int(70), Some(1970));
        assert_eq!(year_from_int(0), Some(0));
        assert_eq!(year_from_int(100), None);
        assert_eq!(year_from_int(2156), None);
    }
}
