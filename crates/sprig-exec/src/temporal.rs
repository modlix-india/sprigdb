//! Dates and times as values: reading them from text and numbers, their
//! text and their numbers, comparison, casts, and the calendar arithmetic
//! the date functions share.

use sprig_engine::row::{Date, DateTime, Time, Value};

use crate::conv::{err_text_of, is_space};
use crate::error::{Result, SqlError};
use crate::expr::{CastTarget, CmpKind, Env, Node};
use crate::num::{BigU, Dec, Round};
use crate::session::{Ctx, SqlMode};
use crate::types::{ExprType, Kind};

/// A date, a time of day or duration, or both, in separate fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Tm {
    pub(crate) neg: bool,
    pub(crate) year: u32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    /// Hours; a duration's can exceed 23.
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) micros: u32,
    /// Whether the value came with a time part.
    pub(crate) has_time: bool,
    /// Whether the value came with a date part.
    pub(crate) has_date: bool,
}

/// How reading a value as a temporal went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Read {
    /// The whole input was the value.
    Clean,
    /// A value was read; something after it was ignored.
    Truncated,
}

impl Tm {
    pub(crate) fn from_date(d: Date) -> Self {
        Tm {
            year: u32::from(d.year),
            month: u32::from(d.month),
            day: u32::from(d.day),
            has_date: true,
            ..Tm::default()
        }
    }

    pub(crate) fn from_datetime(d: DateTime) -> Self {
        Tm {
            hour: u32::from(d.hour),
            minute: u32::from(d.minute),
            second: u32::from(d.second),
            micros: d.micros,
            has_time: true,
            ..Tm::from_date(d.date)
        }
    }

    pub(crate) fn from_time(t: Time) -> Self {
        let abs = t.micros().unsigned_abs();
        let secs = abs / 1_000_000;
        Tm {
            neg: t.micros() < 0,
            hour: (secs / 3600) as u32,
            minute: (secs / 60 % 60) as u32,
            second: (secs % 60) as u32,
            micros: (abs % 1_000_000) as u32,
            has_time: true,
            ..Tm::default()
        }
    }

    pub(crate) fn date(&self) -> Date {
        Date::new(self.year.min(9999) as u16, self.month as u8, self.day as u8)
    }

    pub(crate) fn datetime(&self) -> DateTime {
        DateTime::new(
            self.date(),
            self.hour.min(23) as u8,
            self.minute as u8,
            self.second as u8,
            self.micros,
        )
    }

    pub(crate) fn time_micros(&self) -> i64 {
        let v =
            (i64::from(self.hour) * 3600 + i64::from(self.minute) * 60 + i64::from(self.second))
                * 1_000_000
                + i64::from(self.micros);
        if self.neg { -v } else { v }
    }

    /// Microseconds since 0000-00-00 00:00:00 on the proleptic calendar:
    /// what a datetime compares and subtracts by.
    pub(crate) fn datetime_micros(&self) -> i64 {
        let days = daynr(self.year, self.month, self.day);
        (days * 86_400
            + i64::from(self.hour) * 3600
            + i64::from(self.minute) * 60
            + i64::from(self.second))
            * 1_000_000
            + i64::from(self.micros)
    }

    /// The fields packed into one number in their order (year, month,
    /// day, time): what the servers compare dates by, so a partial date
    /// (`2024-00-10`) or a day beyond its month (`2024-02-31`) sorts by
    /// its fields, not by the calendar.
    pub(crate) fn packed(&self) -> i64 {
        let ymd = (i64::from(self.year) * 13 + i64::from(self.month)) * 32 + i64::from(self.day);
        ((ymd * 24 + i64::from(self.hour)) * 3600
            + i64::from(self.minute) * 60
            + i64::from(self.second))
            * 1_000_000
            + i64::from(self.micros)
    }

    fn is_zero_date(&self) -> bool {
        self.year == 0 && self.month == 0 && self.day == 0
    }
}

/// The servers' day number: days since year 0 (`TO_DAYS`); 0 for the zero
/// date.
///
/// A day 0 is the day before the 1st, and a month 0 the 31 days before
/// January: what the servers' calendar arithmetic gives for the partial
/// dates they accept.
pub(crate) fn daynr(year: u32, month: u32, day: u32) -> i64 {
    if year == 0 && month == 0 {
        return 0;
    }
    let before_january = if month == 0 { 31 } else { 0 };
    days_from_civil(i64::from(year), month.max(1), 1) + i64::from(day) - 1 + 719_528
        - before_january
}

/// The civil date of a count of days since the Unix epoch.
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The count of days since the Unix epoch of a civil date.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of a day number, or `None` outside years 0 to 9999.
pub(crate) fn date_from_daynr(nr: i64) -> Option<(u32, u32, u32)> {
    let (y, m, d) = civil_from_days(nr - 719_528);
    (0..=9999).contains(&y).then_some((y as u32, m, d))
}

pub(crate) fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0 && year != 0) {
                29
            } else {
                28
            }
        }
        _ => 31,
    }
}

/// The date and time of a count of seconds since the epoch, taken as local
/// time already.
pub(crate) fn datetime_from_unix(secs: i64, micros: u32) -> DateTime {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    DateTime::new(
        Date::new(y.clamp(0, 9999) as u16, m as u8, d as u8),
        (rem / 3600) as u8,
        (rem / 60 % 60) as u8,
        (rem % 60) as u8,
        micros,
    )
}

/// The text of a temporal value with `fsp` fractional-second digits.
pub(crate) fn fmt_value(v: &Value, fsp: u8) -> String {
    match v {
        Value::Date(d) => fmt_date(*d),
        Value::DateTime(dt) => fmt_datetime(*dt, fsp),
        Value::Time(t) => fmt_time(*t, fsp),
        _ => String::new(),
    }
}

pub(crate) fn fmt_date(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year, d.month, d.day)
}

fn frac(micros: u32, fsp: u8) -> String {
    if fsp == 0 {
        return String::new();
    }
    let digits = format!("{micros:06}");
    format!(".{}", &digits[..usize::from(fsp.min(6))])
}

pub(crate) fn fmt_datetime(dt: DateTime, fsp: u8) -> String {
    format!(
        "{} {:02}:{:02}:{:02}{}",
        fmt_date(dt.date),
        dt.hour,
        dt.minute,
        dt.second,
        frac(dt.micros, fsp)
    )
}

pub(crate) fn fmt_time(t: Time, fsp: u8) -> String {
    let abs = t.micros().unsigned_abs();
    let (secs, micros) = (abs / 1_000_000, (abs % 1_000_000) as u32);
    format!(
        "{}{:02}:{:02}:{:02}{}",
        if t.micros() < 0 { "-" } else { "" },
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        frac(micros, fsp)
    )
}

fn number(whole: u64, micros: u32, neg: bool) -> Dec {
    if micros == 0 {
        Dec {
            neg: neg && whole != 0,
            mag: BigU::from_u128(u128::from(whole)),
            scale: 0,
        }
    } else {
        Dec {
            neg,
            mag: BigU::from_u128(u128::from(whole) * 1_000_000 + u128::from(micros)),
            scale: 6,
        }
    }
}

/// The number a temporal value is among numbers: `YYYYMMDD`,
/// `YYYYMMDDhhmmss[.ffffff]` or `[-]hhmmss[.ffffff]`. The fraction is kept
/// only when it isn't zero.
pub(crate) fn to_dec(v: &Value) -> Dec {
    match v {
        Value::Date(d) => number(date_number(*d), 0, false),
        Value::DateTime(dt) => number(
            date_number(dt.date) * 1_000_000
                + u64::from(dt.hour) * 10_000
                + u64::from(dt.minute) * 100
                + u64::from(dt.second),
            dt.micros,
            false,
        ),
        Value::Time(t) => {
            let abs = t.micros().unsigned_abs();
            let (secs, micros) = (abs / 1_000_000, (abs % 1_000_000) as u32);
            number(
                secs / 3600 * 10_000 + secs / 60 % 60 * 100 + secs % 60,
                micros,
                t.micros() < 0,
            )
        }
        _ => Dec::zero(),
    }
}

pub(crate) fn date_number(d: Date) -> u64 {
    u64::from(d.year) * 10_000 + u64::from(d.month) * 100 + u64::from(d.day)
}

fn digits_at(s: &[u8], mut i: usize, max: usize) -> (u32, usize, usize) {
    let start = i;
    let mut v: u32 = 0;
    while i < s.len() && i - start < max && s[i].is_ascii_digit() {
        v = v.saturating_mul(10).saturating_add(u32::from(s[i] - b'0'));
        i += 1;
    }
    (v, i, i - start)
}

/// Reads up to six fraction digits after a `.`; further digits round the
/// value up or are cut. Returns the microseconds, whether to carry one
/// second, and the position after the digits.
fn fraction(s: &[u8], mut i: usize, round: bool) -> (u32, bool, usize) {
    let mut micros = 0u32;
    let mut n = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        if n < 6 {
            micros = micros * 10 + u32::from(s[i] - b'0');
        } else if n == 6 && round && s[i] >= b'5' {
            micros += 1;
        }
        n += 1;
        i += 1;
    }
    for _ in n..6 {
        micros *= 10;
    }
    if micros >= 1_000_000 {
        (0, true, i)
    } else {
        (micros, false, i)
    }
}

fn two_digit_year(y: u32) -> u32 {
    if y < 70 { 2000 + y } else { 1900 + y }
}

/// A two-digit year of a parsed value, made a full one unless every field
/// is zero (`00-00-00` stays the zero date).
fn two_digit_year_unless_zero(t: &Tm) -> u32 {
    let all_zero =
        t.year == 0 && t.month == 0 && t.day == 0 && t.hour == 0 && t.minute == 0 && t.second == 0;
    if all_zero { 0 } else { two_digit_year(t.year) }
}

/// How strictly a value is read as a date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateMode {
    /// As the session's `sql_mode` says (casts, comparisons, storing).
    Session,
    /// The zero date and a zero month or day are fine: the functions that
    /// only take a date apart (`YEAR`, `MONTH`, `DAY`, `DATE_FORMAT`, ...).
    /// MySQL still refuses the string `0000-00-00` under `NO_ZERO_DATE`.
    Fuzzy,
    /// Only a real calendar date: the functions that compute with it
    /// (`TO_DAYS`, `DAYOFWEEK`, `DATE_ADD`, `DATEDIFF`, ...).
    Real,
    /// MariaDB's reading of what it compares with a date: any day up to
    /// 31 in any month, zero parts allowed.
    Loose,
}

/// Whether the fields are a date `mode` accepts. `number` says the value
/// was a number, not text.
fn valid_date(ctx: &Ctx<'_>, t: &Tm, mode: DateMode, number: bool) -> bool {
    let sql = ctx.session.sql_mode;
    if t.month > 12 || t.day > 31 || t.year > 9999 {
        return false;
    }
    if t.is_zero_date() {
        return match mode {
            DateMode::Session => !sql.has(SqlMode::NO_ZERO_DATE),
            DateMode::Fuzzy => number || !sql.has(SqlMode::NO_ZERO_DATE),
            DateMode::Real => false,
            DateMode::Loose => true,
        };
    }
    if t.month == 0 || t.day == 0 {
        return match mode {
            DateMode::Session => !sql.has(SqlMode::NO_ZERO_IN_DATE),
            DateMode::Fuzzy | DateMode::Loose => true,
            DateMode::Real => false,
        };
    }
    mode == DateMode::Loose
        || sql.has(SqlMode::ALLOW_INVALID_DATES)
        || t.day <= days_in_month(t.year, t.month)
}

/// Adds one second to a datetime's fields (a fraction that rounded up).
fn carry_second(t: &mut Tm) {
    let total = t.datetime_micros() + 1_000_000;
    let days = total / 86_400_000_000;
    let rest = total % 86_400_000_000 / 1_000_000;
    if let Some((y, m, d)) = date_from_daynr(days) {
        t.year = y;
        t.month = m;
        t.day = d;
    }
    t.hour = (rest / 3600) as u32;
    t.minute = (rest / 60 % 60) as u32;
    t.second = (rest % 60) as u32;
}

/// Reads a date or datetime from text: the delimited form
/// (`2024-1-5 3:4:5.5`, any punctuation between parts, a space or `T`
/// before the time) or the all-digits form (`20240105`, `240105103000`).
/// `None` when the text isn't one or names a date `mode` refuses.
pub(crate) fn parse_datetime_mode(ctx: &Ctx<'_>, s: &[u8], mode: DateMode) -> Option<(Tm, Read)> {
    let round = ctx.session.sql_mode.has(SqlMode::TIME_ROUND_FRACTIONAL);
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let mut run = i;
    while run < s.len() && s[run].is_ascii_digit() {
        run += 1;
    }
    let n = run - i;
    if n == 0 {
        return None;
    }
    let mut t = Tm {
        has_date: true,
        ..Tm::default()
    };
    let compact = run == s.len() || s[run] == b'.' || n > 4;
    let mut carry = false;
    let mut short_time = false;
    if compact && (run == s.len() || s[run] == b'.' || s[run] == b'T' || is_space(s[run])) && n >= 5
    {
        let year_len = if n == 8 || n >= 14 { 4 } else { 2 };
        let (y, p, _) = digits_at(s, i, year_len);
        t.year = y;
        let (mo, p, _) = digits_at(s, p, 2);
        let (d, mut p, _) = digits_at(s, p, 2);
        // A compact date, a T and a compact time.
        if p == run && s.get(p) == Some(&b'T') && s.get(p + 1).is_some_and(u8::is_ascii_digit) {
            p += 1;
        }
        let (h, p, hl) = digits_at(s, p, 2);
        let (mi, p, _) = digits_at(s, p, 2);
        let (se, p, _) = digits_at(s, p, 2);
        t.month = mo;
        t.day = d;
        t.hour = h;
        t.minute = mi;
        t.second = se;
        t.has_time = hl > 0;
        if year_len == 2 {
            t.year = two_digit_year_unless_zero(&t);
        }
        i = p;
        if i < s.len() && s[i] == b'.' && t.has_time {
            let (m, c, p) = fraction(s, i + 1, round);
            t.micros = m;
            carry = c;
            i = p;
        }
    } else {
        let mut parts = [0u32; 6];
        let mut count = 0;
        let mut year_digits = 0;
        while count < 6 {
            let (v, p, len) = digits_at(s, i, if count == 0 { 4 } else { 2 });
            if len == 0 {
                break;
            }
            if count == 0 {
                year_digits = len;
            }
            parts[count] = v;
            count += 1;
            i = p;
            if count == 6 || i >= s.len() {
                break;
            }
            if count == 3 {
                // Between the date and the time: spaces or a T.
                if s[i] == b'T' {
                    i += 1;
                } else if is_space(s[i]) {
                    while i < s.len() && is_space(s[i]) {
                        i += 1;
                    }
                } else {
                    break;
                }
                continue;
            }
            if s[i].is_ascii_punctuation() && i + 1 < s.len() && s[i + 1].is_ascii_digit() {
                i += 1;
            } else {
                break;
            }
        }
        if count < 3 {
            return None;
        }
        t.year = parts[0];
        t.month = parts[1];
        t.day = parts[2];
        t.hour = parts[3];
        t.minute = parts[4];
        t.second = parts[5];
        if year_digits <= 2 {
            t.year = two_digit_year_unless_zero(&t);
        }
        t.has_time = count > 3;
        short_time = count == 4 || count == 5;
        if count == 6 && i < s.len() && s[i] == b'.' {
            let (m, c, p) = fraction(s, i + 1, round);
            t.micros = m;
            carry = c;
            i = p;
        }
    }
    if t.hour > 23 || t.minute > 59 || t.second > 59 || !valid_date(ctx, &t, mode, false) {
        return None;
    }
    if carry {
        carry_second(&mut t);
    }
    let rest = &s[i..];
    let read = if rest.iter().all(|&b| is_space(b)) {
        Read::Clean
    } else {
        Read::Truncated
    };
    // MySQL refuses a time without seconds that other text follows.
    if !ctx.maria() && read == Read::Truncated && short_time {
        return None;
    }
    Some((t, read))
}

/// Reads a time from text: `[-][D ]hh:mm[:ss[.f]]`, `hhmmss[.f]`, or the
/// time part of a datetime. `None` when the text isn't one. A time beyond
/// 838:59:59 is clipped and reported as truncated.
///
/// A date with a time gives its time; a date alone is not recognized
/// and reads as digits (`'2024-01-15'` is 00:20:24, with a warning).
pub(crate) fn parse_time(ctx: &Ctx<'_>, s: &[u8]) -> Option<(Tm, Read)> {
    parse_time_clip(ctx, s, true)
}

/// [`parse_time`]; without `clip` the hours are kept as written (up to a
/// million), as MariaDB's `EXTRACT` reads a duration.
pub(crate) fn parse_time_clip(ctx: &Ctx<'_>, s: &[u8], clip: bool) -> Option<(Tm, Read)> {
    let round = ctx.session.sql_mode.has(SqlMode::TIME_ROUND_FRACTIONAL);
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    // A string with a date in it gives its time part.
    let body = &s[i..];
    // A date in front: two delimiters that are no colon or point.
    let dashes = body
        .iter()
        .take_while(|&&b| b != b' ' && b != b'T')
        .filter(|&&b| {
            if ctx.maria() {
                b.is_ascii_punctuation() && b != b':' && b != b'.'
            } else {
                b == b'-'
            }
        })
        .count();
    let digit_run = body.iter().take_while(|b| b.is_ascii_digit()).count();
    // ... or a date written with colons that a time follows after a space
    // ('00:00:00 12:00:00' is the zero date at noon).
    let token = body.iter().take_while(|&&b| b != b' ').count();
    let colons = body[..token].split(|&b| b == b':').count() - 1;
    let colon_dated = colons >= 2
        && body[token..]
            .iter()
            .find(|&&b| !is_space(b))
            .is_some_and(u8::is_ascii_digit)
        && token < body.len();
    let dated = (dashes >= 2 || colon_dated) && body.first() != Some(&b'-');
    // MySQL does not take a `T` between date and time when it wants a
    // time: the string is then read as digits.
    let t_form = body.iter().position(|&b| b == b'T').is_some_and(|p| p >= 8);
    if (dated || (digit_run >= 12 && !body.contains(&b':'))) && !(t_form && !ctx.maria()) {
        match parse_datetime_mode(ctx, s, DateMode::Loose) {
            // MySQL wants the day to exist in its month.
            Some((dt, _))
                if !ctx.maria()
                    && dt.has_time
                    && dt.month != 0
                    && dt.day > days_in_month(dt.year, dt.month) =>
            {
                return None;
            }
            Some((dt, read)) if dt.has_time && (read == Read::Clean || dated) => {
                return Some((
                    Tm {
                        year: 0,
                        month: 0,
                        day: 0,
                        has_date: false,
                        has_time: true,
                        ..dt
                    },
                    read,
                ));
            }
            // MySQL reads a date followed by other text as midnight.
            Some((_, Read::Truncated)) if dated && !ctx.maria() => {
                return Some((
                    Tm {
                        has_time: true,
                        ..Tm::default()
                    },
                    Read::Truncated,
                ));
            }
            // MySQL reads a date whose time it cannot take (a time
            // without seconds that text follows) as midnight too.
            None if dated
                && !ctx.maria()
                && parse_datetime_mode(ctx, &body[..token], DateMode::Loose).is_some() =>
            {
                return Some((
                    Tm {
                        has_time: true,
                        ..Tm::default()
                    },
                    Read::Truncated,
                ));
            }
            // What looks like a date with a time but is no date.
            None if dated && body.contains(&b':') => return None,
            _ => {}
        }
    }
    let mut t = Tm {
        has_time: true,
        ..Tm::default()
    };
    if i < s.len() && s[i] == b'-' {
        t.neg = true;
        i += 1;
    }
    let (first, p, len) = digits_at(s, i, 10);
    if len == 0 {
        return None;
    }
    let mut days = 0u32;
    let mut day_form = false;
    let mut v = first;
    let mut pos = p;
    if pos < s.len() && s[pos] == b' ' {
        let (h, p2, l2) = digits_at(s, pos + 1, 4);
        if l2 > 0 {
            days = first;
            day_form = true;
            v = h;
            pos = p2;
        }
    }
    let mut carry = false;
    if pos < s.len() && s[pos] == b':' {
        let (m, p2, l2) = digits_at(s, pos + 1, 2);
        if l2 == 0 {
            return None;
        }
        t.hour = v;
        t.minute = m;
        pos = p2;
        if pos < s.len() && s[pos] == b':' {
            let (sec, p3, l3) = digits_at(s, pos + 1, 2);
            if l3 > 0 {
                t.second = sec;
                pos = p3;
            }
        }
    } else if day_form {
        t.hour = v;
    } else {
        // All digits: hhmmss from the right.
        t.second = v % 100;
        t.minute = v / 100 % 100;
        t.hour = v / 10_000;
    }
    if pos < s.len() && s[pos] == b'.' {
        let (m, c, p2) = fraction(s, pos + 1, round);
        t.micros = m;
        carry = c;
        pos = p2;
    }
    if t.minute > 59 || t.second > 59 {
        return None;
    }
    // Far beyond the range is still beyond it; the fields stay small.
    t.hour = t
        .hour
        .saturating_add(days.saturating_mul(24))
        .min(1_000_000);
    if carry {
        t.second += 1;
        if t.second == 60 {
            t.second = 0;
            t.minute += 1;
            if t.minute == 60 {
                t.minute = 0;
                t.hour += 1;
            }
        }
    }
    let mut read = if s[pos..].iter().all(|&b| is_space(b)) {
        Read::Clean
    } else {
        Read::Truncated
    };
    // MySQL refuses a time followed by a space and more text.
    if !ctx.maria() && read == Read::Truncated && s[pos] == b' ' && s[i..pos].contains(&b':') {
        return None;
    }
    if clip && t.time_micros().abs() > Time::MAX_MICROS {
        let max_frac = if ctx.maria() { 999_999 } else { 0 };
        t.hour = 838;
        t.minute = 59;
        t.second = 59;
        t.micros = max_frac;
        read = Read::Truncated;
    }
    Some((t, read))
}

/// Reads a number as a datetime: `YYMMDD`, `YYYYMMDD`, `YYMMDDhhmmss` or
/// `YYYYMMDDhhmmss`, by its size. Zero is the zero date.
/// A fraction counts only
/// for a number that carries a time (`YYYYMMDDhhmmss.f`); behind a date
/// (`YYYYMMDD.f`) it is dropped.
pub(crate) fn number_to_datetime_mode(ctx: &Ctx<'_>, d: &Dec, mode: DateMode) -> Option<Tm> {
    if d.neg {
        return None;
    }
    let whole = d.round(0, Round::Truncate);
    let n = whole.mag.to_u128().and_then(|v| u64::try_from(v).ok())?;
    let micros = {
        let frac = d.sub(&whole).round(6, Round::Truncate);
        frac.mag.to_u128().unwrap_or(0) as u32
    };
    let (date, time, has_time) = if n == 0 {
        (0, 0, false)
    } else if n < 101 {
        return None;
    } else if n <= 691_231 {
        (20_000_000 + n, 0, false)
    } else if n <= 991_231 {
        (19_000_000 + n, 0, false)
    } else if n < 10_000_101 && !(mode == DateMode::Fuzzy && !ctx.maria()) {
        // (MySQL's functions that only take a date apart read these as
        // YYYYMMDD too: 1000000 is 0100-00-00.)
        return None;
    } else if n <= 99_991_231 {
        (n, 0, false)
    } else if n < 101_000_000 {
        return None;
    } else if n <= 691_231_235_959 {
        (20_000_000 + n / 1_000_000, n % 1_000_000, true)
    } else if n <= 991_231_235_959 {
        (19_000_000 + n / 1_000_000, n % 1_000_000, true)
    } else {
        // Thirteen digits and more: YYYYMMDDhhmmss, a year below 1000
        // included (1000000000000 is 0100-00-00 00:00:00).
        (n / 1_000_000, n % 1_000_000, true)
    };
    let t = Tm {
        year: (date / 10_000) as u32,
        month: (date / 100 % 100) as u32,
        day: (date % 100) as u32,
        hour: (time / 10_000) as u32,
        minute: (time / 100 % 100) as u32,
        second: (time % 100) as u32,
        micros: if has_time { micros } else { 0 },
        has_date: true,
        has_time,
        neg: false,
    };
    (t.hour <= 23 && t.minute <= 59 && t.second <= 59 && valid_date(ctx, &t, mode, true))
        .then_some(t)
}

fn warn_incorrect(ctx: &mut Ctx<'_>, what: &str, v: &Value) {
    let text = match v {
        Value::Str(s) | Value::Json(s) => err_text_of(s, false),
        Value::Bytes(s) => err_text_of(s, true),
        other => crate::conv::to_text(other, &ExprType::double(), ctx.maria())
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default(),
    };
    ctx.warn(1292, format!("Incorrect {what} value: '{text}'"));
}

fn warn_truncated(ctx: &mut Ctx<'_>, what: &str, v: &Value) {
    if let Value::Str(s) | Value::Bytes(s) = v {
        ctx.warn(
            1292,
            format!(
                "Truncated incorrect {what} value: '{}'",
                err_text_of(s, false)
            ),
        );
    }
}

/// A value read as a date or datetime, with the servers' warning when it
/// isn't one (`None`) or has trailing text. A `TIME` is placed on the
/// statement's date.
pub(crate) fn to_datetime(ctx: &mut Ctx<'_>, v: &Value) -> Option<Tm> {
    to_datetime_mode(ctx, v, DateMode::Session)
}

/// [`to_datetime`] under a [`DateMode`].
pub(crate) fn to_datetime_mode(ctx: &mut Ctx<'_>, v: &Value, mode: DateMode) -> Option<Tm> {
    match v {
        Value::Null => None,
        Value::Date(_) | Value::DateTime(_) => {
            let t = match v {
                Value::Date(d) => Tm::from_date(*d),
                Value::DateTime(d) => Tm::from_datetime(*d),
                _ => Tm::default(),
            };
            // A stored partial date is no date to compute with.
            if mode == DateMode::Real && (t.month == 0 || t.day == 0) {
                warn_incorrect(ctx, "datetime", v);
                return None;
            }
            Some(t)
        }
        Value::Time(t) => {
            let today = today(ctx);
            let base = daynr(today.year, today.month, today.day) * 86_400_000_000;
            let total = base + t.micros();
            let (y, m, d) = date_from_daynr(total.div_euclid(86_400_000_000))?;
            let rest = total.rem_euclid(86_400_000_000);
            Some(Tm {
                year: y,
                month: m,
                day: d,
                hour: (rest / 3_600_000_000) as u32,
                minute: (rest / 60_000_000 % 60) as u32,
                second: (rest / 1_000_000 % 60) as u32,
                micros: (rest % 1_000_000) as u32,
                has_date: true,
                has_time: true,
                neg: false,
            })
        }
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => {
            match parse_datetime_mode(ctx, s, mode) {
                Some((t, Read::Clean)) => Some(t),
                Some((t, Read::Truncated)) => {
                    warn_truncated(ctx, "datetime", v);
                    Some(t)
                }
                None => {
                    warn_incorrect(ctx, "datetime", v);
                    None
                }
            }
        }
        other => {
            let d = temporal_number(ctx, other)?;
            // (MySQL rounds a datetime number's seventh fraction digit.)
            let d = if ctx.maria() || d.scale <= 6 {
                d
            } else {
                d.round(6, Round::HalfUp)
            };
            let t = number_to_datetime_mode(ctx, &d, mode);
            match &t {
                None => warn_incorrect(ctx, "datetime", other),
                // A fraction behind a date number is dropped with a
                // remark.
                Some(t) if !t.has_time && d.scale > 0 => {
                    let level = if ctx.maria() {
                        crate::error::Level::Note
                    } else {
                        crate::error::Level::Warning
                    };
                    ctx.push(
                        level,
                        1292,
                        format!("Truncated incorrect date value: '{d}'"),
                    );
                }
                Some(_) => {}
            }
            t
        }
    }
}

/// The fractional-second digits the argument of a function that reads a
/// date brings. As [`fsp_of`], but on MariaDB a string literal that is
/// written like a date and is none (`'2023-02-29'`) brings six.
pub(crate) fn fsp_of_date(n: &Node, env: Env<'_>) -> u8 {
    if env.maria() && n.ty.kind == Kind::Str {
        if let Some(Value::Str(s) | Value::Bytes(s)) = &n.lit {
            let scratch = Ctx::new(env.s);
            let dated = s
                .iter()
                .skip_while(|b| is_space(**b))
                .take_while(|&&b| b != b' ' && b != b'T')
                .filter(|&&b| b.is_ascii_punctuation() && b != b':' && b != b'.')
                .count()
                >= 2;
            let is_date = match parse_datetime_mode(&scratch, s, DateMode::Loose) {
                Some((t, _)) => t.month == 0 || t.day <= days_in_month(t.year, t.month),
                None => false,
            };
            if dated && !is_date {
                return 6;
            }
        }
    }
    fsp_of(n, env)
}

/// The fractional-second digits a function's argument brings: a temporal
/// type's own, a decimal's scale, the digits written in a string literal,
/// six for any other string.
pub(crate) fn fsp_of(n: &Node, env: Env<'_>) -> u8 {
    // MySQL knows a constant string's digits when it is computed while
    // the statement is prepared; MariaDB does for a constant CAST(x AS
    // CHAR).
    if (!env.maria() || n.ty.cast_char) && n.ty.kind == Kind::Str && n.lit.is_none() && n.konst {
        let mut scratch = Ctx::new(env.s);
        return match n.eval(&mut scratch, &[]) {
            Ok(Value::Str(s) | Value::Bytes(s)) => {
                let after = s.rsplit(|&b| b == b'.').next().unwrap_or(&[]);
                if after.len() == s.len() {
                    0
                } else {
                    after
                        .iter()
                        .take_while(|b| b.is_ascii_digit())
                        .count()
                        .min(6) as u8
                }
            }
            _ => 0,
        };
    }
    match n.ty.kind {
        Kind::Time | Kind::DateTime | Kind::Timestamp | Kind::Decimal => n.ty.dec.min(6),
        // A double brings its fixed decimals, six when they aren't fixed.
        Kind::Double | Kind::Float => n.ty.dec.min(6),
        Kind::Str if n.alt.as_ref().is_some_and(|a| a.temporal_fsp.is_some()) => {
            n.alt.as_ref().and_then(|a| a.temporal_fsp).unwrap_or(0)
        }
        Kind::Str => match &n.lit {
            Some(Value::Str(s) | Value::Bytes(s))
                if {
                    // A text that starts like a date must be one; any
                    // other must read as a time.
                    let scratch = Ctx::new(env.s);
                    let dated = s
                        .iter()
                        .skip_while(|b| is_space(**b))
                        .take_while(|&&b| b != b' ' && b != b'T')
                        .filter(|&&b| b.is_ascii_punctuation() && b != b':' && b != b'.')
                        .count()
                        >= 2;
                    let date = || match parse_datetime_mode(&scratch, s, DateMode::Loose) {
                        Some((t, _)) => t.month == 0 || t.day <= days_in_month(t.year, t.month),
                        None => false,
                    };
                    if dated && s.contains(&b':') {
                        !date()
                    } else {
                        !date() && parse_time(&scratch, s).is_none()
                    }
                } =>
            {
                6
            }
            Some(Value::Str(s) | Value::Bytes(s)) => {
                let after = s.rsplit(|&b| b == b'.').next().unwrap_or(&[]);
                if after.len() == s.len() {
                    0
                } else {
                    after
                        .iter()
                        .take_while(|b| b.is_ascii_digit())
                        .count()
                        .min(6) as u8
                }
            }
            // A string whose type has fixed decimals (a cast to CHAR, a
            // function's text) brings those.
            // (Not MariaDB's CAST(x AS CHAR), whose decimals are no
            // fraction digits.)
            _ if n.ty.dec < crate::types::NOT_FIXED && !n.ty.cast_char => n.ty.dec.min(6),
            _ => 6,
        },
        _ => 0,
    }
}

/// A value read as a time, with the servers' warning when it isn't one.
pub(crate) fn to_time(ctx: &mut Ctx<'_>, v: &Value) -> Option<Tm> {
    match v {
        Value::Null => None,
        Value::Time(t) => Some(Tm::from_time(*t)),
        Value::DateTime(d) => Some(Tm {
            year: 0,
            month: 0,
            day: 0,
            has_date: false,
            ..Tm::from_datetime(*d)
        }),
        Value::Date(_) => Some(Tm {
            has_time: true,
            ..Tm::default()
        }),
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => match parse_time(ctx, s) {
            Some((t, Read::Clean)) => Some(t),
            Some((t, Read::Truncated)) => {
                warn_truncated(ctx, "time", v);
                Some(t)
            }
            None => {
                warn_incorrect(ctx, "time", v);
                None
            }
        },
        other => {
            let d = temporal_number(ctx, other)?;
            let parsed = number_to_time(ctx, &d);
            if parsed.is_none() {
                if ctx.maria() {
                    warn_incorrect(ctx, "time", other);
                } else {
                    ctx.warn(1292, format!("Truncated incorrect time value: '{d}'"));
                }
            }
            parsed
        }
    }
}

/// A number read as a date or time: a double by its exact binary value
/// (`20240115103003.14159e0` has the fraction .140625 there), cut after
/// nine fraction digits; anything else as a decimal.
fn temporal_number(ctx: &mut Ctx<'_>, v: &Value) -> Option<Dec> {
    let x = match v {
        Value::Double(x) => *x,
        Value::Float(f) => f64::from(*f),
        other => return crate::conv::to_dec(ctx, other),
    };
    exact_dec(x, 9).or_else(|| crate::conv::to_dec(ctx, v))
}

/// The exact value of a finite double, cut after `digits` fraction
/// digits; `None` where that needs more than 128 bits.
fn exact_dec(x: f64, digits: u32) -> Option<Dec> {
    if !x.is_finite() {
        return None;
    }
    let bits = x.abs().to_bits();
    let exp = ((bits >> 52) & 0x7ff) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (mant, e) = if exp == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), exp - 1075)
    };
    let mut d = if e >= 0 {
        if e > 64 {
            return None;
        }
        Dec::parse(&(u128::from(mant) << e).to_string())?
    } else {
        let shift = u32::try_from(-e).ok()?;
        if shift > 120 {
            return None;
        }
        let m = u128::from(mant);
        let int = m >> shift;
        let mut rest = m & ((1u128 << shift) - 1);
        let mut text = format!("{int}.");
        for _ in 0..digits {
            rest *= 10;
            text.push(char::from(b'0' + (rest >> shift) as u8));
            rest &= (1u128 << shift) - 1;
        }
        let text = text.trim_end_matches('0').trim_end_matches('.');
        Dec::parse(text)?
    };
    if x < 0.0 {
        d = d.negated();
    }
    Some(d)
}

/// Reads a number as a time: `hhmmss[.f]` up to 838:59:59. Beyond that
/// a number that is a date and time (`YYYYMMDDhhmmss`) gives its time;
/// MariaDB also reads a date number (`YYYYMMDD`) as midnight and clips
/// a seven-digit number to the largest time, with a warning; MySQL has
/// no answer for those.
fn number_to_time(ctx: &mut Ctx<'_>, d: &Dec) -> Option<Tm> {
    // MySQL rounds a seventh fraction digit (carrying into the seconds
    // and beyond once the time is read); MariaDB cuts it.
    let round_up = !ctx.maria()
        && d.scale > 6
        && d.abs()
            .round(6, Round::Truncate)
            .cmp(&d.abs().round(6, Round::HalfUp))
            != std::cmp::Ordering::Equal;
    let whole = d.abs().round(0, Round::Truncate);
    let n = whole
        .mag
        .to_u128()
        .and_then(|v| u64::try_from(v).ok())
        .unwrap_or(u64::MAX);
    let micros = d
        .abs()
        .sub(&whole)
        .round(6, Round::Truncate)
        .mag
        .to_u128()
        .unwrap_or(0) as u32;
    if n <= 8_385_959 {
        // MariaDB refuses a time number whose microseconds are 999999.
        if ctx.maria() && micros == 999_999 {
            return None;
        }
        let mut t = Tm {
            neg: d.neg,
            hour: (n / 10_000) as u32,
            minute: (n / 100 % 100) as u32,
            second: (n % 100) as u32,
            micros,
            has_time: true,
            ..Tm::default()
        };
        if t.minute > 59 || t.second > 59 {
            return None;
        }
        if round_up {
            t.micros += 1;
            if t.micros == 1_000_000 {
                t.micros = 0;
                t.second += 1;
                if t.second == 60 {
                    t.second = 0;
                    t.minute += 1;
                    if t.minute == 60 {
                        t.minute = 0;
                        t.hour += 1;
                    }
                }
            }
        }
        return Some(t);
    }
    let as_datetime = |ctx: &Ctx<'_>| {
        number_to_datetime_mode(ctx, &d.abs(), DateMode::Loose).map(|t| Tm {
            year: 0,
            month: 0,
            day: 0,
            has_date: false,
            has_time: true,
            ..t
        })
    };
    if !ctx.maria() {
        // MySQL wants a real date there (no zero month or day).
        return if n >= 10_000_000_000 {
            number_to_datetime_mode(ctx, &d.abs(), DateMode::Session)
                .filter(|t| t.month != 0 && t.day != 0)
                .map(|t| Tm {
                    year: 0,
                    month: 0,
                    day: 0,
                    has_date: false,
                    has_time: true,
                    ..t
                })
        } else {
            None
        };
    }
    if n >= 10_000_000 {
        return as_datetime(ctx);
    }
    ctx.warn(1292, format!("Truncated incorrect time value: '{d}'"));
    Some(Tm {
        neg: d.neg,
        hour: 838,
        minute: 59,
        second: 59,
        has_time: true,
        ..Tm::default()
    })
}

/// The statement's date in the session's time zone.
pub(crate) fn today(ctx: &Ctx<'_>) -> Tm {
    Tm::from_datetime(now(ctx))
}

/// The statement's start time in the session's time zone.
pub(crate) fn now(ctx: &Ctx<'_>) -> DateTime {
    datetime_from_unix(
        ctx.session.now_seconds + i64::from(ctx.session.time_zone_offset),
        ctx.session.now_micros,
    )
}

/// Cuts (or, on MySQL, rounds) microseconds to `fsp` digits.
fn cut_micros(ctx: &Ctx<'_>, micros: u32, fsp: u8) -> (u32, bool) {
    let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
    let mut m = micros / unit * unit;
    if ctx.session.sql_mode.has(SqlMode::TIME_ROUND_FRACTIONAL)
        && micros % unit >= unit / 2
        && unit > 1
    {
        m += unit;
    }
    if m >= 1_000_000 {
        (0, true)
    } else {
        (m, false)
    }
}

/// A `Tm` as a value of `kind` with `fsp` fraction digits.
pub(crate) fn tm_value(ctx: &Ctx<'_>, mut t: Tm, kind: Kind, fsp: u8) -> Value {
    match kind {
        Kind::Date => Value::Date(t.date()),
        Kind::Time => {
            let (m, carry) = cut_micros(ctx, t.micros, fsp);
            t.micros = m;
            let mut micros = t.time_micros();
            if carry {
                micros += if t.neg { -1_000_000 } else { 1_000_000 };
            }
            Value::Time(
                Time::from_micros(micros.clamp(-Time::MAX_MICROS, Time::MAX_MICROS))
                    .unwrap_or_default(),
            )
        }
        _ => {
            let (m, carry) = cut_micros(ctx, t.micros, fsp);
            t.micros = m;
            if carry {
                let last = t.year == 9999
                    && t.month == 12
                    && t.day == 31
                    && t.hour == 23
                    && t.minute == 59
                    && t.second == 59;
                if last {
                    // Rounding up leaves the calendar.
                    return Value::Null;
                }
                carry_second(&mut t);
            }
            Value::DateTime(t.datetime())
        }
    }
}

/// A time or datetime value without its fraction of a second: cut on
/// MariaDB, rounded on MySQL.
pub(crate) fn to_whole_seconds(ctx: &Ctx<'_>, v: &Value) -> Value {
    match v {
        Value::Time(t) => tm_value(ctx, Tm::from_time(*t), Kind::Time, 0),
        Value::DateTime(d) => match tm_value(ctx, Tm::from_datetime(*d), Kind::DateTime, 0) {
            // The very last second rounds up to nowhere: MySQL's number
            // keeps the date and shows midnight.
            Value::Null => Value::DateTime(DateTime::new(d.date, 0, 0, 0, 0)),
            other => other,
        },
        other => other.clone(),
    }
}

/// A value as the number its date, datetime or time compares by. `None`
/// when it can't be read as one (the comparison is then `NULL`).
pub(crate) fn cmp_key(ctx: &mut Ctx<'_>, kind: CmpKind, v: &Value) -> Result<Option<Dec>> {
    cmp_key_strict(ctx, kind, v, true)
}

/// [`cmp_key`]; `strict` is whether MySQL refuses a string that is no
/// date (it does in the comparison operators; in `BETWEEN`, `IN` and
/// `CASE` such a string compares as the zero date, like a number that is
/// no date does everywhere).
pub(crate) fn cmp_key_strict(
    ctx: &mut Ctx<'_>,
    kind: CmpKind,
    v: &Value,
    strict: bool,
) -> Result<Option<Dec>> {
    let t = match kind {
        // What is no time compares as the zero time.
        CmpKind::Time => {
            let t = match v {
                Value::Str(s) | Value::Bytes(s) => match parse_time(ctx, s) {
                    Some((t, Read::Clean)) => Some(t),
                    Some((t, Read::Truncated)) => {
                        warn_truncated(ctx, "time", v);
                        Some(t)
                    }
                    None => {
                        warn_incorrect(ctx, "time", v);
                        None
                    }
                },
                other => to_time(ctx, other),
            };
            // MySQL compares a number that is no time as beyond every time
            // of its sign (equal to none).
            if t.is_none()
                && !ctx.maria()
                && matches!(
                    v,
                    Value::Int(_) | Value::UInt(_) | Value::Decimal(_) | Value::Double(_)
                )
            {
                let d =
                    crate::conv::to_dec(&mut Ctx::new(ctx.session), v).unwrap_or_else(Dec::zero);
                let whole = d.abs().round(0, Round::Truncate);
                let n = whole.to_i64().unwrap_or(i64::MAX);
                // Within the hours of a time its minutes and seconds count
                // as they are, even beyond 59 (739265 is 73:92:65).
                let micros = if n <= 8_385_959 {
                    let frac = d.abs().sub(&whole).round(6, Round::Truncate);
                    let us = frac
                        .mag
                        .mul_pow10(6 - frac.scale.min(6))
                        .to_u128()
                        .unwrap_or(0) as i64;
                    (n / 10_000 * 3600 + n / 100 % 100 * 60 + n % 100) * 1_000_000 + us
                } else {
                    3_020_400_000_000
                };
                return Ok(Some(Dec::from_i64(if d.neg { -micros } else { micros })));
            }
            Some(Dec::from_i64(t.map_or(0, |t| t.time_micros())))
        }
        _ => {
            let direct = matches!(v, Value::Date(_) | Value::DateTime(_) | Value::Time(_));
            let text = matches!(v, Value::Str(_) | Value::Bytes(_));
            if !direct && !ctx.maria() && strict && text {
                // MySQL refuses a comparison with something that is no
                // date.
                let mut probe = Ctx::new(ctx.session);
                if to_datetime(&mut probe, v).is_none() && !v.is_null() {
                    let text = crate::conv::to_text(v, &ExprType::double(), false)
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                        .unwrap_or_default();
                    let what = if kind == CmpKind::Date {
                        "DATE"
                    } else {
                        "DATETIME"
                    };
                    return Err(SqlError::new(
                        1525,
                        "HY000",
                        format!("Incorrect {what} value: '{text}'"),
                    ));
                }
            }
            // MariaDB reads what it compares with a date loosely (any
            // day up to 31 counts), and what is no date as the zero date.
            let read = if ctx.maria() {
                to_datetime_mode(ctx, v, DateMode::Loose)
            } else {
                to_datetime(ctx, v)
            };
            let read = if read.is_none() && !v.is_null() {
                Some(Tm::default())
            } else {
                read
            };
            read.map(|t| {
                let t = if kind == CmpKind::Date {
                    Tm {
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micros: 0,
                        ..t
                    }
                } else {
                    t
                };
                Dec::from_i64(t.packed())
            })
        }
    };
    Ok(t)
}

/// A value as a temporal of `kind` (a merged result type).
pub(crate) fn coerce(ctx: &mut Ctx<'_>, v: Value, kind: Kind, fsp: u8) -> Value {
    if v.is_null() {
        return v;
    }
    let t = if kind == Kind::Time {
        to_time(ctx, &v)
    } else {
        to_datetime(ctx, &v)
    };
    match t {
        Some(t) => tm_value(ctx, t, kind, fsp),
        None => Value::Null,
    }
}

/// `CAST(a AS DATE | TIME | DATETIME)`.
pub(crate) fn cast(a: Node, target: CastTarget, env: Env<'_>) -> Result<Node> {
    let (ty, kind, fsp) = match target {
        CastTarget::Date => (ExprType::date(), Kind::Date, 0),
        CastTarget::Time(f) => (ExprType::time(f.min(6)), Kind::Time, f.min(6)),
        CastTarget::DateTime(f) => (ExprType::datetime(f.min(6)), Kind::DateTime, f.min(6)),
        _ => return Err(SqlError::unsupported("this CAST")),
    };
    let (a2, a_time) = (a.clone(), a.clone());
    let mut node = Node::new(ty.nullable(), "cast", move |ctx, row| {
        let v = a.eval(ctx, row)?;
        Ok(coerce(ctx, v, kind, fsp))
    });
    if env.maria() {
        // In a comparison, and as the argument of a date function,
        // MariaDB reads a cast of what is no date or time loosely: a day
        // that its month lacks counts, and what is no date at all is the
        // zero value, not NULL.
        let loose: crate::expr::EvalFn = std::sync::Arc::new(move |ctx, row| {
            let v = a2.eval(ctx, row)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            if kind == Kind::Time {
                return Ok(match coerce(ctx, v, kind, fsp) {
                    Value::Null => Value::Time(Time::default()),
                    other => other,
                });
            }
            Ok(match to_datetime_mode(ctx, &v, DateMode::Loose) {
                Some(t) => tm_value(ctx, t, kind, fsp),
                None => Value::DateTime(DateTime::default()),
            })
        });
        // A reader that wants a time takes a date whose day its month
        // lacks, but nothing that is no date at all.
        let a3 = a_time.clone();
        let as_time: crate::expr::EvalFn = std::sync::Arc::new(move |ctx, row| {
            let v = a3.eval(ctx, row)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            Ok(match to_datetime_mode(ctx, &v, DateMode::Loose) {
                Some(t) => tm_value(ctx, t, kind, fsp),
                None => Value::Null,
            })
        });
        node.alt = Some(std::sync::Arc::new(crate::expr::Alt {
            date_key: Some(loose),
            as_time: (kind != Kind::Time).then_some(as_time),
            ..crate::expr::Alt::default()
        }));
    }
    Ok(node)
}
