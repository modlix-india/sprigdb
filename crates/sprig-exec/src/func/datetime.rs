//! Date and time functions on values. The current time is an input: the
//! statement's start time and the session's time zone come from the
//! session.

use bytes::Bytes;
use sprig_engine::row::Value;

use crate::conv;
use crate::error::{Result, SqlError};
use crate::expr::{Env, Node, string_node};
use crate::num::{Dec, Round};
use crate::session::Ctx;
use crate::temporal::{
    self, DateMode, Tm, date_from_daynr, daynr, days_in_month, fmt_date, fmt_datetime,
    to_datetime_mode, to_time,
};
use crate::types::{Derivation, ExprType, Kind};

fn int_type(len: u32, env: Env<'_>) -> ExprType {
    ExprType {
        prec: len.min(10) as u8,
        width: if env.maria() { 4 } else { 0 },
        ..ExprType::int(len, false)
    }
    .nullable()
}

/// `NOW([fsp])`, `CURDATE()`, `CURTIME([fsp])`, `UTC_TIMESTAMP([fsp])`.
pub(crate) fn current(name: &str, args: &[Node], env: Env<'_>) -> Result<Node> {
    let fsp = match args.first() {
        None => 0,
        Some(n) => match n.lit_i64() {
            Some(v @ 0..=6) => v as u8,
            _ => {
                return Err(SqlError::new(
                    1426,
                    "42000",
                    format!("Too big precision specified for '{name}'. Maximum is 6"),
                ));
            }
        },
    };
    let offset = i64::from(env.s.time_zone_offset);
    Ok(match name {
        "CURDATE" | "CURRENT_DATE" => Node::new(ExprType::date(), "curdate", |ctx, _| {
            Ok(Value::Date(temporal::now(ctx).date))
        }),
        "CURTIME" | "CURRENT_TIME" => Node::new(ExprType::time(fsp), "curtime", move |ctx, _| {
            let t = Tm {
                year: 0,
                month: 0,
                day: 0,
                has_date: false,
                ..Tm::from_datetime(temporal::now(ctx))
            };
            Ok(temporal::tm_value(ctx, cut(t, fsp), Kind::Time, fsp))
        }),
        _ => {
            let utc = name.starts_with("UTC");
            let ty = if env.maria() && !utc {
                ExprType {
                    kind: Kind::Timestamp,
                    ..ExprType::datetime(fsp)
                }
            } else {
                ExprType::datetime(fsp)
            };
            Node::new(ty, "now", move |ctx, _| {
                let shift = if utc { 0 } else { offset };
                let dt = temporal::datetime_from_unix(
                    ctx.session.now_seconds + shift,
                    ctx.session.now_micros,
                );
                Ok(temporal::tm_value(
                    ctx,
                    cut(Tm::from_datetime(dt), fsp),
                    Kind::DateTime,
                    fsp,
                ))
            })
        }
    })
}

/// The clock's fraction is cut, never rounded.
fn cut(mut t: Tm, fsp: u8) -> Tm {
    let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
    t.micros = t.micros / unit * unit;
    t
}

/// What a field-extraction function returns for a date and time.
type PartFn = fn(&Tm) -> Option<i64>;

fn weekday(t: &Tm, sunday_first: bool) -> i64 {
    (daynr(t.year, t.month, t.day) + 5 + i64::from(sunday_first)) % 7
}

fn real_date(t: &Tm) -> bool {
    t.month != 0 && t.day != 0
}

fn days_in_year(year: u32) -> i64 {
    if days_in_month(year, 2) == 29 {
        366
    } else {
        365
    }
}

/// The week number of a date under a `WEEK()` mode, and the year the week
/// belongs to.
pub(crate) fn week(t: &Tm, mode: i64) -> (i64, u32) {
    let mut format = mode & 7;
    if format & 1 == 0 {
        format ^= 4;
    }
    let monday_first = format & 1 != 0;
    let mut week_year = format & 2 != 0;
    let first_weekday = format & 4 != 0;
    let nr = daynr(t.year, t.month, t.day);
    let mut first = daynr(t.year, 1, 1);
    let mut wd = (first + 5 + i64::from(!monday_first)) % 7;
    let mut year = t.year;
    if t.month == 1 && i64::from(t.day) <= 7 - wd {
        if !week_year && ((first_weekday && wd != 0) || (!first_weekday && wd >= 4)) {
            return (0, year);
        }
        week_year = true;
        year = year.saturating_sub(1);
        let days = days_in_year(year);
        first -= days;
        wd = (wd + 53 * 7 - days) % 7;
    }
    let days = if (first_weekday && wd != 0) || (!first_weekday && wd >= 4) {
        nr - (first + (7 - wd))
    } else {
        nr - (first - wd)
    };
    // A date before the year's first week that is not in January (a
    // month 0) is not caught above: the servers' day count wraps around
    // as an unsigned 32-bit number.
    let days = if days < 0 {
        i64::from(days as u32)
    } else {
        days
    };
    if week_year && days >= 52 * 7 {
        wd = (wd + days_in_year(year)) % 7;
        if (!first_weekday && wd < 4) || (first_weekday && wd == 0) {
            return (1, year + 1);
        }
    }
    (days / 7 + 1, year)
}

/// `YEAR`, `MONTH`, `DAY`, `HOUR`, ... : a field or a day count of the
/// argument.
pub(crate) fn part(name: &str, a: Node, env: Env<'_>) -> Result<Node> {
    let (len, time_arg, f): (u32, bool, PartFn) = match name {
        "YEAR" => (4, false, |t| Some(i64::from(t.year))),
        "MONTH" => (2, false, |t| Some(i64::from(t.month))),
        "DAY" | "DAYOFMONTH" => (2, false, |t| Some(i64::from(t.day))),
        "QUARTER" => (1, false, |t| Some(i64::from(t.month.div_ceil(3)))),
        "WEEK" => (2, false, |t| Some(week(t, 0).0)),
        "HOUR" => (2, true, |t| Some(i64::from(t.hour))),
        "MINUTE" => (2, true, |t| Some(i64::from(t.minute))),
        "SECOND" => (2, true, |t| Some(i64::from(t.second))),
        "MICROSECOND" => (6, true, |t| Some(i64::from(t.micros))),
        "DAYOFWEEK" => (1, false, |t| real_date(t).then(|| weekday(t, true) + 1)),
        "WEEKDAY" => (1, false, |t| real_date(t).then(|| weekday(t, false))),
        "DAYOFYEAR" => (3, false, |t| {
            real_date(t).then(|| daynr(t.year, t.month, t.day) - daynr(t.year, 1, 1) + 1)
        }),
        "TO_DAYS" => (6, false, |t| {
            real_date(t).then(|| daynr(t.year, t.month, t.day))
        }),
        "WEEKOFYEAR" => (2, false, |t| real_date(t).then(|| week(t, 3).0)),
        _ => return Err(SqlError::no_function(name)),
    };
    // The functions that take a date apart accept partial dates; those
    // that compute with it want a real one.
    let mode = match name {
        "YEAR" | "MONTH" | "DAY" | "DAYOFMONTH" | "QUARTER" => DateMode::Fuzzy,
        _ => DateMode::Real,
    };
    Ok(Node::new(
        int_type(len, env),
        "datepart",
        move |ctx, row| {
            let t = if time_arg {
                let v = a.time_arg(ctx, row)?;
                to_time(ctx, &v)
            } else {
                let v = a.temporal(ctx, row)?;
                to_datetime_mode(ctx, &v, mode)
            };
            Ok(t.and_then(|t| f(&t)).map_or(Value::Null, Value::Int))
        },
    ))
}

/// `DATE(x)`, `TIME(x)`, `LAST_DAY(x)`.
pub(crate) fn convert(name: &str, a: Node, env: Env<'_>) -> Result<Node> {
    let fsp = temporal::fsp_of(&a, env);
    let maria = env.maria();
    Ok(match name {
        // MariaDB's DATE() keeps a partial date; MySQL's follows the
        // session's mode.
        "DATE" => Node::new(ExprType::date().nullable(), "date", move |ctx, row| {
            let v = a.temporal(ctx, row)?;
            let mode = if maria {
                DateMode::Fuzzy
            } else {
                DateMode::Session
            };
            Ok(to_datetime_mode(ctx, &v, mode).map_or(Value::Null, |t| Value::Date(t.date())))
        }),
        "LAST_DAY" => Node::new(ExprType::date().nullable(), "last_day", move |ctx, row| {
            let v = a.temporal(ctx, row)?;
            Ok(match to_datetime_mode(ctx, &v, DateMode::Fuzzy) {
                Some(mut t) if t.month != 0 => {
                    t.day = days_in_month(t.year, t.month);
                    Value::Date(t.date())
                }
                Some(_) => {
                    if !maria {
                        ctx.warn(1292, "Incorrect datetime value: '0000-00-00'");
                    }
                    Value::Null
                }
                None => Value::Null,
            })
        }),
        _ => Node::new(ExprType::time(fsp).nullable(), "time", move |ctx, row| {
            let v = a.time_arg(ctx, row)?;
            Ok(to_time(ctx, &v)
                .map_or(Value::Null, |t| temporal::tm_value(ctx, t, Kind::Time, fsp)))
        }),
    })
}

/// A unit of `INTERVAL n unit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    Microsecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    SecondMicrosecond,
    MinuteMicrosecond,
    MinuteSecond,
    HourMicrosecond,
    HourSecond,
    HourMinute,
    DayMicrosecond,
    DaySecond,
    DayMinute,
    DayHour,
    YearMonth,
}

impl Unit {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "MICROSECOND" => Unit::Microsecond,
            "SECOND" => Unit::Second,
            "MINUTE" => Unit::Minute,
            "HOUR" => Unit::Hour,
            "DAY" => Unit::Day,
            "WEEK" => Unit::Week,
            "MONTH" => Unit::Month,
            "QUARTER" => Unit::Quarter,
            "YEAR" => Unit::Year,
            "SECOND_MICROSECOND" => Unit::SecondMicrosecond,
            "MINUTE_MICROSECOND" => Unit::MinuteMicrosecond,
            "MINUTE_SECOND" => Unit::MinuteSecond,
            "HOUR_MICROSECOND" => Unit::HourMicrosecond,
            "HOUR_SECOND" => Unit::HourSecond,
            "HOUR_MINUTE" => Unit::HourMinute,
            "DAY_MICROSECOND" => Unit::DayMicrosecond,
            "DAY_SECOND" => Unit::DaySecond,
            "DAY_MINUTE" => Unit::DayMinute,
            "DAY_HOUR" => Unit::DayHour,
            "YEAR_MONTH" => Unit::YearMonth,
            _ => return None,
        })
    }

    /// Whether the unit moves whole days or months only: a `DATE` stays
    /// a `DATE`.
    fn is_date_unit(self) -> bool {
        matches!(
            self,
            Unit::Day | Unit::Week | Unit::Month | Unit::Quarter | Unit::Year | Unit::YearMonth
        )
    }

    /// Whether the unit counts months.
    fn is_month_unit(self) -> bool {
        matches!(
            self,
            Unit::Month | Unit::Quarter | Unit::Year | Unit::YearMonth
        )
    }

    /// Whether the unit reaches microseconds.
    fn has_micros(self) -> bool {
        matches!(
            self,
            Unit::Microsecond
                | Unit::SecondMicrosecond
                | Unit::MinuteMicrosecond
                | Unit::HourMicrosecond
                | Unit::DayMicrosecond
        )
    }

    /// Whether the unit has a day part or larger.
    fn has_days(self) -> bool {
        self.is_date_unit()
            || matches!(
                self,
                Unit::DayMicrosecond | Unit::DaySecond | Unit::DayMinute | Unit::DayHour
            )
    }

    /// The parts a composite unit's value is made of, largest first, as
    /// microseconds per part (0 marks the microsecond part itself, -1
    /// months, -12 years).
    fn parts(self) -> &'static [i64] {
        const D: i64 = 86_400_000_000;
        const H: i64 = 3_600_000_000;
        const M: i64 = 60_000_000;
        const S: i64 = 1_000_000;
        match self {
            Unit::SecondMicrosecond => &[S, 0],
            Unit::MinuteMicrosecond => &[M, S, 0],
            Unit::MinuteSecond => &[M, S],
            Unit::HourMicrosecond => &[H, M, S, 0],
            Unit::HourSecond => &[H, M, S],
            Unit::HourMinute => &[H, M],
            Unit::DayMicrosecond => &[D, H, M, S, 0],
            Unit::DaySecond => &[D, H, M, S],
            Unit::DayMinute => &[D, H, M],
            Unit::DayHour => &[D, H],
            Unit::YearMonth => &[-12, -1],
            _ => &[],
        }
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// An interval as the servers apply it: whole months, then microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Interval {
    pub(crate) months: i64,
    pub(crate) micros: i64,
}

/// A decimal as a whole number, halves away from zero.
fn dec_whole(d: &Dec) -> Option<i64> {
    let r = d.round(0, Round::HalfUp);
    let m = i64::try_from(r.mag.to_u128()?).ok()?;
    Some(if r.neg { -m } else { m })
}

/// The value of `INTERVAL <amount> <unit>`. `None` when it is beyond
/// any date.
///
/// A simple unit takes a number: whole (a decimal rounds, a string gives
/// its leading integer), except `SECOND`, which keeps its fraction. A
/// composite unit takes text made of numbers with anything between them
/// (`'1 02:30:00'`); when there are fewer numbers than the unit has
/// parts, the leading parts are the missing ones, and the microsecond
/// part reads as a fraction (`'1.5'` is one and a half seconds).
pub(crate) fn interval_of(
    ctx: &mut Ctx<'_>,
    amount: &Node,
    unit: Unit,
    row: &[Value],
) -> Result<Option<Interval>> {
    let textual = matches!(amount.ty.kind, Kind::Str);
    let parts = unit.parts();
    if parts.is_empty() {
        if unit == Unit::Second {
            let Some(d) = amount.dec(ctx, row)? else {
                return Ok(None);
            };
            let scaled = d.mul(&Dec::from_i64(1_000_000)).round(0, Round::HalfUp);
            let Some(m) = scaled.mag.to_u128().and_then(|v| i64::try_from(v).ok()) else {
                return Ok(Some(Interval {
                    months: 0,
                    micros: i64::MAX,
                }));
            };
            return Ok(Some(Interval {
                months: 0,
                micros: if scaled.neg { -m } else { m },
            }));
        }
        let n = if textual {
            // A string gives its leading integer.
            match amount.eval(ctx, row)? {
                Value::Str(s) | Value::Bytes(s) => leading_integer(&s),
                Value::Null => return Ok(None),
                _ => 0,
            }
        } else {
            let Some(d) = amount.dec(ctx, row)? else {
                return Ok(None);
            };
            dec_whole(&d).unwrap_or(if d.neg { i64::MIN / 2 } else { i64::MAX / 2 })
        };
        let big = Interval {
            months: 0,
            micros: i64::MAX,
        };
        return Ok(Some(match unit {
            Unit::Month => Interval {
                months: n,
                micros: 0,
            },
            Unit::Quarter => Interval {
                months: n.saturating_mul(3),
                micros: 0,
            },
            Unit::Year => Interval {
                months: n.saturating_mul(12),
                micros: 0,
            },
            Unit::Microsecond => Interval {
                months: 0,
                micros: n,
            },
            Unit::Minute => n.checked_mul(60_000_000).map_or(big, micros),
            Unit::Hour => n.checked_mul(3_600_000_000).map_or(big, micros),
            Unit::Day => n.checked_mul(MICROS_PER_DAY).map_or(big, micros),
            _ => n.checked_mul(7 * MICROS_PER_DAY).map_or(big, micros),
        }));
    }
    // A composite unit: the amount's text.
    let v = amount.eval(ctx, row)?;
    if v.is_null() {
        return Ok(None);
    }
    // (MariaDB has no interval for an unsigned integer beyond the signed
    // range.)
    if ctx.maria() && matches!(v, Value::UInt(u) if u > i64::MAX as u64) {
        return Ok(None);
    }
    let text = match &v {
        Value::Str(s) | Value::Bytes(s) => s.clone(),
        other => conv::to_text(other, &amount.ty, ctx.maria()).unwrap_or_default(),
    };
    let trimmed: &[u8] = {
        let mut t: &[u8] = &text;
        while t.first().is_some_and(|b| conv::is_space(*b)) {
            t = &t[1..];
        }
        t
    };
    let neg = trimmed.first() == Some(&b'-');
    let mut groups: Vec<&[u8]> = Vec::new();
    let mut i = usize::from(neg);
    while i < trimmed.len() {
        if trimmed[i].is_ascii_digit() {
            let start = i;
            while i < trimmed.len() && trimmed[i].is_ascii_digit() {
                i += 1;
            }
            groups.push(&trimmed[start..i]);
        } else {
            i += 1;
        }
    }
    // More numbers than the unit has parts: no interval.
    if groups.len() > parts.len() {
        if !ctx.maria() {
            ctx.warn(1441, "Datetime function: datetime field overflow");
        }
        return Ok(None);
    }
    let missing = parts.len() - groups.len();
    let mut out = Interval::default();
    for (g, part) in groups.iter().zip(&parts[missing..]) {
        let mut n: i64 = 0;
        for &b in g.iter() {
            n = n.saturating_mul(10).saturating_add(i64::from(b - b'0'));
        }
        match *part {
            // The microsecond part is a fraction: '5' is half a second.
            // MariaDB cuts it at six digits; MySQL takes a longer one as
            // that many microseconds.
            0 => {
                let mut frac: i64 = 0;
                for k in 0..6 {
                    frac = frac * 10 + g.get(k).map_or(0, |b| i64::from(b - b'0'));
                }
                if !ctx.maria() && g.len() > 6 {
                    frac = n;
                }
                out.micros = out.micros.saturating_add(frac);
            }
            p if p < 0 => out.months = out.months.saturating_add(n.saturating_mul(-p)),
            p => out.micros = out.micros.saturating_add(n.saturating_mul(p)),
        }
    }
    if neg {
        out.months = -out.months;
        out.micros = -out.micros;
    }
    Ok(Some(out))
}

fn micros(micros: i64) -> Interval {
    Interval { months: 0, micros }
}

/// The integer a string starts with (after spaces and a sign); 0 for
/// none.
fn leading_integer(s: &[u8]) -> i64 {
    let mut i = 0;
    while i < s.len() && conv::is_space(s[i]) {
        i += 1;
    }
    let neg = s.get(i) == Some(&b'-');
    if neg || s.get(i) == Some(&b'+') {
        i += 1;
    }
    let mut n: i64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        n = n.saturating_mul(10).saturating_add(i64::from(s[i] - b'0'));
        i += 1;
    }
    if neg { -n } else { n }
}

/// Adds an interval to a date and time. `None` when the result leaves
/// years 0 to 9999.
pub(crate) fn add_interval(t: &Tm, iv: Interval, sub: bool) -> Option<Tm> {
    let (months, micros) = if sub {
        (iv.months.checked_neg()?, iv.micros.checked_neg()?)
    } else {
        (iv.months, iv.micros)
    };
    let mut out = *t;
    if months != 0 {
        let total = i64::from(t.year) * 12 + i64::from(t.month) - 1;
        let total = total.checked_add(months)?;
        if !(0..120_000).contains(&total) {
            return None;
        }
        out.year = (total / 12) as u32;
        out.month = (total % 12) as u32 + 1;
        out.day = t.day.min(days_in_month(out.year, out.month));
    }
    if micros != 0 {
        let total = out.datetime_micros().checked_add(micros)?;
        if total < 0 {
            return None;
        }
        let (y, m, d) = date_from_daynr(total / MICROS_PER_DAY)?;
        let rest = total % MICROS_PER_DAY;
        out.year = y;
        out.month = m;
        out.day = d;
        out.hour = (rest / 3_600_000_000) as u32;
        out.minute = (rest / 60_000_000 % 60) as u32;
        out.second = (rest / 1_000_000 % 60) as u32;
        out.micros = (rest % 1_000_000) as u32;
    }
    (out.year <= 9999 && daynr(out.year, out.month, out.day) > 0).then_some(out)
}

/// `DATE_ADD(d, INTERVAL n unit)` and `DATE_SUB(...)`.
///
/// The result's type follows the first argument: a `DATE` stays one for
/// a unit of days or months and becomes a `DATETIME` otherwise; a
/// `DATETIME` stays one; a `TIME` stays a `TIME` where the server can
/// add to a time (MariaDB: anything up to days; MySQL: hours and
/// smaller, and any unit that reaches microseconds; with the others
/// MySQL adds to today's date and MariaDB, beyond days, has no answer);
/// anything else is read as a date and answered as text.
pub(crate) fn date_add(d: Node, n: Node, unit: Unit, sub: bool, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    // The fraction digits the interval brings.
    let ifsp: u8 = if unit.has_micros() {
        6
    } else if unit == Unit::Second {
        match n.ty.kind {
            Kind::Int | Kind::Year | Kind::Bit | Kind::Null => 0,
            Kind::Decimal => n.ty.dec.min(6),
            _ => 6,
        }
    } else {
        0
    };
    #[derive(Clone, Copy, PartialEq)]
    enum Out {
        Date,
        DateTime,
        Time,
        /// A TIME argument MariaDB cannot add this unit to.
        Never,
        Text,
    }
    let mut text_fsp = 0;
    let (ty, out) = match d.ty.kind {
        Kind::Date if unit.is_date_unit() => (ExprType::date(), Out::Date),
        Kind::Date => (ExprType::datetime(ifsp), Out::DateTime),
        Kind::DateTime | Kind::Timestamp => {
            (ExprType::datetime(d.ty.dec.min(6).max(ifsp)), Out::DateTime)
        }
        Kind::Time => {
            let fsp = d.ty.dec.min(6).max(ifsp);
            if maria {
                if matches!(unit, Unit::Week) || unit.is_month_unit() {
                    (ExprType::datetime(fsp), Out::Never)
                } else {
                    (ExprType::time(fsp), Out::Time)
                }
            } else if unit.is_date_unit()
                || matches!(unit, Unit::DayHour | Unit::DayMinute | Unit::DaySecond)
            {
                (ExprType::datetime(fsp), Out::DateTime)
            } else {
                (ExprType::time(fsp), Out::Time)
            }
        }
        _ => {
            // MariaDB's text has as many fraction digits as the argument
            // or the interval brings; MySQL's has six when there is a
            // fraction at all.
            text_fsp = temporal::fsp_of_date(&d, env).max(ifsp);
            let dec = if maria { text_fsp } else { 0 };
            let chars = if maria {
                19 + if dec > 0 { u32::from(dec) + 1 } else { 0 }
            } else {
                29
            };
            let mut ty = ExprType::string(chars, env.conn(), Derivation::Coercible);
            ty.fixed = true;
            if maria {
                ty.dec = dec;
            }
            (ty, Out::Text)
        }
    };
    let fsp = if out == Out::Text {
        if maria { ty.dec.min(6) } else { 6 }
    } else {
        ty.dec.min(6)
    };
    // The computed date or time; for a text result, the value behind it.
    let compute: crate::expr::EvalFn = std::sync::Arc::new(move |ctx, row| {
        let v = d.temporal(ctx, row)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        let Some(iv) = interval_of(ctx, &n, unit, row)? else {
            return Ok(Value::Null);
        };
        let overflow = |ctx: &mut Ctx<'_>, what: &str| {
            ctx.warn(1441, format!("Datetime function: {what} field overflow"));
            Value::Null
        };
        if out == Out::Never {
            return Ok(overflow(ctx, "datetime"));
        }
        if out == Out::Time {
            let Value::Time(t) = v else {
                return Ok(Value::Null);
            };
            let delta = if sub {
                iv.micros.checked_neg()
            } else {
                Some(iv.micros)
            };
            let total = delta.and_then(|x| t.micros().checked_add(x));
            // 838:59:59, with a fraction on MariaDB only.
            let limit = 3_020_399_000_000 + if ctx.maria() { 999_999 } else { 0 };
            return Ok(match total {
                Some(m) if m.abs() <= limit => {
                    sprig_engine::row::Time::from_micros(m).map_or(Value::Null, Value::Time)
                }
                _ => overflow(ctx, "time"),
            });
        }
        let Some(t) = to_datetime_mode(ctx, &v, DateMode::Real) else {
            return Ok(Value::Null);
        };
        // MariaDB's DATE_ADD refuses a day its month lacks (STR_TO_DATE
        // answers one).
        if ctx.maria() && t.day > days_in_month(t.year, t.month) {
            ctx.warn(
                1292,
                format!(
                    "Incorrect datetime value: '{}'",
                    fmt_datetime(t.datetime(), 0)
                ),
            );
            return Ok(Value::Null);
        }
        let Some(r) = add_interval(&t, iv, sub) else {
            let backwards = (iv.micros < 0) != sub || (iv.months < 0) != sub;
            if !ctx.maria() && out == Out::Text && t.year <= 1 && backwards {
                return Ok(Value::Date(sprig_engine::row::Date::new(0, 0, 0)));
            }
            return Ok(overflow(ctx, "datetime"));
        };
        // A result in year 0: none on MariaDB; MySQL's text shows the
        // zero date.
        if r.year == 0 {
            if ctx.maria() || out != Out::Text {
                return Ok(overflow(ctx, "datetime"));
            }
            return Ok(Value::Date(sprig_engine::row::Date::new(0, 0, 0)));
        }
        Ok(match out {
            Out::Date => Value::Date(r.date()),
            Out::Text if !(t.has_time || !unit.is_date_unit()) => Value::Date(r.date()),
            _ => temporal::tm_value(ctx, r, Kind::DateTime, fsp),
        })
    });
    if out != Out::Text {
        return Ok(Node::new(ty.nullable(), "date_add", move |ctx, row| {
            compute(ctx, row)
        }));
    }
    let behind = compute.clone();
    let mut node = Node::new(ty.nullable(), "date_add", move |ctx, row| {
        Ok(match compute(ctx, row)? {
            Value::Date(d) => Value::str(&fmt_date(d)),
            Value::DateTime(dt) => {
                let digits = if maria {
                    fsp
                } else if dt.micros == 0 {
                    0
                } else {
                    6
                };
                Value::str(&fmt_datetime(dt, digits))
            }
            other => other,
        })
    });
    // Among numbers the text is the date or datetime it was computed as
    // (`YYYYMMDDhhmmss.ffffff`), not the number its text starts with.
    let (b1, b2, b3) = (behind.clone(), behind.clone(), behind.clone());
    node.alt = Some(std::sync::Arc::new(crate::expr::Alt {
        temporal: Some(behind),
        temporal_fsp: Some(text_fsp),
        f64: Some(std::sync::Arc::new(move |ctx, row| {
            let v = b1(ctx, row)?;
            Ok(conv::to_f64(ctx, &v))
        })),
        dec: Some(std::sync::Arc::new(move |ctx, row| {
            let v = b2(ctx, row)?;
            Ok(conv::to_dec(ctx, &v))
        })),
        int: Some(std::sync::Arc::new(move |ctx, row| {
            let v = b3(ctx, row)?;
            Ok(conv::to_int(ctx, &v))
        })),
        ..crate::expr::Alt::default()
    }));
    Ok(node)
}

/// `DATEDIFF(a, b)`: whole days from `b` to `a`.
pub(crate) fn datediff(a: Node, b: Node, env: Env<'_>) -> Node {
    Node::new(int_type(7, env), "datediff", move |ctx, row| {
        let (x, y) = (a.temporal(ctx, row)?, b.temporal(ctx, row)?);
        let (x, y) = (
            to_datetime_mode(ctx, &x, DateMode::Real),
            to_datetime_mode(ctx, &y, DateMode::Real),
        );
        Ok(match (x, y) {
            (Some(x), Some(y)) if real_date(&x) && real_date(&y) => {
                Value::Int(daynr(x.year, x.month, x.day) - daynr(y.year, y.month, y.day))
            }
            _ => Value::Null,
        })
    })
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

/// Formats a date and time by a `DATE_FORMAT` pattern. `None` where a
/// specifier needs a part the value doesn't have (a month name of month
/// 0).
pub(crate) fn format(t: &Tm, fmt: &[u8], maria: bool) -> Option<Vec<u8>> {
    use std::io::Write as _;
    // MariaDB has no day of the year for a month 0; MySQL computes one
    // (a month 0 lies before January), and both compute a weekday.
    let no_month = t.month == 0 && (maria || t.year == 0);
    let no_weekday = t.month == 0 && t.year == 0;
    let zero = t.year == 0 && t.month == 0 && t.day == 0;
    let mut out: Vec<u8> = Vec::with_capacity(fmt.len() + 16);
    let hour12 = |h: u32| if h % 12 == 0 { 12 } else { h % 12 };
    let mut i = 0;
    while i < fmt.len() {
        let c = fmt[i];
        i += 1;
        if c != b'%' || i >= fmt.len() {
            out.push(c);
            continue;
        }
        let spec = fmt[i];
        i += 1;
        let _ = match spec {
            b'Y' => write!(out, "{:04}", t.year),
            b'y' => write!(out, "{:02}", t.year % 100),
            b'm' => write!(out, "{:02}", t.month),
            b'c' => write!(out, "{}", t.month),
            b'd' => write!(out, "{:02}", t.day),
            b'e' => write!(out, "{}", t.day),
            b'H' => write!(out, "{:02}", t.hour),
            b'k' => write!(out, "{}", t.hour),
            b'h' | b'I' => write!(out, "{:02}", hour12(t.hour)),
            b'l' => write!(out, "{}", hour12(t.hour)),
            b'i' => write!(out, "{:02}", t.minute),
            b's' | b'S' => write!(out, "{:02}", t.second),
            b'f' => write!(out, "{:06}", t.micros),
            b'p' => write!(out, "{}", if t.hour % 24 < 12 { "AM" } else { "PM" }),
            b'r' => write!(
                out,
                "{:02}:{:02}:{:02} {}",
                hour12(t.hour),
                t.minute,
                t.second,
                if t.hour % 24 < 12 { "AM" } else { "PM" }
            ),
            b'T' => write!(out, "{:02}:{:02}:{:02}", t.hour, t.minute, t.second),
            b'M' => {
                if t.month == 0 {
                    return None;
                }
                write!(out, "{}", MONTHS[t.month as usize - 1])
            }
            b'b' => {
                if t.month == 0 {
                    return None;
                }
                write!(out, "{}", &MONTHS[t.month as usize - 1][..3])
            }
            b'W' | b'a' | b'w' => {
                if no_weekday {
                    return None;
                }
                let wd = weekday(t, false) as usize;
                match spec {
                    b'W' => write!(out, "{}", DAYS[wd]),
                    b'a' => write!(out, "{}", &DAYS[wd][..3]),
                    _ => write!(out, "{}", (wd + 1) % 7),
                }
            }
            b'D' => {
                let suffix = match (t.day % 100, t.day % 10) {
                    (11..=13, _) => "th",
                    (_, 1) => "st",
                    (_, 2) => "nd",
                    (_, 3) => "rd",
                    _ => "th",
                };
                write!(out, "{}{suffix}", t.day)
            }
            b'j' => {
                if no_month {
                    return None;
                }
                // (Padded on the left with zeros, sign and all: `0-6`.)
                let n = daynr(t.year, t.month, t.day) - daynr(t.year, 1, 1) + 1;
                write!(out, "{:0>3}", n.to_string())
            }
            // The week of the all-zero date has no meaning; MariaDB
            // prints what its arithmetic wraps around to.
            b'U' | b'u' | b'V' if zero => write!(out, "613566757"),
            b'v' if zero => write!(out, "01"),
            b'X' if zero => write!(out, "0000"),
            b'x' if zero => write!(out, "0001"),
            b'U' => write!(out, "{:02}", week(t, 0).0),
            b'u' => write!(out, "{:02}", week(t, 1).0),
            b'V' => write!(out, "{:02}", week(t, 2).0),
            b'v' => write!(out, "{:02}", week(t, 3).0),
            b'X' => write!(out, "{:04}", week(t, 2).1),
            b'x' => write!(out, "{:04}", week(t, 3).1),
            other => {
                out.push(other);
                Ok(())
            }
        };
    }
    Some(out)
}

/// `DATE_FORMAT(d, format)`.
pub(crate) fn date_format(d: Node, f: Node, env: Env<'_>) -> Node {
    let f = string_node(f, env);
    let chars = f.ty.len.saturating_mul(10).min(1 << 20);
    let mut ty = ExprType::string(chars, env.conn(), Derivation::Coercible).nullable();
    if env.maria() {
        ty.dec = 0;
    }
    // MariaDB formats a TIME by its own fields: the date fields are
    // zero, the hours can pass 23, and a negative time shows its sign.
    let time_only = env.maria() && d.ty.kind == Kind::Time;
    let maria = env.maria();
    Node::new(ty, "date_format", move |ctx, row| {
        let v = d.temporal(ctx, row)?;
        let t = if time_only {
            to_time(ctx, &v)
        } else {
            to_datetime_mode(ctx, &v, DateMode::Fuzzy)
        };
        let Some(t) = t else {
            return Ok(Value::Null);
        };
        let (Value::Str(pattern) | Value::Bytes(pattern)) = f.eval(ctx, row)? else {
            return Ok(Value::Null);
        };
        if pattern.is_empty() {
            return Ok(Value::Null);
        }
        Ok(match format(&t, &pattern, maria) {
            Some(mut out) => {
                if time_only && t.neg {
                    out.insert(0, b'-');
                }
                Value::Str(Bytes::from(out))
            }
            None => Value::Null,
        })
    })
}

/// The largest `TIMESTAMP` second each server converts.
fn max_unix(maria: bool) -> i64 {
    if maria { 4_294_967_295 } else { 32_536_771_199 }
}

/// `UNIX_TIMESTAMP([d])`: seconds since the epoch of a local date and
/// time; 0 outside the `TIMESTAMP` range.
pub(crate) fn unix_timestamp(arg: Option<Node>, env: Env<'_>) -> Node {
    let offset = i64::from(env.s.time_zone_offset);
    let maria = env.maria();
    let Some(a) = arg else {
        let ty = ExprType {
            prec: 10,
            ..ExprType::int(if maria { 17 } else { 11 }, false)
        };
        return Node::new(ty, "unix_timestamp", |ctx, _| {
            Ok(Value::Int(ctx.session.now_seconds))
        });
    };
    // MySQL gives a string six decimals unless it is a literal that
    // reads as a date (then as many as it writes).
    // A string has six decimals, unless it is a constant that reads as
    // a date: then as many as it writes (both servers compute a constant
    // while preparing).
    let reads_as_date = |sv: &[u8]| {
        let scratch = Ctx::new(env.s);
        temporal::parse_datetime_mode(&scratch, sv, DateMode::Loose)
            .is_some_and(|(t, _)| t.month == 0 || t.day <= days_in_month(t.year, t.month))
    };
    let has_own_fsp = a.alt.as_ref().is_some_and(|x| x.temporal_fsp.is_some());
    let constant_text = if a.ty.kind == Kind::Str && !has_own_fsp && (a.lit.is_some() || a.konst) {
        let mut scratch = Ctx::new(env.s);
        match a.eval(&mut scratch, &[]) {
            Ok(Value::Str(s) | Value::Bytes(s)) => Some(s),
            _ => None,
        }
    } else {
        None
    };
    let fsp = if let Some(sv) = &constant_text {
        if !reads_as_date(sv) {
            6
        } else if a.lit.is_some() {
            temporal::fsp_of(&a, env)
        } else {
            let after = sv.rsplit(|&b| b == b'.').next().unwrap_or(&[]);
            if after.len() == sv.len() {
                0
            } else {
                after
                    .iter()
                    .take_while(|b| b.is_ascii_digit())
                    .count()
                    .min(6) as u8
            }
        }
    } else if !maria && a.ty.kind == Kind::Str && !has_own_fsp {
        6
    } else {
        temporal::fsp_of(&a, env)
    };
    let ty = if fsp == 0 {
        ExprType {
            prec: 10,
            ..ExprType::int(17, false)
        }
    } else {
        ExprType::decimal(12 + u32::from(fsp), u32::from(fsp), false)
    }
    .nullable();
    Node::new(ty, "unix_timestamp", move |ctx, row| {
        let v = a.temporal(ctx, row)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        // What is no date, the zero date, and a time outside the range:
        // NULL on MariaDB, 0 on MySQL.
        let t = to_datetime_mode(ctx, &v, DateMode::Real);
        let secs = match &t {
            Some(t) if t.month != 0 && t.day != 0 => {
                t.datetime_micros() / 1_000_000 - 719_528 * 86_400 - offset
            }
            _ => 0,
        };
        let (secs, micros) = if secs <= 0 || secs > max_unix(maria) {
            if maria {
                return Ok(Value::Null);
            }
            (0, 0)
        } else {
            (secs, t.map_or(0, |t| t.micros))
        };
        if fsp == 0 {
            return Ok(Value::Int(secs));
        }
        let d = Dec::from_i64(secs * 1_000_000 + i64::from(micros));
        let d = Dec { scale: 6, ..d }.round(i32::from(fsp), Round::Truncate);
        Ok(conv::dec_value(&d))
    })
}

/// `FROM_UNIXTIME(n [, format])`: the session-zone date and time of an
/// epoch second; `NULL` when negative or beyond the `TIMESTAMP` range.
pub(crate) fn from_unixtime(n: Node, f: Option<Node>, env: Env<'_>) -> Node {
    let offset = i64::from(env.s.time_zone_offset);
    let maria = env.maria();
    let fsp = match n.ty.kind {
        Kind::Decimal => n.ty.dec.min(6),
        Kind::Int | Kind::Year | Kind::Bit | Kind::Null => 0,
        _ => 6,
    };
    let f = f.map(|f| string_node(f, env));
    let ty = match &f {
        Some(f) => ExprType::string(
            f.ty.len.saturating_mul(10).min(1 << 20),
            env.conn(),
            Derivation::Coercible,
        ),
        None if maria => ExprType {
            kind: Kind::Timestamp,
            ..ExprType::datetime(fsp)
        },
        None => ExprType::datetime(fsp),
    }
    .nullable();
    Node::new(ty, "from_unixtime", move |ctx, row| {
        let Some(d) = n.dec(ctx, row)? else {
            return Ok(Value::Null);
        };
        if d.neg && !d.is_zero() {
            return Ok(Value::Null);
        }
        let micros_total = d.mul(&Dec::from_i64(1_000_000)).round(0, Round::Truncate);
        let Some(total) = micros_total
            .mag
            .to_u128()
            .and_then(|v| i64::try_from(v).ok())
        else {
            return Ok(Value::Null);
        };
        let secs = total / 1_000_000;
        if secs > max_unix(maria) || (maria && total == 0) {
            return Ok(Value::Null);
        }
        let dt = temporal::datetime_from_unix(secs + offset, (total % 1_000_000) as u32);
        let t = Tm::from_datetime(dt);
        match &f {
            None => Ok(temporal::tm_value(ctx, cut(t, fsp), Kind::DateTime, fsp)),
            Some(f) => {
                let (Value::Str(pattern) | Value::Bytes(pattern)) = f.eval(ctx, row)? else {
                    return Ok(Value::Null);
                };
                Ok(match format(&t, &pattern, maria) {
                    Some(out) => Value::Str(Bytes::from(out)),
                    None => Value::Null,
                })
            }
        }
    })
}

/// `DAYNAME(d)` and `MONTHNAME(d)`.
pub(crate) fn name_of(name: &str, a: Node, env: Env<'_>) -> Node {
    let month = name == "MONTHNAME";
    let ty = ExprType {
        dec: if env.maria() {
            0
        } else {
            crate::types::NOT_FIXED
        },
        ..ExprType::string(9, env.conn(), Derivation::Coercible)
    }
    .nullable();
    // MySQL's DAYNAME read as a number is the weekday's (0 for Monday).
    let a_num = a.clone();
    let weekday_number: Option<crate::expr::EvalFn> = (!env.maria() && !month).then(|| {
        let f: crate::expr::EvalFn = std::sync::Arc::new(move |ctx, row| {
            let v = a_num.temporal(ctx, row)?;
            Ok(match to_datetime_mode(ctx, &v, DateMode::Real) {
                Some(t) if real_date(&t) => Value::Int(weekday(&t, false)),
                _ => Value::Null,
            })
        });
        f
    });
    let mut node = Node::new(ty, "dayname", move |ctx, row| {
        let v = a.temporal(ctx, row)?;
        let mode = if month {
            DateMode::Fuzzy
        } else {
            DateMode::Real
        };
        let Some(t) = to_datetime_mode(ctx, &v, mode) else {
            return Ok(Value::Null);
        };
        Ok(if month {
            if t.month == 0 {
                Value::Null
            } else {
                Value::str(MONTHS[t.month as usize - 1])
            }
        } else if real_date(&t) {
            Value::str(DAYS[weekday(&t, false) as usize])
        } else {
            Value::Null
        })
    });
    if let Some(w) = weekday_number {
        let (w1, w2, w3) = (w.clone(), w.clone(), w);
        node.alt = Some(std::sync::Arc::new(crate::expr::Alt {
            f64: Some(std::sync::Arc::new(move |ctx, row| {
                let v = w1(ctx, row)?;
                Ok(conv::to_f64(ctx, &v))
            })),
            dec: Some(std::sync::Arc::new(move |ctx, row| {
                let v = w2(ctx, row)?;
                Ok(conv::to_dec(ctx, &v))
            })),
            int: Some(std::sync::Arc::new(move |ctx, row| {
                let v = w3(ctx, row)?;
                Ok(conv::to_int(ctx, &v))
            })),
            ..crate::expr::Alt::default()
        }));
    }
    node
}

/// `WEEK(d [, mode])` and `YEARWEEK(d [, mode])`.
pub(crate) fn week_fn(name: &str, d: Node, mode: Option<Node>, env: Env<'_>) -> Node {
    let yearweek = name == "YEARWEEK";
    let default_mode = i64::from(env.s.default_week_format);
    let len = if yearweek { 6 } else { 2 };
    Node::new(int_type(len, env), "week", move |ctx, row| {
        let v = d.temporal(ctx, row)?;
        let m = match &mode {
            None => {
                if yearweek {
                    0
                } else {
                    default_mode
                }
            }
            // A NULL mode counts as 0.
            Some(m) => m.int(ctx, row)?.map_or(0, |i| i.v),
        };
        let Some(t) = to_datetime_mode(ctx, &v, DateMode::Real) else {
            return Ok(Value::Null);
        };
        Ok(Value::Int(if yearweek {
            let (w, y) = week(&t, m | 2);
            i64::from(y) * 100 + w
        } else {
            week(&t, m).0
        }))
    })
}

/// `EXTRACT(unit FROM d)`: the unit's fields written one after the other
/// as a number.
///
/// The date units (`YEAR`, `MONTH`, `QUARTER`, `WEEK`, `YEAR_MONTH`, and
/// `DAY` of a date) read the argument as a date. For the others the
/// servers differ: MySQL reads the argument as a time (so a datetime's
/// day does not count, and hours run to 838); MariaDB keeps a date's day
/// of the month and reads everything else as a duration whose hours
/// beyond 23 are days (`EXTRACT(DAY FROM 20240115)` is 84).
pub(crate) fn extract(unit: Unit, d: Node, env: Env<'_>) -> Node {
    let date_unit = matches!(
        unit,
        Unit::Year | Unit::Month | Unit::Quarter | Unit::Week | Unit::YearMonth
    );
    let len = match unit {
        Unit::DayMicrosecond => 20,
        Unit::HourMicrosecond => 13,
        Unit::MinuteMicrosecond => 11,
        Unit::SecondMicrosecond | Unit::DaySecond => 9,
        Unit::DayMinute | Unit::HourSecond => 7,
        Unit::Microsecond | Unit::YearMonth => 7,
        Unit::DayHour | Unit::HourMinute | Unit::MinuteSecond | Unit::Year => 5,
        _ => 3,
    };
    let maria = env.maria();
    // MySQL counts the day of a column's date and of DATE_ADD's result.
    let column = d.ty.column || &*d.text == "date_add";
    // A text that stands for a computed date or datetime (DATE_ADD's).
    let dated_alt = d.alt.as_ref().is_some_and(|a| a.temporal.is_some());
    Node::new(int_type(len, env), "extract", move |ctx, row| {
        let v = if date_unit || unit == Unit::Day {
            d.temporal(ctx, row)?
        } else {
            // (MariaDB's EXTRACT reads a cast of what is no date as the
            // NULL it is, unlike HOUR() and its kin.)
            d.temporal(ctx, row)?
        };
        if v.is_null() {
            return Ok(Value::Null);
        }
        let dated_text = |b: &[u8]| {
            let body: Vec<u8> = b
                .iter()
                .copied()
                .skip_while(|c| conv::is_space(*c))
                .collect();
            body.first() != Some(&b'-')
                && body
                    .iter()
                    .take_while(|&&c| c != b' ' && c != b'T')
                    .filter(|&&c| c == b'-')
                    .count()
                    >= 2
        };
        // A duration: hours beyond 23 become days.
        let split = |mut t: Tm| {
            t.day = t.hour / 24;
            t.hour %= 24;
            t
        };
        let t: Option<Tm> = if date_unit {
            match &v {
                // MariaDB: a TIME has no date.
                Value::Time(_) if maria => Some(Tm::default()),
                _ => to_datetime_mode(ctx, &v, DateMode::Fuzzy),
            }
        } else if !maria {
            // MySQL counts the day of a date column and of a text that is
            // a full date and time; anything else is read as a time.
            let full = |b: &[u8]| dated_text(b) && b.contains(&b':') && !b.contains(&b'T');
            // ... and of a date that more text follows after a space
            // ('2024-01-15 abc' is the 15th, midnight), when the date
            // itself reads; a bare date is read as a time.
            let spaced = |b: &[u8]| {
                let body = b.trim_ascii();
                dated_text(body) && !body.contains(&b'T') && body.contains(&b' ')
            };
            // Twelve or more digits in a row, or a date and a time of
            // digits around a `T`, are a date and time too.
            let compact = |b: &[u8]| {
                let body: Vec<u8> = b
                    .iter()
                    .copied()
                    .skip_while(|c| conv::is_space(*c))
                    .collect();
                let run = body.iter().take_while(|c| c.is_ascii_digit()).count();
                run >= 12
                    || (run >= 6
                        && body.get(run) == Some(&b'T')
                        && body.get(run + 1).is_some_and(u8::is_ascii_digit))
            };
            let mut probe = Ctx::new(ctx.session);
            // (So is a whole number of eleven digits or more.)
            let compact_number = match &v {
                Value::Int(i) => *i >= 10_000_000_000,
                Value::UInt(u) => *u >= 10_000_000_000,
                _ => false,
            };
            let compact_text = matches!(&v, Value::Str(b) | Value::Bytes(b) if compact(b));
            match &v {
                _ if unit == Unit::Day => to_datetime_mode(ctx, &v, DateMode::Fuzzy),
                _ if compact_text || compact_number => {
                    // What is no real date there is no time either.
                    let t = to_datetime_mode(&mut probe, &v, DateMode::Real);
                    if t.is_none() {
                        let text = match &v {
                            Value::Str(b) | Value::Bytes(b) => b.clone(),
                            other => conv::to_text(other, &d.ty, false).unwrap_or_default(),
                        };
                        ctx.warn(
                            1292,
                            format!(
                                "Truncated incorrect time value: '{}'",
                                conv::err_text(&text)
                            ),
                        );
                    }
                    t
                }
                Value::Date(_) | Value::DateTime(_) if column => {
                    to_datetime_mode(ctx, &v, DateMode::Fuzzy)
                }
                // MySQL counts the day of DATE_ADD's datetime text, as of a
                // text that is a full date and time.
                Value::DateTime(_) if dated_alt => to_datetime_mode(ctx, &v, DateMode::Fuzzy),
                Value::Str(b) | Value::Bytes(b)
                    if (full(b) || spaced(b))
                        && temporal::parse_datetime_mode(&probe, b, DateMode::Loose).is_some() =>
                {
                    to_datetime_mode(ctx, &v, DateMode::Fuzzy)
                }
                _ => to_time(ctx, &v),
            }
        } else {
            let mut probe = Ctx::new(ctx.session);
            match &v {
                Value::Date(_) | Value::DateTime(_) => to_datetime_mode(ctx, &v, DateMode::Fuzzy),
                Value::Time(t) => Some(split(Tm::from_time(*t))),
                // A string of digits is a date, or nothing, for a unit with
                // a day part but no microseconds; DAY_MICROSECOND reads it
                // as a duration ('20240115' is 2024:01:15, 84 days) unless
                // it has the twelve digits of a date and time.
                Value::Str(b) | Value::Bytes(b)
                    if unit.has_days()
                        && (unit != Unit::DayMicrosecond || b.trim_ascii().len() >= 12)
                        && digits_only(b) =>
                {
                    let t = to_datetime_mode(&mut probe, &v, DateMode::Fuzzy);
                    if t.is_none() {
                        ctx.warn(
                            1292,
                            format!("Incorrect interval value: '{}'", conv::err_text(b)),
                        );
                    }
                    t
                }
                // A unit with a day part tries a date first (but digits
                // with a fraction are a duration).
                Value::Str(b) | Value::Bytes(b)
                    if dated_text(b)
                        || (unit.has_days()
                            && !digits_only(b)
                            && !digits_and_fraction(b)
                            && to_datetime_mode(&mut probe, &v, DateMode::Fuzzy).is_some()) =>
                {
                    to_datetime_mode(ctx, &v, DateMode::Fuzzy)
                }
                Value::Str(b) | Value::Bytes(b) => match temporal::parse_time_clip(ctx, b, false) {
                    Some((t, _)) => Some(split(t)),
                    None => {
                        ctx.warn(
                            1292,
                            format!("Incorrect time value: '{}'", conv::err_text(b)),
                        );
                        None
                    }
                },
                // A number of thirteen digits or more is a date and time
                // when it reads as one; any other is hours, minutes and
                // seconds, with the hours of 9999 years at most.
                other => {
                    let Some(n) = conv::to_dec(ctx, other) else {
                        return Ok(Value::Null);
                    };
                    let whole = n.abs().round(0, Round::Truncate);
                    let digits = whole.mag.to_u128().and_then(|x| u64::try_from(x).ok());
                    let frac = n.abs().sub(&whole).round(6, Round::Truncate);
                    let micros = frac
                        .mag
                        .mul_pow10(6u32.saturating_sub(frac.scale))
                        .to_u128()
                        .unwrap_or(0) as u32;
                    // (The digits are taken as they stand: year 100 and
                    // the 31st of February pass.)
                    let dated = digits
                        .filter(|d| *d >= 1_000_000_000_000 && !n.neg)
                        .and_then(|d| {
                            let (date, time) = (d / 1_000_000, d % 1_000_000);
                            let t = Tm {
                                year: u32::try_from(date / 10_000).ok()?,
                                month: (date / 100 % 100) as u32,
                                day: (date % 100) as u32,
                                hour: (time / 10_000) as u32,
                                minute: (time / 100 % 100) as u32,
                                second: (time % 100) as u32,
                                micros,
                                has_date: true,
                                has_time: true,
                                neg: false,
                            };
                            (t.year <= 9999
                                && t.month <= 12
                                && t.day <= 31
                                && t.hour <= 23
                                && t.minute <= 59
                                && t.second <= 59)
                                .then_some(t)
                        });
                    if dated.is_some() {
                        dated
                    } else {
                        // (Microseconds of 999999 are no time to MariaDB.)
                        let digits = digits.filter(|_| micros != 999_999);
                        let t = digits.and_then(|d| {
                            let hour = u32::try_from(d / 10_000).ok()?;
                            let t = Tm {
                                neg: n.neg,
                                hour,
                                minute: (d / 100 % 100) as u32,
                                second: (d % 100) as u32,
                                micros,
                                has_time: true,
                                ..Tm::default()
                            };
                            (hour <= 87_649_415 && t.minute <= 59 && t.second <= 59)
                                .then(|| split(t))
                        });
                        if t.is_none() {
                            let text = conv::to_text(other, &d.ty, true).unwrap_or_default();
                            ctx.warn(
                                1292,
                                format!("Incorrect interval value: '{}'", conv::err_text(&text)),
                            );
                        }
                        t
                    }
                }
            }
        };
        let Some(t) = t else {
            return Ok(Value::Null);
        };
        let (y, mo, da) = (i64::from(t.year), i64::from(t.month), i64::from(t.day));
        let (h, mi, se, us) = (
            i64::from(t.hour),
            i64::from(t.minute),
            i64::from(t.second),
            i64::from(t.micros),
        );
        // (Hours read from a number can be far beyond a time's: the
        // arithmetic wraps rather than fails.)
        let hms = h.wrapping_mul(10_000) + mi * 100 + se;
        let mega = |x: i64| x.wrapping_mul(1_000_000);
        let n = match unit {
            Unit::Year => y,
            Unit::Month => mo,
            Unit::Day => da,
            Unit::Quarter => (mo + 2) / 3,
            // (MariaDB's arithmetic wraps around for the all-zero date.)
            Unit::Week if maria && y == 0 && mo == 0 && da == 0 => 613_566_757,
            Unit::Week => week(&t, i64::from(ctx.session.default_week_format)).0,
            Unit::YearMonth => y * 100 + mo,
            Unit::Hour => h,
            Unit::Minute => mi,
            Unit::Second => se,
            Unit::Microsecond => us,
            Unit::DayHour => (da * 100).wrapping_add(h),
            Unit::DayMinute => (da * 10_000).wrapping_add(h.wrapping_mul(100)) + mi,
            Unit::DaySecond => mega(da).wrapping_add(hms),
            Unit::DayMicrosecond => mega(mega(da).wrapping_add(hms)).wrapping_add(us),
            Unit::HourMinute => h.wrapping_mul(100) + mi,
            Unit::HourSecond => hms,
            Unit::HourMicrosecond => mega(hms).wrapping_add(us),
            Unit::MinuteSecond => mi * 100 + se,
            Unit::MinuteMicrosecond => (mi * 100 + se) * 1_000_000 + us,
            Unit::SecondMicrosecond => se * 1_000_000 + us,
        };
        Ok(Value::Int(if t.neg { n.wrapping_neg() } else { n }))
    })
}

/// Whether a text is digits, a point and digits (around spaces).
fn digits_and_fraction(b: &[u8]) -> bool {
    match b.trim_ascii().split(|&c| c == b'.').collect::<Vec<_>>()[..] {
        [i, f] => {
            !i.is_empty() && i.iter().all(u8::is_ascii_digit) && f.iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

/// Whether a text is digits only (around spaces).
fn digits_only(b: &[u8]) -> bool {
    let body = b.trim_ascii();
    !body.is_empty() && body.iter().all(u8::is_ascii_digit)
}

/// `TIMESTAMPDIFF(unit, a, b)`: how many whole units lie from `a` to
/// `b`, toward zero. Months count calendar months: a month is whole when
/// the day and time of `b` have reached those of `a`.
pub(crate) fn timestampdiff(unit: Unit, a: Node, b: Node) -> Node {
    let ty = ExprType::bigint(false).nullable();
    Node::new(ty, "timestampdiff", move |ctx, row| {
        let (x, y) = (a.temporal(ctx, row)?, b.temporal(ctx, row)?);
        let (x, y) = (
            to_datetime_mode(ctx, &x, DateMode::Real),
            to_datetime_mode(ctx, &y, DateMode::Real),
        );
        let (Some(x), Some(y)) = (x, y) else {
            return Ok(Value::Null);
        };
        let micros = y.datetime_micros() - x.datetime_micros();
        let months = || {
            let (lo, hi, sign) = if micros >= 0 {
                (&x, &y, 1)
            } else {
                (&y, &x, -1)
            };
            let mut m = (i64::from(hi.year) - i64::from(lo.year)) * 12 + i64::from(hi.month)
                - i64::from(lo.month);
            let rest = |t: &Tm| {
                (i64::from(t.day) * 86_400
                    + i64::from(t.hour) * 3600
                    + i64::from(t.minute) * 60
                    + i64::from(t.second))
                    * 1_000_000
                    + i64::from(t.micros)
            };
            if rest(hi) < rest(lo) {
                m -= 1;
            }
            m * sign
        };
        Ok(Value::Int(match unit {
            Unit::Microsecond => micros,
            Unit::Second => micros / 1_000_000,
            Unit::Minute => micros / 60_000_000,
            Unit::Hour => micros / 3_600_000_000,
            Unit::Day => micros / MICROS_PER_DAY,
            Unit::Week => micros / (7 * MICROS_PER_DAY),
            Unit::Month => months(),
            Unit::Quarter => months() / 3,
            Unit::Year => months() / 12,
            _ => return Ok(Value::Null),
        }))
    })
}

/// A time zone given as an offset (`+05:30`), in seconds east of UTC.
/// `SYSTEM` and `UTC` count as UTC; any other named zone is not known
/// here (there is no time zone database).
fn zone_offset(s: &[u8], maria: bool) -> Option<i64> {
    if s.eq_ignore_ascii_case(b"SYSTEM") || s.eq_ignore_ascii_case(b"UTC") {
        return Some(0);
    }
    let (sign, rest) = match s.first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let colon = rest.iter().position(|&b| b == b':')?;
    let num = |b: &[u8]| -> Option<i64> {
        if b.is_empty() || b.len() > 2 || !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(b).ok()?.parse().ok()
    };
    let (h, m) = (num(&rest[..colon])?, num(&rest[colon + 1..])?);
    if m > 59 {
        return None;
    }
    let secs = h * 3600 + m * 60;
    // MariaDB takes -12:59 to +13:00, MySQL -13:59 to +14:00.
    let east = if maria { 13 } else { 14 } * 3600;
    let ok = if sign > 0 { secs <= east } else { secs < east };
    ok.then_some(sign * secs)
}

/// `CONVERT_TZ(dt, from, to)` between zones given as offsets. A value
/// outside the `TIMESTAMP` range comes back as it is.
pub(crate) fn convert_tz(d: Node, from: Node, to: Node, env: Env<'_>) -> Node {
    let fsp = temporal::fsp_of_date(&d, env);
    let (from, to) = (string_node(from, env), string_node(to, env));
    let maria = env.maria();
    Node::new(
        ExprType::datetime(fsp).nullable(),
        "convert_tz",
        move |ctx, row| {
            let v = d.temporal(ctx, row)?;
            let (f, t) = (from.eval(ctx, row)?, to.eval(ctx, row)?);
            let Some(tm) = to_datetime_mode(ctx, &v, DateMode::Real) else {
                return Ok(Value::Null);
            };
            let (Value::Str(f) | Value::Bytes(f)) = f else {
                return Ok(Value::Null);
            };
            let (Value::Str(t) | Value::Bytes(t)) = t else {
                return Ok(Value::Null);
            };
            let (Some(f), Some(t)) = (zone_offset(&f, maria), zone_offset(&t, maria)) else {
                return Ok(Value::Null);
            };
            let secs = tm.datetime_micros() / 1_000_000 - 719_528 * 86_400 - f;
            let last = if maria { 4_294_967_295 } else { 32_536_771_199 };
            if secs <= 0 || secs > last {
                return Ok(temporal::tm_value(ctx, tm, Kind::DateTime, fsp));
            }
            let dt = temporal::datetime_from_unix(secs + t, tm.micros);
            Ok(temporal::tm_value(
                ctx,
                Tm::from_datetime(dt),
                Kind::DateTime,
                fsp,
            ))
        },
    )
}

/// `STR_TO_DATE(text, format)`: reads `text` by a `DATE_FORMAT` pattern.
/// The result is a `DATE`, a `TIME` or a `DATETIME` by what a constant
/// pattern names (a `DATETIME` otherwise); text that does not fit the
/// pattern gives `NULL` with warning 1411.
pub(crate) fn str_to_date(sv: Node, f: Node, env: Env<'_>) -> Result<Node> {
    let (sv, f) = (string_node(sv, env), string_node(f, env));
    let pattern = match &f.lit {
        Some(Value::Str(p) | Value::Bytes(p)) => Some(p.clone()),
        _ => None,
    };
    let (has_date, has_time, has_frac) = match &pattern {
        Some(p) => {
            let spec = |set: &[u8]| p.windows(2).any(|w| w[0] == b'%' && set.contains(&w[1]));
            (
                spec(b"YymcdeMbWaDjUuVvXxw"),
                spec(b"HkhIliSsfTrp"),
                spec(b"f"),
            )
        }
        None => (true, true, true),
    };
    let fsp = if has_frac { 6 } else { 0 };
    let (ty, kind) = match (has_date, has_time) {
        (true, false) => (ExprType::date(), Kind::Date),
        (false, true) => (ExprType::time(fsp), Kind::Time),
        _ => (ExprType::datetime(fsp), Kind::DateTime),
    };
    Ok(Node::new(ty.nullable(), "str_to_date", move |ctx, row| {
        let (Value::Str(text) | Value::Bytes(text)) = sv.eval(ctx, row)? else {
            return Ok(Value::Null);
        };
        let (Value::Str(pat) | Value::Bytes(pat)) = f.eval(ctx, row)? else {
            return Ok(Value::Null);
        };
        let bad = |ctx: &mut Ctx<'_>| {
            ctx.warn(
                1411,
                format!(
                    "Incorrect datetime value: '{}' for function str_to_date",
                    conv::err_text(&text)
                ),
            );
            Value::Null
        };
        let Some((t, rest)) = parse_by_format(&text, &pat) else {
            return Ok(bad(ctx));
        };
        if rest {
            let what = match kind {
                Kind::Date => "date",
                Kind::Time => "time",
                _ => "datetime",
            };
            ctx.warn(
                1292,
                format!(
                    "Truncated incorrect {what} value: '{}'",
                    conv::err_text(&text)
                ),
            );
        }
        if kind != Kind::Time {
            let mode = ctx.session.sql_mode;
            let zero_date = t.year == 0 && t.month == 0 && t.day == 0;
            let partial = !zero_date && (t.month == 0 || t.day == 0);
            let refused = t.month > 12
                || t.day > 31
                || (!ctx.maria() && t.month != 0 && t.day > days_in_month(t.year, t.month))
                || (zero_date && mode.has(crate::session::SqlMode::NO_ZERO_DATE))
                || (partial && mode.has(crate::session::SqlMode::NO_ZERO_IN_DATE));
            if refused {
                return Ok(bad(ctx));
            }
        }
        Ok(temporal::tm_value(ctx, t, kind, fsp))
    }))
}

/// Reads `text` by `pattern`: the fields, and whether text was left
/// over. `None` when it does not fit. A text that ends before the pattern
/// does is fine, the remaining fields zero (whether a date with zero
/// parts is accepted is the caller's question).
fn parse_by_format(text: &[u8], pattern: &[u8]) -> Option<(Tm, bool)> {
    let mut t = Tm {
        has_date: true,
        has_time: true,
        ..Tm::default()
    };
    let mut i = 0;
    let mut p = 0;
    let mut pm: Option<bool> = None;
    let mut hour12 = false;
    let mut yday: Option<u32> = None;
    let skip_space = |i: &mut usize| {
        while *i < text.len() && conv::is_space(text[*i]) {
            *i += 1;
        }
    };
    let number = |i: &mut usize, max: usize| -> Option<u32> {
        let start = *i;
        let mut v = 0u32;
        while *i < text.len() && *i - start < max && text[*i].is_ascii_digit() {
            v = v
                .saturating_mul(10)
                .saturating_add(u32::from(text[*i] - b'0'));
            *i += 1;
        }
        (*i > start).then_some(v)
    };
    let word = |i: &mut usize, names: &[&str], short: bool| -> Option<u32> {
        for (k, name) in names.iter().enumerate() {
            let n = if short { &name[..3] } else { name };
            if text.len() >= *i + n.len()
                && text[*i..*i + n.len()].eq_ignore_ascii_case(n.as_bytes())
            {
                *i += n.len();
                return Some(k as u32);
            }
        }
        None
    };
    while p < pattern.len() {
        if i >= text.len() {
            break;
        }
        let c = pattern[p];
        p += 1;
        if c != b'%' || p >= pattern.len() {
            if conv::is_space(c) {
                skip_space(&mut i);
            } else {
                skip_space(&mut i);
                if text.get(i) != Some(&c) {
                    return None;
                }
                i += 1;
            }
            continue;
        }
        let spec = pattern[p];
        p += 1;
        skip_space(&mut i);
        match spec {
            b'Y' => {
                // One or two digits are a two-digit year (70 to 99 in the
                // 1900s, the rest in the 2000s).
                let start = i;
                let y = number(&mut i, 4)?;
                t.year = if i - start <= 2 {
                    if y < 70 { 2000 + y } else { 1900 + y }
                } else {
                    y
                };
            }
            b'y' => {
                let y = number(&mut i, 2)?;
                t.year = if y < 70 { 2000 + y } else { 1900 + y };
            }
            b'm' | b'c' => t.month = number(&mut i, 2)?,
            b'd' | b'e' => t.day = number(&mut i, 2)?,
            b'D' => {
                t.day = number(&mut i, 2)?;
                // st, nd, rd, th
                if text.len() >= i + 2 && text[i].is_ascii_alphabetic() {
                    i += 2;
                }
            }
            b'H' | b'k' => t.hour = number(&mut i, 2)?,
            b'h' | b'I' | b'l' => {
                t.hour = number(&mut i, 2)?;
                hour12 = true;
            }
            b'i' => t.minute = number(&mut i, 2)?,
            b's' | b'S' => t.second = number(&mut i, 2)?,
            b'f' => {
                let start = i;
                let mut v = number(&mut i, 6)?;
                for _ in (i - start)..6 {
                    v = v.saturating_mul(10);
                }
                t.micros = v.min(999_999);
            }
            b'p' => {
                let two = text.get(i..i + 2)?;
                pm = Some(if two.eq_ignore_ascii_case(b"PM") {
                    true
                } else if two.eq_ignore_ascii_case(b"AM") {
                    false
                } else {
                    return None;
                });
                i += 2;
            }
            b'M' => t.month = word(&mut i, &MONTHS, false)? + 1,
            b'b' => t.month = word(&mut i, &MONTHS, true)? + 1,
            b'W' => {
                word(&mut i, &DAYS, false)?;
            }
            b'a' => {
                word(&mut i, &DAYS, true)?;
            }
            b'j' => yday = Some(number(&mut i, 3)?),
            b'T' => {
                t.hour = number(&mut i, 2)?;
                if text.get(i) != Some(&b':') {
                    return None;
                }
                i += 1;
                t.minute = number(&mut i, 2)?;
                if text.get(i) != Some(&b':') {
                    return None;
                }
                i += 1;
                t.second = number(&mut i, 2)?;
            }
            b'%' => {
                if text.get(i) != Some(&b'%') {
                    return None;
                }
                i += 1;
            }
            _ => return None,
        }
    }
    if hour12 {
        if t.hour > 12 || t.hour == 0 {
            return None;
        }
        t.hour = t.hour % 12 + if pm == Some(true) { 12 } else { 0 };
    }
    if let Some(n) = yday {
        if n == 0 || t.year == 0 {
            return None;
        }
        let (y, m, d) = date_from_daynr(daynr(t.year, 1, 1) + i64::from(n) - 1)?;
        t.year = y;
        t.month = m;
        t.day = d;
    }
    if t.hour > 23 || t.minute > 59 || t.second > 59 {
        return None;
    }
    let rest = text[i..].iter().any(|b| !conv::is_space(*b));
    Some((t, rest))
}
