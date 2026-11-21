//! Dates and times as the servers store them: fields, not instants.
//!
//! A `DATE` may be `0000-00-00` or have a zero month or day, a `TIME` is a
//! signed duration of up to 838 hours, a `DATETIME` has no zone, and a
//! `TIMESTAMP` is seconds since the Unix epoch in UTC. Which of those values
//! a statement may store is the SQL layer's rule; these types hold all of
//! them.

use std::fmt;

/// Microseconds in a second.
pub const MICROS_PER_SEC: i64 = 1_000_000;

/// A calendar date. The derived order is chronological, with partial and
/// zero dates in field order (`0000-00-00` first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Date {
    /// 0 to 9999.
    pub year: u16,
    /// 0 to 12; 0 in a partial or zero date.
    pub month: u8,
    /// 0 to 31; 0 in a partial or zero date.
    pub day: u8,
}

impl Date {
    /// A date from its fields.
    #[must_use]
    pub const fn new(year: u16, month: u8, day: u8) -> Self {
        Self { year, month, day }
    }

    /// Whether the fields are in range (year to 9999, month to 12, day to
    /// 31). The calendar isn't checked: `2023-02-31` is storable under
    /// `ALLOW_INVALID_DATES`.
    #[must_use]
    pub const fn is_storable(self) -> bool {
        self.year <= 9999 && self.month <= 12 && self.day <= 31
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// A date and a time of day, without a zone. The derived order is
/// chronological.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct DateTime {
    /// The date.
    pub date: Date,
    /// 0 to 23.
    pub hour: u8,
    /// 0 to 59.
    pub minute: u8,
    /// 0 to 59.
    pub second: u8,
    /// 0 to 999,999.
    pub micros: u32,
}

impl DateTime {
    /// A datetime from its fields.
    #[must_use]
    pub const fn new(date: Date, hour: u8, minute: u8, second: u8, micros: u32) -> Self {
        Self {
            date,
            hour,
            minute,
            second,
            micros,
        }
    }

    /// Whether the fields are in range.
    #[must_use]
    pub const fn is_storable(self) -> bool {
        self.date.is_storable()
            && self.hour <= 23
            && self.minute <= 59
            && self.second <= 59
            && self.micros <= 999_999
    }
}

impl fmt::Display for DateTime {
    /// With six fraction digits when the fraction isn't zero.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {:02}:{:02}:{:02}",
            self.date, self.hour, self.minute, self.second
        )?;
        if self.micros != 0 {
            write!(f, ".{:06}", self.micros)?;
        }
        Ok(())
    }
}

/// A `TIME`: a signed duration from `-838:59:59.999999` to
/// `838:59:59.999999`, held in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time {
    micros: i64,
}

impl Time {
    /// The largest magnitude, `838:59:59.999999`, in microseconds.
    pub const MAX_MICROS: i64 = (838 * 3600 + 59 * 60 + 59) * MICROS_PER_SEC + 999_999;

    /// A time from signed microseconds, or `None` beyond ±838:59:59.999999.
    #[must_use]
    pub const fn from_micros(micros: i64) -> Option<Self> {
        if micros < -Self::MAX_MICROS || micros > Self::MAX_MICROS {
            None
        } else {
            Some(Self { micros })
        }
    }

    /// A time from its sign and fields, or `None` if a field is out of range.
    #[must_use]
    pub fn new(negative: bool, hours: u16, minutes: u8, seconds: u8, micros: u32) -> Option<Self> {
        if minutes > 59 || seconds > 59 || micros > 999_999 {
            return None;
        }
        let secs = i64::from(hours) * 3600 + i64::from(minutes) * 60 + i64::from(seconds);
        let total = secs * MICROS_PER_SEC + i64::from(micros);
        Self::from_micros(if negative { -total } else { total })
    }

    /// The signed duration in microseconds.
    #[must_use]
    pub const fn micros(self) -> i64 {
        self.micros
    }
}

impl fmt::Display for Time {
    /// `[-]hh:mm:ss[.ffffff]`, with three hour digits past 99.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let abs = self.micros.unsigned_abs();
        let (secs, frac) = (abs / 1_000_000, abs % 1_000_000);
        let sign = if self.micros < 0 { "-" } else { "" };
        write!(
            f,
            "{sign}{:02}:{:02}:{:02}",
            secs / 3600,
            secs / 60 % 60,
            secs % 60
        )?;
        if frac != 0 {
            write!(f, ".{frac:06}")?;
        }
        Ok(())
    }
}

/// A `TIMESTAMP`: seconds since the Unix epoch in UTC, with microseconds.
/// Second 0 with no fraction is the zero timestamp `0000-00-00 00:00:00`.
/// The derived order is chronological, the zero timestamp first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp {
    /// Seconds since `1970-01-01 00:00:00` UTC. MariaDB 11.8 stores up to
    /// `u32::MAX` (year 2106), MySQL 8.4 up to `i32::MAX` (year 2038).
    pub seconds: u32,
    /// 0 to 999,999.
    pub micros: u32,
}

impl Timestamp {
    /// A timestamp from its parts.
    #[must_use]
    pub const fn new(seconds: u32, micros: u32) -> Self {
        Self { seconds, micros }
    }

    /// Whether this is the zero timestamp.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.seconds == 0 && self.micros == 0
    }
}

/// How many bytes hold the fraction of a temporal of precision `fsp`
/// (0 to 6): 0, 1, 1, 2, 2, 3, 3.
#[must_use]
pub const fn frac_bytes(fsp: u8) -> usize {
    (fsp as usize).div_ceil(2)
}

/// Whether `micros` has no digits past precision `fsp`.
#[must_use]
pub const fn frac_fits(micros: u32, fsp: u8) -> bool {
    micros <= 999_999 && fsp <= 6 && micros.is_multiple_of(10u32.pow(6 - fsp as u32))
}

// ---- packed forms -----------------------------------------------------------
//
// One byte layout per type serves keys and row records: big-endian, and
// ordered like the values, so the bytes compare as the values do.
//
//   DATE          3 bytes   year (14 bits) | month (4) | day (5), right-aligned
//   DATETIME(f)   5 bytes   year (14) | month (4) | day (5) | hour (5) | minute (6) | second (6)
//   TIMESTAMP(f)  4 bytes   seconds since the epoch, UTC
//   TIME(f)       3 bytes   whole seconds rounded down, plus 2^23
//   fraction      0, 1, 2 or 3 bytes for f = 0, 1-2, 3-4, 5-6: hundredths,
//                 ten-thousandths or millionths of a second (after the
//                 rounding down, so never negative)

fn put_frac(out: &mut Vec<u8>, micros: u32, fsp: u8) {
    let n = frac_bytes(fsp);
    let unit = [0, 10_000, 100, 1][n];
    if n > 0 {
        out.extend_from_slice(&(micros / unit).to_be_bytes()[4 - n..]);
    }
}

fn get_frac(input: &[u8], fsp: u8) -> Option<(u32, &[u8])> {
    let n = frac_bytes(fsp);
    if fsp > 6 || input.len() < n {
        return None;
    }
    let (head, rest) = input.split_at(n);
    let mut be = [0u8; 4];
    be[4 - n..].copy_from_slice(head);
    let micros = u32::from_be_bytes(be).checked_mul([0, 10_000, 100, 1][n])?;
    (micros <= 999_999).then_some((micros, rest))
}

/// Appends a `DATE`'s 3 packed bytes.
pub fn pack_date(d: Date, out: &mut Vec<u8>) {
    let v = (u32::from(d.year) << 9) | (u32::from(d.month) << 5) | u32::from(d.day);
    out.extend_from_slice(&v.to_be_bytes()[1..]);
}

/// Reads a packed `DATE`; `None` if the input is short or a field is out of
/// range.
#[must_use]
pub fn unpack_date(input: &[u8]) -> Option<(Date, &[u8])> {
    let (head, rest) = input.split_at_checked(3)?;
    let v = u32::from_be_bytes([0, head[0], head[1], head[2]]);
    // The masks keep 14, 4 and 5 bits.
    #[allow(clippy::cast_possible_truncation)]
    let d = Date::new((v >> 9) as u16, ((v >> 5) & 0xF) as u8, (v & 0x1F) as u8);
    d.is_storable().then_some((d, rest))
}

/// Appends a `DATETIME(fsp)`'s packed bytes: 5, plus the fraction's.
pub fn pack_datetime(d: DateTime, fsp: u8, out: &mut Vec<u8>) {
    let v = (u64::from(d.date.year) << 26)
        | (u64::from(d.date.month) << 22)
        | (u64::from(d.date.day) << 17)
        | (u64::from(d.hour) << 12)
        | (u64::from(d.minute) << 6)
        | u64::from(d.second);
    out.extend_from_slice(&v.to_be_bytes()[3..]);
    put_frac(out, d.micros, fsp);
}

/// Reads a packed `DATETIME(fsp)`.
#[must_use]
pub fn unpack_datetime(input: &[u8], fsp: u8) -> Option<(DateTime, &[u8])> {
    let (head, rest) = input.split_at_checked(5)?;
    let mut be = [0u8; 8];
    be[3..].copy_from_slice(head);
    let v = u64::from_be_bytes(be);
    let (micros, rest) = get_frac(rest, fsp)?;
    // The masks keep 14, 4, 5, 5, 6 and 6 bits.
    #[allow(clippy::cast_possible_truncation)]
    let d = DateTime::new(
        Date::new(
            (v >> 26) as u16,
            ((v >> 22) & 0xF) as u8,
            ((v >> 17) & 0x1F) as u8,
        ),
        ((v >> 12) & 0x1F) as u8,
        ((v >> 6) & 0x3F) as u8,
        (v & 0x3F) as u8,
        micros,
    );
    d.is_storable().then_some((d, rest))
}

/// Appends a `TIMESTAMP(fsp)`'s packed bytes: 4, plus the fraction's.
pub fn pack_timestamp(t: Timestamp, fsp: u8, out: &mut Vec<u8>) {
    out.extend_from_slice(&t.seconds.to_be_bytes());
    put_frac(out, t.micros, fsp);
}

/// Reads a packed `TIMESTAMP(fsp)`.
#[must_use]
pub fn unpack_timestamp(input: &[u8], fsp: u8) -> Option<(Timestamp, &[u8])> {
    let (head, rest) = input.split_at_checked(4)?;
    let seconds = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
    let (micros, rest) = get_frac(rest, fsp)?;
    Some((Timestamp::new(seconds, micros), rest))
}

const TIME_BIAS: i64 = 1 << 23;

/// Appends a `TIME(fsp)`'s packed bytes: 3, plus the fraction's.
pub fn pack_time(t: Time, fsp: u8, out: &mut Vec<u8>) {
    let secs = t.micros().div_euclid(MICROS_PER_SEC) + TIME_BIAS;
    out.extend_from_slice(&secs.to_be_bytes()[5..]);
    // A remainder of 10^6 fits.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let frac = t.micros().rem_euclid(MICROS_PER_SEC) as u32;
    put_frac(out, frac, fsp);
}

/// Reads a packed `TIME(fsp)`.
#[must_use]
pub fn unpack_time(input: &[u8], fsp: u8) -> Option<(Time, &[u8])> {
    let (head, rest) = input.split_at_checked(3)?;
    let secs = i64::from(u32::from_be_bytes([0, head[0], head[1], head[2]])) - TIME_BIAS;
    let (micros, rest) = get_frac(rest, fsp)?;
    let t = Time::from_micros(secs * MICROS_PER_SEC + i64::from(micros))?;
    Some((t, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_range_and_text() {
        let t = Time::new(false, 838, 59, 59, 999_999).unwrap();
        assert_eq!(t.micros(), Time::MAX_MICROS);
        assert_eq!(t.to_string(), "838:59:59.999999");
        assert_eq!(
            Time::new(true, 1, 2, 3, 0).unwrap().to_string(),
            "-01:02:03"
        );
        assert!(Time::new(false, 839, 0, 0, 0).is_none());
        assert!(Time::new(false, 1, 60, 0, 0).is_none());
        assert!(Time::from_micros(Time::MAX_MICROS + 1).is_none());
        assert!(Time::new(true, 0, 0, 1, 0).unwrap() < Time::default());
    }

    #[test]
    fn date_order_and_text() {
        assert!(Date::new(0, 0, 0) < Date::new(0, 1, 1));
        assert!(Date::new(2024, 2, 29) < Date::new(2024, 3, 0));
        assert_eq!(Date::new(987, 6, 5).to_string(), "0987-06-05");
        let dt = DateTime::new(Date::new(2024, 1, 2), 3, 4, 5, 60);
        assert_eq!(dt.to_string(), "2024-01-02 03:04:05.000060");
        assert!(dt.is_storable());
        assert!(!DateTime::new(Date::new(2024, 13, 2), 3, 4, 5, 0).is_storable());
    }

    #[test]
    fn fraction_widths() {
        assert_eq!([0, 1, 2, 3, 4, 5, 6].map(frac_bytes), [0, 1, 1, 2, 2, 3, 3]);
        assert!(frac_fits(120_000, 2));
        assert!(!frac_fits(123_000, 2));
        assert!(frac_fits(0, 0));
        assert!(!frac_fits(1, 5));
        assert!(frac_fits(999_999, 6));
    }
    #[test]
    fn packed_forms_round_trip_and_order() {
        let dates = [
            Date::new(0, 0, 0),
            Date::new(0, 1, 1),
            Date::new(999, 12, 31),
            Date::new(2024, 2, 0),
            Date::new(2024, 2, 29),
            Date::new(9999, 12, 31),
        ];
        let mut prev: Option<Vec<u8>> = None;
        for d in dates {
            let mut b = Vec::new();
            pack_date(d, &mut b);
            assert_eq!(unpack_date(&b), Some((d, &[][..])));
            assert!(prev.as_ref().is_none_or(|p| p < &b));
            prev = Some(b);
        }
        assert_eq!(unpack_date(&[0xFF, 0xFF, 0xFF]), None);
        assert_eq!(unpack_date(&[0, 0]), None);

        let times = [
            -Time::MAX_MICROS,
            -1_500_000,
            -1_000_000,
            -1,
            0,
            1,
            999_999,
            1_000_000,
            Time::MAX_MICROS,
        ];
        let mut prev: Option<Vec<u8>> = None;
        for m in times {
            let t = Time::from_micros(m).unwrap();
            let mut b = Vec::new();
            pack_time(t, 6, &mut b);
            assert_eq!(b.len(), 6);
            assert_eq!(unpack_time(&b, 6), Some((t, &[][..])));
            assert!(prev.as_ref().is_none_or(|p| p < &b), "{m}");
            prev = Some(b);
        }
        let dt = DateTime::new(Date::new(9999, 12, 31), 23, 59, 59, 990_000);
        for (fsp, len) in [(2u8, 6usize), (3, 7), (6, 8)] {
            let mut b = Vec::new();
            pack_datetime(dt, fsp, &mut b);
            assert_eq!(b.len(), len);
            assert_eq!(unpack_datetime(&b, fsp), Some((dt, &[][..])));
        }
        let ts = Timestamp::new(u32::MAX, 500_000);
        let mut b = Vec::new();
        pack_timestamp(ts, 1, &mut b);
        assert_eq!(b, [0xFF, 0xFF, 0xFF, 0xFF, 50]);
        assert_eq!(unpack_timestamp(&b, 1), Some((ts, &[][..])));
        // A fraction byte above 99 is corruption.
        assert_eq!(unpack_timestamp(&[0, 0, 0, 0, 100], 2), None);
    }
}
