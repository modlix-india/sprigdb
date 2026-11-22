//! Named time zones (IANA names such as `Europe/Berlin`), from the time
//! zone database bundled with `jiff`, as `CONVERT_TZ` and the session's
//! `time_zone` use them.

use jiff::Timestamp;
use jiff::civil;
use jiff::tz::{AmbiguousOffset, TimeZone};

/// The zone of a name, case not mattering; `None` when the database has
/// no such name.
pub(crate) fn lookup(name: &str) -> Option<TimeZone> {
    if name.is_empty() || name.starts_with('+') || name.starts_with('-') {
        return None;
    }
    jiff::tz::db().get(name).ok()
}

/// The last instant whose changes of offset the servers know: their time
/// zone tables hold the changes up to 2038-01-19 03:14:07 UTC, and later
/// instants keep the offset of then (no summer time after 2037).
const LAST_KNOWN: i64 = 2_147_483_647;

/// The zone's offset from UTC, in seconds, at an instant.
pub(crate) fn offset_at(tz: &TimeZone, unix: i64) -> i64 {
    match Timestamp::from_second(unix.min(LAST_KNOWN)) {
        Ok(t) => i64::from(tz.to_offset(t).seconds()),
        Err(_) => 0,
    }
}

/// The instant (seconds since the epoch) of a local date and time, given
/// as seconds since 1970-01-01 00:00 of the zone's wall clock. A time the
/// clocks skip (the spring gap) is the instant of the change; a time that
/// happens twice (the autumn fold) is its first occurrence.
pub(crate) fn local_to_unix(tz: &TimeZone, local: i64) -> i64 {
    let late = local - offset_at(tz, LAST_KNOWN);
    if late > LAST_KNOWN {
        return late;
    }
    let Some(dt) = civil_of(local) else {
        return local;
    };
    match tz.to_ambiguous_timestamp(dt).offset() {
        AmbiguousOffset::Unambiguous { offset } => local - i64::from(offset.seconds()),
        AmbiguousOffset::Fold { before, .. } => local - i64::from(before.seconds()),
        AmbiguousOffset::Gap { before, after } => {
            // The change happens between these two instants.
            let (mut lo, mut hi) = (
                local - i64::from(after.seconds()),
                local - i64::from(before.seconds()),
            );
            let target = i64::from(after.seconds());
            while hi - lo > 1 {
                let mid = lo + (hi - lo) / 2;
                if offset_at(tz, mid) == target {
                    hi = mid;
                } else {
                    lo = mid;
                }
            }
            hi
        }
    }
}

/// A civil date and time from seconds since 1970-01-01 00:00.
fn civil_of(local: i64) -> Option<civil::DateTime> {
    let days = local.div_euclid(86_400);
    let secs = local.rem_euclid(86_400);
    let epoch = civil::date(1970, 1, 1);
    let date = epoch.checked_add(jiff::Span::new().days(days)).ok()?;
    let time = civil::Time::new(
        (secs / 3600) as i8,
        ((secs / 60) % 60) as i8,
        (secs % 60) as i8,
        0,
    )
    .ok()?;
    Some(date.to_datetime(time))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zones_by_name() {
        let ny = lookup("america/new_york").expect("bundled database");
        // 2024-01-15 10:00 UTC is 05:00 in New York (EST).
        assert_eq!(offset_at(&ny, 1_705_312_800), -5 * 3600);
        // 2024-03-10 02:30 does not exist there: the change, 07:00 UTC.
        let gap_local = 1_710_037_800; // 2024-03-10 02:30 as wall clock seconds
        assert_eq!(local_to_unix(&ny, gap_local), 1_710_054_000);
        // 2024-11-03 01:30 happens twice: the first, 05:30 UTC.
        let fold_local = 1_730_597_400;
        assert_eq!(local_to_unix(&ny, fold_local), 1_730_611_800);
        assert!(lookup("Europe/Nowhere").is_none());
        assert!(lookup("+01:00").is_none());
    }
}
