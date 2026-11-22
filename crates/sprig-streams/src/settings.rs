//! The `stream.*` keys this crate registers: `stream.partitions_default`
//! and `stream.produce_wait_ms`, both read by the Kafka front end; the
//! `kafka.*` and `listener.kafka.*` keys are the gateway's.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// Tenant, database and global.
const TENANT_DB: Scopes = Scopes(0b10_0110);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 45,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `stream.partitions_default`: partitions of a new stream.
    PARTITIONS_DEFAULT: u64 = "stream.partitions_default", 12, TENANT_DB,
    HardBounds::range(1, 10_000),
    "Partitions of a new stream"
);

/// `stream.produce_wait_ms`: how long a Kafka produce waits for its
/// records' offsets.
pub static PRODUCE_WAIT_MS: Setting<u64> = Setting {
    key: "stream.produce_wait_ms",
    default: 2000,
    scopes: Scopes(0b10_0100),
    hard: HardBounds::range(0, 600_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 45,
    doc: "How long a Kafka produce waits for its records' offsets",
};
sprig_settings::register!(PRODUCE_WAIT_MS);

/// `'7 days'`, `'72 hours'`, `'90 minutes'` (rounded up to whole hours),
/// or a bare number of hours, as `CREATE STREAM ... RETENTION` takes it.
#[must_use]
pub fn parse_retention(text: &str) -> Option<u64> {
    let t = text.trim().to_ascii_lowercase();
    let mut parts = t.split_ascii_whitespace();
    let n: u64 = parts.next()?.parse().ok()?;
    let hours = match parts.next() {
        None | Some("h" | "hour" | "hours") => n,
        Some("d" | "day" | "days") => n.checked_mul(24)?,
        Some("w" | "week" | "weeks") => n.checked_mul(168)?,
        Some("m" | "min" | "minute" | "minutes") => n.div_ceil(60),
        Some("ms") => n.div_ceil(3_600_000),
        _ => return None,
    };
    if parts.next().is_some() || hours == 0 {
        return None;
    }
    Some(hours)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_text() {
        assert_eq!(parse_retention("7 days"), Some(168));
        assert_eq!(parse_retention("72 hours"), Some(72));
        assert_eq!(parse_retention("90 minutes"), Some(2));
        assert_eq!(parse_retention("12"), Some(12));
        assert_eq!(parse_retention("0 days"), None);
        assert_eq!(parse_retention("7 fortnights"), None);
    }
}
