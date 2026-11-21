//! Exact decimal numbers, held as digits.
//!
//! Storage needs no arithmetic: a `DECIMAL(p,s)` value is its sign, the
//! digits before the point and exactly `s` digits after it. Rounding and
//! range rules belong to the SQL layer, which hands the engine a value that
//! already fits its column.

use std::cmp::Ordering;
use std::fmt;

/// An exact decimal: a sign, integer digits and fraction digits.
///
/// Equality is equality of the representation, so `1.0` and `1.00` are
/// different values of different scales; [`Decimal::cmp_numeric`] is the
/// numeric order, under which they are equal. Zero is never negative.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Decimal {
    negative: bool,
    /// Most significant first, no leading zeros; empty for a zero integer part.
    int: Vec<u8>,
    /// Exactly `scale` digits, trailing zeros kept.
    frac: Vec<u8>,
}

impl Decimal {
    /// The most digits a decimal holds (`DECIMAL(65, s)`).
    pub const MAX_PRECISION: usize = 65;
    /// The most fraction digits (`DECIMAL(p, 38)`).
    pub const MAX_SCALE: usize = 38;

    /// Zero, with scale 0.
    #[must_use]
    pub fn zero() -> Self {
        Self::default()
    }

    /// Builds a decimal from its sign and digits (each 0 to 9, most
    /// significant first). Leading zeros of the integer part are dropped;
    /// fraction digits are kept as given, so their count is the scale.
    ///
    /// Returns `None` for a digit above 9, more than 38 fraction digits or
    /// more than 65 digits in all.
    #[must_use]
    pub fn from_parts(negative: bool, int: &[u8], frac: &[u8]) -> Option<Self> {
        if int.iter().chain(frac).any(|&d| d > 9) {
            return None;
        }
        let lead = int.iter().take_while(|&&d| d == 0).count();
        let int = &int[lead..];
        if frac.len() > Self::MAX_SCALE || int.len() + frac.len() > Self::MAX_PRECISION {
            return None;
        }
        let zero = int.is_empty() && frac.iter().all(|&d| d == 0);
        Some(Self {
            negative: negative && !zero,
            int: int.to_vec(),
            frac: frac.to_vec(),
        })
    }

    /// Parses `[+-]digits[.digits]` (at least one digit in all). No
    /// exponent, no spaces: this reads values the SQL layer has already
    /// normalised, and the text [`fmt::Display`] writes.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let (negative, body) = match s.as_bytes().first()? {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        let digits = |t: &str| -> Option<Vec<u8>> {
            t.bytes()
                .map(|b| b.is_ascii_digit().then(|| b - b'0'))
                .collect()
        };
        Self::from_parts(negative, &digits(int)?, &digits(frac)?)
    }

    /// The decimal of an integer, with scale 0.
    #[must_use]
    pub fn from_i64(v: i64) -> Self {
        let mut d = Self::from_u64(v.unsigned_abs());
        d.negative = v < 0;
        d
    }

    /// The decimal of an unsigned integer, with scale 0.
    #[must_use]
    pub fn from_u64(mut v: u64) -> Self {
        let mut int = Vec::new();
        while v > 0 {
            // A remainder of 10 fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            int.push((v % 10) as u8);
            v /= 10;
        }
        int.reverse();
        Self {
            negative: false,
            int,
            frac: Vec::new(),
        }
    }

    /// Whether the value is below zero.
    #[must_use]
    pub fn is_negative(&self) -> bool {
        self.negative
    }

    /// Whether the value is zero (at any scale).
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.int.is_empty() && self.frac.iter().all(|&d| d == 0)
    }

    /// The digits before the point, without leading zeros.
    #[must_use]
    pub fn int_digits(&self) -> &[u8] {
        &self.int
    }

    /// The digits after the point; their count is the scale.
    #[must_use]
    pub fn frac_digits(&self) -> &[u8] {
        &self.frac
    }

    /// The number of fraction digits.
    #[must_use]
    pub fn scale(&self) -> u8 {
        // At most 38 by construction.
        #[allow(clippy::cast_possible_truncation)]
        let s = self.frac.len() as u8;
        s
    }

    /// The same number at another scale: zeros are appended, or trailing
    /// zeros dropped. `None` if a non-zero digit would be lost or the result
    /// would pass 65 digits.
    #[must_use]
    pub fn with_scale(&self, scale: u8) -> Option<Self> {
        let scale = usize::from(scale);
        let mut frac = self.frac.clone();
        if frac.len() > scale && frac[scale..].iter().any(|&d| d != 0) {
            return None;
        }
        frac.resize(scale, 0);
        Self::from_parts(self.negative, &self.int, &frac)
    }

    /// Whether a `DECIMAL(precision, scale)` column holds this value as it
    /// is: the same scale, and no more integer digits than the column has.
    #[must_use]
    pub fn fits(&self, precision: u8, scale: u8) -> bool {
        self.frac.len() == usize::from(scale)
            && precision >= scale
            && self.int.len() <= usize::from(precision - scale)
    }

    /// The numeric order: `1.0` equals `1.00`, and `-1` is below `0.5`.
    #[must_use]
    pub fn cmp_numeric(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.cmp_magnitude(other),
            (true, true) => other.cmp_magnitude(self),
        }
    }

    fn cmp_magnitude(&self, other: &Self) -> Ordering {
        self.int
            .len()
            .cmp(&other.int.len())
            .then_with(|| self.int.cmp(&other.int))
            .then_with(|| {
                let n = self.frac.len().max(other.frac.len());
                let digit = |f: &[u8], i: usize| f.get(i).copied().unwrap_or(0);
                (0..n)
                    .map(|i| digit(&self.frac, i).cmp(&digit(&other.frac, i)))
                    .find(|o| o.is_ne())
                    .unwrap_or(Ordering::Equal)
            })
    }
}

impl fmt::Display for Decimal {
    /// The text the servers print: `-12.50`, `0.05`, `0`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(self.int.len() + self.frac.len() + 3);
        if self.negative {
            s.push('-');
        }
        if self.int.is_empty() {
            s.push('0');
        }
        s.extend(self.int.iter().map(|&d| char::from(b'0' + d)));
        if !self.frac.is_empty() {
            s.push('.');
            s.extend(self.frac.iter().map(|&d| char::from(b'0' + d)));
        }
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::parse(s).unwrap()
    }

    #[test]
    fn parse_and_print() {
        for s in [
            "0",
            "0.00",
            "-12.50",
            "0.05",
            "999.99",
            "12345678901234567890",
        ] {
            assert_eq!(d(s).to_string(), s);
        }
        assert_eq!(d("+007.10").to_string(), "7.10");
        assert_eq!(d("-0.00").to_string(), "0.00");
        assert_eq!(d(".5").to_string(), "0.5");
        assert_eq!(d("5.").to_string(), "5");
        assert!(!d("-0.00").is_negative());
        for bad in ["", "-", ".", "1e5", " 1", "1.2.3", "12a"] {
            assert!(Decimal::parse(bad).is_none(), "{bad:?}");
        }
        assert!(Decimal::parse(&"9".repeat(66)).is_none());
        assert!(Decimal::parse(&format!("0.{}", "1".repeat(39))).is_none());
        assert!(Decimal::parse(&"9".repeat(65)).is_some());
    }

    #[test]
    fn integers() {
        assert_eq!(
            Decimal::from_i64(i64::MIN).to_string(),
            i64::MIN.to_string()
        );
        assert_eq!(
            Decimal::from_u64(u64::MAX).to_string(),
            u64::MAX.to_string()
        );
        assert_eq!(Decimal::from_i64(0), Decimal::zero());
    }

    #[test]
    fn scale_and_fit() {
        assert_eq!(d("1.5").with_scale(3).unwrap().to_string(), "1.500");
        assert_eq!(d("1.500").with_scale(1).unwrap().to_string(), "1.5");
        assert!(d("1.55").with_scale(1).is_none());
        assert!(d("123.45").fits(5, 2));
        assert!(!d("1234.45").fits(5, 2));
        assert!(!d("123.4").fits(5, 2));
        assert!(d("0.45").fits(2, 2));
    }

    #[test]
    fn numeric_order() {
        let sorted = [
            "-100", "-99.9", "-1.01", "-1", "-0.5", "-0.05", "0", "0.001", "0.01", "0.5", "1",
            "1.01", "9", "10", "99.99", "100",
        ];
        for (i, a) in sorted.iter().enumerate() {
            for (j, b) in sorted.iter().enumerate() {
                assert_eq!(d(a).cmp_numeric(&d(b)), i.cmp(&j), "{a} {b}");
            }
        }
        assert_eq!(d("1.0").cmp_numeric(&d("1.00")), Ordering::Equal);
        assert_ne!(d("1.0"), d("1.00"));
        assert_eq!(d("0").cmp_numeric(&d("-0.000")), Ordering::Equal);
    }
}
