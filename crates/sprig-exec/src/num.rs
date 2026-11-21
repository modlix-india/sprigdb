//! Exact decimal arithmetic for `DECIMAL` expressions: an unbounded
//! unsigned integer in base 10^9 and a signed, scaled number on top of it.
//!
//! The servers' results are reproduced by rule, not by sharing their
//! representation: sums, differences and products are exact; a quotient is
//! computed to a whole number of 9-digit groups and cut there, as both
//! servers do, before the caller rounds it to the result type's scale.

use std::cmp::Ordering;
use std::fmt;

use sprig_engine::row::Decimal;

const BASE: u64 = 1_000_000_000;

/// An unsigned integer of any size: base 10^9 limbs, least significant
/// first, without high zero limbs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct BigU(Vec<u32>);

impl BigU {
    pub(crate) fn zero() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn from_u128(mut v: u128) -> Self {
        let mut limbs = Vec::new();
        while v > 0 {
            limbs.push((v % u128::from(BASE)) as u32);
            v /= u128::from(BASE);
        }
        Self(limbs)
    }

    pub(crate) fn to_u128(&self) -> Option<u128> {
        let mut v: u128 = 0;
        for &l in self.0.iter().rev() {
            v = v
                .checked_mul(u128::from(BASE))?
                .checked_add(u128::from(l))?;
        }
        Some(v)
    }

    /// From decimal digits (values 0 to 9), most significant first.
    pub(crate) fn from_digits(d: &[u8]) -> Self {
        let mut limbs = Vec::with_capacity(d.len() / 9 + 1);
        let mut end = d.len();
        while end > 0 {
            let start = end.saturating_sub(9);
            let mut v = 0u32;
            for &x in &d[start..end] {
                v = v * 10 + u32::from(x);
            }
            limbs.push(v);
            end = start;
        }
        let mut b = Self(limbs);
        b.trim();
        b
    }

    /// The decimal digits, most significant first; none for zero.
    pub(crate) fn digits(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.0.len() * 9);
        for (i, &l) in self.0.iter().enumerate().rev() {
            let s = if i + 1 == self.0.len() {
                format!("{l}")
            } else {
                format!("{l:09}")
            };
            out.extend(s.bytes().map(|b| b - b'0'));
        }
        out
    }

    pub(crate) fn num_digits(&self) -> usize {
        match self.0.last() {
            None => 0,
            Some(&top) => (self.0.len() - 1) * 9 + (top.ilog10() as usize + 1),
        }
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    pub(crate) fn cmp(&self, o: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&o.0.len())
            .then_with(|| self.0.iter().rev().cmp(o.0.iter().rev()))
    }

    pub(crate) fn add(&self, o: &Self) -> Self {
        let n = self.0.len().max(o.0.len());
        let mut out = Vec::with_capacity(n + 1);
        let mut carry = 0u64;
        for i in 0..n {
            let s = u64::from(self.0.get(i).copied().unwrap_or(0))
                + u64::from(o.0.get(i).copied().unwrap_or(0))
                + carry;
            out.push((s % BASE) as u32);
            carry = s / BASE;
        }
        if carry > 0 {
            out.push(carry as u32);
        }
        Self(out)
    }

    /// `self - o`; `self` must not be smaller.
    pub(crate) fn sub(&self, o: &Self) -> Self {
        let mut out = Vec::with_capacity(self.0.len());
        let mut borrow = 0i64;
        for i in 0..self.0.len() {
            let mut s = i64::from(self.0[i]) - i64::from(o.0.get(i).copied().unwrap_or(0)) - borrow;
            if s < 0 {
                s += BASE as i64;
                borrow = 1;
            } else {
                borrow = 0;
            }
            out.push(s as u32);
        }
        let mut b = Self(out);
        b.trim();
        b
    }

    pub(crate) fn mul(&self, o: &Self) -> Self {
        if self.is_zero() || o.is_zero() {
            return Self::zero();
        }
        if let (Some(a), Some(b)) = (self.to_u128(), o.to_u128()) {
            if let Some(p) = a.checked_mul(b) {
                return Self::from_u128(p);
            }
        }
        let mut out = vec![0u64; self.0.len() + o.0.len() + 1];
        for (i, &a) in self.0.iter().enumerate() {
            let mut carry = 0u64;
            for (j, &b) in o.0.iter().enumerate() {
                let cur = out[i + j] + u64::from(a) * u64::from(b) + carry;
                out[i + j] = cur % BASE;
                carry = cur / BASE;
            }
            let mut k = i + o.0.len();
            while carry > 0 {
                let cur = out[k] + carry;
                out[k] = cur % BASE;
                carry = cur / BASE;
                k += 1;
            }
        }
        let mut b = Self(out.into_iter().map(|x| x as u32).collect());
        b.trim();
        b
    }

    pub(crate) fn mul_small(&self, m: u32) -> Self {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        let mut carry = 0u64;
        for &l in &self.0 {
            let cur = u64::from(l) * u64::from(m) + carry;
            out.push((cur % BASE) as u32);
            carry = cur / BASE;
        }
        if carry > 0 {
            out.push(carry as u32);
        }
        let mut b = Self(out);
        b.trim();
        b
    }

    pub(crate) fn divmod_small(&self, d: u32) -> (Self, u32) {
        let mut out = vec![0u32; self.0.len()];
        let mut rem = 0u64;
        for i in (0..self.0.len()).rev() {
            let cur = rem * BASE + u64::from(self.0[i]);
            out[i] = (cur / u64::from(d)) as u32;
            rem = cur % u64::from(d);
        }
        let mut b = Self(out);
        b.trim();
        (b, rem as u32)
    }

    /// `self * 10^n`.
    pub(crate) fn mul_pow10(&self, n: u32) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let (q, r) = ((n / 9) as usize, n % 9);
        let scaled = if r == 0 {
            self.clone()
        } else {
            self.mul_small(10u32.pow(r))
        };
        let mut limbs = vec![0u32; q];
        limbs.extend(scaled.0);
        Self(limbs)
    }

    /// `(self / 10^n, self % 10^n)`.
    pub(crate) fn div_pow10(&self, n: u32) -> (Self, Self) {
        let (q, r) = ((n / 9) as usize, n % 9);
        if q >= self.0.len() {
            return (Self::zero(), self.clone());
        }
        let low = Self(self.0[..q].to_vec());
        let high = Self(self.0[q..].to_vec());
        if r == 0 {
            let mut low = low;
            low.trim();
            return (high, low);
        }
        let (quot, small) = high.divmod_small(10u32.pow(r));
        // remainder = small * BASE^q + low
        let mut limbs = low.0;
        limbs.resize(q, 0);
        limbs.push(small);
        let mut rem = Self(limbs);
        rem.trim();
        (quot, rem)
    }

    /// `(self / d, self % d)`; `d` must not be zero.
    pub(crate) fn divmod(&self, d: &Self) -> (Self, Self) {
        if self.cmp(d) == Ordering::Less {
            return (Self::zero(), self.clone());
        }
        if let (Some(a), Some(b)) = (self.to_u128(), d.to_u128()) {
            return (Self::from_u128(a / b), Self::from_u128(a % b));
        }
        if d.0.len() == 1 {
            let (q, r) = self.divmod_small(d.0[0]);
            return (q, Self::from_u128(u128::from(r)));
        }
        // Long division, one base-10^9 quotient limb at a time; each limb is
        // found by bisection on `d * q <= rem`.
        let mut quot = vec![0u32; self.0.len()];
        let mut rem = Self::zero();
        for i in (0..self.0.len()).rev() {
            // rem = rem * BASE + limb
            let mut limbs = Vec::with_capacity(rem.0.len() + 1);
            limbs.push(self.0[i]);
            limbs.extend(&rem.0);
            rem = Self(limbs);
            rem.trim();
            if rem.cmp(d) == Ordering::Less {
                continue;
            }
            let (mut lo, mut hi) = (1u32, (BASE - 1) as u32);
            while lo < hi {
                let mid = lo + (hi - lo).div_ceil(2);
                if d.mul_small(mid).cmp(&rem) == Ordering::Greater {
                    hi = mid - 1;
                } else {
                    lo = mid;
                }
            }
            quot[i] = lo;
            rem = rem.sub(&d.mul_small(lo));
        }
        let mut q = Self(quot);
        q.trim();
        (q, rem)
    }
}

/// How digits beyond a wanted scale are dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Round {
    /// Half away from zero: the servers' rule for exact numbers.
    HalfUp,
    /// Toward zero.
    Truncate,
    /// Toward positive infinity.
    Ceil,
    /// Toward negative infinity.
    Floor,
}

/// A signed decimal number: `(-1)^neg * mag / 10^scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dec {
    pub(crate) neg: bool,
    pub(crate) mag: BigU,
    pub(crate) scale: u32,
}

impl Dec {
    pub(crate) fn zero() -> Self {
        Self {
            neg: false,
            mag: BigU::zero(),
            scale: 0,
        }
    }

    pub(crate) fn from_i64(v: i64) -> Self {
        Self {
            neg: v < 0,
            mag: BigU::from_u128(u128::from(v.unsigned_abs())),
            scale: 0,
        }
    }

    pub(crate) fn from_u64(v: u64) -> Self {
        Self {
            neg: false,
            mag: BigU::from_u128(u128::from(v)),
            scale: 0,
        }
    }

    pub(crate) fn from_decimal(d: &Decimal) -> Self {
        let mut digits = Vec::with_capacity(d.int_digits().len() + d.frac_digits().len());
        digits.extend_from_slice(d.int_digits());
        digits.extend_from_slice(d.frac_digits());
        Self {
            neg: d.is_negative(),
            mag: BigU::from_digits(&digits),
            scale: d.frac_digits().len() as u32,
        }
    }

    /// The engine's value, or `None` when it has more than 65 digits or
    /// more than 38 after the point.
    pub(crate) fn to_decimal(&self) -> Option<Decimal> {
        let (int, frac) = self.split_digits();
        Decimal::from_parts(self.neg, &int, &frac)
    }

    /// The integer and fraction digits, most significant first.
    pub(crate) fn split_digits(&self) -> (Vec<u8>, Vec<u8>) {
        let digits = self.mag.digits();
        let scale = self.scale as usize;
        if digits.len() <= scale {
            let mut frac = vec![0u8; scale - digits.len()];
            frac.extend(digits);
            (Vec::new(), frac)
        } else {
            let split = digits.len() - scale;
            (digits[..split].to_vec(), digits[split..].to_vec())
        }
    }

    /// From digit text `[+-]digits[.digits]`; `None` for anything else.
    pub(crate) fn parse(s: &str) -> Option<Self> {
        let (neg, body) = match s.as_bytes().first()? {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        let mut digits = Vec::with_capacity(int.len() + frac.len());
        for b in int.bytes().chain(frac.bytes()) {
            if !b.is_ascii_digit() {
                return None;
            }
            digits.push(b - b'0');
        }
        let mag = BigU::from_digits(&digits);
        Some(Self {
            neg: neg && !mag.is_zero(),
            mag,
            scale: frac.len() as u32,
        })
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.mag.is_zero()
    }

    /// The number of digits before the point (0 for a value below 1).
    pub(crate) fn int_digits(&self) -> usize {
        self.mag.num_digits().saturating_sub(self.scale as usize)
    }

    pub(crate) fn negated(&self) -> Self {
        Self {
            neg: !self.neg && !self.is_zero(),
            mag: self.mag.clone(),
            scale: self.scale,
        }
    }

    pub(crate) fn abs(&self) -> Self {
        Self {
            neg: false,
            mag: self.mag.clone(),
            scale: self.scale,
        }
    }

    /// The magnitude at `scale` (not smaller than the value's own).
    fn mag_at(&self, scale: u32) -> BigU {
        self.mag.mul_pow10(scale - self.scale)
    }

    pub(crate) fn cmp(&self, o: &Self) -> Ordering {
        match (self.neg, o.neg) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (neg, _) => {
                let s = self.scale.max(o.scale);
                let c = self.mag_at(s).cmp(&o.mag_at(s));
                if neg { c.reverse() } else { c }
            }
        }
    }

    pub(crate) fn add(&self, o: &Self) -> Self {
        let scale = self.scale.max(o.scale);
        let (a, b) = (self.mag_at(scale), o.mag_at(scale));
        let (neg, mag) = if self.neg == o.neg {
            (self.neg, a.add(&b))
        } else {
            match a.cmp(&b) {
                Ordering::Less => (o.neg, b.sub(&a)),
                // Two numbers that cancel leave a plain zero, without
                // fraction digits (a later division counts them).
                Ordering::Equal => return Self::zero(),
                Ordering::Greater => (self.neg, a.sub(&b)),
            }
        };
        Self {
            neg: neg && !mag.is_zero(),
            mag,
            scale,
        }
    }

    pub(crate) fn sub(&self, o: &Self) -> Self {
        self.add(&Self {
            neg: !o.neg,
            mag: o.mag.clone(),
            scale: o.scale,
        })
    }

    pub(crate) fn mul(&self, o: &Self) -> Self {
        let mag = self.mag.mul(&o.mag);
        Self {
            neg: self.neg != o.neg && !mag.is_zero(),
            mag,
            scale: self.scale + o.scale,
        }
    }

    /// The quotient as both servers compute it: to a whole number of
    /// 9-digit groups of fraction digits, and cut (not rounded) there.
    /// `None` for a zero divisor.
    pub(crate) fn div(&self, o: &Self, incr: u32) -> Option<Self> {
        if o.is_zero() {
            return None;
        }
        // Both scales count in whole 9-digit groups; the increment only
        // adds what those groups don't already give.
        let (g1, g2) = (self.scale.div_ceil(9) * 9, o.scale.div_ceil(9) * 9);
        let extra = incr.saturating_sub((g1 - self.scale) + (g2 - o.scale));
        let mut frac = (g1 + g2 + extra).div_ceil(9) * 9;
        // A number holds nine groups in all.
        let magnitude = |d: &Self| d.mag.num_digits() as i64 - i64::from(d.scale);
        let int_digits = (magnitude(self) - magnitude(o) + 1).max(0);
        let int_groups = (int_digits as u32).div_ceil(9).min(8);
        frac = frac.min((9 - int_groups) * 9);
        // (a / 10^s1) / (b / 10^s2) = a * 10^(frac + s2 - s1) / b / 10^frac
        let shift = i64::from(frac) + i64::from(o.scale) - i64::from(self.scale);
        let num = if shift >= 0 {
            self.mag.mul_pow10(shift as u32)
        } else {
            self.mag.div_pow10((-shift) as u32).0
        };
        let (q, _) = num.divmod(&o.mag);
        Some(Self {
            neg: self.neg != o.neg && !q.is_zero(),
            mag: q,
            scale: frac,
        })
    }

    /// The remainder with the dividend's sign. `None` for a zero divisor.
    pub(crate) fn rem(&self, o: &Self) -> Option<Self> {
        if o.is_zero() {
            return None;
        }
        let scale = self.scale.max(o.scale);
        let (_, r) = self.mag_at(scale).divmod(&o.mag_at(scale));
        Some(Self {
            neg: self.neg && !r.is_zero(),
            mag: r,
            scale,
        })
    }

    /// The value at `scale` fraction digits; a negative `scale` rounds left
    /// of the point (the result then has scale 0).
    pub(crate) fn round(&self, scale: i32, mode: Round) -> Self {
        if scale >= self.scale as i32 {
            return Self {
                neg: self.neg,
                mag: self.mag_at(scale as u32),
                scale: scale as u32,
            };
        }
        let drop = (self.scale as i32 - scale) as u32;
        let (mut q, r) = self.mag.div_pow10(drop);
        let up = match mode {
            Round::Truncate => false,
            Round::HalfUp => {
                let half = BigU::from_u128(5).mul_pow10(drop - 1);
                r.cmp(&half) != Ordering::Less
            }
            Round::Ceil => !self.neg && !r.is_zero(),
            Round::Floor => self.neg && !r.is_zero(),
        };
        if up {
            q = q.add(&BigU::from_u128(1));
        }
        if scale < 0 {
            q = q.mul_pow10((-scale) as u32);
        }
        Self {
            neg: self.neg && !q.is_zero(),
            mag: q,
            scale: scale.max(0) as u32,
        }
    }

    /// The nearest double.
    pub(crate) fn to_f64(&self) -> f64 {
        self.to_string().parse::<f64>().unwrap_or(0.0)
    }

    /// The value as a signed 64-bit integer after rounding half away from
    /// zero; `Err(negative)` when it doesn't fit.
    pub(crate) fn to_i64(&self) -> Result<i64, bool> {
        let r = self.round(0, Round::HalfUp);
        let m = r.mag.to_u128().ok_or(r.neg)?;
        if r.neg {
            if m <= 1u128 << 63 {
                Ok((m as i128).wrapping_neg() as i64)
            } else {
                Err(true)
            }
        } else if m <= i64::MAX as u128 {
            Ok(m as i64)
        } else {
            Err(false)
        }
    }

    /// The value as an unsigned 64-bit integer after rounding half away
    /// from zero; `Err(negative)` when it doesn't fit.
    pub(crate) fn to_u64(&self) -> Result<u64, bool> {
        let r = self.round(0, Round::HalfUp);
        if r.neg {
            return Err(true);
        }
        let m = r.mag.to_u128().ok_or(false)?;
        u64::try_from(m).map_err(|_| false)
    }

    /// The largest value of `precision` digits with `scale` of them after
    /// the point, with this value's sign.
    pub(crate) fn max_of(precision: u32, scale: u32, neg: bool) -> Self {
        Self {
            neg,
            mag: BigU::from_digits(&vec![9u8; precision as usize]),
            scale,
        }
    }
}

impl fmt::Display for Dec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (int, frac) = self.split_digits();
        let mut s = String::with_capacity(int.len() + frac.len() + 3);
        if self.neg {
            s.push('-');
        }
        if int.is_empty() {
            s.push('0');
        }
        s.extend(int.iter().map(|&d| char::from(b'0' + d)));
        if !frac.is_empty() {
            s.push('.');
            s.extend(frac.iter().map(|&d| char::from(b'0' + d)));
        }
        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).expect("decimal text")
    }

    #[test]
    fn big_round_trips_and_arithmetic() {
        let a = BigU::from_digits(&[9; 40]);
        assert_eq!(a.digits(), vec![9; 40]);
        assert_eq!(a.num_digits(), 40);
        let one = BigU::from_u128(1);
        let b = a.add(&one);
        assert_eq!(b.num_digits(), 41);
        assert_eq!(b.sub(&one), a);
        let sq = a.mul(&a);
        let (q, r) = sq.divmod(&a);
        assert_eq!(q, a);
        assert!(r.is_zero());
        let (q, r) = sq.add(&one).divmod(&a);
        assert_eq!(q, a);
        assert_eq!(r, one);
        let (q, r) = b.div_pow10(13);
        assert_eq!(q.num_digits(), 28);
        assert!(r.is_zero());
        assert_eq!(a.mul_pow10(11).div_pow10(11).0, a);
    }

    #[test]
    fn decimal_rules() {
        assert_eq!(d("1.5").add(&d("1.25")).to_string(), "2.75");
        assert_eq!(d("1.5").sub(&d("2.25")).to_string(), "-0.75");
        assert_eq!(d("1.5").mul(&d("-1.25")).to_string(), "-1.875");
        // A quotient is cut at a whole number of 9-digit groups.
        let q = d("2").div(&d("3"), 4).expect("non-zero");
        assert_eq!(q.to_string(), "0.666666666");
        assert_eq!(q.round(4, Round::HalfUp).to_string(), "0.6667");
        let q = d("2.00000").div(&d("3"), 4).expect("non-zero");
        assert_eq!(q.round(9, Round::HalfUp).to_string(), "0.666666666");
        let q = d("1.5").div(&d("7.0"), 4).expect("non-zero");
        assert_eq!(q.to_string(), "0.214285714285714285");
        assert!(d("1").div(&d("0.00"), 4).is_none());
        assert_eq!(
            d("-7.5").rem(&d("2")).expect("non-zero").to_string(),
            "-1.5"
        );
        assert_eq!(d("2.5").round(0, Round::HalfUp).to_string(), "3");
        assert_eq!(d("-2.5").round(0, Round::HalfUp).to_string(), "-3");
        assert_eq!(d("-2.5").round(0, Round::Truncate).to_string(), "-2");
        assert_eq!(d("-2.5").round(0, Round::Floor).to_string(), "-3");
        assert_eq!(d("2.1").round(0, Round::Ceil).to_string(), "3");
        assert_eq!(d("1250").round(-2, Round::HalfUp).to_string(), "1300");
        assert_eq!(d("0.004").round(2, Round::HalfUp).to_string(), "0.00");
        assert!(!d("-0.004").round(2, Round::HalfUp).neg);
        assert_eq!(d("1.5").round(3, Round::HalfUp).to_string(), "1.500");
        assert_eq!(d("-9223372036854775808").to_i64(), Ok(i64::MIN));
        assert_eq!(d("9223372036854775807.5").to_i64(), Err(false));
        assert_eq!(d("18446744073709551615.4").to_u64(), Ok(u64::MAX));
        assert_eq!(d("-0.4").to_u64(), Ok(0));
        assert_eq!(d("-0.5").to_u64(), Err(true));
        assert_eq!(d("1.10").cmp(&d("1.1")), Ordering::Equal);
        assert_eq!(d("-1.10").cmp(&d("1.1")), Ordering::Less);
        assert_eq!(d("0.001").int_digits(), 0);
        assert_eq!(d("123.4").int_digits(), 3);
    }
}
