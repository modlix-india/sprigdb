//! Memcomparable encodings: `memcmp(enc(a), enc(b))` orders like the
//! values do.
//!
//! - NULL marker: `0x00` NULL, `0x01` present (NULL first).
//! - Signed integers: big-endian with the sign bit flipped, in the column's
//!   width (1, 2, 3, 4 or 8 bytes).
//! - Unsigned integers: big-endian in the column's width.
//! - Byte strings: `00` escaped as `00 FF`, terminated by `00 00`, so the
//!   encoding is prefix-free and a shorter string sorts first.
//! - DESC: every byte of the column's encoding inverted, terminator included.
//! - Floating point: the IEEE 754 bits, big-endian; the sign bit flipped for
//!   a positive number, every bit inverted for a negative one; `-0.0` is
//!   written as `+0.0`.
//! - Decimals: one encoding for every precision and scale, so `1.0` and
//!   `1.00` are the same bytes. See [`put_decimal`].

use sprig_common::{Result, SprigError};

/// Appends the NULL marker.
pub fn put_null_marker(out: &mut Vec<u8>, is_null: bool) {
    out.push(u8::from(!is_null));
}

/// Appends a signed integer in `width` bytes (1, 2, 3, 4 or 8), sign bit
/// flipped. The value must fit the width.
pub fn put_int(out: &mut Vec<u8>, v: i64, width: usize) {
    debug_assert!(matches!(width, 1..=4 | 8));
    let bits = width * 8;
    // Two's complement in `bits` bits, then flip that width's sign bit.
    #[allow(clippy::cast_sign_loss)]
    let raw = (v as u64) ^ (1u64 << (bits - 1));
    let be = raw.to_be_bytes();
    out.extend_from_slice(&be[8 - width..]);
}

/// Appends an unsigned integer in `width` bytes (1, 2, 3, 4 or 8).
pub fn put_uint(out: &mut Vec<u8>, v: u64, width: usize) {
    debug_assert!(matches!(width, 1..=4 | 8));
    let be = v.to_be_bytes();
    out.extend_from_slice(&be[8 - width..]);
}

/// Appends an `i64` (8 bytes).
pub fn put_i64(out: &mut Vec<u8>, v: i64) {
    put_int(out, v, 8);
}

/// Appends an `i32` (4 bytes), MySQL's `INT`.
pub fn put_i32(out: &mut Vec<u8>, v: i32) {
    put_int(out, i64::from(v), 4);
}

/// Appends a `u64` (8 bytes).
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    put_uint(out, v, 8);
}

/// Reads a signed integer of `width` bytes; returns it and the rest.
///
/// # Errors
/// The input is shorter than `width`.
pub fn get_int(input: &[u8], width: usize) -> Result<(i64, &[u8])> {
    let (head, rest) = split(input, width)?;
    let mut be = [0u8; 8];
    be[8 - width..].copy_from_slice(head);
    let bits = width * 8;
    let raw = u64::from_be_bytes(be) ^ (1u64 << (bits - 1));
    // Sign-extend from `bits`.
    let shift = 64 - bits;
    #[allow(clippy::cast_possible_wrap)]
    let v = ((raw << shift) as i64) >> shift;
    Ok((v, rest))
}

/// Reads an unsigned integer of `width` bytes.
///
/// # Errors
/// The input is shorter than `width`.
pub fn get_uint(input: &[u8], width: usize) -> Result<(u64, &[u8])> {
    let (head, rest) = split(input, width)?;
    let mut be = [0u8; 8];
    be[8 - width..].copy_from_slice(head);
    Ok((u64::from_be_bytes(be), rest))
}

fn split(input: &[u8], n: usize) -> Result<(&[u8], &[u8])> {
    if input.len() < n {
        return Err(SprigError::internal("memcomparable key is truncated"));
    }
    Ok(input.split_at(n))
}

/// Appends a byte string: `00` escaped as `00 FF`, then the `00 00`
/// terminator.
pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.reserve(b.len() + 2);
    for &c in b {
        out.push(c);
        if c == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Reads a byte string written by [`put_bytes`].
///
/// # Errors
/// A bad escape or a missing terminator.
pub fn get_bytes(input: &[u8]) -> Result<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let c = input[i];
        if c != 0 {
            out.push(c);
            i += 1;
            continue;
        }
        match input.get(i + 1) {
            Some(0) => return Ok((out, &input[i + 2..])),
            Some(0xFF) => {
                out.push(0);
                i += 2;
            }
            _ => break,
        }
    }
    Err(SprigError::internal("memcomparable string is malformed"))
}

/// Inverts the bytes from `from` to the end, for a DESC column.
pub fn invert_from(out: &mut [u8], from: usize) {
    for b in &mut out[from..] {
        *b = !*b;
    }
}

/// Skips a byte string written by [`put_bytes`] without copying it.
///
/// # Errors
/// A bad escape or a missing terminator.
pub fn skip_bytes(input: &[u8]) -> Result<&[u8]> {
    let mut i = 0;
    while i < input.len() {
        if input[i] != 0 {
            i += 1;
            continue;
        }
        match input.get(i + 1) {
            Some(0) => return Ok(&input[i + 2..]),
            Some(0xFF) => i += 2,
            _ => break,
        }
    }
    Err(SprigError::internal("memcomparable string is malformed"))
}

/// Appends a `DOUBLE`: 8 bytes that order like the numbers. `-0.0` and
/// `+0.0` get the same bytes. A NaN (never a stored value) keeps its bits,
/// so it sorts beyond the infinity of its sign.
pub fn put_f64(out: &mut Vec<u8>, v: f64) {
    let bits = if v == 0.0 { 0 } else { v.to_bits() };
    let enc = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    out.extend_from_slice(&enc.to_be_bytes());
}

/// Reads a `DOUBLE` written by [`put_f64`].
///
/// # Errors
/// The input is shorter than 8 bytes.
pub fn get_f64(input: &[u8]) -> Result<(f64, &[u8])> {
    let (head, rest) = split(input, 8)?;
    let mut be = [0u8; 8];
    be.copy_from_slice(head);
    let enc = u64::from_be_bytes(be);
    let bits = if enc >> 63 == 1 {
        enc & !(1 << 63)
    } else {
        !enc
    };
    Ok((f64::from_bits(bits), rest))
}

/// Appends a `FLOAT`: 4 bytes, built like [`put_f64`]'s.
pub fn put_f32(out: &mut Vec<u8>, v: f32) {
    let bits = if v == 0.0 { 0 } else { v.to_bits() };
    let enc = if bits >> 31 == 1 {
        !bits
    } else {
        bits | (1 << 31)
    };
    out.extend_from_slice(&enc.to_be_bytes());
}

/// Reads a `FLOAT` written by [`put_f32`].
///
/// # Errors
/// The input is shorter than 4 bytes.
pub fn get_f32(input: &[u8]) -> Result<(f32, &[u8])> {
    let (head, rest) = split(input, 4)?;
    let mut be = [0u8; 4];
    be.copy_from_slice(head);
    let enc = u32::from_be_bytes(be);
    let bits = if enc >> 31 == 1 {
        enc & !(1 << 31)
    } else {
        !enc
    };
    Ok((f32::from_bits(bits), rest))
}

const DECIMAL_ZERO: u8 = 0x80;
const DECIMAL_POSITIVE: i32 = 0xC0;
/// The largest exponent magnitude the head byte holds: 126 integer digits,
/// or 126 zeros after the point. `DECIMAL` needs 65 and 38.
const DECIMAL_MAX_EXP: i32 = 63;

/// Appends a decimal number given as its sign and decimal digits (each 0 to
/// 9) before and after the point.
///
/// The number is normalised first, so equal numbers get equal bytes whatever
/// their scale: written as `0.d1 d2 ... dn x 100^e` in base 100 with `d1`
/// and `dn` not zero. Then:
///
/// ```text
/// zero      0x80
/// positive  0xC0 + e | 2*d + 1 for d1 .. d(n-1) | 2*dn
/// negative  the positive form of the magnitude, every byte inverted
/// ```
///
/// The last digit byte is the only even one (odd, for a negative number), so
/// the encoding ends itself and a number that continues another's digits
/// sorts after it. No `00` or `FF` byte is produced.
pub fn put_decimal(out: &mut Vec<u8>, negative: bool, int: &[u8], frac: &[u8]) {
    debug_assert!(int.iter().chain(frac).all(|&d| d <= 9));
    let lead = int.iter().take_while(|&&d| d == 0).count();
    let int = &int[lead..];
    let mut pairs = Vec::with_capacity(int.len() / 2 + frac.len() / 2 + 2);
    let odd = int.len() % 2;
    if odd == 1 {
        pairs.push(int[0]);
    }
    pairs.extend(int[odd..].chunks(2).map(|c| c[0] * 10 + c[1]));
    let int_pairs = pairs.len();
    pairs.extend(
        frac.chunks(2)
            .map(|c| c[0] * 10 + c.get(1).copied().unwrap_or(0)),
    );
    let Some(first) = pairs.iter().position(|&p| p != 0) else {
        out.push(DECIMAL_ZERO);
        return;
    };
    let last = pairs.iter().rposition(|&p| p != 0).unwrap_or(first);
    let exp = i32::try_from(int_pairs).unwrap_or(i32::MAX) - i32::try_from(first).unwrap_or(0);
    debug_assert!((-DECIMAL_MAX_EXP..=DECIMAL_MAX_EXP).contains(&exp));
    let start = out.len();
    // In range by the assertion above.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    out.push((DECIMAL_POSITIVE + exp) as u8);
    out.extend(pairs[first..last].iter().map(|&p| 2 * p + 1));
    out.push(2 * pairs[last]);
    if negative {
        invert_from(out, start);
    }
}

/// The parts of a decimal read by [`get_decimal`]: whether it is negative,
/// its integer digits (no leading zeros) and its fraction digits (no
/// trailing zeros).
pub type DecimalParts = (bool, Vec<u8>, Vec<u8>);

/// Reads a decimal written by [`put_decimal`], in normalised form: the
/// caller pads the fraction to its column's scale.
///
/// # Errors
/// The input is truncated or isn't an encoding [`put_decimal`] writes.
pub fn get_decimal(input: &[u8]) -> Result<(DecimalParts, &[u8])> {
    let malformed = || SprigError::internal("memcomparable decimal is malformed");
    let (&head, mut rest) = input.split_first().ok_or_else(malformed)?;
    if head == DECIMAL_ZERO {
        return Ok(((false, Vec::new(), Vec::new()), rest));
    }
    let negative = head < DECIMAL_ZERO;
    let flip = |b: u8| if negative { !b } else { b };
    let exp = i32::from(flip(head)) - DECIMAL_POSITIVE;
    if !(-DECIMAL_MAX_EXP..=DECIMAL_MAX_EXP).contains(&exp) {
        return Err(malformed());
    }
    let mut pairs = Vec::new();
    loop {
        let (&b, tail) = rest.split_first().ok_or_else(malformed)?;
        rest = tail;
        let b = flip(b);
        if b / 2 > 99 {
            return Err(malformed());
        }
        pairs.push(b / 2);
        if b % 2 == 0 {
            break;
        }
    }
    if pairs.first() == Some(&0) || pairs.last() == Some(&0) {
        return Err(malformed());
    }
    let (mut int, mut frac) = (Vec::new(), Vec::new());
    let int_pairs = usize::try_from(exp).unwrap_or(0);
    for k in 0..int_pairs {
        let p = pairs.get(k).copied().unwrap_or(0);
        int.extend_from_slice(&[p / 10, p % 10]);
    }
    frac.resize(2 * usize::try_from(-exp).unwrap_or(0), 0);
    for &p in pairs.iter().skip(int_pairs) {
        frac.extend_from_slice(&[p / 10, p % 10]);
    }
    if int.first() == Some(&0) {
        int.remove(0);
    }
    while frac.last() == Some(&0) {
        frac.pop();
    }
    Ok(((negative, int, frac), rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn enc_int(v: i64, w: usize) -> Vec<u8> {
        let mut o = Vec::new();
        put_int(&mut o, v, w);
        o
    }

    fn enc_bytes(b: &[u8]) -> Vec<u8> {
        let mut o = Vec::new();
        put_bytes(&mut o, b);
        o
    }

    #[test]
    fn int_widths_round_trip() {
        for (w, lo, hi) in [
            (1usize, -128i64, 127i64),
            (2, -32768, 32767),
            (3, -8_388_608, 8_388_607),
            (4, i64::from(i32::MIN), i64::from(i32::MAX)),
            (8, i64::MIN, i64::MAX),
        ] {
            for v in [lo, lo + 1, -1, 0, 1, hi - 1, hi] {
                let e = enc_int(v, w);
                assert_eq!(e.len(), w);
                assert_eq!(get_int(&e, w).unwrap().0, v, "width {w} value {v}");
            }
            assert!(enc_int(lo, w) < enc_int(-1, w));
            assert!(enc_int(-1, w) < enc_int(0, w));
            assert!(enc_int(0, w) < enc_int(hi, w));
        }
    }

    fn enc_dec(neg: bool, int: &[u8], frac: &[u8]) -> Vec<u8> {
        let mut o = Vec::new();
        put_decimal(&mut o, neg, int, frac);
        o
    }

    fn digits(s: &str) -> (bool, Vec<u8>, Vec<u8>) {
        let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
        let (i, f) = body.split_once('.').unwrap_or((body, ""));
        let d = |t: &str| t.bytes().map(|b| b - b'0').collect::<Vec<u8>>();
        (neg, d(i), d(f))
    }

    #[test]
    fn decimal_bytes_and_order() {
        let e = |s: &str| {
            let (neg, int, frac) = digits(s);
            enc_dec(neg, &int, &frac)
        };
        assert_eq!(e("0"), [0x80]);
        assert_eq!(e("0.000"), [0x80]);
        assert_eq!(e("1"), [0xC1, 2]);
        assert_eq!(e("1.0"), e("1.00"));
        assert_eq!(e("100"), [0xC2, 2]);
        assert_eq!(e("12.34"), [0xC1, 25, 68]);
        assert_eq!(e("0.01"), [0xC0, 2]);
        assert_eq!(e("0.0001"), [0xBF, 2]);
        assert_eq!(e("-1"), [0x3E, 0xFD]);
        let sorted = [
            "-1000000", "-100", "-99.99", "-12.35", "-12.34", "-12.3", "-12", "-1.5", "-1", "-0.5",
            "-0.05", "-0.0001", "0", "0.0001", "0.05", "0.5", "1", "1.5", "12", "12.3", "12.34",
            "12.35", "99.99", "100", "1000000",
        ];
        for w in sorted.windows(2) {
            assert!(e(w[0]) < e(w[1]), "{} {}", w[0], w[1]);
        }
        for s in sorted {
            let (neg, int, frac) = digits(s);
            let int: Vec<u8> = int.into_iter().skip_while(|&d| d == 0).collect();
            let mut bytes = e(s);
            bytes.extend_from_slice(b"rest");
            let (parts, rest) = get_decimal(&bytes).unwrap();
            assert_eq!(parts, (neg, int, frac), "{s}");
            assert_eq!(rest, b"rest");
        }
        // The widest column's extremes fit the head byte.
        let nines = vec![9u8; 65];
        let big = enc_dec(false, &nines, &[]);
        assert_eq!(big[0], 0xC0 + 33);
        assert_eq!(get_decimal(&big).unwrap().0, (false, nines, vec![]));
        let mut tiny = vec![0u8; 37];
        tiny.push(1);
        let small = enc_dec(true, &[], &tiny);
        assert_eq!(small[0], !(0xC0 - 18));
        assert_eq!(get_decimal(&small).unwrap().0, (true, vec![], tiny));
        for bad in [
            &[][..],
            &[0xC1],
            &[0xC1, 3],
            &[0xC1, 0],
            &[0xC1, 201, 2],
            &[0x7F, 0xFD],
        ] {
            assert!(get_decimal(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn float_edges() {
        let e = |v: f64| {
            let mut o = Vec::new();
            put_f64(&mut o, v);
            o
        };
        assert_eq!(e(-0.0), e(0.0));
        let sorted = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1.0,
            -f64::MIN_POSITIVE,
            -5e-324,
            0.0,
            5e-324,
            f64::MIN_POSITIVE,
            1.0,
            f64::MAX,
            f64::INFINITY,
        ];
        for w in sorted.windows(2) {
            assert!(e(w[0]) < e(w[1]), "{} {}", w[0], w[1]);
        }
        for v in sorted {
            assert_eq!(get_f64(&e(v)).unwrap().0.to_bits(), v.to_bits());
        }
        assert!(get_f64(&[0; 7]).is_err());
        assert!(get_f32(&[0; 3]).is_err());
    }

    proptest! {
        #[test]
        fn f64_order_and_round_trip(a in any::<f64>(), b in any::<f64>()) {
            prop_assume!(!a.is_nan() && !b.is_nan());
            let (mut x, mut y) = (Vec::new(), Vec::new());
            put_f64(&mut x, a);
            put_f64(&mut y, b);
            prop_assert_eq!(a.partial_cmp(&b), Some(x.cmp(&y)));
            let back = get_f64(&x).unwrap().0;
            prop_assert_eq!(back.to_bits(), if a == 0.0 { 0 } else { a.to_bits() });
        }

        #[test]
        fn f32_order_and_round_trip(a in any::<f32>(), b in any::<f32>()) {
            prop_assume!(!a.is_nan() && !b.is_nan());
            let (mut x, mut y) = (Vec::new(), Vec::new());
            put_f32(&mut x, a);
            put_f32(&mut y, b);
            prop_assert_eq!(a.partial_cmp(&b), Some(x.cmp(&y)));
            let back = get_f32(&x).unwrap().0;
            prop_assert_eq!(back.to_bits(), if a == 0.0 { 0 } else { a.to_bits() });
        }

        #[test]
        fn skip_matches_get(a in proptest::collection::vec(prop_oneof![Just(0u8), Just(0xFFu8), any::<u8>()], 0..12)) {
            let mut e = enc_bytes(&a);
            e.extend_from_slice(b"\0tail");
            prop_assert_eq!(skip_bytes(&e).unwrap(), get_bytes(&e).unwrap().1);
        }

        #[test]
        fn i64_order(a: i64, b: i64) {
            prop_assert_eq!(a.cmp(&b), enc_int(a, 8).cmp(&enc_int(b, 8)));
        }

        #[test]
        fn i32_order(a: i32, b: i32) {
            prop_assert_eq!(a.cmp(&b), enc_int(a.into(), 4).cmp(&enc_int(b.into(), 4)));
        }

        #[test]
        fn u64_order(a: u64, b: u64) {
            let (mut x, mut y) = (Vec::new(), Vec::new());
            put_uint(&mut x, a, 8);
            put_uint(&mut y, b, 8);
            prop_assert_eq!(a.cmp(&b), x.cmp(&y));
            prop_assert_eq!(get_uint(&x, 8).unwrap().0, a);
        }

        #[test]
        fn bytes_order_and_round_trip(
            a in proptest::collection::vec(prop_oneof![Just(0u8), Just(1u8), Just(0xFFu8), any::<u8>()], 0..12),
            b in proptest::collection::vec(prop_oneof![Just(0u8), Just(1u8), Just(0xFFu8), any::<u8>()], 0..12),
        ) {
            let (ea, eb) = (enc_bytes(&a), enc_bytes(&b));
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
            let mut tail = ea.clone();
            tail.extend_from_slice(b"rest");
            let (d, rest) = get_bytes(&tail).unwrap();
            prop_assert_eq!(d, a.clone());
            prop_assert_eq!(rest, b"rest");
            // DESC reverses the order.
            let (mut da, mut db) = (ea.clone(), eb.clone());
            invert_from(&mut da, 0);
            invert_from(&mut db, 0);
            prop_assert_eq!(b.cmp(&a), da.cmp(&db));
        }
    }
}
