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

    proptest! {
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
