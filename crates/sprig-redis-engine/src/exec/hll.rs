//! `HyperLogLog`: a sketch held in a string value, in Redis's layout so
//! clients that read or copy sketches with `GET`/`SET` keep working.
//!
//! - 16,384 six-bit registers (precision 14). An element's 64-bit
//!   `MurmurHash64A` (seed `0xadc83b19`) picks a register with its low 14
//!   bits; the register keeps the largest run length of zero bits (plus
//!   one) seen in the other 50 bits.
//! - A 16-byte header: `HYLL`, the encoding (0 dense, 1 sparse), three
//!   unused bytes, and a cached count (8 bytes, little-endian; the top bit
//!   of the last byte says it is stale).
//! - Dense: the registers packed 6 bits each, least significant bit first.
//!   Sparse: runs of zero registers (`00xxxxxx`: 1 to 64; `01xxxxxx
//!   yyyyyyyy`: 1 to 16,384) and of equal values (`1vvvvvxx`: value 1 to
//!   32, 1 to 4 registers). A sketch starts sparse and turns dense when its
//!   sparse form passes 3,000 bytes or a register passes 32.
//! - The count is Ertl's improved estimator over the register histogram.

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::reply;

use super::{Cx, Out, tri};
use crate::layout::{Body, Meta};
use crate::store::{Batch, StoreError};

/// Runs a `HyperLogLog` command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "pfadd" => pfadd(cx, a),
        "pfcount" => pfcount(cx, a),
        "pfmerge" => pfmerge(cx, a),
        _ => return None,
    })
}

const P: u32 = 14;
const M: usize = 1 << P;
const Q: u32 = 64 - P;
const HDR: usize = 16;
const DENSE: usize = HDR + (M * 6).div_ceil(8);
const SPARSE_MAX: usize = 3000;
const SEED: u64 = 0xadc8_3b19;

/// `MurmurHash64A` (Austin Appleby, public domain), blocks little-endian.
#[must_use]
pub fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const MUL: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(MUL);
    let chunks = key.chunks_exact(8);
    let rest = chunks.remainder();
    for c in chunks {
        let mut k = u64::from_le_bytes(c.try_into().unwrap_or([0; 8]));
        k = k.wrapping_mul(MUL);
        k ^= k >> R;
        k = k.wrapping_mul(MUL);
        h ^= k;
        h = h.wrapping_mul(MUL);
    }
    if !rest.is_empty() {
        for (i, b) in rest.iter().enumerate().rev() {
            h ^= u64::from(*b) << (8 * i);
        }
        h = h.wrapping_mul(MUL);
    }
    h ^= h >> R;
    h = h.wrapping_mul(MUL);
    h ^= h >> R;
    h
}

/// The register an element updates and the value it offers.
fn index_and_count(e: &[u8]) -> (usize, u8) {
    let h = murmur64a(e, SEED);
    let index = (h & (M as u64 - 1)) as usize;
    let rest = (h >> P) | (1u64 << Q);
    (index, (rest.trailing_zeros() + 1) as u8)
}

/// The text of every refused sketch.
fn not_hll() -> Frame {
    Frame::error("WRONGTYPE Key is not a valid HyperLogLog string value.")
}

/// A key's sketch (none when absent) and its stored metadata.
type Found = (Option<Sketch>, Option<Meta>);

/// A decoded sketch.
struct Sketch {
    regs: Vec<u8>,
    /// The cached count, when valid.
    cached: Option<u64>,
}

fn get_reg(d: &[u8], i: usize) -> u8 {
    let bit = i * 6;
    let (b, fb) = (bit / 8, bit % 8);
    let lo = u16::from(d[b]) >> fb;
    let hi = d.get(b + 1).map_or(0, |x| u16::from(*x) << (8 - fb));
    ((lo | hi) & 63) as u8
}

fn set_reg(d: &mut [u8], i: usize, v: u8) {
    let bit = i * 6;
    let (b, fb) = (bit / 8, bit % 8);
    let v = u16::from(v & 63);
    d[b] = (u16::from(d[b]) & !(63 << fb) | (v << fb)) as u8;
    if fb > 2
        && let Some(x) = d.get_mut(b + 1)
    {
        let sh = 8 - fb;
        *x = (u16::from(*x) & !(63 >> sh) | (v >> sh)) as u8;
    }
}

/// Decodes a stored sketch; `None` when `v` isn't one.
fn decode(v: &[u8]) -> Option<Sketch> {
    if v.len() < HDR || &v[..4] != b"HYLL" {
        return None;
    }
    let card = u64::from_le_bytes(v[8..16].try_into().ok()?);
    let cached = (v[15] & 0x80 == 0).then_some(card);
    let mut regs = vec![0u8; M];
    match v[4] {
        0 => {
            if v.len() != DENSE {
                return None;
            }
            for (i, r) in regs.iter_mut().enumerate() {
                *r = get_reg(&v[HDR..], i);
            }
        }
        1 => {
            let mut at = HDR;
            let mut i = 0usize;
            while at < v.len() {
                let op = v[at];
                if op & 0xc0 == 0 {
                    i += usize::from(op & 0x3f) + 1;
                    at += 1;
                } else if op & 0xc0 == 0x40 {
                    let n = (usize::from(op & 0x3f) << 8) | usize::from(*v.get(at + 1)?);
                    i += n + 1;
                    at += 2;
                } else {
                    let val = ((op >> 2) & 0x1f) + 1;
                    let n = usize::from(op & 3) + 1;
                    if i + n > M {
                        return None;
                    }
                    regs[i..i + n].fill(val);
                    i += n;
                    at += 1;
                }
                if i > M {
                    return None;
                }
            }
            if i != M {
                return None;
            }
        }
        _ => return None,
    }
    Some(Sketch { regs, cached })
}

/// The sparse form of `regs`, or `None` when it would be too long or a
/// register is above 32.
fn sparse(regs: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < M {
        let v = regs[i];
        let mut j = i;
        while j < M && regs[j] == v {
            j += 1;
        }
        let mut run = j - i;
        if v == 0 {
            while run > 0 {
                let n = run.min(M);
                if n <= 64 {
                    out.push((n - 1) as u8);
                } else {
                    out.push(0x40 | ((n - 1) >> 8) as u8);
                    out.push(((n - 1) & 0xff) as u8);
                }
                run -= n;
            }
        } else {
            if v > 32 {
                return None;
            }
            while run > 0 {
                let n = run.min(4);
                out.push(0x80 | ((v - 1) << 2) | (n - 1) as u8);
                run -= n;
            }
        }
        if out.len() > SPARSE_MAX {
            return None;
        }
        i = j;
    }
    Some(out)
}

/// The stored bytes of `regs`: sparse when it fits (and `dense` isn't
/// asked for), with a stale cached count.
fn encode(regs: &[u8], dense: bool) -> Bytes {
    let body = if dense { None } else { sparse(regs) };
    let mut v = Vec::with_capacity(DENSE);
    v.extend_from_slice(b"HYLL");
    v.push(u8::from(body.is_some()));
    v.extend_from_slice(&[0, 0, 0]);
    v.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0x80]);
    if let Some(b) = body {
        v.extend_from_slice(&b);
    } else {
        let mut d = vec![0u8; DENSE - HDR];
        for (i, r) in regs.iter().enumerate() {
            if *r != 0 {
                set_reg(&mut d, i, *r);
            }
        }
        v.extend_from_slice(&d);
    }
    Bytes::from(v)
}

/// An empty sparse sketch (its cached count stale, as after any `PFADD`
/// that changed it).
fn empty() -> Bytes {
    encode(&[0u8; M], false)
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let prev = z;
        z += x * y;
        y += y;
        if prev == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if prev == z {
            return z / 3.0;
        }
    }
}

/// Ertl's estimate of the number of distinct elements behind `regs`.
fn estimate(regs: &[u8]) -> u64 {
    let mut histo = [0u32; 64];
    for r in regs {
        histo[usize::from(*r)] += 1;
    }
    let m = M as f64;
    let q = Q as usize;
    let mut z = m * tau((m - f64::from(histo[q + 1])) / m);
    for j in (1..=q).rev() {
        z += f64::from(histo[j]);
        z *= 0.5;
    }
    z += m * sigma(f64::from(histo[0]) / m);
    let alpha_inf = 0.5 / std::f64::consts::LN_2;
    (alpha_inf * m * m / z).round() as u64
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a sketch: `Ok(None)` when absent, `Err` (the reply) when it
    /// holds something else; with its stored metadata.
    fn sketch(&mut self, key: &[u8]) -> Result<Result<Found, Frame>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok((None, raw)),
            Some(Meta {
                body: b @ (Body::Str(_) | Body::Chunked { .. }),
                ..
            }) => match self.string_of(b)?.as_deref().and_then(decode) {
                Some(sk) => Ok((Some(sk), raw)),
                None => Err(not_hll()),
            },
            Some(_) => Err(reply::wrongtype()),
        })
    }
}

/// `PFADD k [e ...]`: 1 when a register changed or the key was created.
fn pfadd<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (sk, raw) = tri!(cx.sketch(&a[1])?);
    let exp = raw
        .as_ref()
        .filter(|m| !m.expired(cx.now))
        .and_then(|m| m.exp);
    let created = sk.is_none();
    let mut regs = sk.map_or_else(|| vec![0u8; M], |s| s.regs);
    let mut changed = false;
    for e in &a[2..] {
        let (i, c) = index_and_count(e);
        if regs[i] < c {
            regs[i] = c;
            changed = true;
        }
    }
    if changed {
        cx.put_string(&a[1], raw.as_ref(), encode(&regs, false), exp)?;
    } else if created {
        cx.put_string(&a[1], raw.as_ref(), empty(), None)?;
    }
    Ok(Frame::Integer(i64::from(changed || created)))
}

/// `PFCOUNT k ...`: one key's estimate, or the union's.
fn pfcount<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    if a.len() == 2 {
        let (sk, _) = tri!(cx.sketch(&a[1])?);
        return Ok(Frame::Integer(match sk {
            None => 0,
            Some(s) => s.cached.unwrap_or_else(|| estimate(&s.regs)) as i64,
        }));
    }
    let mut regs = vec![0u8; M];
    for k in &a[1..] {
        let (sk, _) = tri!(cx.sketch(k)?);
        if let Some(s) = sk {
            for (r, x) in regs.iter_mut().zip(s.regs) {
                *r = (*r).max(x);
            }
        }
    }
    Ok(Frame::Integer(estimate(&regs) as i64))
}

/// `PFMERGE dst [src ...]`: the registers' maximum over `dst` and the
/// sources, stored dense.
fn pfmerge<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut regs = vec![0u8; M];
    let mut dst = None;
    for (n, k) in a[1..].iter().enumerate() {
        let (sk, raw) = tri!(cx.sketch(k)?);
        if n == 0 {
            dst = Some(raw);
        }
        if let Some(s) = sk {
            for (r, x) in regs.iter_mut().zip(s.regs) {
                *r = (*r).max(x);
            }
        }
    }
    let raw = dst.flatten();
    let exp = raw
        .as_ref()
        .filter(|m| !m.expired(cx.now))
        .and_then(|m| m.exp);
    cx.put_string(&a[1], raw.as_ref(), encode(&regs, true), exp)?;
    Ok(Frame::ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_round_trip_in_both_forms() {
        let mut regs = vec![0u8; M];
        for (i, r) in regs.iter_mut().enumerate().step_by(97) {
            *r = (i % 51) as u8 + 1;
        }
        let d = decode(&encode(&regs, true)).unwrap();
        assert_eq!(d.regs, regs);
        let small: Vec<u8> = regs.iter().map(|r| (*r).min(32)).collect();
        let s = encode(&small, false);
        assert_eq!(s[4], 1);
        assert_eq!(decode(&s).unwrap().regs, small);
        assert_eq!(decode(&empty()).unwrap().cached, None);
        assert_eq!(estimate(&[0u8; M]), 0);
    }

    #[test]
    fn estimates_are_close() {
        let mut regs = vec![0u8; M];
        for i in 0..100_000u32 {
            let (j, c) = index_and_count(format!("e{i}").as_bytes());
            regs[j] = regs[j].max(c);
        }
        let e = estimate(&regs) as f64;
        assert!((e - 100_000.0).abs() / 100_000.0 < 0.03, "{e}");
    }
}
