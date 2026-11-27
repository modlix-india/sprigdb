//! MariaDB's fixed binary types `UUID`, `INET4` and `INET6`: their stored
//! bytes, text forms and order.
//!
//! A value is held as its stored bytes, which order as the server orders the
//! values (a plain byte comparison):
//! - `INET4`: the address's 4 bytes; `INET6`: its 16 bytes.
//! - `UUID`: 16 bytes. A time-ordered UUID (its version byte, the 7th, from
//!   `0x01` to `0x5f`, and the variant's top bit set) is stored with its
//!   five groups in reverse order (node, clock sequence, high, middle and low
//!   time), so such UUIDs order by node and then by time; any other UUID is
//!   stored as written. A text that, stored as written, would read back as a
//!   reordered UUID (version byte `0x80` or above, and the 9th byte from
//!   `0x01` to `0x80`) is not a UUID.
//!
//! These rules are the server's answers to black-box tests (MariaDB 11.8).

use std::fmt::Write as _;

/// One of the three types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fbt {
    /// `UUID`: 16 bytes.
    Uuid,
    /// `INET4`: 4 bytes.
    Inet4,
    /// `INET6`: 16 bytes.
    Inet6,
}

impl Fbt {
    /// The stored size in bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        match self {
            Fbt::Inet4 => 4,
            Fbt::Uuid | Fbt::Inet6 => 16,
        }
    }

    /// The type's name as the server writes it (`uuid`, `inet4`, `inet6`).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Fbt::Uuid => "uuid",
            Fbt::Inet4 => "inet4",
            Fbt::Inet6 => "inet6",
        }
    }

    /// The longest text of a value, in characters.
    #[must_use]
    pub const fn text_len(self) -> u32 {
        match self {
            Fbt::Uuid => 36,
            Fbt::Inet4 => 15,
            Fbt::Inet6 => 39,
        }
    }

    /// The stored bytes of a value given as text; `None` when the text
    /// isn't one.
    #[must_use]
    pub fn parse(self, text: &[u8]) -> Option<Vec<u8>> {
        match self {
            Fbt::Uuid => uuid_parse(text).map(|t| uuid_store(&t).to_vec()),
            Fbt::Inet4 => inet4_parse(text).map(|b| b.to_vec()),
            Fbt::Inet6 => inet6_parse(text).map(|b| b.to_vec()),
        }
    }

    /// The stored bytes of a value given as a binary string: the bytes
    /// themselves when the length is the type's, else `None`.
    #[must_use]
    pub fn from_binary(self, b: &[u8]) -> Option<Vec<u8>> {
        (b.len() == self.bytes()).then(|| b.to_vec())
    }

    /// The text of stored bytes; `None` when the length is wrong.
    #[must_use]
    pub fn text(self, stored: &[u8]) -> Option<String> {
        match self {
            Fbt::Uuid => {
                let s: &[u8; 16] = stored.try_into().ok()?;
                Some(uuid_text(&uuid_written(s)))
            }
            Fbt::Inet4 => {
                let s: &[u8; 4] = stored.try_into().ok()?;
                Some(format!("{}.{}.{}.{}", s[0], s[1], s[2], s[3]))
            }
            Fbt::Inet6 => {
                let s: &[u8; 16] = stored.try_into().ok()?;
                Some(inet6_text(s))
            }
        }
    }

    /// The bytes of a value in written order (`HEX(u)`, `CAST(u AS
    /// BINARY)`): a UUID's 16 bytes as its text spells them; the stored
    /// bytes for the addresses.
    #[must_use]
    pub fn written(self, stored: &[u8]) -> Vec<u8> {
        match (self, <&[u8; 16]>::try_from(stored)) {
            (Fbt::Uuid, Ok(s)) => uuid_written(s).to_vec(),
            _ => stored.to_vec(),
        }
    }
}

/// Whether a UUID, as written, is stored with its groups reversed.
fn uuid_swaps(t: &[u8; 16]) -> bool {
    (0x01..=0x5f).contains(&t[6]) && t[8] & 0x80 != 0
}

/// Whether stored bytes hold a reordered UUID.
fn uuid_swapped(s: &[u8; 16]) -> bool {
    s[6] >= 0x80 && (0x01..=0x80).contains(&s[8])
}

fn reverse_groups(t: &[u8; 16]) -> [u8; 16] {
    let mut s = [0u8; 16];
    s[0..6].copy_from_slice(&t[10..16]);
    s[6..8].copy_from_slice(&t[8..10]);
    s[8..10].copy_from_slice(&t[6..8]);
    s[10..12].copy_from_slice(&t[4..6]);
    s[12..16].copy_from_slice(&t[0..4]);
    s
}

fn unreverse_groups(s: &[u8; 16]) -> [u8; 16] {
    let mut t = [0u8; 16];
    t[10..16].copy_from_slice(&s[0..6]);
    t[8..10].copy_from_slice(&s[6..8]);
    t[6..8].copy_from_slice(&s[8..10]);
    t[4..6].copy_from_slice(&s[10..12]);
    t[0..4].copy_from_slice(&s[12..16]);
    t
}

/// The stored bytes of a UUID as written.
#[must_use]
pub fn uuid_store(t: &[u8; 16]) -> [u8; 16] {
    if uuid_swaps(t) { reverse_groups(t) } else { *t }
}

/// A UUID's bytes as written, from its stored bytes.
#[must_use]
pub fn uuid_written(s: &[u8; 16]) -> [u8; 16] {
    if uuid_swapped(s) {
        unreverse_groups(s)
    } else {
        *s
    }
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// A UUID's 16 bytes as written: 32 hex digits, with hyphens anywhere
/// after the first digit except at the end.
#[must_use]
pub fn uuid_parse(text: &[u8]) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut n = 0usize;
    for (i, &c) in text.iter().enumerate() {
        if c == b'-' {
            if i == 0 || i + 1 == text.len() {
                return None;
            }
            continue;
        }
        let d = hex_digit(c)?;
        if n >= 32 {
            return None;
        }
        out[n / 2] |= if n.is_multiple_of(2) { d << 4 } else { d };
        n += 1;
    }
    if n != 32 {
        return None;
    }
    // Stored as written, it would read back reordered.
    if !uuid_swaps(&out) && uuid_swapped(&out) {
        return None;
    }
    Some(out)
}

/// A UUID's text: lowercase, hyphens after the 4th, 6th, 8th and 10th byte.
#[must_use]
pub fn uuid_text(t: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, b) in t.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// An IPv4 address: four decimal parts of one to three digits, each at
/// most 255.
#[must_use]
pub fn inet4_parse(text: &[u8]) -> Option<[u8; 4]> {
    let mut out = [0u8; 4];
    let mut parts = 0usize;
    for part in text.split(|&c| c == b'.') {
        if parts == 4 || part.is_empty() || part.len() > 3 || !part.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let v = part.iter().fold(0u32, |v, &c| v * 10 + u32::from(c - b'0'));
        out[parts] = u8::try_from(v).ok()?;
        parts += 1;
    }
    (parts == 4).then_some(out)
}

/// An IPv6 address: up to eight groups of one to four hex digits, one `::`
/// for a run of zero groups, and optionally an IPv4 address in the last
/// two groups. A plain IPv4 address is taken as its IPv4-mapped form
/// (`::ffff:a.b.c.d`).
#[must_use]
pub fn inet6_parse(text: &[u8]) -> Option<[u8; 16]> {
    if let Some(v4) = inet4_parse(text) {
        let mut out = [0u8; 16];
        out[10] = 0xff;
        out[11] = 0xff;
        out[12..].copy_from_slice(&v4);
        return Some(out);
    }
    let (head, tail) = match find_double_colon(text) {
        Some(i) => (&text[..i], Some(&text[i + 2..])),
        None => (text, None),
    };
    let mut groups: Vec<u16> = Vec::with_capacity(8);
    let mut back: Vec<u16> = Vec::with_capacity(8);
    let read = |part: &[u8], out: &mut Vec<u16>, last: bool| -> Option<()> {
        if part.is_empty() {
            return Some(());
        }
        let pieces: Vec<&[u8]> = part.split(|&c| c == b':').collect();
        for (i, p) in pieces.iter().enumerate() {
            if last && i + 1 == pieces.len() && p.contains(&b'.') {
                let v4 = inet4_parse(p)?;
                out.push(u16::from_be_bytes([v4[0], v4[1]]));
                out.push(u16::from_be_bytes([v4[2], v4[3]]));
                continue;
            }
            if p.is_empty() || p.len() > 4 {
                return None;
            }
            let mut v = 0u16;
            for &c in *p {
                v = v << 4 | u16::from(hex_digit(c)?);
            }
            out.push(v);
        }
        Some(())
    };
    if let Some(t) = tail {
        if find_double_colon(t).is_some() {
            return None;
        }
        read(head, &mut groups, false)?;
        read(t, &mut back, true)?;
        if groups.len() + back.len() > 7 {
            return None;
        }
        groups.resize(8 - back.len(), 0);
        groups.extend(back);
    } else {
        read(head, &mut groups, true)?;
        if groups.len() != 8 {
            return None;
        }
    }
    let mut out = [0u8; 16];
    for (i, g) in groups.iter().enumerate() {
        out[2 * i..2 * i + 2].copy_from_slice(&g.to_be_bytes());
    }
    Some(out)
}

fn find_double_colon(t: &[u8]) -> Option<usize> {
    t.windows(2).position(|w| w == b"::")
}

/// An IPv6 address's text as the server writes it: an IPv4-mapped address
/// (`::ffff:a.b.c.d`), and an IPv4-compatible one past `::ffff`, end in
/// their IPv4 address; otherwise lowercase groups, the longest run of zero
/// groups (the first of equal runs, even a single group) written `::`.
#[must_use]
pub fn inet6_text(b: &[u8; 16]) -> String {
    let v4 = |b: &[u8; 16]| format!("{}.{}.{}.{}", b[12], b[13], b[14], b[15]);
    if b[..10].iter().all(|x| *x == 0) && b[10] == 0xff && b[11] == 0xff {
        return format!("::ffff:{}", v4(b));
    }
    if b[..12].iter().all(|x| *x == 0) && (b[12] != 0 || b[13] != 0) {
        return format!("::{}", v4(b));
    }
    let g: Vec<u16> = (0..8)
        .map(|i| u16::from_be_bytes([b[2 * i], b[2 * i + 1]]))
        .collect();
    let (mut best, mut best_len) = (usize::MAX, 0usize);
    let mut i = 0;
    while i < 8 {
        if g[i] == 0 {
            let start = i;
            while i < 8 && g[i] == 0 {
                i += 1;
            }
            if i - start > best_len {
                best = start;
                best_len = i - start;
            }
        } else {
            i += 1;
        }
    }
    let hex = |s: &[u16]| {
        s.iter()
            .map(|x| format!("{x:x}"))
            .collect::<Vec<_>>()
            .join(":")
    };
    if best_len == 0 {
        return hex(&g);
    }
    format!("{}::{}", hex(&g[..best]), hex(&g[best + best_len..]))
}

/// An `INET4` value's stored bytes as an `INET6` value: its IPv4-mapped
/// address.
#[must_use]
pub fn inet4_as_inet6(b: &[u8; 4]) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[10] = 0xff;
    out[11] = 0xff;
    out[12..].copy_from_slice(b);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Option<String> {
        Fbt::Uuid
            .parse(s.as_bytes())
            .and_then(|b| Fbt::Uuid.text(&b))
    }

    #[test]
    fn uuid_texts() {
        let c = "12345678-1234-1234-1234-123456789abc";
        assert_eq!(u(c).as_deref(), Some(c));
        assert_eq!(u("12345678123412341234123456789ABC").as_deref(), Some(c));
        assert_eq!(u("1-2345678123412341234123456789abc").as_deref(), Some(c));
        assert_eq!(u("12--345678123412341234123456789abc").as_deref(), Some(c));
        for bad in [
            "-12345678123412341234123456789abc",
            "12345678123412341234123456789abc-",
            "{12345678-1234-1234-1234-123456789abc}",
            "12345678123412341234123456789ab",
            "12345678123412341234123456789abcd",
            " 12345678-1234-1234-1234-123456789abc",
            "",
            // Stored as written, these would read back reordered.
            "01234567-89ab-cdef-0123-456789abcdef",
            "00000000-0000-8000-0100-000000000000",
            "00000000-0000-ff00-8000-000000000000",
        ] {
            assert_eq!(u(bad), None, "{bad}");
        }
        for good in [
            "00000000-0000-8000-0000-000000000000",
            "00000000-0000-8000-0001-000000000000",
            "00000000-0000-ff00-8100-000000000000",
            "00000000-0000-7f00-1000-000000000000",
            "01234567-89ab-8def-8123-456789abcdef",
        ] {
            assert_eq!(u(good).as_deref(), Some(good));
        }
    }

    #[test]
    fn uuids_order_as_the_server_orders_them() {
        // An ORDER BY of these on MariaDB 11.8, in its order.
        let sorted = [
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0001-000000000000",
            "00000000-0000-0000-8000-000000000003",
            "00000000-0000-0001-0000-000000000000",
            "00000000-0000-1000-0000-000000000003",
            "00000000-0000-6000-0000-000000000003",
            "00000000-0000-6000-8000-000000000003",
            "00000000-0000-7000-8000-000000000003",
            "00000000-0001-1000-8000-000000000000",
            "00000000-0000-1001-8000-000000000000",
            "00000000-0000-1000-8001-000000000000",
            "00000000-0001-0000-0000-000000000000",
            "20000000-0000-1000-8000-000000000001",
            "20000000-0000-4000-8000-000000000001",
            "10000000-0000-1000-8000-000000000002",
            "10000000-0000-4000-8000-000000000002",
            "00000000-0000-1000-8000-000000000003",
            "00000000-0000-4000-8000-000000000003",
            "00000000-0000-1000-c000-000000000003",
            "10000000-0000-0000-0000-000000000002",
            "20000000-0000-0000-0000-000000000001",
        ];
        let stored: Vec<Vec<u8>> = sorted
            .iter()
            .map(|s| Fbt::Uuid.parse(s.as_bytes()).unwrap())
            .collect();
        for w in stored.windows(2) {
            assert!(w[0] < w[1], "{w:?}");
        }
        // A binary string is taken as stored bytes.
        let raw = [0, 0, 0, 0, 0, 0, 0x80, 0, 0x10, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            Fbt::Uuid.text(&raw).as_deref(),
            Some("00000000-0000-1000-8000-000000000000")
        );
        // HEX(u) and CAST(u AS BINARY) give the bytes as written.
        let s = Fbt::Uuid
            .parse(b"00000000-0000-1000-8000-000000000001")
            .unwrap();
        assert_eq!(
            Fbt::Uuid.written(&s),
            [0, 0, 0, 0, 0, 0, 0x10, 0, 0x80, 0, 0, 0, 0, 0, 0, 1]
        );
    }

    fn i6(s: &str) -> Option<String> {
        Fbt::Inet6
            .parse(s.as_bytes())
            .and_then(|b| Fbt::Inet6.text(&b))
    }

    #[test]
    fn inet6_texts() {
        for (input, out) in [
            ("1.2.3.4", "::ffff:1.2.3.4"),
            ("::ffff:1.2.3.4", "::ffff:1.2.3.4"),
            ("2001:0DB8:0000:0000:0000:0000:0000:0001", "2001:db8::1"),
            ("2001:db8:0:0:1:0:0:1", "2001:db8::1:0:0:1"),
            ("::", "::"),
            ("1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8"),
            ("1::", "1::"),
            ("::1.2.3.4", "::1.2.3.4"),
            ("1:0:0:2:0:0:0:3", "1:0:0:2::3"),
            ("1:0:2:0:3:0:4:0", "1::2:0:3:0:4:0"),
            ("0:0:1:0:0:0:0:0", "0:0:1::"),
            ("::ffff:0:1.2.3.4", "::ffff:0:102:304"),
            ("::1:2:3:4:5:6", "::1:2:3:4:5:6"),
            ("::0.0.1.2", "::102"),
            ("::0.1.0.0", "::0.1.0.0"),
            ("::0.0.255.255", "::ffff"),
            ("::ffff:0.0.0.1", "::ffff:0.0.0.1"),
            ("::01.2.3.4", "::1.2.3.4"),
            ("::1:2:3:4:5:6:7", "::1:2:3:4:5:6:7"),
            ("1:2:3:4:5:6:7::", "1:2:3:4:5:6:7::"),
            ("ABCD::", "abcd::"),
            ("1:2:3:4:5:6:1.2.3.4", "1:2:3:4:5:6:102:304"),
            ("1::1.2.3.4", "1::102:304"),
            ("1.2.3.004", "::ffff:1.2.3.4"),
            ("::1", "::1"),
        ] {
            assert_eq!(i6(input).as_deref(), Some(out), "{input}");
        }
        for bad in [
            "1.2.3",
            " ::1",
            "fe80::1%eth0",
            "00001::",
            ":::",
            "1:2:3:4:5:6:7:8:9",
            "1::2::3",
            "1.2.3.4::",
            "::1.2.3.4.5",
            "::256.1.1.1",
            ":1::",
            "1:",
            "1.2.3.0004",
            "",
        ] {
            assert_eq!(i6(bad), None, "{bad}");
        }
    }

    #[test]
    fn inet4_texts() {
        let i4 = |s: &str| {
            Fbt::Inet4
                .parse(s.as_bytes())
                .and_then(|b| Fbt::Inet4.text(&b))
        };
        assert_eq!(i4("01.2.3.4").as_deref(), Some("1.2.3.4"));
        assert_eq!(i4("255.255.255.255").as_deref(), Some("255.255.255.255"));
        for bad in [
            "1.2.3.256",
            "::1",
            "1..2.3",
            "1.2.3.4.",
            "0x1.2.3.4",
            "1.2.3.-4",
            "",
        ] {
            assert_eq!(i4(bad), None, "{bad}");
        }
        assert_eq!(
            Fbt::Inet6.text(&inet4_as_inet6(&[1, 2, 3, 4])).as_deref(),
            Some("::ffff:1.2.3.4")
        );
    }
}
