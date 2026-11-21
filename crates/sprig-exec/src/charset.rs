//! Character-set conversion of string values: latin1 (the servers' latin1
//! is Windows-1252), ascii and utf8mb4, as `CONVERT(... USING ...)`, the
//! aggregation of operands of different character sets and the result
//! sent to a utf8mb4 client do it.

use sprig_engine::keys::collation::Charset;

/// Windows-1252's characters at 0x80 to 0x9F (five positions are unused
/// there and stand for the control character of that number).
const HIGH: [u32; 32] = [
    0x20AC, 0x81, 0x201A, 0x192, 0x201E, 0x2026, 0x2020, 0x2021, 0x2C6, 0x2030, 0x160, 0x2039,
    0x152, 0x8D, 0x17D, 0x8F, 0x90, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x2DC,
    0x2122, 0x161, 0x203A, 0x153, 0x9D, 0x17E, 0x178,
];

/// The character a latin1 byte stands for.
pub(crate) fn latin1_char(b: u8) -> char {
    let cp = match b {
        0x80..=0x9F => HIGH[usize::from(b - 0x80)],
        _ => u32::from(b),
    };
    char::from_u32(cp).unwrap_or('?')
}

/// The latin1 byte of a character, if latin1 has it.
pub(crate) fn latin1_byte(c: char) -> Option<u8> {
    let cp = u32::from(c);
    if cp < 0x80 || (0xA0..=0xFF).contains(&cp) {
        return Some(cp as u8);
    }
    HIGH.iter().position(|h| *h == cp).map(|i| 0x80 + i as u8)
}

/// What a conversion met that the target cannot hold.
pub(crate) enum Trouble {
    /// A character of the source the target lacks (replaced by `?`): its
    /// bytes in the source.
    Unmappable(Vec<u8>),
    /// Bytes that are no character of the source (UTF-8 read from a
    /// binary string): the bad bytes and the rest after them.
    Invalid(Vec<u8>),
}

/// Converts `b` from `from` to `to`. A character the target lacks becomes
/// `?`; the first such trouble is reported. Conversions from or to the
/// binary character set keep the bytes (a binary string read as UTF-8 is
/// checked).
pub(crate) fn convert(b: &[u8], from: Charset, to: Charset) -> (Vec<u8>, Option<Trouble>) {
    if from == to || to == Charset::Binary {
        return (b.to_vec(), None);
    }
    match (from, to) {
        (Charset::Binary, Charset::Utf8mb4) => utf8_check(b),
        (Charset::Binary, Charset::Latin1) => (b.to_vec(), None),
        (Charset::Binary | Charset::Latin1, Charset::Ascii) => {
            let mut trouble = None;
            let out = b
                .iter()
                .map(|&c| {
                    if c < 0x80 {
                        c
                    } else {
                        trouble.get_or_insert_with(|| Trouble::Unmappable(vec![c]));
                        b'?'
                    }
                })
                .collect();
            (out, trouble)
        }
        (Charset::Latin1, Charset::Utf8mb4) => {
            let mut s = String::with_capacity(b.len() * 2);
            for &c in b {
                s.push(latin1_char(c));
            }
            (s.into_bytes(), None)
        }
        // Bytes beyond ASCII in an ascii string are no characters.
        (Charset::Ascii, _) => {
            let mut trouble = None;
            let out = b
                .iter()
                .map(|&c| {
                    if c < 0x80 {
                        c
                    } else {
                        trouble.get_or_insert_with(|| Trouble::Invalid(vec![c]));
                        b'?'
                    }
                })
                .collect();
            (out, trouble)
        }
        (Charset::Utf8mb4, Charset::Latin1 | Charset::Ascii) => {
            let s = String::from_utf8_lossy(b);
            let mut out = Vec::with_capacity(b.len());
            let mut trouble = None;
            for c in s.chars() {
                let byte = if to == Charset::Latin1 {
                    latin1_byte(c)
                } else {
                    c.is_ascii().then_some(c as u8)
                };
                match byte {
                    Some(x) => out.push(x),
                    None => {
                        let mut buf = [0u8; 4];
                        trouble.get_or_insert_with(|| {
                            Trouble::Unmappable(c.encode_utf8(&mut buf).as_bytes().to_vec())
                        });
                        out.push(b'?');
                    }
                }
            }
            (out, trouble)
        }
        _ => (b.to_vec(), None),
    }
}

/// UTF-8 checked: each bad byte becomes `?`.
fn utf8_check(b: &[u8]) -> (Vec<u8>, Option<Trouble>) {
    let mut out = Vec::with_capacity(b.len());
    let mut rest = b;
    let mut trouble = None;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.extend_from_slice(s.as_bytes());
                return (out, trouble);
            }
            Err(e) => {
                let good = e.valid_up_to();
                out.extend_from_slice(&rest[..good]);
                trouble.get_or_insert_with(|| Trouble::Invalid(rest[good..].to_vec()));
                out.push(b'?');
                let skip = e.error_len().unwrap_or(rest.len() - good).max(1);
                rest = &rest[good + skip..];
            }
        }
    }
}

/// The name a server prints for a character set.
pub(crate) fn name(c: Charset) -> &'static str {
    match c {
        Charset::Binary => "binary",
        Charset::Ascii => "ascii",
        Charset::Latin1 => "latin1",
        Charset::Utf8mb4 => "utf8mb4",
    }
}

/// Bytes in upper-case hexadecimal, as MySQL's messages show them.
pub(crate) fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

/// Bytes as MariaDB's messages show them: printable ASCII as it is, the
/// rest as `\xNN`.
pub(crate) fn escaped(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        if (0x20..0x7f).contains(&x) {
            s.push(char::from(x));
        } else {
            let _ = write!(s, "\\x{x:02X}");
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latin1_is_windows_1252() {
        assert_eq!(latin1_char(0xE9), 'é');
        assert_eq!(latin1_char(0x80), '€');
        assert_eq!(latin1_char(0x81), '\u{81}');
        assert_eq!(latin1_byte('€'), Some(0x80));
        assert_eq!(latin1_byte('中'), None);
        let (b, t) = convert("é中".as_bytes(), Charset::Utf8mb4, Charset::Latin1);
        assert_eq!(b, vec![0xE9, b'?']);
        assert!(matches!(t, Some(Trouble::Unmappable(_))));
        let (b, _) = convert(&[0xE9, 0x80], Charset::Latin1, Charset::Utf8mb4);
        assert_eq!(String::from_utf8(b).unwrap(), "é€");
        let (b, t) = convert(&[0xFF, b'A'], Charset::Binary, Charset::Utf8mb4);
        assert_eq!(b, b"?A".to_vec());
        assert!(matches!(t, Some(Trouble::Invalid(_))));
    }
}
