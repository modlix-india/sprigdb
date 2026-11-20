//! String keys under the supported collations: memcomparable encodings whose byte
//! order is the collation's order.
//!
//! Supported: `binary`; `utf8mb4_bin` and its NO PAD twins
//! (`utf8mb4_nopad_bin`, MySQL's `utf8mb4_0900_bin`); `utf8mb4_general_ci`
//! and `utf8mb4_general_nopad_ci`; `utf8mb4_uca1400_ai_ci` (MariaDB 11.8's
//! default, and so the sysbench table's) and `utf8mb4_uca1400_nopad_ai_ci`
//! (which MySQL's `utf8mb4_0900_ai_ci` aliases). All are one-level.
//!
//! Weights:
//! - `_bin`: the code point.
//! - `general_ci`: one weight per BMP character from a table probed from the
//!   oracle (`tools/gen_tables.py`); every character outside the BMP weighs
//!   U+FFFD.
//! - `uca1400_ai_ci`: the primary weights of the public DUCET 14.0.0, with its
//!   contractions (longest match first), at most 8 weights per character or
//!   contraction, and UCA implicit weights for code points the DUCET doesn't
//!   list. The oracle agrees on every code point and every contraction.
//!
//! **PAD SPACE.** The comparison treats the shorter string as padded with
//! spaces, so a weight below the space's after a common prefix makes the
//! longer string *smaller*: `'a\t' < 'a'` (checked on the oracle). Trimming
//! trailing spaces is enough for equality but not for order, so a PAD SPACE
//! key is a sequence of tokens, one per non-space weight `w` preceded by `k`
//! space weights:
//!
//! ```text
//! w < space: 0x01 + min(k, 62) [k u32 BE if k >= 62] | w
//! end:       0x40
//! w > space: 0xFF - min(k, 63) [!k u32 BE if k >= 63] | w
//! ```
//!
//! Every token below the space sorts before the end, and the end before
//! every token above it; among "below" tokens a longer run of spaces sorts
//! later, among "above" tokens earlier. That is exactly the padded order.
//! Strings equal under PAD SPACE (`'a'`, `'a  '`) get identical keys. NO PAD
//! keys are the weights, `00`-escaped and terminated by `00 00`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Read;
use std::sync::OnceLock;

use super::memcmp;

/// Most weights MariaDB keeps for one character or contraction (U+FDFA has
/// 18 primaries in the DUCET; the oracle shows 8).
const MAX_WEIGHTS: usize = 8;
const UCA_SPACE: u32 = 0x0209;
const GENERAL_SPACE: u32 = 0x0020;
const BIN_SPACE: u32 = 0x20;

/// The pad attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pad {
    /// Trailing spaces don't count; shorter strings compare as space-padded.
    Space,
    /// Every character counts.
    NoPad,
}

/// One supported collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Collation {
    /// Bytes, NO PAD.
    Binary,
    /// `utf8mb4_bin`: code points, PAD SPACE.
    Utf8mb4Bin,
    /// `utf8mb4_nopad_bin` and MySQL's `utf8mb4_0900_bin`.
    Utf8mb4NopadBin,
    /// `utf8mb4_general_ci`.
    Utf8mb4GeneralCi,
    /// `utf8mb4_general_nopad_ci`.
    Utf8mb4GeneralNopadCi,
    /// `utf8mb4_uca1400_ai_ci`, MariaDB 11.8's default for `utf8mb4`.
    Utf8mb4Uca1400AiCi,
    /// `utf8mb4_uca1400_nopad_ai_ci`, also MySQL's `utf8mb4_0900_ai_ci`.
    Utf8mb4Uca1400NopadAiCi,
}

#[derive(Clone, Copy)]
enum Source {
    Bytes,
    CodePoint,
    General,
    Uca,
}

impl Collation {
    /// The collation of a MariaDB or MySQL name, if the engine supports it.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "binary" => Self::Binary,
            "utf8mb4_bin" => Self::Utf8mb4Bin,
            "utf8mb4_nopad_bin" | "utf8mb4_0900_bin" => Self::Utf8mb4NopadBin,
            "utf8mb4_general_ci" => Self::Utf8mb4GeneralCi,
            "utf8mb4_general_nopad_ci" => Self::Utf8mb4GeneralNopadCi,
            "utf8mb4_uca1400_ai_ci" => Self::Utf8mb4Uca1400AiCi,
            "utf8mb4_uca1400_nopad_ai_ci" | "utf8mb4_0900_ai_ci" => Self::Utf8mb4Uca1400NopadAiCi,
            _ => return None,
        })
    }

    /// MariaDB's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Utf8mb4Bin => "utf8mb4_bin",
            Self::Utf8mb4NopadBin => "utf8mb4_nopad_bin",
            Self::Utf8mb4GeneralCi => "utf8mb4_general_ci",
            Self::Utf8mb4GeneralNopadCi => "utf8mb4_general_nopad_ci",
            Self::Utf8mb4Uca1400AiCi => "utf8mb4_uca1400_ai_ci",
            Self::Utf8mb4Uca1400NopadAiCi => "utf8mb4_uca1400_nopad_ai_ci",
        }
    }

    /// The pad attribute.
    #[must_use]
    pub fn pad(self) -> Pad {
        match self {
            Self::Utf8mb4Bin | Self::Utf8mb4GeneralCi | Self::Utf8mb4Uca1400AiCi => Pad::Space,
            _ => Pad::NoPad,
        }
    }

    fn source(self) -> Source {
        match self {
            Self::Binary => Source::Bytes,
            Self::Utf8mb4Bin | Self::Utf8mb4NopadBin => Source::CodePoint,
            Self::Utf8mb4GeneralCi | Self::Utf8mb4GeneralNopadCi => Source::General,
            Self::Utf8mb4Uca1400AiCi | Self::Utf8mb4Uca1400NopadAiCi => Source::Uca,
        }
    }

    fn space(self) -> u32 {
        match self.source() {
            Source::Bytes | Source::CodePoint => BIN_SPACE,
            Source::General => GENERAL_SPACE,
            Source::Uca => UCA_SPACE,
        }
    }

    /// The level-1 weights of `s` (UTF-8; invalid sequences weigh as U+FFFD
    /// would), without any padding rule applied. For `binary`, the bytes.
    pub fn weights(self, s: &[u8], out: &mut Vec<u32>) {
        match self.source() {
            Source::Bytes => out.extend(s.iter().map(|&b| u32::from(b))),
            Source::CodePoint => for_each_char(s, |c| out.push(c)),
            Source::General => {
                let t = general_table();
                for_each_char(s, |c| {
                    let w = usize::try_from(c)
                        .ok()
                        .and_then(|i| t.get(i))
                        .map_or(0xFFFD, |&w| u32::from(w));
                    out.push(w);
                });
            }
            Source::Uca => {
                let mut cps = Vec::with_capacity(s.len());
                for_each_char(s, |c| cps.push(c));
                uca_weights(&cps, out);
            }
        }
    }

    /// Appends the memcomparable key of `s`.
    pub fn encode_key(self, s: &[u8], out: &mut Vec<u8>) {
        if let Source::Bytes = self.source() {
            memcmp::put_bytes(out, s);
            return;
        }
        let mut w = Vec::with_capacity(s.len());
        self.weights(s, &mut w);
        let put: fn(&mut Vec<u8>, u32) = match self.source() {
            Source::CodePoint => put_utf8,
            _ => put_u16,
        };
        match self.pad() {
            Pad::NoPad => {
                let mut raw = Vec::with_capacity(w.len() * 2);
                for &x in &w {
                    put(&mut raw, x);
                }
                memcmp::put_bytes(out, &raw);
            }
            Pad::Space => encode_padded(&w, self.space(), put, out),
        }
    }

    /// Compares two strings under the collation (the reference the key
    /// encoding is tested against).
    #[must_use]
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        let (mut wl, mut wr) = (Vec::new(), Vec::new());
        self.weights(left, &mut wl);
        self.weights(right, &mut wr);
        match self.pad() {
            Pad::NoPad => wl.cmp(&wr),
            Pad::Space => {
                let space = self.space();
                let len = wl.len().max(wr.len());
                for pos in 0..len {
                    let l = wl.get(pos).copied().unwrap_or(space);
                    let r = wr.get(pos).copied().unwrap_or(space);
                    if l != r {
                        return l.cmp(&r);
                    }
                }
                Ordering::Equal
            }
        }
    }
}

fn put_u16(out: &mut Vec<u8>, w: u32) {
    // Table weights are u16; implicit weights are built to fit.
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(w as u16).to_be_bytes());
}

fn put_utf8(out: &mut Vec<u8>, cp: u32) {
    let c = char::from_u32(cp).unwrap_or(char::REPLACEMENT_CHARACTER);
    let mut buf = [0u8; 4];
    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
}

fn encode_padded(w: &[u32], space: u32, put: fn(&mut Vec<u8>, u32), out: &mut Vec<u8>) {
    let mut end = w.len();
    while end > 0 && w[end - 1] == space {
        end -= 1;
    }
    let mut k: u32 = 0;
    for &x in &w[..end] {
        if x == space {
            k += 1;
            continue;
        }
        if x < space {
            if k >= 62 {
                out.push(0x3F);
                out.extend_from_slice(&k.to_be_bytes());
            } else {
                #[allow(clippy::cast_possible_truncation)]
                out.push(0x01 + k as u8);
            }
        } else if k >= 63 {
            out.push(0xC0);
            out.extend_from_slice(&(u32::MAX - k).to_be_bytes());
        } else {
            #[allow(clippy::cast_possible_truncation)]
            out.push(0xFF - k as u8);
        }
        put(out, x);
        k = 0;
    }
    out.push(0x40);
}

fn for_each_char(s: &[u8], mut f: impl FnMut(u32)) {
    match std::str::from_utf8(s) {
        Ok(t) => t.chars().for_each(|c| f(u32::from(c))),
        Err(_) => String::from_utf8_lossy(s)
            .chars()
            .for_each(|c| f(u32::from(c))),
    }
}

// ---- UCA 14.0.0 level 1 ---------------------------------------------------

struct UcaTable {
    /// BMP: index into `pool` as (offset << 8 | len), or `u32::MAX` if absent.
    bmp: Vec<u32>,
    supp: HashMap<u32, (u32, u8)>,
    pool: Vec<u16>,
    /// By first code point, longest first.
    contractions: HashMap<u32, Vec<Contraction>>,
}

/// A contraction's code points and its weights.
type Contraction = (Vec<u32>, Vec<u16>);

impl UcaTable {
    fn single(&self, cp: u32) -> Option<&[u16]> {
        let (off, len) = if cp < 0x1_0000 {
            let v = self.bmp[cp as usize];
            if v == u32::MAX {
                return None;
            }
            (v >> 8, v & 0xFF)
        } else {
            let &(off, len) = self.supp.get(&cp)?;
            (off, u32::from(len))
        };
        Some(&self.pool[off as usize..(off + len) as usize])
    }
}

static UCA_DATA: &[u8] = include_bytes!("../../data/uca1400_l1.bin.z");
static GENERAL_DATA: &[u8] = include_bytes!("../../data/general_ci.bin.z");

fn inflate(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // The tables are compiled in; failure is a build defect the tests catch.
    #[allow(clippy::expect_used)]
    flate2::read::ZlibDecoder::new(data)
        .read_to_end(&mut out)
        .expect("sprig-engine collation table is corrupt");
    out
}

struct Cursor<'a>(&'a [u8]);

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> &[u8] {
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        h
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u16(&mut self) -> u16 {
        let b = self.take(2);
        u16::from_le_bytes([b[0], b[1]])
    }
    fn u32(&mut self) -> u32 {
        let b = self.take(4);
        u32::from_le_bytes([b[0], b[1], b[2], b[3]])
    }
}

fn uca_table() -> &'static UcaTable {
    static T: OnceLock<UcaTable> = OnceLock::new();
    T.get_or_init(|| {
        let raw = inflate(UCA_DATA);
        let mut c = Cursor(&raw);
        assert_eq!(c.take(4), b"SPU1", "uca1400 table magic");
        let mut t = UcaTable {
            bmp: vec![u32::MAX; 0x1_0000],
            supp: HashMap::new(),
            pool: Vec::new(),
            contractions: HashMap::new(),
        };
        for _ in 0..c.u32() {
            let cp = c.u32();
            let n = usize::from(c.u8());
            let off = u32::try_from(t.pool.len()).unwrap_or(u32::MAX);
            let ws: Vec<u16> = (0..n).map(|_| c.u16()).collect();
            let total = n.min(MAX_WEIGHTS);
            t.pool.extend_from_slice(&ws[..total]);
            #[allow(clippy::cast_possible_truncation)]
            if cp < 0x1_0000 {
                t.bmp[cp as usize] = (off << 8) | total as u32;
            } else {
                t.supp.insert(cp, (off, total as u8));
            }
        }
        for _ in 0..c.u32() {
            let ncp = usize::from(c.u8());
            let cps: Vec<u32> = (0..ncp).map(|_| c.u32()).collect();
            let n = usize::from(c.u8());
            let mut ws: Vec<u16> = (0..n).map(|_| c.u16()).collect();
            ws.truncate(MAX_WEIGHTS);
            t.contractions.entry(cps[0]).or_default().push((cps, ws));
        }
        for v in t.contractions.values_mut() {
            v.sort_by_key(|(cps, _)| std::cmp::Reverse(cps.len()));
        }
        t
    })
}

const CORE_HAN: &[(u32, u32)] = &[
    (0x4E00, 0x9FFF),
    (0xFA0E, 0xFA0F),
    (0xFA11, 0xFA11),
    (0xFA13, 0xFA14),
    (0xFA1F, 0xFA1F),
    (0xFA21, 0xFA21),
    (0xFA23, 0xFA24),
    (0xFA27, 0xFA29),
];
const EXT_HAN: &[(u32, u32)] = &[
    (0x3400, 0x4DBF),
    (0x2_0000, 0x2_A6DF),
    (0x2_A700, 0x2_B738),
    (0x2_B740, 0x2_B81D),
    (0x2_B820, 0x2_CEA1),
    (0x2_CEB0, 0x2_EBE0),
    (0x3_0000, 0x3_134A),
];
/// (range, base, offset). The Tangut Supplement range ends at U+18D7F: the
/// oracle weighs U+18D80–U+18D8F as unassigned code points.
const SPECIAL: &[(u32, u32, u32, u32)] = &[
    (0x1_7000, 0x1_8AFF, 0xFB00, 0x1_7000),
    (0x1_8D00, 0x1_8D7F, 0xFB00, 0x1_7000),
    (0x1_B170, 0x1_B2FF, 0xFB01, 0x1_B170),
    (0x1_8B00, 0x1_8CFF, 0xFB02, 0x1_8B00),
];

fn in_ranges(cp: u32, r: &[(u32, u32)]) -> bool {
    r.iter().any(|&(a, b)| (a..=b).contains(&cp))
}

/// UCA implicit primary weights (UTS #10 §10.1) for a code point the DUCET
/// doesn't list.
fn implicit(cp: u32) -> [u32; 2] {
    for &(a, b, base, off) in SPECIAL {
        if (a..=b).contains(&cp) {
            return [base, ((cp - off) & 0x7FFF) | 0x8000];
        }
    }
    let base = if in_ranges(cp, CORE_HAN) {
        0xFB40
    } else if in_ranges(cp, EXT_HAN) {
        0xFB80
    } else {
        0xFBC0
    };
    [base + (cp >> 15), (cp & 0x7FFF) | 0x8000]
}

fn uca_weights(cps: &[u32], out: &mut Vec<u32>) {
    let t = uca_table();
    let mut i = 0;
    while i < cps.len() {
        let cp = cps[i];
        if let Some(cands) = t.contractions.get(&cp)
            && let Some((m, ws)) = cands.iter().find(|(m, _)| cps[i..].starts_with(m))
        {
            out.extend(ws.iter().map(|&w| u32::from(w)));
            i += m.len();
            continue;
        }
        match t.single(cp) {
            Some(ws) => out.extend(ws.iter().map(|&w| u32::from(w))),
            None => out.extend_from_slice(&implicit(cp)),
        }
        i += 1;
    }
}

fn general_table() -> &'static [u16] {
    static T: OnceLock<Vec<u16>> = OnceLock::new();
    T.get_or_init(|| {
        let raw = inflate(GENERAL_DATA);
        let mut c = Cursor(&raw);
        assert_eq!(c.take(4), b"SPG1", "general_ci table magic");
        (0..0x1_0000).map(|_| c.u16()).collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const ALL: [Collation; 7] = [
        Collation::Binary,
        Collation::Utf8mb4Bin,
        Collation::Utf8mb4NopadBin,
        Collation::Utf8mb4GeneralCi,
        Collation::Utf8mb4GeneralNopadCi,
        Collation::Utf8mb4Uca1400AiCi,
        Collation::Utf8mb4Uca1400NopadAiCi,
    ];

    fn key(c: Collation, s: &str) -> Vec<u8> {
        let mut o = Vec::new();
        c.encode_key(s.as_bytes(), &mut o);
        o
    }

    fn hex_weights(c: Collation, s: &str) -> String {
        let mut w = Vec::new();
        c.weights(s.as_bytes(), &mut w);
        w.iter().fold(String::new(), |mut s, x| {
            use std::fmt::Write as _;
            let _ = write!(s, "{x:04X}");
            s
        })
    }

    /// Weight strings from MariaDB 11.8 (`HEX(WEIGHT_STRING(...))`).
    #[test]
    fn oracle_weights() {
        let uca = Collation::Utf8mb4Uca1400AiCi;
        assert_eq!(hex_weights(uca, "a"), "2075");
        assert_eq!(hex_weights(uca, "a "), "20750209");
        assert_eq!(hex_weights(uca, "a\t"), "20750201");
        assert_eq!(hex_weights(Collation::Utf8mb4GeneralCi, "a\t"), "00410009");
        // U+FDFA keeps 8 of its 18 DUCET primaries.
        assert_eq!(
            hex_weights(uca, "\u{FDFA}"),
            "27C22802282D02092762280228022819"
        );
        // Implicit weights: a Han character, an unassigned code point and
        // the end of the Tangut Supplement block.
        assert_eq!(hex_weights(uca, "\u{4E00}"), "FB40CE00");
        assert_eq!(hex_weights(uca, "\u{18D80}"), "FBC38D80");
        assert_eq!(hex_weights(uca, "\u{18D00}"), "FB009D00");
    }

    #[test]
    fn oracle_equalities_and_order() {
        let uca = Collation::Utf8mb4Uca1400AiCi;
        assert_eq!(uca.compare(b"A", "\u{e1}".as_bytes()), Ordering::Equal);
        assert_eq!(key(uca, "abc"), key(uca, "ABC  "));
        assert_eq!(key(uca, "\u{df}"), key(uca, "ss"));
        // PAD SPACE: 'a\t' < 'a' (oracle: STRCMP = -1); NO PAD: greater.
        for c in [
            Collation::Utf8mb4Bin,
            Collation::Utf8mb4GeneralCi,
            Collation::Utf8mb4Uca1400AiCi,
        ] {
            assert!(key(c, "a\t") < key(c, "a"), "{}", c.name());
            assert_eq!(key(c, "a"), key(c, "a   "), "{}", c.name());
        }
        assert!(
            key(Collation::Utf8mb4Uca1400NopadAiCi, "a\t")
                > key(Collation::Utf8mb4Uca1400NopadAiCi, "a")
        );
        assert!(key(Collation::Utf8mb4NopadBin, "a ") > key(Collation::Utf8mb4NopadBin, "a"));
        // general_ci: every supplementary character weighs U+FFFD.
        let g = Collation::Utf8mb4GeneralCi;
        assert_eq!(
            g.compare("\u{1F600}".as_bytes(), "\u{1F4A9}".as_bytes()),
            Ordering::Equal
        );
    }

    fn alphabet() -> impl Strategy<Value = String> {
        let chars = prop_oneof![
            Just(' '),
            Just('\t'),
            Just('\u{1}'),
            Just('a'),
            Just('A'),
            Just('b'),
            Just('\u{e9}'),
            Just('\u{df}'),
            Just('s'),
            Just('\u{a0}'),
            Just('\u{200b}'),
            Just('\u{4e00}'),
            Just('\u{1f600}'),
            Just('\u{0306}'),
            Just('\u{0418}'),
            Just('L'),
            Just('\u{b7}'),
        ];
        proptest::collection::vec(chars, 0..10).prop_map(|v| v.into_iter().collect())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn key_order_matches_comparison(a in alphabet(), b in alphabet()) {
            for c in ALL {
                let want = c.compare(a.as_bytes(), b.as_bytes());
                let got = key(c, &a).cmp(&key(c, &b));
                prop_assert_eq!(want, got, "{} {:?} {:?}", c.name(), a, b);
            }
        }

        #[test]
        fn long_space_runs(k1 in 0usize..140, k2 in 0usize..140, hi in any::<bool>()) {
            let tail = if hi { "b" } else { "\t" };
            let a = format!("a{}{tail}", " ".repeat(k1));
            let b = format!("a{}{tail}", " ".repeat(k2));
            for c in [Collation::Utf8mb4Bin, Collation::Utf8mb4GeneralCi, Collation::Utf8mb4Uca1400AiCi] {
                prop_assert_eq!(c.compare(a.as_bytes(), b.as_bytes()), key(c, &a).cmp(&key(c, &b)));
            }
        }
    }

    #[test]
    fn names() {
        for c in ALL {
            assert_eq!(Collation::from_name(c.name()), Some(c));
        }
        assert_eq!(
            Collation::from_name("utf8mb4_0900_ai_ci"),
            Some(Collation::Utf8mb4Uca1400NopadAiCi)
        );
        assert_eq!(Collation::from_name("latin1_swedish_ci"), None);
    }
}
