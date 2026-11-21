//! String keys under the supported collations: memcomparable encodings whose byte
//! order is the collation's order.
//!
//! The collations, by family:
//!
//! - `binary`: bytes, NO PAD.
//! - `_bin`: `utf8mb4_bin` and its NO PAD twin (`utf8mb4_nopad_bin`, MySQL's
//!   `utf8mb4_0900_bin`), `latin1_bin`, `ascii_bin` and their NO PAD twins.
//! - `general_ci`: `utf8mb4_general_ci` (and nopad), `latin1_swedish_ci` (and
//!   nopad), `latin1_general_ci`, `latin1_general_cs`, `ascii_general_ci`
//!   (and nopad). One weight per character.
//! - UCA 4.0.0 and 5.2.0: `utf8mb4_unicode_ci`, `utf8mb4_unicode_520_ci` and
//!   their nopad twins. One level, no contractions.
//! - UCA 14.0.0, MariaDB's `utf8mb4_uca1400_*`: `ai_ci` (level 1), `as_ci`
//!   (levels 1 and 2), `ai_cs` (1 and 3), `as_cs` (1 to 3), each PAD SPACE and
//!   `nopad`, with the DUCET's contractions.
//! - UCA 9.0.0, MySQL's `utf8mb4_0900_ai_ci`, `_as_ci` and `_as_cs`: NO PAD,
//!   no contractions.
//!
//! A `utf8mb3` collation is its `utf8mb4` twin: the strings are UTF-8 either
//! way and weigh the same. The same collation name can mean different
//! weights on the two servers (`utf8mb4_0900_ai_ci` is UCA 9.0.0 on MySQL
//! and an alias of `utf8mb4_uca1400_nopad_ai_ci` on MariaDB), so
//! [`Collation::resolve`] takes the server's [`Flavor`].
//!
//! Weights:
//! - `_bin`: the code point, or the byte.
//! - `utf8mb4_general_ci`: one weight per BMP character from a table probed
//!   from the oracle (`tools/gen_tables.py`); every character outside the
//!   BMP weighs U+FFFD.
//! - `utf8mb4_uca1400_ai_ci` and its nopad twin: the primary weights of the
//!   public DUCET 14.0.0, with its contractions (longest match first), at
//!   most 8 weights per character or contraction, and UCA implicit weights
//!   for code points the DUCET doesn't list. The oracle agrees on every code
//!   point and every contraction.
//! - Every other table: the servers' own answers, one `WEIGHT_STRING` per
//!   code point (or byte) and level (`tools/gen_collations.py`).
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
//!
//! **More than one level.** Strings compare by their level-1 weights; if
//! those are equal, by the next level's, and so on. The key is the level-1
//! key as above, then each further level's weights, `00`-escaped and
//! terminated. On the further levels nothing weighs less than the space, so
//! padding is the same as dropping trailing space weights, which is what the
//! key does (and what MariaDB's NO PAD collations do there too). MySQL's
//! `0900` collations compare whole weight strings, nothing dropped.

mod tables;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Read;
use std::sync::OnceLock;

use super::memcmp;
use tables::TableId;

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

/// Which server's meaning of a collation name is wanted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Flavor {
    /// MariaDB 11.8.
    MariaDb,
    /// MySQL 8.4.
    MySql,
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
    /// `utf8mb4_uca1400_nopad_ai_ci`, which MariaDB also calls
    /// `utf8mb4_0900_ai_ci`.
    Utf8mb4Uca1400NopadAiCi,
    /// `utf8mb4_uca1400_ai_cs`: levels 1 and 3.
    Utf8mb4Uca1400AiCs,
    /// `utf8mb4_uca1400_as_ci`: levels 1 and 2.
    Utf8mb4Uca1400AsCi,
    /// `utf8mb4_uca1400_as_cs`: levels 1 to 3.
    Utf8mb4Uca1400AsCs,
    /// `utf8mb4_uca1400_nopad_ai_cs`.
    Utf8mb4Uca1400NopadAiCs,
    /// `utf8mb4_uca1400_nopad_as_ci`, which MariaDB also calls
    /// `utf8mb4_0900_as_ci`.
    Utf8mb4Uca1400NopadAsCi,
    /// `utf8mb4_uca1400_nopad_as_cs`, which MariaDB also calls
    /// `utf8mb4_0900_as_cs`.
    Utf8mb4Uca1400NopadAsCs,
    /// `utf8mb4_unicode_ci`: UCA 4.0.0, level 1.
    Utf8mb4UnicodeCi,
    /// `utf8mb4_unicode_nopad_ci` (MariaDB only).
    Utf8mb4UnicodeNopadCi,
    /// `utf8mb4_unicode_520_ci`: UCA 5.2.0, level 1.
    Utf8mb4Unicode520Ci,
    /// `utf8mb4_unicode_520_nopad_ci` (MariaDB only).
    Utf8mb4Unicode520NopadCi,
    /// MySQL 8.4's `utf8mb4_0900_ai_ci`, its default: UCA 9.0.0, level 1,
    /// NO PAD.
    Utf8mb4Mysql0900AiCi,
    /// MySQL 8.4's `utf8mb4_0900_as_ci`: levels 1 and 2.
    Utf8mb4Mysql0900AsCi,
    /// MySQL 8.4's `utf8mb4_0900_as_cs`: levels 1 to 3.
    Utf8mb4Mysql0900AsCs,
    /// `latin1_swedish_ci`, the default of `latin1`.
    Latin1SwedishCi,
    /// `latin1_swedish_nopad_ci` (MariaDB only).
    Latin1SwedishNopadCi,
    /// `latin1_general_ci`.
    Latin1GeneralCi,
    /// `latin1_general_cs`.
    Latin1GeneralCs,
    /// `latin1_bin`: bytes, PAD SPACE.
    Latin1Bin,
    /// `latin1_nopad_bin` (MariaDB only).
    Latin1NopadBin,
    /// `ascii_general_ci`.
    AsciiGeneralCi,
    /// `ascii_general_nopad_ci` (MariaDB only).
    AsciiGeneralNopadCi,
    /// `ascii_bin`: bytes, PAD SPACE.
    AsciiBin,
    /// `ascii_nopad_bin` (MariaDB only).
    AsciiNopadBin,
}

/// How a column's bytes split into characters: what length checks and
/// index prefixes count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Charset {
    /// Bytes without a character set: one byte is one unit.
    Binary,
    /// 7-bit ASCII, one byte per character.
    Ascii,
    /// `latin1` (Windows-1252 in both servers), one byte per character.
    Latin1,
    /// UTF-8. `utf8mb3` columns are this too: their strings are UTF-8
    /// limited to the BMP, a rule the SQL layer keeps.
    Utf8mb4,
}

impl Charset {
    /// The number of characters in `s`.
    #[must_use]
    pub fn char_count(self, s: &[u8]) -> usize {
        match self {
            Charset::Utf8mb4 => s.iter().filter(|&&b| b & 0xC0 != 0x80).count(),
            _ => s.len(),
        }
    }

    /// The first `chars` characters of `s` (all of it if it has fewer).
    #[must_use]
    pub fn char_prefix(self, s: &[u8], chars: usize) -> &[u8] {
        match self {
            Charset::Utf8mb4 => {
                let end = s
                    .iter()
                    .enumerate()
                    .filter(|&(_, &b)| b & 0xC0 != 0x80)
                    .nth(chars)
                    .map_or(s.len(), |(i, _)| i);
                &s[..end]
            }
            _ => &s[..chars.min(s.len())],
        }
    }
}

#[derive(Clone, Copy)]
enum Source {
    /// `binary`: the bytes are the key.
    Bytes,
    /// A single-byte `_bin` collation: each byte weighs itself.
    ByteIdentity,
    /// A single-byte collation with one weight per byte.
    ByteTable(TableId),
    CodePoint,
    General,
    /// The DUCET 14.0.0 primaries the first two `uca1400` collations use.
    Uca,
    /// A probed table and the physical levels (from 1) the collation uses.
    Table(TableId, &'static [u8]),
}

const L1: &[u8] = &[1];
const L12: &[u8] = &[1, 2];
const L13: &[u8] = &[1, 3];
const L123: &[u8] = &[1, 2, 3];

impl Collation {
    /// Every collation the engine has.
    #[must_use]
    pub fn all() -> &'static [Collation] {
        &[
            Self::Binary,
            Self::Utf8mb4Bin,
            Self::Utf8mb4NopadBin,
            Self::Utf8mb4GeneralCi,
            Self::Utf8mb4GeneralNopadCi,
            Self::Utf8mb4Uca1400AiCi,
            Self::Utf8mb4Uca1400NopadAiCi,
            Self::Utf8mb4Uca1400AiCs,
            Self::Utf8mb4Uca1400AsCi,
            Self::Utf8mb4Uca1400AsCs,
            Self::Utf8mb4Uca1400NopadAiCs,
            Self::Utf8mb4Uca1400NopadAsCi,
            Self::Utf8mb4Uca1400NopadAsCs,
            Self::Utf8mb4UnicodeCi,
            Self::Utf8mb4UnicodeNopadCi,
            Self::Utf8mb4Unicode520Ci,
            Self::Utf8mb4Unicode520NopadCi,
            Self::Utf8mb4Mysql0900AiCi,
            Self::Utf8mb4Mysql0900AsCi,
            Self::Utf8mb4Mysql0900AsCs,
            Self::Latin1SwedishCi,
            Self::Latin1SwedishNopadCi,
            Self::Latin1GeneralCi,
            Self::Latin1GeneralCs,
            Self::Latin1Bin,
            Self::Latin1NopadBin,
            Self::AsciiGeneralCi,
            Self::AsciiGeneralNopadCi,
            Self::AsciiBin,
            Self::AsciiNopadBin,
        ]
    }

    /// The collation of a name among the first seven the engine had
    /// (`binary`, `utf8mb4_bin`, `utf8mb4_general_ci`,
    /// `utf8mb4_uca1400_ai_ci`, their nopad twins and two MySQL aliases).
    /// Tables created through this function keep their keys, so the set it
    /// answers never changes; [`Collation::resolve`] knows every collation.
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

    /// The collation a server of `flavor` means by `name`, if the engine
    /// has it. `utf8mb3_x` and `utf8_x` give the `utf8mb4_x` collation, which
    /// weighs the same strings the same way.
    ///
    /// On MariaDB the `utf8mb4_0900_*` names are aliases of
    /// `utf8mb4_uca1400_nopad_*`; on MySQL they are the UCA 9.0.0
    /// collations, and the names only MariaDB has (`uca1400`, `nopad`) are
    /// unknown.
    #[must_use]
    pub fn resolve(name: &str, flavor: Flavor) -> Option<Self> {
        let lower = name.to_ascii_lowercase();
        let mb3 = lower
            .strip_prefix("utf8mb3_")
            .or_else(|| lower.strip_prefix("utf8_"));
        if mb3.is_some_and(|rest| rest.starts_with("0900")) {
            return None;
        }
        let lower = mb3.map_or(lower.clone(), |rest| format!("utf8mb4_{rest}"));
        let mysql = flavor == Flavor::MySql;
        Some(match lower.as_str() {
            "utf8mb4_0900_bin" => Self::Utf8mb4NopadBin,
            "utf8mb4_0900_ai_ci" if mysql => Self::Utf8mb4Mysql0900AiCi,
            "utf8mb4_0900_as_ci" if mysql => Self::Utf8mb4Mysql0900AsCi,
            "utf8mb4_0900_as_cs" if mysql => Self::Utf8mb4Mysql0900AsCs,
            "utf8mb4_0900_ai_ci" => Self::Utf8mb4Uca1400NopadAiCi,
            "utf8mb4_0900_as_ci" => Self::Utf8mb4Uca1400NopadAsCi,
            "utf8mb4_0900_as_cs" => Self::Utf8mb4Uca1400NopadAsCs,
            n if mysql && (n.contains("uca1400") || n.contains("nopad")) => return None,
            n => *Self::all()
                .iter()
                .find(|c| c.name() == n && !c.is_mysql_only())?,
        })
    }

    fn is_mysql_only(self) -> bool {
        matches!(self.source(), Source::Table(TableId::Mysql0900, _))
    }

    /// The collation's name: MariaDB's, or MySQL's for the three collations
    /// only MySQL has.
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
            Self::Utf8mb4Uca1400AiCs => "utf8mb4_uca1400_ai_cs",
            Self::Utf8mb4Uca1400AsCi => "utf8mb4_uca1400_as_ci",
            Self::Utf8mb4Uca1400AsCs => "utf8mb4_uca1400_as_cs",
            Self::Utf8mb4Uca1400NopadAiCs => "utf8mb4_uca1400_nopad_ai_cs",
            Self::Utf8mb4Uca1400NopadAsCi => "utf8mb4_uca1400_nopad_as_ci",
            Self::Utf8mb4Uca1400NopadAsCs => "utf8mb4_uca1400_nopad_as_cs",
            Self::Utf8mb4UnicodeCi => "utf8mb4_unicode_ci",
            Self::Utf8mb4UnicodeNopadCi => "utf8mb4_unicode_nopad_ci",
            Self::Utf8mb4Unicode520Ci => "utf8mb4_unicode_520_ci",
            Self::Utf8mb4Unicode520NopadCi => "utf8mb4_unicode_520_nopad_ci",
            Self::Utf8mb4Mysql0900AiCi => "utf8mb4_0900_ai_ci",
            Self::Utf8mb4Mysql0900AsCi => "utf8mb4_0900_as_ci",
            Self::Utf8mb4Mysql0900AsCs => "utf8mb4_0900_as_cs",
            Self::Latin1SwedishCi => "latin1_swedish_ci",
            Self::Latin1SwedishNopadCi => "latin1_swedish_nopad_ci",
            Self::Latin1GeneralCi => "latin1_general_ci",
            Self::Latin1GeneralCs => "latin1_general_cs",
            Self::Latin1Bin => "latin1_bin",
            Self::Latin1NopadBin => "latin1_nopad_bin",
            Self::AsciiGeneralCi => "ascii_general_ci",
            Self::AsciiGeneralNopadCi => "ascii_general_nopad_ci",
            Self::AsciiBin => "ascii_bin",
            Self::AsciiNopadBin => "ascii_nopad_bin",
        }
    }

    /// The server whose collation this is, when only one has it: MySQL for
    /// the UCA 9.0.0 collations, MariaDB for the `uca1400` and `nopad` ones.
    /// `None` for a collation both servers have under the same name.
    #[must_use]
    pub fn only_on(self) -> Option<Flavor> {
        if self.is_mysql_only() {
            Some(Flavor::MySql)
        } else if self.name().contains("uca1400") || self.name().contains("nopad") {
            Some(Flavor::MariaDb)
        } else {
            None
        }
    }

    /// The pad attribute.
    #[must_use]
    pub fn pad(self) -> Pad {
        match self {
            Self::Utf8mb4Bin
            | Self::Utf8mb4GeneralCi
            | Self::Utf8mb4Uca1400AiCi
            | Self::Utf8mb4Uca1400AiCs
            | Self::Utf8mb4Uca1400AsCi
            | Self::Utf8mb4Uca1400AsCs
            | Self::Utf8mb4UnicodeCi
            | Self::Utf8mb4Unicode520Ci
            | Self::Latin1SwedishCi
            | Self::Latin1GeneralCi
            | Self::Latin1GeneralCs
            | Self::Latin1Bin
            | Self::AsciiGeneralCi
            | Self::AsciiBin => Pad::Space,
            _ => Pad::NoPad,
        }
    }

    /// The character set of the strings the collation compares.
    #[must_use]
    pub fn charset(self) -> Charset {
        match self {
            Self::Binary => Charset::Binary,
            Self::Latin1SwedishCi
            | Self::Latin1SwedishNopadCi
            | Self::Latin1GeneralCi
            | Self::Latin1GeneralCs
            | Self::Latin1Bin
            | Self::Latin1NopadBin => Charset::Latin1,
            Self::AsciiGeneralCi
            | Self::AsciiGeneralNopadCi
            | Self::AsciiBin
            | Self::AsciiNopadBin => Charset::Ascii,
            _ => Charset::Utf8mb4,
        }
    }

    fn source(self) -> Source {
        use TableId as T;
        match self {
            Self::Binary => Source::Bytes,
            Self::Utf8mb4Bin | Self::Utf8mb4NopadBin => Source::CodePoint,
            Self::Utf8mb4GeneralCi | Self::Utf8mb4GeneralNopadCi => Source::General,
            Self::Utf8mb4Uca1400AiCi | Self::Utf8mb4Uca1400NopadAiCi => Source::Uca,
            Self::Utf8mb4Uca1400AiCs | Self::Utf8mb4Uca1400NopadAiCs => {
                Source::Table(T::Uca1400, L13)
            }
            Self::Utf8mb4Uca1400AsCi | Self::Utf8mb4Uca1400NopadAsCi => {
                Source::Table(T::Uca1400, L12)
            }
            Self::Utf8mb4Uca1400AsCs | Self::Utf8mb4Uca1400NopadAsCs => {
                Source::Table(T::Uca1400, L123)
            }
            Self::Utf8mb4UnicodeCi | Self::Utf8mb4UnicodeNopadCi => Source::Table(T::Unicode, L1),
            Self::Utf8mb4Unicode520Ci | Self::Utf8mb4Unicode520NopadCi => {
                Source::Table(T::Unicode520, L1)
            }
            Self::Utf8mb4Mysql0900AiCi => Source::Table(T::Mysql0900, L1),
            Self::Utf8mb4Mysql0900AsCi => Source::Table(T::Mysql0900, L12),
            Self::Utf8mb4Mysql0900AsCs => Source::Table(T::Mysql0900, L123),
            Self::Latin1SwedishCi | Self::Latin1SwedishNopadCi => {
                Source::ByteTable(T::Latin1Swedish)
            }
            Self::Latin1GeneralCi => Source::ByteTable(T::Latin1GeneralCi),
            Self::Latin1GeneralCs => Source::ByteTable(T::Latin1GeneralCs),
            Self::AsciiGeneralCi | Self::AsciiGeneralNopadCi => Source::ByteTable(T::AsciiGeneral),
            Self::Latin1Bin | Self::Latin1NopadBin | Self::AsciiBin | Self::AsciiNopadBin => {
                Source::ByteIdentity
            }
        }
    }

    /// How many levels the collation compares: 1 to 3.
    #[must_use]
    pub fn levels(self) -> usize {
        match self.source() {
            Source::Table(_, levels) => levels.len(),
            _ => 1,
        }
    }

    /// The space's weight at level index `level` (from 0).
    fn space(self, level: usize) -> u32 {
        match self.source() {
            Source::Bytes | Source::ByteIdentity | Source::CodePoint => BIN_SPACE,
            Source::ByteTable(t) => u32::from(tables::byte_table(t)[0x20]),
            Source::General => GENERAL_SPACE,
            Source::Uca => UCA_SPACE,
            Source::Table(t, levels) => tables::table(t).space(usize::from(levels[level]) - 1),
        }
    }

    /// Whether trailing space weights are dropped on the levels after the
    /// first: everywhere but MySQL's `0900` collations.
    fn trims_later_levels(self) -> bool {
        !self.is_mysql_only()
    }

    /// The level-1 weights of `s` (UTF-8; invalid sequences weigh as U+FFFD
    /// would), without any padding rule applied. For `binary` and the
    /// single-byte `_bin` collations, the bytes.
    pub fn weights(self, s: &[u8], out: &mut Vec<u32>) {
        self.level_weights(s, 0, out);
    }

    /// The weights of `s` at level index `level` (from 0), without any
    /// padding rule applied. A level the collation doesn't have (at or past
    /// [`Collation::levels`]) has no weights.
    pub fn level_weights(self, s: &[u8], level: usize, out: &mut Vec<u32>) {
        if level >= self.levels() {
            return;
        }
        match self.source() {
            Source::Bytes | Source::ByteIdentity => out.extend(s.iter().map(|&b| u32::from(b))),
            Source::ByteTable(t) => {
                let table = tables::byte_table(t);
                out.extend(s.iter().map(|&b| u32::from(table[usize::from(b)])));
            }
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
            Source::Table(t, levels) => {
                let mut cps = Vec::with_capacity(s.len());
                for_each_char(s, |c| cps.push(c));
                tables::table(t).level_weights(&cps, usize::from(levels[level]) - 1, out);
            }
        }
    }

    fn put_weight(self) -> fn(&mut Vec<u8>, u32) {
        match self.source() {
            Source::CodePoint => put_utf8,
            Source::Bytes | Source::ByteIdentity | Source::ByteTable(_) => put_u8,
            Source::General | Source::Uca | Source::Table(..) => put_u16,
        }
    }

    /// A later level's weights with the trailing space weights dropped,
    /// where the collation drops them.
    fn later_level(self, s: &[u8], level: usize) -> Vec<u32> {
        let mut w = Vec::with_capacity(s.len());
        self.level_weights(s, level, &mut w);
        if self.trims_later_levels() {
            let space = self.space(level);
            while w.last() == Some(&space) {
                w.pop();
            }
        }
        w
    }

    /// Appends the memcomparable key of `s`.
    pub fn encode_key(self, s: &[u8], out: &mut Vec<u8>) {
        if let Source::Bytes = self.source() {
            memcmp::put_bytes(out, s);
            return;
        }
        let mut w = Vec::with_capacity(s.len());
        self.weights(s, &mut w);
        let put = self.put_weight();
        match self.pad() {
            Pad::NoPad => {
                let mut raw = Vec::with_capacity(w.len() * 2);
                for &x in &w {
                    put(&mut raw, x);
                }
                memcmp::put_bytes(out, &raw);
            }
            Pad::Space => encode_padded(&w, self.space(0), put, out),
        }
        for level in 1..self.levels() {
            let w = self.later_level(s, level);
            let mut raw = Vec::with_capacity(w.len() * 2);
            for &x in &w {
                put_u16(&mut raw, x);
            }
            memcmp::put_bytes(out, &raw);
        }
    }

    /// Skips one key written by [`Collation::encode_key`] and returns what
    /// follows it. Weights are one-way, so a string key can be stepped over
    /// but not turned back into its string.
    ///
    /// # Errors
    /// The input isn't a key of this collation.
    pub fn skip_key(self, input: &[u8]) -> sprig_common::Result<&[u8]> {
        let mut rest = self.skip_first_level(input)?;
        for _ in 1..self.levels() {
            rest = memcmp::skip_bytes(rest)?;
        }
        Ok(rest)
    }

    fn skip_first_level(self, input: &[u8]) -> sprig_common::Result<&[u8]> {
        if matches!(self.source(), Source::Bytes) || self.pad() == Pad::NoPad {
            return memcmp::skip_bytes(input);
        }
        let malformed = || sprig_common::SprigError::internal("collation key is malformed");
        let mut i = 0;
        loop {
            let marker = *input.get(i).ok_or_else(malformed)?;
            i += 1;
            match marker {
                0x40 => return Ok(&input[i..]),
                0x3F | 0xC0 => i += 4,
                0x01..=0x3E | 0xC1..=0xFF => {}
                _ => return Err(malformed()),
            }
            i += match self.source() {
                Source::CodePoint => match *input.get(i).ok_or_else(malformed)? {
                    0x00..=0x7F => 1,
                    0xC0..=0xDF => 2,
                    0xE0..=0xEF => 3,
                    0xF0..=0xF7 => 4,
                    _ => return Err(malformed()),
                },
                Source::Bytes | Source::ByteIdentity | Source::ByteTable(_) => 1,
                Source::General | Source::Uca | Source::Table(..) => 2,
            };
        }
    }

    /// Compares two strings under the collation (the reference the key
    /// encoding is tested against).
    #[must_use]
    pub fn compare(self, left: &[u8], right: &[u8]) -> Ordering {
        let (mut wl, mut wr) = (Vec::new(), Vec::new());
        self.weights(left, &mut wl);
        self.weights(right, &mut wr);
        let first = match self.pad() {
            Pad::NoPad => wl.cmp(&wr),
            Pad::Space => {
                let space = self.space(0);
                let len = wl.len().max(wr.len());
                (0..len)
                    .map(|pos| {
                        let l = wl.get(pos).copied().unwrap_or(space);
                        let r = wr.get(pos).copied().unwrap_or(space);
                        l.cmp(&r)
                    })
                    .find(|o| o.is_ne())
                    .unwrap_or(Ordering::Equal)
            }
        };
        first.then_with(|| {
            (1..self.levels())
                .map(|level| {
                    self.later_level(left, level)
                        .cmp(&self.later_level(right, level))
                })
                .find(|o| o.is_ne())
                .unwrap_or(Ordering::Equal)
        })
    }

    /// The bytes `WEIGHT_STRING(s)` returns on a server of `flavor`: every
    /// level's weights, level 1 first. Weights are two bytes big-endian, one
    /// byte for the single-byte sets, and three bytes (the code point) for
    /// `utf8mb4_bin`; MySQL's `utf8mb4_0900_bin` gives the UTF-8 bytes, and
    /// its multi-level `0900` collations put a `0000` between levels where
    /// MariaDB puts nothing.
    #[must_use]
    pub fn weight_string(self, s: &[u8], flavor: Flavor) -> Vec<u8> {
        let mut out = Vec::with_capacity(s.len() * 2);
        let mut w = Vec::with_capacity(s.len());
        for level in 0..self.levels() {
            if level > 0 && self.is_mysql_only() {
                out.extend_from_slice(&[0, 0]);
            }
            w.clear();
            self.level_weights(s, level, &mut w);
            for &x in &w {
                match self.source() {
                    Source::CodePoint if flavor == Flavor::MySql && self.pad() == Pad::NoPad => {
                        put_utf8(&mut out, x);
                    }
                    Source::CodePoint => out.extend_from_slice(&x.to_be_bytes()[1..]),
                    Source::Bytes | Source::ByteIdentity | Source::ByteTable(_) => {
                        put_u8(&mut out, x);
                    }
                    Source::General | Source::Uca | Source::Table(..) => put_u16(&mut out, x),
                }
            }
        }
        out
    }
}

fn put_u8(out: &mut Vec<u8>, w: u32) {
    // Byte weights.
    #[allow(clippy::cast_possible_truncation)]
    out.push(w as u8);
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
            // The first seven here; all of them in `tests/types_collations.rs`.
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
            for c in ALL.into_iter().filter(|c| c.pad() == Pad::Space) {
                prop_assert_eq!(c.compare(a.as_bytes(), b.as_bytes()), key(c, &a).cmp(&key(c, &b)), "{}", c.name());
            }
        }
    }

    fn ws(c: Collation, s: &[u8], flavor: Flavor) -> String {
        c.weight_string(s, flavor)
            .iter()
            .fold(String::new(), |mut out, b| {
                use std::fmt::Write as _;
                let _ = write!(out, "{b:02X}");
                out
            })
    }

    /// `HEX(WEIGHT_STRING(...))` as MariaDB 11.8 and MySQL 8.4 answer it.
    #[test]
    fn oracle_weight_strings() {
        let s = "aB \u{e9} ".as_bytes();
        let maria = Flavor::MariaDb;
        assert_eq!(
            ws(Collation::Utf8mb4Uca1400AsCs, s, maria),
            "2075208F020920DB0209002000200020002000240020000200080002000200020002"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Uca1400AiCs, s, maria),
            "2075208F020920DB0209000200080002000200020002"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Uca1400NopadAsCi, s, maria),
            "2075208F020920DB0209002000200020002000240020"
        );
        // A contraction with weights on every level.
        assert_eq!(
            ws(Collation::Utf8mb4Uca1400AsCs, "L\u{b7}".as_bytes(), maria),
            "21B00020011800080002"
        );
        let t = "a\u{df}\u{1F600}".as_bytes();
        assert_eq!(
            ws(Collation::Utf8mb4UnicodeCi, t, maria),
            "0E330FEA0FEAFFFD"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Unicode520Ci, t, maria),
            "120F14101410FBC3F600"
        );
        assert_eq!(ws(Collation::Utf8mb4Bin, t, maria), "0000610000DF01F600");
        let l = b"\x61\xDF\x20\xC9\xFF";
        assert_eq!(ws(Collation::Latin1SwedishCi, l, maria), "41DF2045FF");
        assert_eq!(ws(Collation::Latin1GeneralCi, l, maria), "4197205FAE");
        assert_eq!(ws(Collation::Latin1GeneralCs, l, maria), "4297205FAE");
        assert_eq!(ws(Collation::Latin1Bin, l, maria), "61DF20C9FF");
        assert_eq!(ws(Collation::AsciiGeneralCi, b"aZ~ ", maria), "415A7E20");

        let mysql = Flavor::MySql;
        assert_eq!(
            ws(Collation::Utf8mb4Mysql0900AsCs, s, mysql),
            "1C471C6002091CAA020900000020002000200020002400200000000200080002000200020002"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Mysql0900AsCi, s, mysql),
            "1C471C6002091CAA02090000002000200020002000240020"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Mysql0900AiCi, s, mysql),
            "1C471C6002091CAA0209"
        );
        assert_eq!(ws(Collation::Utf8mb4Mysql0900AsCs, b"", mysql), "00000000");
        let u = "a\u{df}\u{1F600} ".as_bytes();
        assert_eq!(
            ws(Collation::Utf8mb4UnicodeCi, u, mysql),
            "0E330FEA0FEAFFFD0209"
        );
        assert_eq!(
            ws(Collation::Utf8mb4GeneralCi, u, mysql),
            "00410053FFFD0020"
        );
        assert_eq!(
            ws(Collation::Utf8mb4Bin, u, mysql),
            "0000610000DF01F600000020"
        );
        assert_eq!(ws(Collation::Utf8mb4NopadBin, u, mysql), "61C39FF09F988020");
    }

    /// Comparisons both oracles were asked.
    #[test]
    fn oracle_comparisons_on_several_levels() {
        let lt = |c: Collation, a: &str, b: &str| {
            assert_eq!(
                c.compare(a.as_bytes(), b.as_bytes()),
                Ordering::Less,
                "{} {a:?} {b:?}",
                c.name()
            );
            assert!(key(c, a) < key(c, b), "{} {a:?} {b:?}", c.name());
        };
        let eq = |c: Collation, a: &str, b: &str| {
            assert_eq!(
                c.compare(a.as_bytes(), b.as_bytes()),
                Ordering::Equal,
                "{} {a:?} {b:?}",
                c.name()
            );
            assert_eq!(key(c, a), key(c, b), "{} {a:?} {b:?}", c.name());
        };
        let m = Collation::Utf8mb4Uca1400AsCs;
        eq(m, "a", "a ");
        lt(m, "a", "A");
        lt(m, "a ", "A");
        lt(m, "a\t", "a");
        // MariaDB's NO PAD collations still ignore trailing minimal weights
        // on the later levels.
        eq(Collation::Utf8mb4Uca1400NopadAiCs, "a", "a\u{302}");
        let y = Collation::Utf8mb4Mysql0900AsCs;
        lt(y, "a", "a ");
        lt(y, "a", "A");
        lt(y, "A", "a ");
        lt(y, "a", "a\t");
        lt(Collation::Utf8mb4Mysql0900AiCi, "a", "a\t");
        eq(Collation::Utf8mb4Mysql0900AiCi, "a", "A");
        eq(Collation::Utf8mb4Mysql0900AiCi, "\u{e9}", "E");
        lt(Collation::Utf8mb4Mysql0900AsCi, "e", "\u{e9}");
        eq(Collation::Utf8mb4Mysql0900AsCi, "\u{c9}", "\u{e9}");
    }

    /// The probed `uca1400` table and the DUCET-built one are two routes to
    /// the same level-1 weights. Here: every contraction the DUCET-built
    /// table knows. Every single code point is compared in
    /// `tests/types_collations.rs`.
    #[test]
    fn uca1400_tables_agree_on_contractions() {
        let (old, new) = (Collation::Utf8mb4Uca1400AiCi, Collation::Utf8mb4Uca1400AsCs);
        let (mut a, mut b) = (Vec::new(), Vec::new());
        let mut seen = 0;
        for (&first, list) in &uca_table().contractions {
            for (cps, _) in list {
                let s: String = cps.iter().filter_map(|&c| char::from_u32(c)).collect();
                a.clear();
                b.clear();
                old.weights(s.as_bytes(), &mut a);
                new.weights(s.as_bytes(), &mut b);
                assert_eq!(a, b, "contraction starting U+{first:04X}");
                seen += 1;
            }
        }
        assert!(seen > 900);
    }

    #[test]
    fn resolving_names() {
        use Flavor::{MariaDb, MySql};
        for &c in Collation::all() {
            match c.only_on() {
                Some(MySql) => {
                    assert_eq!(Collation::resolve(c.name(), MySql), Some(c));
                    assert_ne!(Collation::resolve(c.name(), MariaDb), Some(c));
                }
                Some(MariaDb) => {
                    assert_eq!(Collation::resolve(c.name(), MariaDb), Some(c));
                    assert_eq!(Collation::resolve(c.name(), MySql), None, "{}", c.name());
                }
                None => {
                    assert_eq!(Collation::resolve(c.name(), MariaDb), Some(c));
                    assert_eq!(Collation::resolve(c.name(), MySql), Some(c));
                }
            }
            assert!((1..=3).contains(&c.levels()));
        }
        assert_eq!(
            Collation::resolve("utf8mb4_0900_ai_ci", MariaDb),
            Some(Collation::Utf8mb4Uca1400NopadAiCi)
        );
        assert_eq!(
            Collation::resolve("UTF8MB4_0900_AS_CS", MySql),
            Some(Collation::Utf8mb4Mysql0900AsCs)
        );
        assert_eq!(
            Collation::resolve("utf8mb4_0900_bin", MySql),
            Some(Collation::Utf8mb4NopadBin)
        );
        assert_eq!(
            Collation::resolve("utf8mb3_general_ci", MySql),
            Some(Collation::Utf8mb4GeneralCi)
        );
        assert_eq!(
            Collation::resolve("utf8_unicode_ci", MariaDb),
            Some(Collation::Utf8mb4UnicodeCi)
        );
        assert_eq!(
            Collation::resolve("utf8mb3_uca1400_as_cs", MariaDb),
            Some(Collation::Utf8mb4Uca1400AsCs)
        );
        assert_eq!(Collation::resolve("utf8mb3_0900_ai_ci", MySql), None);
        assert_eq!(Collation::resolve("utf8mb4_swedish_ci", MariaDb), None);
        assert_eq!(Collation::all().len(), 30);
        let mut none = Vec::new();
        Collation::Utf8mb4Uca1400AsCi.level_weights(b"a", 2, &mut none);
        Collation::Utf8mb4Bin.level_weights(b"a", 1, &mut none);
        assert!(none.is_empty());
        assert_eq!(Collation::Latin1SwedishCi.charset(), Charset::Latin1);
        assert_eq!(Collation::AsciiNopadBin.charset(), Charset::Ascii);
        assert_eq!(Collation::Utf8mb4Mysql0900AiCi.pad(), Pad::NoPad);
    }

    /// The keys of the first seven collations are on disk in tables the
    /// first SQL backend created: these bytes must never change.
    #[test]
    fn first_collations_keep_their_bytes() {
        let k = |c: Collation, s: &str| key(c, s);
        assert_eq!(k(Collation::Binary, "a\0b"), [0x61, 0, 0xFF, 0x62, 0, 0]);
        assert_eq!(
            k(Collation::Utf8mb4Bin, "a b "),
            [0xFF, 0x61, 0xFE, 0x62, 0x40]
        );
        assert_eq!(
            k(Collation::Utf8mb4Bin, "a\t"),
            [0xFF, 0x61, 0x01, 0x09, 0x40]
        );
        assert_eq!(k(Collation::Utf8mb4Bin, ""), [0x40]);
        assert_eq!(
            k(Collation::Utf8mb4NopadBin, "\u{e9} "),
            [0xC3, 0xA9, 0x20, 0, 0]
        );
        assert_eq!(
            k(Collation::Utf8mb4GeneralCi, "a\u{e9}"),
            [0xFF, 0x00, 0x41, 0xFF, 0x00, 0x45, 0x40]
        );
        assert_eq!(
            k(Collation::Utf8mb4GeneralNopadCi, "a"),
            [0x00, 0xFF, 0x41, 0, 0]
        );
        assert_eq!(
            k(Collation::Utf8mb4Uca1400AiCi, "a "),
            [0xFF, 0x20, 0x75, 0x40]
        );
        assert_eq!(
            k(Collation::Utf8mb4Uca1400AiCi, "a  b"),
            [0xFF, 0x20, 0x75, 0xFD, 0x20, 0x8F, 0x40]
        );
        assert_eq!(
            k(Collation::Utf8mb4Uca1400NopadAiCi, "a "),
            [0x20, 0x75, 0x02, 0x09, 0, 0]
        );
        for name in [
            "binary",
            "utf8mb4_bin",
            "utf8mb4_nopad_bin",
            "utf8mb4_0900_bin",
            "utf8mb4_general_ci",
            "utf8mb4_general_nopad_ci",
            "utf8mb4_uca1400_ai_ci",
            "utf8mb4_uca1400_nopad_ai_ci",
            "utf8mb4_0900_ai_ci",
        ] {
            assert!(Collation::from_name(name).is_some(), "{name}");
        }
        // The names added later are not this function's to answer.
        for name in [
            "utf8mb4_unicode_ci",
            "utf8mb4_uca1400_as_cs",
            "latin1_swedish_ci",
        ] {
            assert_eq!(Collation::from_name(name), None, "{name}");
        }
        assert_eq!(
            Collation::from_name("utf8mb4_0900_ai_ci"),
            Some(Collation::Utf8mb4Uca1400NopadAiCi)
        );
    }

    /// Keys of the collations with several levels, written out.
    #[test]
    fn several_levels_in_a_key() {
        // Level 1 tokens, the end, then level 3 with its trailing 0002
        // dropped: 'a' and 'a ' are one key.
        assert_eq!(
            key(Collation::Utf8mb4Uca1400AiCs, "a"),
            [0xFF, 0x20, 0x75, 0x40, 0, 0]
        );
        assert_eq!(
            key(Collation::Utf8mb4Uca1400AiCs, "A "),
            [0xFF, 0x20, 0x75, 0x40, 0x00, 0xFF, 0x08, 0, 0]
        );
        // MySQL keeps every weight: level 1, level 2 (0020), level 3 (0002).
        assert_eq!(
            key(Collation::Utf8mb4Mysql0900AsCs, "a"),
            [
                0x1C, 0x47, 0, 0, 0x00, 0xFF, 0x20, 0, 0, 0x00, 0xFF, 0x02, 0, 0
            ]
        );
        assert_eq!(
            key(Collation::Latin1SwedishCi, "a\t "),
            [0xFF, 0x41, 0x01, 0x09, 0x40]
        );
        assert_eq!(key(Collation::AsciiNopadBin, "a "), [0x61, 0x20, 0, 0]);
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
