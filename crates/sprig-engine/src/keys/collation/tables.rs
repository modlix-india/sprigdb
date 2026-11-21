//! The multi-level weight tables (`data/collations.bin.z`), built by
//! `tools/gen_collations.py` from the answers of MariaDB 11.8 and MySQL 8.4
//! used as black boxes: one `WEIGHT_STRING` per code point and level.
//!
//! A table maps a unit (a code point, or a byte for the single-byte sets) to
//! its weights at each level, in runs: consecutive units whose weights are
//! equal, or equal but for a last weight that grows by one per unit (the
//! shape of UCA implicit weights). Contractions are kept by their first
//! unit, longest first, and match contiguously.

use std::collections::HashMap;
use std::io::Read;
use std::sync::OnceLock;

/// One table of the data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TableId {
    /// MariaDB's `uca1400` family: UCA 14.0.0, three levels, contractions.
    Uca1400 = 0,
    /// `utf8mb4_unicode_ci`: UCA 4.0.0, level 1.
    Unicode = 1,
    /// `utf8mb4_unicode_520_ci`: UCA 5.2.0, level 1.
    Unicode520 = 2,
    /// MySQL's `utf8mb4_0900_*`: UCA 9.0.0, three levels.
    Mysql0900 = 3,
    /// `latin1_swedish_ci`, per byte.
    Latin1Swedish = 4,
    /// `latin1_general_ci`, per byte.
    Latin1GeneralCi = 5,
    /// `latin1_general_cs`, per byte.
    Latin1GeneralCs = 6,
    /// `ascii_general_ci`, per byte.
    AsciiGeneral = 7,
}

const NAMES: [&str; 8] = [
    "uca1400",
    "unicode_ci",
    "unicode_520_ci",
    "mysql0900",
    "latin1_swedish_ci",
    "latin1_general_ci",
    "latin1_general_cs",
    "ascii_general_ci",
];

struct Run {
    start: u32,
    len: u32,
    delta: u8,
    n: u8,
    off: u32,
}

/// Units below this are found through a direct index; the rest by binary
/// search over the runs.
const BMP: usize = 0x1_0000;

#[derive(Default)]
struct Level {
    runs: Vec<Run>,
    pool: Vec<u16>,
    /// For each BMP unit, its run's position plus one, or 0 for none. Empty
    /// for the small single-byte tables.
    bmp: Vec<u16>,
}

impl Level {
    /// Builds the direct index of a table that covers the BMP.
    fn index_bmp(&mut self) {
        let covers_bmp = self.runs.last().is_some_and(|r| r.start as usize >= BMP);
        if !covers_bmp || self.runs.len() >= usize::from(u16::MAX) {
            return;
        }
        let mut bmp = vec![0u16; BMP];
        for (i, r) in self.runs.iter().enumerate() {
            let start = r.start as usize;
            if start >= BMP {
                break;
            }
            let end = (start + r.len as usize).min(BMP);
            // Fewer runs than `u16::MAX`, checked above.
            #[allow(clippy::cast_possible_truncation)]
            bmp[start..end].fill(i as u16 + 1);
        }
        self.bmp = bmp;
    }

    fn run_of(&self, unit: u32) -> Option<&Run> {
        if let Some(&slot) = self.bmp.get(unit as usize) {
            return usize::from(slot).checked_sub(1).map(|i| &self.runs[i]);
        }
        let i = self.runs.partition_point(|r| r.start <= unit);
        let r = &self.runs[i.checked_sub(1)?];
        (unit - r.start < r.len).then_some(r)
    }

    fn push(&self, unit: u32, out: &mut Vec<u32>) {
        let Some(r) = self.run_of(unit) else {
            return;
        };
        let off = r.off as usize;
        let ws = &self.pool[off..off + usize::from(r.n)];
        let Some((last, rest)) = ws.split_last() else {
            return;
        };
        out.extend(rest.iter().map(|&w| u32::from(w)));
        out.push(u32::from(*last) + u32::from(r.delta) * (unit - r.start));
    }
}

struct Contraction {
    units: Vec<u32>,
    weights: Vec<Vec<u16>>,
}

/// One collation family's weights.
#[derive(Default)]
pub(super) struct WeightTable {
    levels: Vec<Level>,
    /// By first unit, longest first.
    contractions: HashMap<u32, Vec<Contraction>>,
    /// One bit per BMP unit: set if a contraction starts with it. Most
    /// characters start none, and this spares them the map lookup.
    starts: Vec<u64>,
    /// The weight of U+0020 at each level: what `PAD SPACE` pads with.
    space: Vec<u32>,
}

impl WeightTable {
    /// Appends the weights of `units` at physical level `level` (from 0).
    pub(super) fn level_weights(&self, units: &[u32], level: usize, out: &mut Vec<u32>) {
        let lv = &self.levels[level];
        let mut i = 0;
        while i < units.len() {
            if self.may_start(units[i])
                && let Some(cands) = self.contractions.get(&units[i])
                && let Some(c) = cands.iter().find(|c| units[i..].starts_with(&c.units))
            {
                out.extend(c.weights[level].iter().map(|&w| u32::from(w)));
                i += c.units.len();
                continue;
            }
            lv.push(units[i], out);
            i += 1;
        }
    }

    /// Whether some contraction may start with `unit`.
    fn may_start(&self, unit: u32) -> bool {
        if self.contractions.is_empty() {
            return false;
        }
        match self.starts.get(unit as usize / 64) {
            Some(word) => word & (1 << (unit % 64)) != 0,
            None => true,
        }
    }

    /// The space's weight at physical level `level` (from 0).
    pub(super) fn space(&self, level: usize) -> u32 {
        self.space[level]
    }

    /// The one weight of each byte, for a single-byte table.
    fn byte_weights(&self) -> [u8; 256] {
        let mut t = [0u8; 256];
        let mut w = Vec::with_capacity(1);
        for (b, slot) in t.iter_mut().enumerate() {
            w.clear();
            // 0 to 255.
            #[allow(clippy::cast_possible_truncation)]
            self.levels[0].push(b as u32, &mut w);
            // Bytes the set doesn't define (above 0x7F in `ascii`) weigh
            // themselves, so every byte string still has an order.
            #[allow(clippy::cast_possible_truncation)]
            let weight = w.first().map_or(b as u8, |&x| x as u8);
            *slot = weight;
        }
        t
    }
}

static DATA: &[u8] = include_bytes!("../../../data/collations.bin.z");

struct Tables {
    tables: Vec<WeightTable>,
    bytes: Vec<[u8; 256]>,
}

fn tables() -> &'static Tables {
    static T: OnceLock<Tables> = OnceLock::new();
    T.get_or_init(|| {
        // The data is compiled in; a failure here is a build defect, caught
        // by the unit tests.
        #[allow(clippy::expect_used)]
        let tables = parse(DATA).expect("sprig-engine's collation tables are corrupt");
        let bytes = tables.iter().map(WeightTable::byte_weights).collect();
        Tables { tables, bytes }
    })
}

/// The table `id`.
pub(super) fn table(id: TableId) -> &'static WeightTable {
    &tables().tables[id as usize]
}

/// The per-byte weights of the single-byte table `id`.
pub(super) fn byte_table(id: TableId) -> &'static [u8; 256] {
    &tables().bytes[id as usize]
}

struct Reader<'a> {
    b: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.b.len() < n {
            return None;
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Some(h)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
}

fn parse(packed: &[u8]) -> Option<Vec<WeightTable>> {
    let mut raw = Vec::new();
    flate2::read::ZlibDecoder::new(packed)
        .read_to_end(&mut raw)
        .ok()?;
    let mut reader = Reader { b: &raw };
    if reader.take(4)? != b"SPE1" {
        return None;
    }
    let count = reader.u16()?;
    let mut by_name: HashMap<String, WeightTable> = HashMap::new();
    for _ in 0..count {
        let name_len = usize::from(reader.u16()?);
        let name = String::from_utf8(reader.take(name_len)?.to_vec()).ok()?;
        let levels = reader.u8()?;
        let mut table = WeightTable::default();
        for _ in 0..levels {
            let mut lv = Level::default();
            for _ in 0..reader.u32()? {
                let start = reader.u32()?;
                let len = reader.u32()?;
                let delta = reader.u8()?;
                let n = reader.u8()?;
                let off = u32::try_from(lv.pool.len()).ok()?;
                for _ in 0..n {
                    lv.pool.push(reader.u16()?);
                }
                lv.runs.push(Run {
                    start,
                    len,
                    delta,
                    n,
                    off,
                });
            }
            table.levels.push(lv);
        }
        for _ in 0..reader.u32()? {
            let unit_count = usize::from(reader.u8()?);
            let mut units = Vec::with_capacity(unit_count);
            for _ in 0..unit_count {
                units.push(reader.u32()?);
            }
            let mut weights = Vec::new();
            for _ in 0..levels {
                let n = reader.u8()?;
                let mut ws = Vec::with_capacity(usize::from(n));
                for _ in 0..n {
                    ws.push(reader.u16()?);
                }
                weights.push(ws);
            }
            table
                .contractions
                .entry(*units.first()?)
                .or_default()
                .push(Contraction { units, weights });
        }
        for v in table.contractions.values_mut() {
            v.sort_by_key(|c| std::cmp::Reverse(c.units.len()));
        }
        table.starts = vec![0u64; BMP / 64];
        for &first in table.contractions.keys() {
            if let Some(word) = table.starts.get_mut(first as usize / 64) {
                *word |= 1 << (first % 64);
            }
        }
        for level in &mut table.levels {
            level.index_bmp();
        }
        for level in 0..table.levels.len() {
            let mut w = Vec::new();
            table.levels[level].push(0x20, &mut w);
            table.space.push(*w.first()?);
        }
        by_name.insert(name, table);
    }
    NAMES.iter().map(|n| by_name.remove(*n)).collect()
}
