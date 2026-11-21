//! A small byte-oriented compressor for the generated golden files, so
//! that tens of thousands of recorded answers stay a few megabytes. The
//! format is the test suite's own: a sequence of
//! `<literal count><literals><match length><match distance>` with counts
//! as variable-length integers; a match copies bytes already written.

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        out.push((v & 0x7F) as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(data: &[u8], pos: &mut usize) -> usize {
    let mut v = 0usize;
    let mut shift = 0;
    while let Some(&b) = data.get(*pos) {
        *pos += 1;
        v |= usize::from(b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    v
}

const MIN_MATCH: usize = 6;
const WINDOW: usize = 1 << 20;

fn hash(b: &[u8]) -> usize {
    let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
    (v.wrapping_mul(2_654_435_761) >> 14) as usize
}

/// Compresses `data`.
pub fn compress(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() / 3);
    let mut table = vec![usize::MAX; 1 << 18];
    let mut i = 0;
    let mut literal_start = 0;
    while i + MIN_MATCH <= data.len() {
        let h = hash(&data[i..]);
        let candidate = table[h];
        table[h] = i;
        if candidate != usize::MAX && i - candidate <= WINDOW {
            let mut len = 0;
            while i + len < data.len() && data[candidate + len] == data[i + len] {
                len += 1;
            }
            if len >= MIN_MATCH {
                put_varint(&mut out, i - literal_start);
                out.extend_from_slice(&data[literal_start..i]);
                put_varint(&mut out, len);
                put_varint(&mut out, i - candidate);
                // Index a few positions inside the match.
                let end = i + len;
                let mut j = i + 1;
                while j + MIN_MATCH <= data.len() && j < end {
                    table[hash(&data[j..])] = j;
                    j += 3;
                }
                i = end;
                literal_start = i;
                continue;
            }
        }
        i += 1;
    }
    put_varint(&mut out, data.len() - literal_start);
    out.extend_from_slice(&data[literal_start..]);
    out
}

/// Restores what [`compress`] wrote.
pub fn decompress(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() * 4);
    let mut pos = 0;
    while pos < data.len() {
        let literals = get_varint(data, &mut pos);
        let end = (pos + literals).min(data.len());
        out.extend_from_slice(&data[pos..end]);
        pos = end;
        if pos >= data.len() {
            break;
        }
        let len = get_varint(data, &mut pos);
        let distance = get_varint(data, &mut pos);
        if distance == 0 || distance > out.len() {
            break;
        }
        let from = out.len() - distance;
        for k in 0..len {
            out.push(out[from + k]);
        }
    }
    out
}
