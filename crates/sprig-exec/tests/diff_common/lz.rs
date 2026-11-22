//! The compressor of the generated golden files, so that tens of
//! thousands of recorded answers stay a few megabytes: the LZ4 frame
//! format, through `lz4_flex`.

use std::io::{Read, Write};

/// The LZ4 frame format's magic number, little-endian.
const MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

/// Compresses `data`.
pub fn compress(data: &[u8]) -> Vec<u8> {
    let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::with_capacity(data.len() / 3));
    enc.write_all(data).expect("compressing into memory");
    enc.finish().expect("compressing into memory")
}

/// Restores what [`compress`] wrote.
pub fn decompress(data: &[u8]) -> Vec<u8> {
    assert!(
        data.starts_with(&MAGIC) || data.is_empty(),
        "a golden file that is not in the LZ4 frame format"
    );
    let mut out = Vec::with_capacity(data.len() * 4);
    lz4_flex::frame::FrameDecoder::new(data)
        .read_to_end(&mut out)
        .expect("a golden file in the LZ4 frame format");
    out
}
