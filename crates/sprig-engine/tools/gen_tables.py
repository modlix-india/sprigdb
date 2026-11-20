#!/usr/bin/env python3
"""Generates sprig-engine's collation weight tables (data/*.bin.z).

Two inputs, neither of them MariaDB code:
- the public DUCET of UCA 14.0.0 (https://www.unicode.org/Public/UCA/14.0.0/allkeys.txt,
  Unicode licence), for utf8mb4_uca1400_ai_ci's primary (level 1) weights and its
  contractions. Code points absent from the DUCET get UCA implicit weights,
  computed at run time in Rust (keys/collation.rs).
- utf8mb4_general_ci's per-character weights for the BMP, probed from a MariaDB
  11.8 server used as a black box, one WEIGHT_STRING per code point:
    SELECT seq, HEX(WEIGHT_STRING(CONVERT(CHAR(seq USING utf32) USING utf8mb4)
           COLLATE utf8mb4_general_ci)) FROM seq_0_to_65535
  (saved as a two-column TSV). Characters outside the BMP all weigh U+FFFD.

Usage: gen_tables.py --allkeys allkeys-14.0.0.txt --general general_ci.tsv --out ../data

Formats (little-endian, then zlib):
  uca1400_l1.bin: b"SPU1", u32 count, count x (u32 cp, u8 n, n x u16 primary),
                  u32 count, count x (u8 ncp, ncp x u32 cp, u8 n, n x u16 primary)
                  (single code points, then contractions). n = 0 means ignorable at level 1.
  general_ci.bin: b"SPG1", 65536 x u16.
"""
import argparse, os, re, struct, zlib

def parse_allkeys(path):
    singles, contractions = {}, {}
    pat = re.compile(r"\[[.*]([0-9A-F]{4})\.[0-9A-F]{4}\.[0-9A-F]{4}\]")
    for line in open(path, encoding="utf-8"):
        line = line.split("#", 1)[0].strip()
        if not line or line.startswith("@"):
            continue
        lhs, rhs = line.split(";", 1)
        cps = [int(x, 16) for x in lhs.split()]
        prim = [int(p, 16) for p in pat.findall(rhs)]
        prim = [p for p in prim if p != 0]
        if len(cps) == 1:
            singles[cps[0]] = prim
        else:
            contractions[tuple(cps)] = prim
    return singles, contractions

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--allkeys", required=True)
    ap.add_argument("--general", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    singles, contractions = parse_allkeys(a.allkeys)
    b = bytearray(b"SPU1")
    b += struct.pack("<I", len(singles))
    for cp in sorted(singles):
        w = singles[cp]
        b += struct.pack("<IB", cp, len(w)) + b"".join(struct.pack("<H", x) for x in w)
    b += struct.pack("<I", len(contractions))
    for cps in sorted(contractions):
        w = contractions[cps]
        b += struct.pack("<B", len(cps)) + b"".join(struct.pack("<I", c) for c in cps)
        b += struct.pack("<B", len(w)) + b"".join(struct.pack("<H", x) for x in w)
    open(os.path.join(a.out, "uca1400_l1.bin.z"), "wb").write(zlib.compress(bytes(b), 9))
    g = [0] * 65536
    for line in open(a.general):
        cp, hx = line.split()
        g[int(cp)] = int(hx, 16)
    gb = bytearray(b"SPG1") + b"".join(struct.pack("<H", x) for x in g)
    open(os.path.join(a.out, "general_ci.bin.z"), "wb").write(zlib.compress(bytes(gb), 9))
    print(f"uca1400: {len(singles)} code points, {len(contractions)} contractions")

if __name__ == "__main__":
    main()
