#!/usr/bin/env python3
"""Generates sprig-engine's multi-level collation weight tables (data/collations.bin.z).

Every weight in the file is an answer of a running server used as a black box:
one WEIGHT_STRING per code point (or per byte, for the single-byte sets), per
level, plus one per multi-character sequence the public DUCET of UCA 14.0.0
lists (https://www.unicode.org/Public/UCA/14.0.0/allkeys.txt, Unicode licence),
kept when the server weighs the sequence differently from its characters one by
one. No server source file is read.

Tables and where they are observed:
  uca1400            MariaDB 11.8  utf8mb4_uca1400_as_cs, levels 1-3 (LEVEL k)
  unicode_ci         MariaDB 11.8  utf8mb4_unicode_ci        (MySQL 8.4 answers the same; checked)
  unicode_520_ci     MariaDB 11.8  utf8mb4_unicode_520_ci    (same; checked)
  mysql0900          MySQL 8.4     utf8mb4_0900_as_cs, whose weight string is
                                   level 1 | 0000 | level 2 | 0000 | level 3
  latin1_swedish_ci, latin1_general_ci, latin1_general_cs, ascii_general_ci
                     MariaDB 11.8  one weight per byte      (same on MySQL; checked)

Also checked before writing: the other strengths are subsets of the three
levels (uca1400 ai_ci = level 1, as_ci = 1-2, ai_cs = 1 and 3; MySQL 0900
ai_ci = level 1, as_ci = levels 1-2).

Usage:
  gen_collations.py --maria '<command that runs SQL from stdin on MariaDB, tab output, no header>' \
                    --mysql '<the same for MySQL 8.4>' --allkeys allkeys-14.0.0.txt \
                    --out ../data/collations.bin.z
  The MySQL side needs a table of code points:  r_types_probe.cp (n INT PRIMARY KEY),
  0..1114111 without the surrogates; --prepare creates it.

Format after zlib inflation (little-endian):
  "SPE1", u16 table_count, then per table:
    u16 name_len, name, u8 levels,
    per level: u32 run_count, runs of
      u32 start, u32 len, u8 delta (0 or 1), u8 n, n x u16 weights
      (unit start+i weighs the run's weights, the last one plus delta*i)
    u32 contraction_count, per contraction:
      u8 unit_count, unit_count x u32, per level: u8 n, n x u16 weights
"""
import argparse, shlex, struct, subprocess, sys, zlib

SURROGATES = "NOT BETWEEN 55296 AND 57343"


def run(cmd, sql):
    p = subprocess.run(shlex.split(cmd), input=sql, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(p.stderr)
    return p.stdout


def words(h):
    b = bytes.fromhex(h)
    return [int.from_bytes(b[i:i + 2], "big") for i in range(0, len(b), 2)]


def table_of(out, width=2):
    t = {}
    for line in out.splitlines():
        cp, _, h = line.partition("\t")
        h = "" if h == "NULL" else h
        b = bytes.fromhex(h)
        t[int(cp)] = [int.from_bytes(b[i:i + width], "big") for i in range(0, len(b), width)]
    return t


def maria_chars(a, coll, level=""):
    lv = f" LEVEL {level}" if level else ""
    return table_of(run(a.maria, f"SELECT seq, HEX(WEIGHT_STRING(CONVERT(CHAR(seq USING utf32) USING utf8mb4) "
                                 f"COLLATE {coll}{lv})) FROM seq_0_to_1114111 WHERE seq {SURROGATES}"))


def mysql_chars(a, coll):
    return table_of(run(a.mysql, "SELECT n, HEX(WEIGHT_STRING(CONVERT(CONVERT(UNHEX(LPAD(HEX(n),8,'0')) USING utf32) "
                                 f"USING utf8mb4) COLLATE {coll})) FROM r_types_probe.cp ORDER BY n"))


def byte_table(cmd, coll, charset, count, numbers):
    return table_of(run(cmd, f"SELECT {numbers[0]}, HEX(WEIGHT_STRING(CONVERT(UNHEX(LPAD(HEX({numbers[0]}),2,'0')) "
                             f"USING {charset}) COLLATE {coll})) FROM {numbers[1]} WHERE {numbers[0]} < {count} "
                             f"ORDER BY {numbers[0]}"), width=1)


def split_levels(ws, levels):
    """MySQL's weight string: levels separated by a 0000 weight."""
    out, cur = [], []
    for w in ws:
        if w == 0:
            out.append(cur)
            cur = []
        else:
            cur.append(w)
    out.append(cur)
    if len(out) != levels:
        sys.exit(f"expected {levels} levels, got {out}")
    return out


def runs_of(weights):
    runs = []  # [start, len, delta, first]
    for cp in sorted(weights):
        w = weights[cp]
        if runs:
            r = runs[-1]
            start, n, delta, first = r
            if start + n == cp and len(first) == len(w) and (not w or first[:-1] == w[:-1]):
                if not w:
                    r[1] += 1
                    continue
                for d in ([delta] if delta is not None else [0, 1]):
                    if first[-1] + d * n == w[-1]:
                        r[1] += 1
                        r[2] = d
                        break
                else:
                    runs.append([cp, 1, None, w])
                continue
        runs.append([cp, 1, None, w])
    return runs


def sequences(path):
    seqs = []
    for line in open(path, encoding="utf-8"):
        if line.startswith(("#", "@")) or not line.strip():
            continue
        cps = line.split(";")[0].split()
        if len(cps) > 1:
            seqs.append([int(c, 16) for c in cps])
    return seqs


def seq_expr(seq, coll):
    h = "".join(f"{c:08X}" for c in seq)
    return f"CONVERT(CONVERT(UNHEX('{h}') USING utf32) USING utf8mb4) COLLATE {coll}"


def maria_contractions(a, seqs, singles):
    stmts = [f"SELECT {i}, " + ", ".join(f"HEX(WEIGHT_STRING({seq_expr(s, 'utf8mb4_uca1400_as_cs')} LEVEL {l}))"
                                         for l in (1, 2, 3)) + ";" for i, s in enumerate(seqs)]
    out = {}
    for line in run(a.maria, "\n".join(stmts)).splitlines():
        f = line.split("\t")
        got = [words(h) for h in f[1:]]
        s = seqs[int(f[0])]
        apart = [sum((singles[l][c] for c in s), []) for l in range(3)]
        if got != apart:
            out[tuple(s)] = got
    return out


def mysql_contractions(a, seqs, singles):
    stmts = [f"SELECT {i}, HEX(WEIGHT_STRING({seq_expr(s, 'utf8mb4_0900_as_cs')}));" for i, s in enumerate(seqs)]
    out = {}
    for line in run(a.mysql, "\n".join(stmts)).splitlines():
        i, _, h = line.partition("\t")
        got = split_levels(words(h), 3)
        s = seqs[int(i)]
        apart = [sum((singles[l][c] for c in s), []) for l in range(3)]
        if got != apart:
            out[tuple(s)] = got
    return out


def emit(name, levels, contractions):
    body = bytearray()
    nb = name.encode()
    body += struct.pack("<H", len(nb)) + nb + struct.pack("<B", len(levels))
    total = 0
    for lw in levels:
        rs = runs_of(lw)
        total += len(rs)
        body += struct.pack("<I", len(rs))
        for start, n, delta, first in rs:
            body += struct.pack("<IIBB", start, n, delta or 0, len(first))
            body += struct.pack(f"<{len(first)}H", *first)
    body += struct.pack("<I", len(contractions))
    for s in sorted(contractions):
        body += struct.pack("<B", len(s)) + struct.pack(f"<{len(s)}I", *s)
        for lw in contractions[s]:
            body += struct.pack("<B", len(lw)) + struct.pack(f"<{len(lw)}H", *lw)
    print(f"{name}: {len(levels)} levels, {total} runs, {len(contractions)} contractions", file=sys.stderr)
    return body


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--maria", required=True)
    ap.add_argument("--mysql", required=True)
    ap.add_argument("--allkeys", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--prepare", action="store_true")
    a = ap.parse_args()
    if a.prepare:
        run(a.mysql, "CREATE DATABASE IF NOT EXISTS r_types_probe; CREATE TABLE IF NOT EXISTS r_types_probe.cp "
                     "(n INT PRIMARY KEY); SET SESSION cte_max_recursion_depth = 1200000; INSERT IGNORE INTO "
                     "r_types_probe.cp WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM s WHERE n < 1114111) "
                     f"SELECT n FROM s WHERE n {SURROGATES};")
        run(a.maria, "CREATE DATABASE IF NOT EXISTS r_types_probe;")
    print("MariaDB", run(a.maria, "SELECT VERSION()").strip(), "MySQL", run(a.mysql, "SELECT VERSION()").strip(),
          file=sys.stderr)
    seqs = sequences(a.allkeys)
    tables = []

    uca = [maria_chars(a, "utf8mb4_uca1400_as_cs", l) for l in (1, 2, 3)]
    for other, lv in (("utf8mb4_uca1400_ai_ci", [1]), ("utf8mb4_uca1400_as_ci", [1, 2]), ("utf8mb4_uca1400_ai_cs", [1, 3])):
        for l in lv:
            if maria_chars(a, other, l) != uca[l - 1]:
                sys.exit(f"{other} level {l} differs from utf8mb4_uca1400_as_cs")
    tables.append(emit("uca1400", uca, maria_contractions(a, seqs, uca)))

    for name, coll in (("unicode_ci", "utf8mb4_unicode_ci"), ("unicode_520_ci", "utf8mb4_unicode_520_ci")):
        t = maria_chars(a, coll)
        if mysql_chars(a, coll) != t:
            sys.exit(f"{coll}: MySQL and MariaDB differ")
        tables.append(emit(name, [t], {}))

    full = mysql_chars(a, "utf8mb4_0900_as_cs")
    my = [{}, {}, {}]
    for cp, ws in full.items():
        for l, lw in enumerate(split_levels(ws, 3)):
            my[l][cp] = lw
    if mysql_chars(a, "utf8mb4_0900_ai_ci") != my[0]:
        sys.exit("utf8mb4_0900_ai_ci isn't level 1 of utf8mb4_0900_as_cs")
    for cp, ws in mysql_chars(a, "utf8mb4_0900_as_ci").items():
        if split_levels(ws, 2) != [my[0][cp], my[1][cp]]:
            sys.exit("utf8mb4_0900_as_ci isn't levels 1-2 of utf8mb4_0900_as_cs")
    tables.append(emit("mysql0900", my, mysql_contractions(a, seqs, my)))

    for coll, charset, count in (("latin1_swedish_ci", "latin1", 256), ("latin1_general_ci", "latin1", 256),
                                 ("latin1_general_cs", "latin1", 256), ("ascii_general_ci", "ascii", 128)):
        t = byte_table(a.maria, coll, charset, count, ("seq", "seq_0_to_255"))
        if byte_table(a.mysql, coll, charset, count, ("n", "r_types_probe.cp")) != t:
            sys.exit(f"{coll}: MySQL and MariaDB differ")
        if any(len(w) != 1 for w in t.values()) or len(t) != count:
            sys.exit(f"{coll}: not one weight per byte")
        tables.append(emit(coll, [t], {}))

    body = bytearray(b"SPE1") + struct.pack("<H", len(tables))
    for t in tables:
        body += t
    z = zlib.compress(bytes(body), 9)
    open(a.out, "wb").write(z)
    print(f"{a.out}: {len(body)} bytes, {len(z)} compressed", file=sys.stderr)


if __name__ == "__main__":
    main()
