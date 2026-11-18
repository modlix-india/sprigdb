#!/usr/bin/env python3
"""The E0 differential: E0's SQL statements and Redis commands sent in lockstep
to the endpoint under test and to a reference server, answers compared.

SQL (sysbench's table and statements, plus E0's collations and error cases):
  diff.py sql --target H:P --ref H:P --flavor mysql|mariadb --user U --password P [--ref-user U --ref-password P] --db e0diff
compares, statement by statement: OK / ERR / rows, error code and SQLSTATE,
affected rows, column names and types, row values (text and binary protocol),
and, for the prepared point select, every result packet byte for byte (column
definitions and rows). Error messages that differ are listed separately.
--flavor picks the reference's dialect: mysql (MySQL 8.4, utf8mb4_0900_ai_ci)
for the MySQL persona, mariadb (MariaDB 11.8, utf8mb4_uca1400_ai_ci) for the
MariaDB persona.

Redis (E0's commands in RESP2, a pipeline, and RESP3 replies):
  diff.py redis --target H:P --ref H:P [--user U --password P]
Keys carry a per-run prefix, so nothing is flushed. TTLs compare within 2 s;
HELLO, INFO, CONFIG GET and COMMAND compare by reply shape only.

Each run writes a measurement (<model>/correct/<workload> matched of total) in
the market format and a full report under the log directory. Exit status 1
when anything differs.
"""
import argparse
import datetime
import os
import random
import struct
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(HERE)), "lib"))
import mysqlwire  # noqa: E402
import resp  # noqa: E402
import result  # noqa: E402


class Tally:
    def __init__(self, log):
        self.total = self.matched = 0
        self.bytes_total = self.bytes_matched = 0
        self.msg_diffs = []
        self.diffs = []
        self.log = log

    def note(self, line):
        self.log.write(line + "\n")

    def check(self, label, ok, detail=""):
        self.total += 1
        if ok:
            self.matched += 1
            self.note(f"ok    {label}")
        else:
            self.diffs.append(f"{label}: {detail}")
            self.note(f"DIFF  {label}: {detail}")


# ---------------------------------------------------------------- SQL

def sb_c(i):
    r = random.Random(i)
    return "-".join("".join(str(r.randrange(10)) for _ in range(11)) for _ in range(10))


def sb_pad(i):
    r = random.Random(-i)
    return "-".join("".join(str(r.randrange(10)) for _ in range(11)) for _ in range(5))


def strip_schema(p):
    """A column definition packet with its schema name blanked (self-tests compare two databases)."""
    try:
        cat, i = mysqlwire.lenenc_bytes(p, 0)
        if cat != b"def":
            return p
        _, j = mysqlwire.lenenc_bytes(p, i)
        return p[:i] + b"\x00" + p[j:]
    except (IndexError, TypeError, struct.error):
        return p


def norm(res):
    """What must be equal between two answers to one statement."""
    if res.kind == "err":
        return ("err", res.err.code, res.err.state)
    if res.kind == "ok":
        return ("ok", res.affected, res.warnings, res.status & 0x0003)
    cols = tuple((c["name"], c["type"], c["flags"] & ~0x4000, c["decimals"]) for c in res.columns)  # 0x4000: MariaDB-only "part of key" bits vary
    return ("rows", cols, tuple(res.rows), res.warnings)


def sql_steps(T, flavor):
    ai = "utf8mb4_0900_ai_ci" if flavor == "mysql" else "utf8mb4_uca1400_ai_ci"
    s = []
    q = lambda sql, label=None: s.append(("q", sql, None, label or sql[:90]))  # noqa: E731
    p = lambda sql, argsets, label=None, raw=False: s.append(("p", sql, argsets, label or sql[:90], raw))  # noqa: E731
    q("SET autocommit=1")
    q("SET NAMES utf8mb4")
    q(f"CREATE TABLE {T}(id INTEGER NOT NULL AUTO_INCREMENT, k INTEGER DEFAULT '0' NOT NULL, "
      f"c CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT '' NOT NULL, PRIMARY KEY (id)) /*! ENGINE = innodb */")
    vals = ",".join(f"({(i * 7919) % 100 + 1}, '{sb_c(i)}', '{sb_pad(i)}')" for i in range(1, 101))
    q(f"INSERT INTO {T}(k, c, pad) VALUES {vals}", "INSERT 100 rows (sysbench prepare form)")
    q(f"CREATE INDEX k_1 ON {T}(k)")
    for i in (1, 50, 100, 101, 0, -1):
        q(f"SELECT c FROM {T} WHERE id={i}")
    q(f"SELECT id, k, c, pad FROM {T} WHERE id=7")
    q(f"SELECT id, k FROM {T} WHERE id=8")
    p(f"SELECT c FROM {T} WHERE id=?", [(i,) for i in list(range(1, 101)) + [101, 0, 2**31]], "prepared point select", raw=True)
    p(f"SELECT id, k, c, pad FROM {T} WHERE id=?", [(3,), (99,), (500,)], "prepared full-row select", raw=True)
    p(f"UPDATE {T} SET k=k+1 WHERE id=?", [(5,), (5,), (77,), (1000,)], "index update (k=k+1)")
    q(f"SELECT k FROM {T} WHERE id=5")
    p(f"UPDATE {T} SET c=? WHERE id=?", [(sb_c(1000 + i), i) for i in (6, 6, 9, 999)], "non-index update")
    q(f"SELECT c FROM {T} WHERE id=6")
    q(f"UPDATE {T} SET c=c WHERE id=6", "update that changes nothing")
    p(f"DELETE FROM {T} WHERE id=?", [(10,), (10,), (11,)], "delete")
    p(f"INSERT INTO {T} (id, k, c, pad) VALUES (?, ?, ?, ?)", [(10, 3, sb_c(10), sb_pad(10)), (10, 3, "x", "y"),
                                                              (200, 4, sb_c(200), sb_pad(200))], "insert (and duplicate key)")
    q(f"SELECT id, k, c FROM {T} WHERE id=10")
    q(f"SELECT c FROM {T} WHERE id=11")
    q(f"INSERT INTO {T} (id, k, c, pad) VALUES (12, 1, 'dup', 'dup')", "duplicate primary key")
    q("BEGIN")
    q(f"SELECT c FROM {T} WHERE id=13")
    q(f"UPDATE {T} SET k=k+1 WHERE id=13")
    q("COMMIT")
    q(f"SELECT k FROM {T} WHERE id=13")
    q("BEGIN")
    q(f"UPDATE {T} SET k=1000 WHERE id=14")
    q(f"DELETE FROM {T} WHERE id=15")
    q("ROLLBACK")
    q(f"SELECT k FROM {T} WHERE id=14", "rolled-back update is gone")
    q(f"SELECT c FROM {T} WHERE id=15", "rolled-back delete is gone")
    q(f"SELECT c FROM {T}_missing WHERE id=1", "unknown table")
    q(f"SELECT nope FROM {T} WHERE id=1", "unknown column")
    q(f"INSERT INTO {T} (id, k, c, pad) VALUES (300, 'notanumber', 'c', 'p')", "bad integer value")
    # collations: one table per collation, VARCHAR primary key, lookups by case and accent variants
    words = ["abc", "ABD", "Straße", "café", "zeta", "Ångström"]
    for name, coll in (("bin", "utf8mb4_bin"), ("gci", "utf8mb4_general_ci"), ("aici", ai)):
        t = f"{T}_{name}"
        q(f"CREATE TABLE {t} (name VARCHAR(32) COLLATE {coll} NOT NULL, v INTEGER NOT NULL, PRIMARY KEY (name))")
        q(f"INSERT INTO {t} (name, v) VALUES " + ",".join(f"('{w}', {i})" for i, w in enumerate(words)))
        for probe in ("abc", "ABC", "abd", "strasse", "STRASSE", "cafe", "CAFÉ", "angstrom", "zeta ", "nope"):
            q(f"SELECT name, v FROM {t} WHERE name='{probe}'", f"{coll}: name='{probe}'")
        p(f"SELECT v FROM {t} WHERE name=?", [("ABC",), ("cafe",), ("Ångström",)], f"{coll}: prepared lookup")
        q(f"INSERT INTO {t} (name, v) VALUES ('ABC', 9)", f"{coll}: insert of a case variant")
    return s


def run_sql(a, out_dir):
    T = f"e0d_{a.run_id.replace('-', '_')}"
    log = open(os.path.join(out_dir, f"sql-{a.flavor}.txt"), "w")
    t = Tally(log)
    tgt = mysqlwire.Conn(*a.target.rsplit(":", 1), a.user, a.password, a.db)
    ref = mysqlwire.Conn(*a.ref.rsplit(":", 1), a.ref_user or a.user, a.ref_password or a.password, a.ref_db or a.db)
    t.note(f"target {a.target} {tgt.server_version}; reference {a.ref} {ref.server_version}; table {T}")
    for step in sql_steps(T, a.flavor):
        kind, sql, argsets, label = step[:4]
        if kind == "q":
            rt, rr = tgt.query(sql), ref.query(sql)
            nt, nr = [norm(x) for x in rt], [norm(x) for x in rr]
            t.check(label, nt == nr, f"target {nt!r:.300} / reference {nr!r:.300}")
            for x, y in zip(rt, rr):
                if x.kind == y.kind == "err" and x.err.msg != y.err.msg:
                    t.msg_diffs.append(f"{label}: {x.err.msg!r} vs {y.err.msg!r}")
            continue
        raw = step[4]
        try:
            st = tgt.prepare(sql)
        except mysqlwire.MySQLError as e:
            st = e
        try:
            sr = ref.prepare(sql)
        except mysqlwire.MySQLError as e:
            sr = e
        if isinstance(st, Exception) or isinstance(sr, Exception):
            t.check(f"PREPARE {label}", type(st) is type(sr) and getattr(st, "code", 0) == getattr(sr, "code", 0),
                    f"target {st!r:.200} / reference {sr!r:.200}")
            continue
        meta_t = [(c["name"], c["type"]) for c in st["columns"]]
        meta_r = [(c["name"], c["type"]) for c in sr["columns"]]
        t.check(f"PREPARE {label}", (st["nparams"], meta_t) == (sr["nparams"], meta_r),
                f"target {st['nparams']} {meta_t} / reference {sr['nparams']} {meta_r}")
        if raw:
            t.bytes_total += 1
            fix = strip_schema if a.self_test else (lambda x: x)
            same = [fix(x) for x in st["packets"][1:]] == [fix(x) for x in sr["packets"][1:]] and st["packets"][0][5:] == sr["packets"][0][5:]
            t.bytes_matched += same
            if not same:
                t.note(f"BYTES PREPARE {label}: target {[x.hex() for x in st['packets']]} / reference {[x.hex() for x in sr['packets']]}")
        for args in argsets:
            rt, rr = tgt.execute(st, args), ref.execute(sr, args)
            nt, nr = [norm(x) for x in rt], [norm(x) for x in rr]
            t.check(f"EXECUTE {label} {args!r:.60}", nt == nr, f"target {nt!r:.300} / reference {nr!r:.300}")
            if raw:
                t.bytes_total += 1
                bt = [fix(p) for x in rt for p in x.packets]
                br = [fix(p) for x in rr for p in x.packets]
                t.bytes_matched += bt == br
                if bt != br:
                    t.note(f"BYTES EXECUTE {label} {args!r}: target {[x.hex() for x in bt]} / reference {[x.hex() for x in br]}")
        tgt.close_stmt(st)
        ref.close_stmt(sr)
    for c in (tgt, ref):  # best-effort cleanup
        for suffix in ("", "_bin", "_gci", "_aici"):
            try:
                c.query(f"DROP TABLE IF EXISTS {T}{suffix}")
            except Exception:  # noqa: BLE001
                pass
    tgt.close()
    ref.close()
    return t


# ---------------------------------------------------------------- Redis

SHAPE_ONLY = {"HELLO", "INFO", "CONFIG", "COMMAND", "CLIENT"}


def shape(v):
    if isinstance(v, resp.Err):
        return ("err", v.code)
    if isinstance(v, dict):
        return ("map", tuple(sorted(str(k) for k in v)))
    if isinstance(v, list):
        return (type(v).__name__,)
    return (type(v).__name__,)


def same_reply(cmd, x, y):
    name = cmd[0].upper()
    if name in SHAPE_ONLY:
        if name == "HELLO" and isinstance(x, (dict, list)) and isinstance(y, (dict, list)):
            def proto(v):
                if isinstance(v, dict):
                    return v.get(b"proto")
                for i in range(0, len(v) - 1, 2):
                    if v[i] == b"proto":
                        return v[i + 1]
            return type(x) is type(y) and proto(x) == proto(y)
        return shape(x) == shape(y)
    if name in ("TTL", "PTTL") and isinstance(x, int) and isinstance(y, int) and x >= 0 and y >= 0:
        return abs(x - y) <= (2 if name == "TTL" else 2000)
    if isinstance(x, resp.Err) and isinstance(y, resp.Err):
        return x.code == y.code
    return type(x) is type(y) and x == y


def redis_steps(P):
    k = lambda n: f"{P}:{n}"  # noqa: E731
    c = [
        ["PING"], ["PING", "hello"],
        ["SET", k("a"), "1"], ["GET", k("a")], ["GET", k("missing")], ["INCR", k("a")], ["INCR", k("new")],
        ["SET", k("s"), "hello"], ["INCR", k("s")], ["SET", k("a"), "5", "NX"], ["SET", k("b"), "5", "NX"],
        ["SET", k("a"), "7", "XX"], ["SET", k("c"), "1", "XX"], ["GET", k("a")], ["GET", k("c")],
        ["SET", k("e"), "v", "EX", "100"], ["TTL", k("e")], ["SET", k("p"), "v", "PX", "100000"], ["TTL", k("p")],
        ["SET", k("ep"), "v", "EX", "100", "NX"], ["SET", k("ep"), "w", "EX", "100", "NX"], ["GET", k("ep")],
        ["SET", k("ep"), "w", "XX"], ["TTL", k("ep")], ["TTL", k("a")], ["TTL", k("missing")],
        ["EXPIRE", k("a"), "50"], ["EXPIRE", k("missing"), "5"], ["TTL", k("a")],
        ["DEL", k("a"), k("b"), k("missing")], ["GET", k("a")],
        ["MSET", k("m1"), "x", k("m2"), "y"], ["MGET", k("m1"), k("m2"), k("none")], ["MGET", k("m1")],
        ["HSET", k("h"), "f1", "v1", "f2", "v2"], ["HSET", k("h"), "f1", "v9"], ["HGET", k("h"), "f1"],
        ["HGET", k("h"), "f2"], ["HGET", k("h"), "nope"], ["HGET", k("none"), "f"],
        ["LPUSH", k("l"), "a", "b", "c"], ["LPUSH", k("l"), "d"],
        ["GET", k("h")], ["GET", k("l")], ["LPUSH", k("s"), "x"], ["HSET", k("s"), "f", "v"], ["HGET", k("l"), "f"],
        ["INCR", k("h")], ["SET", k("big"), "9223372036854775807"], ["INCR", k("big")], ["GET", k("big")],
        ["SET", k("neg"), "-5"], ["INCR", k("neg")], ["SET", k("sp"), " 5"], ["INCR", k("sp")],
        ["SET", k("x")], ["GET"], ["INCR"], ["HSET", k("h"), "f"], ["MSET", k("m1")], ["LPUSH", k("l")],
        ["SET", k("e2"), "v", "EX", "0"], ["SET", k("e3"), "v", "EX", "-1"], ["SET", k("e4"), "v", "EX", "abc"],
        ["SET", k("e5"), "v", "NX", "XX"], ["SET", k("e6"), "v", "EX", "10", "PX", "100"],
        ["EXPIRE", k("s"), "0"], ["GET", k("s")], ["EXPIRE", k("m1"), "-1"], ["GET", k("m1")],
        ["EXPIRE", k("m2"), "notanumber"], ["DEL", k("h")], ["HGET", k("h"), "f1"], ["DEL", k("l"), k("l")],
        ["SET", k("bin"), b"\x00\xff\r\n binary"], ["GET", k("bin")], ["SET", k("empty"), ""], ["GET", k("empty")],
        ["SET", k("u"), "ünïcødé"], ["GET", k("u")], ["SELECT", "0"],
        ["INFO", "server"], ["CONFIG", "GET", "maxmemory"], ["COMMAND", "COUNT"], ["NOSUCHCOMMAND", "x"],
    ]
    return [[x if isinstance(x, bytes) else str(x) for x in cmd] for cmd in c]


def run_redis(a, out_dir):
    P = f"e0d:{a.run_id}"
    PR = P + (":ref" if a.self_test else "")
    log = open(os.path.join(out_dir, "redis.txt"), "w")
    t = Tally(log)
    tgt = resp.Conn(a.target, a.user, a.password)
    ref = resp.Conn(a.ref, a.ref_user, a.ref_password)
    t.note(f"target {a.target}; reference {a.ref}; prefix {P}")
    for cmd, cmd_ref in zip(redis_steps(P), redis_steps(PR)):
        x, y = tgt.cmd(*cmd), ref.cmd(*cmd_ref)
        t.check(" ".join(c if isinstance(c, str) else repr(c) for c in cmd), same_reply(cmd, x, y), f"target {x!r:.200} / reference {y!r:.200}")
        if isinstance(x, resp.Err) and isinstance(y, resp.Err) and x.line != y.line:
            t.msg_diffs.append(f"{cmd[0]}: {x.line!r} vs {y.line!r}")
    # one pipeline: 60 commands in one write
    def pipeline(P):
        pipe = []
        for i in range(20):
            pipe += [["SET", f"{P}:pl{i}", str(i)], ["INCR", f"{P}:pl{i}"], ["GET", f"{P}:pl{i}"]]
        return pipe + [["MGET"] + [f"{P}:pl{i}" for i in range(20)], ["HSET", f"{P}:plh", "a", "1"], ["LPUSH", f"{P}:pll", "q"],
                       ["INCR", f"{P}:plh"], ["DEL"] + [f"{P}:pl{i}" for i in range(20)]]
    pipe = pipeline(P)
    xs, ys = tgt.pipeline(pipe), ref.pipeline(pipeline(PR))
    for cmd, x, y in zip(pipe, xs, ys):
        t.check("pipeline " + " ".join(cmd)[:80], same_reply(cmd, x, y), f"target {x!r:.200} / reference {y!r:.200}")
    # RESP3
    def r3(P):
        return [["HELLO", "3"], ["SET", f"{P}:r3", "v"], ["GET", f"{P}:r3"], ["GET", f"{P}:r3none"], ["MGET", f"{P}:r3", f"{P}:r3none"],
                ["HSET", f"{P}:r3h", "f", "1"], ["HGET", f"{P}:r3h", "f"], ["HGET", f"{P}:r3h", "g"], ["TTL", f"{P}:r3"],
                ["INCR", f"{P}:r3"], ["HELLO", "2"], ["GET", f"{P}:r3none"]]
    for cmd, cmd_ref0 in zip(r3(P), r3(PR)):
        if cmd[0] == "HELLO" and a.password:
            cmd_t = cmd + ["AUTH", a.user or "default", a.password]
        else:
            cmd_t = cmd
        cmd_r = cmd_ref0 + (["AUTH", a.ref_user or "default", a.ref_password] if cmd[0] == "HELLO" and a.ref_password else [])
        x, y = tgt.cmd(*cmd_t), ref.cmd(*cmd_r)
        t.check("resp3 " + " ".join(cmd), same_reply(cmd, x, y), f"target {x!r:.200} / reference {y!r:.200}")
    for c, pfx in ((tgt, P), (ref, PR)):  # cleanup
        try:
            c.cmd("DEL", *[f"{pfx}:{n}" for n in ("new", "s", "c", "e", "p", "ep", "m1", "m2", "big", "neg", "sp", "bin",
                                                 "empty", "u", "plh", "pll", "r3", "r3h", "e2", "e3", "e4", "e5", "e6")])
        except Exception:  # noqa: BLE001
            pass
    tgt.close()
    ref.close()
    return t


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["sql", "redis"])
    ap.add_argument("--target", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--flavor", default="mysql", choices=["mysql", "mariadb"])
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--ref-user", default="")
    ap.add_argument("--ref-password", default="")
    ap.add_argument("--db", default="e0diff")
    ap.add_argument("--ref-db", default="", help="the reference's database (default --db)")
    ap.add_argument("--self-test", action="store_true", help="target and reference are the same server: use separate key prefixes")
    ap.add_argument("--product", default="SprigDB E0")
    ap.add_argument("--version", default="")
    ap.add_argument("--build", default=os.environ.get("BENCH_BUILD", "not recorded"))
    ap.add_argument("--run-id", default=datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    ap.add_argument("--logs", default=os.environ.get("BENCH_LOGS", "."))
    ap.add_argument("--no-record", action="store_true", help="print only; write no result file")
    a = ap.parse_args()
    rel = f"e0-diff-{a.what}-{a.flavor if a.what == 'sql' else 'redis'}-{result.slug(a.product)}-{a.run_id}"
    out_dir = os.path.join(a.logs, rel)
    os.makedirs(out_dir, exist_ok=True)
    t0 = time.time()
    t = run_sql(a, out_dir) if a.what == "sql" else run_redis(a, out_dir)
    t.log.write(f"\n{t.matched}/{t.total} matched; bytes {t.bytes_matched}/{t.bytes_total}; {len(t.msg_diffs)} message differences\n")
    for m in t.msg_diffs:
        t.log.write(f"message: {m}\n")
    t.log.close()
    print(f"{a.what} {a.flavor if a.what == 'sql' else ''}: {t.matched}/{t.total} matched"
          + (f", result packets byte-identical {t.bytes_matched}/{t.bytes_total}" if t.bytes_total else "")
          + f", {len(t.msg_diffs)} error messages differ ({time.time() - t0:.1f} s); report {out_dir}")
    for d in t.diffs[:40]:
        print("  DIFF", d[:300])
    if not a.no_record:
        comp = {"mysql": "MySQL 8.4", "mariadb": "MariaDB 11.8 (direct)"}[a.flavor] if a.what == "sql" else "Redis 7.4 / 8.1"
        model, wl = ("sql", f"e0-sql-diff-{a.flavor}") if a.what == "sql" else ("kv", "e0-redis-diff")
        common = dict(kind="measurement", model=model, op="correct", product=a.product, product_version=a.version,
                      competitor=comp, build=a.build, source=f"logs/{rel}", date=datetime.date.today().isoformat(),
                      command="e0/diff.sh " + " ".join(sys.argv[1:]).replace(a.password or "\0", "***"))
        result.write(dict(common, workload=wl, metric="matched", value=t.matched, total=t.total, unit="statements" if a.what == "sql" else "commands",
                          better="higher", run=f"{wl}-{result.slug(a.product)}-{a.run_id}",
                          title=f"E0 differential against {comp}: {t.matched} of {t.total} answers equal",
                          notes=("; ".join(t.diffs[:10]) + (f"; and {len(t.diffs) - 10} more" if len(t.diffs) > 10 else "")) or
                          f"No differences. Error messages differing: {len(t.msg_diffs)}."))
        if t.bytes_total:
            result.write(dict(common, workload=f"e0-point-select-bytes-{a.flavor}", metric="matched", value=t.bytes_matched,
                              total=t.bytes_total, unit="answers", better="higher",
                              run=f"e0-point-select-bytes-{a.flavor}-{result.slug(a.product)}-{a.run_id}",
                              title=f"Prepared point selects: {t.bytes_matched} of {t.bytes_total} answers byte-identical to {comp}",
                              notes="Every result packet (prepare response without the statement id, column definitions, rows, EOF/OK) compared byte for byte."))
    sys.exit(0 if t.matched == t.total else 1)


if __name__ == "__main__":
    main()
