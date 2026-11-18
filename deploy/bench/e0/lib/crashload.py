#!/usr/bin/env python3
"""Writer and verifier for the E0 kill -9 runner (crash.py). Runs in a
container on the harness network; standard library only.

  crashload.py write  --proto redis|sql --target H:P [--user U --password P --db D] --loop N --dir /acks [--conns 8] [--seconds 30]
  crashload.py verify --proto redis|sql --target H:P [...] --loop N --dir /acks

write: each connection writes unique records and appends a line to its own
acknowledgement file only after the server's reply says the write is done.
Redis: SET of a unique key; every 4th write a two-key MSET (both keys or
neither may exist); every 8th write a pipeline of 8 SETs; and INCR of the
connection's counter (the value after recovery must be at least the last
acknowledged reply). SQL: single-row autocommit INSERTs, and every 4th write a
transaction that inserts 3 rows and bumps a counter row, acknowledged at COMMIT.
The writer stops at the first connection error (the kill) or after --seconds.

verify: reads every acknowledgement file of the loop and checks the server:
lost = acknowledged records missing (or with another value); partial =
multi-record writes found incomplete (acknowledged or not). Prints one JSON line.
"""
import argparse
import glob
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mysqlwire  # noqa: E402
import resp  # noqa: E402


def redis_writer(a, cid, stop):
    path = os.path.join(a.dir, f"loop{a.loop}-c{cid}.ack")
    f = open(path, "a", buffering=1)
    try:
        c = resp.Conn(a.target, a.user, a.password, timeout=10)
    except Exception as e:  # noqa: BLE001
        f.write(f"#connect-failed {e}\n")
        return
    n = 0
    ctr = f"cr:{a.loop}:c{cid}:ctr"
    try:
        while not stop.is_set():
            n += 1
            base = f"cr:{a.loop}:c{cid}:{n}"
            if n % 8 == 0:
                cmds = [["SET", f"{base}:p{i}", f"v{n}.{i}"] for i in range(8)]
                rs = c.pipeline(cmds)
                for cmd, r in zip(cmds, rs):
                    if r == "OK":
                        f.write(f"S {cmd[1]} {cmd[2]}\n")
            elif n % 4 == 0:
                f.write(f"T {base}:a {base}:b\n")  # attempted: atomicity checked whether or not acknowledged
                r = c.cmd("MSET", f"{base}:a", f"v{n}", f"{base}:b", f"v{n}")
                if r == "OK":
                    f.write(f"M {base}:a {base}:b v{n}\n")
            else:
                r = c.cmd("SET", base, f"v{n}")
                if r == "OK":
                    f.write(f"S {base} v{n}\n")
            r = c.cmd("INCR", ctr)
            if isinstance(r, int):
                f.write(f"I {ctr} {r}\n")
    except Exception:  # noqa: BLE001  the kill ends the run
        pass
    f.close()


def sql_conn(a):
    h, p = a.target.rsplit(":", 1)
    return mysqlwire.Conn(h, int(p), a.user, a.password, a.db, timeout=10)


def sql_setup(a):
    c = sql_conn(a)
    c.q1(f"CREATE TABLE IF NOT EXISTS crash_t (id INTEGER NOT NULL, v CHAR(40) NOT NULL DEFAULT '', PRIMARY KEY (id))")
    c.q1(f"CREATE TABLE IF NOT EXISTS crash_ctr (id INTEGER NOT NULL, n INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (id))")
    c.close()


def sql_writer(a, cid, stop):
    path = os.path.join(a.dir, f"loop{a.loop}-c{cid}.ack")
    f = open(path, "a", buffering=1)
    try:
        c = sql_conn(a)
        ins = c.prepare("INSERT INTO crash_t (id, v) VALUES (?, ?)")
    except Exception as e:  # noqa: BLE001
        f.write(f"#connect-failed {e}\n")
        return
    base = (a.loop * 16 + cid) * 1_000_000  # unique INTEGER ids per loop and connection (loops <= 130, conns <= 16)
    ctr = a.loop * 16 + cid
    n = 0
    try:
        c.q1(f"INSERT INTO crash_ctr (id, n) VALUES ({ctr}, 0)")
        f.write(f"I {ctr} 0\n")
        while not stop.is_set():
            n += 1
            rid = base + n * 4
            if n % 4 == 0:
                f.write(f"T {rid} {rid + 1} {rid + 2}\n")
                c.q1("BEGIN")
                for i in range(3):
                    r = c.execute(ins, (rid + i, f"t{n}"))[0]
                    if r.kind == "err":
                        raise r.err
                c.q1(f"UPDATE crash_ctr SET n=n+1 WHERE id={ctr}")
                r = c.query("COMMIT")[0]
                if r.kind == "ok":
                    f.write(f"M {rid} {rid + 1} {rid + 2} t{n}\n")
                    f.write(f"I {ctr} {n // 4}\n")
            else:
                r = c.execute(ins, (rid, f"s{n}"))[0]
                if r.kind == "ok":
                    f.write(f"S {rid} s{n}\n")
    except Exception:  # noqa: BLE001
        pass
    f.close()


def write(a):
    if a.proto == "sql":
        for _ in range(60):
            try:
                sql_setup(a)
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
    stop = threading.Event()
    fn = redis_writer if a.proto == "redis" else sql_writer
    ts = [threading.Thread(target=fn, args=(a, i, stop), daemon=True) for i in range(a.conns)]
    for t in ts:
        t.start()
    end = time.time() + a.seconds
    while time.time() < end and any(t.is_alive() for t in ts):
        time.sleep(0.05)
    stop.set()
    for t in ts:
        t.join(15)


def parse_acks(a):
    singles, multis, attempts, ctrs = {}, [], [], {}
    for p in glob.glob(os.path.join(a.dir, f"loop{a.loop}-c*.ack")):
        for line in open(p):
            w = line.split()
            if not w or w[0].startswith("#"):
                continue
            if w[0] == "S":
                singles[w[1]] = w[2]
            elif w[0] == "M":
                multis.append((w[1:-1], w[-1]))
            elif w[0] == "T":
                attempts.append(w[1:])
            elif w[0] == "I":
                ctrs[w[1]] = max(ctrs.get(w[1], 0), int(w[2]))
    return singles, multis, attempts, ctrs


def verify(a):
    singles, multis, attempts, ctrs = parse_acks(a)
    lost, partial, details = 0, 0, []
    acked = len(singles) + len(multis) + len(ctrs)
    if a.proto == "redis":
        c = resp.Conn(a.target, a.user, a.password, timeout=30)
        keys = list(singles)
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            for k, v in zip(chunk, c.cmd("MGET", *chunk)):
                if v != singles[k].encode():
                    lost += 1
                    details.append(f"lost {k}: {v!r}")
        for ks, v in multis:
            got = c.cmd("MGET", *ks)
            if any(x != v.encode() for x in got):
                lost += 1
                details.append(f"lost MSET {ks}: {got!r}")
        for ks in attempts:
            got = c.cmd("MGET", *ks)
            if len({x is None for x in got}) > 1:
                partial += 1
                details.append(f"partial MSET {ks}: {got!r}")
        for k, n in ctrs.items():
            v = c.cmd("GET", k)
            if v is None or int(v) < n:
                lost += 1
                details.append(f"counter {k}: {v!r} < acknowledged {n}")
    else:
        c = sql_conn(a)
        st = c.prepare("SELECT v FROM crash_t WHERE id=?")
        def get(i):
            r = c.execute(st, (int(i),))[0]
            return r.rows[0][0].decode().strip() if r.rows else None
        for k, v in singles.items():
            if get(k) != v:
                lost += 1
                details.append(f"lost row {k}")
        for ks, v in multis:
            if any(get(k) != v for k in ks):
                lost += 1
                details.append(f"lost transaction {ks}")
        for ks in attempts:
            present = [get(k) is not None for k in ks]
            if len(set(present)) > 1:
                partial += 1
                details.append(f"partial transaction {ks}: {present}")
        for k, n in ctrs.items():
            r = c.q1(f"SELECT n FROM crash_ctr WHERE id={int(k)}")
            v = int(r.rows[0][0]) if r.rows else None
            if v is None or v < n:
                lost += 1
                details.append(f"counter {k}: {v} < acknowledged {n}")
    print(json.dumps({"loop": a.loop, "acked": acked, "lost": lost, "partial": partial, "attempted_multi": len(attempts),
                      "details": details[:20]}))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["write", "verify"])
    ap.add_argument("--proto", required=True, choices=["redis", "sql"])
    ap.add_argument("--target", required=True)
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--db", default="e0crash")
    ap.add_argument("--loop", type=int, required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--conns", type=int, default=8)
    ap.add_argument("--seconds", type=float, default=30)
    a = ap.parse_args()
    write(a) if a.mode == "write" else verify(a)


if __name__ == "__main__":
    main()
