#!/usr/bin/env python3
"""The Redis command corpus as a differential: every command of the corpus in
crates/sprig-redis/tests/diff_live.rs (CORPUS: strings, keys, hashes, lists,
sets, sorted sets, errors) is sent to the endpoint under test and to a
reference Redis, and the replies are compared after the same normalization
the Rust test uses (unordered replies sorted, TTLs bucketed). Standard
library only, RESP2.

  kv/redis-corpus.py --target HOST:PORT --ref HOST:PORT --product Valkey \
      --version 9.0.0 [--user U --password P] [--ref-version 7.4.11] --flush

--flush is required: the corpus starts from FLUSHALL on both sides, so point
it only at servers (or a SprigDB tenant database) that hold nothing else.
Writes one measurement: kv / correct / redis-corpus / matched (of total).
"""
import argparse
import os
import re
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import result  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SRC = os.path.normpath(os.path.join(HERE, "..", "..", "..", "crates", "sprig-redis", "tests", "diff_live.rs"))


class Err:
    def __init__(self, line):
        code, _, detail = line.partition(" ")
        self.code, self.detail = code, detail


class Resp:
    def __init__(self, hp, user=None, password=None, timeout=10):
        h, p = hp.rsplit(":", 1)
        self.s = socket.create_connection((h, int(p)), timeout=timeout)
        self.f = self.s.makefile("rb")
        if password:
            r = self.cmd(["AUTH", user, password] if user else ["AUTH", password])
            if isinstance(r, Err):
                raise SystemExit(f"AUTH failed on {hp}: {r.code} {r.detail}")

    def cmd(self, args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.s.sendall(b"".join(out))
        return self.read()

    def read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t == b"+":
            return ("simple", rest.decode("utf-8", "replace"))
        if t == b"-":
            return Err(rest.decode("utf-8", "replace"))
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:-2]
            return data
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise ValueError(f"unexpected reply type {line!r}")


def show(v):
    """A reply as comparable text, as the Rust test's show_v prints it."""
    if v is None:
        return "nil"
    if isinstance(v, Err):
        return f"ERR[{v.code}] {v.detail}"
    if isinstance(v, int):
        return f":{v}"
    if isinstance(v, bytes):
        return "$" + v.decode("utf-8", "replace")
    if isinstance(v, tuple):
        return "+OK" if v[1] == "OK" else "+" + v[1]
    if isinstance(v, list):
        return "[" + ", ".join(show(x) for x in v) + "]"
    return repr(v)


def normalize(cmd, v):
    name = cmd[0].lower()
    withscores = any(a.lower() == "withscores" for a in cmd)
    if isinstance(v, list):
        if name in ("smembers", "sunion", "sinter", "sdiff", "keys", "hkeys", "hvals", "zunion") and not withscores:
            v = sorted(v, key=show)
        elif name == "hgetall":
            pairs = [(v[i], v[i + 1] if i + 1 < len(v) else None) for i in range(0, len(v), 2)]
            pairs.sort(key=lambda kv: show(kv[0]))
            v = [x for kv in pairs for x in kv]
    elif isinstance(v, int) and not isinstance(v, bool):
        if name == "ttl" and v > 0:
            v = ("simple", "positive")
        elif name == "pttl" and v > 0:
            v = (v + 500) // 1000
        elif name in ("expiretime", "pexpiretime") and v > 0:
            v = ("simple", "future")
    return show(v)


def corpus(path):
    src = open(path, encoding="utf-8").read()
    m = re.search(r'const CORPUS: &str = r"(.*?)";', src, re.S)
    if not m:
        raise SystemExit(f"no CORPUS constant in {path}")
    cmds = []
    for line in m.group(1).splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            cmds.append([w for w in line.split(" ") if w])
    return cmds


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", required=True)
    p.add_argument("--ref", required=True)
    p.add_argument("--product", required=True)
    p.add_argument("--version", default="")
    p.add_argument("--competitor")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--ref-user")
    p.add_argument("--ref-password")
    p.add_argument("--ref-version", default="")
    p.add_argument("--corpus-source", default=DEFAULT_SRC)
    p.add_argument("--flush", action="store_true")
    p.add_argument("--log", help="file for the mismatch list")
    p.add_argument("--run")
    p.add_argument("--build")
    p.add_argument("--out")
    a = p.parse_args()
    if not a.flush:
        raise SystemExit("--flush is required (the corpus starts from FLUSHALL on both sides)")
    cmds = corpus(a.corpus_source)
    sut = Resp(a.target, a.user, a.password)
    ref = Resp(a.ref, a.ref_user, a.ref_password)
    for c in (sut, ref):
        c.cmd(["FLUSHALL"])
    bad = []
    for c in cmds:
        try:
            x = normalize(c, sut.cmd(c))
        except Exception as e:  # a dropped connection counts as a difference
            x = f"CLIENT-ERROR {e}"
            sut = Resp(a.target, a.user, a.password)
        y = normalize(c, ref.cmd(c))
        if x != y:
            bad.append(f"{' '.join(c)}\n   {a.product}: {x}\n   reference: {y}")
    sut.cmd(["FLUSHALL"])
    ref.cmd(["FLUSHALL"])
    text = "\n".join(f"MISMATCH {b}" for b in bad) + f"\ncorpus: {len(cmds)} commands, {len(bad)} mismatches\n"
    log = a.log or os.path.join(result.results_root(a.out), "..", "logs", f"redis-corpus-{result.slug(a.product)}.txt")
    os.makedirs(os.path.dirname(log), exist_ok=True)
    open(log, "w", encoding="utf-8").write(text)
    print(text[-2000:])
    rec = {
        "model": "kv", "op": "correct", "workload": "redis-corpus", "metric": "matched",
        "value": len(cmds) - len(bad), "total": len(cmds), "unit": "commands", "better": "higher",
        "product": a.product, "product_version": a.version, "competitor": a.competitor or a.product,
        "title": f"Redis command corpus: {len(cmds) - len(bad)} of {len(cmds)} replies equal to Redis {a.ref_version}".strip(),
        "notes": f"{len(bad)} mismatches; list in {os.path.basename(log)}",
        "config": {"reference": f"Redis {a.ref_version} at {a.ref}", "protocol": "RESP2"},
        "source": os.path.relpath(log, os.path.join(result.results_root(a.out), "..")),
        "command": " ".join(x if not x.startswith("--password") else x for x in sys.argv).replace(a.password or "\0", "***"),
    }
    if a.run:
        rec["run"] = a.run
    if a.build:
        rec["build"] = a.build
    print(result.write(rec, a.out))


if __name__ == "__main__":
    main()
