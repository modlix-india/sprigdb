#!/usr/bin/env python3
"""The openCypher TCK over Bolt, for any Bolt server: reads the feature files
(scenarios, outlines with examples, backgrounds, parameters, result tables,
side effects, expected errors), runs each scenario on an emptied graph, and
counts the scenarios that pass. Standard library only (graph/boltlite.py).

  graph/tck.py --target 127.0.0.1:47801 --product Memgraph --version 3.6.1 \
      --features <tck>/features [--user u --password p]

How it judges (the same rules for every product, so run it on a reference
Neo4j too and compare like with like; SprigDB's own runner is
crates/sprig-graph/tests/tck.rs, in process):
  - results: the header and the rows, as a multiset ("in any order") or a
    list ("in order"); values compared structurally (nodes by labels and
    properties, relationships by type and properties, paths, lists, maps,
    floats by value); temporal values by their ISO text, as the TCK writes them;
  - errors: an expected error passes when the query fails (the class isn't
    compared);
  - side effects: graph snapshots before and after (node and relationship
    ids, labels in use, property triples), as the TCK's reference API counts them.
Scenarios needing procedures or graphs not shipped with the features fail.
Records graph / compat / opencypher-tck / passed (of all scenarios).
"""
import argparse
import datetime
import glob
import os
import re
import sys
import zoneinfo

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
import boltlite  # noqa: E402
from boltlite import BoltError, Struct  # noqa: E402
import result  # noqa: E402

# ---------------------------------------------------------------- feature files


def parse_feature(path):
    lines = open(path, encoding="utf-8").read().splitlines()
    scenarios, background, cur, i = [], [], None, 0
    feature = os.path.basename(path)

    def table_at(j):
        rows = []
        while j < len(lines) and lines[j].strip().startswith("|"):
            cells = [c.strip() for c in re.split(r"(?<!\\)\|", lines[j].strip())[1:-1]]
            rows.append([c.replace("\\|", "|") for c in cells])
            j += 1
        return rows, j

    while i < len(lines):
        s = lines[i].strip()
        if s.startswith("Background:"):
            cur = {"name": "background", "steps": background}
        elif s.startswith("Scenario Outline:") or s.startswith("Scenario:"):
            cur = {"name": s.split(":", 1)[1].strip(), "steps": [], "outline": s.startswith("Scenario Outline"),
                   "examples": [], "feature": feature}
            scenarios.append(cur)
        elif s.startswith("Examples:") and cur is not None:
            rows, i = table_at(i + 1)
            cur["examples"].append(rows)
            continue
        elif re.match(r"(Given|When|Then|And|But) ", s) and cur is not None:
            text = s.split(" ", 1)[1]
            step = {"text": text.rstrip(":").strip(), "doc": None, "table": None}
            j = i + 1
            if j < len(lines) and lines[j].strip().startswith('"""'):
                k = j + 1
                ind = len(lines[j]) - len(lines[j].lstrip())
                doc = []
                while not lines[k].strip().startswith('"""'):
                    doc.append(lines[k][ind:] if len(lines[k]) >= ind else lines[k].strip())
                    k += 1
                step["doc"] = "\n".join(doc)
                j = k + 1
            elif j < len(lines) and lines[j].strip().startswith("|"):
                step["table"], j = table_at(j)
            cur["steps"].append(step)
            i = j
            continue
        i += 1
    out = []
    for sc in scenarios:
        steps = background + sc["steps"]
        if not sc["outline"]:
            out.append((f"{feature} {sc['name']}", steps))
            continue
        for ex in sc["examples"]:
            head = ex[0]
            for n, row in enumerate(ex[1:]):
                sub = dict(zip(head, row))

                def rep(x):
                    if x is None:
                        return None
                    for k, v in sub.items():
                        x = x.replace(f"<{k}>", v)
                    return x
                st = [{"text": rep(s["text"]), "doc": rep(s["doc"]),
                       "table": [[rep(c) for c in r] for r in s["table"]] if s["table"] else None} for s in steps]
                out.append((f"{feature} {sc['name']} #{n + 1}", st))
    return out

# ---------------------------------------------------------------- TCK values


class P:
    def __init__(self, s):
        self.s, self.i = s, 0

    def ws(self):
        while self.i < len(self.s) and self.s[self.i].isspace():
            self.i += 1

    def peek(self, t):
        self.ws()
        return self.s.startswith(t, self.i)

    def eat(self, t):
        if not self.peek(t):
            raise ValueError(f"expected {t!r} at {self.i} in {self.s!r}")
        self.i += len(t)

    def name(self):
        self.ws()
        if self.s[self.i] == "`":
            j = self.s.index("`", self.i + 1)
            n = self.s[self.i + 1:j]
            self.i = j + 1
            return n
        m = re.compile(r"[A-Za-z_][A-Za-z0-9_]*").match(self.s, self.i)
        self.i = m.end()
        return m.group(0)

    def value(self):
        self.ws()
        c = self.s[self.i]
        if c == "'":
            j, buf = self.i + 1, []
            while self.s[j] != "'":
                if self.s[j] == "\\":
                    j += 1
                    buf.append({"n": "\n", "t": "\t"}.get(self.s[j], self.s[j]))
                else:
                    buf.append(self.s[j])
                j += 1
            self.i = j + 1
            return ("s", "".join(buf))
        if c == "[":
            self.eat("[")
            if self.peek(":"):
                return self.rel_body()
            items = []
            while not self.peek("]"):
                items.append(self.value())
                if self.peek(","):
                    self.eat(",")
            self.eat("]")
            return ("l", items)
        if c == "{":
            return ("m", self.map())
        if c == "(":
            return self.node()
        if c == "<":
            self.eat("<")
            parts = [self.node()]
            while not self.peek(">"):
                if self.peek("<-"):
                    self.eat("<-")
                    self.eat("[")
                    r = self.rel_body()
                    self.eat("-")
                    parts += [("in", r), self.node()]
                else:
                    self.eat("-")
                    self.eat("[")
                    r = self.rel_body()
                    self.eat("->")
                    parts += [("out", r), self.node()]
            self.eat(">")
            return ("p", parts)
        m = re.compile(r"-?(?:\d+\.\d*(?:[eE][-+]?\d+)?|\.\d+(?:[eE][-+]?\d+)?|\d+[eE][-+]?\d+)").match(self.s, self.i)
        if m:
            self.i = m.end()
            return ("f", float(m.group(0)))
        m = re.compile(r"-?(?:0x[0-9a-fA-F]+|0o[0-7]+|\d+)").match(self.s, self.i)
        if m:
            self.i = m.end()
            return ("i", int(m.group(0), 0))
        for word, v in (("null", ("n",)), ("true", ("b", True)), ("false", ("b", False)), ("NaN", ("f", float("nan"))),
                        ("-Infinity", ("f", float("-inf"))), ("Infinity", ("f", float("inf"))), ("Inf", ("f", float("inf")))):
            if self.s.startswith(word, self.i):
                self.i += len(word)
                return v
        raise ValueError(f"bad value at {self.i} in {self.s!r}")

    def map(self):
        self.eat("{")
        d = {}
        while not self.peek("}"):
            k = self.name()
            self.eat(":")
            d[k] = self.value()
            if self.peek(","):
                self.eat(",")
        self.eat("}")
        return d

    def node(self):
        self.eat("(")
        labels = []
        while self.peek(":"):
            self.eat(":")
            labels.append(self.name())
        props = self.map() if self.peek("{") else {}
        self.eat(")")
        return ("N", sorted(labels), props)

    def rel_body(self):
        self.eat(":")
        t = self.name()
        props = self.map() if self.peek("{") else {}
        self.eat("]")
        return ("R", t, props)


def tck_value(s):
    p = P(s)
    v = p.value()
    p.ws()
    if p.i != len(s):
        raise ValueError(f"trailing text in {s!r}")
    return v

# ---------------------------------------------------------------- Bolt values


def frac(ns):
    if ns == 0:
        return ""
    t = f"{ns:09d}"
    for n in (3, 6, 9):
        if t[n:] == "0" * (9 - n):
            return "." + t[:n]
    return "." + t


def ltime(ns_of_day):
    h, rem = divmod(ns_of_day, 3600 * 10**9)
    m, rem = divmod(rem, 60 * 10**9)
    s, ns = divmod(rem, 10**9)
    out = f"{h:02d}:{m:02d}"
    if s or ns:
        out += f":{s:02d}" + frac(ns)
    return out


def offset(sec):
    if sec == 0:
        return "Z"
    sign = "+" if sec > 0 else "-"
    sec = abs(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{sign}{h:02d}:{m:02d}" + (f":{s:02d}" if s else "")


EPOCH = datetime.date(1970, 1, 1)


def dt_local(secs, ns):
    d = EPOCH + datetime.timedelta(days=secs // 86400)
    return f"{d.isoformat()}T{ltime((secs % 86400) * 10**9 + ns)}"


def duration(months, days, secs, ns):
    y, mo = divmod(abs(months), 12)
    sign = -1 if months < 0 else 1
    out = "P"
    if y:
        out += f"{sign * y}Y"
    if mo:
        out += f"{sign * mo}M"
    if days:
        out += f"{days}D"
    total = secs * 10**9 + ns
    neg = total < 0
    total = abs(total)
    h, rem = divmod(total, 3600 * 10**9)
    m, rem = divmod(rem, 60 * 10**9)
    s, n = divmod(rem, 10**9)
    t = ""
    if h:
        t += f"{'-' if neg else ''}{h}H"
    if m:
        t += f"{'-' if neg else ''}{m}M"
    if s or n:
        t += f"{'-' if neg else ''}{s}" + (f".{n:09d}".rstrip("0") if n else "") + "S"
    if t:
        out += "T" + t
    return out if out != "P" else "PT0S"


def from_bolt(v):
    if v is None:
        return ("n",)
    if isinstance(v, bool):
        return ("b", v)
    if isinstance(v, int):
        return ("i", v)
    if isinstance(v, float):
        return ("f", v)
    if isinstance(v, str):
        return ("s", v)
    if isinstance(v, bytes):
        return ("s", v.hex())
    if isinstance(v, list):
        return ("l", [from_bolt(x) for x in v])
    if isinstance(v, dict):
        return ("m", {k: from_bolt(x) for k, x in v.items()})
    if isinstance(v, Struct):
        f, t = v.fields, v.tag
        if t == 0x4E:
            return ("N", sorted(f[1]), {k: from_bolt(x) for k, x in f[2].items()})
        if t == 0x52:
            return ("R", f[3], {k: from_bolt(x) for k, x in f[4].items()})
        if t == 0x72:
            return ("R", f[1], {k: from_bolt(x) for k, x in f[2].items()})
        if t == 0x50:
            nodes, rels, idx = f[0], f[1], f[2]
            parts = [from_bolt(nodes[0])]
            for j in range(0, len(idx), 2):
                r, n = idx[j], idx[j + 1]
                parts += [("out" if r > 0 else "in", from_bolt(rels[abs(r) - 1])), from_bolt(nodes[n])]
            return ("p", parts)
        if t == 0x44:
            return ("s", (EPOCH + datetime.timedelta(days=f[0])).isoformat())
        if t == 0x74:
            return ("s", ltime(f[0]))
        if t == 0x54:
            return ("s", ltime(f[0]) + offset(f[1]))
        if t == 0x64:
            return ("s", dt_local(f[0], f[1]))
        if t == 0x46:
            return ("s", dt_local(f[0], f[1]) + offset(f[2]))
        if t == 0x66:
            local = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=f[0])
            try:
                off = int(local.replace(tzinfo=zoneinfo.ZoneInfo(f[2])).utcoffset().total_seconds())
            except Exception:
                off = 0
            return ("s", dt_local(f[0], f[1]) + offset(off) + f"[{f[2]}]")
        if t == 0x49:
            return ("s", dt_local(f[0] + f[2], f[1]) + offset(f[2]))
        if t == 0x69:
            utc = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=f[0])
            try:
                off = int(utc.astimezone(zoneinfo.ZoneInfo(f[2])).utcoffset().total_seconds())
            except Exception:
                off = 0
            return ("s", dt_local(f[0] + off, f[1]) + offset(off) + f"[{f[2]}]")
        if t == 0x45:
            return ("s", duration(*f))
        return ("x", t, [from_bolt(x) for x in f])
    return ("x", repr(v))


def canon(v, unordered_lists=False):
    k = v[0]
    if k == "f":
        x = v[1]
        return "f:nan" if x != x else f"f:{x!r}"
    if k in ("i", "b", "s"):
        return f"{k}:{v[1]!r}"
    if k == "n":
        return "null"
    if k == "l":
        items = [canon(x, unordered_lists) for x in v[1]]
        return "[" + ",".join(sorted(items) if unordered_lists else items) + "]"
    if k == "m":
        return "{" + ",".join(f"{a}:{canon(b, unordered_lists)}" for a, b in sorted(v[1].items()) if b != ("n",)) + "}"
    if k == "N":
        return "(" + ":".join(v[1]) + canon(("m", v[2])) + ")"
    if k == "R":
        return "[" + v[1] + canon(("m", v[2])) + "]"
    if k in ("out", "in"):
        return k + canon(v[1])
    if k == "p":
        return "<" + "".join(canon(x) for x in v[1]) + ">"
    return repr(v)

# ---------------------------------------------------------------- running


def snapshot(c):
    nodes = {r[0] for r in c.query("MATCH (n) RETURN id(n)")[1]}
    rels = {r[0] for r in c.query("MATCH ()-[r]->() RETURN id(r)")[1]}
    labels = {r[0] for r in c.query("MATCH (n) UNWIND labels(n) AS l RETURN DISTINCT l")[1]}
    props = {("n", r[0], r[1], canon(from_bolt(r[2]))) for r in
             c.query("MATCH (n) UNWIND keys(n) AS k RETURN id(n), k, n[k]")[1]}
    props |= {("r", r[0], r[1], canon(from_bolt(r[2]))) for r in
              c.query("MATCH ()-[r]->() UNWIND keys(r) AS k RETURN id(r), k, r[k]")[1]}
    return nodes, rels, labels, props


def effects(a, b):
    return {"+nodes": len(b[0] - a[0]), "-nodes": len(a[0] - b[0]), "+relationships": len(b[1] - a[1]),
            "-relationships": len(a[1] - b[1]), "+labels": len(b[2] - a[2]), "-labels": len(a[2] - b[2]),
            "+properties": len(b[3] - a[3]), "-properties": len(a[3] - b[3])}


def run_scenario(c, steps, features_dir):
    params, res, err, eff, before = {}, None, None, None, None
    for st in steps:
        t = st["text"]
        if t in ("an empty graph", "any graph"):
            c.query("MATCH (n) DETACH DELETE n")
        elif t.startswith("the ") and t.endswith(" graph"):
            name = t[4:-6]
            files = glob.glob(os.path.join(features_dir, "..", "**", f"{name}.cypher"), recursive=True)
            if not files:
                return False, f"graph {name} not shipped with the features"
            c.query("MATCH (n) DETACH DELETE n")
            c.query(open(files[0], encoding="utf-8").read())
        elif t.startswith("having executed"):
            c.query(st["doc"])
        elif t.startswith("parameters are"):
            params = {r[0]: to_bolt(tck_value(r[1])) for r in st["table"]}
        elif t.startswith("there exists a procedure"):
            return False, "needs a test procedure"
        elif t.startswith("executing query"):
            before = snapshot(c)
            try:
                res = c.query(st["doc"], params)
            except BoltError as e:
                err = e
            eff = effects(before, snapshot(c))
        elif t.startswith("executing control query"):
            try:
                res, err = c.query(st["doc"], params), None
            except BoltError as e:
                err = e
        elif t.startswith("the result should be empty"):
            if err:
                return False, f"error {err.code}"
            if res[1]:
                return False, f"{len(res[1])} rows"
        elif t.startswith("the result should be"):
            if err:
                return False, f"error {err.code}"
            ordered = "in order" in t
            ign = "ignoring element order for lists" in t
            head, rows = st["table"][0], st["table"][1:]
            if list(res[0]) != head:
                return False, f"columns {res[0]} != {head}"
            try:
                want = ["|".join(canon(tck_value(x), ign) for x in r) for r in rows]
            except ValueError as e:
                return False, f"unparsed expectation: {e}"
            got = ["|".join(canon(from_bolt(x), ign) for x in r) for r in res[1]]
            if not ordered:
                want, got = sorted(want), sorted(got)
            if want != got:
                return False, f"rows differ: want {want[:3]} got {got[:3]}"
        elif re.match(r"an? \w+ should be raised", t):
            if err is None:
                return False, "no error raised"
        elif t.startswith("no side effects"):
            if err is None and eff and any(eff.values()):
                return False, f"side effects {eff}"
        elif t.startswith("the side effects should be"):
            want = {k: 0 for k in eff}
            want.update({r[0]: int(r[1]) for r in st["table"]})
            if eff != want:
                return False, f"side effects {eff} != {want}"
        else:
            return False, f"unknown step: {t}"
    return True, ""


def to_bolt(v):
    k = v[0]
    if k == "n":
        return None
    if k in ("i", "f", "b", "s"):
        return v[1]
    if k == "l":
        return [to_bolt(x) for x in v[1]]
    if k == "m":
        return {a: to_bolt(b) for a, b in v[1].items()}
    raise ValueError("graph value as a parameter")


def reconnect(a, wait=180):
    """A new connection, retried while the server restarts; None if it stays down."""
    import time
    end = time.time() + wait
    while time.time() < end:
        try:
            return boltlite.Bolt(a.target, a.user, a.password)
        except OSError:
            time.sleep(3)
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for k in ("target", "product", "features"):
        ap.add_argument("--" + k, required=True)
    for k in ("version", "competitor", "user", "password", "run", "build", "out", "only"):
        ap.add_argument("--" + k, default="")
    a = ap.parse_args()
    files = sorted(glob.glob(os.path.join(a.features, "**", "*.feature"), recursive=True))
    scen = [s for f in files for s in parse_feature(f) if not a.only or a.only in s[0]]
    c = boltlite.Bolt(a.target, a.user, a.password)
    passed, lines, crashes = 0, [], []
    for name, steps in scen:
        try:
            ok, why = run_scenario(c, steps, a.features)
        except (BoltError, OSError, ValueError, KeyError, IndexError) as e:
            ok, why = False, f"{type(e).__name__}: {e}"
            try:
                c.close()
            except OSError:
                pass
            c = reconnect(a)
            if c is None:
                lines.append(f"FAIL\t{name}\tserver gone after this scenario; run stopped")
                break
            if isinstance(e, (OSError, ConnectionError)):
                crashes.append(name)
        passed += ok
        lines.append(f"{'PASS' if ok else 'FAIL'}\t{name}\t{why[:300]}")
    log = os.path.join(result.results_root(a.out or None), "..", "logs", f"tck-{result.slug(a.product)}.tsv")
    os.makedirs(os.path.dirname(log), exist_ok=True)
    open(log, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    print(f"openCypher TCK: {passed} of {len(scen)} scenarios pass on {c.server}")
    rec = {"model": "graph", "op": "compat", "workload": "opencypher-tck-bolt", "metric": "passed", "value": passed,
           "total": len(scen), "unit": "scenarios", "better": "higher", "product": a.product,
           "product_version": a.version, "competitor": a.competitor or a.product,
           "title": f"openCypher TCK over Bolt: {passed} of {len(scen)} scenarios",
           "notes": "bench runner graph/tck.py (errors by presence, side effects by snapshot)"
                    + (f"; the server dropped the connection {len(crashes)} times (first: {crashes[0]})" if crashes else ""),
           "crashes": crashes,
           "config": {"features": "openCypher tck 677cbaf", "server": c.server},
           "source": os.path.relpath(log, os.path.join(result.results_root(a.out or None), "..")),
           "command": f"graph/tck.py --target {a.target} --product {a.product} --features <tck>/features"}
    for k in ("run", "build"):
        if getattr(a, k):
            rec[k] = getattr(a, k)
    if not a.only:
        print(result.write(rec, a.out or None))


if __name__ == "__main__":
    main()
