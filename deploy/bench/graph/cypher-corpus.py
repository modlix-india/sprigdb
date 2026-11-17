#!/usr/bin/env python3
"""The Cypher differential corpus over Bolt: the SETUP graph and every CORPUS
statement of crates/sprig-graph/tests/bolt_live.rs run on the endpoint under
test and on a reference Neo4j 5; rows are compared as the Rust test compares
them (sorted unless the statement has ORDER BY; columns too). Standard
library only (graph/boltlite.py, Bolt 4.4).

  graph/cypher-corpus.py --target 127.0.0.1:47801 --product Memgraph --version 3.6.1 \
      --ref 127.0.0.1:47802 --ref-user neo4j --ref-password pw [--user u --password p]

Both servers are emptied first (MATCH (n) DETACH DELETE n): use servers that
hold nothing else. Records graph / correct / cypher-corpus / matched.
"""
import argparse
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
import boltlite  # noqa: E402
import result  # noqa: E402

SRC = os.path.normpath(os.path.join(HERE, "..", "..", "..", "crates", "sprig-graph", "tests", "bolt_live.rs"))


def rust_strings(block):
    out = []
    for m in re.finditer(r'"((?:[^"\\]|\\.)*)"', block):
        out.append(m.group(1).encode().decode("unicode_escape"))
    return out


def corpus(path):
    src = open(path, encoding="utf-8").read()
    setup = rust_strings(re.search(r"const SETUP: &str = (\".*?\");", src, re.S).group(1))[0]
    block = re.search(r"const CORPUS: &\[&str\] = &\[(.*?)\n\];", src, re.S).group(1)
    block = "\n".join(l for l in block.splitlines() if not l.strip().startswith("//"))
    return setup, rust_strings(block)


def rows(recs, ordered):
    v = [" | ".join(boltlite.norm(x) for x in r) for r in recs]
    return v if ordered else sorted(v)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for k in ("target", "ref", "product"):
        p.add_argument("--" + k, required=True)
    for k in ("version", "competitor", "user", "password", "ref-user", "ref-password", "ref-version", "run", "build", "out", "log"):
        p.add_argument("--" + k, default="")
    p.add_argument("--corpus-source", default=SRC)
    a = p.parse_args()
    setup, stmts = corpus(a.corpus_source)
    t = boltlite.Bolt(a.target, a.user, a.password)
    r = boltlite.Bolt(a.ref, a.ref_user, a.ref_password)
    for c in (t, r):
        c.query("MATCH (n) DETACH DELETE n")
        c.query(setup)
    same, lines = 0, []
    for q in stmts:
        try:
            wf, want = r.query(q)
        except boltlite.BoltError as e:
            wf, want = ["error"], [[e.code]]
        try:
            gf, got = t.query(q)
        except boltlite.BoltError as e:
            gf, got = ["error"], [[e.code]]
        except (OSError, ValueError) as e:
            gf, got = ["client-error"], [[str(e)]]
            t = boltlite.Bolt(a.target, a.user, a.password)
        o = "ORDER BY" in q
        if rows(got, o) == rows(want, o) and gf == wf:
            same += 1
        else:
            lines.append(f"DIFF {q}\n  target:    {gf} {rows(got, o)[:6]}\n  reference: {wf} {rows(want, o)[:6]}")
    for c in (t, r):
        c.query("MATCH (n) DETACH DELETE n")
    text = "\n".join(lines) + f"\ncypher corpus: {same} of {len(stmts)} statements identical to the reference ({r.server})\n"
    log = a.log or os.path.join(result.results_root(a.out or None), "..", "logs", f"cypher-corpus-{result.slug(a.product)}.txt")
    os.makedirs(os.path.dirname(log), exist_ok=True)
    open(log, "w", encoding="utf-8").write(text)
    print(text[-3000:])
    rec = {"model": "graph", "op": "correct", "workload": "cypher-corpus", "metric": "matched", "value": same,
           "total": len(stmts), "unit": "statements", "better": "higher", "product": a.product,
           "product_version": a.version, "competitor": a.competitor or a.product,
           "title": f"Cypher corpus: {same} of {len(stmts)} statements identical to {r.server}",
           "notes": f"{len(stmts) - same} differ; list in {os.path.basename(log)}",
           "config": {"reference": f"{r.server} at {a.ref}", "bolt": f"{t.version[0]}.{t.version[1]}"},
           "source": os.path.relpath(log, os.path.join(result.results_root(a.out or None), "..")),
           "command": f"graph/cypher-corpus.py --target {a.target} --ref {a.ref} --product {a.product}"}
    for k in ("run", "build"):
        if getattr(a, k):
            rec[k] = getattr(a, k)
    print(result.write(rec, a.out or None))


if __name__ == "__main__":
    main()
