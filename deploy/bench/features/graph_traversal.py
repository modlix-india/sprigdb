#!/usr/bin/env python3
"""Feature probe: graph traversal. Edges 1->2->3->4 and 1->5; one query for
the nodes reachable from 1 in one or two hops must return 2, 3 and 5."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import check

EDGES = [(1, 2), (2, 3), (3, 4), (1, 5)]


def sql(run, t):
    n = f"ge{t.stamp}"
    vals = ",".join(f"({a},{b})" for a, b in EDGES)
    run(f"CREATE TABLE {n} (src INT, dst INT, PRIMARY KEY (src, dst)); INSERT INTO {n} VALUES {vals}")
    try:
        r = run(f"WITH RECURSIVE w (node, depth) AS (SELECT dst, 1 FROM {n} WHERE src = 1 UNION ALL "
                f"SELECT e.dst, w.depth + 1 FROM w JOIN {n} e ON e.src = w.node WHERE w.depth < 2) "
                f"SELECT DISTINCT node FROM w ORDER BY node")
        check(r == [["2"], ["3"], ["5"]], f"got {r}")
    finally:
        run(f"DROP TABLE {n}")
    return "recursive CTE (WITH RECURSIVE) over an edge table"


def mongo(t):
    n = f"ge{t.stamp}"
    docs = ",".join(f"{{_id:{a * 10 + b},src:{a},dst:{b}}}" for a, b in EDGES)
    out = t.mongo(f"""db.{n}.insertMany([{docs}]);
const r=db.{n}.aggregate([{{$match:{{src:1}}}},{{$graphLookup:{{from:'{n}',startWith:'$dst',connectFromField:'dst',connectToField:'src',as:'p',maxDepth:0}}}}]).toArray();
const s=new Set(); r.forEach(d=>{{s.add(d.dst); d.p.forEach(x=>s.add(x.dst));}}); db.{n}.drop(); print(JSON.stringify([...s].sort()));""")
    check(out.splitlines()[-1] == "[2,3,5]", out)
    return "$graphLookup over an edge collection"


def surreal(t):
    rel = " ".join(f"RELATE gn:{a}->ge->gn:{b};" for a, b in EDGES)
    t.surreal(" ".join(f"CREATE gn:{i};" for i in range(1, 6)) + " " + rel)
    try:
        r = t.surreal("SELECT VALUE array::sort(array::distinct(array::flatten([->ge->gn, ->ge->gn->ge->gn])).map(|$v| meta::id($v))) FROM ONLY gn:1;")[0]
        check(r == [2, 3, 5], f"got {r}")
    finally:
        t.surreal("REMOVE TABLE gn; REMOVE TABLE ge;")
    return "record links: ->ge->gn and ->ge->gn->ge->gn"


def arango(t):
    v, e = f"gv{t.stamp}", f"gee{t.stamp}"
    t.arango("/_api/collection", {"name": v})
    t.arango("/_api/collection", {"name": e, "type": 3})
    try:
        t.aql(f"FOR i IN 1..5 INSERT {{_key: TO_STRING(i)}} INTO {v}")
        t.aql(f"FOR x IN @e INSERT {{_from: CONCAT('{v}/', x[0]), _to: CONCAT('{v}/', x[1])}} INTO {e}", {"e": EDGES})
        r = t.aql(f"FOR n IN 1..2 OUTBOUND '{v}/1' {e} SORT TO_NUMBER(n._key) RETURN DISTINCT TO_NUMBER(n._key)")
        check(r == [2, 3, 5], f"got {r}")
    finally:
        for n in (e, v):
            t.arango(f"/_api/collection/{n}", method="DELETE")
    return "AQL FOR v IN 1..2 OUTBOUND"


def couchbase(t):
    b = t.c["bucket"]
    vals = ",".join(f"('ge{a}{b2}', {{'t':'ge','src':{a},'dst':{b2}}})" for a, b2 in EDGES)
    t.n1ql(f"UPSERT INTO `{b}` (KEY, VALUE) VALUES {vals}")
    try:
        r = t.n1ql(f"WITH RECURSIVE w AS (SELECT e.dst AS node, 1 AS depth FROM `{b}` e WHERE e.t = 'ge' AND e.src = 1 "
                   f"UNION SELECT e.dst AS node, w.depth + 1 AS depth FROM w JOIN `{b}` e ON e.src = w.node "
                   f"WHERE e.t = 'ge' AND w.depth < 2) SELECT DISTINCT RAW w.node FROM w ORDER BY w.node",
                   scan_consistency="request_plus")
        check(r == [2, 3, 5], f"got {r}")
    finally:
        t.n1ql(f"DELETE FROM `{b}` WHERE t = 'ge'")
    return "SQL++ WITH RECURSIVE"


PROBES = {"mysql": lambda t: sql(t.mysql, t), "sprig": lambda t: sql(t.mysql, t), "pg": lambda t: sql(t.psql, t),
          "mongo": mongo, "surreal": surreal, "arango": arango, "couchbase": couchbase}
if __name__ == "__main__":
    lib.run("graph_traversal", "graph", PROBES, "Graph traversal")
