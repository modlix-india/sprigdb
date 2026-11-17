#!/usr/bin/env python3
"""Feature probe: vector search. Four 3-dimensional vectors; a nearest-neighbour
query for [1, 0.05, 0] through the product's vector index must return
document 1 first and 4 second."""
import json
import time

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import Fail, NotOffered, NotProbed, check

VECS = {1: [1, 0, 0], 2: [0, 1, 0], 3: [0, 0, 1], 4: [0.9, 0.1, 0]}
Q = [1, 0.05, 0]


def pg(t):
    n = f"vs{t.stamp}"
    vals = ",".join(f"({i},'{json.dumps(v)}')" for i, v in VECS.items())
    t.psql(f"CREATE EXTENSION IF NOT EXISTS vector; CREATE TABLE {n} (id INT PRIMARY KEY, e vector(3));"
           f"INSERT INTO {n} VALUES {vals}; CREATE INDEX ON {n} USING hnsw (e vector_l2_ops)")
    try:
        r = t.psql(f"SET enable_seqscan = off; SELECT id FROM {n} ORDER BY e <-> '{json.dumps(Q)}' LIMIT 2")
        r = [x[0] for x in r if x[0] != "SET"]
        check(r == ["1", "4"], f"got {r}")
    finally:
        t.psql(f"DROP TABLE {n}")
    return "pgvector: HNSW index, ORDER BY e <-> q"


def mysql(t):
    try:
        t.mysql(f"CREATE TABLE vs{t.stamp} (id INT PRIMARY KEY, e VECTOR(3))")
    except Fail as e:
        raise NotOffered(f"no VECTOR type: {e}")
    t.mysql(f"DROP TABLE vs{t.stamp}")
    raise Fail("VECTOR column accepted, but no distance search in this edition (not probed further)")


def mongo_vs(t):
    n = f"vs{t.stamp}"
    docs = ",".join(f"{{_id:{i},e:{json.dumps(v)}}}" for i, v in VECS.items())
    js = f"""db.{n}.insertMany([{docs}]);
let r; try {{ db.runCommand({{createSearchIndexes:'{n}', indexes:[{{name:'vx', type:'vectorSearch', definition:{{fields:[{{type:'vector', path:'e', numDimensions:3, similarity:'euclidean'}}]}}}}]}}); }}
catch (e) {{ db.{n}.drop(); print('NOINDEX '+e.message); quit(); }}
let ids=[]; for (let i=0;i<60;i++) {{ try {{ ids=db.{n}.aggregate([{{$vectorSearch:{{index:'vx', path:'e', queryVector:{json.dumps(Q)}, numCandidates:10, limit:2}}}}]).toArray().map(d=>d._id); }} catch(e) {{ ids=['ERR '+e.message]; }} if (ids.length==2 && typeof ids[0]=='number') break; sleep(500); }}
db.{n}.drop(); print(JSON.stringify(ids));"""
    out = t.mongo(js).splitlines()[-1]
    if out.startswith("NOINDEX"):
        raise NotOffered(f"createSearchIndexes refused: {out[8:200]}")
    check(out == "[1,4]", f"got {out}")
    return "createSearchIndexes (vectorSearch) + $vectorSearch"


def surreal(t):
    sets = " ".join(f"CREATE vd:{i} SET e = {json.dumps(v)};" for i, v in VECS.items())
    t.surreal(f"DEFINE TABLE vd; DEFINE INDEX vi ON vd FIELDS e HNSW DIMENSION 3 DIST EUCLIDEAN; {sets}")
    try:
        r = t.surreal(f"SELECT meta::id(id) AS i, vector::distance::knn() AS d FROM vd WHERE e <|2,40|> {json.dumps(Q)} ORDER BY d;")[0]
        r = [x["i"] for x in r]
        check(r == [1, 4], f"got {r}")
    finally:
        t.surreal("REMOVE TABLE vd;")
    return "HNSW index, KNN operator <|2,40|>"


def arango(t):
    n = f"vs{t.stamp}"
    t.arango("/_api/collection", {"name": n})
    try:
        t.aql(f"FOR d IN @docs INSERT d INTO {n}", {"docs": [{"_key": str(i), "e": v} for i, v in VECS.items()]})
        try:
            t.arango(f"/_api/index?collection={n}", {"type": "vector", "fields": ["e"],
                                                     "params": {"metric": "l2", "dimension": 3, "nLists": 1}})
        except Fail as e:
            raise NotOffered(f"vector index refused: {e}")
        r = t.aql(f"FOR d IN {n} SORT APPROX_NEAR_L2(d.e, @q) LIMIT 2 RETURN TO_NUMBER(d._key)", {"q": Q})
        check(r == [1, 4], f"got {r}")
    finally:
        t.arango(f"/_api/collection/{n}", method="DELETE")
    return "vector index (experimental flag) + APPROX_NEAR_L2"


def couchbase(t):
    if not t.c.get("search"):
        raise NotProbed(t.c.get("search_off", "no Search service endpoint configured"))
    b = t.c["bucket"]
    for i, v in VECS.items():
        t.n1ql(f"UPSERT INTO `{b}` (KEY, VALUE) VALUES ('vs{i}', {{'t':'vs','e':{json.dumps(v)}}})")
    name = f"vs{t.stamp}"
    defn = {"type": "fulltext-index", "name": name, "sourceType": "gocbcore", "sourceName": b,
            "params": {"mapping": {"default_mapping": {"enabled": True, "dynamic": False, "properties": {
                "e": {"enabled": True, "fields": [{"name": "e", "type": "vector", "dims": 3, "similarity": "l2_norm", "index": True}]}}}},
                "doc_config": {"mode": "type_field", "type_field": "type"}, "store": {"indexType": "scorch"}}}
    h = t.c["search"]
    st, res = t.http("PUT", f"http://{h}/api/index/{name}", defn, {"Content-Type": "application/json"},
                     (t.c["user"], t.c["password"]))
    try:
        if st >= 400:
            raise NotOffered(f"search index with a vector field refused: HTTP {st} {str(res)[:200]}")
        ids = []
        for _ in range(30):
            st, res = t.http("POST", f"http://{h}/api/index/{name}/query",
                             {"query": {"match_none": {}}, "knn": [{"field": "e", "vector": Q, "k": 2}], "size": 2},
                             {"Content-Type": "application/json"}, (t.c["user"], t.c["password"]))
            ids = [x["id"] for x in (res.get("hits") or [])] if isinstance(res, dict) else []
            if len(ids) == 2:
                break
            time.sleep(1)
        check(ids == ["vs1", "vs4"], f"got {ids} {str(res)[:200]}")
    finally:
        t.http("DELETE", f"http://{h}/api/index/{name}", auth=(t.c["user"], t.c["password"]))
        t.n1ql(f"DELETE FROM `{b}` WHERE t = 'vs'")
    return "Search index with a vector field + knn query"


PROBES = {"pg": pg, "mysql": mysql, "mongo": mongo_vs, "sprig": mongo_vs, "surreal": surreal, "arango": arango,
          "couchbase": couchbase}
if __name__ == "__main__":
    lib.run("vector_search", "vec", PROBES, "Vector search")
