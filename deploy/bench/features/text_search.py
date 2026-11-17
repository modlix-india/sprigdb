#!/usr/bin/env python3
"""Feature probe: full-text search. Three documents; a search for 'foxes'
(stemmed) through the product's text index must return documents 1 and 3."""
import time

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import NotProbed, check

DOCS = {1: "the quick brown fox jumps", 2: "lazy dogs sleep all day", 3: "a fox and a hound"}


def sqlmy(t):
    n = f"ts{t.stamp}"
    vals = ",".join(f"({i},'{s}')" for i, s in DOCS.items())
    t.mysql(f"CREATE TABLE {n} (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body)); INSERT INTO {n} VALUES {vals}")
    try:
        r = []
        for _ in range(20):
            r = t.mysql(f"SELECT id FROM {n} WHERE MATCH(body) AGAINST ('fox' IN NATURAL LANGUAGE MODE) ORDER BY id")
            if r == [["1"], ["3"]]:
                break
            time.sleep(1)
        check(r == [["1"], ["3"]], f"got {r}")
    finally:
        t.mysql(f"DROP TABLE {n}")
    return "FULLTEXT index + MATCH ... AGAINST"


def pg(t):
    n = f"ts{t.stamp}"
    vals = ",".join(f"({i},'{s}')" for i, s in DOCS.items())
    t.psql(f"CREATE TABLE {n} (id INT PRIMARY KEY, body TEXT); INSERT INTO {n} VALUES {vals};"
           f"CREATE INDEX ON {n} USING gin (to_tsvector('english', body))")
    try:
        r = t.psql(f"SELECT id FROM {n} WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'foxes') ORDER BY id")
        check(r == [["1"], ["3"]], f"got {r}")
    finally:
        t.psql(f"DROP TABLE {n}")
    return "GIN index on to_tsvector + @@ plainto_tsquery (stemmed)"


def mongo(t):
    n = f"ts{t.stamp}"
    docs = ",".join(f"{{_id:{i},body:'{s}'}}" for i, s in DOCS.items())
    out = t.mongo(f"""db.{n}.insertMany([{docs}]); db.{n}.createIndex({{body:'text'}});
let r=[]; for (let i=0;i<20;i++) {{ r=db.{n}.find({{$text:{{$search:'foxes'}}}}).sort({{_id:1}}).toArray().map(d=>d._id); if (r.length==2) break; sleep(500); }}
db.{n}.drop(); print(JSON.stringify(r));""")
    check(out.splitlines()[-1] == "[1,3]", out)
    return "text index + $text (stemmed)"


def surreal(t):
    sets = " ".join(f"CREATE td:{i} SET body = '{s}';" for i, s in DOCS.items())
    t.surreal("DEFINE ANALYZER an TOKENIZERS blank, class FILTERS lowercase, snowball(english);"
              f"DEFINE TABLE td; DEFINE INDEX ti ON td FIELDS body SEARCH ANALYZER an BM25; {sets}")
    try:
        r = t.surreal("SELECT VALUE meta::id(id) FROM td WHERE body @@ 'foxes';")[0]
        check(sorted(r) == [1, 3], f"got {r}")
    finally:
        t.surreal("REMOVE TABLE td; REMOVE ANALYZER an;")
    return "SEARCH index (BM25, snowball) + @@"


def arango(t):
    n, v = f"ts{t.stamp}", f"tv{t.stamp}"
    t.arango("/_api/collection", {"name": n})
    try:
        t.aql(f"FOR d IN @docs INSERT d INTO {n}", {"docs": [{"_key": str(i), "body": s} for i, s in DOCS.items()]})
        t.arango("/_api/view", {"name": v, "type": "arangosearch",
                                "links": {n: {"analyzers": ["text_en"], "fields": {"body": {}}}}})
        r = t.aql(f"FOR d IN {v} SEARCH ANALYZER(d.body IN TOKENS('foxes', 'text_en'), 'text_en') "
                  f"OPTIONS {{waitForSync: true}} SORT TO_NUMBER(d._key) RETURN TO_NUMBER(d._key)")
        check(r == [1, 3], f"got {r}")
    finally:
        t.arango(f"/_api/view/{v}", method="DELETE")
        t.arango(f"/_api/collection/{n}", method="DELETE")
    return "ArangoSearch view with text_en + SEARCH"


def couchbase(t):
    if not t.c.get("search"):
        raise NotProbed(t.c.get("search_off", "no Search service endpoint configured"))
    b, h = t.c["bucket"], t.c["search"]
    auth = (t.c["user"], t.c["password"])
    for i, s in DOCS.items():
        t.n1ql(f"UPSERT INTO `{b}` (KEY, VALUE) VALUES ('ts{i}', {{'t':'ts','body':'{s}'}})")
    name = f"ts{t.stamp}"
    defn = {"type": "fulltext-index", "name": name, "sourceType": "gocbcore", "sourceName": b,
            "params": {"mapping": {"default_analyzer": "en", "default_mapping": {"enabled": True, "dynamic": True}}}}
    st, res = t.http("PUT", f"http://{h}/api/index/{name}", defn, {"Content-Type": "application/json"}, auth)
    try:
        check(st < 400, f"index HTTP {st} {str(res)[:200]}")
        ids = []
        for _ in range(30):
            st, res = t.http("POST", f"http://{h}/api/index/{name}/query",
                             {"query": {"match": "foxes", "field": "body", "analyzer": "en"}, "size": 10},
                             {"Content-Type": "application/json"}, auth)
            ids = sorted(x["id"] for x in (res.get("hits") or [])) if isinstance(res, dict) else []
            if len(ids) >= 2:
                break
            time.sleep(1)
        check(ids == ["ts1", "ts3"], f"got {ids}")
    finally:
        t.http("DELETE", f"http://{h}/api/index/{name}", auth=auth)
        t.n1ql(f"DELETE FROM `{b}` WHERE t = 'ts'")
    return "Search service index (en analyzer) + match query"


PROBES = {"mysql": sqlmy, "sprig": sqlmy, "pg": pg, "mongo": mongo, "surreal": surreal, "arango": arango,
          "couchbase": couchbase}
if __name__ == "__main__":
    lib.run("text_search", "fts", PROBES, "Full-text search")
