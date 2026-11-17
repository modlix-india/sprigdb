#!/usr/bin/env python3
"""BEIR full-text relevance and speed: index a BEIR corpus (scifact,
nfcorpus), run the test queries, and record nDCG@10 against the qrels,
latency, index time and freshness lag (a new document's time to become
searchable). Records fts / complex / beir-<dataset>: ndcg_at_10, p50_ms,
p99_ms; fts / load: index_s; fts / feed: freshness_ms.
Speed workload: benchmark VMs only.

  fts/beir.py --engine opensearch|mongo|mysql --target HOST:PORT --product NAME \
      --dataset /data/beir/scifact [--user U --password P --db D]

opensearch: HTTP (standard analyzer + english stemmer on title and text);
mongo: an Atlas-style search index + $search (SprigDB); mysql: a FULLTEXT
index + MATCH ... AGAINST in natural language mode (SprigDB, MySQL, MariaDB).
Needs pymongo 4.10.1 or PyMySQL 1.1.1 in the venv for those engines.
"""
import argparse
import json
import math
import os
import statistics
import sys
import time
import urllib.request
import base64

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import result  # noqa: E402


def read_beir(d):
    corpus = {}
    for line in open(os.path.join(d, "corpus.jsonl"), encoding="utf-8"):
        j = json.loads(line)
        corpus[j["_id"]] = (j.get("title", ""), j.get("text", ""))
    queries = {}
    for line in open(os.path.join(d, "queries.jsonl"), encoding="utf-8"):
        j = json.loads(line)
        queries[j["_id"]] = j["text"]
    qrels = {}
    for i, line in enumerate(open(os.path.join(d, "qrels", "test.tsv"), encoding="utf-8")):
        if i == 0:
            continue
        q, doc, score = line.rstrip("\n").split("\t")
        qrels.setdefault(q, {})[doc] = int(score)
    return corpus, {q: queries[q] for q in qrels}, qrels


def ndcg10(ranked, rel):
    dcg = sum((2 ** rel.get(d, 0) - 1) / math.log2(i + 2) for i, d in enumerate(ranked[:10]))
    ideal = sorted(rel.values(), reverse=True)[:10]
    idcg = sum((2 ** r - 1) / math.log2(i + 2) for i, r in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


class OpenSearch:
    def __init__(self, a):
        self.base, self.auth = f"http://{a.target}", (a.user, a.password) if a.user else None

    def call(self, method, path, body=None, nd=False):
        data = None
        if body is not None:
            data = body.encode() if nd else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/x-ndjson" if nd else "application/json"})
        if self.auth:
            req.add_header("Authorization", "Basic " + base64.b64encode(f"{self.auth[0]}:{self.auth[1]}".encode()).decode())
        with urllib.request.urlopen(req, timeout=600) as r:
            return json.load(r)

    def load(self, corpus):
        try:
            self.call("DELETE", "/beir")
        except Exception:
            pass
        an = {"analysis": {"analyzer": {"en": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "porter_stem"]}}}}
        self.call("PUT", "/beir", {"settings": an, "mappings": {"properties": {
            "title": {"type": "text", "analyzer": "en"}, "text": {"type": "text", "analyzer": "en"}}}})
        items = list(corpus.items())
        for s in range(0, len(items), 2000):
            nd = "".join(json.dumps({"index": {"_id": k}}) + "\n" + json.dumps({"title": t, "text": x}) + "\n"
                         for k, (t, x) in items[s:s + 2000])
            self.call("POST", "/beir/_bulk", nd, nd=True)
        self.call("POST", "/beir/_refresh")

    def search(self, q):
        r = self.call("POST", "/beir/_search", {"size": 10, "_source": False,
                                                "query": {"multi_match": {"query": q, "fields": ["title", "text"]}}})
        return [h["_id"] for h in r["hits"]["hits"]]

    def add(self, k, text):
        self.call("PUT", f"/beir/_doc/{k}", {"title": "", "text": text})


class MongoSearch:
    def __init__(self, a):
        import pymongo
        cred = f"{a.user}:{a.password}@" if a.user else ""
        self.c = pymongo.MongoClient(f"mongodb://{cred}{a.target}/?authSource=admin&directConnection=true")[a.db or "beir"]["beir"]

    def load(self, corpus):
        self.c.drop()
        items = list(corpus.items())
        for s in range(0, len(items), 2000):
            self.c.insert_many([{"_id": k, "title": t, "text": x} for k, (t, x) in items[s:s + 2000]])
        self.c.database.command({"createSearchIndexes": "beir", "indexes": [{"name": "default", "definition": {
            "analyzer": "lucene.english", "mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "text": {"type": "string"}}}}}]})
        while not (ix := list(self.c.aggregate([{"$listSearchIndexes": {"name": "default"}}]))) or not ix[0].get("queryable"):
            time.sleep(2)

    def search(self, q):
        return [d["_id"] for d in self.c.aggregate([{"$search": {"text": {"query": q, "path": ["title", "text"]}}},
                                                     {"$limit": 10}, {"$project": {"_id": 1}}])]

    def add(self, k, text):
        self.c.insert_one({"_id": k, "title": "", "text": text})


class MySQL:
    def __init__(self, a):
        import pymysql
        h, p = a.target.rsplit(":", 1)
        self.c = pymysql.connect(host=h, port=int(p), user=a.user, password=a.password, database=a.db or "beir", autocommit=True)

    def load(self, corpus):
        cur = self.c.cursor()
        cur.execute("DROP TABLE IF EXISTS beir")
        cur.execute("CREATE TABLE beir (id VARCHAR(64) PRIMARY KEY, title TEXT, body MEDIUMTEXT, FULLTEXT KEY ft (title, body))")
        items = list(corpus.items())
        for s in range(0, len(items), 1000):
            cur.executemany("INSERT INTO beir VALUES (%s, %s, %s)", [(k, t, x) for k, (t, x) in items[s:s + 1000]])

    def search(self, q):
        cur = self.c.cursor()
        cur.execute("SELECT id FROM beir WHERE MATCH(title, body) AGAINST (%s IN NATURAL LANGUAGE MODE) "
                    "ORDER BY MATCH(title, body) AGAINST (%s IN NATURAL LANGUAGE MODE) DESC LIMIT 10", (q, q))
        return [r[0] for r in cur.fetchall()]

    def add(self, k, text):
        self.c.cursor().execute("INSERT INTO beir VALUES (%s, '', %s)", (k, text))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True, choices=["opensearch", "mongo", "mysql"])
    for k in ("target", "product", "dataset"):
        ap.add_argument("--" + k, required=True)
    for k in ("version", "competitor", "user", "password", "db", "run", "build", "out"):
        ap.add_argument("--" + k, default="")
    ap.add_argument("--no-load", action="store_true")
    a = ap.parse_args()
    corpus, queries, qrels = read_beir(a.dataset)
    eng = {"opensearch": OpenSearch, "mongo": MongoSearch, "mysql": MySQL}[a.engine](a)
    name = os.path.basename(a.dataset.rstrip("/"))
    common = {"model": "fts", "workload": f"beir-{name}", "product": a.product, "product_version": a.version,
              "competitor": a.competitor or a.product, "config": {"engine": a.engine, "docs": len(corpus), "queries": len(queries)},
              "command": " ".join(sys.argv), "source": f"fts/beir.py ({name})"}
    if a.build:
        common["build"] = a.build
    out = []
    if not a.no_load:
        t = time.time()
        eng.load(corpus)
        out.append(("load", "index_s", round(time.time() - t, 1), "s", "lower"))
    scores, lat = [], []
    for q, text in queries.items():
        t = time.perf_counter()
        ranked = eng.search(text)
        lat.append((time.perf_counter() - t) * 1000)
        scores.append(ndcg10(ranked, qrels[q]))
    lat.sort()
    out += [("complex", "ndcg_at_10", round(statistics.mean(scores), 4), "", "higher"),
            ("complex", "p50_ms", round(lat[len(lat) // 2], 2), "ms", "lower"),
            ("complex", "p99_ms", round(lat[max(0, int(len(lat) * 0.99) - 1)], 2), "ms", "lower")]
    token = f"zqfresh{int(time.time())}"
    t = time.perf_counter()
    eng.add(token, f"freshness probe {token}")
    while token not in eng.search(token):
        if time.perf_counter() - t > 120:
            break
        time.sleep(0.02)
    out.append(("feed", "freshness_ms", round((time.perf_counter() - t) * 1000, 1), "ms", "lower"))
    for op, m, v, unit, better in out:
        result.write({**common, "op": op, "metric": m, "value": v, "unit": unit, "better": better,
                      "run": f"beir-{name}-{m}-{a.run or 'run'}"}, a.out or None)
        print(f"{m}: {v} {unit}")


if __name__ == "__main__":
    main()
