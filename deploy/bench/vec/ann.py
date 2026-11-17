#!/usr/bin/env python3
"""ann-benchmarks-style vector search: load an ann-benchmarks HDF5 dataset
(sift-128-euclidean, glove-100-angular), build the product's vector index,
run the test queries, and record recall@10 against the dataset's ground truth,
queries per second (--threads clients), p50/p99 latency and build time.
Records vec / complex / ann-<dataset>: recall_at_10, qps, p50_ms, p99_ms,
build_s, variant ef=<n> (one point per search-effort setting).
Speed workload: benchmark VMs only.

  vec/ann.py --engine pgvector|qdrant|mongo --target HOST:PORT --product NAME \
      --dataset /data/sift-128-euclidean.hdf5 [--user U --password P --db D] \
      [--efs 16,32,64,128,256] [--threads 8] [--limit-queries 10000] [--no-load]

Needs a venv with h5py 3.12.1 and numpy 2.1.3, plus psycopg[binary] 3.2.3
(pgvector) or pymongo 4.10.1 (SprigDB, MongoDB); Qdrant goes over HTTP.
"""
import argparse
import concurrent.futures as cf
import json
import os
import statistics
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import result  # noqa: E402


class PgVector:
    def __init__(self, a, dim, metric):
        import psycopg
        h, p = a.target.rsplit(":", 1)
        self.dsn = f"host={h} port={p} user={a.user} password={a.password} dbname={a.db or 'postgres'}"
        self.conn = psycopg.connect(self.dsn, autocommit=True)
        self.op = "<->" if metric == "euclidean" else "<=>"
        self.ops = "vector_l2_ops" if metric == "euclidean" else "vector_cosine_ops"
        self.dim = dim

    def load(self, train):
        c = self.conn
        c.execute("CREATE EXTENSION IF NOT EXISTS vector; DROP TABLE IF EXISTS ann")
        c.execute(f"CREATE TABLE ann (id int PRIMARY KEY, v vector({self.dim}))")
        with c.cursor().copy("COPY ann (id, v) FROM STDIN") as cp:
            for i, v in enumerate(train):
                cp.write_row((i, "[" + ",".join(map(str, v)) + "]"))
        t = time.time()
        c.execute(f"CREATE INDEX ON ann USING hnsw (v {self.ops}) WITH (m = 16, ef_construction = 200)")
        return time.time() - t

    def searcher(self, ef):
        import psycopg
        conn = psycopg.connect(self.dsn, autocommit=True)
        conn.execute(f"SET hnsw.ef_search = {ef}")

        def q(v):
            vec = "[" + ",".join(map(str, v)) + "]"
            return [r[0] for r in conn.execute(f"SELECT id FROM ann ORDER BY v {self.op} %s::vector LIMIT 10", (vec,))]
        return q


class Qdrant:
    def __init__(self, a, dim, metric):
        self.base = f"http://{a.target}"
        self.dim, self.dist = dim, "Euclid" if metric == "euclidean" else "Cosine"

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.base + path, data=json.dumps(body).encode() if body is not None else None,
                                     method=method, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=600) as r:
            return json.load(r)

    def load(self, train):
        try:
            self.call("DELETE", "/collections/ann")
        except Exception:
            pass
        self.call("PUT", "/collections/ann", {"vectors": {"size": self.dim, "distance": self.dist},
                                              "hnsw_config": {"m": 16, "ef_construct": 200}})
        t = time.time()
        for s in range(0, len(train), 1000):
            pts = [{"id": i, "vector": list(map(float, train[i]))} for i in range(s, min(s + 1000, len(train)))]
            self.call("PUT", "/collections/ann/points?wait=true", {"points": pts})
        while self.call("GET", "/collections/ann")["result"]["status"] != "green":
            time.sleep(2)
        return time.time() - t

    def searcher(self, ef):
        def q(v):
            r = self.call("POST", "/collections/ann/points/search",
                          {"vector": list(map(float, v)), "limit": 10, "params": {"hnsw_ef": ef}})
            return [p["id"] for p in r["result"]]
        return q


class Mongo:
    """SprigDB or MongoDB Atlas-style: createSearchIndexes (vectorSearch) + $vectorSearch."""

    def __init__(self, a, dim, metric):
        import pymongo
        cred = f"{a.user}:{a.password}@" if a.user else ""
        self.cli = pymongo.MongoClient(f"mongodb://{cred}{a.target}/?authSource=admin&directConnection=true")
        self.coll = self.cli[a.db or "ann"]["ann"]
        self.dim, self.sim = dim, "euclidean" if metric == "euclidean" else "cosine"

    def load(self, train):
        self.coll.drop()
        for s in range(0, len(train), 1000):
            self.coll.insert_many([{"_id": i, "v": list(map(float, train[i]))} for i in range(s, min(s + 1000, len(train)))])
        t = time.time()
        self.coll.database.command({"createSearchIndexes": "ann", "indexes": [{"name": "vx", "type": "vectorSearch",
                                    "definition": {"fields": [{"type": "vector", "path": "v", "numDimensions": self.dim,
                                                               "similarity": self.sim}]}}]})
        while True:
            ix = list(self.coll.aggregate([{"$listSearchIndexes": {"name": "vx"}}]))
            if ix and ix[0].get("queryable"):
                break
            time.sleep(2)
        return time.time() - t

    def searcher(self, ef):
        def q(v):
            return [d["_id"] for d in self.coll.aggregate([{"$vectorSearch": {"index": "vx", "path": "v",
                    "queryVector": list(map(float, v)), "numCandidates": ef, "limit": 10}}, {"$project": {"_id": 1}}])]
        return q


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True, choices=["pgvector", "qdrant", "mongo"])
    for k in ("target", "product", "dataset"):
        ap.add_argument("--" + k, required=True)
    for k in ("version", "competitor", "user", "password", "db", "run", "build", "out"):
        ap.add_argument("--" + k, default="")
    ap.add_argument("--efs", default="16,32,64,128,256")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--limit-queries", type=int, default=10000)
    ap.add_argument("--no-load", action="store_true")
    a = ap.parse_args()
    import h5py
    f = h5py.File(a.dataset, "r")
    train, test, truth = f["train"], f["test"][: a.limit_queries], f["neighbors"][: a.limit_queries]
    name = os.path.basename(a.dataset).rsplit(".", 1)[0]
    metric = "euclidean" if "euclidean" in name else "angular"
    eng = {"pgvector": PgVector, "qdrant": Qdrant, "mongo": Mongo}[a.engine](a, train.shape[1], metric)
    common = {"model": "vec", "workload": f"ann-{name}", "product": a.product, "product_version": a.version,
              "competitor": a.competitor or a.product, "config": {"engine": a.engine, "threads": a.threads,
              "queries": len(test), "index": "HNSW m=16 ef_construction=200 where the engine takes them"},
              "command": " ".join(sys.argv), "source": f"vec/ann.py ({name})"}
    if a.build:
        common["build"] = a.build
    if not a.no_load:
        build_s = eng.load(train[:])
        result.write({**common, "op": "load", "metric": "build_s", "value": round(build_s, 1), "unit": "s",
                      "better": "lower", "run": f"ann-{name}-build-{a.run or 'run'}"}, a.out or None)
    for ef in map(int, a.efs.split(",")):
        q = eng.searcher(ef)
        lat, hits = [], 0

        def one(i):
            t = time.perf_counter()
            ids = q(test[i])
            return time.perf_counter() - t, len(set(ids[:10]) & set(truth[i][:10].tolist()))
        t0 = time.perf_counter()
        with cf.ThreadPoolExecutor(a.threads) as ex:
            for dt, h in ex.map(one, range(len(test))):
                lat.append(dt * 1000)
                hits += h
        wall = time.perf_counter() - t0
        lat.sort()
        vals = {"recall_at_10": (round(hits / (10 * len(test)), 4), "", "higher"),
                "qps": (round(len(test) / wall, 1), "queries/s", "higher"),
                "p50_ms": (round(lat[len(lat) // 2], 2), "ms", "lower"),
                "p99_ms": (round(lat[int(len(lat) * 0.99) - 1], 2), "ms", "lower")}
        for m, (v, unit, better) in vals.items():
            result.write({**common, "op": "complex", "variant": f"ef={ef}", "metric": m, "value": v, "unit": unit,
                          "better": better, "run": f"ann-{name}-ef{ef}-{m}-{a.run or 'run'}"}, a.out or None)
        print(f"ef={ef}: recall@10 {vals['recall_at_10'][0]}, {vals['qps'][0]} q/s, p99 {vals['p99_ms'][0]} ms")


if __name__ == "__main__":
    main()
