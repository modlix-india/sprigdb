#!/usr/bin/env python3
"""Feature probe: a change feed. A client reads, in order, the change a write
made, through the product's change interface (change streams, logical
decoding, binary log, changefeed, WAL tailing)."""
import time

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import NotProbed, check


def mongo(t):
    n = f"cf{t.stamp}"
    out = t.mongo(f"""db.createCollection('{n}'); const cs=db.{n}.watch(); db.{n}.insertOne({{_id:1,v:'x'}});
let ev=null; for (let i=0;i<50 && !ev;i++) {{ ev=cs.tryNext(); if(!ev) sleep(200); }}
cs.close(); db.{n}.drop(); print(ev ? ev.operationType+':'+ev.documentKey._id : 'none');""")
    check(out.splitlines()[-1] == "insert:1", out)
    return "change stream delivered the insert"


def pg(t):
    n, slot = f"cf{t.stamp}", f"bench_slot_{t.stamp}"
    t.psql(f"CREATE TABLE {n} (id INT PRIMARY KEY)")
    t.psql(f"SELECT pg_create_logical_replication_slot('{slot}', 'test_decoding')")
    try:
        t.psql(f"INSERT INTO {n} VALUES (1)")
        r = t.psql(f"SELECT data FROM pg_logical_slot_get_changes('{slot}', NULL, NULL)")
        check(any("INSERT" in row[0] and n in row[0] for row in r), f"changes {r}")
    finally:
        t.psql(f"SELECT pg_drop_replication_slot('{slot}')")
        t.psql(f"DROP TABLE {n}")
    return "logical decoding slot (test_decoding) returned the insert"


def mysql(t):
    n = f"cf{t.stamp}"
    t.mysql(f"CREATE TABLE {n} (id INT PRIMARY KEY)")
    try:
        f, pos = t.mysql("SHOW BINARY LOG STATUS")[0][:2]
        t.mysql(f"INSERT INTO {n} VALUES (1)")
        ev = t.mysql(f"SHOW BINLOG EVENTS IN '{f}' FROM {pos}")
        check(any("Write_rows" in " ".join(e) for e in ev), f"events {ev[:5]}")
    finally:
        t.mysql(f"DROP TABLE {n}")
    return "row-based binary log holds the insert (read by replication-protocol clients)"


def surreal(t):
    t.surreal("DEFINE TABLE cfd CHANGEFEED 1h; CREATE cfd:1 SET v = 'x';")
    try:
        r = []
        for _ in range(15):
            r = t.surreal("SHOW CHANGES FOR TABLE cfd SINCE 0 LIMIT 10;")[0]
            if r:
                break
            time.sleep(1)
        check(r and "cfd:1" in str(r), f"changes {str(r)[:200]}")
    finally:
        t.surreal("REMOVE TABLE cfd;")
    return "table CHANGEFEED + SHOW CHANGES returned the create"


def arango(t):
    n = f"cf{t.stamp}"
    base = f"http://{t.c['http']}"
    auth = (t.c["user"], t.c["password"])
    t.arango("/_api/collection", {"name": n})
    try:
        st, last = t.http("GET", f"{base}/_api/wal/lastTick", auth=auth)
        check(st == 200, f"lastTick {st} {last}")
        t.aql(f"INSERT {{_key:'1'}} INTO {n}")
        body = ""
        for _ in range(10):
            st, body = t.http("GET", f"{base}/_api/wal/tail?from={last['tick']}", auth=auth)
            if "2300" in str(body) and n in str(body):
                break
            time.sleep(0.5)
        check("2300" in str(body) and n in str(body), f"tail {st} {str(body)[:200]}")
    finally:
        t.arango(f"/_api/collection/{n}", method="DELETE")
    return "WAL tailing API returned the insert (marker 2300)"


def couchbase(t):
    raise NotProbed("Couchbase's change feed is DCP, a binary protocol read through an SDK or connector; not installed")


PROBES = {"mongo": mongo, "sprig": mongo, "pg": pg, "mysql": mysql, "surreal": surreal, "arango": arango,
          "couchbase": couchbase}
if __name__ == "__main__":
    lib.run("change_feed", "multi", PROBES, "Change feeds")
