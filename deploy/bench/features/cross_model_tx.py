#!/usr/bin/env python3
"""Feature probe: one transaction across two data models. Writes to two
different models (SprigDB: a table and an event stream; SurrealDB: a document
and a graph edge; ArangoDB: a document and an edge collection) are rolled back
together, then committed together. Products with one data model (MySQL,
PostgreSQL, MongoDB: JSON columns and documents are one model) have nothing to
cross."""
import time

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import NotOffered, NotProbed, check


def sprig(t):
    tb, st = f"xm{t.stamp}", f"xms{t.stamp}"
    t.mysql(f"CREATE TABLE {tb} (id INT PRIMARY KEY); CREATE STREAM {st} PARTITIONS 1")
    try:
        t.mysql(f"START TRANSACTION; INSERT INTO {tb} VALUES (1); INSERT INTO {st} (`key`, value) VALUES ('k', 'v'); ROLLBACK")
        time.sleep(3)
        a = t.mysql(f"SELECT (SELECT COUNT(*) FROM {tb}), (SELECT COUNT(*) FROM {st})")
        check(a == [["0", "0"]], f"after rollback table, stream = {a}")
        t.mysql(f"START TRANSACTION; INSERT INTO {tb} VALUES (1); INSERT INTO {st} (`key`, value) VALUES ('k', 'v'); COMMIT")
        b = []
        for _ in range(20):
            time.sleep(1)
            b = t.mysql(f"SELECT (SELECT COUNT(*) FROM {tb}), (SELECT COUNT(*) FROM {st})")
            if b == [["1", "1"]]:
                break
        check(b == [["1", "1"]], f"after commit table, stream = {b}")
    finally:
        t.mysql(f"DROP STREAM {st}; DROP TABLE {tb}")
    return "SQL transaction over a table and an event stream: rolled back together, committed together"


def surreal(t):
    t.surreal("DEFINE TABLE xp; DEFINE TABLE xk;")
    t.surreal("BEGIN TRANSACTION; CREATE xp:1; CREATE xp:2; RELATE xp:1->xk->xp:2; CANCEL TRANSACTION;", allow_err=True)
    r = t.surreal("SELECT count() FROM xp GROUP ALL; SELECT count() FROM xk GROUP ALL;")
    check(all(x in ([], [{"count": 0}]) for x in r), f"after cancel {r}")
    t.surreal("BEGIN TRANSACTION; CREATE xp:1; CREATE xp:2; RELATE xp:1->xk->xp:2; COMMIT TRANSACTION;")
    r = t.surreal("SELECT count() FROM xp GROUP ALL; SELECT count() FROM xk GROUP ALL; SELECT VALUE ->xk->xp FROM xp:1; REMOVE TABLE xp; REMOVE TABLE xk;")
    check(r[0] == [{"count": 2}] and r[1] == [{"count": 1}], f"after commit {r[:3]}")
    return "documents and a RELATE graph edge in one transaction"


def arango(t):
    v, e = f"xv{t.stamp}", f"xe{t.stamp}"
    t.arango("/_api/collection", {"name": v})
    t.arango("/_api/collection", {"name": e, "type": 3})
    try:
        act = (f"function(){{var db=require('@arangodb').db; db.{v}.insert({{_key:'a'}}); db.{v}.insert({{_key:'b'}});"
               f" db.{e}.insert({{_from:'{v}/a',_to:'{v}/b'}}); %s}}")
        t.http("POST", f"http://{t.c['http']}/_db/{t.c.get('db', '_system')}/_api/transaction",
               {"collections": {"write": [v, e]}, "action": act % "throw 'abort';"},
               {"Content-Type": "application/json"}, (t.c["user"], t.c["password"]))
        a = t.aql(f"RETURN [LENGTH({v}), LENGTH({e})]")[0]
        check(a == [0, 0], f"after abort {a}")
        t.arango("/_api/transaction", {"collections": {"write": [v, e]}, "action": act % ""})
        b = t.aql(f"RETURN [LENGTH({v}), LENGTH({e})]")[0]
        check(b == [2, 1], f"after commit {b}")
    finally:
        for n in (e, v):
            t.arango(f"/_api/collection/{n}", method="DELETE")
    return "document and edge collections in one JavaScript transaction"


def single(name):
    def f(t):
        raise NotOffered(f"{name} has one data model; nothing to cross in a transaction")
    return f


def couchbase(t):
    raise NotProbed("Couchbase writes KV and SQL++ in one transaction only through an SDK's transactions library; not installed")


PROBES = {"sprig": sprig, "surreal": surreal, "arango": arango, "couchbase": couchbase,
          "mysql": single("MySQL"), "pg": single("PostgreSQL"), "mongo": single("MongoDB")}
if __name__ == "__main__":
    lib.run("cross_model_tx", "multi", PROBES, "Cross-model transactions")
