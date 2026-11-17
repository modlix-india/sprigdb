#!/usr/bin/env python3
"""Feature probe: a multi-document (multi-row) transaction. Two documents
written in one transaction and rolled back leave nothing; written and
committed, both are there."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib  # noqa: E402
from lib import check


def sql(run, t, begin="START TRANSACTION"):
    n = f"tx{t.stamp}"
    run(f"CREATE TABLE {n} (id INT PRIMARY KEY, v INT)")
    try:
        r = run(f"{begin}; INSERT INTO {n} VALUES (1,1); INSERT INTO {n} VALUES (2,2); ROLLBACK; SELECT COUNT(*) FROM {n}")
        check(r[-1] == ["0"], f"after rollback {r}")
        r = run(f"{begin}; INSERT INTO {n} VALUES (1,1); INSERT INTO {n} VALUES (2,2); COMMIT; SELECT COUNT(*) FROM {n}")
        check(r[-1] == ["2"], f"after commit {r}")
    finally:
        run(f"DROP TABLE {n}")
    return "two rows: rolled back 0, committed 2"


def mongo(t):
    n = f"tx{t.stamp}"
    out = t.mongo(f"""db.createCollection('{n}');
let s=db.getMongo().startSession(); let c=s.getDatabase(db.getName()).getCollection('{n}');
s.startTransaction(); c.insertOne({{_id:1}}); c.insertOne({{_id:2}}); s.abortTransaction(); const a=db.{n}.countDocuments({{}});
s.startTransaction(); c.insertOne({{_id:1}}); c.insertOne({{_id:2}}); s.commitTransaction(); const b=db.{n}.countDocuments({{}});
db.{n}.drop(); print(a+','+b);""")
    check(out.splitlines()[-1] == "0,2", out)
    return "session transaction: aborted 0, committed 2"


def surreal(t):
    t.surreal("DEFINE TABLE txd;")
    t.surreal("BEGIN TRANSACTION; CREATE txd:1; CREATE txd:2; CANCEL TRANSACTION;", allow_err=True)
    r = t.surreal("SELECT count() FROM txd GROUP ALL;")
    check(r[-1] in ([], [{"count": 0}]), f"after cancel {r[-1]}")
    r = t.surreal("BEGIN TRANSACTION; CREATE txd:1; CREATE txd:2; COMMIT TRANSACTION; SELECT count() FROM txd GROUP ALL; REMOVE TABLE txd;")
    check(r[-2] == [{"count": 2}], f"after commit {r[-2]}")
    return "BEGIN/CANCEL/COMMIT TRANSACTION: 0 then 2"


def arango(t):
    n = f"tx{t.stamp}"
    t.arango("/_api/collection", {"name": n})
    try:
        fail = t.http("POST", f"http://{t.c['http']}/_db/{t.c.get('db', '_system')}/_api/transaction",
                      {"collections": {"write": [n]}, "action": f"function(){{var db=require('@arangodb').db; db.{n}.insert({{_key:'1'}}); db.{n}.insert({{_key:'2'}}); throw 'abort';}}"},
                      {"Content-Type": "application/json"}, (t.c["user"], t.c["password"]))
        a = t.aql(f"RETURN LENGTH({n})")[0]
        check(a == 0, f"after abort {a} ({fail[0]})")
        t.arango("/_api/transaction", {"collections": {"write": [n]}, "action": f"function(){{var db=require('@arangodb').db; db.{n}.insert({{_key:'1'}}); db.{n}.insert({{_key:'2'}});}}"})
        b = t.aql(f"RETURN LENGTH({n})")[0]
        check(b == 2, f"after commit {b}")
    finally:
        t.arango(f"/_api/collection/{n}", method="DELETE")
    return "JavaScript transaction: aborted 0, committed 2"


def couchbase(t):
    b, k = t.c["bucket"], t.stamp
    tx = t.n1ql("BEGIN WORK", tximplicit=False)[0]["txid"]
    t.n1ql(f"INSERT INTO `{b}` (KEY, VALUE) VALUES ('tx{k}a', {{'v':1}})", txid=tx)
    t.n1ql(f"INSERT INTO `{b}` (KEY, VALUE) VALUES ('tx{k}b', {{'v':2}})", txid=tx)
    t.n1ql("ROLLBACK WORK", txid=tx)
    a = t.n1ql(f"SELECT RAW COUNT(*) FROM `{b}` USE KEYS ['tx{k}a','tx{k}b']")[0]
    check(a == 0, f"after rollback {a}")
    tx = t.n1ql("BEGIN WORK")[0]["txid"]
    t.n1ql(f"INSERT INTO `{b}` (KEY, VALUE) VALUES ('tx{k}a', {{'v':1}})", txid=tx)
    t.n1ql(f"INSERT INTO `{b}` (KEY, VALUE) VALUES ('tx{k}b', {{'v':2}})", txid=tx)
    t.n1ql("COMMIT WORK", txid=tx)
    c = t.n1ql(f"SELECT RAW COUNT(*) FROM `{b}` USE KEYS ['tx{k}a','tx{k}b']")[0]
    t.n1ql(f"DELETE FROM `{b}` USE KEYS ['tx{k}a','tx{k}b']")
    check(c == 2, f"after commit {c}")
    return "SQL++ BEGIN WORK / ROLLBACK WORK / COMMIT WORK: 0 then 2"


PROBES = {
    "mysql": lambda t: sql(t.mysql, t), "pg": lambda t: sql(t.psql, t, "BEGIN"),
    "mongo": mongo, "sprig": mongo, "surreal": surreal, "arango": arango, "couchbase": couchbase,
}
if __name__ == "__main__":
    lib.run("multi_doc_tx", "doc", PROBES, "Multi-document transactions")
