#!/usr/bin/env python3
"""Feature probe: a join. Two tables (collections) of customers and orders,
one query that returns each order with its customer's name, matched by value."""
import lib
from lib import check

WANT = [["a", "10"], ["a", "11"], ["b", "20"]]


def sql(run, t):
    c, o = f"jc{t.stamp}", f"jo{t.stamp}"
    run(f"CREATE TABLE {c} (id INT PRIMARY KEY, name VARCHAR(10)); CREATE TABLE {o} (id INT PRIMARY KEY, cust INT, total INT);"
        f"INSERT INTO {c} VALUES (1,'a'),(2,'b'); INSERT INTO {o} VALUES (1,1,10),(2,1,11),(3,2,20);")
    try:
        rows = run(f"SELECT c.name, o.total FROM {o} o JOIN {c} c ON c.id = o.cust ORDER BY o.total")
        check(rows == WANT, f"got {rows}")
    finally:
        run(f"DROP TABLE {o}; DROP TABLE {c}")
    return "JOIN ... ON returns the 3 matched rows"


def mongo(t):
    out = t.mongo(f"""const c=db.jc{t.stamp}, o=db.jo{t.stamp};
c.insertMany([{{_id:1,name:'a'}},{{_id:2,name:'b'}}]); o.insertMany([{{_id:1,cust:1,total:10}},{{_id:2,cust:1,total:11}},{{_id:3,cust:2,total:20}}]);
const r=o.aggregate([{{$lookup:{{from:'jc{t.stamp}',localField:'cust',foreignField:'_id',as:'c'}}}},{{$unwind:'$c'}},{{$sort:{{total:1}}}},{{$project:{{_id:0,n:'$c.name',t:'$total'}}}}]).toArray();
c.drop(); o.drop(); print(JSON.stringify(r.map(x=>[x.n,String(x.t)])));""")
    check(out.splitlines()[-1] == '[["a","10"],["a","11"],["b","20"]]', out)
    return "$lookup returns the 3 matched documents"


def surreal(t):
    r = t.surreal(f"""CREATE jc:1 SET name='a'; CREATE jc:2 SET name='b';
CREATE jo:1 SET cust=1, total=10; CREATE jo:2 SET cust=1, total=11; CREATE jo:3 SET cust=2, total=20;
SELECT total, (SELECT VALUE name FROM jc WHERE meta::id(id) = $parent.cust)[0] AS name FROM jo ORDER BY total;
REMOVE TABLE jc; REMOVE TABLE jo;""")
    rows = [[x["name"], str(x["total"])] for x in r[5]]
    check(rows == WANT, f"got {rows}")
    return "correlated subquery matching by value (SurrealQL has no JOIN keyword)"


def arango(t):
    c, o = f"jc{t.stamp}", f"jo{t.stamp}"
    for n in (c, o):
        t.arango("/_api/collection", {"name": n})
    try:
        t.aql(f"FOR d IN [{{_key:'1',name:'a'}},{{_key:'2',name:'b'}}] INSERT d INTO {c}")
        t.aql(f"FOR d IN [{{cust:'1',total:10}},{{cust:'1',total:11}},{{cust:'2',total:20}}] INSERT d INTO {o}")
        r = t.aql(f"FOR x IN {o} FOR y IN {c} FILTER y._key == x.cust SORT x.total RETURN [y.name, TO_STRING(x.total)]")
        check(r == WANT, f"got {r}")
    finally:
        for n in (c, o):
            t.arango(f"/_api/collection/{n}", method="DELETE")
    return "AQL nested FOR with FILTER on the key"


def couchbase(t):
    b = t.c["bucket"]
    t.n1ql(f"UPSERT INTO `{b}` (KEY, VALUE) VALUES ('jc1',{{'t':'c{t.stamp}','id':1,'name':'a'}}),('jc2',{{'t':'c{t.stamp}','id':2,'name':'b'}}),"
           f"('jo1',{{'t':'o{t.stamp}','cust':1,'total':10}}),('jo2',{{'t':'o{t.stamp}','cust':1,'total':11}}),('jo3',{{'t':'o{t.stamp}','cust':2,'total':20}})")
    try:
        r = t.n1ql(f"SELECT c.name, o.total FROM `{b}` o JOIN `{b}` c ON c.id = o.cust AND c.t = 'c{t.stamp}' "
                   f"WHERE o.t = 'o{t.stamp}' ORDER BY o.total", scan_consistency="request_plus")
        rows = [[x["name"], str(x["total"])] for x in r]
        check(rows == WANT, f"got {rows}")
    finally:
        t.n1ql(f"DELETE FROM `{b}` WHERE t IN ['c{t.stamp}','o{t.stamp}']")
    return "SQL++ JOIN ... ON"


PROBES = {
    "mysql": lambda t: sql(t.mysql, t), "sprig": lambda t: sql(t.mysql, t), "pg": lambda t: sql(t.psql, t),
    "mongo": mongo, "surreal": surreal, "arango": arango, "couchbase": couchbase,
}
if __name__ == "__main__":
    lib.run("joins", "sql", PROBES, "Joins")
